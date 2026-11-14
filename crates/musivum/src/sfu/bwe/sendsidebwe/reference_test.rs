//! New for musivum: the port held against the original over a whole session.
//!
//! The original has one test, of one feedback report. Here a reproducible pseudo-random
//! session is run through the congestion detector: packets sent at a paced rate, and the
//! transport-wide feedback a subscriber would give for them while the delay is steady, rises
//! (a queue building), stays high with loss, and recovers; a pause in sending; three probe
//! clusters (a quiet one, one that builds some delay, one during loss); a time of loss
//! without delay; a reset; feedback reports that arrive out of order, that are repeated and
//! that overlap the one before; the 16 bit sequence number and the 24 bit reference time
//! wrapping. After every report a line is made of what the detector holds: the congestion
//! state, its reason and the estimated capacity, what the listener was told, the packet
//! groups (their number, sums over all of them, the last two in full), the two measurements,
//! the trend of the captured traffic ratio, the probe packet group; and of what a feedback
//! tracker and a packet tracker of their own give for the same report with explicit times.
//!
//! A Go test laid over the original's package at the pin (`go test -overlay`, not kept in the
//! repository) ran the same session and printed the same lines: 439 of them, all identical to
//! the port's. `testdata/reference_output.txt` holds some dozens of those lines of the
//! original, each after its number in the session. With `MUSIVUM_SSBWE_SESSION_OUT` set to a
//! path, the test also writes all its lines there, to compare the whole session again.
//!
//! Where the original reads the clock itself, the session is laid out so that the clock does
//! not decide: the send times start 40 s before now, which puts the age at which packet
//! groups are pruned in the middle of the pause in sending, 10 s from either end of it; a
//! probe is finalized only once its last packet is reported, so that the wait for it to
//! settle is not asked; whether a probe is allowed is asked only where the answer does not
//! hang on the regulator's clock, or where the probe was made an hour ago. The trend detector
//! stamps its samples with the clock: equal samples one after the other are collapsed as long
//! as the session takes less than half a second from one to the next. Not in the lines: the
//! interval between reports that the detector's own feedback tracker estimates, which it
//! measures with the clock (the tracker next to it is given times).

use std::fmt::Write as _;

use parking_lot::Mutex;

use super::super::packet_group::ClassStat;
use super::*;
use crate::go::time;
use crate::protocol::logger;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::{
    RecvDelta, RunLengthChunk, StatusChunkTypeTcc, StatusVectorChunk, SymbolSizeTypeTcc,
};

// splitmix64
struct Rng {
    s: u64,
}

impl Rng {
    fn next(&mut self) -> u64 {
        self.s = self.s.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn n(&mut self, n: u64) -> i64 {
        (self.next() % n) as i64
    }
}

#[derive(Default)]
struct Listener {
    events: Mutex<Vec<String>>,
}

impl bwe::BweListener for Listener {
    fn on_congestion_state_change(
        &self,
        from_state: bwe::CongestionState,
        to_state: bwe::CongestionState,
        estimated_available_channel_capacity: i64,
    ) {
        self.events.lock().push(format!(
            "{from_state}>{to_state}@{estimated_available_channel_capacity}"
        ));
    }
}

// Go's %.9f
fn f(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    format!("{v:.9}")
}

fn list(l: &[String]) -> String {
    format!("[{}]", l.join(" "))
}

fn ints(l: &[i64]) -> String {
    list(&l.iter().map(i64::to_string).collect::<Vec<_>>())
}

fn stat(c: &ClassStat) -> String {
    format!(
        "{}/{},{}/{},{}/{}",
        c.primary.num_packets,
        c.primary.num_bytes,
        c.rtx.num_packets,
        c.rtx.num_bytes,
        c.probe.num_packets,
        c.probe.num_bytes
    )
}

fn group(pg: Option<&PacketGroup>) -> String {
    let Some(pg) = pg else {
        return "nil".to_string();
    };
    format!(
        "({}-{} s={}..{} r={}..{} a={} l={} ds={} dr={} inh={} pqd={} fin={})",
        pg.min_sequence_number,
        pg.max_sequence_number,
        pg.min_send_time,
        pg.max_send_time,
        pg.min_recv_time,
        pg.max_recv_time,
        stat(&pg.acked),
        stat(&pg.lost),
        pg.aggregate_send_delta,
        pg.aggregate_recv_delta,
        pg.inherited_queuing_delay,
        pg.propagated_queuing_delay(),
        pg.is_finalized()
    )
}

fn traffic(ts: Option<&TrafficStats>) -> String {
    let Some(ts) = ts else {
        return "nil".to_string();
    };
    format!(
        "(s={}..{} ds={} dr={} a={}/{} l={}/{} dur={} wl={} lp={} ctr={} abr={})",
        ts.min_send_time,
        ts.max_send_time,
        ts.send_delta,
        ts.recv_delta,
        ts.acked_packets,
        ts.acked_bytes,
        ts.lost_packets,
        ts.lost_bytes,
        ts.duration(),
        f(ts.weighted_loss()),
        ts.loss_penalty(),
        f(ts.captured_traffic_ratio()),
        ts.acknowledged_bitrate()
    )
}

fn qd(q: Option<&QdMeasurement>) -> String {
    let Some(q) = q else {
        return "nil".to_string();
    };
    let (lo, hi) = q.group_range();
    format!(
        "(n={} j={} d={} s={}..{} pqds={} sealed={} idx={}..{} qr={} tc={})",
        q.num_groups,
        q.num_jqr_groups,
        q.num_dqr_groups,
        q.min_send_time,
        q.max_send_time,
        ints(&q.propagated_queuing_delays),
        q.is_sealed(),
        lo,
        hi,
        q.queuing_region(),
        f(q.trend_coefficient())
    )
}

fn loss(l: Option<&LossMeasurement>) -> String {
    let Some(l) = l else {
        return "nil".to_string();
    };
    let (lo, hi) = l.group_range();
    format!(
        "(n={} sj={} sd={} idx={}..{} wl={} qr={} ts={})",
        l.num_groups,
        l.is_jqr_sealed,
        l.is_dqr_sealed,
        lo,
        hi,
        f(l.weighted_loss),
        l.queuing_region(),
        traffic(Some(&l.ts))
    )
}

fn ctr(c: &CongestionDetectorInner) -> String {
    let Some(trend) = c.congested_ctr_trend.as_ref() else {
        return "nil".to_string();
    };
    format!(
        "(dir={} lo={} hi={} enough={} cts={} cpg={})",
        trend.get_direction(),
        f(trend.get_lowest()),
        f(trend.get_highest()),
        trend.has_enough_samples(),
        traffic(c.congested_traffic_stats.as_ref()),
        group(c.congested_packet_group.as_ref())
    )
}

// delay of the queue in microseconds and loss in permille, at a send time in microseconds
fn phase(t: i64) -> (i64, u64) {
    let ms = t / 1000;
    if ms < 6000 {
        (0, 0)
    } else if ms < 10000 {
        ((ms - 6000) * 40, 0)
    } else if ms < 13000 {
        (160000, 150)
    } else if ms < 16000 {
        ((160000 - (ms - 13000) * 60).max(0), 0)
    } else if ms < 46500 {
        (0, 0)
    } else if ms < 46900 {
        ((ms - 46500) * 90, 0)
    } else if ms < 47500 {
        ((36000 - (ms - 46900) * 60).max(0), 0)
    } else if ms < 50000 {
        (0, 0)
    } else if ms < 56000 {
        (0, 250)
    } else if ms < 64000 {
        (0, 0)
    } else {
        ((ms - 64000) * 100, 0)
    }
}

#[derive(Clone, Copy, Default)]
struct Pkt {
    sn: u16,
    arrival: i64,
    lost: bool,
    reserved: bool,
}

struct Ind {
    sn: u16,
    status: u16,
    delta: i64,
}

struct Report {
    fb: TransportLayerCc,
    inds: Vec<Ind>,
    idx: usize,
}

#[derive(Clone, Copy, Default)]
struct ProbeStat {
    pp: i64,
    pb: i64,
    np: i64,
    nb: i64,
    nr: i64,
    nrb: i64,
}

const RECV_EPOCH: i64 = ((1 << 24) - 300) * 64000;
const PROBE_START: [i64; 4] = [0, 44_000_000, 46_500_000, 53_000_000];

struct Session {
    out: Vec<String>,
    rng: Rng,
    l: Arc<Listener>,
    c: CongestionDetector,
    trk: PacketTracker,
    tw: TwccFeedback,

    pkts: Vec<Pkt>,
    next_base: usize,
    since_fb: usize,
    fb_every: usize,
    fb_count: u8,
    fb_idx: usize,
    held: Option<Report>,
    cut: i64,
    last_send_t: i64,
    last_delay: i64,
}

impl Session {
    fn new() -> Self {
        let l = Arc::new(Listener::default());
        let c = CongestionDetector::new(CongestionDetectorParams {
            config: DEFAULT_CONGESTION_DETECTOR_CONFIG,
            logger: logger::get_logger(),
        });
        c.set_bwe_listener(Some(l.clone()));
        c.packet_tracker.lock.lock().sequence_number = 65000;
        let trk = PacketTracker::new(PacketTrackerParams {
            logger: logger::get_logger(),
        });
        trk.lock.lock().sequence_number = 65000;
        let tw = TwccFeedback::new(TwccFeedbackParams {
            logger: logger::get_logger(),
        });

        Self {
            out: Vec::new(),
            rng: Rng { s: 20261114 },
            l,
            c,
            trk,
            tw,
            pkts: Vec::new(),
            next_base: 0,
            since_fb: 0,
            fb_every: 30,
            fb_count: 250,
            fb_idx: 0,
            held: None,
            cut: 0,
            last_send_t: 0,
            last_delay: 0,
        }
    }

    fn deliver(&mut self, r: &Report, tag: &str) {
        self.c.handle_twcc_feedback(&r.fb);

        // the pieces on their own, with explicit times
        let (ref_t, ooo) = self
            .tw
            .process_report(&r.fb, Time::from_unix_nano(self.cut * 1000));
        let (mut cnt, mut sum_s, mut sum_r) = (0, 0i64, 0i64);
        let mut last_pi = PacketInfo::default();
        let mut recv_ref = ref_t;
        for ind in &r.inds {
            let mut recv_time = 0;
            if ind.status == 3 {
                continue;
            }
            if ind.status != 0 {
                recv_ref += ind.delta;
                recv_time = recv_ref;
            }
            let (pi, sd, rd) = self
                .trk
                .record_packet_indication_from_remote(ind.sn, recv_time);
            if pi.send_time != 0 {
                cnt += 1;
                sum_s += sd;
                sum_r += rd;
                last_pi = pi;
            }
        }

        let mut sb = String::new();
        let _ = write!(
            sb,
            "fb={}{} t={} base={} n={} ref={} cnt={}",
            r.idx,
            tag,
            self.last_send_t,
            r.fb.base_sequence_number,
            r.fb.packet_status_count,
            r.fb.reference_time,
            r.fb.fb_pkt_count
        );
        let events = std::mem::take(&mut *self.l.events.lock());
        // asked before the detector is looked into: its lock is not reentrant
        let goal = self.c.probe_cluster_is_goal_reached();
        let trk_max = self.c.packet_tracker.probe_max_sequence_number();
        let mut finalize = false;
        {
            let c = self.c.lock.lock();
            let _ = write!(
                sb,
                " | st={} rsn={} cap={} ev={} rtt={}",
                c.congestion_state,
                c.congestion_reason,
                c.estimated_available_channel_capacity,
                list(&events),
                f(c.rtt)
            );

            let ng = c.packet_groups.len();
            let (mut sa, mut sab, mut sl, mut slb, mut sds, mut sdr) = (0, 0, 0, 0, 0, 0);
            for pg in &c.packet_groups {
                sa += pg.acked.num_packets();
                sab += pg.acked.num_bytes();
                sl += pg.lost.num_packets();
                slb += pg.lost.num_bytes();
                sds += pg.aggregate_send_delta;
                sdr += pg.aggregate_recv_delta;
            }
            let g0 = c
                .packet_groups
                .first()
                .map_or(0, |pg| pg.min_sequence_number);
            let _ = write!(
                sb,
                " | ng={ng} g0={g0} sum={sa}/{sab}/{sl}/{slb}/{sds}/{sdr}"
            );
            let last = group(c.packet_groups.last());
            let (mut prev, mut pts) = ("nil".to_string(), "nil".to_string());
            if ng > 1 {
                prev = group(Some(&c.packet_groups[ng - 2]));
                let mut ts = TrafficStats::new(TrafficStatsParams {
                    config: DEFAULT_WEIGHTED_LOSS_CONFIG,
                    ..Default::default()
                });
                ts.merge(&c.packet_groups[ng - 2].traffic());
                pts = traffic(Some(&ts));
            }
            let _ = write!(sb, " last={last} prev={prev} pts={pts}");
            let _ = write!(
                sb,
                " | qd={} loss={}",
                qd(c.qd_measurement.as_ref()),
                loss(c.loss_measurement.as_ref())
            );
            let _ = write!(
                sb,
                " | ctr={} est={}",
                ctr(&c),
                traffic(c.estimate_traffic_stats.as_ref())
            );
            let _ = write!(
                sb,
                " | tw={}/{}/{}/{}/{}/{} trk={}/{}/{}/{}/{}/{}",
                ref_t,
                ooo,
                self.tw.estimated_feedback_interval.0,
                self.tw.num_reports,
                self.tw.num_reports_out_of_order,
                self.tw.cycles,
                cnt,
                sum_s,
                sum_r,
                last_pi.sequence_number,
                last_pi.send_time,
                last_pi.recv_time
            );
            if let Some(ppg) = c.probe_packet_group.as_ref() {
                let (sig, abr) = self.c.params.config.probe_signal.probe_signal(ppg);
                let done = !ppg.done_at.is_zero();
                let _ = write!(
                    sb,
                    " | ppg=(id={} max={} trkmax={} done={} grp={} sig={}/{} valid={} goal={})",
                    ppg.pci.id,
                    ppg.max_sequence_number,
                    trk_max,
                    done,
                    group(Some(&ppg.packet_group)),
                    sig,
                    abr,
                    self.c.params.config.probe_signal.is_valid(ppg.pci),
                    goal
                );
                finalize = done && trk_max != 0 && ppg.max_sequence_number >= trk_max;
            } else {
                sb.push_str(" | ppg=nil");
            }
        }
        if finalize {
            let (ps, capacity, ok) = self.c.probe_cluster_finalize();
            let _ = write!(
                sb,
                " fin={}/{}/{} canProbe={} dur={}",
                ps,
                capacity,
                ok,
                self.c.can_probe(),
                self.c.probe_duration().0
            );
        }
        self.out.push(sb);
    }

    fn do_feedback(&mut self) {
        let end = self.pkts.len();
        self.cut = RECV_EPOCH + self.last_send_t + 20_000 + self.last_delay + 2_500;
        let r3 = self.rng.n(3);
        let o8 = self.rng.n(8) as usize;
        let overlap = if r3 == 0 { o8 } else { 0 };
        let b = self.next_base.saturating_sub(overlap);

        let mut sym = Vec::new();
        let mut deltas = Vec::new();
        let mut inds = Vec::new();
        let mut ref64 = -1i64;
        let mut prev = 0i64;
        for p in &self.pkts[b..end] {
            if p.lost || p.arrival > self.cut {
                sym.push(0u16);
                inds.push(Ind {
                    sn: p.sn,
                    status: 0,
                    delta: 0,
                });
                continue;
            }
            if p.reserved {
                sym.push(3);
                inds.push(Ind {
                    sn: p.sn,
                    status: 3,
                    delta: 0,
                });
                continue;
            }
            if ref64 < 0 {
                ref64 = p.arrival / 64000;
                prev = ref64 * 64000;
            }
            let d = p.arrival - prev;
            prev = p.arrival;
            let u = d / 250;
            let mut s = 1u16;
            if !(0..=255).contains(&u) {
                s = 2;
                assert!(
                    (-32768..=32767).contains(&u),
                    "a delta that does not fit: {d}"
                );
            }
            sym.push(s);
            deltas.push(RecvDelta {
                type_tcc_packet: SymbolTypeTcc::from(s),
                delta: d,
            });
            inds.push(Ind {
                sn: p.sn,
                status: s,
                delta: d,
            });
        }
        if ref64 < 0 {
            ref64 = self.cut / 64000;
        }

        let mut chunks = Vec::new();
        let mut i = 0;
        while i < sym.len() {
            let mut run = 1;
            while i + run < sym.len() && sym[i + run] == sym[i] {
                run += 1;
            }
            let pick = self.rng.n(4);
            if run >= 8 || (run >= 3 && pick == 0) {
                chunks.push(PacketStatusChunk::RunLengthChunk(RunLengthChunk {
                    type_tcc: StatusChunkTypeTcc::RunLengthChunk,
                    packet_status_symbol: SymbolTypeTcc::from(sym[i]),
                    run_length: run as u16,
                }));
                i += run;
                continue;
            }
            let get = |k: usize| sym.get(i + k).copied().unwrap_or(0);
            let all_bit = (0..14).all(|k| get(k) <= 1);
            if all_bit && pick == 1 {
                chunks.push(PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                    type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                    symbol_size: SymbolSizeTypeTcc::OneBit,
                    symbol_list: (0..14).map(|k| SymbolTypeTcc::from(get(k))).collect(),
                }));
                i += 14;
                continue;
            }
            chunks.push(PacketStatusChunk::StatusVectorChunk(StatusVectorChunk {
                type_tcc: StatusChunkTypeTcc::StatusVectorChunk,
                symbol_size: SymbolSizeTypeTcc::TwoBit,
                symbol_list: (0..7).map(|k| SymbolTypeTcc::from(get(k))).collect(),
            }));
            i += 7;
        }

        let fb = TransportLayerCc {
            sender_ssrc: 1,
            media_ssrc: 2,
            base_sequence_number: self.pkts[b].sn,
            packet_status_count: (end - b) as u16,
            reference_time: (ref64 & 0xFFFFFF) as u32,
            fb_pkt_count: self.fb_count,
            packet_chunks: chunks,
            recv_deltas: deltas,
        };
        self.fb_count = self.fb_count.wrapping_add(1);
        let idx = self.fb_idx;
        let r = Report { fb, inds, idx };
        self.fb_idx += 1;
        self.next_base = end;
        self.since_fb = 0;
        self.fb_every = (20 + self.rng.n(25)) as usize;

        match idx {
            10 => self.c.update_rtt(0.12),
            20 => self.c.update_rtt(0.0),
            30 => self.c.update_rtt(0.2),
            _ => {}
        }
        let swap = idx == 40 || idx == 100 || idx == 280;
        let repeat = idx == 60 || idx == 105 || idx == 290;
        if swap {
            self.held = Some(r);
            return;
        }
        self.deliver(&r, "");
        if let Some(h) = self.held.take() {
            self.deliver(&h, "late");
        }
        if repeat {
            self.deliver(&r, "again");
        }
    }

    fn run(&mut self) {
        let base = mono::unix_micro() - 40_000_000;
        let mut probe_state = [0; 4];
        let mut pcis = [ccutils::ProbeClusterInfo::default(); 4];
        let mut probe_stat = [ProbeStat::default(); 4];
        let mut reset_done = false;

        let mut t_send = 0i64;
        while t_send < 66_000_000 {
            if (19_500_000..40_000_000).contains(&t_send) {
                if self.since_fb > 0 {
                    self.do_feedback();
                }
                t_send = 40_000_000;
            }

            for id in 1..=3 {
                if probe_state[id] == 0 && t_send >= PROBE_START[id] {
                    let mut created_at = mono::now();
                    if id != 1 {
                        created_at = created_at.add(-time::HOUR);
                    }
                    pcis[id] = ccutils::ProbeClusterInfo {
                        id: ccutils::ProbeClusterId(id as u32),
                        created_at,
                        goal: ccutils::ProbeClusterGoal {
                            available_bandwidth_bps: 1_000_000,
                            expected_usage_bps: 1_200_000,
                            desired_bps: 1_500_000,
                            duration: 300 * time::MILLISECOND,
                            desired_bytes: 60_000,
                        },
                        ..Default::default()
                    };
                    self.c.probe_cluster_starting(pcis[id]);
                    self.trk.probe_cluster_starting(pcis[id].id);
                    probe_state[id] = 1;
                    let (ps, capacity, ok) = self.c.probe_cluster_finalize();
                    self.out.push(format!(
                        "ev probe-start id={} t={} canProbe={} dur={} goal={} fin={}/{}/{}",
                        id,
                        t_send,
                        self.c.can_probe(),
                        self.c.probe_duration().0,
                        self.c.probe_cluster_is_goal_reached(),
                        ps,
                        capacity,
                        ok
                    ));
                }
                if probe_state[id] == 1 && t_send >= PROBE_START[id] + 400_000 {
                    let st = probe_stat[id];
                    pcis[id].result = ccutils::ProbeClusterResult {
                        start_time: 5_000_000_000,
                        end_time: 5_400_000_000,
                        packets_probe: st.pp,
                        bytes_probe: st.pb,
                        packets_non_probe_primary: st.np,
                        bytes_non_probe_primary: st.nb,
                        packets_non_probe_rtx: st.nr,
                        bytes_non_probe_rtx: st.nrb,
                        is_completed: true,
                    };
                    self.c.probe_cluster_done(pcis[id]);
                    self.trk.probe_cluster_done(pcis[id].id);
                    probe_state[id] = 2;
                    self.out.push(format!(
                        "ev probe-done id={} t={} bytes={} goal={} trkmax={}",
                        id,
                        t_send,
                        pcis[id].result.bytes(),
                        self.c.probe_cluster_is_goal_reached(),
                        self.trk.probe_max_sequence_number()
                    ));
                }
            }
            if !reset_done && t_send >= 62_000_000 {
                self.c.reset();
                reset_done = true;
                let c = self.c.lock.lock();
                self.out.push(format!(
                    "ev reset t={} st={} rsn={} cap={} ng={} rtt={} qd={} loss={} ctr={}",
                    t_send,
                    c.congestion_state,
                    c.congestion_reason,
                    c.estimated_available_channel_capacity,
                    c.packet_groups.len(),
                    f(c.rtt),
                    qd(c.qd_measurement.as_ref()),
                    loss(c.loss_measurement.as_ref()),
                    ctr(&c)
                ));
            }

            let size = 200 + self.rng.n(1000);
            let is_rtx = self.rng.n(50) == 0;
            let gap_n = self.rng.n(3000);
            let loss_draw = self.rng.n(1000) as u64;
            let late_draw = self.rng.n(40);
            let late_extra = 30_000 + self.rng.n(50_000);
            let reserved_draw = self.rng.n(300);
            let jitter = self.rng.n(2000);
            let probe_draw = self.rng.n(2);

            let mut pcid = ccutils::PROBE_CLUSTER_ID_INVALID;
            let mut is_probe = false;
            let mut gap = 2000 + gap_n;
            let mut in_probe = 0;
            for (id, state) in probe_state.iter().enumerate().skip(1) {
                if *state == 1 {
                    pcid = ccutils::ProbeClusterId(id as u32);
                    in_probe = id;
                    is_probe = probe_draw == 0;
                    gap = 500 + gap_n / 3;
                }
            }

            let (delay, loss_permille) = phase(t_send);
            let sn = self
                .c
                .packet_tracker
                .record_packet_send_and_get_sequence_number(
                    base + t_send,
                    size as usize,
                    is_rtx,
                    pcid,
                    is_probe,
                );
            let sn2 = self.trk.record_packet_send_and_get_sequence_number(
                base + t_send,
                size as usize,
                is_rtx,
                pcid,
                is_probe,
            );
            assert_eq!(sn, sn2, "the trackers disagree");
            let mut p = Pkt {
                sn,
                lost: loss_draw < loss_permille,
                reserved: reserved_draw == 0 && in_probe == 0,
                ..Default::default()
            };
            let extra = if late_draw == 0 { late_extra } else { 0 };
            p.arrival = RECV_EPOCH + t_send + 20_000 + delay + jitter + extra;
            p.arrival -= p.arrival % 250;
            self.pkts.push(p);
            if in_probe != 0 {
                let st = &mut probe_stat[in_probe];
                if is_rtx {
                    st.nr += 1;
                    st.nrb += size;
                } else if is_probe {
                    st.pp += 1;
                    st.pb += size;
                } else {
                    st.np += 1;
                    st.nb += size;
                }
            }
            self.last_send_t = t_send;
            self.last_delay = delay;
            self.since_fb += 1;
            t_send += gap;
            if self.since_fb >= self.fb_every {
                self.do_feedback();
            }
        }
        if self.since_fb > 0 {
            self.do_feedback();
        }
        if let Some(h) = self.held.take() {
            self.deliver(&h, "late");
        }
    }
}

// New for musivum.
#[test]
fn a_session_gives_what_the_original_gives() {
    let mut s = Session::new();
    s.run();

    if let Ok(path) = std::env::var("MUSIVUM_SSBWE_SESSION_OUT") {
        std::fs::write(path, s.out.join("\n") + "\n").expect("the lines are written");
    }

    assert_eq!(439, s.out.len(), "lines");
    assert_eq!(13822, s.pkts.len(), "packets");
    assert_eq!(429, s.fb_idx, "reports");

    let mut pinned = 0;
    for line in include_str!("testdata/reference_output.txt").lines() {
        let (n, expected) = line.split_once('\t').expect("a number, a tab, a line");
        let n: usize = n.parse().expect("the number of a line");
        assert_eq!(expected, s.out[n], "line {n}");
        pinned += 1;
    }
    assert!(pinned >= 36, "{pinned} lines of the original");
}
