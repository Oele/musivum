//! New for musivum: the port held against the original, operation by operation.
//!
//! A script is a list of operations on statistics objects, one per line (`new R 1`,
//! `r.update 1 <packet time> <sequence number> <time stamp> ...`, `r.proto 1`, ...). A Go
//! test laid over the original package at the pin (`go test -overlay`, kept outside the
//! repository) generated the scripts and wrote down what the original answers to each
//! operation; `replay` below runs a script through the port and writes down the same. What
//! is written leaves out what the original reads from the wall clock: start and end times,
//! durations and rates, the delay since the last sender report in a reception report, the
//! NTP and RTP time of a sender report a sender makes (of which it says whether they belong
//! together) and the drifts computed from those, and the round trip time measured from a
//! receiver report that answers such a sender report.
//!
//! `testdata/reference.ops` and `testdata/reference.out` are a small script and the
//! original's answers, pinned here. `MUSIVUM_RTPSTATS_REFERENCE` may name a directory with
//! larger ones (`<name>.ops` and `<name>.go.out`), which are then replayed as well; what the
//! port answers is written next to them as `<name>.rust.out`.
//!
//! One thing is written differently on purpose: a time of the last key frame, FIR or layer
//! lock PLI that is not set is the year 1 in the original's message and 1970 in the port's
//! (`docs/port-status.md`, "Where the port cannot be the original"). Both are printed as
//! "not set".

use std::collections::HashMap;
use std::fmt::Write as _;

use musivum_protocol::Timestamp;

use super::*;
use crate::go::time::Duration;
use crate::mediatransportutil::NtpTime;
use crate::protocol::livekit;
use crate::protocol::utils::rtputil::RtpConverter;
use crate::webrtc::rtcp::reception_report::ReceptionReport;

enum Obj {
    R(RtpStatsReceiver),
    S(RtpStatsSender),
    RL(RtpStatsReceiverLite),
    SL(RtpStatsSenderLite),
}

// what every statistics object has
macro_rules! lite {
    ($obj:expr, $r:ident => $e:expr) => {
        match $obj {
            Obj::R($r) => $e,
            Obj::S($r) => $e,
            Obj::RL($r) => $e,
            Obj::SL($r) => $e,
        }
    };
}

// what the receiver and the sender have
macro_rules! base {
    ($obj:expr, $r:ident => $e:expr) => {
        match $obj {
            Obj::R($r) => $e,
            Obj::S($r) => $e,
            _ => panic!("neither a receiver nor a sender"),
        }
    };
}

fn b(v: bool) -> u8 {
    v as u8
}

fn f(v: f64) -> String {
    format!("{:016x}", v.to_bits())
}

fn f32s(v: f32) -> String {
    format!("{:08x}", v.to_bits())
}

fn gap(m: &HashMap<i32, u32>) -> String {
    if m.is_empty() {
        return "-".to_string();
    }
    let mut keys: Vec<i32> = m.keys().copied().collect();
    keys.sort();
    let parts: Vec<String> = keys.iter().map(|k| format!("{}:{}", k, m[k])).collect();
    parts.join(",")
}

// Whether a time of a message is set: not missing and not the zero time, which is 1970 in
// the port's messages. (The original's zero time is the year 1 where the field is a
// `time.Time` and 1970 where it is nanoseconds.)
fn time_set(ts: &Option<Timestamp>) -> bool {
    match ts {
        None => false,
        Some(ts) => ts.seconds != 0 || ts.nanos != 0,
    }
}

fn delta(d: Option<&RtpDeltaInfo>) -> String {
    let Some(d) = d else {
        return "nil".to_string();
    };
    // the start and the end are read from the wall clock: only that they are set and in order
    let tset = d.start_time.unix_nano() != 0 && d.end_time.unix_nano() >= d.start_time.unix_nano();
    format!(
        "t={} p={} b={} hb={} dup={} bdup={} hdup={} pad={} bpad={} hpad={} fec={}/{}/{}/{} lost={} miss={} ooo={} fr={} rtt={} jit={} nacks={} nrep={} plis={} firs={}",
        b(tset),
        d.packets,
        d.bytes,
        d.header_bytes,
        d.packets_duplicate,
        d.bytes_duplicate,
        d.header_bytes_duplicate,
        d.packets_padding,
        d.bytes_padding,
        d.header_bytes_padding,
        d.fec_packets,
        d.fec_bytes,
        d.fec_packets_discarded,
        d.fec_packets_recovered,
        d.packets_lost,
        d.packets_missing,
        d.packets_out_of_order,
        d.frames,
        d.rtt_max,
        f(d.jitter_max),
        d.nacks,
        d.nack_repeated,
        d.plis,
        d.firs,
    )
}

fn drift(d: &Option<livekit::RtpDrift>) -> String {
    let Some(d) = d else {
        return "nil".to_string();
    };
    let st = d.start_time.unwrap();
    let et = d.end_time.unwrap();
    format!(
        "st={}.{:09} et={}.{:09} dur={} sts={} ets={} ticks={} samples={} ms={} rate={}",
        st.seconds,
        st.nanos,
        et.seconds,
        et.nanos,
        f(d.duration),
        d.start_timestamp,
        d.end_timestamp,
        d.rtp_clock_ticks,
        d.drift_samples,
        f(d.drift_ms),
        f(d.clock_rate),
    )
}

#[derive(Default)]
struct Replay {
    n: usize,
    out: String,
    objs: HashMap<u64, Obj>,
    rates: HashMap<u64, u32>,
    last_sr_ntp: HashMap<u64, u64>,
}

impl Replay {
    fn p(&mut self, s: String) {
        let _ = writeln!(self.out, "{} {}", self.n, s);
    }

    // the fields of a statistics message that do not depend on the wall clock; `rtt` says
    // whether the round trip times are among them, `report_drift` whether the drifts
    // computed from sender reports are.
    fn proto(&mut self, p: Option<livekit::RtpStats>, rtt: bool, report_drift: bool) {
        let Some(p) = p else {
            self.p("proto nil".to_string());
            return;
        };
        let (mut rtt_current, mut rtt_max) = (p.rtt_current, p.rtt_max);
        if !rtt {
            (rtt_current, rtt_max) = (0, 0);
        }
        self.p(format!(
            "proto packets={} bytes={} hbytes={} lost={} losspct={} dup={} bdup={} hdup={} pad={} bpad={} hpad={} ooo={} frames={} kf={} kfset={} jit={} jitmax={} gap={} nacks={} acks={} misses={} rep={} plis={} pliset={} firs={} firset={} rtt={} rttmax={} llplis={} llset={} fec={}/{}/{}/{}",
            p.packets,
            p.bytes,
            p.header_bytes,
            p.packets_lost,
            f32s(p.packet_loss_percentage),
            p.packets_duplicate,
            p.bytes_duplicate,
            p.header_bytes_duplicate,
            p.packets_padding,
            p.bytes_padding,
            p.header_bytes_padding,
            p.packets_out_of_order,
            p.frames,
            p.key_frames,
            b(time_set(&p.last_key_frame)),
            f(p.jitter_current),
            f(p.jitter_max),
            gap(&p.gap_histogram),
            p.nacks,
            p.nack_acks,
            p.nack_misses,
            p.nack_repeated,
            p.plis,
            b(time_set(&p.last_pli)),
            p.firs,
            b(time_set(&p.last_fir)),
            rtt_current,
            rtt_max,
            p.layer_lock_plis,
            b(time_set(&p.last_layer_lock_pli)),
            p.fec_packets,
            p.fec_bytes,
            p.fec_packets_discarded,
            p.fec_packets_recovered,
        ));
        self.p(format!("drift.packet {}", drift(&p.packet_drift)));
        if report_drift {
            self.p(format!("drift.ntp {}", drift(&p.ntp_report_drift)));
            self.p(format!(
                "drift.received {}",
                drift(&p.received_report_drift)
            ));
            self.p(format!("drift.rebased {}", drift(&p.rebased_report_drift)));
        }
    }

    fn recv(&self, id: u64) -> &RtpStatsReceiver {
        match &self.objs[&id] {
            Obj::R(r) => r,
            _ => panic!("{id} is not a receiver"),
        }
    }

    fn send(&self, id: u64) -> &RtpStatsSender {
        match &self.objs[&id] {
            Obj::S(r) => r,
            _ => panic!("{id} is not a sender"),
        }
    }

    fn recv_lite(&self, id: u64) -> &RtpStatsReceiverLite {
        match &self.objs[&id] {
            Obj::RL(r) => r,
            _ => panic!("{id} is not a lite receiver"),
        }
    }

    fn send_lite(&self, id: u64) -> &RtpStatsSenderLite {
        match &self.objs[&id] {
            Obj::SL(r) => r,
            _ => panic!("{id} is not a lite sender"),
        }
    }

    fn delta_info(&self, id: u64, snapshot_id: u32) -> Option<RtpDeltaInfo> {
        base!(&self.objs[&id], r => r.delta_info(snapshot_id))
    }

    fn exec(&mut self, line: &str) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let u = |i: usize| -> u64 {
            fields[i]
                .parse()
                .unwrap_or_else(|e| panic!("bad number in {line:?}: {e}"))
        };
        let s = |i: usize| -> i64 {
            fields[i]
                .parse()
                .unwrap_or_else(|e| panic!("bad number in {line:?}: {e}"))
        };
        let bb = |i: usize| -> bool { fields[i] == "1" };
        let mut id = 0u64;
        if fields.len() > 1 && !fields[0].starts_with("fn.") && fields[0] != "new" {
            id = u(1);
        }
        let sr_state = |i: usize| -> Option<livekit::RtcpSenderReportState> {
            if fields[i] == "nil" {
                return None;
            }
            Some(livekit::RtcpSenderReportState {
                rtp_timestamp: u(i) as u32,
                rtp_timestamp_ext: u(i + 1),
                ntp_timestamp: u(i + 2),
                at: s(i + 3),
                at_adjusted: s(i + 4),
                ..Default::default()
            })
        };

        match fields[0] {
            "new" => {
                let oid = u(2);
                let obj = match fields[1] {
                    "R" => Obj::R(RtpStatsReceiver::new(RtpStatsParams::default())),
                    "S" => Obj::S(RtpStatsSender::new(
                        RtpStatsParams { is_rtx: bb(4) },
                        u(3) as usize,
                    )),
                    "RL" => Obj::RL(RtpStatsReceiverLite::new(RtpStatsParams::default())),
                    "SL" => Obj::SL(RtpStatsSenderLite::new(RtpStatsParams::default())),
                    other => panic!("unknown kind {other:?}"),
                };
                self.objs.insert(oid, obj);
            }

            // what every statistics object has
            "clock" => {
                self.rates.insert(id, u(2) as u32);
                lite!(&self.objs[&id], r => r.set_clock_rate(u(2) as u32));
            }
            "stop" => lite!(&self.objs[&id], r => r.stop()),
            "nack" => lite!(&self.objs[&id], r => r.update_nack(u(2) as u32)),
            "nackp" => lite!(
                &self.objs[&id],
                r => r.update_nack_processed(u(2) as u32, u(3) as u32, u(4) as u32)
            ),
            "pli" => lite!(&self.objs[&id], r => r.update_pli(u(2) as u32)),
            "pliat" => lite!(&self.objs[&id], r => r.update_pli_and_time(u(2) as u32)),
            "plitime" => lite!(&self.objs[&id], r => r.update_pli_time()),
            "checkpli" => {
                let ok = lite!(&self.objs[&id], r => r.check_and_update_pli(s(2), bb(3)));
                self.p(format!("checkpli {}", b(ok)));
            }
            "active" => {
                let (active, last_pli) = lite!(&self.objs[&id], r => (r.is_active(), r.last_pli()));
                self.p(format!("active {} pliset={}", b(active), b(last_pli != 0)));
            }

            // what the receiver and the sender have
            "fir" => base!(&self.objs[&id], r => r.update_fir(u(2) as u32)),
            "firtime" => base!(&self.objs[&id], r => r.update_fir_time()),
            "kf" => base!(&self.objs[&id], r => r.update_key_frame(u(2) as u32)),
            "rtt" => base!(&self.objs[&id], r => r.update_rtt(u(2) as u32)),
            "fec" => base!(&self.objs[&id], r => r.update_fec(u(2), u(3), u(4), u(5))),

            // receiver
            "r.update" => {
                let fs = self.recv(id).update(
                    s(2),
                    u(3) as u16,
                    u(4) as u32,
                    bb(5),
                    u(6) as usize,
                    u(7) as usize,
                    u(8) as usize,
                );
                self.p(format!(
                    "flow {} {} {} {} {} {} {}",
                    fs.unhandled_reason,
                    fs.loss_start_inclusive,
                    fs.loss_end_exclusive,
                    b(fs.is_duplicate),
                    b(fs.is_out_of_order),
                    fs.ext_sequence_number,
                    fs.ext_timestamp,
                ));
            }
            "r.sr" => {
                let input = if fields[2] == "nil" {
                    None
                } else {
                    Some(livekit::RtcpSenderReportState {
                        ntp_timestamp: u(2),
                        rtp_timestamp: u(3) as u32,
                        at: s(4),
                        ..Default::default()
                    })
                };
                let ok = self.recv(id).set_rtcp_sender_report_data(input.as_ref());
                match self.recv(id).get_rtcp_sender_report_data() {
                    None => self.p(format!("sr {} srdata nil", b(ok))),
                    Some(sd) => self.p(format!(
                        "sr {} srdata {} {} {} {} {} {} {}",
                        b(ok),
                        sd.rtp_timestamp,
                        sd.rtp_timestamp_ext,
                        sd.ntp_timestamp,
                        sd.at,
                        sd.at_adjusted,
                        sd.packets,
                        sd.octets,
                    )),
                }
            }
            "r.snap" => {
                let snapshot_id = self.recv(id).new_snapshot_id();
                self.p(format!("snap {snapshot_id}"));
            }
            "r.delta" => {
                let d = self.recv(id).delta_info(u(2) as u32);
                self.p(format!("delta {}", delta(d.as_ref())));
            }
            "r.rr" => {
                match self
                    .recv(id)
                    .get_rtcp_reception_report(u(2) as u32, u(3) as u8, u(4) as u32)
                {
                    None => self.p("rr nil".to_string()),
                    // the delay is left out: it is the time since the last sender report by
                    // the wall clock
                    Some(rr) => self.p(format!(
                        "rr {} {} {} {} {} {}",
                        rr.ssrc,
                        rr.fraction_lost,
                        rr.total_lost,
                        rr.last_sequence_number,
                        rr.jitter,
                        rr.last_sender_report,
                    )),
                }
            }
            "r.proto" => {
                let p = self.recv(id).to_proto();
                self.proto(p, true, true);
            }
            "r.misc" => {
                let r = self.recv(id);
                let (kf, kft) = r.key_frame();
                let line = format!(
                    "misc hts={} hsn={} ehsn={} active={} rtt={} kf={} kfset={} pliset={} srtset={}",
                    r.highest_timestamp(),
                    r.highest_sequence_number(),
                    r.extended_highest_sequence_number(),
                    b(r.is_active()),
                    r.get_rtt(),
                    kf,
                    b(!kft.is_zero()),
                    b(r.last_pli() != 0),
                    b(!r.last_sender_report_time().is_zero()),
                );
                self.p(line);
            }

            // sender
            "s.update" => self.send(id).update(
                s(2),
                u(3),
                u(4),
                bb(5),
                u(6) as usize,
                u(7) as usize,
                u(8) as usize,
                bb(9),
            ),
            "s.rrin" => {
                let (rtt, changed) = self.send(id).update_from_receiver_report(&ReceptionReport {
                    ssrc: u(2) as u32,
                    fraction_lost: u(3) as u8,
                    total_lost: u(4) as u32,
                    last_sequence_number: u(5) as u32,
                    jitter: u(6) as u32,
                    last_sender_report: u(7) as u32,
                    delay: u(8) as u32,
                });
                self.p(format!("rrin {} {}", rtt, b(changed)));
            }
            "s.rrin_lsr" => {
                // the last sender report this object made is the one the report answers: the
                // round trip time is then the time that passed by the wall clock, a
                // millisecond or less
                let (rtt, _) = self.send(id).update_from_receiver_report(&ReceptionReport {
                    ssrc: u(2) as u32,
                    fraction_lost: u(3) as u8,
                    total_lost: u(4) as u32,
                    last_sequence_number: u(5) as u32,
                    jitter: u(6) as u32,
                    last_sender_report: (self.last_sr_ntp[&id] >> 16) as u32,
                    delay: u(7) as u32,
                });
                self.p(format!("rrin_lsr small={}", b(rtt <= 1000)));
            }
            "s.srout" => {
                let ssrc = u(2) as u32;
                let Some(publisher) = sr_state(3) else {
                    let sr = self.send(id).get_rtcp_sender_report(ssrc, None, 0, false);
                    self.p(format!("srout nil={}", b(sr.is_none())));
                    return;
                };
                let ts_offset = u(8);
                let pass = bb(9);
                let Some(sr) =
                    self.send(id)
                        .get_rtcp_sender_report(ssrc, Some(&publisher), ts_offset, pass)
                else {
                    self.p("srout nil".to_string());
                    return;
                };
                self.last_sr_ntp.insert(id, sr.ntp_time);
                // the NTP time and the RTP time are made from the wall clock: what is printed
                // of them is whether they belong together
                let conv = RtpConverter::new(self.rates[&id] as i64);
                let d = if pass {
                    NtpTime(sr.ntp_time)
                        .time()
                        .sub(NtpTime(publisher.ntp_timestamp).time())
                } else {
                    Duration(
                        NtpTime(sr.ntp_time)
                            .time()
                            .unix_nano()
                            .wrapping_sub(publisher.at_adjusted),
                    )
                };
                let exp = publisher
                    .rtp_timestamp_ext
                    .wrapping_sub(ts_offset)
                    .wrapping_add(conv.to_rtp_ext(d)) as u32;
                let diff = sr.rtp_time.wrapping_sub(exp) as i32;
                self.p(format!(
                    "srout {} {} {} consistent={}",
                    sr.ssrc,
                    sr.packet_count,
                    sr.octet_count,
                    b((-1..=1).contains(&diff)),
                ));
            }
            "s.adjust" => self
                .send(id)
                .maybe_adjust_first_packet_time(sr_state(2).as_ref(), u(7)),
            "s.expts" => match self.send(id).get_expected_rtp_timestamp(s(2)) {
                Err(err) => self.p(format!("expts err {err}")),
                Ok(ts) => self.p(format!("expts {ts}")),
            },
            "s.snap" => {
                let snapshot_id = self.send(id).new_snapshot_id();
                self.p(format!("snap {snapshot_id}"));
            }
            "s.ssnap" => {
                let snapshot_id = self.send(id).new_sender_snapshot_id();
                self.p(format!("ssnap {snapshot_id}"));
            }
            "s.delta" => {
                let d = self.send(id).delta_info(u(2) as u32);
                self.p(format!("delta {}", delta(d.as_ref())));
            }
            "s.sdelta" => {
                let (ds, dr) = self.send(id).delta_info_sender(u(2) as u32);
                self.p(format!("sdelta.s {}", delta(ds.as_ref())));
                self.p(format!("sdelta.r {}", delta(dr.as_ref())));
            }
            "s.proto" => {
                // the drifts from sender reports are left out: the reports a sender makes
                // carry the wall clock
                let p = self.send(id).to_proto();
                self.proto(p, true, false);
            }
            "s.proto_nortt" => {
                let p = self.send(id).to_proto();
                self.proto(p, false, false);
            }
            "s.misc" => {
                let r = self.send(id);
                let (kf, kft) = r.key_frame();
                let line = format!(
                    "misc ehsn={} seen={} rrset={} active={} rtt={} kf={} kfset={} pliset={}",
                    r.ext_highest_sequence_number(),
                    r.get_packets_seen_minus_padding(),
                    b(r.last_receiver_report_time() != 0),
                    b(r.is_active()),
                    r.get_rtt(),
                    kf,
                    b(!kft.is_zero()),
                    b(r.last_pli() != 0),
                );
                self.p(line);
            }
            "s.llpli" => self.send(id).update_layer_lock_pli_and_time(u(2) as u32),
            "s.seed" => self.send(id).seed(self.send(u(2))),

            // lite
            "rl.update" => {
                let fs = self.recv_lite(id).update(s(2), u(3) as usize, u(4) as u16);
                self.p(format!(
                    "flowlite {} {} {} {}",
                    b(fs.is_not_handled),
                    fs.loss_start_inclusive,
                    fs.loss_end_exclusive,
                    fs.ext_sequence_number,
                ));
            }
            "rl.snap" => {
                let snapshot_id = self.recv_lite(id).new_snapshot_lite_id();
                self.p(format!("snap {snapshot_id}"));
            }
            "rl.delta" => match self.recv_lite(id).delta_info_lite(u(2) as u32) {
                None => self.p("deltalite nil".to_string()),
                Some(d) => {
                    let tset = d.start_time.unix_nano() != 0
                        && d.end_time.unix_nano() >= d.start_time.unix_nano();
                    self.p(format!(
                        "deltalite t={} p={} b={} lost={} ooo={} nacks={}",
                        b(tset),
                        d.packets,
                        d.bytes,
                        d.packets_lost,
                        d.packets_out_of_order,
                        d.nacks,
                    ));
                }
            },
            "rl.proto" => {
                let p = self.recv_lite(id).to_proto();
                self.proto(p, true, true);
            }
            "sl.update" => self.send_lite(id).update(s(2), u(3) as usize, u(4)),
            "sl.proto" => {
                let p = self.send_lite(id).to_proto();
                self.proto(p, true, true);
            }
            "sl.misc" => {
                let ehsn = self.send_lite(id).ext_highest_sequence_number();
                self.p(format!("misc ehsn={ehsn}"));
            }

            // the functions of the package
            "fn.aggdelta" => {
                let a = self.delta_info(u(1), u(2) as u32);
                let c = self.delta_info(u(3), u(4) as u32);
                let agg = aggregate_rtp_delta_info(&[a.as_ref(), None, c.as_ref()]);
                self.p(format!("aggdelta {}", delta(agg.as_ref())));
                let agg = aggregate_rtp_delta_info(&[]);
                self.p(format!("aggdelta.empty {}", delta(agg.as_ref())));
            }
            "fn.recdelta" => {
                let (ds, dr) = self.send(u(1)).delta_info_sender(u(2) as u32);
                let (ds_rtx, dr_rtx) = self.send(u(3)).delta_info_sender(u(4) as u32);
                let rs = reconcile_rtp_delta_info_with_rtx(ds, ds_rtx.as_ref());
                self.p(format!("recdelta.s {}", delta(rs.as_ref())));
                let rr = reconcile_rtp_delta_info_with_rtx(dr, dr_rtx.as_ref());
                self.p(format!("recdelta.r {}", delta(rr.as_ref())));
            }
            "fn.recproto" => {
                let p = reconcile_rtp_stats_with_rtx(
                    self.send(u(1)).to_proto(),
                    self.send(u(2)).to_proto().as_ref(),
                );
                match p {
                    None => self.p("recproto nil".to_string()),
                    Some(p) => self.p(format!(
                        "recproto packets={} lost={} losspct={} dup={} bdup={} hdup={} pad={} bpad={} hpad={}",
                        p.packets,
                        p.packets_lost,
                        f32s(p.packet_loss_percentage),
                        p.packets_duplicate,
                        p.bytes_duplicate,
                        p.header_bytes_duplicate,
                        p.packets_padding,
                        p.bytes_padding,
                        p.header_bytes_padding,
                    )),
                }
            }
            "fn.propdelay" => {
                let d = rtcp_sender_report_propagation_delay(&livekit::RtcpSenderReportState {
                    ntp_timestamp: u(1),
                    at_adjusted: s(2),
                    ..Default::default()
                });
                self.p(format!("propdelay {}", d.nanoseconds()));
            }

            other => panic!("unknown operation {other:?} in {line:?}"),
        }
    }
}

// What the port answers to a script, in the lines the Go test writes for the original.
fn replay(ops: &str) -> String {
    let mut r = Replay::default();
    for line in ops.lines() {
        r.n += 1;
        r.exec(line);
    }
    r.out
}

#[track_caller]
fn assert_same_lines(name: &str, expected: &str, actual: &str) {
    let expected: Vec<&str> = expected.lines().collect();
    let actual: Vec<&str> = actual.lines().collect();
    let mut differing = Vec::new();
    for i in 0..expected.len().max(actual.len()) {
        let e = expected.get(i).copied().unwrap_or("<no line>");
        let a = actual.get(i).copied().unwrap_or("<no line>");
        if e != a {
            differing.push(format!("line {}:\n  original: {e}\n  port:     {a}", i + 1));
        }
    }
    assert!(
        differing.is_empty(),
        "{name}: {} of {} lines differ, the first of them:\n{}",
        differing.len(),
        expected.len(),
        differing[..differing.len().min(5)].join("\n")
    );
}

#[test]
fn the_port_answers_a_pinned_script_as_the_original() {
    let ops = include_str!("testdata/reference.ops");
    let expected = include_str!("testdata/reference.out");
    assert_eq!(62, ops.lines().count());
    assert_eq!(50, expected.lines().count());
    assert_same_lines("testdata/reference", expected, &replay(ops));
}

// The buffer, the down track and the forwarder share these objects between threads.
#[test]
fn the_statistics_objects_can_be_shared_between_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RtpStatsReceiver>();
    assert_send_sync::<RtpStatsSender>();
    assert_send_sync::<RtpStatsReceiverLite>();
    assert_send_sync::<RtpStatsSenderLite>();
}

#[test]
fn the_port_answers_the_scripts_of_musivum_rtpstats_reference_as_the_original() {
    let Ok(dir) = std::env::var("MUSIVUM_RTPSTATS_REFERENCE") else {
        return;
    };
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if let Some(name) = name.strip_suffix(".ops") {
            names.push(name.to_string());
        }
    }
    names.sort();
    assert!(!names.is_empty(), "no scripts in {dir}");
    for name in names {
        let ops = std::fs::read_to_string(format!("{dir}/{name}.ops")).unwrap();
        let expected = std::fs::read_to_string(format!("{dir}/{name}.go.out")).unwrap();
        let actual = replay(&ops);
        std::fs::write(format!("{dir}/{name}.rust.out"), &actual).unwrap();
        println!(
            "{name}: {} operations, {} lines of the original compared",
            ops.lines().count(),
            expected.lines().count()
        );
        assert_same_lines(&name, &expected, &actual);

        // and once more with every line of the log formatted
        let mut logged = String::new();
        let log = crate::testutils::capture_log(tracing::Level::DEBUG, || logged = replay(&ops));
        println!("{name}: {} lines logged", log.lines().count());
        assert_same_lines(&name, &expected, &logged);
    }
}

// With the log captured at the most verbose level every line the code writes is formatted,
// the statistics objects in them included: the answers stay the same, and the messages are
// the original's. (What the original logs was not compared: its logger writes nothing in a
// test.)
#[test]
fn the_port_logs_the_pinned_script_without_changing_its_answers() {
    let ops = include_str!("testdata/reference.ops");
    let expected = include_str!("testdata/reference.out");
    let mut actual = String::new();
    let log = crate::testutils::capture_log(tracing::Level::DEBUG, || actual = replay(ops));
    assert_same_lines("testdata/reference", expected, &actual);

    let lines: Vec<&str> = log.lines().collect();
    let starts = [
        "DEBUG musivum: rtp receiver stream start rtpStats=RtpStatsReceiver { startTime: ",
        " WARN musivum: dropping packet, pre-start timestamp resSN=WrapAroundUpdateResult { is_unhandled: true, ",
        " WARN musivum: dropping packet, old sequence number resSN=WrapAroundUpdateResult { is_unhandled: false, ",
        "DEBUG musivum: propagation delay step change currentSenderReport=RtcpSenderReportState { RtpTimestamp: 4294963600, RtpTimestampExt: 4294963600, NtpTimestamp: 1700000000.070000000, At: 1700000000.100000000, AtAdjusted: 0, Packets: 0, Octets: 0 } rtpStats=RtpStatsReceiver { ",
        " INFO musivum: received sender report, anachronous, dropping current=RtcpSenderReportState { RtpTimestamp: 4294962700, ",
        "DEBUG musivum: rtp sender stream start rtpStats=RtpStatsSender { startTime: ",
        " INFO musivum: adjusting start sequence number currSN=69998 gapSN=-7 currTS=5999997000 gapTS=-6000 packetTime=1700000000.039000000 timeSinceHighest=4ms marker=false hdrSize=12 payloadSize=600 paddingSize=0 rtpStats=RtpStatsSender { ",
        " INFO musivum: adjusting start timestamp currSN=69998 gapSN=-7 currTS=5999997000 gapTS=-6000 packetTime=1700000000.039000000 timeSinceHighest=4ms marker=false hdrSize=12 payloadSize=600 paddingSize=0 rtpStats=RtpStatsSender { ",
        "DEBUG musivum: rtp receiver lite stream start rtpStats=RtpStatsReceiverLite { startTime: ",
        "DEBUG musivum: rtp sender lite stream start rtpStats=RtpStatsSenderLite { startTime: ",
    ];
    assert_eq!(starts.len(), lines.len(), "{log}");
    for (start, line) in starts.iter().zip(&lines) {
        assert!(
            line.starts_with(start),
            "{line:?} does not start with {start:?}"
        );
    }
    // the second of them, after what it starts with: the fields of `receiverUpdateLoggingFields`
    // in the order the original writes them, and then the statistics object
    assert!(
        lines[1].contains(
            " gapSN=59996 resTS=WrapAroundUpdateResult { is_unhandled: true, is_restart: false, pre_extended_start: 0, pre_extended_highest: 4294969000, extended_val: 8584967296 } gapTS=4289998296 snRolloverCount=0 expectedTSJump=90 tsRolloverCount=-1 packetTime=1700000000.102000000 timeSinceHighest=1ms sequenceNumber=60000 timestamp=4290000000 marker=false hdrSize=12 payloadSize=500 paddingSize=0 rtpStats=RtpStatsReceiver { "
        ),
        "{}",
        lines[1]
    );
    // and the end of a statistics object that a line writes
    assert!(
        lines[1].contains(
            " extStartSN: 65533, extHighestSN: 65540, extStartTS: 4294960000, extHighestTS: 4294969000, propagationDelayEstimator: OwdEstimator { "
        ),
        "{}",
        lines[1]
    );
    assert!(
        lines[1].contains(
            " clockSkewCount: 0, clockSkewMediaPathCount: 0, outOfOrderSenderReportCount: 0, timeReversedCount: 0, packetsDroppedPreStartTimestamp: 1, packetsDroppedOldTimestamp: 0, packetsDroppedPreStartSequenceNumber: 0, packetsDroppedOldSequenceNumber: 0, restartPackets: [] }"
        ),
        "{}",
        lines[1]
    );
}
