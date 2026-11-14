//! New for musivum: the original's tests go no further than the score. Here is what
//! `get_stat` hands to the callback for the streams a provider gives, as a receiver and as a
//! sender, and the worker that calls it.
//!
//! What is expected of `get_stat` is what the original reports at the same steps:
//! `testdata/reference_output_test.go` took them and printed the lines that `EXPECTED` holds,
//! and says how it is run. A provider is given new streams before every step: the original
//! discounts the jitter in the provider's own remote view, the port in the copy of it that
//! it was handed.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::*;
use crate::protocol::logger;
use crate::testutils;

#[derive(Default)]
struct ReceiverProvider {
    streams: Mutex<HashMap<u32, buffer::StreamStatsWithLayers>>,
}

impl ReceiverProvider {
    fn set_streams(&self, streams: Vec<(u32, buffer::StreamStatsWithLayers)>) {
        *self.streams.lock() = streams.into_iter().collect();
    }
}

impl ConnectionStatsReceiverProvider for ReceiverProvider {
    fn get_delta_stats(&self) -> HashMap<u32, buffer::StreamStatsWithLayers> {
        self.streams.lock().clone()
    }

    fn get_last_sender_report_time(&self) -> Time {
        Time::ZERO
    }
}

#[derive(Default)]
struct SenderProvider {
    streams: Mutex<HashMap<u32, buffer::StreamStatsWithLayers>>,
    last_rr: Mutex<Time>,
    packets_sent: Mutex<u64>,
}

impl SenderProvider {
    fn set_streams(&self, streams: Vec<(u32, buffer::StreamStatsWithLayers)>) {
        *self.streams.lock() = streams.into_iter().collect();
    }
}

impl ConnectionStatsSenderProvider for SenderProvider {
    fn get_delta_stats_sender(&self) -> HashMap<u32, buffer::StreamStatsWithLayers> {
        self.streams.lock().clone()
    }

    fn get_primary_stream_last_receiver_report_time(&self) -> Time {
        *self.last_rr.lock()
    }

    fn get_primary_stream_packets_sent(&self) -> u64 {
        *self.packets_sent.lock()
    }
}

fn delta(base: Time, k: u32) -> rtpstats::RtpDeltaInfo {
    rtpstats::RtpDeltaInfo {
        start_time: base.add(60 * time::SECOND),
        end_time: base.add(65 * time::SECOND),
        packets: 250 + k,
        bytes: 30000 + k as u64 * 100,
        header_bytes: 3000 + k as u64,
        packets_duplicate: 3 + k,
        bytes_duplicate: 360 + k as u64,
        header_bytes_duplicate: 36,
        packets_padding: 4 + k,
        bytes_padding: 400 + k as u64,
        header_bytes_padding: 48,
        packets_lost: 10 + k,
        packets_missing: 2,
        packets_out_of_order: 5 + k,
        frames: 20 + k,
        rtt_max: 120 + k,
        jitter_max: 25000.7 + k as f64,
        nacks: 7 + k,
        nack_repeated: 1,
        plis: 2 + k,
        firs: 1 + k,
        ..Default::default()
    }
}

// a stream as the original's literals make it: statistics, layers, a remote view
fn stream(
    rtp_stats: Option<rtpstats::RtpDeltaInfo>,
    layers: Vec<(i32, Option<rtpstats::RtpDeltaInfo>)>,
    rtp_stats_remote_view: Option<rtpstats::RtpDeltaInfo>,
) -> buffer::StreamStatsWithLayers {
    buffer::StreamStatsWithLayers {
        rtp_stats: rtp_stats.map(Arc::new),
        layers: layers
            .into_iter()
            .map(|(layer, layer_stats)| (layer, layer_stats.map(Arc::new)))
            .collect(),
        rtp_stats_remote_view: rtp_stats_remote_view.map(Arc::new),
    }
}

fn format_stat(base: Time, stat: Option<&livekit::AnalyticsStat>) -> String {
    let Some(stat) = stat else {
        return "no stat".to_string();
    };
    let mut streams: Vec<&livekit::AnalyticsStream> = stat.streams.iter().collect();
    streams.sort_by_key(|as_| as_.ssrc);
    let mut s = format!("score={:08x} mime={}", stat.score.to_bits(), stat.mime);
    for as_ in streams {
        let since_base = |ts: &Option<musivum_protocol::Timestamp>| {
            time::timestamppb_as_time(ts.as_ref().expect("a time"))
                .sub(base)
                .milliseconds()
        };
        write!(
            s,
            " [ssrc={} start={} end={} pp={} pb={} rp={} rb={} padp={} padb={} lost={} ooo={} \
             frames={} rtt={} jitter={} nacks={} plis={} firs={} layers=",
            as_.ssrc,
            since_base(&as_.start_time),
            since_base(&as_.end_time),
            as_.primary_packets,
            as_.primary_bytes,
            as_.retransmit_packets,
            as_.retransmit_bytes,
            as_.padding_packets,
            as_.padding_bytes,
            as_.packets_lost,
            as_.packets_out_of_order,
            as_.frames,
            as_.rtt,
            as_.jitter,
            as_.nacks,
            as_.plis,
            as_.firs,
        )
        .expect("written");
        let mut layers: Vec<&livekit::AnalyticsVideoLayer> = as_.video_layers.iter().collect();
        layers.sort_by_key(|l| l.layer);
        for l in layers {
            write!(s, "({} {} {} {})", l.layer, l.packets, l.bytes, l.frames).expect("written");
        }
        s.push(']');
    }
    s
}

// Statistics with a callback that keeps what it is given.
struct Session {
    cs: Arc<ConnectionStats>,
    last: Arc<Mutex<Option<livekit::AnalyticsStat>>>,
    calls: Arc<AtomicUsize>,
    base: Time,
}

impl Session {
    fn new(mut params: ConnectionStatsParams, mime_type: mime::MimeType, base: Time) -> Session {
        params.logger = logger::get_logger();
        let cs = ConnectionStats::new(params);
        let last = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        cs.on_stats_update({
            let this = Arc::downgrade(&cs);
            let last = last.clone();
            let calls = calls.clone();
            move |cs: &ConnectionStats, stat: &livekit::AnalyticsStat| {
                assert!(
                    std::ptr::eq(cs, this.as_ptr()),
                    "not the statistics the callback was given to"
                );
                *last.lock() = Some(stat.clone());
                calls.fetch_add(1, Ordering::SeqCst);
            }
        });
        cs.start_at(mime_type, false, base.add(-5 * time::SECOND));
        Session {
            cs,
            last,
            calls,
            base,
        }
    }

    async fn step(&self, name: &str) -> String {
        *self.last.lock() = None;
        time::sleep(2 * time::MILLISECOND).await;
        self.cs.get_stat();
        let (mos, quality) = self.cs.get_score_and_quality();
        format!(
            "{name}: calls={} mos={:08x} quality={} {}",
            self.calls.load(Ordering::SeqCst),
            mos.to_bits(),
            quality as i32,
            format_stat(self.base, self.last.lock().as_ref()),
        )
    }
}

const EXPECTED: &[&str] = &[
    "receiver, no streams: calls=0 mos=40900000 quality=2 no stat",
    "receiver, audio: calls=1 mos=407ba3af quality=1 score=407ba3af mime=audio/opus [ssrc=1 start=60000 end=65000 pp=250 pb=30000 rp=3 rb=360 padp=4 padb=400 lost=10 ooo=5 frames=20 rtt=120 jitter=25000 nacks=7 plis=2 firs=1 layers=]",
    "receiver, audio, two streams: calls=2 mos=4065283a quality=1 score=4065283a mime=audio/opus [ssrc=1 start=60000 end=65000 pp=251 pb=30100 rp=4 rb=361 padp=5 padb=401 lost=11 ooo=6 frames=21 rtt=121 jitter=25001 nacks=8 plis=3 firs=2 layers=] [ssrc=2 start=60000 end=65000 pp=252 pb=30200 rp=5 rb=362 padp=6 padb=402 lost=12 ooo=7 frames=22 rtt=122 jitter=25002 nacks=9 plis=4 firs=3 layers=]",
    "receiver, streams without statistics: calls=2 mos=4065283a quality=1 no stat",
    "receiver, simulcast: calls=1 mos=404e6b00 quality=1 score=404e6b00 mime=video/VP8 [ssrc=10 start=60000 end=65000 pp=253 pb=30300 rp=6 rb=363 padp=7 padb=403 lost=13 ooo=8 frames=23 rtt=123 jitter=25003 nacks=10 plis=5 firs=4 layers=(0 266 31066 23)] [ssrc=11 start=60000 end=65000 pp=254 pb=30400 rp=7 rb=364 padp=8 padb=404 lost=14 ooo=9 frames=24 rtt=124 jitter=25004 nacks=11 plis=6 firs=5 layers=(1 269 31168 24)] [ssrc=13 start=60000 end=65000 pp=256 pb=30600 rp=9 rb=366 padp=10 padb=406 lost=16 ooo=11 frames=26 rtt=126 jitter=25006 nacks=13 plis=8 firs=7 layers=]",
    "receiver, layers of one stream: calls=2 mos=401d2371 quality=1 score=401d2371 mime=video/VP8 [ssrc=20 start=60000 end=65000 pp=257 pb=30700 rp=10 rb=367 padp=11 padb=407 lost=17 ooo=12 frames=27 rtt=127 jitter=25007 nacks=14 plis=9 firs=8 layers=(0 281 31576 28)(1 284 31678 29)(2 287 31780 30)]",
    "receiver, one stream with one layer: calls=3 mos=401366af quality=1 score=401366af mime=video/VP8 [ssrc=20 start=60000 end=65000 pp=257 pb=30700 rp=10 rb=367 padp=11 padb=407 lost=17 ooo=12 frames=27 rtt=127 jitter=25007 nacks=14 plis=9 firs=8 layers=]",
    "receiver, codec updated to audio: calls=4 mos=40356259 quality=1 score=40356259 mime=audio/opus [ssrc=20 start=60000 end=65000 pp=257 pb=30700 rp=10 rb=367 padp=11 padb=407 lost=17 ooo=12 frames=27 rtt=127 jitter=25007 nacks=14 plis=9 firs=8 layers=]",
    "sender, not streaming: calls=0 mos=40900000 quality=2 no stat",
    "sender, streaming, no report yet: calls=0 mos=40900000 quality=2 no stat",
    "sender, report from before the start: calls=1 mos=40900000 quality=2 score=40900000 mime=video/VP8 [ssrc=5 start=60000 end=65000 pp=261 pb=31100 rp=14 rb=371 padp=15 padb=411 lost=20 ooo=16 frames=31 rtt=132 jitter=30000 nacks=18 plis=13 firs=12 layers=]",
    "sender, remote view: calls=2 mos=408bdf80 quality=2 score=408bdf80 mime=video/VP8 [ssrc=5 start=60000 end=65000 pp=261 pb=31100 rp=14 rb=371 padp=15 padb=411 lost=20 ooo=16 frames=31 rtt=132 jitter=4988 nacks=18 plis=13 firs=12 layers=]",
    "sender, remote view with less jitter, and more missing than lost: calls=3 mos=408e0788 quality=2 score=408e0788 mime=video/VP8 [ssrc=5 start=60000 end=65000 pp=263 pb=31300 rp=16 rb=373 padp=17 padb=413 lost=0 ooo=18 frames=33 rtt=134 jitter=0 nacks=20 plis=15 firs=14 layers=]",
    "sender, no remote view: calls=4 mos=408e0788 quality=2 score=408e0788 mime=video/VP8 [ssrc=5 start=60000 end=65000 pp=265 pb=31500 rp=18 rb=375 padp=19 padb=415 lost=25 ooo=20 frames=35 rtt=0 jitter=0 nacks=22 plis=17 firs=16 layers=]",
    "sender, stopped streaming: calls=4 mos=408e0788 quality=2 no stat",
    "sender, two streams: calls=5 mos=3fcdf3b6 quality=0 score=3fcdf3b6 mime=video/VP8 [ssrc=5 start=60000 end=65000 pp=266 pb=31600 rp=19 rb=376 padp=20 padb=416 lost=58 ooo=21 frames=36 rtt=137 jitter=64983 nacks=23 plis=18 firs=17 layers=(0 305 32392 36)] [ssrc=6 start=60000 end=65000 pp=268 pb=31800 rp=21 rb=378 padp=22 padb=418 lost=43 ooo=23 frames=38 rtt=139 jitter=44982 nacks=25 plis=20 firs=19 layers=(0 311 32596 38)]",
    "sender, closed: mos=3fcdf3b6 quality=0",
    "no provider: calls=0 mos=40900000 quality=2 no stat",
];

#[tokio::test(flavor = "multi_thread")]
async fn get_stat_reports_what_the_original_reports() {
    let mut lines = Vec::new();
    // to the millisecond, as the times are compared after a round trip through a timestamp
    let base = Time::from_unix_nano(time::now().unix_milli() * 1_000_000);

    // a receiver of audio
    let trp = Arc::new(ReceiverProvider::default());
    let receiver_provider = || {
        let trp: Weak<ReceiverProvider> = Arc::downgrade(&trp);
        Some(trp as Weak<dyn ConnectionStatsReceiverProvider>)
    };
    let r = Session::new(
        ConnectionStatsParams {
            receiver_provider: receiver_provider(),
            include_rtt: true,
            include_jitter: true,
            ..Default::default()
        },
        mime::MimeType::Opus,
        base,
    );
    lines.push(r.step("receiver, no streams").await);
    trp.set_streams(vec![(1, stream(Some(delta(base, 0)), vec![], None))]);
    lines.push(r.step("receiver, audio").await);
    trp.set_streams(vec![
        (
            1,
            stream(Some(delta(base, 1)), vec![(0, Some(delta(base, 1)))], None),
        ),
        (
            2,
            stream(Some(delta(base, 2)), vec![(0, Some(delta(base, 2)))], None),
        ),
    ]);
    lines.push(r.step("receiver, audio, two streams").await);
    trp.set_streams(vec![
        (1, stream(None, vec![], None)),
        (2, stream(None, vec![(0, Some(delta(base, 2)))], None)),
    ]);
    lines.push(r.step("receiver, streams without statistics").await);

    // a receiver of video
    let rv = Session::new(
        ConnectionStatsParams {
            receiver_provider: receiver_provider(),
            ..Default::default()
        },
        mime::MimeType::Vp8,
        base,
    );
    let mut no_frames = delta(base, 5);
    no_frames.frames = 0;
    trp.set_streams(vec![
        (
            10,
            stream(Some(delta(base, 3)), vec![(0, Some(delta(base, 3)))], None),
        ),
        (
            11,
            stream(Some(delta(base, 4)), vec![(1, Some(delta(base, 4)))], None),
        ),
        (12, stream(None, vec![(2, Some(delta(base, 5)))], None)),
        (
            13,
            stream(
                Some(delta(base, 6)),
                vec![(2, None), (1, Some(no_frames))],
                None,
            ),
        ),
    ]);
    lines.push(rv.step("receiver, simulcast").await);
    let layers_of_one_stream = || {
        vec![(
            20,
            stream(
                Some(delta(base, 7)),
                vec![
                    (0, Some(delta(base, 8))),
                    (1, Some(delta(base, 9))),
                    (2, Some(delta(base, 10))),
                ],
                None,
            ),
        )]
    };
    trp.set_streams(layers_of_one_stream());
    lines.push(rv.step("receiver, layers of one stream").await);
    trp.set_streams(vec![(
        20,
        stream(Some(delta(base, 7)), vec![(0, Some(delta(base, 8)))], None),
    )]);
    lines.push(rv.step("receiver, one stream with one layer").await);
    rv.cs.update_codec(mime::MimeType::Opus, true);
    trp.set_streams(layers_of_one_stream());
    lines.push(rv.step("receiver, codec updated to audio").await);

    // a sender of video, which also has a receiver provider that is not to be asked
    let tsp = Arc::new(SenderProvider::default());
    let sender_provider: Weak<SenderProvider> = Arc::downgrade(&tsp);
    let s = Session::new(
        ConnectionStatsParams {
            sender_provider: Some(sender_provider),
            receiver_provider: receiver_provider(),
            ..Default::default()
        },
        mime::MimeType::Vp8,
        base,
    );
    lines.push(s.step("sender, not streaming").await);
    *tsp.packets_sent.lock() = 100;
    lines.push(s.step("sender, streaming, no report yet").await);

    *tsp.packets_sent.lock() = 200;
    *tsp.last_rr.lock() = base.add(-time::HOUR);
    let mut remote = delta(base, 12);
    remote.jitter_max = 30000.5;
    tsp.set_streams(vec![(
        5,
        stream(
            Some(delta(base, 11)),
            vec![(0, Some(delta(base, 11)))],
            Some(remote),
        ),
    )]);
    lines.push(s.step("sender, report from before the start").await);

    *tsp.packets_sent.lock() = 300;
    *tsp.last_rr.lock() = base.add(time::HOUR);
    let mut remote = delta(base, 12);
    remote.jitter_max = 30000.5;
    tsp.set_streams(vec![(
        5,
        stream(
            Some(delta(base, 11)),
            vec![(0, Some(delta(base, 11)))],
            Some(remote),
        ),
    )]);
    lines.push(s.step("sender, remote view").await);

    *tsp.packets_sent.lock() = 400;
    let mut remote = delta(base, 14);
    remote.jitter_max = 100.5;
    remote.packets_lost = 3;
    remote.packets_missing = 8;
    tsp.set_streams(vec![(
        5,
        stream(
            Some(delta(base, 13)),
            vec![(0, Some(delta(base, 13)))],
            Some(remote),
        ),
    )]);
    lines.push(
        s.step("sender, remote view with less jitter, and more missing than lost")
            .await,
    );

    *tsp.packets_sent.lock() = 500;
    tsp.set_streams(vec![(
        5,
        stream(
            Some(delta(base, 15)),
            vec![(0, Some(delta(base, 15)))],
            None,
        ),
    )]);
    lines.push(s.step("sender, no remote view").await);

    // no packets sent since
    lines.push(s.step("sender, stopped streaming").await);

    *tsp.packets_sent.lock() = 600;
    let mut remote = delta(base, 17);
    remote.packets_lost = 60;
    remote.jitter_max = 90000.25;
    let mut remote2 = delta(base, 19);
    remote2.packets_lost = 45;
    remote2.jitter_max = 70000.75;
    tsp.set_streams(vec![
        (
            5,
            stream(
                Some(delta(base, 16)),
                vec![(0, Some(delta(base, 16)))],
                Some(remote),
            ),
        ),
        (
            6,
            stream(
                Some(delta(base, 18)),
                vec![(0, Some(delta(base, 18)))],
                Some(remote2),
            ),
        ),
    ]);
    lines.push(s.step("sender, two streams").await);

    // closed: the calls on the scorer are dropped, the score stays
    s.cs.close();
    s.cs.update_pause_at(true, base);
    s.cs.update_mute_at(true, base);
    s.cs.update_layer_mute_at(true, base);
    let (mos, quality) = s.cs.get_score_and_quality();
    lines.push(format!(
        "sender, closed: mos={:08x} quality={}",
        mos.to_bits(),
        quality as i32
    ));

    // neither provider
    let n = Session::new(ConnectionStatsParams::default(), mime::MimeType::Vp8, base);
    lines.push(n.step("no provider").await);

    assert_eq!(EXPECTED.len(), lines.len());
    for (expected, line) in EXPECTED.iter().zip(&lines) {
        assert_eq!(expected, line);
    }

    // what the original cannot have: a provider that is gone is no provider, and a sender
    // whose provider is gone does not turn to its receiver provider
    trp.set_streams(vec![(1, stream(Some(delta(base, 0)), vec![], None))]);
    drop(tsp);
    assert_eq!(
        "calls=5 mos=3fcdf3b6 quality=0 no stat",
        s.step("").await.trim_start_matches(": ")
    );
    drop(trp);
    assert_eq!(
        "calls=2 mos=4065283a quality=1 no stat",
        r.step("").await.trim_start_matches(": ")
    );
}

// New for musivum: the worker asks on its interval, reports once more when the statistics
// are closed, and then no more.
#[tokio::test(flavor = "multi_thread")]
async fn the_worker_reports_until_closed() {
    let base = time::now();
    let trp = Arc::new(ReceiverProvider::default());
    trp.set_streams(vec![(1, stream(Some(delta(base, 0)), vec![], None))]);
    let receiver_provider = || {
        let trp: Weak<ReceiverProvider> = Arc::downgrade(&trp);
        Some(trp as Weak<dyn ConnectionStatsReceiverProvider>)
    };
    let counted = |cs: &ConnectionStats| {
        let calls = Arc::new(AtomicUsize::new(0));
        let mimes = Arc::new(Mutex::new(Vec::new()));
        cs.on_stats_update({
            let calls = calls.clone();
            let mimes = mimes.clone();
            move |_: &ConnectionStats, stat: &livekit::AnalyticsStat| {
                mimes.lock().push(stat.mime.clone());
                calls.fetch_add(1, Ordering::SeqCst);
            }
        });
        (calls, mimes)
    };
    let wait_for = std::time::Duration::from_secs(5);
    let tick = std::time::Duration::from_millis(5);

    let cs = ConnectionStats::new(ConnectionStatsParams {
        update_interval: 20 * time::MILLISECOND,
        receiver_provider: receiver_provider(),
        ..Default::default()
    });
    let (calls, mimes) = counted(&cs);
    time::sleep(60 * time::MILLISECOND).await;
    assert_eq!(0, calls.load(Ordering::SeqCst), "not started yet");
    cs.start(mime::MimeType::Opus, false);
    // a second start does nothing
    cs.start(mime::MimeType::Vp8, false);
    testutils::eventually_async(|| calls.load(Ordering::SeqCst) >= 3, wait_for, tick).await;
    assert!(mimes.lock().iter().all(|mime| mime == "audio/opus"));

    cs.close();
    // the report of the worker as it ends may still be on its way
    time::sleep(100 * time::MILLISECOND).await;
    let reported = calls.load(Ordering::SeqCst);
    time::sleep(100 * time::MILLISECOND).await;
    assert_eq!(reported, calls.load(Ordering::SeqCst));

    // closed long before the first tick: the one report is that of the worker as it ends
    let cs = ConnectionStats::new(ConnectionStatsParams {
        update_interval: time::HOUR,
        receiver_provider: receiver_provider(),
        ..Default::default()
    });
    let (calls, _) = counted(&cs);
    cs.start(mime::MimeType::Vp8, false);
    time::sleep(60 * time::MILLISECOND).await;
    assert_eq!(0, calls.load(Ordering::SeqCst));
    cs.close();
    testutils::eventually_async(|| calls.load(Ordering::SeqCst) == 1, wait_for, tick).await;
    time::sleep(60 * time::MILLISECOND).await;
    assert_eq!(1, calls.load(Ordering::SeqCst));

    // dropped without being closed: the worker, which does not hold the statistics, ends
    // at its next tick and reports nothing
    let cs = ConnectionStats::new(ConnectionStatsParams {
        update_interval: 20 * time::MILLISECOND,
        receiver_provider: receiver_provider(),
        ..Default::default()
    });
    let (calls, _) = counted(&cs);
    cs.start(mime::MimeType::Opus, false);
    testutils::eventually_async(|| calls.load(Ordering::SeqCst) >= 1, wait_for, tick).await;
    let this = Arc::downgrade(&cs);
    drop(cs);
    testutils::eventually_async(|| this.upgrade().is_none(), wait_for, tick).await;
    time::sleep(60 * time::MILLISECOND).await;
    let reported = calls.load(Ordering::SeqCst);
    time::sleep(100 * time::MILLISECOND).await;
    assert_eq!(reported, calls.load(Ordering::SeqCst));
}
