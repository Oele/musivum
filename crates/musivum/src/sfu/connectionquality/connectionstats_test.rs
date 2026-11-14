// Ported from LiveKit, pkg/sfu/connectionquality/connectionstats_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the streams a test hands to the provider, written as
// map literals in the original, are made with `stream_stats`; the cases of
// `TestPacketRateMode`, a map in the original, are run in the order they are written in.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::super::scorer::C_PPS_QUANTIZATION;
use super::*;
use crate::protocol::logger;
use crate::testutils;

// -----------------------------------------------

#[derive(Default)]
struct TestReceiverProvider {
    streams: Mutex<HashMap<u32, buffer::StreamStatsWithLayers>>,
    last_sender_report_time: Mutex<Time>,
}

fn new_test_receiver_provider() -> Arc<TestReceiverProvider> {
    Arc::new(TestReceiverProvider::default())
}

impl TestReceiverProvider {
    fn set_streams(&self, streams: HashMap<u32, buffer::StreamStatsWithLayers>) {
        *self.streams.lock() = streams;
    }

    fn set_last_sender_report_time(&self, at: Time) {
        *self.last_sender_report_time.lock() = at;
    }
}

impl ConnectionStatsReceiverProvider for TestReceiverProvider {
    fn get_delta_stats(&self) -> HashMap<u32, buffer::StreamStatsWithLayers> {
        self.streams.lock().clone()
    }

    fn get_last_sender_report_time(&self) -> Time {
        *self.last_sender_report_time.lock()
    }
}

// what `ConnectionStatsParams` takes
fn receiver_provider(
    trp: &Arc<TestReceiverProvider>,
) -> Option<Weak<dyn ConnectionStatsReceiverProvider>> {
    let trp: Weak<TestReceiverProvider> = Arc::downgrade(trp);
    Some(trp)
}

// `{RTPStats: &rtpstats.RTPDeltaInfo{...}}` of the original
fn stream_stats(rtp_stats: rtpstats::RtpDeltaInfo) -> buffer::StreamStatsWithLayers {
    buffer::StreamStatsWithLayers {
        rtp_stats: Some(Arc::new(rtp_stats)),
        ..Default::default()
    }
}

// -----------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_quality() {
    let trp = new_test_receiver_provider();
    // "quality scorer operation"
    {
        let cs = ConnectionStats::new(ConnectionStatsParams {
            include_rtt: true,
            include_jitter: true,
            enable_bitrate_score: true,
            receiver_provider: receiver_provider(&trp),
            logger: logger::get_logger(),
            ..Default::default()
        });

        let duration = 5 * time::SECOND;
        let mut now = time::now();
        cs.start_at(mime::MimeType::Opus, false, now.add(-duration));
        cs.update_mute_at(false, now.add(-1 * time::SECOND));

        // no data and not enough unmute time should return default state which is EXCELLENT quality
        cs.update_score_at(now);
        let (mut mos, mut quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // best conditions (no loss, jitter/rtt = 0) - quality should stay EXCELLENT
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // introduce loss and the score should drop - 12% loss for Opus -> POOR
        now = now.add(duration);
        trp.set_streams(HashMap::from([
            (
                1,
                stream_stats(rtpstats::RtpDeltaInfo {
                    start_time: now,
                    end_time: now.add(duration),
                    packets: 120,
                    packets_lost: 30,
                    ..Default::default()
                }),
            ),
            (
                2,
                stream_stats(rtpstats::RtpDeltaInfo {
                    start_time: now,
                    end_time: now.add(duration),
                    packets: 130,
                    packets_lost: 0,
                    ..Default::default()
                }),
            ),
        ]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(2.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Poor, quality);

        // should climb to GOOD quality in one iteration if the conditions improve.
        // although significant loss (12%) in the previous window, lowest score is
        // bound so that climbing back does not take too long even under excellent conditions.
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);

        // should stay at GOOD if conditions continue to be good
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);

        // should climb up to EXCELLENT if conditions continue to be good
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // introduce loss and the score should drop - 5% loss for Opus -> GOOD
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                packets_lost: 13,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);

        // should stay at GOOD quality for another iteration even if the conditions improve
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);

        // should climb up to EXCELLENT if conditions continue to be good
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // mute when quality is POOR should return quality to EXCELLENT
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                packets_lost: 30,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(2.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Poor, quality);

        now = now.add(duration);
        cs.update_mute_at(true, now.add(1 * time::SECOND));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // unmute at specific time to ensure next window does not satisfy the unmute time threshold.
        // that means even if the next update has 0 packets, it should hold state and stay at EXCELLENT quality
        cs.update_mute_at(false, now.add(3 * time::SECOND));

        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 0,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // next update with no packets,
        // but last RTCP is not set, should knock quality down to POOR
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 0,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(2.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Poor, quality);

        // another dry spell, but last RTCP is not stale, should keep quality at POOR
        now = now.add(duration);
        trp.set_last_sender_report_time(now.add(time::SECOND));
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 0,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(2.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Poor, quality);

        // yet another dry spell, but last RTCP is stale, should knock down quality at LOST
        now = now.add(duration);
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 0,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(1.3f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Lost, quality);

        // mute when LOST should not bump up score/quality
        now = now.add(duration);
        cs.update_mute_at(true, now.add(1 * time::SECOND));
        (mos, quality) = cs.get_score_and_quality();
        assert!(1.3f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Lost, quality);

        // unmute and send packets to bring quality back up
        now = now.add(duration);
        cs.update_mute_at(false, now.add(2 * time::SECOND));
        for _ in 0..3 {
            trp.set_streams(HashMap::from([(
                1,
                stream_stats(rtpstats::RtpDeltaInfo {
                    start_time: now,
                    end_time: now.add(duration),
                    packets: 250,
                    packets_lost: 0,
                    ..Default::default()
                }),
            )]));
            cs.update_score_at(now.add(duration));
            now = now.add(duration);
        }
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // with lesser number of packet (simulating DTX).
        // even higher loss (like 10%) should not knock down quality due to quadratic weighting of packet loss ratio
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 50,
                packets_lost: 5,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // mute/unmute to bring quality back up
        now = now.add(duration);
        cs.update_mute_at(true, now.add(1 * time::SECOND));
        cs.update_mute_at(false, now.add(2 * time::SECOND));

        // RTT and jitter can knock quality down.
        // at 2% loss, quality should stay at EXCELLENT purely based on loss, but with added RTT/jitter, should drop to GOOD
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                packets_lost: 5,
                rtt_max: 400,
                jitter_max: 30000.0,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);

        // mute/unmute to bring quality back up
        now = now.add(duration);
        cs.update_mute_at(true, now.add(1 * time::SECOND));
        cs.update_mute_at(false, now.add(2 * time::SECOND));

        // bitrate based calculation can drop quality even if there is no loss
        cs.add_bitrate_transition_at(1_000_000, now);
        cs.add_bitrate_transition_at(2_000_000, now.add(2 * time::SECOND));

        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                bytes: 8_000_000 / 8 / 5,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);

        // test layer mute via UpdateLayerMute API
        cs.add_bitrate_transition_at(1_000_000, now);
        cs.add_bitrate_transition_at(2_000_000, now.add(2 * time::SECOND));

        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                bytes: 8_000_000 / 8 / 5,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);

        now = now.add(duration);
        cs.update_layer_mute_at(true, now);
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // unmute layer
        cs.update_layer_mute_at(false, now.add(2 * time::SECOND));

        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                bytes: 8_000_000 / 8 / 5,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);

        // pause
        now = now.add(duration);
        cs.update_pause_at(true, now);
        (mos, quality) = cs.get_score_and_quality();
        assert!(2.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Poor, quality);

        // resume
        cs.update_pause_at(false, now.add(2 * time::SECOND));

        // although conditions are perfect, climbing back from POOR (because of pause above)
        // will only climb to GOOD.
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                bytes: 8_000_000 / 8 / 5,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        (mos, quality) = cs.get_score_and_quality();
        assert!(4.1f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Good, quality);
    }

    // "quality scorer dependent rtt"
    {
        let cs = ConnectionStats::new(ConnectionStatsParams {
            include_rtt: false,
            include_jitter: true,
            receiver_provider: receiver_provider(&trp),
            logger: logger::get_logger(),
            ..Default::default()
        });

        let duration = 5 * time::SECOND;
        let now = time::now();
        cs.start_at(mime::MimeType::Opus, false, now.add(-duration));
        cs.update_mute_at(false, now.add(-1 * time::SECOND));

        // RTT does not knock quality down because it is dependent and hence not taken into account
        // at 2% loss, quality should stay at EXCELLENT purely based on loss. With high RTT (700 ms)
        // quality should drop to GOOD if RTT were taken into consideration
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                packets_lost: 5,
                rtt_max: 700,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        let (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);
    }

    // "quality scorer dependent jitter"
    {
        let cs = ConnectionStats::new(ConnectionStatsParams {
            include_rtt: true,
            include_jitter: false,
            receiver_provider: receiver_provider(&trp),
            logger: logger::get_logger(),
            ..Default::default()
        });

        let duration = 5 * time::SECOND;
        let now = time::now();
        cs.start_at(mime::MimeType::Opus, false, now.add(-duration));
        cs.update_mute_at(false, now.add(-1 * time::SECOND));

        // Jitter does not knock quality down because it is dependent and hence not taken into account
        // at 2% loss, quality should stay at EXCELLENT purely based on loss. With high jitter (200 ms)
        // quality should drop to GOOD if jitter were taken into consideration
        trp.set_streams(HashMap::from([(
            1,
            stream_stats(rtpstats::RtpDeltaInfo {
                start_time: now,
                end_time: now.add(duration),
                packets: 250,
                packets_lost: 5,
                jitter_max: 200.0,
                ..Default::default()
            }),
        )]));
        cs.update_score_at(now.add(duration));
        let (mos, quality) = cs.get_score_and_quality();
        assert!(4.6f32 > mos);
        assert_eq!(livekit::ConnectionQuality::Excellent, quality);
    }

    // "codecs - packet"
    {
        struct ExpectedQuality {
            packet_loss_percentage: f64,
            expected_mos: f32,
            expected_quality: livekit::ConnectionQuality,
        }
        struct TestCase {
            name: &'static str,
            mime_type: mime::MimeType,
            is_fec_enabled: bool,
            packets_expected: u32,
            expected_qualities: Vec<ExpectedQuality>,
        }
        let test_cases = [
            // NOTE: Because of EWMA (Exponentially Weighted Moving Average), these cut off points are not exact
            // "audio/opus" - no fec - 0 <= loss < 2.5%: EXCELLENT, 2.5% <= loss < 7.5%: GOOD, >= 7.5%: POOR
            TestCase {
                name: "audio/opus - no fec",
                mime_type: mime::MimeType::Opus,
                is_fec_enabled: false,
                packets_expected: 200,
                expected_qualities: vec![
                    ExpectedQuality {
                        packet_loss_percentage: 1.0,
                        expected_mos: 4.6,
                        expected_quality: livekit::ConnectionQuality::Excellent,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 4.0,
                        expected_mos: 4.1,
                        expected_quality: livekit::ConnectionQuality::Good,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 9.2,
                        expected_mos: 2.1,
                        expected_quality: livekit::ConnectionQuality::Poor,
                    },
                ],
            },
            // "audio/opus" - fec - 0 <= loss < 3.75%: EXCELLENT, 3.75% <= loss < 11.25%: GOOD, >= 11.25%: POOR
            TestCase {
                name: "audio/opus - fec",
                mime_type: mime::MimeType::Opus,
                is_fec_enabled: true,
                packets_expected: 200,
                expected_qualities: vec![
                    ExpectedQuality {
                        packet_loss_percentage: 3.0,
                        expected_mos: 4.6,
                        expected_quality: livekit::ConnectionQuality::Excellent,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 4.4,
                        expected_mos: 4.1,
                        expected_quality: livekit::ConnectionQuality::Good,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 15.0,
                        expected_mos: 2.1,
                        expected_quality: livekit::ConnectionQuality::Poor,
                    },
                ],
            },
            // "audio/red" - no fec - 0 <= loss < 5%: EXCELLENT, 5% <= loss < 15%: GOOD, >= 15%: POOR
            TestCase {
                name: "audio/red - no fec",
                mime_type: mime::MimeType::Red,
                is_fec_enabled: false,
                packets_expected: 200,
                expected_qualities: vec![
                    ExpectedQuality {
                        packet_loss_percentage: 4.0,
                        expected_mos: 4.6,
                        expected_quality: livekit::ConnectionQuality::Excellent,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 6.0,
                        expected_mos: 4.1,
                        expected_quality: livekit::ConnectionQuality::Good,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 19.5,
                        expected_mos: 2.1,
                        expected_quality: livekit::ConnectionQuality::Poor,
                    },
                ],
            },
            // "audio/red" - fec - 0 <= loss < 7.5%: EXCELLENT, 7.5% <= loss < 22.5%: GOOD, >= 22.5%: POOR
            TestCase {
                name: "audio/red - fec",
                mime_type: mime::MimeType::Red,
                is_fec_enabled: true,
                packets_expected: 200,
                expected_qualities: vec![
                    ExpectedQuality {
                        packet_loss_percentage: 6.0,
                        expected_mos: 4.6,
                        expected_quality: livekit::ConnectionQuality::Excellent,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 10.0,
                        expected_mos: 4.1,
                        expected_quality: livekit::ConnectionQuality::Good,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 30.0,
                        expected_mos: 2.1,
                        expected_quality: livekit::ConnectionQuality::Poor,
                    },
                ],
            },
            // "video/*" - 0 <= loss < 2%: EXCELLENT, 2% <= loss < 6%: GOOD, >= 6%: POOR
            TestCase {
                name: "video/*",
                mime_type: mime::MimeType::Vp8,
                is_fec_enabled: false,
                packets_expected: 200,
                expected_qualities: vec![
                    ExpectedQuality {
                        packet_loss_percentage: 1.0,
                        expected_mos: 4.6,
                        expected_quality: livekit::ConnectionQuality::Excellent,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 3.5,
                        expected_mos: 4.1,
                        expected_quality: livekit::ConnectionQuality::Good,
                    },
                    ExpectedQuality {
                        packet_loss_percentage: 8.0,
                        expected_mos: 2.1,
                        expected_quality: livekit::ConnectionQuality::Poor,
                    },
                ],
            },
        ];

        for tc in &test_cases {
            let cs = ConnectionStats::new(ConnectionStatsParams {
                include_rtt: true,
                include_jitter: true,
                receiver_provider: receiver_provider(&trp),
                logger: logger::get_logger(),
                ..Default::default()
            });

            let duration = 5 * time::SECOND;
            let mut now = time::now();
            cs.start_at(tc.mime_type, tc.is_fec_enabled, now.add(-duration));

            for eq in &tc.expected_qualities {
                trp.set_streams(HashMap::from([(
                    123,
                    stream_stats(rtpstats::RtpDeltaInfo {
                        start_time: now,
                        end_time: now.add(duration),
                        packets: tc.packets_expected,
                        packets_lost: (eq.packet_loss_percentage * tc.packets_expected as f64
                            / 100.0)
                            .ceil() as u32,
                        ..Default::default()
                    }),
                )]));
                cs.update_score_at(now.add(duration));
                let (mos, quality) = cs.get_score_and_quality();
                assert!(
                    eq.expected_mos > mos,
                    "{}: loss {}",
                    tc.name,
                    eq.packet_loss_percentage
                );
                assert_eq!(
                    eq.expected_quality, quality,
                    "{}: loss {}",
                    tc.name, eq.packet_loss_percentage
                );

                now = now.add(duration);
            }
        }
    }

    // "bitrate"
    {
        #[derive(Default)]
        struct Transition {
            bitrate: i64,
            offset: Duration,
        }
        struct TestCase {
            name: &'static str,
            transitions: Vec<Transition>,
            bytes: u64,
            expected_mos: f32,
            expected_quality: livekit::ConnectionQuality,
        }
        let test_cases = [
            // NOTE: Because of EWMA (Exponentially Weighted Moving Average), these cut off points are not exact
            // 1.0 <= expectedBits / actualBits < ~2.7 = EXCELLENT
            // ~2.7 <= expectedBits / actualBits < ~20.1 = GOOD
            // expectedBits / actualBits >= ~20.1 = POOR
            TestCase {
                name: "excellent",
                transitions: vec![
                    Transition {
                        bitrate: 1_000_000,
                        ..Default::default()
                    },
                    Transition {
                        bitrate: 2_000_000,
                        offset: 3 * time::SECOND,
                    },
                ],
                bytes: 6_000_000 / 8,
                expected_mos: 4.6,
                expected_quality: livekit::ConnectionQuality::Excellent,
            },
            TestCase {
                name: "good",
                transitions: vec![
                    Transition {
                        bitrate: 1_000_000,
                        ..Default::default()
                    },
                    Transition {
                        bitrate: 2_000_000,
                        offset: 3 * time::SECOND,
                    },
                ],
                bytes: (7_000_000.0f64 / 8.0 / 4.2).ceil() as u64,
                expected_mos: 4.1,
                expected_quality: livekit::ConnectionQuality::Good,
            },
            TestCase {
                name: "poor",
                transitions: vec![
                    Transition {
                        bitrate: 2_000_000,
                        ..Default::default()
                    },
                    Transition {
                        bitrate: 1_000_000,
                        offset: 3 * time::SECOND,
                    },
                ],
                bytes: (8_000_000.0f64 / 8.0 / 75.0).ceil() as u64,
                expected_mos: 2.1,
                expected_quality: livekit::ConnectionQuality::Poor,
            },
        ];

        for tc in &test_cases {
            let cs = ConnectionStats::new(ConnectionStatsParams {
                include_rtt: true,
                include_jitter: true,
                enable_bitrate_score: true,
                receiver_provider: receiver_provider(&trp),
                logger: logger::get_logger(),
                ..Default::default()
            });

            let duration = 5 * time::SECOND;
            let now = time::now();
            cs.start_at(mime::MimeType::Vp8, false, now);

            for tr in &tc.transitions {
                cs.add_bitrate_transition_at(tr.bitrate, now.add(tr.offset));
            }

            trp.set_streams(HashMap::from([(
                123,
                stream_stats(rtpstats::RtpDeltaInfo {
                    start_time: now,
                    end_time: now.add(duration),
                    packets: 100,
                    bytes: tc.bytes,
                    ..Default::default()
                }),
            )]));
            cs.update_score_at(now.add(duration));
            let (mos, quality) = cs.get_score_and_quality();
            assert!(tc.expected_mos > mos, "{}", tc.name);
            assert_eq!(tc.expected_quality, quality, "{}", tc.name);
        }
    }

    // "layer"
    {
        #[derive(Default)]
        struct Transition {
            distance: f64,
            offset: Duration,
        }
        struct TestCase {
            name: &'static str,
            transitions: Vec<Transition>,
            expected_mos: f32,
            expected_quality: livekit::ConnectionQuality,
        }
        let test_cases = [
            // NOTE: Because of EWMA (Exponentially Weighted Moving Average), these cut off points are not exact
            // each spatial layer missed drops o quality level
            TestCase {
                name: "excellent",
                transitions: vec![
                    Transition {
                        distance: 0.5,
                        ..Default::default()
                    },
                    Transition {
                        distance: 0.0,
                        offset: 3 * time::SECOND,
                    },
                ],
                expected_mos: 4.6,
                expected_quality: livekit::ConnectionQuality::Excellent,
            },
            TestCase {
                name: "good",
                transitions: vec![
                    Transition {
                        distance: 1.0,
                        ..Default::default()
                    },
                    Transition {
                        distance: 1.5,
                        offset: 2 * time::SECOND,
                    },
                ],
                expected_mos: 4.1,
                expected_quality: livekit::ConnectionQuality::Good,
            },
            TestCase {
                name: "poor",
                transitions: vec![
                    Transition {
                        distance: 2.0,
                        ..Default::default()
                    },
                    Transition {
                        distance: 2.6,
                        offset: 1 * time::SECOND,
                    },
                ],
                expected_mos: 2.1,
                expected_quality: livekit::ConnectionQuality::Poor,
            },
        ];

        for tc in &test_cases {
            let cs = ConnectionStats::new(ConnectionStatsParams {
                include_rtt: true,
                include_jitter: true,
                receiver_provider: receiver_provider(&trp),
                logger: logger::get_logger(),
                ..Default::default()
            });

            let duration = 5 * time::SECOND;
            let now = time::now();
            cs.start_at(mime::MimeType::Vp8, false, now);

            for tr in &tc.transitions {
                cs.add_layer_transition_at(tr.distance, now.add(tr.offset));
            }

            trp.set_streams(HashMap::from([(
                123,
                stream_stats(rtpstats::RtpDeltaInfo {
                    start_time: now,
                    end_time: now.add(duration),
                    packets: 200,
                    ..Default::default()
                }),
            )]));
            cs.update_score_at(now.add(duration));
            let (mos, quality) = cs.get_score_and_quality();
            assert!(tc.expected_mos > mos, "{}", tc.name);
            assert_eq!(tc.expected_quality, quality, "{}", tc.name);
        }
    }
}

#[test]
fn test_packet_rate_mode() {
    struct Phase {
        minutes: f64,
        pps: f64,
    }
    struct TestCase {
        name: &'static str,
        weight: f64,
        histogram: Vec<(usize, i64)>, // bin -> count
        phases: Vec<Phase>,
        last: WindowStat,
        mode: i64, // pps
        quality: livekit::ConnectionQuality,
    }
    let test_cases = [
        // 120 windows on the 250 pps layer, 600 windows on the 40 pps layer
        TestCase {
            name: "most common bin",
            weight: 10.0,
            histogram: vec![(125, 120), (20, 600)],
            phases: vec![],
            last: WindowStat {
                packets: 200,
                packets_lost: 8,
                duration: 5 * time::SECOND,
                ..Default::default()
            },
            mode: 40,
            quality: livekit::ConnectionQuality::Good,
        },
        // 4% loss on the layer now watched is not EXCELLENT
        TestCase {
            name: "follows a layer switch",
            weight: 10.0,
            histogram: vec![],
            phases: vec![
                Phase {
                    minutes: 10.0,
                    pps: 250.0,
                },
                Phase {
                    minutes: 5.0,
                    pps: 40.0,
                },
            ],
            last: WindowStat {
                packets: 200,
                packets_lost: 8,
                duration: 5 * time::SECOND,
                ..Default::default()
            },
            mode: 40,
            quality: livekit::ConnectionQuality::Good,
        },
        // idle and DTX rates do not become the reference, so a lost DTX packet counts less
        TestCase {
            name: "keeps the speech rate through a long silence",
            weight: 8.0,
            histogram: vec![],
            phases: vec![
                Phase {
                    minutes: 5.0,
                    pps: 50.0,
                },
                Phase {
                    minutes: 10.0,
                    pps: 0.0,
                },
                Phase {
                    minutes: 30.0,
                    pps: 2.5,
                },
            ],
            last: WindowStat {
                packets: 12,
                packets_lost: 1,
                duration: 5 * time::SECOND,
                ..Default::default()
            },
            mode: 50,
            quality: livekit::ConnectionQuality::Excellent,
        },
    ];
    for tc in &test_cases {
        let q = QualityScorer::new(QualityScorerParams {
            logger: logger::get_logger(),
            ..Default::default()
        });
        // a timeline in the past, the mode must follow update times, not the wall clock
        let mut at = time::now().add(-time::HOUR);
        q.start_at(tc.weight, at);
        for &(bin, count) in &tc.histogram {
            let mut q = q.lock.write();
            q.pps_histogram[bin] = count;
            q.num_pps_readings += count;
        }
        for p in &tc.phases {
            for _ in 0..(p.minutes * 12.0) as i64 {
                at = at.add(5 * time::SECOND);
                q.update_at(
                    &WindowStat {
                        packets: (p.pps * 5.0) as u32,
                        duration: 5 * time::SECOND,
                        ..Default::default()
                    },
                    at,
                );
            }
        }
        at = at.add(tc.last.duration);
        q.update_at(&tc.last, at);

        assert_eq!(
            tc.mode,
            (q.lock.read().pps_mode as f64 * C_PPS_QUANTIZATION) as i64,
            "{}",
            tc.name
        );
        let (_, quality) = q.get_score_and_quality();
        assert_eq!(tc.quality, quality, "{}", tc.name);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upstream_loss_after_rtx_repair() {
    // 1000 video packets, 100 lost, RTX repairs 70 in the same window, 30 (3%) never arrive
    let r = rtpstats::RtpStatsReceiver::new(rtpstats::RtpStatsParams::default());
    r.set_clock_rate(90000);
    let snapshot_id = r.new_snapshot_id();

    let mut packet_time = time::now().unix_nano();
    let mut send = |sn: u16| {
        packet_time += (5 * time::MILLISECOND).nanoseconds();
        r.update(packet_time, sn, sn as u32 * 450, false, 12, 1000, 0);
    };
    for sn in 0u16..1000 {
        if sn == 300 {
            for repaired in 100u16..170 {
                send(repaired);
            }
        }
        if !(100..200).contains(&sn) {
            send(sn);
        }
    }

    let delta = r.delta_info(snapshot_id).expect("a delta");
    assert_eq!(1000u32, delta.packets);
    assert_eq!(30u32, delta.packets_lost);
    assert_eq!(70u32, delta.packets_out_of_order);

    let trp = new_test_receiver_provider();
    trp.set_streams(HashMap::from([(1, stream_stats(delta))]));
    let cs = ConnectionStats::new(ConnectionStatsParams {
        receiver_provider: receiver_provider(&trp),
        logger: logger::get_logger(),
        ..Default::default()
    });
    let now = time::now();
    cs.start_at(mime::MimeType::Vp8, false, now);
    cs.update_score_at(now.add(5 * time::SECOND));

    // packet score 100 - 3% * 10 = 70, smoothed 0.8 * 70 + 0.2 * 100 = 76
    let (score, quality) = cs.scorer.get_score_and_quality();
    testutils::assert_in_delta(76.0, score as f64, 0.01);
    assert_eq!(livekit::ConnectionQuality::Good, quality);
}
