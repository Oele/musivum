//! New for musivum: the scorer held against the original, window by window.
//!
//! `run` feeds a scorer a sequence of windows (packets, loss, missing and out-of-order
//! packets, padding, bytes, round trip time, jitter, the time of the last RTCP) with what
//! happens between them (mute and unmute, a layer muted, a pause, bitrate and layer
//! transitions, a change of codec), all at explicit times and drawn from a generator of
//! numbers that is the same on both sides, and writes down the score, the MOS, the quality
//! and the mode of the packet rate after each. `testdata/reference_output_test.go` does the
//! same with the original scorer, and says how it is run. The scenarios are audio with and
//! without DTX as Opus and as RED, video, and a screen share.
//!
//! `PINNED` holds lines the original wrote, every eighteenth one. `MUSIVUM_SCORER_REFERENCE`
//! may name a file with all of them; they are then all compared, and what the port wrote is
//! put next to it as `<file>.rust`.

use super::connectionstats::get_packet_loss_weight;
use super::scorer::{QualityScorer, QualityScorerParams, WindowStat};
use crate::go::time::{self, Duration, Time};
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::logger;

// SplitMix64
struct Rng {
    state: u64,
}

impl Rng {
    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn intn(&mut self, n: i64) -> i64 {
        (self.next() % n as u64) as i64
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Audio,
    Dtx,
    Video,
    Screen,
}

struct Scenario {
    name: &'static str,
    kind: Kind,
    mime: MimeType,
    fec: bool,
    mime2: MimeType,
    fec2: bool,
    include_rtt: bool,
    include_jitter: bool,
    enable_bitrate: bool,
    seed: u64,
    windows: usize,
}

const BITRATES: [i64; 4] = [150_000, 500_000, 1_200_000, 2_500_000];
const DISTANCES: [f64; 6] = [0.0, 0.5, 1.0, 1.5, 2.0, 2.6];
const LAYERS: [i64; 3] = [400, 1200, 2500];

fn ms(n: i64) -> Duration {
    n * time::MILLISECOND
}

fn emit(lines: &mut Vec<String>, sc: &Scenario, q: &QualityScorer, i: usize, tag: &str) {
    let (score32, quality) = q.get_score_and_quality();
    let (mos, _) = q.get_mos_and_quality();
    let inner = q.lock.read();
    lines.push(format!(
        "{} {} {} {:016x} {:08x} {:08x} {} {}",
        sc.name,
        i,
        tag,
        inner.score.to_bits(),
        score32.to_bits(),
        mos.to_bits(),
        quality as i32,
        inner.pps_mode,
    ));
}

fn run(sc: &Scenario, lines: &mut Vec<String>) {
    let mut rng = Rng { state: sc.seed };
    let q = QualityScorer::new(QualityScorerParams {
        include_rtt: sc.include_rtt,
        include_jitter: sc.include_jitter,
        enable_bitrate_score: sc.enable_bitrate,
        logger: logger::get_logger(),
    });
    let mut at = Time::unix(1_700_000_000, 0);
    q.start_at(get_packet_loss_weight(sc.mime, sc.fec), at);

    let video = sc.kind == Kind::Video || sc.kind == Kind::Screen;
    let (mut muted, mut layer_muted, mut paused) = (false, false, false);
    let mut speech = true;
    let mut layer_pps10 = 2500i64;
    let mut expected = 0i64;

    for i in 0..sc.windows {
        let mut dur_ms = 5000i64;
        if rng.intn(10) == 0 {
            dur_ms = 4000 + rng.intn(2500);
        }
        let start = at;
        let end = at.add(ms(dur_ms));

        if i == sc.windows / 2 {
            q.update_packet_loss_weight(get_packet_loss_weight(sc.mime2, sc.fec2));
        }

        // mute
        let mut r = rng.intn(100);
        let off1 = rng.intn(dur_ms);
        let off2 = off1 + rng.intn(dur_ms - off1 + 1);
        let t1 = start.add(ms(off1));
        let t2 = start.add(ms(off2));
        if muted {
            if r < 50 {
                q.update_mute_at(false, t1);
                muted = false;
                emit(lines, sc, &q, i, "unmute");
            }
        } else if r < 6 {
            q.update_mute_at(true, t1);
            muted = true;
            emit(lines, sc, &q, i, "mute");
        } else if r < 9 {
            q.update_mute_at(true, t1);
            emit(lines, sc, &q, i, "mute");
            q.update_mute_at(false, t2);
            emit(lines, sc, &q, i, "unmute");
        } else if r < 12 {
            q.update_mute_at(false, t1);
            emit(lines, sc, &q, i, "unmute-again");
        }

        if video {
            // layer mute
            r = rng.intn(100);
            let mut t = start.add(ms(rng.intn(dur_ms)));
            if layer_muted {
                if r < 50 {
                    q.update_layer_mute_at(false, t);
                    layer_muted = false;
                    emit(lines, sc, &q, i, "layer-unmute");
                }
            } else if r < 4 {
                q.update_layer_mute_at(true, t);
                layer_muted = true;
                emit(lines, sc, &q, i, "layer-mute");
            } else if r < 6 {
                q.update_layer_mute_at(false, t);
                emit(lines, sc, &q, i, "layer-unmute-again");
            }

            // pause
            r = rng.intn(100);
            t = start.add(ms(rng.intn(dur_ms)));
            if paused {
                if r < 50 {
                    q.update_pause_at(false, t);
                    paused = false;
                    emit(lines, sc, &q, i, "resume");
                }
            } else if r < 4 {
                q.update_pause_at(true, t);
                paused = true;
                emit(lines, sc, &q, i, "pause");
            } else if r < 6 {
                q.update_pause_at(false, t);
                emit(lines, sc, &q, i, "resume-again");
            }

            // bitrate transitions
            r = rng.intn(100);
            if i == 0 || r < 30 {
                let n = 1 + rng.intn(2);
                let mut off = 0i64;
                for _ in 0..n {
                    off += rng.intn(dur_ms / 2);
                    expected = BITRATES[rng.intn(BITRATES.len() as i64) as usize];
                    expected += rng.intn(50_000);
                    q.add_bitrate_transition_at(expected, start.add(ms(off)));
                }
            } else if r < 32 {
                // a sample before the last one, which the aggregator refuses
                q.add_bitrate_transition_at(
                    BITRATES[rng.intn(BITRATES.len() as i64) as usize],
                    start.add(-time::SECOND),
                );
            } else if r < 34 {
                // a sample beyond the end of the window: the update that follows cannot aggregate
                q.add_bitrate_transition_at(expected, end.add(time::SECOND));
            }

            // layer transitions
            r = rng.intn(100);
            if r < 20 {
                let n = 1 + rng.intn(2);
                let mut off = 0i64;
                for _ in 0..n {
                    off += rng.intn(dur_ms / 2);
                    q.add_layer_transition_at(
                        DISTANCES[rng.intn(DISTANCES.len() as i64) as usize],
                        start.add(ms(off)),
                    );
                }
            }
        }

        // the window
        let pps10 = match sc.kind {
            Kind::Audio => 500,
            Kind::Dtx => {
                r = rng.intn(100);
                if r < 15 {
                    speech = !speech;
                }
                if speech { 500 } else { 25 }
            }
            Kind::Video => {
                r = rng.intn(100);
                if r < 8 {
                    layer_pps10 = LAYERS[rng.intn(LAYERS.len() as i64) as usize];
                }
                layer_pps10
            }
            Kind::Screen => {
                r = rng.intn(100);
                if r < 60 {
                    rng.intn(100)
                } else {
                    rng.intn(3000)
                }
            }
        };
        let mut packets = (pps10 * dur_ms / 10000) as u32;
        r = rng.intn(100);
        if r < 5 {
            packets = 0;
        }

        // loss
        r = rng.intn(100);
        let permille = if r < 55 {
            0
        } else if r < 75 {
            rng.intn(30)
        } else if r < 90 {
            30 + rng.intn(70)
        } else {
            100 + rng.intn(300)
        };
        let mut lost = (packets as i64 * permille / 1000) as u32;
        r = rng.intn(100);
        if r < 3 {
            lost += rng.intn(5) as u32;
        }

        let (mut missing, mut ooo, mut padding) = (0u32, 0u32, 0u32);
        if video {
            r = rng.intn(100);
            if r < 10 {
                missing = rng.intn(lost as i64 + 1) as u32;
            }
            r = rng.intn(100);
            if r < 10 {
                ooo = rng.intn(lost as i64 + 3) as u32;
            }
            r = rng.intn(100);
            if r < 15 {
                padding = rng.intn(50) as u32;
            }
        }

        // bytes
        let size = if video { 900i64 } else { 100i64 };
        let mut bytes = (packets as i64 * (size / 2 + rng.intn(size))) as u64;
        if video {
            r = rng.intn(100);
            if r < 10 {
                bytes /= (2 + rng.intn(60)) as u64;
            } else if r < 13 {
                bytes = 0;
            }
        }

        // round trip time
        r = rng.intn(100);
        let rtt = if r < 70 {
            rng.intn(80) as u32
        } else if r < 90 {
            rng.intn(400) as u32
        } else {
            rng.intn(1500) as u32
        };

        // jitter, in microseconds
        r = rng.intn(100);
        let mut jitter = if r < 70 {
            rng.intn(20_000) as f64
        } else {
            rng.intn(400_000) as f64
        };
        jitter += rng.intn(1000) as f64 / 1000.0;

        // the last RTCP
        r = rng.intn(100);
        let mut last_rtcp_at = Time::ZERO;
        if r < 40 {
            last_rtcp_at = end.add(-time::SECOND);
        } else if r < 60 {
            last_rtcp_at = start.add(-10 * time::SECOND);
        }

        let stat = WindowStat {
            started_at: start,
            duration: ms(dur_ms),
            packets,
            packets_padding: padding,
            packets_lost: lost,
            packets_missing: missing,
            packets_out_of_order: ooo,
            bytes,
            rtt_max: rtt,
            jitter_max: jitter,
            last_rtcp_at,
        };
        at = end;
        q.update_at(&stat, at);
        emit(lines, sc, &q, i, "window");
    }
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "opus",
            kind: Kind::Audio,
            mime: MimeType::Opus,
            fec: false,
            mime2: MimeType::Opus,
            fec2: true,
            include_rtt: true,
            include_jitter: true,
            enable_bitrate: false,
            seed: 1,
            windows: 100,
        },
        Scenario {
            name: "opus-dtx",
            kind: Kind::Dtx,
            mime: MimeType::Opus,
            fec: true,
            mime2: MimeType::Red,
            fec2: false,
            include_rtt: true,
            include_jitter: true,
            enable_bitrate: false,
            seed: 2,
            windows: 100,
        },
        Scenario {
            name: "red",
            kind: Kind::Audio,
            mime: MimeType::Red,
            fec: false,
            mime2: MimeType::Pcmu,
            fec2: false,
            include_rtt: false,
            include_jitter: true,
            enable_bitrate: false,
            seed: 3,
            windows: 100,
        },
        Scenario {
            name: "red-dtx",
            kind: Kind::Dtx,
            mime: MimeType::Red,
            fec: true,
            mime2: MimeType::Opus,
            fec2: false,
            include_rtt: true,
            include_jitter: false,
            enable_bitrate: false,
            seed: 4,
            windows: 100,
        },
        Scenario {
            name: "video",
            kind: Kind::Video,
            mime: MimeType::Vp8,
            fec: false,
            mime2: MimeType::Vp9,
            fec2: false,
            include_rtt: true,
            include_jitter: true,
            enable_bitrate: true,
            seed: 5,
            windows: 100,
        },
        Scenario {
            name: "screen",
            kind: Kind::Screen,
            mime: MimeType::H264,
            fec: false,
            mime2: MimeType::Av1,
            fec2: false,
            include_rtt: false,
            include_jitter: false,
            enable_bitrate: true,
            seed: 6,
            windows: 100,
        },
    ]
}

// (the number of the line, the line)
const PINNED: &[(usize, &str)] = &[
    (8, "opus 8 window 403e1b0e673e912a 41f0d873 3fce82ec 0 0"),
    (26, "opus 25 window 4044000000000000 42200000 40041893 0 25"),
    (44, "opus 43 window 4045516f2ea7b816 422a8b79 400c7abf 1 25"),
    (62, "opus 56 window 4059000000000000 42c80000 40900000 2 25"),
    (80, "opus 69 window 404cc8271f28ba34 42664139 403e46c6 1 25"),
    (98, "opus 87 window 40500d7cf35db101 42806be8 40542acc 1 25"),
    (
        116,
        "opus-dtx 1 window 40521d269e9ac703 4290e935 406d6fcb 1 0",
    ),
    (
        134,
        "opus-dtx 19 window 404fc3ad117095c0 427e1d69 4051f798 1 25",
    ),
    (
        152,
        "opus-dtx 31 window 4057ca388db27d0d 42be51c4 408e34da 2 25",
    ),
    (
        170,
        "opus-dtx 49 window 40574992bfb3cb74 42ba4c96 408d10db 2 25",
    ),
    (
        188,
        "opus-dtx 64 window 4055f46ed64c49ba 42afa377 40890601 2 25",
    ),
    (
        206,
        "opus-dtx 82 window 4056a9e6cc9f1658 42b54f36 408b59c5 2 25",
    ),
    (
        224,
        "opus-dtx 97 window 40480eb664c979be 424075b3 401e7c31 1 25",
    ),
    (242, "red 13 window 404a402cf00c6405 42520168 402d39c3 1 25"),
    (260, "red 27 window 4059000000000000 42c80000 40900000 2 25"),
    (278, "red 43 mute 4059000000000000 42c80000 40900000 2 25"),
    (296, "red 59 window 4057fa8a66e18e71 42bfd453 408e93a8 2 25"),
    (314, "red 76 window 4059000000000000 42c80000 40900000 2 25"),
    (332, "red 89 unmute 4059000000000000 42c80000 40900000 2 25"),
    (
        350,
        "red-dtx 2 unmute 4059000000000000 42c80000 40900000 2 0",
    ),
    (
        368,
        "red-dtx 16 window 404a1ae147ae147c 4250d70a 402c3e1e 1 25",
    ),
    (
        386,
        "red-dtx 33 window 405692e44dfaccb4 42b49722 408b13c8 2 25",
    ),
    (
        404,
        "red-dtx 49 window 405683911b1a8961 42b41c89 408ae441 2 25",
    ),
    (
        422,
        "red-dtx 67 window 40553ed0df3d3733 42a9f687 4086523d 2 25",
    ),
    (
        440,
        "red-dtx 84 window 4056e658c3082ac1 42b732c6 408c09ae 2 25",
    ),
    (458, "video 0 window 4048de622aca8efb 4246f311 4023eafa 1 0"),
    (
        476,
        "video 13 window 404963ca2b2d8cb0 424b1e51 40276c08 1 0",
    ),
    (
        494,
        "video 24 window 4059000000000000 42c80000 40900000 2 60",
    ),
    (
        512,
        "video 36 unmute 4059000000000000 42c80000 40900000 2 60",
    ),
    (
        530,
        "video 48 window 403e000000000000 41f00000 3fcdf3b6 0 60",
    ),
    (
        548,
        "video 60 window 404345d64ca84f3e 421a2eb2 3fff260a 0 60",
    ),
    (
        566,
        "video 77 window 4059000000000000 42c80000 40900000 2 125",
    ),
    (
        584,
        "video 91 window 403e000000000000 41f00000 3fcdf3b6 0 125",
    ),
    (
        602,
        "screen 5 window 403e000000000000 41f00000 3fcdf3b6 0 0",
    ),
    (
        620,
        "screen 17 unmute-again 403e000000000000 41f00000 3fcdf3b6 0 43",
    ),
    (
        638,
        "screen 29 window 4045dc28f5c28f5c 422ee148 400ffca2 1 43",
    ),
    (
        656,
        "screen 44 pause 403e000000000000 41f00000 3fcdf3b6 0 5",
    ),
    (
        674,
        "screen 60 window 403e000000000000 41f00000 3fcdf3b6 0 5",
    ),
    (
        692,
        "screen 74 window 403e000000000000 41f00000 3fcdf3b6 0 5",
    ),
    (
        710,
        "screen 87 window 404df71804c505a5 426fb8c0 40462b56 1 5",
    ),
];

#[test]
fn the_scorer_gives_what_the_original_gives() {
    let mut lines = Vec::new();
    for sc in &scenarios() {
        run(sc, &mut lines);
    }

    // 600 windows and what happened between them
    assert_eq!(728, lines.len());
    for &(n, expected) in PINNED {
        assert_eq!(expected, lines[n], "line {n}");
    }

    if let Ok(path) = std::env::var("MUSIVUM_SCORER_REFERENCE") {
        std::fs::write(format!("{path}.rust"), lines.join("\n") + "\n").expect("the port's lines");
        let original = std::fs::read_to_string(&path).expect("the original's lines");
        let original: Vec<&str> = original.lines().collect();
        assert_eq!(original.len(), lines.len(), "the number of lines");
        for (n, (expected, line)) in original.iter().zip(&lines).enumerate() {
            assert_eq!(expected, line, "line {n}");
        }
    }
}
