//! New for musivum: the selectors that have no test of their own in the original, or a thin
//! one (`simulcast.rs`, `vp9.rs`, `base.rs`, `temporallayerselector/vp8.rs` and the
//! `new_from_other` constructors), held against the original.
//!
//! `testdata/reference_input.txt` is a list of commands: selectors to make, layers to set
//! and packets to select. `testdata/reference_output.txt` is what LiveKit's own code at the
//! pin gives for each of them; `testdata/reference_output_test.go` printed it, and says how
//! it is run. The test here runs the same commands on the port and expects the same lines.

use super::temporallayerselector;
use super::*;
use crate::mediatransportutil::codec;
use crate::protocol::logger;
use crate::sfu::buffer;
use crate::webrtc::rtp;
use crate::webrtc::rtp::codecs;

fn ref_int(s: &str) -> i32 {
    s.parse().expect("not a number")
}

fn ref_layer(spatial: &str, temporal: &str) -> buffer::VideoLayer {
    buffer::VideoLayer {
        spatial: ref_int(spatial),
        temporal: ref_int(temporal),
    }
}

fn ref_kind(vls: &dyn VideoLayerSelector) -> &'static str {
    let any = vls.as_any();
    if any.is::<Null>() {
        "null"
    } else if any.is::<Simulcast>() {
        "simulcast"
    } else if any.is::<Vp9>() {
        "vp9"
    } else if any.is::<DependencyDescriptor>() {
        "dd"
    } else {
        "?"
    }
}

fn ref_packet(marker: &str) -> rtp::Packet {
    rtp::Packet {
        header: rtp::Header {
            marker: marker == "1",
            ssrc: 1234,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn test_reference_output() {
    let mut expected = include_str!("testdata/reference_output.txt").lines();

    // the original starts with a nil interface, which the commands never use
    let mut vls: Box<dyn VideoLayerSelector> = Box::new(Null::new(logger::get_logger()));
    for (n, line) in include_str!("testdata/reference_input.txt")
        .lines()
        .enumerate()
    {
        let n = n + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let c: Vec<&str> = line.split_whitespace().collect();
        let mut out = c[0].to_string();
        match c[0] {
            "new" => {
                vls = match c[1] {
                    "null" => Box::new(Null::new(logger::get_logger())),
                    "simulcast" => Box::new(Simulcast::new(logger::get_logger())),
                    "vp9" => Box::new(Vp9::new(logger::get_logger())),
                    "dd" => Box::new(DependencyDescriptor::new(logger::get_logger())),
                    _ => panic!("line {n}: {line}"),
                };
            }
            "from" => {
                vls = match c[1] {
                    "simulcast" => match Simulcast::new_from_other(vls) {
                        Ok(s) => Box::new(s),
                        Err(vls) => {
                            out = "from nil".to_string();
                            vls
                        }
                    },
                    "vp9" => Box::new(Vp9::new_from_other(vls)),
                    "dd" => Box::new(DependencyDescriptor::new_from_other(vls)),
                    _ => panic!("line {n}: {line}"),
                };
            }
            "start" => vls.set_enable_start_at_desired_quality(c[1] == "1"),
            "max" => vls.set_max(ref_layer(c[1], c[2])),
            "maxspatial" => vls.set_max_spatial(ref_int(c[1])),
            "maxtemporal" => vls.set_max_temporal(ref_int(c[1])),
            "seen" => vls.set_max_seen(ref_layer(c[1], c[2])),
            "seenspatial" => vls.set_max_seen_spatial(ref_int(c[1])),
            "seentemporal" => vls.set_max_seen_temporal(ref_int(c[1])),
            "target" => vls.set_target(ref_layer(c[1], c[2])),
            "req" => vls.set_request_spatial(ref_int(c[1])),
            "current" => vls.set_current(ref_layer(c[1], c[2])),
            "tls" => match c[1] {
                "none" => vls.set_temporal_layer_selector(None),
                "null" => vls.set_temporal_layer_selector(Some(Box::new(
                    temporallayerselector::Null::new(),
                ))),
                "vp8" => vls.set_temporal_layer_selector(Some(Box::new(
                    temporallayerselector::Vp8::new(logger::get_logger()),
                ))),
                _ => panic!("line {n}: {line}"),
            },
            "rollback" => vls.rollback(),
            "pkt" => {
                // pkt <layer> <spatial> <temporal> <key frame> <marker> <VP9 flags>
                let mut ext_pkt = buffer::ExtPacket {
                    video_layer: ref_layer(c[2], c[3]),
                    packet: ref_packet(c[5]),
                    is_key_frame: c[4] == "1",
                    ..Default::default()
                };
                if c[6] != "-" {
                    ext_pkt.vp9 = Some(codecs::Vp9Packet {
                        u: c[6].contains('U'),
                        b: c[6].contains('B'),
                        e: c[6].contains('E'),
                        p: c[6].contains('P'),
                        ..Default::default()
                    });
                }
                let r = vls.select(&ext_pkt, ref_int(c[1]));
                out = format!(
                    "pkt sel={} rel={} sw={} res={} m={} dd={}/{}",
                    r.is_selected,
                    r.is_relevant,
                    r.is_switching,
                    r.is_resuming,
                    r.rtp_marker,
                    r.dd_bytes_len,
                    r.dd_bytes_spill.len()
                );
            }
            "tpkt" => {
                // tpkt <temporal> <key frame> <marker> <VP8 flags>
                let mut ext_pkt = buffer::ExtPacket {
                    video_layer: buffer::VideoLayer {
                        spatial: 0,
                        temporal: ref_int(c[1]),
                    },
                    packet: ref_packet(c[3]),
                    is_key_frame: c[2] == "1",
                    ..Default::default()
                };
                if c[4] != "-" {
                    ext_pkt.vp8 = Some(codec::Vp8 {
                        is_key_frame: c[4].contains('K'),
                        s: c[4].contains('S'),
                        y: c[4].contains('Y'),
                        tid: ref_int(c[1]) as u8,
                        ..Default::default()
                    });
                }
                out = format!("tpkt this={}", vls.select_temporal(&ext_pkt));
            }
            _ => panic!("line {n}: {line}"),
        }

        let (locked, layer) = vls.check_sync();
        let got = format!(
            "REF {n}: {out} | kind={} cur={}/{} tgt={}/{} max={}/{} seen={}/{} req={} sync={locked}/{layer} over={}",
            ref_kind(vls.as_ref()),
            vls.get_current().spatial,
            vls.get_current().temporal,
            vls.get_target().spatial,
            vls.get_target().temporal,
            vls.get_max().spatial,
            vls.get_max().temporal,
            vls.get_max_seen().spatial,
            vls.get_max_seen().temporal,
            vls.get_request_spatial(),
            vls.is_overshoot_okay(),
        );
        assert_eq!(expected.next(), Some(got.as_str()), "command: {line}");
    }
    assert_eq!(expected.next(), None, "output that no command gave");
}
