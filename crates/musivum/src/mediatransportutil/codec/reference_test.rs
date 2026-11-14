//! New for musivum: the port held against the original, payload by payload.
//!
//! `testdata/reference_output.txt` is everything the original package at the pin (and pion's
//! `VP9Packet`, which it uses) says about each payload of `testdata/reference_payloads.txt`:
//! one line per payload, `REF <the payload in hex>`, a tab, and the parts of `dump_line`
//! below. `testdata/reference_output_test.go` printed it, and says how it is run. The test
//! here says the same about the same payloads and expects the same lines, also where the
//! original panics (`vp8xt`: `MarshalTo` into a buffer that is as large as `HeaderSize` says
//! and smaller than what the flags make it write). `vp9kf` is `-` for a packet that does not
//! parse and for one that starts a frame without frame data.
//!
//! `MUSIVUM_CODEC_REFERENCE` may name another file of such lines, which is then read as
//! well: the output of the Go test for a larger file of payloads, kept outside the
//! repository.

use std::fmt::Display;
use std::fmt::Write as _;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::h26xhelper::{
    parse_h264_sps, parse_h265_sps, remove_emulation_prevention_bytes, strip_start_code,
};
use super::*;
use crate::webrtc::rtp::codecs::Vp9Packet;

fn b2s(b: bool) -> &'static str {
    if b { "1" } else { "0" }
}

// what f gives, or "panic"
fn safe(f: impl FnOnce() -> String) -> String {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|_| "panic".to_string())
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

// Go's `%v` of a slice.
fn list<T: Display>(v: &[T]) -> String {
    let items: Vec<String> = v.iter().map(|x| x.to_string()).collect();
    format!("[{}]", items.join(" "))
}

fn list2(v: &[Vec<u8>]) -> String {
    let items: Vec<String> = v.iter().map(|x| list(x)).collect();
    format!("[{}]", items.join(" "))
}

// the fields in the order of the struct
fn vp8_fields(v: &Vp8) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        v.first_byte,
        b2s(v.s),
        b2s(v.i),
        b2s(v.m),
        v.picture_id,
        b2s(v.l),
        v.tl0picidx,
        b2s(v.t),
        v.tid,
        b2s(v.y),
        b2s(v.k),
        v.keyidx,
        v.header_size,
        b2s(v.is_key_frame)
    )
}

fn sps_str(res: Result<SpsInfo, Error>) -> String {
    match res {
        Err(err) => format!("err:{err}"),
        Ok(info) => format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            info.chroma_format_idc,
            info.pic_width_in_luma_samples,
            info.pic_height_in_luma_samples,
            b2s(info.conformance_window_flag),
            info.conf_win_left_offset,
            info.conf_win_right_offset,
            info.conf_win_top_offset,
            info.conf_win_bottom_offset,
            info.coded_width,
            info.coded_height,
            info.display_width,
            info.display_height
        ),
    }
}

// the fields in the order of the struct, the eight flags of the first byte together
fn vp9_fields(p: &Vp9Packet) -> String {
    format!(
        "{}{}{}{}{}{}{}{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        b2s(p.i),
        b2s(p.p),
        b2s(p.l),
        b2s(p.f),
        b2s(p.b),
        b2s(p.e),
        b2s(p.v),
        b2s(p.z),
        p.picture_id,
        p.tid,
        b2s(p.u),
        p.sid,
        b2s(p.d),
        list(&p.p_diff),
        p.tl0picidx,
        p.ns,
        b2s(p.y),
        b2s(p.g),
        p.ng,
        list(&p.width),
        list(&p.height),
        list(&p.pgtid),
        list(&p.pgu),
        list2(&p.pgp_diff),
        hex(&p.payload)
    )
}

fn size_str(sz: VideoSize) -> String {
    format!("{}x{}", sz.width, sz.height)
}

// The packet that every payload is read into is trimmed after an error only, as LiveKit's
// buffer does with the packet it keeps.
fn trim_reuse(p: &mut Vp9Packet) {
    fn trim<T>(v: &mut Vec<T>) -> Vec<T> {
        let mut v = std::mem::take(v);
        v.clear();
        v
    }
    *p = Vp9Packet {
        p_diff: trim(&mut p.p_diff),
        width: trim(&mut p.width),
        height: trim(&mut p.height),
        pgtid: trim(&mut p.pgtid),
        pgu: trim(&mut p.pgu),
        pgp_diff: trim(&mut p.pgp_diff),
        ..Default::default()
    };
}

// The bit reader on the payload without its first byte, which chooses what is read.
fn br_dump(payload: &[u8]) -> String {
    if payload.is_empty() {
        return String::new();
    }
    let mut br = BitReader::new(&payload[1..]);
    let mut sb = String::new();
    match payload[0] % 4 {
        0 => {
            for _ in 0..64 {
                match br.read_ue() {
                    Err(_) => {
                        sb.push('e');
                        break;
                    }
                    Ok(v) => {
                        let _ = write!(sb, "{v},");
                    }
                }
            }
        }
        1 => {
            for _ in 0..64 {
                match br.read_se() {
                    Err(_) => {
                        sb.push('e');
                        break;
                    }
                    Ok(v) => {
                        let _ = write!(sb, "{v},");
                    }
                }
            }
        }
        2 => {
            let ns = [1, 3, 8, 13, 32, 48, 64, 65, 7, 0];
            for i in 0..64 {
                match br.read_bits(ns[i % ns.len()]) {
                    Err(_) => {
                        sb.push('e');
                        break;
                    }
                    Ok(v) => {
                        let _ = write!(sb, "{v},");
                    }
                }
            }
        }
        _ => {
            for _ in 0..16 {
                match br.read_flag() {
                    Err(_) => {
                        sb.push('e');
                        break;
                    }
                    Ok(f) => {
                        let _ = write!(sb, "{},", b2s(f));
                    }
                }
                match br.read_ue() {
                    Err(_) => {
                        sb.push('e');
                        break;
                    }
                    Ok(v) => {
                        let _ = write!(sb, "{v},");
                    }
                }
                match br.read_se() {
                    Err(_) => {
                        sb.push('e');
                        break;
                    }
                    Ok(v) => {
                        let _ = write!(sb, "{v},");
                    }
                }
                match br.read_bits(5) {
                    Err(_) => {
                        sb.push('e');
                        break;
                    }
                    Ok(v) => {
                        let _ = write!(sb, "{v},");
                    }
                }
            }
        }
    }
    sb
}

fn at(payload: &[u8], i: usize) -> u8 {
    if i < payload.len() { payload[i] } else { 0 }
}

// Everything the package says about a payload.
fn dump_line(payload: &[u8], reuse: &mut Vp9Packet) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut add = |name: &str, s: String| parts.push(format!("{name}:{s}"));

    add(
        "kf",
        safe(|| {
            [
                b2s(is_h264_key_frame(payload)),
                b2s(is_h265_key_frame(payload)),
                b2s(is_vp8_key_frame(payload)),
                b2s(is_vp9_key_frame(None, payload)),
                b2s(is_av1_key_frame(payload)),
            ]
            .concat()
        }),
    );
    add(
        "ikf",
        safe(|| {
            [
                b2s(is_key_frame("H264", payload)),
                b2s(is_key_frame("h265", payload)),
                b2s(is_key_frame("VP8", payload)),
                b2s(is_key_frame("vp9", payload)),
                b2s(is_key_frame("Av1", payload)),
                b2s(is_key_frame("opus", payload)),
            ]
            .concat()
        }),
    );
    add(
        "h264sz",
        safe(|| size_str(extract_h264_video_size(payload))),
    );
    add(
        "h265sz",
        safe(|| size_str(extract_h265_video_size(payload))),
    );
    add("sps264", safe(|| sps_str(parse_h264_sps(payload))));
    add("sps265", safe(|| sps_str(parse_h265_sps(payload))));
    add("sc", safe(|| hex(strip_start_code(payload))));
    add(
        "epb",
        safe(|| hex(&remove_emulation_prevention_bytes(payload))),
    );

    let mut v = Vp8::default();
    let vp8_err = v.unmarshal(payload);
    add(
        "vp8",
        safe(|| {
            let e = match &vp8_err {
                Ok(()) => "ok".to_string(),
                Err(err) => err.to_string(),
            };
            format!("{};{}", e, vp8_fields(&v))
        }),
    );
    add(
        "vp8m",
        safe(|| match v.marshal() {
            Err(err) => format!("err:{err}"),
            Ok(buf) => hex(&buf),
        }),
    );
    add(
        "vp8sz",
        safe(|| size_str(extract_vp8_video_size(&v, payload))),
    );

    // A VP8 descriptor made from the bytes of the payload, written to a buffer that may be
    // too small.
    let x = Vp8 {
        first_byte: at(payload, 0),
        s: at(payload, 1) & 1 != 0,
        i: at(payload, 1) & 2 != 0,
        m: at(payload, 1) & 4 != 0,
        l: at(payload, 1) & 8 != 0,
        t: at(payload, 1) & 16 != 0,
        y: at(payload, 1) & 32 != 0,
        k: at(payload, 1) & 64 != 0,
        picture_id: ((at(payload, 2) as u16) << 8) | at(payload, 3) as u16,
        tl0picidx: at(payload, 4),
        tid: at(payload, 5),
        keyidx: at(payload, 6),
        header_size: (at(payload, 7) % 10) as usize,
        ..Default::default()
    };
    add(
        "vp8x",
        safe(|| match x.marshal() {
            Err(err) => format!("err:{err}"),
            Ok(buf) => hex(&buf),
        }),
    );
    add(
        "vp8xt",
        safe(|| {
            let mut buf = vec![0xEE; (at(payload, 8) % 9) as usize];
            match x.marshal_to(&mut buf) {
                Err(err) => format!("err:{err}"),
                Ok(n) => format!("{};{}", n, hex(&buf)),
            }
        }),
    );

    let mut p = Vp9Packet::default();
    let res = p.unmarshal(payload);
    add(
        "vp9",
        safe(|| match &res {
            Err(err) => format!("err:{err}"),
            Ok(raw) => format!("{};{}", vp9_fields(&p), hex(raw)),
        }),
    );
    add(
        "vp9kf",
        safe(|| {
            if res.is_err() || (p.b && p.payload.is_empty()) {
                return "-".to_string();
            }
            b2s(is_vp9_key_frame(Some(&p), payload)).to_string()
        }),
    );
    add(
        "vp9ph",
        safe(|| {
            [
                b2s(p.is_partition_head(payload)),
                b2s(p.is_partition_tail(true, payload)),
                b2s(p.is_partition_tail(false, payload)),
            ]
            .concat()
        }),
    );
    add(
        "vp9r",
        safe(|| match reuse.unmarshal(payload) {
            Err(err) => {
                trim_reuse(reuse);
                format!("err:{err}")
            }
            Ok(_) => vp9_fields(reuse),
        }),
    );
    add("br", safe(|| br_dump(payload)));

    parts.join("|")
}

// Says everything about the payload of each line and holds it against the line. Gives the
// payloads of the lines.
fn compare(reference: &str) -> Vec<&str> {
    let mut reuse = Vp9Packet::default();
    let mut payloads = Vec::new();
    let mut differ = 0;
    for line in reference.lines() {
        let line = line.strip_prefix("REF ").expect("not a line of the output");
        let (payload_hex, want) = line.split_once('\t').expect("a line without a payload");
        let got = dump_line(&unhex(payload_hex), &mut reuse);
        payloads.push(payload_hex);
        if got != want {
            differ += 1;
            // the parts that differ, of the first lines that do
            if differ <= 20 {
                eprintln!("payload {payload_hex}");
                for (want, got) in want.split('|').zip(got.split('|')) {
                    if want != got {
                        eprintln!("    original {want}\n    port     {got}");
                    }
                }
            }
        }
    }
    assert_eq!(0, differ, "of {} payloads", payloads.len());
    payloads
}

#[test]
fn the_port_says_what_the_original_says() {
    let lines = compare(include_str!("testdata/reference_output.txt"));
    // the lines are those of the payloads of testdata/reference_payloads.txt, in its order
    let payloads: Vec<&str> = include_str!("testdata/reference_payloads.txt")
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| if line == "empty" { "" } else { line })
        .collect();
    assert_eq!(payloads, lines);

    if let Ok(path) = std::env::var("MUSIVUM_CODEC_REFERENCE") {
        let text = std::fs::read_to_string(&path).unwrap();
        let lines = compare(&text).len();
        assert!(lines > 0, "no lines in {path}");
        eprintln!("{lines} payloads of {path} compared");
    }
}
