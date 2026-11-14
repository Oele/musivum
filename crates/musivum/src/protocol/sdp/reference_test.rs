//! New for musivum: the port held against the original, line for line.
//!
//! The tests of the original leave out `get_ip` and `get_media_stream_track`, and have of
//! `extract_stream_id`, `get_simulcast_rids` and `extract_dtls_role` one outcome each.
//! `testdata/reference_output.txt` is what the helpers of sdp.go give at the pin for two real
//! descriptions (`testdata/offer.sdp`, `testdata/answer.sdp`: an offer of a browser and the
//! answer of LiveKit's server to it, shortened to three media sections and to the candidates
//! on the loopback interface), for five descriptions made up to reach what those two do not,
//! and for fragments read from text. `testdata/reference_output_test.go` printed it, and says
//! how it is run. The test here asks the port the same, in the same order, and expects the
//! same lines.
//!
//! A description is parsed by pion there and by the WebRTC library here, and where a fragment
//! is patched into a description, the result is marshalled by each: the lines agree on that
//! too.

use std::fmt::Write;

use super::*;
use crate::webrtc::peerconnection::{SdpType, SessionDescription};

fn quoted(list: &[String]) -> String {
    list.iter()
        .map(|s| format!("{s:?}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn err_text<T>(result: &Result<T, Error>) -> String {
    match result {
        Ok(_) => "nil".to_string(),
        Err(err) => err.to_string(),
    }
}

// The two strings Go returns next to an error are empty.
fn pair(result: &Result<(String, String), Error>) -> (String, String) {
    result.clone().unwrap_or_default()
}

fn descriptions() -> Vec<(&'static str, String)> {
    let head = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n";
    vec![
        ("offer", include_str!("testdata/offer.sdp").replace('\n', "\r\n")),
        ("answer", include_str!("testdata/answer.sdp").replace('\n', "\r\n")),
        (
            "nothing",
            format!("{head}m=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:0\r\n"),
        ),
        (
            "properties",
            format!(
                "{head}a=fingerprint\r\na=group\r\nm=audio 9/2 RTP/AVP 0 8 9\r\na=mid\r\na=ice-ufrag:a\r\na=msid\r\na=simulcast\r\na=setup\r\n"
            ),
        ),
        (
            "conflicts",
            format!(
                "{head}a=fingerprint:sha-256 AA\r\na=ice-ufrag:a\r\na=ice-pwd:b\r\na=group:LS 0 1\r\n\
                 m=audio 9 UDP/TLS/RTP/SAVPF 0 8 101\r\nc=IN IP4 127.0.0.1\r\na=mid:0\r\na=fingerprint:sha-256 BB\r\na=ice-ufrag:c\r\na=ice-pwd:b\r\n\
                 a=msid:onlystream\r\na=setup:holdconn\r\na=simulcast:recv q;h\r\na=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16; x=1\r\n\
                 a=rtcp-fb:* nack\r\na=rtcp-fb:101 nack\r\na=rtcp-fb:101 nack pli\r\n\
                 m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\na=setup:passive\r\na=rtpmap:96 VP8/90000\r\n"
            ),
        ),
        (
            "password",
            format!(
                "{head}a=ice-ufrag:a\r\na=ice-pwd:b\r\na=fingerprint:sha-256 AA BB\r\na=ice-lite\r\na=ice-options:trickle\r\na=group:bundle 1 0\r\n\
                 m=audio 9 UDP/TLS/RTP/SAVPF 300 96\r\na=mid:0\r\na=ice-ufrag:a\r\na=ice-pwd:c\r\na=ssrc:1234 msid:stream track\r\na=msid:s t extra\r\na=simulcast:send q;h;f extra\r\n\
                 m=video 9 UDP/TLS/RTP/SAVPF 096 98\r\nc=IN IP6 ::1\r\na=mid:1\r\na=end-of-candidates\r\na=candidate:1 1 udp 1 127.0.0.1 9 typ host\r\na=rtpmap:96 VP8/90000\r\na=rtpmap:98 VP9/90000\r\n"
            ),
        ),
        (
            "payloads",
            "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\na=ice-ufrag:a\r\na=group:BUNDLE audio\r\n\
             m=audio 9 UDP/TLS/RTP/SAVPF 0 96\r\na=ssrc:1234 MSID:stream track\r\na=ssrc:5678 msid:other track2\r\na=simulcast:send q,h;~f\r\na=setup:active\r\n\
             m=video 9 UDP/TLS/RTP/SAVPF 0 8 9\r\na=mid:video\r\na=msid:- track3\r\na=rtpmap:0 other/90000/2\r\na=fmtp:8 x=y\r\na=rtcp-fb:9 nack\r\na=rtcp-fb:9 nack\r\na=rtcp-fb:* nack\r\n\
             m=video 9 UDP/TLS/RTP/SAVPF 96 +97\r\na=mid:more\r\na=rtpmap:96 VP8/90000\r\na=rtpmap:97 rtx/90000\r\n"
                .to_string(),
        ),
    ]
}

fn unmarshal(sdp: &str) -> sdp::SessionDescription {
    SessionDescription {
        r#type: SdpType::Offer,
        sdp: sdp.to_string(),
    }
    .unmarshal()
    .unwrap()
}

fn reference_output() -> String {
    let mut out = String::new();
    let descriptions = descriptions();

    for (name, text) in &descriptions {
        let parsed = unmarshal(text);
        writeln!(
            out,
            "REF {name} marshal_gives_the_text_back: {}",
            parsed.marshal() == *text
        )
        .unwrap();

        let fingerprint = extract_fingerprint(&parsed);
        let (fp, alg) = pair(&fingerprint);
        writeln!(
            out,
            "REF {name} fingerprint: {fp:?} {alg:?} err={}",
            err_text(&fingerprint)
        )
        .unwrap();
        let dtls_role = match extract_dtls_role(&parsed) {
            DtlsRole::Client => "client",
            DtlsRole::Server => "server",
            _ => "other",
        };
        writeln!(out, "REF {name} dtls_role: {dtls_role}").unwrap();
        let credential = extract_ice_credential(&parsed);
        let (ufrag, pwd) = pair(&credential);
        writeln!(
            out,
            "REF {name} ice_credential: {ufrag:?} {pwd:?} err={}",
            err_text(&credential)
        )
        .unwrap();
        writeln!(out, "REF {name} ip: {:?}", get_ip(&parsed)).unwrap();
        let mid = get_bundle_mid(&parsed);
        writeln!(
            out,
            "REF {name} bundle_mid: {:?} {}",
            mid.clone().unwrap_or_default(),
            mid.is_some()
        )
        .unwrap();
        for (i, m) in parsed.media_descriptions.iter().enumerate() {
            writeln!(out, "REF {name} media {i} mid: {:?}", get_mid_value(m)).unwrap();
            let stream_id = extract_stream_id(m);
            writeln!(
                out,
                "REF {name} media {i} stream_id: {:?} {}",
                stream_id.clone().unwrap_or_default(),
                stream_id.is_some()
            )
            .unwrap();
            writeln!(
                out,
                "REF {name} media {i} media_stream_track: {:?}",
                get_media_stream_track(m)
            )
            .unwrap();
            let rids = get_simulcast_rids(m);
            writeln!(
                out,
                "REF {name} media {i} simulcast_rids: [{}] {}",
                quoted(rids.as_deref().unwrap_or_default()),
                rids.is_some()
            )
            .unwrap();
            let codecs = codecs_from_media_description(m);
            writeln!(
                out,
                "REF {name} media {i} codecs: {} err={}",
                codecs.as_ref().map_or(0, Vec::len),
                err_text(&codecs)
            )
            .unwrap();
            for (j, c) in codecs.unwrap_or_default().iter().enumerate() {
                writeln!(
                    out,
                    "REF {name} media {i} codec {j}: {} {:?} {} {:?} {:?} [{}]",
                    c.payload_type,
                    c.name,
                    c.clock_rate,
                    c.encoding_parameters,
                    c.fmtp,
                    quoted(&c.rtcp_feedback)
                )
                .unwrap();
            }
        }

        let frag = extract_sdp_fragment(&parsed);
        writeln!(out, "REF {name} fragment: err={}", err_text(&frag)).unwrap();
        let Ok(frag) = frag else {
            continue;
        };
        let marshalled = frag.marshal();
        let text = marshalled.clone().unwrap_or_default();
        writeln!(
            out,
            "REF {name} fragment marshal: {text:?} err={}",
            err_text(&marshalled)
        )
        .unwrap();
        writeln!(out, "REF {name} fragment mid: {:?}", frag.mid()).unwrap();
        writeln!(
            out,
            "REF {name} fragment candidates: [{}]",
            quoted(frag.candidates())
        )
        .unwrap();
        let credential = frag.extract_ice_credential();
        let (ufrag, pwd) = pair(&credential);
        writeln!(
            out,
            "REF {name} fragment ice_credential: {ufrag:?} {pwd:?} err={}",
            err_text(&credential)
        )
        .unwrap();

        // the fragment read back from its text, and patched into each of the descriptions
        let mut reparsed = SdpFragment::default();
        let result = reparsed.unmarshal(&text);
        writeln!(
            out,
            "REF {name} fragment unmarshal: err={}",
            err_text(&result)
        )
        .unwrap();
        if result.is_err() {
            continue;
        }
        writeln!(
            out,
            "REF {name} fragment unmarshal marshal: {:?}",
            reparsed.marshal().unwrap()
        )
        .unwrap();
        for (into, target) in &descriptions {
            let mut parsed_target = unmarshal(target);
            let result = reparsed.patch_ice_credential_and_candidates_into_sdp(&mut parsed_target);
            writeln!(
                out,
                "REF {name} fragment patch into {into}: err={}",
                err_text(&result)
            )
            .unwrap();
            if result.is_err() {
                continue;
            }
            writeln!(
                out,
                "REF {name} fragment patch into {into} gives: {:?}",
                parsed_target.marshal()
            )
            .unwrap();
        }
    }

    // fragments as text
    for (i, text) in [
        "a=ice-ufrag:u\r\na=ice-pwd:p\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:0\r\na=ice-ufrag:v\r\n",
        "a=ice-ufrag:u\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=ice-pwd:p\r\na=ice-pwd:q\r\na=ice-lite\r\na=end-of-candidates\r\na=candidate:\r\na=candidate:1 1 udp 1 127.0.0.1 9 typ host   \r\n",
        "a=ice-pwd:p\r\nm=x\r\n",
        "m=audio 9 UDP/TLS/RTP/SAVPF 0\na=mid:0 \r\r\n\n  \nc=IN IP4 127.0.0.1\na=group:bundle 0 1\na=ice-options:trickle\nb=AS:1\na=mid\na=end-of-candidates:x\n",
        "a=group:BUNDLE\r\na=group:LS 1\r\na=mid:5\r\na=candidate:lost\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=ice-ufrag:u\r\na=ice-pwd:p\r\na=ice-ufrag:\r\n",
        "a=group:BUNDLE 1\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\n",
        "m=é\r\na=mid:é\r\na=ice-ufrag:é:è\r\na=ice-pwd:p\r\n",
        "é=1\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=mid:0\r\n",
        "m=audio 9 UDP/TLS/RTP/SAVPF 0\r\naé\r\n",
        "mé=audio\r\n",
    ]
    .into_iter()
    .enumerate()
    {
        let mut frag = SdpFragment::default();
        let result = frag.unmarshal(text);
        writeln!(out, "REF text {i} unmarshal: err={}", err_text(&result)).unwrap();
        if result.is_err() {
            continue;
        }
        writeln!(out, "REF text {i} marshal: {:?}", frag.marshal().unwrap()).unwrap();
        writeln!(out, "REF text {i} mid: {:?}", frag.mid()).unwrap();
        writeln!(
            out,
            "REF text {i} candidates: [{}]",
            quoted(frag.candidates())
        )
        .unwrap();
        let credential = frag.extract_ice_credential();
        let (ufrag, pwd) = pair(&credential);
        writeln!(
            out,
            "REF text {i} ice_credential: {ufrag:?} {pwd:?} err={}",
            err_text(&credential)
        )
        .unwrap();
    }

    out
}

#[test]
fn the_helpers_give_what_the_original_gives() {
    let actual = reference_output();
    let mut actual = actual.lines();
    for (n, expected) in include_str!("testdata/reference_output.txt")
        .lines()
        .enumerate()
    {
        assert_eq!(Some(expected), actual.next(), "line {}", n + 1);
    }
    assert_eq!(None, actual.next(), "more lines than the original printed");
}
