//! New for musivum: the port held against the original, line for line. The original has no
//! tests.
//!
//! `testdata/reference_output.txt` is what the helpers of utils.go give at the pin: for the
//! two real descriptions of `../sdp/testdata` as messages of the protocol and back, for every
//! type of description, for candidates as JSON, for JSON that Go's decoder takes or refuses
//! as a candidate, and for ICE servers. `testdata/reference_output_test.go` printed it, and
//! says how it is run. The test here asks the port the same, in the same order, and expects
//! the same lines.

use std::fmt::Write;

use super::*;

// A string in quotes, written the same way on the Go side: all but printable ASCII as the
// number of the character.
fn quote(s: &str) -> String {
    let mut b = String::from("\"");
    for r in s.chars() {
        match r {
            '"' | '\\' => {
                b.push('\\');
                b.push(r);
            }
            ' '..='~' => b.push(r),
            _ => write!(b, "\\u{{{:x}}}", r as u32).unwrap(),
        }
    }
    b.push('"');
    b
}

fn optional(s: &Option<String>) -> String {
    match s {
        None => "nil".to_string(),
        Some(s) => quote(s),
    }
}

fn map(m: &HashMap<String, String>) -> String {
    let mut pairs: Vec<String> = m.iter().map(|(k, v)| format!("{k}={v}")).collect();
    pairs.sort();
    format!("[{}]", pairs.join(" "))
}

// pion's `SDPType.String()`, written out here once more so that the test does not ask the
// code it tests.
fn type_name(t: SdpType) -> &'static str {
    match t {
        SdpType::Offer => "offer",
        SdpType::Pranswer => "pranswer",
        SdpType::Answer => "answer",
        SdpType::Rollback => "rollback",
        _ => "unknown",
    }
}

fn reference_output() -> String {
    let mut out = String::new();

    for (name, text, typ) in [
        (
            "offer",
            include_str!("../sdp/testdata/offer.sdp"),
            SdpType::Offer,
        ),
        (
            "answer",
            include_str!("../sdp/testdata/answer.sdp"),
            SdpType::Answer,
        ),
    ] {
        let text = text.replace('\n', "\r\n");
        let sd = SessionDescription {
            r#type: typ,
            sdp: text.clone(),
        };
        let mid_to_track_id = HashMap::from([
            ("0".to_string(), "TR_a".to_string()),
            ("7".to_string(), "TR_b".to_string()),
        ]);
        let p = to_proto_session_description(sd, 7, mid_to_track_id).unwrap();
        writeln!(
            out,
            "REF {name} to_proto: type={} sdp_len={} sdp_equal={} id={} mid_to_track_id={}",
            quote(&p.r#type),
            p.sdp.len(),
            p.sdp == text,
            p.id,
            map(&p.mid_to_track_id)
        )
        .unwrap();
        let (back, id, m) = from_proto_session_description(&p);
        writeln!(
            out,
            "REF {name} from_proto: type={} sdp_equal={} id={id} mid_to_track_id={}",
            quote(type_name(back.r#type)),
            back.sdp == text,
            map(&m)
        )
        .unwrap();
    }
    let empty = SessionDescription {
        r#type: SdpType::Offer,
        sdp: String::new(),
    };
    let mid_to_track_id = HashMap::from([("0".to_string(), "TR_a".to_string())]);
    writeln!(
        out,
        "REF to_proto empty: nil={}",
        to_proto_session_description(empty, 1, mid_to_track_id).is_none()
    )
    .unwrap();
    for (i, typ) in [
        SdpType::Offer,
        SdpType::Pranswer,
        SdpType::Answer,
        SdpType::Rollback,
        SdpType::Unspecified,
    ]
    .into_iter()
    .enumerate()
    {
        let sd = SessionDescription {
            r#type: typ,
            sdp: "x".to_string(),
        };
        let p = to_proto_session_description(sd, 0, HashMap::new()).unwrap();
        let (back, id, m) = from_proto_session_description(&p);
        assert_eq!(typ, back.r#type);
        writeln!(
            out,
            "REF type {i}: proto={} back={} id={id} mid_to_track_id={}",
            quote(&p.r#type),
            quote(type_name(back.r#type)),
            map(&m)
        )
        .unwrap();
    }
    for typ in ["OFFER", "", "unknown", "Unspecified", "answer "] {
        let (back, _, _) = from_proto_session_description(&livekit::SessionDescription {
            r#type: typ.to_string(),
            sdp: "x".to_string(),
            ..Default::default()
        });
        writeln!(
            out,
            "REF type from {}: back={}",
            quote(typ),
            quote(type_name(back.r#type))
        )
        .unwrap();
    }

    for (i, (init, target, r#final)) in [
        (
            IceCandidateInit {
                candidate: "candidate:2353504611 1 udp 2122194687 127.0.0.1 50071 typ host generation 0 ufrag wkc6 network-id 4 network-cost 50".to_string(),
                sdp_mid: Some("0".to_string()),
                sdp_mline_index: Some(0),
                username_fragment: Some("wkc6".to_string()),
                // what the library's type has more is not written
                url: Some("stun:127.0.0.1:3478".to_string()),
            },
            livekit::SignalTarget::Subscriber,
            true,
        ),
        (
            IceCandidateInit {
                candidate: "candidate:1 1 udp 1 127.0.0.1 9 typ host".to_string(),
                ..Default::default()
            },
            livekit::SignalTarget::Publisher,
            false,
        ),
        (
            IceCandidateInit::default(),
            livekit::SignalTarget::Publisher,
            true,
        ),
        (
            IceCandidateInit {
                candidate: "<a&b>\u{2028}\u{2029}\"\\\u{1}\u{8}\u{c}\n\r\t\u{7f}\u{e9}\u{1F600}/"
                    .to_string(),
                sdp_mid: Some(String::new()),
                sdp_mline_index: Some(65535),
                username_fragment: Some(String::new()),
                url: None,
            },
            livekit::SignalTarget::Subscriber,
            false,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let tr = to_proto_trickle(&init, target, r#final);
        writeln!(
            out,
            "REF to_proto_trickle {i}: {} target={} final={}",
            tr.candidate_init, tr.target, tr.r#final
        )
        .unwrap();
        let back = from_proto_trickle(&tr);
        let equal = back.as_ref().is_ok_and(|back| {
            back.candidate == init.candidate
                && back.sdp_mid == init.sdp_mid
                && back.sdp_mline_index == init.sdp_mline_index
                && back.username_fragment == init.username_fragment
        });
        writeln!(
            out,
            "REF to_proto_trickle {i} and back: equal={equal} err={}",
            back.is_err()
        )
        .unwrap();
    }

    for (i, input) in [
        r#"{"candidate":"candidate:2353504611 1 udp 2122194687 127.0.0.1 50071 typ host generation 0 ufrag wkc6 network-id 4 network-cost 50","sdpMid":"0","sdpMLineIndex":3,"usernameFragment":"wkc6"}"#,
        r#"{"candidate":"candidate:1 1 udp 1 127.0.0.1 9 typ host","sdpMid":null,"sdpMLineIndex":null,"usernameFragment":null}"#,
        r#"{"candidate":"candidate:1 1 udp 1 127.0.0.1 9 typ host"}"#,
        r#"{"sdpMid":"1"}"#,
        r#"{"candidate":null,"sdpMLineIndex":0}"#,
        r#"{"candidate":"c","unknown":[1,{"candidate":"inner"}],"sdpMid":"a"}"#,
        r#"{}"#,
        r#"null"#,
        r#""#,
        r#"{"candidate":5}"#,
        r#"{"candidate":"c","sdpMLineIndex":65535}"#,
        r#"{"candidate":"c","sdpMLineIndex":65536}"#,
        r#"{"candidate":"c","sdpMLineIndex":-1}"#,
        r#"{"candidate":"c","sdpMLineIndex":1.5}"#,
        r#"{"candidate":"c","sdpMLineIndex":3.0}"#,
        r#"{"candidate":"c","sdpMLineIndex":1e1}"#,
        r#"{"candidate":"c","sdpMLineIndex":"1"}"#,
        r#"{"candidate":"c","sdpMid":7}"#,
        r#"{"candidate":"c","usernameFragment":{}}"#,
        r#"[1]"#,
        r#""text""#,
        r#"true"#,
        r#"5"#,
        r#"{"candidate":"c""#,
        r#"{"candidate":"c"} x"#,
        " \n{ \"candidate\" : \"c\" }\t ",
        r#"{"Candidate":"c","SDPMID":"x","sdpmlineindex":2,"USERNAMEFRAGMENT":"u"}"#,
        r#"{"candidate":"first","candidate":"second"}"#,
        r#"{"candidate":"exact","CANDIDATE":"folded"}"#,
        r#"{"CANDIDATE":"folded","candidate":"exact"}"#,
        r#"{"candidate":"kept","candidate":null}"#,
        r#"{"sdpMid":"a","sdpMid":null,"sdpMLineIndex":1,"sdpMLineIndex":null}"#,
        r#"{"candidate":"aé\n\"\\\/ 😀"}"#,
        r#"{"candidate":"c","url":"stun:127.0.0.1:3478"}"#,
        r#"{"candidate":"c","sdpMid":"0","sdpMLineIndex":true}"#,
    ]
    .into_iter()
    .enumerate()
    {
        let result = from_proto_trickle(&livekit::TrickleRequest {
            candidate_init: input.to_string(),
            ..Default::default()
        });
        // Go returns a candidate with nothing set next to an error.
        let err = result.is_err();
        let ci = result.unwrap_or_default();
        // The library's type has a field that pion's has not: it is never read.
        assert_eq!(None, ci.url, "input {i}");
        writeln!(
            out,
            "REF from_proto_trickle {i}: candidate={} mid={} index={} ufrag={} err={err}",
            quote(&ci.candidate),
            optional(&ci.sdp_mid),
            ci.sdp_mline_index
                .map_or("nil".to_string(), |index| index.to_string()),
            optional(&ci.username_fragment),
        )
        .unwrap();
    }

    writeln!(
        out,
        "REF from_proto_ice_servers none: len={}",
        from_proto_ice_servers(&[]).len()
    )
    .unwrap();
    for (i, s) in from_proto_ice_servers(&[
        livekit::IceServer {
            urls: vec!["stun:127.0.0.1:3478".to_string()],
            ..Default::default()
        },
        livekit::IceServer {
            urls: vec![
                "turn:127.0.0.1:3478?transport=udp".to_string(),
                "turns:127.0.0.1:5349?transport=tcp".to_string(),
            ],
            username: "user".to_string(),
            credential: "secret".to_string(),
        },
        livekit::IceServer::default(),
    ])
    .iter()
    .enumerate()
    {
        let urls: Vec<String> = s.urls.iter().map(|u| quote(u)).collect();
        writeln!(
            out,
            "REF from_proto_ice_servers {i}: urls=[{}] username={} credential={}",
            urls.join(" "),
            quote(&s.username),
            quote(&s.credential)
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
