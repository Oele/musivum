// Ported from LiveKit's protocol, sdp/sdp_test.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the fuzz test runs its seeds, as `go test` does when
// it is not asked to fuzz.

use super::*;
use crate::webrtc::peerconnection::{SdpType, SessionDescription};

fn strings(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_sdp_util_functions() {
    let sdp = "v=0\r\no=- 4648475892259889561 3 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0 1\r\na=ice-ufrag:1hhfzwf0ijpzm\r\na=ice-pwd:jm5puo2ab1op3vs59ca53bdk7s\r\na=fingerprint:sha-256 40:42:FB:47:87:52:BF:CB:EC:3A:DF:EB:06:DA:2D:B7:2F:59:42:10:23:7B:9D:4C:C9:58:DD:FF:A2:8F:17:67\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\nc=IN IP4 0.0.0.0\r\na=rtcp:9 IN IP4 0.0.0.0\r\na=setup:passive\r\na=mid:0\r\na=sendonly\r\na=rtcp-mux\r\na=rtpmap:96 H264/90000\r\na=rtcp-fb:96 nack\r\na=rtcp-fb:96 goog-remb\r\na=fmtp:96 packetization-mode=1;profile-level-id=42e01f\r\na=ssrc:1505338584 cname:10000000b5810aac\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=rtcp:9 IN IP4 0.0.0.0\r\na=setup:passive\r\na=mid:1\r\na=sendonly\r\na=rtcp-mux\r\na=rtpmap:111 opus/48000/2\r\na=ssrc:697641945 cname:10000000b5810aac\r\n";
    let sd = SessionDescription {
        r#type: SdpType::Offer,
        sdp: sdp.to_string(),
    };
    let parsed = sd.unmarshal().unwrap();

    assert_eq!("0", get_mid_value(&parsed.media_descriptions[0]));
    assert_eq!("1", get_mid_value(&parsed.media_descriptions[1]));

    let (fp, alg) = extract_fingerprint(&parsed).unwrap();
    assert_eq!(
        "40:42:FB:47:87:52:BF:CB:EC:3A:DF:EB:06:DA:2D:B7:2F:59:42:10:23:7B:9D:4C:C9:58:DD:FF:A2:8F:17:67",
        fp
    );
    assert_eq!("sha-256", alg);

    let dtls_role = extract_dtls_role(&parsed);
    assert_eq!(DtlsRole::Server, dtls_role);

    let (ufrag, pwd) = extract_ice_credential(&parsed).unwrap();
    assert_eq!("1hhfzwf0ijpzm", ufrag);
    assert_eq!("jm5puo2ab1op3vs59ca53bdk7s", pwd);

    // not found, and so no stream id
    let stream_id = extract_stream_id(&parsed.media_descriptions[0]);
    assert_eq!(None, stream_id);

    let rids = get_simulcast_rids(&parsed.media_descriptions[1]);
    assert!(rids.is_none());

    let codecs = codecs_from_media_description(&parsed.media_descriptions[0]).unwrap();
    let expected_codec = sdp::Codec {
        payload_type: 96,
        name: "H264".to_string(),
        clock_rate: 90000,
        fmtp: "packetization-mode=1;profile-level-id=42e01f".to_string(),
        rtcp_feedback: strings(&["nack", "goog-remb"]),
        ..Default::default()
    };
    assert_eq!(expected_codec, codecs[0]);

    let bundle_mid = get_bundle_mid(&parsed);
    assert_eq!(Some("0".to_string()), bundle_mid);
}

#[test]
fn test_sdp_fragment() {
    let fragment = "a=ice-lite\r\na=ice-options:trickle ice2\r\na=group:BUNDLE 0 1\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=ice-ufrag:ysXw\r\na=ice-pwd:vw5LmwG4y/e6dPP/zAP9Gp5k\r\na=candidate:1387637174 1 udp 2122260223 192.0.2.1 61764 typ host generation 0 ufrag EsAw network-id 1\r\na=candidate:3471623853 1 udp 2122194687 198.51.100.2 61765 typ host generation 0 ufrag EsAw network-id 2\r\na=candidate:473322822 1 tcp 1518280447 192.0.2.1 9 typ host tcptype active generation 0 ufrag EsAw network-id 1\r\na=candidate:2154773085 1 tcp 1518214911 198.51.100.2 9 typ host tcptype active generation 0 ufrag EsAw network-id 2\r\na=candidate:393455558 0 tcp 1518283007 [2401:4900:633c:959f:2037:680c:7c40:b3db] 9 typ host tcptype active\r\n";

    let lite = true;
    let expected_sdp_fragment = SdpFragment {
        group: "BUNDLE 0 1".to_string(),
        ice: Some(SdpFragmentIce {
            options: "trickle ice2".to_string(),
            lite: Some(lite),
            ..Default::default()
        }),
        media: Some(SdpFragmentMedia {
            info: "audio 9 UDP/TLS/RTP/SAVPF 111".to_string(),
            mid: "0".to_string(),
            ice: Some(SdpFragmentIce {
                ufrag: "ysXw".to_string(),
                pwd: "vw5LmwG4y/e6dPP/zAP9Gp5k".to_string(),
                ..Default::default()
            }),
            candidates: strings(&[
                "1387637174 1 udp 2122260223 192.0.2.1 61764 typ host generation 0 ufrag EsAw network-id 1",
                "3471623853 1 udp 2122194687 198.51.100.2 61765 typ host generation 0 ufrag EsAw network-id 2",
                "473322822 1 tcp 1518280447 192.0.2.1 9 typ host tcptype active generation 0 ufrag EsAw network-id 1",
                "2154773085 1 tcp 1518214911 198.51.100.2 9 typ host tcptype active generation 0 ufrag EsAw network-id 2",
                "393455558 0 tcp 1518283007 [2401:4900:633c:959f:2037:680c:7c40:b3db] 9 typ host tcptype active",
            ]),
            ..Default::default()
        }),
    };

    let mut sdp_fragment = SdpFragment::default();
    sdp_fragment.unmarshal(fragment).unwrap();
    assert_eq!(expected_sdp_fragment, sdp_fragment);

    let expected_marshalled = "a=group:BUNDLE 0 1\r\na=ice-lite\r\na=ice-options:trickle ice2\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=ice-ufrag:ysXw\r\na=ice-pwd:vw5LmwG4y/e6dPP/zAP9Gp5k\r\na=candidate:1387637174 1 udp 2122260223 192.0.2.1 61764 typ host generation 0 ufrag EsAw network-id 1\r\na=candidate:3471623853 1 udp 2122194687 198.51.100.2 61765 typ host generation 0 ufrag EsAw network-id 2\r\na=candidate:473322822 1 tcp 1518280447 192.0.2.1 9 typ host tcptype active generation 0 ufrag EsAw network-id 1\r\na=candidate:2154773085 1 tcp 1518214911 198.51.100.2 9 typ host tcptype active generation 0 ufrag EsAw network-id 2\r\na=candidate:393455558 0 tcp 1518283007 [2401:4900:633c:959f:2037:680c:7c40:b3db] 9 typ host tcptype active\r\n";
    let marshalled = sdp_fragment.marshal().unwrap();
    assert_eq!(expected_marshalled, marshalled);

    let expected_candidates = strings(&[
        "1387637174 1 udp 2122260223 192.0.2.1 61764 typ host generation 0 ufrag EsAw network-id 1",
        "3471623853 1 udp 2122194687 198.51.100.2 61765 typ host generation 0 ufrag EsAw network-id 2",
        "473322822 1 tcp 1518280447 192.0.2.1 9 typ host tcptype active generation 0 ufrag EsAw network-id 1",
        "2154773085 1 tcp 1518214911 198.51.100.2 9 typ host tcptype active generation 0 ufrag EsAw network-id 2",
        "393455558 0 tcp 1518283007 [2401:4900:633c:959f:2037:680c:7c40:b3db] 9 typ host tcptype active",
    ]);

    let mut sdp_fragment1 = SdpFragment::default();
    sdp_fragment1.unmarshal(&marshalled).unwrap();
    assert_eq!(expected_sdp_fragment, sdp_fragment1);
    assert_eq!("0", sdp_fragment1.mid());
    assert_eq!(expected_candidates, sdp_fragment1.candidates());
    let (ufrag, pwd) = sdp_fragment1.extract_ice_credential().unwrap();
    assert_eq!("ysXw", ufrag);
    assert_eq!("vw5LmwG4y/e6dPP/zAP9Gp5k", pwd);

    let mismatched_mid_fragment = "a=ice-options:trickle ice2\r\na=group:BUNDLE 0 1\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:1\r\na=ice-ufrag:ysXw\r\na=ice-pwd:vw5LmwG4y/e6dPP/zAP9Gp5k\r\na=candidate:1387637174 1 udp 2122260223 192.0.2.1 61764 typ host generation 0 ufrag EsAw network-id 1\r\na=candidate:3471623853 1 udp 2122194687 198.51.100.2 61765 typ host generation 0 ufrag EsAw network-id 2\r\na=candidate:473322822 1 tcp 1518280447 192.0.2.1 9 typ host tcptype active generation 0 ufrag EsAw network-id 1\r\na=candidate:2154773085 1 tcp 1518214911 198.51.100.2 9 typ host tcptype active generation 0 ufrag EsAw network-id 2\r\n";
    let mut sdp_fragment2 = SdpFragment::default();
    assert!(sdp_fragment2.unmarshal(mismatched_mid_fragment).is_err());

    let sdp = "v=0\r\no=- 4648475892259889561 3 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0 1\r\na=ice-options:trickle ice2\r\na=fingerprint:sha-256 40:42:FB:47:87:52:BF:CB:EC:3A:DF:EB:06:DA:2D:B7:2F:59:42:10:23:7B:9D:4C:C9:58:DD:FF:A2:8F:17:67\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\nc=IN IP4 0.0.0.0\r\na=ice-ufrag:1hhfzwf0ijpzm\r\na=ice-pwd:jm5puo2ab1op3vs59ca53bdk7s\r\na=rtcp:9 IN IP4 0.0.0.0\r\na=setup:passive\r\na=mid:0\r\na=sendonly\r\na=rtcp-mux\r\na=rtpmap:96 H264/90000\r\na=rtcp-fb:96 nack\r\na=rtcp-fb:96 goog-remb\r\na=fmtp:96 packetization-mode=1;profile-level-id=42e01f\r\na=ssrc:1505338584 cname:10000000b5810aac\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=rtcp:9 IN IP4 0.0.0.0\r\na=setup:passive\r\na=mid:1\r\na=sendonly\r\na=rtcp-mux\r\na=rtpmap:111 opus/48000/2\r\na=ssrc:697641945 cname:10000000b5810aac\r\n";
    let sd = SessionDescription {
        r#type: SdpType::Offer,
        sdp: sdp.to_string(),
    };
    let mut parsed = sd.unmarshal().unwrap();
    sdp_fragment1
        .patch_ice_credential_and_candidates_into_sdp(&mut parsed)
        .unwrap();

    let (ufrag, pwd) = extract_ice_credential(&parsed).unwrap();
    assert_eq!("ysXw", ufrag);
    assert_eq!("vw5LmwG4y/e6dPP/zAP9Gp5k", pwd);

    let mut candidates: Vec<String> = Vec::new();
    for a in &parsed.media_descriptions[0].attributes {
        if a.is_ice_candidate() {
            candidates.push(a.value.clone().unwrap_or_default());
        }
    }
    assert_eq!(expected_candidates, candidates);

    let sdp_fragment3 = extract_sdp_fragment(&parsed).unwrap();

    let expected_sdp_fragment3 = SdpFragment {
        group: "BUNDLE 0 1".to_string(),
        ice: Some(SdpFragmentIce {
            options: "trickle ice2".to_string(),
            ..Default::default()
        }),
        media: Some(SdpFragmentMedia {
            info: "video 9 UDP/TLS/RTP/SAVPF 96".to_string(),
            mid: "0".to_string(),
            ice: Some(SdpFragmentIce {
                ufrag: "ysXw".to_string(),
                pwd: "vw5LmwG4y/e6dPP/zAP9Gp5k".to_string(),
                ..Default::default()
            }),
            candidates: strings(&[
                "1387637174 1 udp 2122260223 192.0.2.1 61764 typ host generation 0 ufrag EsAw network-id 1",
                "3471623853 1 udp 2122194687 198.51.100.2 61765 typ host generation 0 ufrag EsAw network-id 2",
                "473322822 1 tcp 1518280447 192.0.2.1 9 typ host tcptype active generation 0 ufrag EsAw network-id 1",
                "2154773085 1 tcp 1518214911 198.51.100.2 9 typ host tcptype active generation 0 ufrag EsAw network-id 2",
                "393455558 0 tcp 1518283007 [2401:4900:633c:959f:2037:680c:7c40:b3db] 9 typ host tcptype active",
            ]),
            ..Default::default()
        }),
    };
    assert_eq!(expected_sdp_fragment3, sdp_fragment3);

    let marshalled_sdp_fragment3 = sdp_fragment3.marshal().unwrap();
    let expected_marshalled_sdp_fragment3 = "a=group:BUNDLE 0 1\r\na=ice-options:trickle ice2\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=ice-ufrag:ysXw\r\na=ice-pwd:vw5LmwG4y/e6dPP/zAP9Gp5k\r\na=candidate:1387637174 1 udp 2122260223 192.0.2.1 61764 typ host generation 0 ufrag EsAw network-id 1\r\na=candidate:3471623853 1 udp 2122194687 198.51.100.2 61765 typ host generation 0 ufrag EsAw network-id 2\r\na=candidate:473322822 1 tcp 1518280447 192.0.2.1 9 typ host tcptype active generation 0 ufrag EsAw network-id 1\r\na=candidate:2154773085 1 tcp 1518214911 198.51.100.2 9 typ host tcptype active generation 0 ufrag EsAw network-id 2\r\na=candidate:393455558 0 tcp 1518283007 [2401:4900:633c:959f:2037:680c:7c40:b3db] 9 typ host tcptype active\r\n";
    assert_eq!(expected_marshalled_sdp_fragment3, marshalled_sdp_fragment3);
}

#[test]
fn test_sdp_fragment_unmarshal_malformed() {
    // malformed fragments should error out, not panic
    let malformed = [
        "",
        "m",
        "a",
        "m\r\n",
        "a\r\n",
        "m=",
        "a=",
        "mx=audio 9 UDP/TLS/RTP/SAVPF 111",
        "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na",
        "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\nax\r\n",
        "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
        "a=ice-lite\r\na=ice-ufrag:ysXw\r\n",
    ];
    for frag in malformed {
        let mut sdp_fragment = SdpFragment::default();
        assert!(sdp_fragment.unmarshal(frag).is_err(), "fragment: {frag:?}");
    }
}

#[test]
fn fuzz_sdp_fragment_unmarshal() {
    let seeds = [
        "a=group:BUNDLE 0\r\na=ice-lite\r\na=ice-options:trickle ice2\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=ice-ufrag:ysXw\r\na=ice-pwd:vw5LmwG4y/e6dPP/zAP9Gp5k\r\na=candidate:1387637174 1 udp 2122260223 192.0.2.1 61764 typ host\r\na=end-of-candidates\r\n",
        "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\n",
        "m",
        "a",
        "a=",
        "m=\na",
    ];

    for frag in seeds {
        let mut sdp_fragment = SdpFragment::default();
        if sdp_fragment.unmarshal(frag).is_err() {
            continue;
        }

        // accessors and marshalling of anything that parsed should be safe too
        sdp_fragment.mid();
        sdp_fragment.candidates();
        let _ = sdp_fragment.extract_ice_credential();
        let marshalled = match sdp_fragment.marshal() {
            Ok(marshalled) => marshalled,
            Err(err) => panic!("marshal of parsed fragment {frag:?} failed: {err}"),
        };

        // marshalling a parsed fragment should produce something that parses back the same
        let mut reparsed = SdpFragment::default();
        if let Err(err) = reparsed.unmarshal(&marshalled) {
            panic!("re-unmarshal of {marshalled:?} (from {frag:?}) failed: {err}");
        }
        assert_eq!(sdp_fragment, reparsed, "fragment: {frag:?}");
    }
}
