// Ported from LiveKit, pkg/rtc/clientinfo_test.go at v1.13.11.
// Copyright 2022 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the version that the original sets through the
// embedded pointer is set through `Arc::make_mut`. The two tests at the end are new.

use super::*;

#[test]
fn test_client_info_compare_version() {
    let c = ClientInfo {
        client_info: Some(Arc::new(livekit::ClientInfo {
            version: "1".into(),
            ..Default::default()
        })),
    };
    assert_eq!(1, c.compare_version("0.1.0"));
    assert_eq!(0, c.compare_version("1.0.0"));
    assert_eq!(-1, c.compare_version("1.0.5"));
}

#[test]
fn test_client_info_supports_ice_tcp() {
    {
        let name = "GO SDK cannot support TCP";
        let c = ClientInfo {
            client_info: Some(Arc::new(livekit::ClientInfo {
                sdk: Sdk::Go as i32,
                ..Default::default()
            })),
        };
        assert!(!c.supports_ice_tcp(), "{name}");
    }

    {
        let name = "Swift SDK cannot support TCP before 1.0.5";
        let mut c = ClientInfo {
            client_info: Some(Arc::new(livekit::ClientInfo {
                sdk: Sdk::Swift as i32,
                version: "1.0.4".into(),
                ..Default::default()
            })),
        };
        assert!(!c.supports_ice_tcp(), "{name}");
        Arc::make_mut(c.client_info.as_mut().unwrap()).version = "1.0.5".into();
        assert!(c.supports_ice_tcp(), "{name}");
    }
}

// New for musivum: versions the original's test does not have, among them ones that are not
// numbers. The expected values are what the original gives (printed by a Go test at the pin).
#[test]
fn compare_version_gives_what_the_original_gives() {
    let table: [(&str, &str, i64); 30] = [
        ("1", "0.1.0", 1),
        ("1", "1.0.0", 0),
        ("1", "1.0.5", -1),
        ("", "1.0.0", -1),
        ("", "0.0.0", 0),
        ("1.6.3", "1.6.3", 0),
        ("1.6.2", "1.6.3", -1),
        ("1.6.10", "1.6.3", 1),
        ("1.6.3-rc1", "1.6.3", -1),
        ("1.6.4-rc1", "1.6.3", -1),
        ("v1.6.3", "1.6.3", -1),
        ("2", "1.99.99", 1),
        ("1.2.3.4", "1.2.3", 0),
        ("1.2.3", "1.2.3.4", 0),
        ("+1.+2.+3", "1.2.3", 0),
        ("-1.0.0", "0.0.0", -1),
        ("1..3", "1.0.3", 0),
        ("99999999999999999999.0.0", "1.0.0", 1),
        ("-99999999999999999999.0.0", "-1.0.0", -1),
        (" 1.2.3", "1.2.3", -1),
        ("1.2.3 ", "1.2.3", -1),
        ("1_0.0.0", "1.0.0", -1),
        ("0x10.0.0", "1.0.0", -1),
        ("１.0.0", "1.0.0", -1),
        ("2.4.0", "2.4.0", 0),
        ("2.3.9", "2.4.0", -1),
        ("10.0.0", "9.0.0", 1),
        ("+.0.0", "0.0.0", 0),
        ("-.0.0", "0.0.0", 0),
        ("-0.0.0", "0.0.0", 0),
    ];
    for (client_version, version, expected) in table {
        let c = ClientInfo {
            client_info: Some(Arc::new(livekit::ClientInfo {
                version: client_version.into(),
                ..Default::default()
            })),
        };
        assert_eq!(
            expected,
            c.compare_version(version),
            "{client_version:?} against {version:?}"
        );
    }
    assert_eq!(-1, ClientInfo::default().compare_version("0.0.0"));
}

// New for musivum: what each kind of client is taken to support. Each expected line is what
// the original prints for the same client (a Go test at the pin that formats the results of
// the methods in this order); "reconnect=panic" is the nil pointer of the original's
// `CanHandleReconnectResponse`.
#[test]
fn clients_support_what_the_original_says() {
    fn info(sdk: Sdk, version: &str, browser: &str, os: &str) -> livekit::ClientInfo {
        livekit::ClientInfo {
            sdk: sdk as i32,
            version: version.into(),
            browser: browser.into(),
            os: os.into(),
            ..Default::default()
        }
    }

    let table: [(Option<livekit::ClientInfo>, &str); 17] = [
        (
            None,
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=false reconnect=panic icetcp=false encactive=true codecorder=true tracksub=true reqresp=true sctpzero=false reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(livekit::ClientInfo::default()),
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=false reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=false reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Js, "1.6.2", "Firefox", "Linux")),
            "ff=true safari=false go=false linux=true android=false obs=false | red=false prflx=false fire=false codecchange=true reconnect=false icetcp=true encactive=false codecorder=false tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Js, "1.6.3", "firefox mobile", "Android")),
            "ff=true safari=false go=false linux=false android=true obs=false | red=false prflx=false fire=false codecchange=true reconnect=true icetcp=true encactive=false codecorder=false tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Js, "2.0.0", "Safari", "macOS")),
            "ff=false safari=true go=false linux=false android=false obs=false | red=false prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=false cap1=false cap2=false trailer=false",
        ),
        (
            // 'ſ' folds to 's'
            Some(info(Sdk::Js, "2.0.0", "\u{17f}afari", "iOS")),
            "ff=false safari=true go=false linux=false android=false obs=false | red=false prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=false cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Js, "2.0.0", "Chrome", "Windows")),
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Go, "2.3.9", "", "")),
            "ff=false safari=false go=true linux=false android=false obs=false | red=true prflx=true fire=true codecchange=false reconnect=true icetcp=false encactive=true codecorder=true tracksub=true reqresp=true sctpzero=false reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Go, "2.4.0", "", "")),
            "ff=false safari=false go=true linux=false android=false obs=false | red=true prflx=true fire=true codecchange=false reconnect=true icetcp=false encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Swift, "1.0.4", "", "")),
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=true reconnect=true icetcp=false encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(info(Sdk::Swift, "1.0.5", "", "")),
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(livekit::ClientInfo {
                protocol: 9,
                ..info(Sdk::Rust, "", "", "")
            }),
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=false reqresp=false sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(livekit::ClientInfo {
                protocol: 10,
                ..info(Sdk::Rust, "", "", "")
            }),
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            // an SDK this version of the protocol does not name is not UNKNOWN
            Some(livekit::ClientInfo {
                sdk: 99,
                version: "1.0.0".into(),
                ..Default::default()
            }),
            "ff=false safari=false go=false linux=false android=false obs=false | red=true prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
        (
            Some(livekit::ClientInfo {
                capabilities: vec![Capability::CapPacketTrailer as i32],
                ..info(Sdk::Android, "", "OBS Studio", "")
            }),
            "ff=false safari=false go=false linux=false android=false obs=true | red=true prflx=true fire=false codecchange=true reconnect=true icetcp=true encactive=true codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=true cap2=false trailer=true",
        ),
        (
            Some(livekit::ClientInfo {
                capabilities: vec![Capability::CapCompressionDeflateRaw as i32],
                ..info(Sdk::Js, "2.0.0", "FIREFOX", "LINUX")
            }),
            "ff=true safari=false go=false linux=true android=false obs=false | red=false prflx=false fire=false codecchange=true reconnect=true icetcp=true encactive=false codecorder=false tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=true trailer=false",
        ),
        (
            Some(info(Sdk::Js, "2.0.0", "Firefox", "Windows")),
            "ff=true safari=false go=false linux=false android=false obs=false | red=false prflx=false fire=false codecchange=true reconnect=true icetcp=true encactive=false codecorder=true tracksub=true reqresp=true sctpzero=true reuse=true cap1=false cap2=false trailer=false",
        ),
    ];

    for (i, (info, expected)) in table.into_iter().enumerate() {
        let c = ClientInfo {
            client_info: info.map(Arc::new),
        };
        let reconnect = match std::panic::catch_unwind(|| c.can_handle_reconnect_response()) {
            Ok(can) => can.to_string(),
            Err(_) => "panic".to_string(),
        };
        let got = format!(
            "ff={} safari={} go={} linux={} android={} obs={} | red={} prflx={} fire={} codecchange={} reconnect={} icetcp={} encactive={} codecorder={} tracksub={} reqresp={} sctpzero={} reuse={} cap1={} cap2={} trailer={}",
            c.is_firefox(),
            c.is_safari(),
            c.is_go(),
            c.is_linux(),
            c.is_android(),
            c.is_obs(),
            c.supports_audio_red(),
            c.supports_prflx_over_relay(),
            c.fire_track_by_rtp_packet(),
            c.supports_codec_change(),
            reconnect,
            c.supports_ice_tcp(),
            c.supports_change_rtp_sender_encoding_active(),
            c.comply_with_codec_order_in_sdp_answer(),
            c.supports_track_subscribed_event(),
            c.supports_request_response(),
            c.supports_sctp_zero_checksum(),
            c.supports_transceiver_reuse(),
            c.has_capability(Capability::CapPacketTrailer),
            c.has_capability(Capability::CapCompressionDeflateRaw),
            c.supports_packet_trailer(),
        );
        assert_eq!(expected, got, "client {i}");
    }
}
