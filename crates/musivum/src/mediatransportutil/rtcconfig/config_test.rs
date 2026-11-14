// New for musivum: the original has no tests of config.go. The expected values are what the
// original gave in one run (zero values, `Validate`, `UnmarshalString`, `MarshalYAML`,
// yaml.v3 on a document with every option, `time.ParseDuration`), except the node's own
// address, which is the machine's.

use serde_json::json;

use super::super::get_local_ip_addresses;
use super::*;

#[test]
fn test_defaults() {
    // New for musivum.
    let expected = RtcConfig {
        udp_port: PortRange { start: 0, end: 0 },
        tcp_port: 0,
        ice_port_range_start: 0,
        ice_port_range_end: 0,
        use_stun_port_as_ice: false,
        node_ip: NodeIp {
            v4: String::new(),
            v6: String::new(),
        },
        node_ip_auto_generated: false,
        stun_servers: vec![],
        use_external_ip: false,
        require_ipv4: false,
        skip_external_ip_validation: false,
        advertise_internal_ip: false,
        use_ice_lite: false,
        interfaces: InterfacesConfig {
            includes: vec![],
            excludes: vec![],
        },
        ips: IpsConfig {
            includes: vec![],
            excludes: vec![],
        },
        enable_loopback_candidate: false,
        use_mdns: false,
        external_ip_only: false,
        batch_io: BatchIoConfig {
            batch_size: 0,
            max_flush_interval: Duration::ZERO,
        },
        sctp_min_cwnd: 0,
        sctp_fast_rtx_wnd: 0,
        sctp_cwnd_ca_step: 0,
        host_acceptance_min_wait: Duration::ZERO,
        srflx_acceptance_min_wait: Duration::ZERO,
        prflx_acceptance_min_wait: Duration::ZERO,
        relay_acceptance_min_wait: Duration::ZERO,
        force_tcp: false,
    };
    assert_eq!(expected, RtcConfig::default());
    // A section with nothing in it is the same.
    assert_eq!(
        expected,
        serde_json::from_value::<RtcConfig>(json!({})).unwrap()
    );

    assert_eq!(
        [
            "global.stun.twilio.com:3478",
            "stun.l.google.com:19302",
            "stun1.l.google.com:19302"
        ],
        DEFAULT_STUN_SERVERS
    );
}

/// The address `validate` finds without STUN: the first IPv4 address of the machine, or
/// the first of any kind.
fn local_node_ip() -> NodeIp {
    let addresses = get_local_ip_addresses(false, true, None, None).unwrap();
    let mut node_ip = NodeIp::default();
    match addresses.iter().find(|a| parse_ip(a).unwrap().is_ipv4()) {
        Some(v4) => node_ip.v4 = v4.clone(),
        None => node_ip.v6 = addresses[0].clone(),
    }
    node_ip
}

#[test]
fn test_validate() {
    // New for musivum. Nothing here asks a STUN server: `use_external_ip` is off.

    // development: one port
    let mut conf = RtcConfig::default();
    conf.validate(true).unwrap();
    assert_eq!(
        PortRange {
            start: 7882,
            end: 0
        },
        conf.udp_port
    );
    assert_eq!((0, 0), (conf.ice_port_range_start, conf.ice_port_range_end));
    assert!(conf.node_ip_auto_generated);
    assert_eq!(local_node_ip(), conf.node_ip);
    assert_eq!(
        RtcConfig {
            udp_port: conf.udp_port,
            node_ip: conf.node_ip.clone(),
            node_ip_auto_generated: true,
            ..Default::default()
        },
        conf,
        "nothing else is set"
    );

    // production: a range of ports
    let mut conf = RtcConfig::default();
    conf.validate(false).unwrap();
    assert_eq!(PortRange { start: 0, end: 0 }, conf.udp_port);
    assert_eq!(
        (50000, 60000),
        (conf.ice_port_range_start, conf.ice_port_range_end)
    );
    assert!(conf.node_ip_auto_generated);
    assert_eq!(local_node_ip(), conf.node_ip);

    // an address set by hand stays
    let by_hand = NodeIp {
        v4: "10.1.2.3".to_string(),
        ..Default::default()
    };
    let mut conf = RtcConfig {
        node_ip: by_hand.clone(),
        ..Default::default()
    };
    conf.validate(true).unwrap();
    assert_eq!(
        PortRange {
            start: 7882,
            end: 0
        },
        conf.udp_port
    );
    assert!(!conf.node_ip_auto_generated);
    assert_eq!(by_hand, conf.node_ip);

    // a port set by hand stays, and no range is added
    let mut conf = RtcConfig {
        node_ip: by_hand.clone(),
        udp_port: PortRange {
            start: 9000,
            end: 0,
        },
        ..Default::default()
    };
    conf.validate(false).unwrap();
    assert_eq!(
        PortRange {
            start: 9000,
            end: 0
        },
        conf.udp_port
    );
    assert_eq!((0, 0), (conf.ice_port_range_start, conf.ice_port_range_end));

    // the start of a range is enough for no port to be set
    let mut conf = RtcConfig {
        node_ip: by_hand,
        ice_port_range_start: 40000,
        ..Default::default()
    };
    conf.validate(true).unwrap();
    assert_eq!(PortRange { start: 0, end: 0 }, conf.udp_port);
    assert_eq!(
        (40000, 0),
        (conf.ice_port_range_start, conf.ice_port_range_end)
    );

    // reload_external_ip finds the address again
    let mut conf = RtcConfig {
        node_ip: NodeIp {
            v4: "10.1.2.3".to_string(),
            v6: "fd00::1".to_string(),
        },
        ..Default::default()
    };
    conf.reload_external_ip().unwrap();
    assert!(conf.node_ip_auto_generated);
    assert_eq!(local_node_ip(), conf.node_ip);
}

#[test]
fn test_port_range_unmarshal_string() {
    // New for musivum. The start and the end that were read, or the text of the error.
    type Expected = Result<(i64, i64), &'static str>;
    let test_cases: [(&str, Expected); 20] = [
        ("", Ok((0, 0))),
        ("7882", Ok((7882, 0))),
        ("50000-60000", Ok((50000, 60000))),
        ("1 - 2", Ok((1, 2))),
        (" 1 - 2 ", Ok((1, 2))),
        ("5-4", Err("end port 4 must be greater than start port 5")),
        ("5-5", Err("end port 5 must be greater than start port 5")),
        (
            "a-b",
            Err("invalid start port a: strconv.Atoi: parsing \"a\": invalid syntax"),
        ),
        (
            "1-b",
            Err("invalid end port b: strconv.Atoi: parsing \"b\": invalid syntax"),
        ),
        (
            "1-2-3",
            Err("invalid port range 1-2-3, should be <start>-<end>"),
        ),
        (
            "-5",
            Err("invalid start port : strconv.Atoi: parsing \"\": invalid syntax"),
        ),
        (
            "abc",
            Err("invalid port abc: strconv.Atoi: parsing \"abc\": invalid syntax"),
        ),
        (
            "99999999999999999999",
            Err(
                "invalid port 99999999999999999999: strconv.Atoi: parsing \"99999999999999999999\": value out of range",
            ),
        ),
        (
            " 7",
            Err("invalid port  7: strconv.Atoi: parsing \" 7\": invalid syntax"),
        ),
        ("+7", Ok((7, 0))),
        (
            "7-",
            Err("invalid end port : strconv.Atoi: parsing \"\": invalid syntax"),
        ),
        ("70000", Ok((70000, 0))),
        ("0", Ok((0, 0))),
        (
            "1.5",
            Err("invalid port 1.5: strconv.Atoi: parsing \"1.5\": invalid syntax"),
        ),
        ("0-5", Ok((0, 5))),
    ];
    for (input, expected) in test_cases {
        let mut r = PortRange::default();
        let res = r.unmarshal_string(input);
        match expected {
            Ok((start, end)) => {
                assert!(res.is_ok(), "{input:?}: {res:?}");
                assert_eq!(PortRange { start, end }, r, "{input:?}");
            }
            Err(text) => {
                assert_eq!(text, res.unwrap_err().to_string(), "{input:?}");
                assert_eq!(PortRange::default(), r, "{input:?}");
            }
        }
    }
}

#[test]
fn test_port_range() {
    // New for musivum: ToSlice, Valid and MarshalYAML.
    let r = PortRange {
        start: 7882,
        end: 0,
    };
    assert_eq!(vec![7882], r.to_slice());
    assert!(r.valid());
    assert_eq!(json!(7882), serde_json::to_value(r).unwrap());

    let r = PortRange { start: 1, end: 3 };
    assert_eq!(vec![1, 2, 3], r.to_slice());
    assert!(r.valid());
    assert_eq!(json!("1-3"), serde_json::to_value(r).unwrap());

    let r = PortRange { start: 5, end: 2 };
    assert_eq!(vec![5], r.to_slice());
    assert!(r.valid());
    assert_eq!(
        "end port 2 must be greater than start port 5",
        serde_json::to_value(r).unwrap_err().to_string()
    );

    let r = PortRange { start: 0, end: 0 };
    assert_eq!(vec![0], r.to_slice());
    assert!(!r.valid());
    assert_eq!(json!(0), serde_json::to_value(r).unwrap());

    let r = PortRange { start: 3, end: 3 };
    assert_eq!(vec![3], r.to_slice());
    assert_eq!(
        "end port 3 must be greater than start port 3",
        serde_json::to_value(r).unwrap_err().to_string()
    );
}

#[test]
fn test_node_ip() {
    // New for musivum: UnmarshalString, and on what it left IsEmpty, ToStringSlice,
    // PrimaryIP and MarshalYAML.
    struct TestCase {
        input: &'static str,
        v4: &'static str,
        v6: &'static str,
        err: &'static str,
    }
    let tc = |input, v4, v6, err| TestCase { input, v4, v6, err };
    let test_cases = [
        tc("", "", "", ""),
        tc("1.2.3.4", "1.2.3.4", "", ""),
        tc("fd00::1", "", "fd00::1", ""),
        tc("1.2.3.4,fd00::1", "1.2.3.4", "fd00::1", ""),
        tc("fd00::1, 1.2.3.4", "1.2.3.4", "fd00::1", ""),
        tc(
            "1.2.3.4,5.6.7.8",
            "1.2.3.4",
            "",
            "multiple ipv4 addresses provided: 1.2.3.4 and 5.6.7.8",
        ),
        tc(
            "fd00::1,fd00::2",
            "",
            "fd00::1",
            "multiple ipv6 addresses provided: fd00::1 and fd00::2",
        ),
        tc(
            "a,b,c",
            "",
            "",
            "invalid node ip a,b,c, should be <ipv4>,<ipv6>",
        ),
        tc("nonsense", "", "", "invalid ip nonsense"),
        tc("1.2.3.4, x", "1.2.3.4", "", "invalid ip  x"),
        tc("::ffff:1.2.3.4", "1.2.3.4", "", ""),
        tc("1.2.3.4,", "1.2.3.4", "", "invalid ip "),
        tc("FD00:0:0::1", "", "fd00::1", ""),
        tc(" 1.2.3.4 ", "1.2.3.4", "", ""),
        tc("01.2.3.4", "", "", "invalid ip 01.2.3.4"),
    ];
    for tc in test_cases {
        let mut n = NodeIp::default();
        let err = n
            .unmarshal_string(tc.input)
            .map_or_else(|err| err.to_string(), |()| String::new());
        assert_eq!(tc.err, err, "{:?}", tc.input);
        assert_eq!(
            (tc.v4, tc.v6),
            (n.v4.as_str(), n.v6.as_str()),
            "{:?}",
            tc.input
        );

        assert_eq!(tc.v4.is_empty() && tc.v6.is_empty(), n.is_empty());
        let slice: Vec<&str> = [tc.v4, tc.v6]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect();
        assert_eq!(slice, n.to_string_slice(), "{:?}", tc.input);
        assert_eq!(
            if tc.v4.is_empty() { tc.v6 } else { tc.v4 },
            n.primary_ip(),
            "{:?}",
            tc.input
        );
        assert_eq!(
            json!(slice.join(",")),
            serde_json::to_value(&n).unwrap(),
            "{:?}",
            tc.input
        );
    }
}

#[test]
fn test_deserialize() {
    // New for musivum: the section with every option in it, as yaml.v3 decodes it in the
    // original. serde_json stands for a YAML decoder here, which the crate does not have
    // yet: the names and the three kinds of value with a decoding of their own are the same
    // for both.
    let conf: RtcConfig = serde_json::from_value(json!({
        "udp_port": "7882-7890",
        "tcp_port": 7881,
        "port_range_start": 50000,
        "port_range_end": 60000,
        "use_stun_port_as_ice": true,
        "node_ip": "1.2.3.4,fd00::1",
        "stun_servers": ["a:1", "b:2"],
        "use_external_ip": true,
        "require_ipv4": true,
        "skip_external_ip_validation": true,
        "advertise_internal_ip": true,
        "use_ice_lite": true,
        "interfaces": {"includes": ["eth0"], "excludes": ["docker0"]},
        "ips": {"includes": ["10.0.0.0/8"], "excludes": ["10.9.0.0/16"]},
        "enable_loopback_candidate": true,
        "use_mdns": true,
        "external_ip_only": true,
        "batch_io": {"batch_size": 16, "max_flush_interval": "2ms"},
        "sctp_min_cwnd": 3,
        "sctp_fast_rtx_wnd": 4,
        "sctp_cwnd_ca_step": 5,
        "host_acceptance_min_wait": "100ms",
        "srflx_acceptance_min_wait": "1.5s",
        "prflx_acceptance_min_wait": "1m",
        "relay_acceptance_min_wait": "2h45m",
        "force_tcp": true,
        // Not an option: `yaml:"-"` in the original.
        "node_ip_auto_generated": true,
    }))
    .unwrap();
    assert_eq!(
        RtcConfig {
            udp_port: PortRange {
                start: 7882,
                end: 7890
            },
            tcp_port: 7881,
            ice_port_range_start: 50000,
            ice_port_range_end: 60000,
            use_stun_port_as_ice: true,
            node_ip: NodeIp {
                v4: "1.2.3.4".to_string(),
                v6: "fd00::1".to_string(),
            },
            node_ip_auto_generated: false,
            stun_servers: vec!["a:1".to_string(), "b:2".to_string()],
            use_external_ip: true,
            require_ipv4: true,
            skip_external_ip_validation: true,
            advertise_internal_ip: true,
            use_ice_lite: true,
            interfaces: InterfacesConfig {
                includes: vec!["eth0".to_string()],
                excludes: vec!["docker0".to_string()],
            },
            ips: IpsConfig {
                includes: vec!["10.0.0.0/8".to_string()],
                excludes: vec!["10.9.0.0/16".to_string()],
            },
            enable_loopback_candidate: true,
            use_mdns: true,
            external_ip_only: true,
            batch_io: BatchIoConfig {
                batch_size: 16,
                max_flush_interval: Duration::from_millis(2),
            },
            sctp_min_cwnd: 3,
            sctp_fast_rtx_wnd: 4,
            sctp_cwnd_ca_step: 5,
            host_acceptance_min_wait: Duration::from_millis(100),
            srflx_acceptance_min_wait: Duration::from_millis(1500),
            prflx_acceptance_min_wait: Duration::from_secs(60),
            relay_acceptance_min_wait: Duration::from_secs(2 * 3600 + 45 * 60),
            force_tcp: true,
        },
        conf
    );

    // One option at a time: what the original makes of it, or the start of its error.
    let decode = |section: serde_json::Value| {
        serde_json::from_value::<RtcConfig>(section).map_err(|err| err.to_string())
    };
    let port = |section| decode(section).map(|conf| (conf.udp_port.start, conf.udp_port.end));
    assert_eq!(Ok((7882, 0)), port(json!({"udp_port": 7882})));
    // What has no text of its own is passed over.
    assert_eq!(
        Ok((0, 0)),
        port(json!({"udp_port": {"start": 1, "end": 2}}))
    );
    assert_eq!(Ok((0, 0)), port(json!({"udp_port": [1, 2]})));
    assert_eq!(Ok((0, 0)), port(json!({"udp_port": null})));
    assert_eq!(
        Err("end port 4 must be greater than start port 5".to_string()),
        port(json!({"udp_port": "5-4"}))
    );

    let node_ip = |section| decode(section).map(|conf| conf.node_ip.to_string_slice());
    assert_eq!(
        Err("invalid ip x".to_string()),
        node_ip(json!({"node_ip": "x"}))
    );
    assert_eq!(Ok(vec![]), node_ip(json!({"node_ip": {"ipv4": "1.2.3.4"}})));
    assert_eq!(
        Ok(vec!["1.2.3.4".to_string()]),
        node_ip(json!({"node_ip": "1.2.3.4"}))
    );

    // A duration is a string with a unit; a number is none.
    let wait = |section| decode(section).map(|conf| conf.host_acceptance_min_wait);
    assert!(wait(json!({"host_acceptance_min_wait": 5})).is_err());
    assert_eq!(
        Ok(Duration::ZERO),
        wait(json!({"host_acceptance_min_wait": "0"}))
    );
    assert!(wait(json!({"host_acceptance_min_wait": "abc"})).is_err());
    assert!(decode(json!({"tcp_port": -1})).is_err());
}

#[test]
fn test_parse_duration() {
    // New for musivum: Go's time.ParseDuration, which yaml.v3 decodes a duration with.
    let test_cases: [(&str, Option<i64>); 32] = [
        ("100ms", Some(100000000)),
        ("1.5s", Some(1500000000)),
        ("2h45m", Some(9900000000000)),
        ("-1.5h", Some(-5400000000000)),
        ("0", Some(0)),
        ("+5s", Some(5000000000)),
        ("1h0.5s", Some(3600500000000)),
        (".5s", Some(500000000)),
        ("5.s", Some(5000000000)),
        ("1\u{b5}s", Some(1000)),
        ("1\u{3bc}s", Some(1000)),
        ("1us", Some(1000)),
        ("3ns", Some(3)),
        ("1.5ns", Some(1)),
        ("", None),
        ("5", None),
        ("s", None),
        ("1x", None),
        ("1.2.3s", None),
        ("9223372036854775807ns", Some(9223372036854775807)),
        ("9223372036854775808ns", None),
        ("-9223372036854775808ns", Some(-9223372036854775808)),
        ("0.000000000005h", Some(18)),
        ("2562047h47m16.854775807s", Some(9223372036854775807)),
        ("2562047h47m16.854775808s", None),
        ("-", None),
        ("+", None),
        ("1h-5m", None),
        ("0.3333333333333333333333s", Some(333333333)),
        ("-0", Some(0)),
        ("+0", Some(0)),
        ("1.0000000000000000000000001s", Some(1000000000)),
    ];
    for (input, expected) in test_cases {
        assert_eq!(expected.map(Duration), parse_duration(input), "{input:?}");
    }
}
