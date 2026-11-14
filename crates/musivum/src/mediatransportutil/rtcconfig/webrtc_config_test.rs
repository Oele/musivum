// Ported from LiveKit's mediatransportutil, pkg/rtcconfig/webrtc_config_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the candidate type and the rewrite modes the third
// test compares with are this module's where they were pion's. The tests after the three
// of the original are new for musivum and say so: what `new_webrtc_config` makes of the
// options, on made-up lists of interfaces and, where the system's own are used, held to the
// loopback interface by the filter on addresses.

use tracing::Level;

use super::super::ip::ip_test::{StunResponder, free_port};
use super::super::{NodeIp, PortRange};
use super::*;
use crate::testutils::capture_log;
use crate::webrtc::peerconnection::Node;

#[test]
fn test_ip_filter_from_conf() {
    let test_data = IpsConfig {
        includes: vec!["10.0.0.0/19".to_string()],
        excludes: [
            "10.0.0.0/9",
            "10.192.0.0/11",
            "10.244.0.0/16",
            "172.16.0.0/12",
            "192.168.128.0/17",
        ]
        .map(str::to_string)
        .to_vec(),
    };

    let ip_filter = ip_filter_from_conf(&test_data).unwrap();

    let test_cases = [
        ("10.0.0.10", true),
        ("10.0.0.1", true),
        ("10.0.31.255", true),
        ("10.0.32.1", false),
        ("10.192.0.1", false),
        ("10.244.0.1", false),
        ("172.16.0.10", false),
        ("192.168.128.5", false),
    ];

    for (ip, expected) in test_cases {
        let test_ip: IpAddr = ip.parse().unwrap();

        let result = ip_filter(test_ip);
        assert_eq!(
            expected, result,
            "For IP {ip}, expected {expected} but got {result}"
        );
    }

    let test_data = IpsConfig {
        includes: vec!["192.168.128.1".to_string()],
        excludes: vec!["192.168.128.0/17".to_string()],
    };
    assert!(ip_filter_from_conf(&test_data).is_err());
}

#[test]
fn test_interface_filter_from_conf() {
    let test_data = InterfacesConfig {
        includes: ["eth0", "eth1", "eth2"].map(str::to_string).to_vec(),
        excludes: ["eth0", "eth3", "eth4"].map(str::to_string).to_vec(),
    };

    let iface_filter = interface_filter_from_conf(&test_data);

    let test_cases = [
        ("eth0", true),
        ("eth1", true),
        ("eth2", true),
        ("eth3", false),
        ("eth4", false),
        ("eth5", false),
    ];

    for (iface, expected) in test_cases {
        let result = iface_filter(iface);
        assert_eq!(
            expected, result,
            "For interface {iface}, expected {expected} but got {result}"
        );
    }
}

#[test]
fn test_nat_1to1_address_rewrite_rules() {
    // catchAll replaces host candidate when includeInternal is false
    {
        let rules = nat_1to1_address_rewrite_rules(&["1.2.3.4".to_string()], false);
        assert_eq!(1, rules.len());
        assert_eq!(vec!["1.2.3.4"], rules[0].external);
        assert_eq!(IceCandidateType::Host, rules[0].as_candidate_type);
        assert_eq!(IceAddressRewriteMode::Unspecified, rules[0].mode);
    }

    // catchAll appends host candidate when includeInternal is true
    {
        let rules = nat_1to1_address_rewrite_rules(&["1.2.3.4".to_string()], true);
        assert_eq!(1, rules.len());
        assert_eq!(vec!["1.2.3.4"], rules[0].external);
        assert_eq!(IceCandidateType::Host, rules[0].as_candidate_type);
        assert_eq!(IceAddressRewriteMode::Append, rules[0].mode);
    }

    // external/local pairs and catchAll IPs get the same mode
    {
        let rules = nat_1to1_address_rewrite_rules(
            &["5.6.7.8/10.0.0.1".to_string(), "1.2.3.4".to_string()],
            true,
        );
        assert_eq!(2, rules.len());

        assert_eq!(vec!["5.6.7.8"], rules[0].external);
        assert_eq!("10.0.0.1", rules[0].local);
        assert_eq!(IceAddressRewriteMode::Append, rules[0].mode);

        assert_eq!(vec!["1.2.3.4"], rules[1].external);
        assert_eq!(IceAddressRewriteMode::Append, rules[1].mode);
    }
}

// ---------------------------------------------------------------------------
// New for musivum from here on.

fn strings(s: &[&str]) -> Vec<String> {
    s.iter().map(|s| s.to_string()).collect()
}

fn ips(s: &[&str]) -> Vec<IpAddr> {
    s.iter().map(|s| s.parse().unwrap()).collect()
}

fn iface(name: &str, up: bool, loopback: bool, addrs: &[&str]) -> Interface {
    Interface {
        name: name.to_string(),
        up,
        loopback,
        addrs: ips(addrs),
    }
}

/// A configuration that has been through `validate` on a machine whose one address is
/// 192.0.2.10, without that being asked from the machine the test runs on.
fn validated(development: bool) -> RtcConfig {
    let mut conf = RtcConfig {
        node_ip: NodeIp {
            v4: "192.0.2.10".to_string(),
            ..Default::default()
        },
        node_ip_auto_generated: true,
        ..Default::default()
    };
    if development {
        conf.udp_port = PortRange {
            start: 7882,
            end: 0,
        };
    } else {
        conf.ice_port_range_start = 50000;
        conf.ice_port_range_end = 60000;
    }
    conf
}

/// Only the loopback address of the machine is looked at, so that what comes out is the
/// same everywhere.
fn loopback_only(mut conf: RtcConfig) -> RtcConfig {
    conf.ips.includes = strings(&["127.0.0.0/8"]);
    conf.enable_loopback_candidate = true;
    conf
}

#[test]
fn test_nat_1to1_address_rewrite_rules_more() {
    // New for musivum: the cases the original's test leaves out, as the original has them.
    assert_eq!(
        vec![
            IceAddressRewriteRule {
                external: strings(&["5.6.7.8"]),
                local: "10.0.0.1".to_string(),
                as_candidate_type: IceCandidateType::Host,
                mode: IceAddressRewriteMode::Append,
            },
            IceAddressRewriteRule {
                external: strings(&["1.2.3.4", "fd00::1"]),
                local: String::new(),
                as_candidate_type: IceCandidateType::Host,
                mode: IceAddressRewriteMode::Append,
            },
        ],
        nat_1to1_address_rewrite_rules(&strings(&["5.6.7.8/10.0.0.1", "1.2.3.4", "fd00::1"]), true)
    );
    // What is not a pair is taken for an address.
    assert_eq!(
        vec![
            IceAddressRewriteRule {
                external: strings(&["x"]),
                local: "y".to_string(),
                as_candidate_type: IceCandidateType::Host,
                mode: IceAddressRewriteMode::Unspecified,
            },
            IceAddressRewriteRule {
                external: strings(&["a/b/c"]),
                local: String::new(),
                as_candidate_type: IceCandidateType::Host,
                mode: IceAddressRewriteMode::Unspecified,
            },
        ],
        nat_1to1_address_rewrite_rules(&strings(&["a/b/c", "x/y"]), false)
    );
    assert!(nat_1to1_address_rewrite_rules(&[], false).is_empty());

    let mut s = SettingEngineConfig::default();
    set_nat_1to1_address_rewrite_rules(&mut s, &strings(&["1.2.3.4"]), false).unwrap();
    assert_eq!(1, s.ice_address_rewrite_rules.len());
}

#[test]
fn test_ip_net() {
    // New for musivum: Go's net.ParseCIDR and IPNet.Contains, with what they gave in one
    // run of the original's filter.
    let probes = [
        "10.1.2.3",
        "11.1.2.3",
        "fd00::5",
        "::ffff:10.1.2.3",
        "2001:db8::1",
        "::10.1.2.3",
    ];
    let test_cases: [(&str, Option<[bool; 6]>); 13] = [
        ("10.0.0.0/8", Some([true, false, false, true, false, false])),
        ("10.0.0.5/8", Some([true, false, false, true, false, false])),
        ("fd00::/8", Some([false, false, true, false, false, false])),
        (
            "::ffff:10.0.0.0/104",
            Some([true, false, false, true, false, false]),
        ),
        (
            "::ffff:10.0.0.0/80",
            Some([false, false, false, false, false, true]),
        ),
        ("10.0.0.0/33", None),
        ("10.0.0.0/", None),
        ("/8", None),
        ("10.0.0.0/+8", None),
        (
            "10.0.0.0/08",
            Some([true, false, false, true, false, false]),
        ),
        ("0.0.0.0/0", Some([true, true, false, true, false, false])),
        ("::/0", Some([false, false, true, false, true, true])),
        ("10.0.0.0/8/9", None),
    ];
    for (cidr, expected) in test_cases {
        match (IpNet::parse_cidr(cidr), expected) {
            (Ok(net), Some(expected)) => {
                let contains = probes.map(|ip| net.contains(ip.parse().unwrap()));
                assert_eq!(expected, contains, "{cidr}");
            }
            (Err(err), None) => {
                assert_eq!(format!("invalid CIDR address: {cidr}"), err.to_string());
            }
            (res, expected) => panic!("{cidr}: {res:?}, expected {expected:?}"),
        }
    }

    // Excludes alone, and both families.
    let filter = ip_filter_from_conf(&IpsConfig {
        excludes: strings(&["10.0.0.0/8", "fd00::/8"]),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        vec![false, true, false, true],
        ips(&["10.1.2.3", "11.1.2.3", "fd00::5", "2001:db8::1"])
            .into_iter()
            .map(|ip| filter(ip))
            .collect::<Vec<_>>()
    );
    let filter = interface_filter_from_conf(&InterfacesConfig {
        excludes: strings(&["docker0"]),
        ..Default::default()
    });
    assert!(!filter("docker0"));
    assert!(filter("eth0"));
}

#[test]
fn test_local_interfaces() {
    // New for musivum: the addresses a UDP mux of the original listens on.
    let ifaces = [
        iface("lo", true, true, &["127.0.0.1", "::1"]),
        iface(
            "eth0",
            true,
            false,
            &[
                "192.0.2.10",
                "2001:db8::10",
                "fe80::1",
                "fec0::1",
                "::192.0.2.10",
                "169.254.7.7",
            ],
        ),
        iface("eth1", false, false, &["198.51.100.7"]),
        iface("docker0", true, false, &["203.0.113.1"]),
    ];
    let both = [NetworkType::Udp4, NetworkType::Udp6];

    // An interface that is down is left out, and of IPv6 the addresses that are local to
    // the link or the site and the ones that hold an IPv4 address; an IPv4 address that is
    // local to the link stays.
    assert_eq!(
        ips(&["192.0.2.10", "2001:db8::10", "169.254.7.7", "203.0.113.1"]),
        local_interfaces(&ifaces, None, None, &both, false)
    );
    // With the loopback interface its IPv4 address comes in; `::1` is among the addresses
    // that hold an IPv4 address, for the original's test of that.
    assert_eq!(
        ips(&[
            "127.0.0.1",
            "192.0.2.10",
            "2001:db8::10",
            "169.254.7.7",
            "203.0.113.1"
        ]),
        local_interfaces(&ifaces, None, None, &both, true)
    );
    assert_eq!(
        ips(&["192.0.2.10", "169.254.7.7", "203.0.113.1"]),
        local_interfaces(&ifaces, None, None, &[NetworkType::Udp4], false)
    );
    assert_eq!(
        ips(&["2001:db8::10"]),
        local_interfaces(&ifaces, None, None, &[NetworkType::Tcp6], false)
    );

    let if_filter = interface_filter_from_conf(&InterfacesConfig {
        excludes: strings(&["docker0"]),
        ..Default::default()
    });
    let ip_filter = ip_filter_from_conf(&IpsConfig {
        excludes: strings(&["169.254.0.0/16"]),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        ips(&["192.0.2.10", "2001:db8::10"]),
        local_interfaces(&ifaces, Some(&if_filter), Some(&ip_filter), &both, false)
    );
}

#[test]
fn test_host_candidate_ips() {
    // New for musivum: what the rewrite rules make of the local addresses.
    let local = ips(&["10.0.0.1", "10.0.0.2", "fd00::1"]);
    let candidates = |nat_1to1_ips: &[&str], include_internal| {
        let rules = nat_1to1_address_rewrite_rules(&strings(nat_1to1_ips), include_internal);
        host_candidate_ips(&local, &rules).map_err(|err| err.to_string())
    };

    // No rules: the local addresses.
    assert_eq!(Ok(local.clone()), candidates(&[], false));

    // A node address set by hand stands for every local address of its family.
    assert_eq!(
        Ok(ips(&["1.2.3.4", "fd00::1"])),
        candidates(&["1.2.3.4"], false)
    );
    assert_eq!(
        Ok(ips(&["1.2.3.4", "2001:db8::1"])),
        candidates(&["1.2.3.4", "2001:db8::1"], false)
    );
    // ... or is given with them.
    assert_eq!(
        Ok(ips(&["10.0.0.1", "1.2.3.4", "10.0.0.2", "fd00::1"])),
        candidates(&["1.2.3.4"], true)
    );

    // External addresses found for local ones: each replaces its own, and an address none
    // was found for (it is mapped to itself) stays.
    assert_eq!(
        Ok(ips(&["5.6.7.8", "10.0.0.2", "fd00::1"])),
        candidates(
            &["5.6.7.8/10.0.0.1", "10.0.0.2/10.0.0.2", "fd00::1/fd00::1"],
            false
        )
    );
    assert_eq!(
        Ok(ips(&["10.0.0.1", "5.6.7.8", "10.0.0.2", "fd00::1"])),
        candidates(
            &["5.6.7.8/10.0.0.1", "10.0.0.2/10.0.0.2", "fd00::1/fd00::1"],
            true
        )
    );
    // A rule for one local address says nothing about the others; a rule for all is for
    // the ones without a rule of their own, whichever comes first.
    assert_eq!(
        Ok(ips(&["5.6.7.8", "10.0.0.2", "fd00::1"])),
        candidates(&["5.6.7.8/10.0.0.1"], false)
    );
    assert_eq!(
        Ok(ips(&["5.6.7.8", "1.2.3.4", "fd00::1"])),
        candidates(&["1.2.3.4", "5.6.7.8/10.0.0.1"], false)
    );
    // An external address of the other family is given for the local address it is paired
    // with.
    assert_eq!(
        Ok(ips(&["2001:db8::1", "10.0.0.2", "fd00::1"])),
        candidates(&["2001:db8::1/10.0.0.1"], false)
    );
    // Two local addresses behind one external address: it is given once.
    assert_eq!(
        Ok(ips(&["5.6.7.8", "fd00::1"])),
        candidates(&["5.6.7.8/10.0.0.1", "5.6.7.8/10.0.0.2"], false)
    );

    // What is no address is refused.
    assert_eq!(
        Err("invalid address rewrite mapping".to_string()),
        candidates(&["a/b/c"], false)
    );
    assert_eq!(
        Err("invalid address rewrite mapping".to_string()),
        candidates(&["1.2.3.4/local"], false)
    );

    // A rule that replaces and has no external address takes the local address away; one
    // that appends leaves it.
    let rule = |local: &str, mode| IceAddressRewriteRule {
        local: local.to_string(),
        as_candidate_type: IceCandidateType::Host,
        mode,
        ..Default::default()
    };
    assert_eq!(
        ips(&["10.0.0.2", "fd00::1"]),
        host_candidate_ips(&local, &[rule("10.0.0.1", IceAddressRewriteMode::Replace)]).unwrap()
    );
    assert_eq!(
        local,
        host_candidate_ips(&local, &[rule("10.0.0.1", IceAddressRewriteMode::Append)]).unwrap()
    );
    assert!(
        host_candidate_ips(&local, &[rule("", IceAddressRewriteMode::Unspecified)])
            .unwrap()
            .is_empty()
    );
    // A rule for another kind of candidate is not for these.
    assert_eq!(
        local,
        host_candidate_ips(
            &local,
            &[IceAddressRewriteRule {
                external: strings(&["1.2.3.4"]),
                as_candidate_type: IceCandidateType::Srflx,
                ..Default::default()
            }]
        )
        .unwrap()
    );
}

#[test]
fn test_new_webrtc_config_development() {
    // New for musivum: a configuration with nothing set, validated for development.
    let conf = new_webrtc_config(&loopback_only(validated(true)), true).unwrap();

    // The node's socket: the port of `udp_port` on all IPv4 addresses, and the local
    // addresses as they are.
    assert_eq!(
        "0.0.0.0:7882".parse::<SocketAddr>().unwrap(),
        conf.node_config.bind_address
    );
    assert_eq!(ips(&["127.0.0.1"]), conf.node_config.nat_1to1_ips);
    assert!(conf.nat_1to1_ips.is_empty());
    assert!(!conf.use_mdns);

    let s = &conf.setting_engine;
    assert_eq!(65535, s.sctp_max_message_size);
    assert_eq!(
        (0, 0, 0),
        (s.sctp_min_cwnd, s.sctp_fast_rtx_wnd, s.sctp_cwnd_ca_step)
    );
    assert!(s.interface_filter.is_none());
    assert!(s.ip_filter.is_some());
    assert_eq!(MulticastDnsMode::Disabled, s.ice_multicast_dns_mode);
    assert!(s.ice_address_rewrite_rules.is_empty());
    assert_eq!(None, s.ephemeral_udp_port_range);
    assert_eq!(vec![NetworkType::Udp4, NetworkType::Udp6], s.network_types);
    assert_eq!(Duration::ZERO, s.host_acceptance_min_wait);
    assert!(s.include_loopback_candidate);
    assert!(!s.lite);
    // The layer's engine that is built from it, and the addresses of a connection made
    // from these settings: the node's.
    let engine = s.build();
    assert_eq!(MulticastDnsMode::Disabled, engine.ice_multicast_dns_mode);
    assert_eq!(65535, engine.sctp_max_message_size);
    assert!(!engine.lite);
    assert_eq!(None, engine.host_candidate_ips);
    assert_eq!(ips(&["127.0.0.1"]), s.host_candidate_ips().unwrap());

    // Not ICE lite and no address set by hand: the STUN servers of the original as ICE
    // servers.
    assert_eq!(
        vec![IceServer {
            urls: strings(&[
                "stun:global.stun.twilio.com:3478",
                "stun:stun.l.google.com:19302",
                "stun:stun1.l.google.com:19302",
            ]),
            ..Default::default()
        }],
        conf.ice_servers
    );
    let mut with_servers = loopback_only(validated(true));
    with_servers.stun_servers = strings(&["stun.example.org:3478"]);
    assert_eq!(
        strings(&["stun:stun.example.org:3478"]),
        new_webrtc_config(&with_servers, true).unwrap().ice_servers[0].urls
    );
}

#[test]
fn test_new_webrtc_config_options() {
    // New for musivum: every option that is set shows where the description of
    // `new_webrtc_config` says it goes.
    let mut rtc_conf = loopback_only(validated(false));
    // An address set by hand.
    rtc_conf.node_ip = NodeIp {
        v4: "203.0.113.5".to_string(),
        v6: "2001:db8::5".to_string(),
    };
    rtc_conf.node_ip_auto_generated = false;
    rtc_conf.tcp_port = 7881;
    rtc_conf.use_ice_lite = true;
    rtc_conf.use_mdns = true;
    rtc_conf.interfaces.excludes = strings(&["docker0"]);
    rtc_conf.sctp_min_cwnd = 3;
    rtc_conf.sctp_fast_rtx_wnd = 4;
    rtc_conf.sctp_cwnd_ca_step = 5;
    rtc_conf.host_acceptance_min_wait = Duration::from_millis(100);
    rtc_conf.srflx_acceptance_min_wait = Duration::from_millis(200);
    rtc_conf.prflx_acceptance_min_wait = Duration::from_millis(300);
    rtc_conf.relay_acceptance_min_wait = Duration::from_millis(400);
    let conf = new_webrtc_config(&rtc_conf, false).unwrap();

    // The first port of the range, and the address set by hand in the place of the local
    // one.
    assert_eq!(
        "0.0.0.0:50000".parse::<SocketAddr>().unwrap(),
        conf.node_config.bind_address
    );
    assert_eq!(ips(&["203.0.113.5"]), conf.node_config.nat_1to1_ips);
    // Only what STUN found is kept here.
    assert!(conf.nat_1to1_ips.is_empty());
    assert!(conf.use_mdns);
    assert!(conf.ice_servers.is_empty());

    let s = &conf.setting_engine;
    assert_eq!(
        (3, 4, 5),
        (s.sctp_min_cwnd, s.sctp_fast_rtx_wnd, s.sctp_cwnd_ca_step)
    );
    let if_filter = s.interface_filter.as_ref().unwrap();
    assert!(!if_filter("docker0") && if_filter("eth0"));
    let ip_filter = s.ip_filter.as_ref().unwrap();
    assert!(ip_filter("127.0.0.1".parse().unwrap()) && !ip_filter("192.0.2.10".parse().unwrap()));
    assert_eq!(MulticastDnsMode::QueryOnly, s.ice_multicast_dns_mode);
    let engine = s.build();
    assert_eq!(MulticastDnsMode::QueryOnly, engine.ice_multicast_dns_mode);
    assert_eq!(
        vec![IceAddressRewriteRule {
            external: strings(&["203.0.113.5", "2001:db8::5"]),
            local: String::new(),
            as_candidate_type: IceCandidateType::Host,
            mode: IceAddressRewriteMode::Unspecified,
        }],
        s.ice_address_rewrite_rules
    );
    assert_eq!(Some((50000, 60000)), s.ephemeral_udp_port_range);
    assert_eq!(
        vec![
            NetworkType::Udp4,
            NetworkType::Udp6,
            NetworkType::Tcp4,
            NetworkType::Tcp6
        ],
        s.network_types
    );
    assert_eq!(
        [100, 200, 300, 400].map(Duration::from_millis),
        [
            s.host_acceptance_min_wait,
            s.srflx_acceptance_min_wait,
            s.prflx_acceptance_min_wait,
            s.relay_acceptance_min_wait
        ]
    );
    assert!(s.lite);

    // The layer's engine has the settings the layer has. There is no socket for TCP: those
    // types are not given to it.
    assert_eq!(
        vec![NetworkType::Udp4, NetworkType::Udp6],
        engine.network_types
    );
    assert_eq!(
        [100, 200, 300, 400].map(|ms| Some(Duration::from_millis(ms))),
        [
            engine.host_acceptance_min_wait,
            engine.srflx_acceptance_min_wait,
            engine.prflx_acceptance_min_wait,
            engine.relay_acceptance_min_wait
        ]
    );
    assert!(engine.lite);
    // A copy that is changed for one connection, as LiveKit changes its copy of pion's
    // engine: the addresses of that connection are worked out from the copy.
    assert_eq!(ips(&["203.0.113.5"]), s.host_candidate_ips().unwrap());
    let mut copy = s.clone();
    set_nat_1to1_address_rewrite_rules(&mut copy, &strings(&["203.0.113.5"]), true).unwrap();
    assert_eq!(
        ips(&["127.0.0.1", "203.0.113.5"]),
        copy.host_candidate_ips().unwrap()
    );

    // The local address is advertised with the one set by hand.
    rtc_conf.advertise_internal_ip = true;
    let conf = new_webrtc_config(&rtc_conf, false).unwrap();
    assert_eq!(
        ips(&["127.0.0.1", "203.0.113.5"]),
        conf.node_config.nat_1to1_ips
    );
    assert_eq!(
        IceAddressRewriteMode::Append,
        conf.setting_engine.ice_address_rewrite_rules[0].mode
    );

    // Without the loopback address there is nothing to advertise.
    rtc_conf.enable_loopback_candidate = false;
    let conf = new_webrtc_config(&rtc_conf, false).unwrap();
    assert!(conf.node_config.nat_1to1_ips.is_empty());
    assert!(!conf.setting_engine.include_loopback_candidate);
}

#[test]
fn test_new_webrtc_config_ports() {
    // New for musivum.

    // Neither a port nor a range (not validated): any port.
    let conf = new_webrtc_config(&loopback_only(RtcConfig::default()), true).unwrap();
    assert_eq!(0, conf.node_config.bind_address.port());
    assert_eq!(None, conf.setting_engine.ephemeral_udp_port_range);

    // Of a range in `udp_port`, the first port.
    let mut rtc_conf = loopback_only(RtcConfig::default());
    rtc_conf.udp_port = PortRange {
        start: 7882,
        end: 7890,
    };
    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    assert_eq!(7882, conf.node_config.bind_address.port());

    // A range comes before a port, as in the original.
    rtc_conf.ice_port_range_start = 40000;
    rtc_conf.ice_port_range_end = 40100;
    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    assert_eq!(40000, conf.node_config.bind_address.port());
    assert_eq!(
        Some((40000, 40100)),
        conf.setting_engine.ephemeral_udp_port_range
    );

    // A range that ends before it starts, and a port that is none.
    rtc_conf.ice_port_range_end = 39000;
    assert_eq!(
        "invalid port",
        new_webrtc_config(&rtc_conf, true)
            .err()
            .unwrap()
            .to_string()
    );
    let mut rtc_conf = loopback_only(RtcConfig::default());
    rtc_conf.udp_port.start = 70000;
    assert_eq!(
        "invalid port",
        new_webrtc_config(&rtc_conf, true)
            .err()
            .unwrap()
            .to_string()
    );

    // TCP forced: without a TCP port it is the original's error, with one only the TCP
    // network types are left, and the node's UDP socket takes any port.
    let mut rtc_conf = loopback_only(validated(true));
    rtc_conf.force_tcp = true;
    assert_eq!(
        "TCP is forced but not configured",
        new_webrtc_config(&rtc_conf, true)
            .err()
            .unwrap()
            .to_string()
    );
    rtc_conf.tcp_port = 7881;
    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    assert_eq!(0, conf.node_config.bind_address.port());
    assert_eq!(7881, conf.tcp_port);
    assert_eq!(
        vec![NetworkType::Tcp4, NetworkType::Tcp6],
        conf.setting_engine.network_types
    );

    // A TCP port beside UDP.
    rtc_conf.force_tcp = false;
    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    assert_eq!(7882, conf.node_config.bind_address.port());
    assert_eq!(7881, conf.tcp_port);
    assert_eq!(
        vec![
            NetworkType::Udp4,
            NetworkType::Udp6,
            NetworkType::Tcp4,
            NetworkType::Tcp6
        ],
        conf.setting_engine.network_types
    );

    // No TCP port: no TCP.
    rtc_conf.tcp_port = 0;
    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    assert_eq!(0, conf.tcp_port);
    assert_eq!(
        vec![NetworkType::Udp4, NetworkType::Udp6],
        conf.setting_engine.network_types
    );

    // A TCP port that is not one is refused, as `net.ListenTCP` refuses it.
    rtc_conf.tcp_port = 70000;
    assert!(new_webrtc_config(&rtc_conf, true).is_err());

    // A filter that is no network.
    let mut rtc_conf = loopback_only(validated(true));
    rtc_conf.ips.excludes = strings(&["10.0.0.1"]);
    assert_eq!(
        "invalid CIDR address: 10.0.0.1",
        new_webrtc_config(&rtc_conf, true)
            .err()
            .unwrap()
            .to_string()
    );
}

#[test]
fn test_new_node_listens_on_the_tcp_port() {
    // New for musivum: `new_node` opens the TCP listener of `tcp_port`, and none without.
    // A free port is found by binding port 0 and letting it go.
    let free_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut rtc_conf = loopback_only(validated(true));
    rtc_conf.udp_port = PortRange::default();
    rtc_conf.tcp_port = u32::from(free_port);
    let mut conf = new_webrtc_config(&rtc_conf, true).unwrap();
    conf.node_config.bind_address = "127.0.0.1:0".parse().unwrap();
    let node = conf.new_node().unwrap();
    assert_eq!(free_port, node.tcp_local_addr().unwrap().port());

    conf.tcp_port = 0;
    let node = conf.new_node().unwrap();
    assert!(node.tcp_local_addr().is_none());
}

#[test]
fn test_new_webrtc_config_says_what_is_not_applied() {
    // New for musivum: an option the layer cannot honour is named in the log, and one
    // that is not set is not.
    let mut rtc_conf = loopback_only(validated(true));
    rtc_conf.tcp_port = 7881;
    rtc_conf.use_stun_port_as_ice = true;
    rtc_conf.batch_io.batch_size = 16;
    rtc_conf.sctp_min_cwnd = 3;
    let log = capture_log(Level::WARN, || {
        new_webrtc_config(&rtc_conf, true).unwrap();
    });
    for option in [
        "use_stun_port_as_ice",
        "batch_io",
        "sctp_min_cwnd",
        "ips",
        "stun_servers",
    ] {
        assert!(
            log.lines().any(
                |line| line.contains("rtc option is accepted but not applied")
                    && line.contains(&format!("option=\"{option}")),
            ),
            "{option} is not in:\n{log}"
        );
    }
    for option in [
        "tcp_port",
        "force_tcp",
        "use_mdns",
        "port_range_start",
        "interfaces",
    ] {
        assert!(
            !log.contains(&format!("option=\"{option}")),
            "{option} is in:\n{log}"
        );
    }
}

#[test]
fn test_new_webrtc_config_external_ip() {
    // New for musivum: the external address through STUN, from `validate` to a node. The
    // STUN server is the test's own and the one local address is the loopback address, so
    // the address that is found and then tried from the outside is that one.
    let responder = StunResponder::reflecting();
    let port = free_port();
    let mut rtc_conf = loopback_only(RtcConfig {
        use_external_ip: true,
        stun_servers: vec![responder.server()],
        udp_port: PortRange {
            start: port as i64,
            end: 0,
        },
        use_ice_lite: true,
        ..Default::default()
    });
    rtc_conf.ips.includes = strings(&["127.0.0.1/32"]);
    rtc_conf.validate(true).unwrap();
    assert_eq!("127.0.0.1", rtc_conf.node_ip.v4);

    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    // Once for the node's address in `validate`, once for the local address here.
    assert_eq!(2, responder.requests());
    assert_eq!(strings(&["127.0.0.1/127.0.0.1"]), conf.nat_1to1_ips);
    assert_eq!(
        vec![IceAddressRewriteRule {
            external: strings(&["127.0.0.1"]),
            local: "127.0.0.1".to_string(),
            as_candidate_type: IceCandidateType::Host,
            mode: IceAddressRewriteMode::Unspecified,
        }],
        conf.setting_engine.ice_address_rewrite_rules
    );
    assert_eq!(
        SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port),
        conf.node_config.bind_address
    );
    assert_eq!(ips(&["127.0.0.1"]), conf.node_config.nat_1to1_ips);
    assert!(conf.ice_servers.is_empty());
    assert!(conf.setting_engine.lite);

    // With `external_ip_only` the filter lets through the IPv4 addresses an external
    // address was found for, and IPv6 addresses as before (here: none).
    rtc_conf.external_ip_only = true;
    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    let ip_filter = conf.setting_engine.ip_filter.as_ref().unwrap();
    assert!(ip_filter("127.0.0.1".parse().unwrap()));
    assert!(!ip_filter("127.0.0.2".parse().unwrap()));
    assert!(!ip_filter("::1".parse().unwrap()));

    // A server that cannot be asked (it has no address of the local address's family, so
    // nothing is sent): no external address, and the node's address from `validate` stands
    // for the local ones.
    rtc_conf.stun_servers = strings(&["[::1]:9"]);
    rtc_conf.external_ip_only = false;
    let conf = new_webrtc_config(&rtc_conf, true).unwrap();
    assert!(conf.nat_1to1_ips.is_empty());
    assert_eq!(
        strings(&["127.0.0.1"]),
        conf.setting_engine.ice_address_rewrite_rules[0].external
    );
    assert_eq!("", conf.setting_engine.ice_address_rewrite_rules[0].local);

    // What comes out is what a node is made from: on the loopback interface here, where
    // the configuration has all addresses.
    let node = Node::new(NodeConfig {
        bind_address: SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0),
        ..conf.node_config
    })
    .unwrap();
    assert_eq!(
        vec![SocketAddr::new(
            Ipv4Addr::LOCALHOST.into(),
            node.local_addr().port()
        )],
        node.candidates()
    );
    node.close();
}
