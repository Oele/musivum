// New for musivum: the original has no tests. The expected values of these tests are what
// the original gives for the same candidate lines and the same calls, printed by a Go test at
// the pin and copied here as text, in the form that test printed them. Where the parser of
// the WebRTC library answers differently than pion's, the last test says so.

use super::*;
use crate::testutils::capture_log;
use crate::webrtc::peerconnection::IceTcpCandidateType;

const HOST_UDP: &str = "candidate:1467250027 1 udp 2122260223 192.168.1.10 46243 typ host generation 0 ufrag EsAw network-id 1";
const SRFLX: &str = "candidate:842163049 1 udp 1686052607 203.0.113.7 46243 typ srflx raddr 192.168.1.10 rport 46243 generation 0 ufrag EsAw network-id 1";
// Three relay candidates as a browser gives them for a TURN server it reaches over UDP,
// TCP and TLS: the line says "udp" for all three (what is relayed is UDP), the priority
// tells them apart.
const RELAY_UDP: &str = "candidate:3745641921 1 udp 41885439 198.51.100.20 50123 typ relay raddr 203.0.113.7 rport 46243 generation 0 ufrag EsAw network-id 1";
const RELAY_TCP: &str = "candidate:3745641921 1 udp 25108223 198.51.100.20 50124 typ relay raddr 203.0.113.7 rport 51000 generation 0 ufrag EsAw network-id 1";
const RELAY_TLS: &str = "candidate:3745641921 1 udp 8331007 198.51.100.20 50125 typ relay raddr 203.0.113.7 rport 51001 generation 0 ufrag EsAw network-id 1";
const TCP_PASSIVE: &str = "candidate:2 1 tcp 1518280447 192.168.1.10 7881 typ host tcptype passive";
const TCP_ACTIVE: &str = "candidate:1876313031 1 tcp 1518280447 192.168.1.11 9 typ host tcptype active generation 0 ufrag EsAw network-id 1";
const MDNS: &str = "candidate:2880323124 1 udp 2113937151 1f4712db-ea17-4bcf-a596-105139dfd8bf.local 60542 typ host generation 0 ufrag EsAw network-cost 999";
const V6_LINK_LOCAL: &str = "candidate:3167052109 1 udp 2122197247 fe80::1c2b:3aff:fe4d:5e6f 55123 typ host generation 0 ufrag EsAw network-id 2";
const V6_ZONE: &str =
    "candidate:3167052109 1 udp 2122197247 fe80::1c2b:3aff:fe4d:5e6f%eth0 55123 typ host";
const PRFLX: &str =
    "candidate:9 1 udp 1853824767 198.51.100.20 61000 typ prflx raddr 192.168.1.10 rport 46243";
const PRFLX_OTHER: &str =
    "candidate:9 1 udp 1853824767 198.51.100.99 61000 typ prflx raddr 192.168.1.10 rport 46243";
const PRFLX_TCP: &str =
    "candidate:9 1 tcp 1853824767 198.51.100.20 61000 typ prflx raddr 192.168.1.10 rport 46243";
const UPPER: &str = "candidate:7 1 UDP 2122252543 192.0.2.33 3478 typ host";
const INVALID_NAME: &str = "candidate:8 1 udp 2113937151 redacted-ip.invalid 60542 typ host";
const NO_PREFIX: &str = "1467250027 1 udp 2122260223 192.168.1.10 46243 typ host";
const RELAY_OWN_TCP: &str = "candidate:5 1 tcp 8265215 198.51.100.20 50126 typ relay raddr 203.0.113.7 rport 9 tcptype passive";
const SRFLX_TCP: &str = "candidate:6 1 tcp 1010827775 203.0.113.7 9 typ srflx raddr 192.168.1.10 rport 9 tcptype active";
const TCP_TYPE_LATE: &str =
    "candidate:1 1 tcp 1518280447 192.168.1.10 9 typ host generation 0 tcptype active";
const HOST_UDP_PRIO: &str = "candidate:1467250027 1 udp 2122260222 192.168.1.10 46243 typ host generation 0 ufrag EsAw network-id 1";
const TOO_SHORT: &str = "candidate:1 1 udp 2122260223 192.168.1.10";
const BOGUS_TYP: &str = "candidate:1 1 udp 1 192.168.1.10 1 typ bogus";
const BAD_PORT: &str = "candidate:1 1 udp 1 192.168.1.10 port typ host";
const SSLTCP: &str = "candidate:1 1 ssltcp 1 192.168.1.10 443 typ host";
const NO_TYP: &str = "candidate:1 1 udp 2122260223 192.168.1.10 46243 xyz host";
const HOST_NAME: &str = "candidate:1 1 udp 1 example.org 1 typ host";

fn init(line: &str) -> IceCandidateInit {
    IceCandidateInit {
        candidate: line.to_string(),
        ..Default::default()
    }
}

fn m_ice(line: &str) -> ice::Candidate {
    unmarshal_ice_candidate(&init(line))
        .unwrap_or_else(|err| panic!("parse {line:?}: {err}"))
        .unwrap_or_else(|| panic!("parse {line:?}: no candidate"))
}

fn m_parse(line: &str) -> IceCandidate {
    unmarshal_candidate(&m_ice(line)).unwrap_or_else(|err| panic!("convert {line:?}: {err}"))
}

fn pair(local: &IceCandidate, remote: IceCandidate) -> IceCandidatePair {
    IceCandidatePair::new(local.clone(), remote)
}

// pion's `TCPType.String()`.
fn tcp_type(t: IceTcpCandidateType) -> &'static str {
    match t {
        IceTcpCandidateType::Active => "active",
        IceTcpCandidateType::Passive => "passive",
        IceTcpCandidateType::SimultaneousOpen => "so",
        _ => "",
    }
}

// What the Go test printed of a line: the candidate (or how it was not one), whether the
// parsed candidate and the line are mDNS.
fn m_line(line: &str) -> String {
    let init = init(line);
    let mut out = String::new();
    match unmarshal_ice_candidate(&init) {
        Err(_) => out.push_str("parse: err\n"),
        Ok(None) => out.push_str("parse: nil\n"),
        Ok(Some(c)) => {
            match unmarshal_candidate(&c) {
                Err(err) => out.push_str(&format!("convert: err={err}\n")),
                Ok(wc) => out.push_str(&format!(
                    "cand: net={} foundation={:?} priority={} address={:?} protocol={} port={} component={} typ={} tcptype={:?} raddr={:?} rport={}\n",
                    c.network_type(),
                    wc.foundation,
                    wc.priority,
                    wc.address,
                    wc.protocol,
                    wc.port,
                    wc.component,
                    wc.typ,
                    tcp_type(wc.tcp_type),
                    wc.related_address,
                    wc.related_port,
                )),
            }
            out.push_str(&format!("icemdns={}\n", is_ice_candidate_mdns(Some(&c))));
        }
    }
    out.push_str(&format!("mdns={}", is_candidate_mdns(&init)));
    out
}

fn m_ext(cs: &[IceCandidateExtended]) -> String {
    let parts: Vec<String> = cs
        .iter()
        .map(|c| {
            format!(
                "{}/{}/{}:{}/prio={}/sel={}/filt={}/trickle={}",
                c.candidate.typ,
                c.candidate.protocol,
                c.candidate.address,
                c.candidate.port,
                c.candidate.priority,
                c.selected_order,
                c.filtered,
                c.trickle,
            )
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

fn m_info(d: &IceConnectionDetails) -> String {
    let i = d.get_info();
    format!(
        "transport={} type={} conntype={} has={}\n      local={}\n      remote={}",
        i.transport.as_str_name(),
        i.r#type,
        d.get_connection_type(),
        i.has_candidates(),
        m_ext(&i.local),
        m_ext(&i.remote),
    )
}

fn new_details(transport: livekit::SignalTarget) -> IceConnectionDetails {
    IceConnectionDetails::new(transport, crate::protocol::logger::get_logger())
}

#[test]
fn candidate_lines_are_read_as_the_original_reads_them() {
    let lines = [
        (
            "host_udp",
            HOST_UDP,
            "cand: net=udp4 foundation=\"1467250027\" priority=2122260223 address=\"192.168.1.10\" protocol=udp port=46243 component=1 typ=host tcptype=\"\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        ),
        (
            "srflx",
            SRFLX,
            "cand: net=udp4 foundation=\"842163049\" priority=1686052607 address=\"203.0.113.7\" protocol=udp port=46243 component=1 typ=srflx tcptype=\"\" raddr=\"192.168.1.10\" rport=46243\nicemdns=false\nmdns=false",
        ),
        (
            "relay_udp",
            RELAY_UDP,
            "cand: net=udp4 foundation=\"3745641921\" priority=41885439 address=\"198.51.100.20\" protocol=udp port=50123 component=1 typ=relay tcptype=\"\" raddr=\"203.0.113.7\" rport=46243\nicemdns=false\nmdns=false",
        ),
        (
            "relay_tcp",
            RELAY_TCP,
            "cand: net=udp4 foundation=\"3745641921\" priority=25108223 address=\"198.51.100.20\" protocol=udp port=50124 component=1 typ=relay tcptype=\"\" raddr=\"203.0.113.7\" rport=51000\nicemdns=false\nmdns=false",
        ),
        (
            "relay_tls",
            RELAY_TLS,
            "cand: net=udp4 foundation=\"3745641921\" priority=8331007 address=\"198.51.100.20\" protocol=udp port=50125 component=1 typ=relay tcptype=\"\" raddr=\"203.0.113.7\" rport=51001\nicemdns=false\nmdns=false",
        ),
        (
            "tcp_passive",
            TCP_PASSIVE,
            "cand: net=tcp4 foundation=\"2\" priority=1518280447 address=\"192.168.1.10\" protocol=tcp port=7881 component=1 typ=host tcptype=\"passive\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        ),
        (
            "tcp_active",
            TCP_ACTIVE,
            "cand: net=tcp4 foundation=\"1876313031\" priority=1518280447 address=\"192.168.1.11\" protocol=tcp port=9 component=1 typ=host tcptype=\"active\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        ),
        (
            "mdns",
            MDNS,
            "cand: net=udp4 foundation=\"2880323124\" priority=2113937151 address=\"1f4712db-ea17-4bcf-a596-105139dfd8bf.local\" protocol=udp port=60542 component=1 typ=host tcptype=\"\" raddr=\"\" rport=0\nicemdns=true\nmdns=true",
        ),
        (
            "v6_link_local",
            V6_LINK_LOCAL,
            "cand: net=udp6 foundation=\"3167052109\" priority=2122197247 address=\"fe80::1c2b:3aff:fe4d:5e6f\" protocol=udp port=55123 component=1 typ=host tcptype=\"\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        ),
        (
            "prflx",
            PRFLX,
            "cand: net=udp4 foundation=\"9\" priority=1853824767 address=\"198.51.100.20\" protocol=udp port=61000 component=1 typ=prflx tcptype=\"\" raddr=\"192.168.1.10\" rport=46243\nicemdns=false\nmdns=false",
        ),
        (
            "upper",
            UPPER,
            "cand: net=udp4 foundation=\"7\" priority=2122252543 address=\"192.0.2.33\" protocol=udp port=3478 component=1 typ=host tcptype=\"\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        ),
        (
            "no_prefix",
            NO_PREFIX,
            "cand: net=udp4 foundation=\"1467250027\" priority=2122260223 address=\"192.168.1.10\" protocol=udp port=46243 component=1 typ=host tcptype=\"\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        ),
        (
            "relay_own_tcp",
            RELAY_OWN_TCP,
            "cand: net=tcp4 foundation=\"5\" priority=8265215 address=\"198.51.100.20\" protocol=tcp port=50126 component=1 typ=relay tcptype=\"\" raddr=\"203.0.113.7\" rport=9\nicemdns=false\nmdns=false",
        ),
        (
            "srflx_tcp",
            SRFLX_TCP,
            "cand: net=tcp4 foundation=\"6\" priority=1010827775 address=\"203.0.113.7\" protocol=tcp port=9 component=1 typ=srflx tcptype=\"\" raddr=\"192.168.1.10\" rport=9\nicemdns=false\nmdns=false",
        ),
        ("empty", "", "parse: nil\nmdns=false"),
        ("prefix_only", "candidate:", "parse: nil\nmdns=false"),
        // The texts of the errors are the library's, not pion's, and are not compared.
        ("too_short", TOO_SHORT, "parse: err\nmdns=false"),
        ("bogus_typ", BOGUS_TYP, "parse: err\nmdns=false"),
        ("bad_port", BAD_PORT, "parse: err\nmdns=false"),
        ("ssltcp", SSLTCP, "parse: err\nmdns=false"),
        ("host_name", HOST_NAME, "parse: err\nmdns=false"),
    ];
    for (name, line, expected) in lines {
        assert_eq!(expected, m_line(line), "{name}");
    }
    assert!(!is_ice_candidate_mdns(None));
}

#[test]
fn the_connection_types_have_the_names_and_the_order_of_the_original() {
    assert_eq!(
        "unknown udp tcp turn",
        format!(
            "{} {} {} {}",
            IceConnectionType::Unknown,
            IceConnectionType::Udp,
            IceConnectionType::Tcp,
            IceConnectionType::Turn
        )
    );
    assert!(IceConnectionType::Unknown < IceConnectionType::Udp);
    assert!(IceConnectionType::Udp < IceConnectionType::Tcp);
    assert!(IceConnectionType::Tcp < IceConnectionType::Turn);
    assert_eq!(IceConnectionType::Unknown, IceConnectionType::default());

    // S9: the type of the first info that has one
    let info = |r#type| IceConnectionInfo {
        r#type,
        ..Default::default()
    };
    let u = info(IceConnectionType::Unknown);
    let tc = info(IceConnectionType::Tcp);
    let ud = info(IceConnectionType::Udp);
    assert_eq!(IceConnectionType::Unknown, ice_connection_infos_type(&[]));
    assert_eq!(
        IceConnectionType::Unknown,
        ice_connection_infos_type(std::slice::from_ref(&u))
    );
    assert_eq!(
        IceConnectionType::Tcp,
        ice_connection_infos_type(&[u, tc.clone(), ud.clone()])
    );
    assert_eq!(IceConnectionType::Udp, ice_connection_infos_type(&[ud, tc]));
}

#[test]
fn local_candidates_are_kept_once() {
    // S1
    let d = new_details(livekit::SignalTarget::Subscriber);
    assert_eq!(
        "transport=SUBSCRIBER type=unknown conntype=unknown has=false\n      local=[]\n      remote=[]",
        m_info(&d)
    );

    // S2: local candidates
    let d = new_details(livekit::SignalTarget::Publisher);
    d.add_local_ice_candidate(&m_ice(HOST_UDP), false, false);
    d.add_local_ice_candidate(&m_ice(HOST_UDP), true, true);
    d.add_local_candidate(&m_parse(TCP_PASSIVE), true, true);
    d.add_local_candidate(&m_parse(TCP_PASSIVE), false, false);
    d.add_local_candidate(&m_parse(HOST_UDP_PRIO), false, true);
    assert_eq!(
        "transport=PUBLISHER type=unknown conntype=unknown has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=false/trickle=false, host/tcp/192.168.1.10:7881/prio=1518280447/sel=0/filt=true/trickle=true, host/udp/192.168.1.10:46243/prio=2122260222/sel=0/filt=false/trickle=true]\n      remote=[]",
        m_info(&d)
    );
}

#[test]
fn remote_candidates_are_kept_once_and_updated_when_asked() {
    // S3: remote candidates
    let d = new_details(livekit::SignalTarget::Publisher);
    d.add_remote_candidate(&init(HOST_UDP), false, true, false);
    assert_eq!(
        "transport=PUBLISHER type=unknown conntype=unknown has=true\n      local=[]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S3a"
    );
    d.add_remote_candidate(&init(HOST_UDP), true, false, false);
    assert_eq!(
        "transport=PUBLISHER type=unknown conntype=unknown has=true\n      local=[]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S3b"
    );
    d.add_remote_candidate(&init(HOST_UDP), true, false, true);
    assert_eq!(
        "transport=PUBLISHER type=unknown conntype=unknown has=true\n      local=[]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=true/trickle=false]",
        m_info(&d),
        "S3c"
    );
    d.add_remote_candidate(&init(""), false, true, false);
    let logged = capture_log(tracing::Level::DEBUG, || {
        d.add_remote_candidate(&init(TOO_SHORT), false, true, false);
    });
    assert!(logged.contains("could not unmarshal candidate"), "{logged}");
    d.add_remote_ice_candidate(None, false, true, false);
    d.add_remote_ice_candidate(Some(&m_ice(MDNS)), true, false, false);
    d.add_remote_ice_candidate(Some(&m_ice(V6_LINK_LOCAL)), false, false, false);
    assert_eq!(
        "transport=PUBLISHER type=unknown conntype=unknown has=true\n      local=[]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=true/trickle=false, host/udp/1f4712db-ea17-4bcf-a596-105139dfd8bf.local:60542/prio=2113937151/sel=0/filt=true/trickle=false, host/udp/fe80::1c2b:3aff:fe4d:5e6f:55123/prio=2122197247/sel=0/filt=false/trickle=false]",
        m_info(&d),
        "S3d"
    );
}

#[test]
fn the_selected_pair_gives_the_type_of_the_connection() {
    // S4: selected pairs
    let d = new_details(livekit::SignalTarget::Publisher);
    let l1 = m_parse(HOST_UDP);
    let l2 = m_parse(TCP_PASSIVE);
    d.add_local_candidate(&l1, false, true);
    d.add_local_candidate(&l2, false, true);
    for r in [HOST_UDP, SRFLX, RELAY_UDP, TCP_ACTIVE, RELAY_TCP, RELAY_TLS] {
        d.add_remote_candidate(&init(r), false, true, false);
    }
    assert_eq!(
        "transport=PUBLISHER type=unknown conntype=unknown has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=false/trickle=true, host/tcp/192.168.1.10:7881/prio=1518280447/sel=0/filt=false/trickle=true]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=false/trickle=true, srflx/udp/203.0.113.7:46243/prio=1686052607/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50123/prio=41885439/sel=0/filt=false/trickle=true, host/tcp/192.168.1.11:9/prio=1518280447/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50124/prio=25108223/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50125/prio=8331007/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S4"
    );
    d.set_selected_pair(&pair(&l1, m_parse(HOST_UDP)));
    assert_eq!(
        "transport=PUBLISHER type=udp conntype=udp has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=true, host/tcp/192.168.1.10:7881/prio=1518280447/sel=0/filt=false/trickle=true]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=true, srflx/udp/203.0.113.7:46243/prio=1686052607/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50123/prio=41885439/sel=0/filt=false/trickle=true, host/tcp/192.168.1.11:9/prio=1518280447/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50124/prio=25108223/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50125/prio=8331007/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S4a"
    );
    d.set_selected_pair(&pair(&l2, m_parse(TCP_ACTIVE)));
    assert_eq!(
        "transport=PUBLISHER type=tcp conntype=tcp has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=true, host/tcp/192.168.1.10:7881/prio=1518280447/sel=2/filt=false/trickle=true]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=true, srflx/udp/203.0.113.7:46243/prio=1686052607/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50123/prio=41885439/sel=0/filt=false/trickle=true, host/tcp/192.168.1.11:9/prio=1518280447/sel=2/filt=false/trickle=true, relay/udp/198.51.100.20:50124/prio=25108223/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50125/prio=8331007/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S4b"
    );
    d.set_selected_pair(&pair(&l1, m_parse(RELAY_UDP)));
    assert_eq!(
        "transport=PUBLISHER type=turn conntype=turn has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=3/filt=false/trickle=true, host/tcp/192.168.1.10:7881/prio=1518280447/sel=2/filt=false/trickle=true]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=true, srflx/udp/203.0.113.7:46243/prio=1686052607/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50123/prio=41885439/sel=3/filt=false/trickle=true, host/tcp/192.168.1.11:9/prio=1518280447/sel=2/filt=false/trickle=true, relay/udp/198.51.100.20:50124/prio=25108223/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50125/prio=8331007/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S4c"
    );
    d.set_selected_pair(&pair(&l1, m_parse(SRFLX)));
    assert_eq!(
        "transport=PUBLISHER type=udp conntype=udp has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=4/filt=false/trickle=true, host/tcp/192.168.1.10:7881/prio=1518280447/sel=2/filt=false/trickle=true]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=true, srflx/udp/203.0.113.7:46243/prio=1686052607/sel=4/filt=false/trickle=true, relay/udp/198.51.100.20:50123/prio=41885439/sel=3/filt=false/trickle=true, host/tcp/192.168.1.11:9/prio=1518280447/sel=2/filt=false/trickle=true, relay/udp/198.51.100.20:50124/prio=25108223/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50125/prio=8331007/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S4d"
    );
    d.set_selected_pair(&pair(&l1, m_parse(RELAY_TLS)));
    assert_eq!(
        "transport=PUBLISHER type=turn conntype=turn has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=5/filt=false/trickle=true, host/tcp/192.168.1.10:7881/prio=1518280447/sel=2/filt=false/trickle=true]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=true, srflx/udp/203.0.113.7:46243/prio=1686052607/sel=4/filt=false/trickle=true, relay/udp/198.51.100.20:50123/prio=41885439/sel=3/filt=false/trickle=true, host/tcp/192.168.1.11:9/prio=1518280447/sel=2/filt=false/trickle=true, relay/udp/198.51.100.20:50124/prio=25108223/sel=0/filt=false/trickle=true, relay/udp/198.51.100.20:50125/prio=8331007/sel=5/filt=false/trickle=true]",
        m_info(&d),
        "S4e"
    );
    // Clear does not reset the count of selections
    d.clear();
    assert_eq!(
        "transport=PUBLISHER type=unknown conntype=unknown has=false\n      local=[]\n      remote=[]",
        m_info(&d),
        "S4f"
    );
    d.add_local_candidate(&l1, false, false);
    d.set_selected_pair(&pair(&l1, m_parse(HOST_UDP)));
    assert_eq!(
        "transport=PUBLISHER type=udp conntype=udp has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=6/filt=false/trickle=false]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=6/filt=false/trickle=false]",
        m_info(&d),
        "S4g"
    );
}

#[test]
fn a_peer_reflexive_candidate_at_the_address_of_a_relay_candidate_is_turn() {
    let l1 = m_parse(HOST_UDP);
    let l2 = m_parse(TCP_PASSIVE);

    // S5: a peer reflexive candidate at the address of a relay candidate
    let d = new_details(livekit::SignalTarget::Subscriber);
    d.add_local_candidate(&l1, false, false);
    d.add_remote_candidate(&init(RELAY_UDP), false, true, false);
    d.set_selected_pair(&pair(&l1, m_parse(PRFLX)));
    assert_eq!(
        "transport=SUBSCRIBER type=turn conntype=turn has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=false]\n      remote=[relay/udp/198.51.100.20:50123/prio=41885439/sel=0/filt=false/trickle=true, prflx/udp/198.51.100.20:61000/prio=1853824767/sel=1/filt=false/trickle=false]",
        m_info(&d),
        "S5"
    );

    // S6: a peer reflexive candidate and no relay candidate, which then comes
    let d = new_details(livekit::SignalTarget::Subscriber);
    d.add_local_candidate(&l1, false, false);
    d.add_remote_candidate(&init(HOST_UDP), false, true, false);
    d.set_selected_pair(&pair(&l1, m_parse(PRFLX)));
    assert_eq!(
        "transport=SUBSCRIBER type=udp conntype=udp has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=false]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=false/trickle=true, prflx/udp/198.51.100.20:61000/prio=1853824767/sel=1/filt=false/trickle=false]",
        m_info(&d),
        "S6a"
    );
    d.add_remote_candidate(&init(RELAY_UDP), false, true, false);
    assert_eq!(
        "transport=SUBSCRIBER type=turn conntype=turn has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=false]\n      remote=[host/udp/192.168.1.10:46243/prio=2122260223/sel=0/filt=false/trickle=true, prflx/udp/198.51.100.20:61000/prio=1853824767/sel=1/filt=false/trickle=false, relay/udp/198.51.100.20:50123/prio=41885439/sel=0/filt=false/trickle=true]",
        m_info(&d),
        "S6b"
    );

    // S7: peer reflexive candidates that match no relay candidate: another address, another protocol
    let d = new_details(livekit::SignalTarget::Subscriber);
    d.add_local_candidate(&l1, false, false);
    d.add_local_candidate(&l2, false, false);
    d.add_remote_candidate(&init(RELAY_UDP), false, true, false);
    d.set_selected_pair(&pair(&l1, m_parse(PRFLX_OTHER)));
    assert_eq!(
        "transport=SUBSCRIBER type=udp conntype=udp has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=false, host/tcp/192.168.1.10:7881/prio=1518280447/sel=0/filt=false/trickle=false]\n      remote=[relay/udp/198.51.100.20:50123/prio=41885439/sel=0/filt=false/trickle=true, prflx/udp/198.51.100.99:61000/prio=1853824767/sel=1/filt=false/trickle=false]",
        m_info(&d),
        "S7a"
    );
    d.set_selected_pair(&pair(&l2, m_parse(PRFLX_TCP)));
    assert_eq!(
        "transport=SUBSCRIBER type=tcp conntype=tcp has=true\n      local=[host/udp/192.168.1.10:46243/prio=2122260223/sel=1/filt=false/trickle=false, host/tcp/192.168.1.10:7881/prio=1518280447/sel=2/filt=false/trickle=false]\n      remote=[relay/udp/198.51.100.20:50123/prio=41885439/sel=0/filt=false/trickle=true, prflx/udp/198.51.100.99:61000/prio=1853824767/sel=1/filt=false/trickle=false, prflx/tcp/198.51.100.20:61000/prio=1853824767/sel=2/filt=false/trickle=false]",
        m_info(&d),
        "S7b"
    );
}

#[test]
fn a_selected_local_candidate_that_is_not_known_is_logged() {
    let l1 = m_parse(HOST_UDP);
    let l2 = m_parse(TCP_PASSIVE);

    // S8: a local candidate that is not known
    let d = new_details(livekit::SignalTarget::Subscriber);
    d.add_local_candidate(&l2, false, false);
    d.add_remote_candidate(&init(TCP_ACTIVE), false, true, false);
    let logged = capture_log(tracing::Level::DEBUG, || {
        d.set_selected_pair(&pair(&l1, m_parse(TCP_ACTIVE)));
    });
    assert!(
        logged.contains("could not match local candidate"),
        "{logged}"
    );
    assert_eq!(
        "transport=SUBSCRIBER type=tcp conntype=tcp has=true\n      local=[host/tcp/192.168.1.10:7881/prio=1518280447/sel=0/filt=false/trickle=false]\n      remote=[host/tcp/192.168.1.11:9/prio=1518280447/sel=1/filt=false/trickle=true]",
        m_info(&d)
    );
}

// Where `rtc` 0.21.1 reads a candidate line differently than pion's `ice.UnmarshalCandidate`
// (LiveKit's fork of it, v4.4.0-warp.3). The values asserted are the library's; what the
// original gives for the same line is in the comment above each. When the library changes,
// this test fails and the difference is gone from the list.
#[test]
fn where_the_library_reads_a_candidate_line_differently() {
    // The original takes the zone off an IPv6 address:
    // cand: net=udp6 foundation="3167052109" priority=2122197247 address="fe80::1c2b:3aff:fe4d:5e6f" protocol=udp port=55123 component=1 typ=host tcptype="" raddr="" rport=0, icemdns=false, mdns=false
    assert_eq!("parse: err\nmdns=false", m_line(V6_ZONE));

    // The original takes a host name that ends in ".invalid" as it takes one that ends in
    // ".local", so that it counts as mDNS:
    // cand: net=udp4 foundation="8" priority=2113937151 address="redacted-ip.invalid" protocol=udp port=60542 component=1 typ=host tcptype="" raddr="" rport=0, icemdns=true, mdns=true
    assert_eq!("parse: err\nmdns=false", m_line(INVALID_NAME));

    // The original finds "tcptype" wherever it stands among the extensions: tcptype="active".
    assert_eq!(
        "cand: net=tcp4 foundation=\"1\" priority=1518280447 address=\"192.168.1.10\" protocol=tcp port=9 component=1 typ=host tcptype=\"\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        m_line(TCP_TYPE_LATE)
    );

    // The original insists on the word "typ": parse: err=unknown candidate typ (xyz).
    assert_eq!(
        "cand: net=udp4 foundation=\"1\" priority=2122260223 address=\"192.168.1.10\" protocol=udp port=46243 component=1 typ=host tcptype=\"\" raddr=\"\" rport=0\nicemdns=false\nmdns=false",
        m_line(NO_TYP)
    );
}
