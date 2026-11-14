//! New for musivum: the addresses and ports a node listens at. Two connections of the layer
//! over IPv6 only, over UDP and over TCP; both families at one node, and the settings that
//! leave one out; an answer that leaves from the address it was asked at, on a node with two
//! addresses; the ports of a node given to its connections in turn; the standalone port.
//! All on the loopback interface (127.0.0.1, 127.0.0.2, ::1) on ports the system chooses,
//! except a range, which is taken from free ports from 39000 on.

use std::net::{IpAddr, UdpSocket};

use super::*;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

/// A node at these addresses, each advertised as it is, with one port at each.
fn node_at(addresses: &[&str]) -> NodeConfig {
    NodeConfig {
        addresses: addresses.iter().map(|a| NodeAddress::new(ip(a))).collect(),
        ..Default::default()
    }
}

/// A server's connection (ICE lite, the DTLS server) with these network types.
fn server_with(node: &Node, network_types: Vec<NetworkType>) -> Peer {
    Peer::new(
        node,
        SettingEngine {
            lite: true,
            answering_dtls_role: DtlsRole::Server,
            network_types,
            ..Default::default()
        },
    )
}

/// A client's connection with these network types, which dials TCP.
fn client_with(node: &Node, network_types: Vec<NetworkType>) -> Peer {
    Peer::new(
        node,
        SettingEngine {
            network_types,
            active_tcp: true,
            ..Default::default()
        },
    )
}

/// The candidates of a description: protocol, address and port. `rtc` writes each for the
/// second component too, which is left out here.
fn candidates_of(sdp: &str) -> Vec<(String, SocketAddr)> {
    sdp.lines()
        .filter_map(|l| l.trim_end().strip_prefix("a=candidate:"))
        .map(|c| c.split(' ').collect::<Vec<&str>>())
        .filter(|fields| fields[1] == "1")
        .map(|fields| {
            let addr = SocketAddr::new(ip(fields[4]), fields[5].parse().unwrap());
            (fields[2].to_lowercase(), addr)
        })
        .collect()
}

fn sorted(mut v: Vec<(String, SocketAddr)>) -> Vec<(String, SocketAddr)> {
    v.sort();
    v
}

/// The client offers a data channel and the server answers; the client is given the answer
/// with the candidate lines `keep` keeps. Waits until both are connected and the channel
/// has echoed. Gives the answer as the server set it.
async fn connect_and_echo(client: &Peer, server: &Peer, keep: impl Fn(&str) -> bool) -> String {
    let _ = echo_incoming(server);
    let channel = client.pc.create_data_channel("data", None).unwrap();
    let log = ChannelLog::watch(&channel);
    let offer = client.pc.create_offer(None).unwrap();
    client.pc.set_local_description(offer).unwrap();
    server
        .pc
        .set_remote_description(client.pc.local_description().unwrap())
        .unwrap();
    let answer = server.pc.create_answer().unwrap();
    server.pc.set_local_description(answer).unwrap();
    let answer = server.pc.local_description().unwrap();
    let mut kept = String::new();
    for line in answer.sdp.split_inclusive('\n') {
        if !line.starts_with("a=candidate:") || keep(line) {
            kept.push_str(line);
        }
    }
    client
        .pc
        .set_remote_description(SessionDescription {
            r#type: answer.r#type,
            sdp: kept,
        })
        .unwrap();

    connected(client, server).await;
    eventually_async(|| log.is_open(), WAIT, TICK).await;
    channel.send(b"there and back").unwrap();
    eventually_async(
        || {
            log.messages
                .lock()
                .iter()
                .any(|m| &m.data[..] == b"there and back")
        },
        WAIT,
        TICK,
    )
    .await;
    answer.sdp
}

/// The local candidate of the pair a connection has selected: protocol, address, port.
fn selected_local(peer: &Peer) -> (IceProtocol, SocketAddr) {
    let pair = peer.pc.get_selected_candidate_pair().unwrap();
    let local = pair.local();
    (
        local.protocol,
        SocketAddr::new(ip(&local.address), local.port),
    )
}

async fn close_all(node: &Node, peers: &[&Peer]) {
    for peer in peers {
        peer.pc.close().unwrap();
    }
    eventually_async(|| node.connection_count() == 0, WAIT, TICK).await;
}

/// Over IPv6 only: a server's node at `::1`, a client's at `::1`, over UDP.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ipv6_over_udp() {
    let node = Node::new(node_at(&["::1"])).unwrap();
    let at = node.local_addr();
    assert_eq!(at.ip(), ip("::1"));
    let server = Peer::server_on(&node);
    let client = Peer::new(
        &Node::new(node_at(&["::1"])).unwrap(),
        SettingEngine::default(),
    );

    let answer = connect_and_echo(&client, &server, |_| true).await;
    assert_eq!(candidates_of(&answer), vec![("udp".to_string(), at)]);
    assert_eq!(selected_local(&server), (IceProtocol::Udp, at));
    for stats in [node.stats(), client.node.stats()] {
        assert_eq!(stats.datagrams_without_route, 0, "{stats:?}");
        assert_eq!(stats.datagrams_not_sent, 0, "{stats:?}");
    }
    close_all(&node, &[&server, &client]).await;
}

/// Over IPv6 only, over TCP: the server's node listens at `::1`, the client dials it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ipv6_over_tcp() {
    let node = Node::new_with_tcp(node_at(&["::1"]), 0).unwrap();
    let at = node.tcp_local_addr().unwrap();
    assert_eq!(at.ip(), ip("::1"));
    let server = server_with(&node, vec![NetworkType::Tcp6]);
    let client = client_with(
        &Node::new(node_at(&["::1"])).unwrap(),
        vec![NetworkType::Tcp6],
    );

    let answer = connect_and_echo(&client, &server, |_| true).await;
    assert_eq!(candidates_of(&answer), vec![("tcp".to_string(), at)]);
    assert!(answer.contains(" typ host tcptype passive"), "{answer}");
    assert_eq!(selected_local(&server), (IceProtocol::Tcp, at));
    assert_eq!(node.stats().tcp_streams_opened, 1);
    assert_eq!(client.node.stats().tcp_streams_opened, 1);
    close_all(&node, &[&server, &client]).await;
}

/// A node at an IPv4 and an IPv6 address, with TCP: a connection advertises both families
/// over both protocols unless its settings leave some out (network types, or addresses of
/// its own), and a client of each family connects to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_both_families_on_one_node() {
    let node = Node::new_with_tcp(node_at(&["127.0.0.1", "::1"]), 0).unwrap();
    let [udp4, udp6] = node.local_addrs()[..] else {
        panic!("{:?}", node.local_addrs());
    };
    let [tcp4, tcp6] = node.tcp_local_addrs()[..] else {
        panic!("{:?}", node.tcp_local_addrs());
    };
    assert!(udp4.is_ipv4() && tcp4.is_ipv4() && udp6.is_ipv6() && tcp6.is_ipv6());
    let udp = |a| ("udp".to_string(), a);
    let tcp = |a| ("tcp".to_string(), a);

    // What a connection with these settings advertises in an offer of its own.
    let advertised = |setting_engine: SettingEngine| {
        let pc = PeerConnection::new(
            &node,
            Configuration {
                setting_engine,
                ..Default::default()
            },
        )
        .unwrap();
        pc.create_data_channel("data", None).unwrap();
        let offer = pc.create_offer(None).unwrap();
        pc.set_local_description(offer).unwrap();
        let candidates = candidates_of(&pc.local_description().unwrap().sdp);
        pc.close().unwrap();
        sorted(candidates)
    };
    let all = sorted(vec![udp(udp4), udp(udp6), tcp(tcp4), tcp(tcp6)]);
    assert_eq!(advertised(SettingEngine::default()), all);
    let types = |network_types: &[NetworkType]| SettingEngine {
        network_types: network_types.to_vec(),
        ..Default::default()
    };
    use NetworkType::{Tcp4, Tcp6, Udp4, Udp6};
    assert_eq!(advertised(types(&[Udp4, Udp6, Tcp4, Tcp6])), all);
    assert_eq!(advertised(types(&[Udp4])), vec![udp(udp4)]);
    assert_eq!(
        advertised(types(&[Udp6, Tcp6])),
        sorted(vec![udp(udp6), tcp(tcp6)])
    );
    assert_eq!(
        advertised(SettingEngine {
            host_candidate_ips: Some(vec![ip("::1")]),
            ..Default::default()
        }),
        sorted(vec![udp(udp6), tcp(tcp6)])
    );

    // A client of each family, each at a server connection of its own on this node.
    let servers = [Peer::server_on(&node), Peer::server_on(&node)];
    let clients = [
        Peer::new(&Node::new(node_at(&["127.0.0.1"])).unwrap(), types(&[Udp4])),
        Peer::new(&Node::new(node_at(&["::1"])).unwrap(), types(&[Udp6])),
    ];
    for (client, server) in clients.iter().zip(&servers) {
        connect_and_echo(client, server, |_| true).await;
    }
    assert_eq!(selected_local(&servers[0]), (IceProtocol::Udp, udp4));
    assert_eq!(selected_local(&servers[1]), (IceProtocol::Udp, udp6));
    assert_eq!(node.stats().datagrams_without_route, 0);
    close_all(&node, &[&servers[0], &servers[1], &clients[0], &clients[1]]).await;
}

/// A node at two addresses has a socket at each, and a connection answers from the address
/// it was asked at. The client is told of the second address only; an answer from the first
/// would reach no connection on its node (which takes datagrams from the addresses it sent
/// to, as a browser's agent takes answers), and the pair would come up, if at all, as a
/// peer-reflexive one with the first address, which on a host whose first address the
/// client cannot reach would not be there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_answers_leave_from_the_address_asked() {
    let node = Node::new(node_at(&["127.0.0.1", "127.0.0.2"])).unwrap();
    let [first, second] = node.local_addrs()[..] else {
        panic!("{:?}", node.local_addrs());
    };
    assert_eq!(
        (first.ip(), second.ip()),
        (ip("127.0.0.1"), ip("127.0.0.2"))
    );
    let server = Peer::server_on(&node);
    let client = Peer::client();

    let only_second = format!(" {} {} typ host", second.ip(), second.port());
    let answer = connect_and_echo(&client, &server, |line| line.contains(&only_second)).await;
    assert_eq!(
        candidates_of(&answer),
        vec![("udp".to_string(), first), ("udp".to_string(), second)]
    );
    assert_eq!(selected_local(&server), (IceProtocol::Udp, second));
    let pair = client.pc.get_selected_candidate_pair().unwrap();
    assert_eq!(
        SocketAddr::new(ip(&pair.remote().address), pair.remote().port),
        second
    );
    assert_eq!(client.node.stats().datagrams_without_route, 0);
    close_all(&node, &[&server, &client]).await;
}

/// The ports of a node are given to its connections in turn, and a connection is reached at
/// the port it was given. A range is bound on the first ports of it that are free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ports_are_given_out_in_turn() {
    let node = Node::new(NodeConfig {
        udp_ports: UdpPorts::Ports(vec![0, 0]),
        ..Default::default()
    })
    .unwrap();
    let [first, second] = node.local_addrs()[..] else {
        panic!("{:?}", node.local_addrs());
    };
    assert_ne!(first.port(), second.port());
    assert_eq!(node.candidates(), vec![first, second]);
    let servers = [
        Peer::server_on(&node),
        Peer::server_on(&node),
        Peer::server_on(&node),
    ];
    let given: Vec<Vec<SocketAddr>> = servers
        .iter()
        .map(|s| {
            s.pc.inner()
                .host_candidates
                .iter()
                .map(|c| c.addr)
                .collect()
        })
        .collect();
    assert_eq!(given, vec![vec![first], vec![second], vec![first]]);

    let client = Peer::client();
    let answer = connect_and_echo(&client, &servers[1], |_| true).await;
    assert_eq!(candidates_of(&answer), vec![("udp".to_string(), second)]);
    assert_eq!(selected_local(&servers[1]), (IceProtocol::Udp, second));
    close_all(&node, &[&servers[0], &servers[1], &servers[2], &client]).await;

    // A range: past a port that is taken, the first two that are free.
    let (taken, start) = (39000..39900)
        .find_map(|port| {
            UdpSocket::bind(("127.0.0.1", port))
                .ok()
                .map(|socket| (socket, port))
        })
        .expect("a free port from 39000 on");
    let ranged = Node::new(NodeConfig {
        udp_ports: UdpPorts::Range {
            start,
            end: start + 50,
            count: 2,
        },
        ..Default::default()
    })
    .unwrap();
    let ports: Vec<u16> = ranged.local_addrs().iter().map(SocketAddr::port).collect();
    assert_eq!(ports.len(), 2, "{ports:?}");
    assert!(
        ports.iter().all(|p| *p > start && *p <= start + 50) && ports[0] < ports[1],
        "{ports:?} from {start}"
    );
    ranged.close();
    // A range without a free port.
    assert!(
        Node::new(NodeConfig {
            udp_ports: UdpPorts::Range {
                start,
                end: start,
                count: 1,
            },
            ..Default::default()
        })
        .is_err()
    );
    drop(taken);
}

/// A standalone port (LiveKit's `use_stun_port_as_ice`, 3478, here one of the system's
/// choosing) is every connection's, beside the port it is given, and it is reached there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_standalone_port() {
    let node = Node::new(NodeConfig {
        standalone_udp_ports: vec![0],
        ..Default::default()
    })
    .unwrap();
    let [port, standalone] = node.local_addrs()[..] else {
        panic!("{:?}", node.local_addrs());
    };
    let other = Peer::server_on(&node);
    let other_given: Vec<SocketAddr> = other
        .pc
        .inner()
        .host_candidates
        .iter()
        .map(|c| c.addr)
        .collect();
    assert_eq!(other_given, vec![port, standalone]);

    let server = Peer::server_on(&node);
    let client = Peer::client();
    let only_standalone = format!(" {} typ host", standalone.port());
    let answer = connect_and_echo(&client, &server, |line| line.contains(&only_standalone)).await;
    assert_eq!(
        candidates_of(&answer),
        vec![("udp".to_string(), port), ("udp".to_string(), standalone)]
    );
    assert_eq!(selected_local(&server), (IceProtocol::Udp, standalone));
    close_all(&node, &[&other, &server, &client]).await;
}
