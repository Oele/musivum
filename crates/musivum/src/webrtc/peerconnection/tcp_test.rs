//! New for musivum: ICE over TCP between two connections of the layer. The server's node
//! listens on a TCP port of the system's choosing on the loopback interface, and both
//! connections take TCP candidates only, so whatever passes between them can have gone over
//! the one stream the client dials.

use std::io::{Read, Write};
use std::net::TcpStream;

use super::*;

fn tcp_only() -> Vec<NetworkType> {
    vec![NetworkType::Tcp4]
}

/// A node with a TCP listener, as a server is run with `rtc.tcp_port`.
fn tcp_node() -> Node {
    Node::new_with_tcp(NodeConfig::default(), 0).unwrap()
}

/// A server's connection (ICE lite, the DTLS server) that takes TCP candidates only.
fn tcp_server_on(node: &Node) -> Peer {
    Peer::new(
        node,
        SettingEngine {
            lite: true,
            answering_dtls_role: DtlsRole::Server,
            network_types: tcp_only(),
            ..Default::default()
        },
    )
}

/// A client's connection that takes TCP candidates only and dials, on a node of its own.
fn tcp_client() -> Peer {
    Peer::new(
        &Node::new(NodeConfig::default()).unwrap(),
        SettingEngine {
            network_types: tcp_only(),
            active_tcp: true,
            ..Default::default()
        },
    )
}

/// The candidate lines of a description.
fn candidates(sdp: &str) -> Vec<&str> {
    sdp.lines()
        .filter(|l| l.starts_with("a=candidate:"))
        .map(str::trim_end)
        .collect()
}

/// A channel of this side's, and what it tells.
fn open_channel(peer: &Peer, label: &str) -> (Arc<DataChannel>, Arc<ChannelLog>) {
    let channel = peer.pc.create_data_channel(label, None).unwrap();
    let log = ChannelLog::watch(&channel);
    (channel, log)
}

/// Sends a text on the channel and waits for its echo.
async fn echoed(channel: &DataChannel, log: &ChannelLog, text: &str) {
    channel.send_text(text).unwrap();
    eventually_async(
        || {
            log.messages
                .lock()
                .iter()
                .any(|m| m.is_string && &m.data[..] == text.as_bytes())
        },
        WAIT,
        TICK,
    )
    .await;
}

/// The two connect over TCP, media goes both ways, a data channel echoes, and when the client
/// closes, the stream is closed at both ends while the server's connection is still open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ice_over_tcp() {
    let node = tcp_node();
    let tcp_addr = node.tcp_local_addr().unwrap();
    let server = tcp_server_on(&node);
    let client = tcp_client();
    let _ = echo_incoming(&server);

    // Video both ways on one transceiver, and a data channel, in the first offer.
    let from_client = TestTrack::new("from-client", RtpCodecType::Video);
    let from_server = TestTrack::new("from-server", RtpCodecType::Video);
    client.pc.add_track(from_client.clone()).unwrap();
    let (channel, log) = open_channel(&client, "chat");
    let offer = client.pc.create_offer(None).unwrap();
    client.pc.set_local_description(offer).unwrap();
    server
        .pc
        .set_remote_description(client.pc.local_description().unwrap())
        .unwrap();
    server.pc.add_track(from_server.clone()).unwrap();
    let answer = server.pc.create_answer().unwrap();
    server.pc.set_local_description(answer).unwrap();
    client
        .pc
        .set_remote_description(server.pc.local_description().unwrap())
        .unwrap();

    // The server is told to be dialled at its TCP port, the client says it dials; neither
    // has a UDP candidate.
    let answer = server.pc.local_description().unwrap().sdp;
    let passive = format!(
        " {} {} typ host tcptype passive",
        tcp_addr.ip(),
        tcp_addr.port()
    );
    let server_candidates = candidates(&answer);
    assert!(!server_candidates.is_empty(), "{answer}");
    for c in &server_candidates {
        assert!(c.contains(" tcp ") && c.ends_with(&passive), "{c}");
    }
    let offer = client.pc.local_description().unwrap().sdp;
    let client_candidates = candidates(&offer);
    assert!(!client_candidates.is_empty(), "{offer}");
    for c in &client_candidates {
        assert!(
            c.contains(" tcp ") && c.ends_with(" 9 typ host tcptype active"),
            "{c}"
        );
    }

    connected(&client, &server).await;

    // Over the stream: the selected pairs are the passive candidate and the client's, and
    // each node has the one stream.
    let pair = server.pc.get_selected_candidate_pair().unwrap();
    assert_eq!(pair.local().protocol, IceProtocol::Tcp);
    assert_eq!(pair.local().tcp_type, IceTcpCandidateType::Passive);
    assert_eq!(pair.local().port, tcp_addr.port());
    let pair = client.pc.get_selected_candidate_pair().unwrap();
    assert_eq!(pair.local().protocol, IceProtocol::Tcp);
    assert_eq!(pair.local().tcp_type, IceTcpCandidateType::Active);
    assert_eq!(pair.remote().tcp_type, IceTcpCandidateType::Passive);
    assert_eq!(pair.remote().port, tcp_addr.port());
    assert_eq!(node.stats().tcp_streams_opened, 1);
    assert_eq!(client.node.stats().tcp_streams_opened, 1);

    // Media both ways, as written.
    eventually_async(
        || from_client.is_bound() && from_server.is_bound(),
        WAIT,
        TICK,
    )
    .await;
    let client_ssrc = from_client.context().ssrc();
    let server_ssrc = from_server.context().ssrc();
    let mut to_server = Vec::new();
    let mut to_client = Vec::new();
    for seq in 0..20u16 {
        let h = header(client_ssrc, VP8, seq);
        from_client.writer().write_rtp(&h, &[1; 1000]).unwrap();
        to_server.push(marshalled(&h, &[1; 1000]));
        let h = header(server_ssrc, VP8, 100 + seq);
        from_server.writer().write_rtp(&h, &[2; 1000]).unwrap();
        to_client.push(marshalled(&h, &[2; 1000]));
    }
    eventually_async(
        || {
            server.sink.rtp_of(client_ssrc).len() == 20
                && client.sink.rtp_of(server_ssrc).len() == 20
        },
        WAIT,
        TICK,
    )
    .await;
    assert_eq!(server.sink.rtp_of(client_ssrc), to_server);
    assert_eq!(client.sink.rtp_of(server_ssrc), to_client);

    // And RTCP, and the data channel.
    server.pc.write_rtcp(&pli(client_ssrc)).unwrap();
    eventually_async(
        || !client.sink.rtcp_of::<PictureLossIndication>().is_empty(),
        WAIT,
        TICK,
    )
    .await;
    eventually_async(|| log.is_open(), WAIT, TICK).await;
    echoed(&channel, &log, "over a stream").await;

    // Everything went over the stream: nothing was sent to nowhere, nothing was refused.
    for stats in [node.stats(), client.node.stats()] {
        assert_eq!(stats.datagrams_not_sent, 0, "{stats:?}");
        assert_eq!(stats.datagrams_without_route, 0, "{stats:?}");
        assert_eq!(stats.tcp_streams_refused, 0, "{stats:?}");
    }

    // The client closes: its stream ends, and the server reads the end of it, though its
    // own connection is still open.
    client.pc.close().unwrap();
    eventually_async(
        || client.node.stats().tcp_streams_closed == 1 && node.stats().tcp_streams_closed == 1,
        WAIT,
        TICK,
    )
    .await;
    assert_ne!(server.pc.connection_state(), PeerConnectionState::Closed);
    server.pc.close().unwrap();
    eventually_async(|| node.connection_count() == 0, WAIT, TICK).await;
}

/// A binding request with this user name for the connection, without integrity: enough for
/// the node, which reads the name only.
fn binding_request(ufrag: &str) -> Vec<u8> {
    let username = format!("{ufrag}:them");
    let padded = (username.len() + 3) & !3;
    let mut msg = vec![0x00, 0x01, 0x00, (4 + padded) as u8, 0x21, 0x12, 0xA4, 0x42];
    msg.extend_from_slice(&[7u8; 12]);
    msg.extend_from_slice(&[0x00, 0x06, 0x00, username.len() as u8]);
    msg.extend_from_slice(username.as_bytes());
    msg.resize(20 + 4 + padded, 0);
    msg
}

fn framed(packet: &[u8]) -> Vec<u8> {
    let mut out = (packet.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(packet);
    out
}

/// Whether the node closes this stream after what was written: the end of the stream or a
/// reset, before a timeout.
fn closed_by_node(stream: &mut TcpStream) -> bool {
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let mut buf = [0u8; 64];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return true,
            Ok(_) => continue,
            Err(err) => {
                return err.kind() == std::io::ErrorKind::ConnectionReset
                    || err.kind() == std::io::ErrorKind::ConnectionAborted;
            }
        }
    }
}

/// Streams that do not keep to the rules are closed: one whose first frame is not a binding
/// request, one whose first frame is too long, one that names no connection, and one that
/// names a connection and then sends a frame over the limit. The connection that is up over
/// TCP on the same listener (the server offered it this time) goes on as before; and when
/// the server closes it, the client reads the end of its stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_streams_that_break_the_rules_are_closed() {
    let node = tcp_node();
    let tcp_addr = node.tcp_local_addr().unwrap();
    let server = tcp_server_on(&node);
    let client = tcp_client();
    let _ = echo_incoming(&client);
    let (channel, log) = open_channel(&server, "chat");
    negotiate(&server.pc, &client.pc);
    connected(&client, &server).await;
    eventually_async(|| log.is_open(), WAIT, TICK).await;
    echoed(&channel, &log, "before").await;
    let ufrag = ice_ufrag(&server.pc.local_description().unwrap().sdp)
        .unwrap()
        .to_string();

    let rule_breakers: Vec<(&str, Vec<u8>)> = vec![
        ("not a binding request", framed(&[0x80; 40])),
        ("a first frame over 512 octets", framed(&[0; 600])),
        ("a user name nobody has", framed(&binding_request("nobody"))),
    ];
    for (i, (what, bytes)) in rule_breakers.iter().enumerate() {
        let mut stream = TcpStream::connect(tcp_addr).unwrap();
        stream.write_all(bytes).unwrap();
        assert!(closed_by_node(&mut stream), "{what}");
        assert_eq!(node.stats().tcp_streams_refused, i as u64 + 1, "{what}");
    }

    // The server's user name gets the stream to the connection (whose agent throws the
    // request away: it is not signed); a frame over 8192 octets ends it.
    let mut stream = TcpStream::connect(tcp_addr).unwrap();
    stream.write_all(&framed(&binding_request(&ufrag))).unwrap();
    eventually_async(|| node.stats().tcp_streams_opened == 2, WAIT, TICK).await;
    stream.write_all(&[0x20, 0x01]).unwrap();
    stream.write_all(&[0; 64]).unwrap();
    assert!(closed_by_node(&mut stream), "a frame over the limit");
    eventually_async(|| node.stats().tcp_streams_closed == 1, WAIT, TICK).await;

    // Nothing of that touched the connection or its stream.
    echoed(&channel, &log, "after").await;
    assert!(server.is_connected() && client.is_connected());
    let stats = node.stats();
    assert_eq!(stats.tcp_streams_opened, 2, "{stats:?}");
    assert_eq!(stats.tcp_streams_closed, 1, "{stats:?}");
    assert_eq!(stats.tcp_streams_refused, 3, "{stats:?}");
    assert_eq!(client.node.stats().tcp_streams_closed, 0);

    // The server closes: the client reads the end of its stream.
    server.pc.close().unwrap();
    eventually_async(
        || client.node.stats().tcp_streams_closed == 1 && node.stats().tcp_streams_closed == 2,
        WAIT,
        TICK,
    )
    .await;
    client.pc.close().unwrap();
    eventually_async(|| node.connection_count() == 0, WAIT, TICK).await;
}
