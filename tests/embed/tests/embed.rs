//! New for musivum: `musivum_server::embed` as asaroton uses it. musivum is started in this
//! process without an HTTP listener of its own, on loopback ports 39601 to 39607 (39620 to
//! 39639 for TURN relays, which no test allocates); its routes are nested under asaroton's
//! prefix in an application of the test's own, served with `ConnectInfo<SocketAddr>`, which
//! also receives the webhooks; a client of the test's own (a WebSocket for the signalling, a
//! peer connection of musivum's for the media) joins through that prefix. The build has
//! rustls with ring and with aws-lc-rs and no default provider for the process, as
//! asaroton's build has before it installs one: whatever of musivum made a TLS configuration
//! from the default would panic.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use futures_util::{SinkExt, StreamExt};
use musivum::config::Config;
use musivum::protocol::auth::{AccessToken, VideoGrant};
use musivum::protocol::livekit::{self, signal_request, signal_response};
use musivum::protocol::signalling::from_proto_trickle;
use musivum::webrtc::peerconnection::{
    Configuration, Node, NodeConfig, PeerConnection, PeerConnectionState, SdpType,
    SessionDescription,
};
use musivum_protocol::Message as _;
use musivum_server::embed::{EmbedOptions, Embedded, twirp_code};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;

const PREFIX: &str = "/_matrix/client/unstable/io.element.msc4195/rtc/livekit/signal";
const API_KEY: &str = "key1";
const API_SECRET: &str = "a-secret-of-at-least-thirty-two-characters";
const TURN_DOMAIN: &str = "turn.test";

// The ports, on 127.0.0.1.
const RTC_UDP_PORT: u16 = 39601;
const RTC_TCP_PORT: u16 = 39602;
const TURN_UDP_PORT: u16 = 39603;
const TURN_TLS_PORT: u16 = 39604;
const PROMETHEUS_PORT: u16 = 39606;
// `port`: the HTTP listener of musivum's own, which an embedded musivum does not open
const HTTP_PORT: u16 = 39607;

const WAIT: Duration = Duration::from_secs(15);

type WebSocket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
type Hooks = mpsc::UnboundedReceiver<(String, serde_json::Value)>;

// The certificate and key of the TURN server's TLS listener, in files.
fn turn_certificate() -> (PathBuf, PathBuf) {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("embed-turn");
    std::fs::create_dir_all(&dir).unwrap();
    let cert = rcgen::generate_simple_self_signed(vec![TURN_DOMAIN.to_string()]).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert_file, cert.cert.pem()).unwrap();
    std::fs::write(&key_file, cert.signing_key.serialize_pem()).unwrap();
    (cert_file, key_file)
}

// The YAML asaroton writes for LiveKit, on the test's ports, with the webhooks going to the
// test's application and `webhook` taking what `webhook_more` adds.
fn config(app: SocketAddr, cert_file: &Path, key_file: &Path, webhook_more: &str) -> Config {
    let yaml = format!(
        "port: {HTTP_PORT}
bind_addresses: [127.0.0.1]
prometheus:
  port: {PROMETHEUS_PORT}
rtc:
  udp_port: {RTC_UDP_PORT}
  tcp_port: {RTC_TCP_PORT}
  use_external_ip: false
  node_ip: 127.0.0.1
  enable_loopback_candidate: true
  ips:
    includes: [127.0.0.1/32]
turn:
  enabled: true
  domain: {TURN_DOMAIN}
  udp_port: {TURN_UDP_PORT}
  tls_port: {TURN_TLS_PORT}
  cert_file: {}
  key_file: {}
  relay_range_start: 39620
  relay_range_end: 39639
  bind_addresses: [127.0.0.1]
keys:
  {API_KEY}: {API_SECRET}
webhook:
  api_key: {API_KEY}
  urls: [http://{app}/hook]
{webhook_more}",
        cert_file.display(),
        key_file.display(),
    );
    Config::new(&yaml, true).unwrap()
}

// asaroton's application: musivum's routes under its prefix, a fallback of its own, and the
// receiver of the webhooks, served with the clients' addresses. Stopped by the token.
fn serve(
    listener: tokio::net::TcpListener,
    embedded: &Embedded,
) -> (Hooks, CancellationToken, tokio::task::JoinHandle<()>) {
    let (hooks_tx, hooks) = mpsc::unbounded_channel();
    let app = axum::Router::new()
        .route("/hook", post(receive_hook))
        .with_state(hooks_tx)
        .nest(PREFIX, embedded.router())
        .fallback(|| async { (StatusCode::NOT_FOUND, "the application's own") });
    let stop = CancellationToken::new();
    let served = tokio::spawn({
        let stop = stop.clone();
        async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
        }
    });
    (hooks, stop, served)
}

async fn receive_hook(
    State(hooks): State<mpsc::UnboundedSender<(String, serde_json::Value)>>,
    headers: HeaderMap,
    body: String,
) -> StatusCode {
    let auth = headers
        .get("authorization")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let _ = hooks.send((auth, serde_json::from_str(&body).unwrap()));
    StatusCode::OK
}

// A GET of HTTP/1.1 on a connection of its own: the response's head (in lower case) and body.
async fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    (
        head[9..12].parse().unwrap(),
        head.to_ascii_lowercase(),
        body.to_string(),
    )
}

fn join_token(identity: &str, room: &str) -> String {
    AccessToken::new(API_KEY, API_SECRET)
        .set_identity(identity)
        .set_video_grant(VideoGrant {
            room_join: true,
            room: room.to_string(),
            // the publisher's connection is then the primary one, which the client offers
            can_subscribe: Some(false),
            can_publish: Some(true),
            can_publish_data: Some(true),
            ..Default::default()
        })
        .to_jwt()
        .unwrap()
}

// Waits for the event `name`, keeping every event seen on the way in `seen`.
async fn expect(
    events: &mut broadcast::Receiver<livekit::WebhookEvent>,
    seen: &mut Vec<String>,
    name: &str,
) -> livekit::WebhookEvent {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .unwrap_or_else(|_| panic!("no {name} within {WAIT:?}, only {seen:?}"))
            .unwrap();
        seen.push(event.event.clone());
        if event.event == name {
            return event;
        }
    }
}

// Waits until the receiver is told that no event will come, keeping those that still do.
async fn expect_closed(
    events: &mut broadcast::Receiver<livekit::WebhookEvent>,
    seen: &mut Vec<String>,
) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Err(_) => panic!("the events do not end within {WAIT:?}, {seen:?}"),
            Ok(Ok(event)) => seen.push(event.event),
            Ok(Err(broadcast::error::RecvError::Closed)) => return,
            Ok(Err(err)) => panic!("{err}"),
        }
    }
}

// Whether a TCP or a UDP socket can be bound to the port on 127.0.0.1 now (it is closed
// again at once).
fn tcp_free(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn udp_free(port: u16) -> bool {
    std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok()
}

fn assert_ports_free() {
    for (port, free) in [
        (RTC_UDP_PORT, udp_free(RTC_UDP_PORT)),
        (RTC_TCP_PORT, tcp_free(RTC_TCP_PORT)),
        (TURN_UDP_PORT, udp_free(TURN_UDP_PORT)),
        (TURN_TLS_PORT, tcp_free(TURN_TLS_PORT)),
        (PROMETHEUS_PORT, tcp_free(PROMETHEUS_PORT)),
        (HTTP_PORT, tcp_free(HTTP_PORT)),
    ] {
        assert!(free, "port {port} is still in use");
    }
}

// A participant of the test's own: the signalling over a WebSocket through the prefix.
struct Client {
    ws: WebSocket,
    join: livekit::JoinResponse,
}

impl Client {
    // `/rtc` with the join in the query, as LiveKit's clients open it: the join response.
    async fn join(app: SocketAddr, token: &str) -> Client {
        let url = format!(
            "ws://{app}{PREFIX}/rtc?access_token={token}&protocol=16&sdk=js&version=2.15.0&auto_subscribe=0"
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let res = tokio::time::timeout(WAIT, recv(&mut ws))
            .await
            .expect("no join response")
            .expect("the signalling ended before the join response");
        let Some(signal_response::Message::Join(join)) = res.message else {
            panic!("not a join response: {res:?}");
        };
        Client { ws, join }
    }

    // The URLs of TURN over TLS among the ICE servers of the join response.
    fn turns_urls(&self) -> Vec<String> {
        self.join
            .ice_servers
            .iter()
            .flat_map(|s| s.urls.iter())
            .filter(|url| url.starts_with("turns:"))
            .cloned()
            .collect()
    }

    // The publisher's peer connection, the primary one of a participant that may not
    // subscribe: offered with a data channel as LiveKit's clients offer it, answered by the
    // server, and connected to the candidates it trickles.
    async fn connect(&mut self) -> PeerConnection {
        let node = Node::new(NodeConfig::default()).unwrap();
        let pc = PeerConnection::new(&node, Configuration::default()).unwrap();
        pc.create_data_channel("_reliable", None).unwrap();
        let offer = pc.create_offer(None).unwrap();
        pc.set_local_description(offer).unwrap();
        let offer = pc.local_description().unwrap();
        send(
            &mut self.ws,
            signal_request::Message::Offer(livekit::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer.sdp,
                id: 1,
                ..Default::default()
            }),
        )
        .await;

        let deadline = Instant::now() + WAIT;
        let mut answered = false;
        let mut candidates = Vec::new();
        while pc.connection_state() != PeerConnectionState::Connected {
            assert!(
                Instant::now() < deadline,
                "the peer connection does not connect"
            );
            let Ok(res) = tokio::time::timeout(Duration::from_millis(20), recv(&mut self.ws)).await
            else {
                continue;
            };
            match res.expect("the signalling ended").message {
                Some(signal_response::Message::Answer(answer)) => {
                    pc.set_remote_description(SessionDescription {
                        r#type: SdpType::Answer,
                        sdp: answer.sdp,
                    })
                    .unwrap();
                    answered = true;
                }
                Some(signal_response::Message::Trickle(trickle)) => {
                    candidates.push(from_proto_trickle(&trickle).unwrap());
                }
                _ => {}
            }
            if answered {
                for candidate in candidates.drain(..) {
                    pc.add_ice_candidate(candidate).unwrap();
                }
            }
        }
        pc
    }
}

async fn recv(ws: &mut WebSocket) -> Option<livekit::SignalResponse> {
    loop {
        match ws.next().await? {
            Ok(WsMessage::Binary(data)) => {
                return Some(livekit::SignalResponse::decode(data.as_ref()).unwrap());
            }
            Ok(WsMessage::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

async fn send(ws: &mut WebSocket, message: signal_request::Message) {
    let req = livekit::SignalRequest {
        message: Some(message),
    };
    ws.send(WsMessage::Binary(req.encode_to_vec().into()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn musivum_runs_inside_another_server() {
    // The build is asaroton's: two providers and none for the process, so that a TLS
    // configuration made from the process's default panics.
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    assert!(std::panic::catch_unwind(rustls::ClientConfig::builder).is_err());
    let (cert_file, key_file) = turn_certificate();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = listener.local_addr().unwrap();

    // A port that is taken: no start, and nothing left open of what was opened before.
    let taken = std::net::UdpSocket::bind(("127.0.0.1", TURN_UDP_PORT)).unwrap();
    let res = Embedded::start(
        config(app, &cert_file, &key_file, ""),
        EmbedOptions::default(),
    )
    .await;
    assert!(res.is_err(), "started with the TURN port taken");
    drop(taken);
    assert_ports_free();

    // First as LiveKit tells clients of TURN over TLS, and without metrics.
    let embedded = Embedded::start(
        config(app, &cert_file, &key_file, ""),
        EmbedOptions::default(),
    )
    .await
    .unwrap();
    let mut events = embedded.events();
    let mut seen = Vec::new();
    let (mut hooks, stop, served) = serve(listener, &embedded);

    // no HTTP listener of musivum's own, and no metrics without asking
    assert!(tcp_free(HTTP_PORT), "musivum's HTTP port is bound");
    assert!(tcp_free(PROMETHEUS_PORT), "the Prometheus port is bound");
    // the media sockets and TURN are
    assert!(!udp_free(RTC_UDP_PORT) && !tcp_free(RTC_TCP_PORT));
    assert!(!udp_free(TURN_UDP_PORT) && !tcp_free(TURN_TLS_PORT));

    // the token check through the prefix: musivum sees its own path (the CORS header is
    // added for `/rtc/validate` only)
    let token = join_token("alice", "a-room");
    let (status, head, body) =
        get(app, &format!("{PREFIX}/rtc/validate?access_token={token}")).await;
    assert_eq!((200, "success"), (status, body.as_str()), "{head}");
    assert!(head.contains("access-control-allow-origin: *"), "{head}");
    assert_eq!(401, get(app, &format!("{PREFIX}/rtc/validate")).await.0);
    // the rest is the application's
    let (status, _, body) = get(app, "/elsewhere").await;
    assert_eq!((404, "the application's own"), (status, body.as_str()));

    // the room, made directly with its timeouts
    let room = embedded
        .create_room(livekit::CreateRoomRequest {
            name: "a-room".to_string(),
            empty_timeout: 10,
            departure_timeout: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        ("a-room", 10, 1),
        (
            room.name.as_str(),
            room.empty_timeout,
            room.departure_timeout
        )
    );
    let event = expect(&mut events, &mut seen, "room_started").await;
    assert_eq!("a-room", event.room.unwrap().name);
    assert!(
        event.id.starts_with("EV_") && event.created_at > 0,
        "{}",
        event.id
    );
    // and over HTTP, signed
    let (auth, hook) = tokio::time::timeout(WAIT, hooks.recv())
        .await
        .expect("no webhook")
        .unwrap();
    assert_eq!(
        ("room_started", "a-room"),
        (
            hook["event"].as_str().unwrap(),
            hook["room"]["name"].as_str().unwrap()
        )
    );
    assert!(!auth.is_empty());

    // a participant joins through the prefix; TURN over TLS is named on 443
    let mut alice = Client::join(app, &token).await;
    assert_eq!("alice", alice.join.participant.as_ref().unwrap().identity);
    assert_eq!(
        vec![format!("turns:{TURN_DOMAIN}:443?transport=tcp")],
        alice.turns_urls()
    );
    let pc = alice.connect().await;
    let event = expect(&mut events, &mut seen, "participant_joined").await;
    assert_eq!("alice", event.participant.unwrap().identity);

    // the participants, and a removal, directly
    let participants = embedded
        .list_participants(livekit::ListParticipantsRequest {
            room: "a-room".to_string(),
        })
        .await
        .unwrap()
        .participants;
    assert_eq!(
        vec!["alice"],
        participants
            .iter()
            .map(|p| p.identity.as_str())
            .collect::<Vec<_>>()
    );
    let alice_in_room = livekit::RoomParticipantIdentity {
        room: "a-room".to_string(),
        identity: "alice".to_string(),
        ..Default::default()
    };
    embedded
        .remove_participant(alice_in_room.clone())
        .await
        .unwrap();
    let event = expect(&mut events, &mut seen, "participant_left").await;
    assert_eq!("alice", event.participant.unwrap().identity);
    let err = embedded
        .remove_participant(alice_in_room)
        .await
        .unwrap_err();
    assert_eq!("not_found", twirp_code(&err), "{err}");
    // the room closes a second after the last participant left
    expect(&mut events, &mut seen, "room_finished").await;
    let _ = pc.close();
    drop(alice);

    // nobody to wait for: at once, and every port is free after
    let t = Instant::now();
    embedded.shutdown(Duration::from_secs(5)).await;
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert_ports_free();
    // the routes answer no more and hold nothing of musivum: the events end while the
    // application goes on
    let (status, _, _) = get(app, &format!("{PREFIX}/rtc/validate?access_token={token}")).await;
    assert_eq!(503, status);
    expect_closed(&mut events, &mut seen).await;
    stop.cancel();
    served.await.unwrap();

    // Then on the same ports with TURN over TLS named on its own port, a filter of the
    // events, and metrics; shut down with a participant still there.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = listener.local_addr().unwrap();
    let embedded = Embedded::start(
        config(
            app,
            &cert_file,
            &key_file,
            "  filter_params:\n    exclude_events: [room_started]\n",
        ),
        EmbedOptions {
            advertised_turn_tls_port: Some(TURN_TLS_PORT),
            metrics: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut events = embedded.events();
    let mut seen = Vec::new();
    let (_hooks, stop, served) = serve(listener, &embedded);
    assert!(
        !tcp_free(PROMETHEUS_PORT),
        "no Prometheus listener with metrics"
    );
    assert!(tcp_free(HTTP_PORT), "musivum's HTTP port is bound");

    let bob = Client::join(app, &join_token("bob", "b-room")).await;
    assert_eq!(
        vec![format!("turns:{TURN_DOMAIN}:{TURN_TLS_PORT}?transport=tcp")],
        bob.turns_urls()
    );

    let timeout = Duration::from_millis(1500);
    let t = Instant::now();
    embedded.shutdown(timeout).await;
    let elapsed = t.elapsed();
    assert!(
        elapsed >= timeout && elapsed < timeout + Duration::from_secs(3),
        "{elapsed:?}"
    );
    assert_ports_free();
    // bob was closed, and his room with him
    expect(&mut events, &mut seen, "room_finished").await;
    stop.cancel();
    drop(bob);
    served.await.unwrap();
    expect_closed(&mut events, &mut seen).await;
    // the filter of the webhooks holds for the events in process
    assert!(!seen.iter().any(|e| e == "room_started"), "{seen:?}");
}
