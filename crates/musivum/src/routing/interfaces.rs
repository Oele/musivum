// Ported from LiveKit, pkg/routing/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Not here: `Router`, `MessageRouter` and
// `CreateRouter`, which are for the nodes of a cluster. A message is a `Message` and
// `ReadChan()` gives a `ReadChan` (`message.rs`). The null sink and source are made as
// `Arc`s; the channel of the null source, which is only ever closed, is its writing end,
// dropped to close it. `MessageSink` has `Any` as a supertrait, so that a test gets its fake
// back from an `Arc<dyn MessageSink>`. In `ParticipantInit` a pointer that may be nil is an
// `Option`. `MarshalLogObject` is `Debug`, with the keys of the original; the messages of the
// protocol in it are written by their own `Debug`, where the original writes them through
// `logger.Proto`, which leaves out what the protocol marks as sensitive. The conversions
// return serde_json's errors where the original returns those of `encoding/json`. The grants
// are written by serde_json, which gives the same JSON as Go but for the `<`, `>`, `&` and
// the two line separators that Go escapes in strings, and read back by it and not by Go's
// rules (`auth/json.rs`, which differ only for JSON that `to_start_session` does not write).
// The tests at the end are new: the original has none.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::errors::Error;
use super::message::{Message, ReadChan, make_chan};
use crate::go::atomic;
use crate::go::time::Time;
use crate::protocol::auth::ClaimGrants;
use crate::protocol::livekit;
use crate::utils::client_info_without_address;

// MessageSink is an abstraction for writing protobuf messages and having them read by a MessageSource,
// potentially on a different node via a transport
#[cfg_attr(test, musivum_fakes::fake)]
pub trait MessageSink: Any + Send + Sync {
    fn write_message(&self, msg: Message) -> Result<(), Error>;
    fn is_closed(&self) -> bool;
    fn close(&self);
    fn connection_id(&self) -> livekit::ConnectionId;
}

// ----------

pub struct NullMessageSink {
    conn_id: livekit::ConnectionId,
    is_closed: atomic::Bool,
}

impl NullMessageSink {
    pub fn new(conn_id: livekit::ConnectionId) -> Arc<Self> {
        Arc::new(Self {
            conn_id,
            is_closed: atomic::Bool::new(false),
        })
    }
}

impl MessageSink for NullMessageSink {
    fn write_message(&self, _msg: Message) -> Result<(), Error> {
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load()
    }

    fn close(&self) {
        self.is_closed.store(true);
    }

    fn connection_id(&self) -> livekit::ConnectionId {
        self.conn_id.clone()
    }
}

// ------------------------------------------------

#[cfg_attr(test, musivum_fakes::fake)]
pub trait MessageSource: Send + Sync {
    // ReadChan exposes a one way channel to make it easier to use with select
    fn read_chan(&self) -> ReadChan;
    fn is_closed(&self) -> bool;
    fn close(&self);
    fn connection_id(&self) -> livekit::ConnectionId;
}

// ----------

pub struct NullMessageSource {
    conn_id: livekit::ConnectionId,
    // The writing end of `msgChan`, which nothing is written to; `None` once it is closed.
    msg_chan: Mutex<Option<mpsc::Sender<Message>>>,
    read_chan: ReadChan,
    is_closed: atomic::Bool,
}

impl NullMessageSource {
    pub fn new(conn_id: livekit::ConnectionId) -> Arc<Self> {
        let (msg_chan, read_chan) = make_chan(0);
        Arc::new(Self {
            conn_id,
            msg_chan: Mutex::new(Some(msg_chan)),
            read_chan,
            is_closed: atomic::Bool::new(false),
        })
    }
}

impl MessageSource for NullMessageSource {
    fn read_chan(&self) -> ReadChan {
        self.read_chan.clone()
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load()
    }

    fn close(&self) {
        if !self.is_closed.swap(true) {
            *self.msg_chan.lock() = None;
        }
    }

    fn connection_id(&self) -> livekit::ConnectionId {
        self.conn_id.clone()
    }
}

// ------------------------------------------------

#[derive(Clone, Default)]
pub struct StartParticipantSignalResults {
    pub connection_id: livekit::ConnectionId,
    pub request_sink: Option<Arc<dyn MessageSink>>,
    pub response_source: Option<Arc<dyn MessageSource>>,
    pub node_id: livekit::NodeId,
    pub node_selection_reason: String,
}

// ------------------------------------------------

#[derive(Clone, Default, PartialEq)]
pub struct ParticipantInit {
    pub identity: livekit::ParticipantIdentity,
    pub name: livekit::ParticipantName,
    pub reconnect: bool,
    pub reconnect_reason: livekit::ReconnectReason,
    pub auto_subscribe: bool,
    pub auto_subscribe_data_track: Option<bool>,
    pub client: Option<livekit::ClientInfo>,
    pub grants: Option<ClaimGrants>,
    pub api_key: String,
    pub token_expires_at: Time,
    pub region: String,
    pub adaptive_stream: bool,
    pub id: livekit::ParticipantId,
    pub subscriber_allow_pause: Option<bool>,
    pub disable_ice_lite: bool,
    pub create_room: Option<livekit::CreateRoomRequest>,
    pub add_track_requests: Vec<livekit::AddTrackRequest>,
    pub publisher_offer: Option<livekit::SessionDescription>,
    pub sync_state: Option<livekit::SyncState>,
    pub use_single_peer_connection: bool,
}

// MarshalLogObject of the original.
impl fmt::Debug for ParticipantInit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct BoolPtr(Option<bool>);

        impl fmt::Debug for BoolPtr {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    None => f.write_str("not-set"),
                    Some(val) => write!(f, "{val}"),
                }
            }
        }

        let mut e = f.debug_struct("ParticipantInit");
        e.field("Identity", &self.identity);
        e.field("Reconnect", &self.reconnect);
        e.field(
            "ReconnectReason",
            &format_args!("{}", self.reconnect_reason.as_str_name()),
        );
        e.field("AutoSubscribe", &self.auto_subscribe);
        e.field(
            "AutoSubscribeDataTrack",
            &BoolPtr(self.auto_subscribe_data_track),
        );
        e.field("Client", &client_info_without_address(self.client.as_ref()));
        e.field("Grants", &self.grants);
        if !self.token_expires_at.is_zero() {
            e.field("TokenExpiresAt", &self.token_expires_at);
        }
        e.field("Region", &self.region);
        e.field("AdaptiveStream", &self.adaptive_stream);
        e.field("ID", &self.id);
        e.field(
            "SubscriberAllowPause",
            &BoolPtr(self.subscriber_allow_pause),
        );
        e.field("DisableICELite", &self.disable_ice_lite);
        e.field("CreateRoom", &self.create_room);
        e.field("AddTrackRequests", &self.add_track_requests);
        e.field("PublisherOffer", &self.publisher_offer);
        e.field("SyncState", &self.sync_state);
        e.field("UseSinglePeerConnection", &self.use_single_peer_connection);
        e.finish()
    }
}

impl ParticipantInit {
    pub fn to_start_session(
        &self,
        room_name: &livekit::RoomName,
        connection_id: &livekit::ConnectionId,
    ) -> Result<livekit::StartSession, serde_json::Error> {
        let claims = serde_json::to_string(&self.grants)?;

        let mut token_expires_at = 0i64;
        if !self.token_expires_at.is_zero() {
            token_expires_at = self.token_expires_at.unix_secs();
        }

        let mut ss = livekit::StartSession {
            room_name: room_name.to_string(),
            identity: self.identity.to_string(),
            name: self.name.to_string(),
            connection_id: connection_id.to_string(),
            reconnect: self.reconnect,
            reconnect_reason: self.reconnect_reason as i32,
            auto_subscribe: self.auto_subscribe,
            client: self.client.clone(),
            grants_json: claims,
            api_key: self.api_key.clone(),
            token_expires_at,
            adaptive_stream: self.adaptive_stream,
            participant_id: self.id.to_string(),
            disable_ice_lite: self.disable_ice_lite,
            create_room: self.create_room.clone(),
            add_track_requests: self.add_track_requests.clone(),
            publisher_offer: self.publisher_offer.clone(),
            sync_state: self.sync_state.clone(),
            use_single_peer_connection: self.use_single_peer_connection,
            ..Default::default()
        };
        if let Some(auto_subscribe_data_track) = self.auto_subscribe_data_track {
            ss.auto_subscribe_data_track = Some(auto_subscribe_data_track);
        }
        if let Some(subscriber_allow_pause) = self.subscriber_allow_pause {
            ss.subscriber_allow_pause = Some(subscriber_allow_pause);
        }

        Ok(ss)
    }
}

pub fn participant_init_from_start_session(
    ss: &livekit::StartSession,
    region: &str,
) -> Result<ParticipantInit, serde_json::Error> {
    // `null`, which is what grants that are nil are written as, leaves the claims as they
    // are in Go: empty.
    let claims: Option<ClaimGrants> = serde_json::from_str(&ss.grants_json)?;
    let claims = claims.unwrap_or_default();
    let mut token_expires_at = Time::ZERO;
    if ss.token_expires_at > 0 {
        token_expires_at = Time::unix(ss.token_expires_at, 0);
    }

    let mut pi = ParticipantInit {
        identity: livekit::ParticipantIdentity::from(&ss.identity),
        name: livekit::ParticipantName::from(&ss.name),
        reconnect: ss.reconnect,
        reconnect_reason: ss.reconnect_reason(),
        client: ss.client.clone(),
        auto_subscribe: ss.auto_subscribe,
        grants: Some(claims),
        api_key: ss.api_key.clone(),
        token_expires_at,
        region: region.to_string(),
        adaptive_stream: ss.adaptive_stream,
        id: livekit::ParticipantId::from(&ss.participant_id),
        disable_ice_lite: ss.disable_ice_lite,
        create_room: ss.create_room.clone(),
        add_track_requests: ss.add_track_requests.clone(),
        publisher_offer: ss.publisher_offer.clone(),
        sync_state: ss.sync_state.clone(),
        use_single_peer_connection: ss.use_single_peer_connection,
        ..Default::default()
    };
    if let Some(auto_subscribe_data_track) = ss.auto_subscribe_data_track {
        pi.auto_subscribe_data_track = Some(auto_subscribe_data_track);
    }
    if let Some(subscriber_allow_pause) = ss.subscriber_allow_pause {
        pi.subscriber_allow_pause = Some(subscriber_allow_pause);
    }

    // TODO: clean up after 1.7 eol
    if pi.create_room.is_none() {
        pi.create_room = Some(livekit::CreateRoomRequest {
            name: ss.room_name.clone(),
            ..Default::default()
        });
    }

    Ok(pi)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::auth::VideoGrant;

    // New for musivum: the original has no test of the conversions. What a session is started
    // with comes back from the message it was written to, with the region of who reads it.
    #[test]
    fn a_participant_init_comes_back_from_its_start_session() {
        let pi = ParticipantInit {
            identity: "alice".into(),
            name: "Alice".into(),
            reconnect: true,
            reconnect_reason: livekit::ReconnectReason::RrPublisherFailed,
            auto_subscribe: true,
            auto_subscribe_data_track: Some(false),
            client: Some(livekit::ClientInfo {
                sdk: livekit::client_info::Sdk::Js as i32,
                version: "2.1.0".to_string(),
                protocol: 16,
                ..Default::default()
            }),
            grants: Some(ClaimGrants {
                identity: "alice".to_string(),
                name: "Alice".to_string(),
                kind_details: vec!["forwarded".to_string()],
                video: Some(VideoGrant {
                    room_join: true,
                    room: "a-room".to_string(),
                    can_publish: Some(false),
                    can_publish_sources: vec!["camera".to_string()],
                    ..Default::default()
                }),
                metadata: "<b>\"&\u{2028}".to_string(),
                attributes: HashMap::from([
                    ("b".to_string(), "2".to_string()),
                    ("a".to_string(), "1".to_string()),
                ]),
                ..Default::default()
            }),
            api_key: "the-api-key".to_string(),
            token_expires_at: Time::unix(1_700_000_000, 0),
            region: "there".to_string(),
            adaptive_stream: true,
            id: "PA_1".into(),
            subscriber_allow_pause: Some(true),
            disable_ice_lite: true,
            create_room: Some(livekit::CreateRoomRequest {
                name: "a-room".to_string(),
                empty_timeout: 30,
                ..Default::default()
            }),
            add_track_requests: vec![livekit::AddTrackRequest {
                cid: "c1".to_string(),
                name: "camera".to_string(),
                ..Default::default()
            }],
            publisher_offer: Some(livekit::SessionDescription {
                r#type: "offer".to_string(),
                sdp: "v=0".to_string(),
                id: 1,
                ..Default::default()
            }),
            sync_state: Some(livekit::SyncState::default()),
            use_single_peer_connection: true,
        };

        let ss = pi
            .to_start_session(&"a-room".into(), &"CO_1".into())
            .unwrap();
        assert_eq!("a-room", ss.room_name);
        assert_eq!("alice", ss.identity);
        assert_eq!("Alice", ss.name);
        assert_eq!("CO_1", ss.connection_id);
        assert_eq!("PA_1", ss.participant_id);
        assert_eq!("the-api-key", ss.api_key);
        assert_eq!(1_700_000_000, ss.token_expires_at);
        assert_eq!(
            livekit::ReconnectReason::RrPublisherFailed,
            ss.reconnect_reason()
        );
        assert_eq!(Some(false), ss.auto_subscribe_data_track);
        assert_eq!(Some(true), ss.subscriber_allow_pause);
        assert!(
            ss.grants_json
                .starts_with(r#"{"identity":"alice","name":"Alice","#),
            "{}",
            ss.grants_json
        );

        let back = participant_init_from_start_session(&ss, "here").unwrap();
        assert_eq!(
            ParticipantInit {
                region: "here".to_string(),
                ..pi.clone()
            },
            back
        );

        // Nothing set: no grants are written as `null` and read as empty grants, no time is
        // 0, and the room to create is the room of the session.
        let ss = ParticipantInit::default()
            .to_start_session(&"a-room".into(), &"CO_2".into())
            .unwrap();
        assert_eq!("null", ss.grants_json);
        assert_eq!(0, ss.token_expires_at);
        assert_eq!(None, ss.auto_subscribe_data_track);
        assert_eq!(None, ss.subscriber_allow_pause);
        let back = participant_init_from_start_session(&ss, "").unwrap();
        assert_eq!(
            ParticipantInit {
                grants: Some(ClaimGrants::default()),
                create_room: Some(livekit::CreateRoomRequest {
                    name: "a-room".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            back
        );

        // what is not JSON is an error, as in the original
        let ss = livekit::StartSession::default();
        assert!(participant_init_from_start_session(&ss, "").is_err());
    }

    // New for musivum: the keys of the original's `MarshalLogObject`; the name, the API key
    // and the address of the client are not logged.
    #[test]
    fn a_participant_init_is_logged_with_the_keys_of_the_original() {
        let pi = ParticipantInit {
            identity: "alice".into(),
            name: "Alice".into(),
            client: Some(livekit::ClientInfo {
                address: "192.0.2.7".to_string(),
                ..Default::default()
            }),
            api_key: "the-api-key".to_string(),
            subscriber_allow_pause: Some(true),
            ..Default::default()
        };
        let text = format!("{pi:?}");
        assert!(
            text.starts_with(
                "ParticipantInit { Identity: \"alice\", Reconnect: false, \
                 ReconnectReason: RR_UNKNOWN, AutoSubscribe: false, \
                 AutoSubscribeDataTrack: not-set, Client: Some("
            ),
            "{text}"
        );
        assert!(
            text.ends_with(
                "Grants: None, Region: \"\", AdaptiveStream: false, ID: \"\", \
                 SubscriberAllowPause: true, DisableICELite: false, CreateRoom: None, \
                 AddTrackRequests: [], PublisherOffer: None, SyncState: None, \
                 UseSinglePeerConnection: false }"
            ),
            "{text}"
        );
        for absent in ["Alice", "the-api-key", "192.0.2.7", "TokenExpiresAt"] {
            assert!(!text.contains(absent), "{absent} in {text}");
        }
    }
}
