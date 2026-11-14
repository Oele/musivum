// Ported from LiveKit's protocol, auth/grants.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A grant that the original holds by a pointer is an
// `Option`, and so is a `*bool`. `Clone()` is the derived `Clone` (a clone of nothing is the
// caller's `None`), and so the methods that the original also allows on a nil grant
// (`Allows`, `CanRead` and the like) are methods of a grant that is there. The JSON form is
// written with serde to the names and the `omitempty` of the original's tags, and read by
// Go's rules (`json.rs`). `MarshalLogObject` is `Debug`, with the keys of the original; the
// room configuration is logged without what it could give away, see there. A value of an
// enumeration of the protocol is taken as the enumeration or as the number a message holds.

use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::interfaces::Error;
use super::json::{self, is_false};
use crate::protocol::livekit::{self, TrackSource, participant_info};

#[derive(Clone, Default, PartialEq)]
pub struct RoomConfiguration(pub livekit::RoomConfiguration);

// ErrSensitiveCredentials is Error::SensitiveCredentials, in interfaces.rs.

impl Deref for RoomConfiguration {
    type Target = livekit::RoomConfiguration;

    fn deref(&self) -> &livekit::RoomConfiguration {
        &self.0
    }
}

impl DerefMut for RoomConfiguration {
    fn deref_mut(&mut self) -> &mut livekit::RoomConfiguration {
        &mut self.0
    }
}

impl From<livekit::RoomConfiguration> for RoomConfiguration {
    fn from(c: livekit::RoomConfiguration) -> RoomConfiguration {
        RoomConfiguration(c)
    }
}

impl From<RoomConfiguration> for livekit::RoomConfiguration {
    fn from(c: RoomConfiguration) -> livekit::RoomConfiguration {
        c.0
    }
}

// MarshalJSON of the original: the JSON form Protocol Buffers defines.
impl Serialize for RoomConfiguration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

// UnmarshalJSON of the original: protojson, which leaves out what it does not know.
impl<'de> Deserialize<'de> for RoomConfiguration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<RoomConfiguration, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        json::from_value(value)
            .map(RoomConfiguration)
            .map_err(D::Error::custom)
    }
}

// The original logs the room configuration through `logger.Proto`, which leaves out, field
// by field, what the protocol marks as sensitive (the secrets of an egress, metadata).
// musivum has no such logger. So that a log line cannot give those away, this writes the
// fields of the message itself that are not marked, the length of its metadata as the
// protocol's format for it says, and of what lies deeper only that it is there: whether
// there is an egress, and the names of the agents.
impl fmt::Debug for RoomConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let agents: Vec<&str> = self.agents.iter().map(|a| a.agent_name.as_str()).collect();
        f.debug_struct("RoomConfiguration")
            .field("name", &self.name)
            .field("emptyTimeout", &self.empty_timeout)
            .field("departureTimeout", &self.departure_timeout)
            .field("maxParticipants", &self.max_participants)
            .field(
                "metadata",
                &format_args!("<redacted ({} bytes)>", self.metadata.len()),
            )
            .field("egress", &self.egress.is_some())
            .field("minPlayoutDelay", &self.min_playout_delay)
            .field("maxPlayoutDelay", &self.max_playout_delay)
            .field("syncStreams", &self.sync_streams)
            .field("agents", &agents)
            .field("tags", &self.tags)
            .finish()
    }
}

// What the original hands to checkOutputForCredentials as `any`.
enum Output<'a> {
    EncodedFile(&'a livekit::encoded_file_output::Output),
    SegmentedFile(&'a livekit::segmented_file_output::Output),
    // The output of an image, which checkOutputForCredentials does not look into.
    Image,
    AutoTrack(&'a livekit::auto_track_egress::Output),
}

impl RoomConfiguration {
    // CheckCredentials checks if the room configuration contains sensitive credentials
    // and returns an error if it does.
    //
    // This is used to prevent sensitive credentials from being leaked to the client.
    // It is not used to validate the credentials themselves, as that is done by the
    // egress service.
    pub fn check_credentials(&self) -> Result<(), Error> {
        let Some(egress) = &self.egress else {
            return Ok(());
        };

        if let Some(participant) = &egress.participant {
            for output in &participant.file_outputs {
                check_output_for_credentials(output.output.as_ref().map(Output::EncodedFile))?;
            }
            for output in &participant.segment_outputs {
                check_output_for_credentials(output.output.as_ref().map(Output::SegmentedFile))?;
            }
        }
        if let Some(room) = &egress.room {
            for output in &room.file_outputs {
                check_output_for_credentials(output.output.as_ref().map(Output::EncodedFile))?;
            }
            for output in &room.segment_outputs {
                check_output_for_credentials(output.output.as_ref().map(Output::SegmentedFile))?;
            }
            for output in &room.image_outputs {
                check_output_for_credentials(output.output.as_ref().map(|_| Output::Image))?;
            }
            if !room.stream_outputs.is_empty() {
                // do not leak stream key
                return Err(Error::SensitiveCredentials);
            }
        }
        if let Some(tracks) = &egress.tracks {
            check_output_for_credentials(tracks.output.as_ref().map(Output::AutoTrack))?;
        }
        Ok(())
    }
}

fn check_output_for_credentials(output: Option<Output<'_>>) -> Result<(), Error> {
    use livekit::auto_track_egress::Output as AutoTrackEgress;
    use livekit::encoded_file_output::Output as EncodedFileOutput;
    use livekit::segmented_file_output::Output as SegmentedFileOutput;

    let Some(output) = output else {
        return Ok(());
    };

    match output {
        Output::EncodedFile(EncodedFileOutput::S3(s3)) => {
            if !s3.secret.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::SegmentedFile(SegmentedFileOutput::S3(s3)) => {
            if !s3.secret.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::AutoTrack(AutoTrackEgress::S3(s3)) => {
            if !s3.secret.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::EncodedFile(EncodedFileOutput::Gcp(gcp)) => {
            if !gcp.credentials.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::SegmentedFile(SegmentedFileOutput::Gcp(gcp)) => {
            if !gcp.credentials.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::AutoTrack(AutoTrackEgress::Gcp(gcp)) => {
            if !gcp.credentials.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::EncodedFile(EncodedFileOutput::Azure(azure)) => {
            if !azure.account_key.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::SegmentedFile(SegmentedFileOutput::Azure(azure)) => {
            if !azure.account_key.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::AutoTrack(AutoTrackEgress::Azure(azure)) => {
            if !azure.account_key.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::EncodedFile(EncodedFileOutput::AliOss(ali_oss)) => {
            if !ali_oss.secret.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::SegmentedFile(SegmentedFileOutput::AliOss(ali_oss)) => {
            if !ali_oss.secret.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        Output::AutoTrack(AutoTrackEgress::AliOss(ali_oss)) => {
            if !ali_oss.secret.is_empty() {
                return Err(Error::SensitiveCredentials);
            }
        }
        // The original has no case for the outputs of an image (`ImageOutput_S3` and the
        // others): credentials in them are not found. Ported as it is.
        Output::Image => {}
    }
    Ok(())
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClaimGrants {
    #[serde(rename = "identity", skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(rename = "name", skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "kind", skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(
        rename = "kindDetails",
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "json::string_list"
    )]
    pub kind_details: Vec<String>,
    #[serde(rename = "video", skip_serializing_if = "Option::is_none")]
    pub video: Option<VideoGrant>,
    #[serde(rename = "sip", skip_serializing_if = "Option::is_none")]
    pub sip: Option<SipGrant>,
    #[serde(rename = "agent", skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentGrant>,
    #[serde(rename = "inference", skip_serializing_if = "Option::is_none")]
    pub inference: Option<InferenceGrant>,
    #[serde(rename = "observability", skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityGrant>,
    #[serde(rename = "agentEndpoint", skip_serializing_if = "Option::is_none")]
    pub agent_endpoint: Option<AgentEndpointGrant>,
    #[serde(rename = "agentThread", skip_serializing_if = "Option::is_none")]
    pub agent_thread: Option<AgentThreadGrant>,
    // Room configuration to use if this participant initiates the room
    #[serde(rename = "roomConfig", skip_serializing_if = "Option::is_none")]
    pub room_config: Option<RoomConfiguration>,
    // Cloud-only, config preset to use
    // when both room and roomPreset are set, parameters in room overrides the preset
    #[serde(rename = "roomPreset", skip_serializing_if = "String::is_empty")]
    pub room_preset: String,
    // for verifying integrity of the message body
    #[serde(rename = "sha256", skip_serializing_if = "String::is_empty")]
    pub sha256: String,
    #[serde(rename = "metadata", skip_serializing_if = "String::is_empty")]
    pub metadata: String,
    // Key/value attributes to attach to the participant
    #[serde(
        rename = "attributes",
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "json::sorted_map",
        deserialize_with = "json::string_map"
    )]
    pub attributes: HashMap<String, String>,
}

impl ClaimGrants {
    pub fn set_participant_kind(&mut self, kind: impl Into<i32>) {
        self.kind = kind_from_proto(kind);
    }

    pub fn get_participant_kind(&self) -> participant_info::Kind {
        kind_to_proto(&self.kind)
    }

    pub fn set_kind_detail<D: Into<i32> + Copy>(&mut self, details: &[D]) {
        self.kind_details = kind_details_from_proto(details);
    }

    pub fn get_kind_details(&self) -> Vec<participant_info::KindDetail> {
        kind_details_to_proto(&self.kind_details)
    }

    pub fn get_room_configuration(&self) -> Option<&livekit::RoomConfiguration> {
        self.room_config.as_ref().map(|c| &c.0)
    }

    // Clone: derived. A clone shares nothing with what it was made from.
}

// MarshalLogObject of the original: the name, the metadata, the attributes and the hash are
// not in it.
impl fmt::Debug for ClaimGrants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClaimGrants")
            .field("Identity", &self.identity)
            .field("Kind", &self.kind)
            .field("KindDetails", &self.kind_details)
            .field("Video", &self.video)
            .field("SIP", &self.sip)
            .field("Agent", &self.agent)
            .field("Inference", &self.inference)
            .field("Observability", &self.observability)
            .field("AgentEndpoint", &self.agent_endpoint)
            .field("AgentThread", &self.agent_thread)
            .field("RoomConfig", &self.room_config)
            .field("RoomPreset", &self.room_preset)
            .finish()
    }
}

// -------------------------------------------------------------

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct VideoGrant {
    // actions on rooms
    #[serde(rename = "roomCreate", skip_serializing_if = "is_false")]
    pub room_create: bool,
    #[serde(rename = "roomList", skip_serializing_if = "is_false")]
    pub room_list: bool,
    #[serde(rename = "roomRecord", skip_serializing_if = "is_false")]
    pub room_record: bool,

    // actions on a particular room
    #[serde(rename = "roomAdmin", skip_serializing_if = "is_false")]
    pub room_admin: bool,
    #[serde(rename = "roomJoin", skip_serializing_if = "is_false")]
    pub room_join: bool,
    #[serde(rename = "room", skip_serializing_if = "String::is_empty")]
    pub room: String,

    // permissions within a room, if none of the permissions are set explicitly
    // it will be granted with all publish and subscribe permissions
    #[serde(rename = "canPublish", skip_serializing_if = "Option::is_none")]
    pub can_publish: Option<bool>,
    #[serde(rename = "canSubscribe", skip_serializing_if = "Option::is_none")]
    pub can_subscribe: Option<bool>,
    #[serde(rename = "canPublishData", skip_serializing_if = "Option::is_none")]
    pub can_publish_data: Option<bool>,
    // TrackSource types that a participant may publish.
    // When set, it supersedes CanPublish. Only sources explicitly set here can be published
    #[serde(
        rename = "canPublishSources",
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "json::string_list"
    )]
    pub can_publish_sources: Vec<String>, // keys keep track of each source
    // by default, a participant is not allowed to update its own metadata
    #[serde(
        rename = "canUpdateOwnMetadata",
        skip_serializing_if = "Option::is_none"
    )]
    pub can_update_own_metadata: Option<bool>,

    // actions on ingresses
    #[serde(rename = "ingressAdmin", skip_serializing_if = "is_false")]
    pub ingress_admin: bool, // applies to all ingress

    // participant is not visible to other participants
    #[serde(rename = "hidden", skip_serializing_if = "is_false")]
    pub hidden: bool,
    // indicates to the room that current participant is a recorder
    #[serde(rename = "recorder", skip_serializing_if = "is_false")]
    pub recorder: bool,
    // indicates that the holder can register as an Agent framework worker
    #[serde(rename = "agent", skip_serializing_if = "is_false")]
    pub agent: bool,

    // if a participant can subscribe to metrics
    #[serde(
        rename = "canSubscribeMetrics",
        skip_serializing_if = "Option::is_none"
    )]
    pub can_subscribe_metrics: Option<bool>,

    // if a participant can manage an agent session via RemoteSession
    #[serde(
        rename = "canManageAgentSession",
        skip_serializing_if = "Option::is_none"
    )]
    pub can_manage_agent_session: Option<bool>,

    // destination room which this participant can forward to
    #[serde(rename = "destinationRoom", skip_serializing_if = "String::is_empty")]
    pub destination_room: String,
}

impl VideoGrant {
    pub fn set_can_publish(&mut self, val: bool) {
        self.can_publish = Some(val);
    }

    pub fn set_can_publish_data(&mut self, val: bool) {
        self.can_publish_data = Some(val);
    }

    pub fn set_can_subscribe(&mut self, val: bool) {
        self.can_subscribe = Some(val);
    }

    pub fn set_can_publish_sources<S: Into<i32> + Copy>(&mut self, sources: &[S]) {
        self.can_publish_sources = Vec::with_capacity(sources.len());
        for s in sources {
            self.can_publish_sources.push(source_to_string(*s));
        }
    }

    pub fn set_can_update_own_metadata(&mut self, val: bool) {
        self.can_update_own_metadata = Some(val);
    }

    pub fn set_can_subscribe_metrics(&mut self, val: bool) {
        self.can_subscribe_metrics = Some(val);
    }

    pub fn set_can_manage_agent_session(&mut self, val: bool) {
        self.can_manage_agent_session = Some(val);
    }

    pub fn get_can_publish(&self) -> bool {
        self.can_publish.unwrap_or(true)
    }

    pub fn get_can_publish_source(&self, source: impl Into<i32>) -> bool {
        if !self.get_can_publish() {
            return false;
        }
        // don't differentiate between nil and unset, since that distinction doesn't survive serialization
        if self.can_publish_sources.is_empty() {
            return true;
        }
        let source_str = source_to_string(source);
        for s in &self.can_publish_sources {
            if *s == source_str {
                return true;
            }
        }
        false
    }

    pub fn get_can_publish_sources(&self) -> Vec<TrackSource> {
        if self.can_publish_sources.is_empty() {
            return Vec::new();
        }

        let mut sources = Vec::with_capacity(self.can_publish_sources.len());
        for s in &self.can_publish_sources {
            sources.push(source_to_proto(s));
        }
        sources
    }

    pub fn get_can_publish_data(&self) -> bool {
        match self.can_publish_data {
            None => self.get_can_publish(),
            Some(can_publish_data) => can_publish_data,
        }
    }

    pub fn get_can_subscribe(&self) -> bool {
        self.can_subscribe.unwrap_or(true)
    }

    pub fn get_can_update_own_metadata(&self) -> bool {
        self.can_update_own_metadata.unwrap_or(false)
    }

    pub fn get_can_subscribe_metrics(&self) -> bool {
        self.can_subscribe_metrics.unwrap_or(false)
    }

    pub fn get_can_manage_agent_session(&self) -> bool {
        self.can_manage_agent_session.unwrap_or(false)
    }

    pub fn matches_permission(&self, permission: Option<&livekit::ParticipantPermission>) -> bool {
        let Some(permission) = permission else {
            return false;
        };

        if self.get_can_publish() != permission.can_publish {
            return false;
        }
        if self.get_can_publish_data() != permission.can_publish_data {
            return false;
        }
        if self.get_can_subscribe() != permission.can_subscribe {
            return false;
        }
        if self.get_can_update_own_metadata() != permission.can_update_metadata {
            return false;
        }
        if self.hidden != permission.hidden {
            return false;
        }
        if self.recorder != permission.recorder {
            return false;
        }
        if self.agent != permission.agent {
            return false;
        }
        if !self
            .get_can_publish_sources()
            .into_iter()
            .map(i32::from)
            .eq(permission.can_publish_sources.iter().copied())
        {
            return false;
        }
        if self.get_can_subscribe_metrics() != permission.can_subscribe_metrics {
            return false;
        }
        if self.get_can_manage_agent_session() != permission.can_manage_agent_session {
            return false;
        }

        true
    }

    pub fn update_from_permission(&mut self, permission: Option<&livekit::ParticipantPermission>) {
        let Some(permission) = permission else {
            return;
        };

        self.set_can_publish(permission.can_publish);
        self.set_can_publish_data(permission.can_publish_data);
        self.set_can_publish_sources(&permission.can_publish_sources);
        self.set_can_subscribe(permission.can_subscribe);
        self.set_can_update_own_metadata(permission.can_update_metadata);
        self.hidden = permission.hidden;
        self.recorder = permission.recorder;
        self.agent = permission.agent;
        self.set_can_subscribe_metrics(permission.can_subscribe_metrics);
        self.set_can_manage_agent_session(permission.can_manage_agent_session);
    }

    pub fn to_permission(&self) -> livekit::ParticipantPermission {
        livekit::ParticipantPermission {
            can_publish: self.get_can_publish(),
            can_publish_data: self.get_can_publish_data(),
            can_subscribe: self.get_can_subscribe(),
            can_publish_sources: self
                .get_can_publish_sources()
                .into_iter()
                .map(i32::from)
                .collect(),
            can_update_metadata: self.get_can_update_own_metadata(),
            hidden: self.hidden,
            recorder: self.recorder,
            agent: self.agent,
            can_subscribe_metrics: self.get_can_subscribe_metrics(),
            can_manage_agent_session: self.get_can_manage_agent_session(),
        }
    }

    // Clone: derived.
}

// MarshalLogObject of the original.
impl fmt::Debug for VideoGrant {
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

        f.debug_struct("VideoGrant")
            .field("RoomCreate", &self.room_create)
            .field("RoomList", &self.room_list)
            .field("RoomRecord", &self.room_record)
            .field("RoomAdmin", &self.room_admin)
            .field("RoomJoin", &self.room_join)
            .field("Room", &self.room)
            .field("CanPublish", &BoolPtr(self.can_publish))
            .field("CanSubscribe", &BoolPtr(self.can_subscribe))
            .field("CanPublishData", &BoolPtr(self.can_publish_data))
            .field("CanPublishSources", &self.can_publish_sources)
            .field(
                "CanUpdateOwnMetadata",
                &BoolPtr(self.can_update_own_metadata),
            )
            .field("IngressAdmin", &self.ingress_admin)
            .field("Hidden", &self.hidden)
            .field("Recorder", &self.recorder)
            .field("Agent", &self.agent)
            .field("CanSubscribeMetrics", &BoolPtr(self.can_subscribe_metrics))
            .field(
                "CanManageAgentSession",
                &BoolPtr(self.can_manage_agent_session),
            )
            .field("DestinationRoom", &self.destination_room)
            .finish()
    }
}

// ----------------------------------------------------------------

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SipGrant {
    // Admin grants access to all SIP features.
    #[serde(rename = "admin", skip_serializing_if = "is_false")]
    pub admin: bool,

    // Call allows making outbound SIP calls.
    #[serde(rename = "call", skip_serializing_if = "is_false")]
    pub call: bool,
}

// Clone: derived.

// MarshalLogObject of the original.
impl fmt::Debug for SipGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SipGrant")
            .field("Admin", &self.admin)
            .field("Call", &self.call)
            .finish()
    }
}

// ------------------------------------------------------------------

// ------------------------------------------------------------------

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentGrant {
    // Admin grants to create/update/delete Cloud Agents.
    #[serde(rename = "admin", skip_serializing_if = "is_false")]
    pub admin: bool,
    // SimulationAdmin grants access to manage simulations and scenarios for evaluating agents.
    #[serde(rename = "simulationAdmin", skip_serializing_if = "is_false")]
    pub simulation_admin: bool,
    // DatabaseAdmin grants access to a project's agent databases (AgentDB).
    #[serde(rename = "databaseAdmin", skip_serializing_if = "is_false")]
    pub database_admin: bool,
    // DispatchAdmin grants access to manage a project's agent dispatch queue.
    #[serde(rename = "dispatchAdmin", skip_serializing_if = "is_false")]
    pub dispatch_admin: bool,
}

// Clone: derived.

// MarshalLogObject of the original.
impl fmt::Debug for AgentGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentGrant")
            .field("Admin", &self.admin)
            .field("SimulationAdmin", &self.simulation_admin)
            .field("DatabaseAdmin", &self.database_admin)
            .field("DispatchAdmin", &self.dispatch_admin)
            .finish()
    }
}

// ------------------------------------------------------------------

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InferenceGrant {
    // Perform grants to all inference features (LLM, STT, TTS)
    #[serde(rename = "perform", skip_serializing_if = "is_false")]
    pub perform: bool,
}

// Clone: derived.

// MarshalLogObject of the original.
impl fmt::Debug for InferenceGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InferenceGrant")
            .field("Perform", &self.perform)
            .finish()
    }
}

// ------------------------------------------------------------------

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservabilityGrant {
    // Write grants to publish observability data
    #[serde(rename = "write", skip_serializing_if = "is_false")]
    pub write: bool,
}

// Clone: derived.

// MarshalLogObject of the original.
impl fmt::Debug for ObservabilityGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservabilityGrant")
            .field("Write", &self.write)
            .finish()
    }
}

// ------------------------------------------------------------------

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentEndpointGrant {
    // Call grants to invoke an agent's non-public HTTP endpoints.
    #[serde(rename = "call", skip_serializing_if = "is_false")]
    pub call: bool,
    // AgentName restricts the grant to one agent; empty grants every agent in the project.
    #[serde(rename = "agentName", skip_serializing_if = "String::is_empty")]
    pub agent_name: String,
    // Deployment restricts the grant to one deployment; empty grants every
    // deployment. A worker registered without one is addressed as "default".
    #[serde(rename = "deployment", skip_serializing_if = "String::is_empty")]
    pub deployment: String,
}

impl AgentEndpointGrant {
    // Allows reports whether the grant authorizes calling non-public endpoints of
    // (agentName, deployment). Matching is exact and case-sensitive; an empty scope
    // field matches any value.
    pub fn allows(&self, agent_name: &str, deployment: &str) -> bool {
        if !self.call {
            return false;
        }
        if !self.agent_name.is_empty() && self.agent_name != agent_name {
            return false;
        }
        self.deployment.is_empty() || self.deployment == deployment
    }

    // Clone: derived.
}

// MarshalLogObject of the original.
impl fmt::Debug for AgentEndpointGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentEndpointGrant")
            .field("Call", &self.call)
            .field("AgentName", &self.agent_name)
            .field("Deployment", &self.deployment)
            .finish()
    }
}

// ------------------------------------------------------------------

// AgentThreadGrant gives a token access to threads, across the project's
// agents: list and create within Scope, and read, write and delete the threads
// in ThreadIDs. All extends it to every thread and scope of the project; the
// action flags still apply. It grants thread data only; which endpoints a token
// may call is agentEndpoint's.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentThreadGrant {
    #[serde(rename = "all", skip_serializing_if = "is_false")]
    pub all: bool,
    #[serde(rename = "scope", skip_serializing_if = "String::is_empty")]
    pub scope: String,
    #[serde(
        rename = "threadIds",
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "json::string_list"
    )]
    pub thread_ids: Vec<String>,
    #[serde(rename = "list", skip_serializing_if = "is_false")]
    pub list: bool,
    #[serde(rename = "create", skip_serializing_if = "is_false")]
    pub create: bool,
    #[serde(rename = "write", skip_serializing_if = "is_false")]
    pub write: bool,
    #[serde(rename = "read", skip_serializing_if = "is_false")]
    pub read: bool,
    #[serde(rename = "delete", skip_serializing_if = "is_false")]
    pub delete: bool,
}

impl AgentThreadGrant {
    // CanCreate reports whether the holder may create threads in scope. A scoped
    // grant creates only in its own scope.
    pub fn can_create(&self, scope: &str) -> bool {
        self.create && self.in_scope(scope)
    }

    // CanList reports whether the holder may list the threads of scope.
    pub fn can_list(&self, scope: &str) -> bool {
        self.list && self.in_scope(scope)
    }

    // CanUpdate reports whether the holder may change any thread's attributes and
    // idle TTL, which only a project-wide grant may.
    pub fn can_update(&self) -> bool {
        self.all && self.write
    }

    // CanRead reports whether the holder may read threadID.
    pub fn can_read(&self, thread_id: &str) -> bool {
        self.read && self.includes(thread_id)
    }

    // CanWrite reports whether the holder may send to threadID.
    pub fn can_write(&self, thread_id: &str) -> bool {
        self.write && self.includes(thread_id)
    }

    // CanDelete reports whether the holder may delete threadID.
    pub fn can_delete(&self, thread_id: &str) -> bool {
        self.delete && self.includes(thread_id)
    }

    fn in_scope(&self, scope: &str) -> bool {
        self.all || (!self.scope.is_empty() && self.scope == scope)
    }

    fn includes(&self, thread_id: &str) -> bool {
        !thread_id.is_empty() && (self.all || self.thread_ids.iter().any(|id| id == thread_id))
    }

    // Clone: derived.
}

// MarshalLogObject of the original.
impl fmt::Debug for AgentThreadGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentThreadGrant")
            .field("All", &self.all)
            .field("Scope", &self.scope)
            .field("ThreadIDs", &self.thread_ids)
            .field("List", &self.list)
            .field("Create", &self.create)
            .field("Write", &self.write)
            .field("Read", &self.read)
            .field("Delete", &self.delete)
            .finish()
    }
}

// ------------------------------------------------------------------

// Go's `strings.ToLower(value.String())` for a value of an enumeration of the protocol: its
// name, or for a value that has none its number.
fn lower_name<E: TryFrom<i32>>(value: i32, as_str_name: impl Fn(&E) -> &'static str) -> String {
    match E::try_from(value) {
        Ok(e) => as_str_name(&e).to_lowercase(),
        Err(_) => value.to_string(),
    }
}

// Go's `strings.ToUpper`, which puts one character in the place of each. (Rust's
// `to_uppercase` makes "SS" of "ß", which would make a kind of "ingreß".)
fn to_upper(s: &str) -> String {
    s.chars()
        .map(|c| {
            let mut upper = c.to_uppercase();
            match (upper.next(), upper.next()) {
                (Some(u), None) => u,
                _ => c,
            }
        })
        .collect()
}

fn source_to_string(source: impl Into<i32>) -> String {
    lower_name(source.into(), TrackSource::as_str_name)
}

fn source_to_proto(source_str: &str) -> TrackSource {
    if let Some(val) = TrackSource::from_str_name(&to_upper(source_str)) {
        return val;
    }
    TrackSource::Unknown
}

fn kind_from_proto(source: impl Into<i32>) -> String {
    lower_name(source.into(), participant_info::Kind::as_str_name)
}

fn kind_to_proto(source_str: &str) -> participant_info::Kind {
    if let Some(val) = participant_info::Kind::from_str_name(&to_upper(source_str)) {
        return val;
    }
    participant_info::Kind::Standard
}

fn kind_details_from_proto<D: Into<i32> + Copy>(details: &[D]) -> Vec<String> {
    let mut result = Vec::with_capacity(details.len());
    for d in details {
        result.push(lower_name(
            (*d).into(),
            participant_info::KindDetail::as_str_name,
        ));
    }
    result
}

fn kind_details_to_proto(details: &[String]) -> Vec<participant_info::KindDetail> {
    let mut result = Vec::with_capacity(details.len());
    for d in details {
        if let Some(val) = participant_info::KindDetail::from_str_name(&to_upper(d)) {
            result.push(val);
        }
    }
    result
}

#[cfg(test)]
#[path = "grants_test.rs"]
mod grants_test;
