// Ported from LiveKit's protocol, utils/id.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `LocalNodeID` is left out, as in `guid`.

use super::guid;

pub const GUID_SIZE: usize = guid::SIZE;

pub const ROOM_PREFIX: &str = guid::ROOM_PREFIX;
pub const NODE_PREFIX: &str = guid::NODE_PREFIX;
pub const PARTICIPANT_PREFIX: &str = guid::PARTICIPANT_PREFIX;
pub const TRACK_PREFIX: &str = guid::TRACK_PREFIX;
pub const DATA_TRACK_PREFIX: &str = guid::DATA_TRACK_PREFIX;
pub const API_KEY_PREFIX: &str = guid::API_KEY_PREFIX;
pub const EGRESS_PREFIX: &str = guid::EGRESS_PREFIX;
pub const INGRESS_PREFIX: &str = guid::INGRESS_PREFIX;
pub const SIP_TRUNK_PREFIX: &str = guid::SIP_TRUNK_PREFIX;
pub const SIP_DISPATCH_RULE_PREFIX: &str = guid::SIP_DISPATCH_RULE_PREFIX;
pub const SIP_CALL_PREFIX: &str = guid::SIP_CALL_PREFIX;
pub const SIP_TRANSFER_PREFIX: &str = guid::SIP_TRANSFER_PREFIX;
pub const RPC_PREFIX: &str = guid::RPC_PREFIX;
pub const WHIP_RESOURCE_PREFIX: &str = guid::WHIP_RESOURCE_PREFIX;
pub const RTMP_RESOURCE_PREFIX: &str = guid::RTMP_RESOURCE_PREFIX;
pub const URL_RESOURCE_PREFIX: &str = guid::URL_RESOURCE_PREFIX;
pub const SIP_HOSTNAME_PREFIX: &str = guid::SIP_HOSTNAME_PREFIX;
pub const AGENT_WORKER_PREFIX: &str = guid::AGENT_WORKER_PREFIX;
pub const AGENT_JOB_PREFIX: &str = guid::AGENT_JOB_PREFIX;
pub const AGENT_DISPATCH_PREFIX: &str = guid::AGENT_DISPATCH_PREFIX;
pub const CLOUD_AGENT_PREFIX: &str = guid::CLOUD_AGENT_PREFIX;
pub const CLOUD_AGENT_REGION_PREFIX: &str = guid::CLOUD_AGENT_REGION_PREFIX;
pub const CLOUD_AGENT_VERSION_PREFIX: &str = guid::CLOUD_AGENT_VERSION_PREFIX;
pub const CLOUD_AGENT_SECRET_PREFIX: &str = guid::CLOUD_AGENT_SECRET_PREFIX;
pub const CLOUD_AGENT_WORKER_PREFIX: &str = guid::CLOUD_AGENT_WORKER_PREFIX;
pub const CLOUD_AGENT_PRIVATE_LINK_PREFIX: &str = guid::CLOUD_AGENT_PRIVATE_LINK_PREFIX;
pub const CLOUD_AGENT_PRIVATE_LINK_CLUSTER_PREFIX: &str =
    guid::CLOUD_AGENT_PRIVATE_LINK_CLUSTER_PREFIX;
pub const CLOUD_AGENT_PRIVATE_LINK_GATEWAY_PREFIX: &str =
    guid::CLOUD_AGENT_PRIVATE_LINK_GATEWAY_PREFIX;
pub const CLOUD_AGENT_PRIVATE_LINK_SECRET_PREFIX: &str =
    guid::CLOUD_AGENT_PRIVATE_LINK_SECRET_PREFIX;
pub const SCENARIO_PREFIX: &str = guid::SCENARIO_PREFIX;
pub const SCENARIO_GROUP_PREFIX: &str = guid::SCENARIO_GROUP_PREFIX;

pub fn new_guid(prefix: &str) -> String {
    guid::new(prefix)
}

// HashedID creates a hashed ID from a unique string
pub fn hashed_id(id: &str) -> String {
    guid::hashed_id(id)
}

pub fn marshal_guid<T: guid::Guid>(id: &T) -> guid::GuidBlock {
    guid::marshal(id)
}

pub fn unmarshal_guid<T: guid::Guid>(b: guid::GuidBlock) -> T {
    guid::unmarshal(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::livekit;

    // New for musivum: upstream has no test for this file; what it hands on to is tested in
    // `guid`.
    #[test]
    fn the_helpers_are_those_of_guid() {
        let id = new_guid(TRACK_PREFIX);
        assert!(id.starts_with("TR_"));
        assert_eq!(TRACK_PREFIX.len() + GUID_SIZE, id.len());
        assert_eq!(guid::hashed_id("test"), hashed_id("test"));

        let id = livekit::ParticipantId::from(new_guid(PARTICIPANT_PREFIX));
        assert_eq!(
            id,
            unmarshal_guid::<livekit::ParticipantId>(marshal_guid(&id))
        );
    }
}
