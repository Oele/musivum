// Ported from LiveKit, pkg/rtc/types/protocol_version.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `ProtocolVersion`, an `int` upstream, is a struct
// around the number, 32 bits wide as the field of the protocol it is converted from
// (`ClientInfo.protocol`); `CurrentProtocol`, an untyped constant upstream that is used both
// as that field and as a `ProtocolVersion`, is the number. The test at the end is new: the
// original has none.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(pub i32);

pub const CURRENT_PROTOCOL: i32 = 18;

impl ProtocolVersion {
    pub fn supports_packed_stream_id(self) -> bool {
        self.0 > 0
    }

    pub fn supports_protobuf(self) -> bool {
        self.0 > 0
    }

    pub fn handles_data_packets(self) -> bool {
        self.0 > 1
    }

    /// SubscriberAsPrimary indicates clients initiate subscriber connection as primary
    pub fn subscriber_as_primary(self) -> bool {
        self.0 > 2
    }

    /// SupportsSpeakerChanged - if client handles speaker info deltas, instead of a comprehensive list
    pub fn supports_speaker_changed(self) -> bool {
        self.0 > 2
    }

    /// SupportsTransceiverReuse - if transceiver reuse is supported, optimizes SDP size
    pub fn supports_transceiver_reuse(self) -> bool {
        self.0 > 3
    }

    /// SupportsConnectionQuality - avoid sending frequent ConnectionQuality updates for lower protocol versions
    pub fn supports_connection_quality(self) -> bool {
        self.0 > 4
    }

    pub fn supports_session_migrate(self) -> bool {
        self.0 > 5
    }

    pub fn supports_ice_lite(self) -> bool {
        self.0 > 5
    }

    pub fn supports_unpublish(self) -> bool {
        self.0 > 6
    }

    /// SupportFastStart - if client supports fast start, server side will send media streams
    /// in the first offer
    pub fn support_fast_start(self) -> bool {
        self.0 > 7
    }

    pub fn supports_disconnected_update(self) -> bool {
        self.0 > 8
    }

    pub fn supports_sync_stream_id(self) -> bool {
        self.0 > 9
    }

    pub fn supports_connection_quality_lost(self) -> bool {
        self.0 > 10
    }

    pub fn supports_async_room_id(self) -> bool {
        self.0 > 11
    }

    pub fn supports_identity_based_reconnection(self) -> bool {
        self.0 > 11
    }

    pub fn supports_regions_in_leave_request(self) -> bool {
        self.0 > 12
    }

    pub fn supports_non_error_signal_response(self) -> bool {
        self.0 > 14
    }

    pub fn supports_moving(self) -> bool {
        self.0 > 15
    }

    pub fn supports_packet_trailer(self) -> bool {
        self.0 > 16
    }

    pub fn supports_negotiated_data_channels(self) -> bool {
        self.0 > 17
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The table is what the original gives: for
    // each feature the first version that supports it (printed by a Go test that walked the
    // versions from -2 to 30 through the methods at the pin).
    #[test]
    fn each_feature_starts_at_the_version_of_the_original() {
        type Supports = fn(ProtocolVersion) -> bool;
        let table: [(&str, Supports, i32); 21] = [
            (
                "SupportsPackedStreamId",
                ProtocolVersion::supports_packed_stream_id,
                1,
            ),
            ("SupportsProtobuf", ProtocolVersion::supports_protobuf, 1),
            (
                "HandlesDataPackets",
                ProtocolVersion::handles_data_packets,
                2,
            ),
            (
                "SubscriberAsPrimary",
                ProtocolVersion::subscriber_as_primary,
                3,
            ),
            (
                "SupportsSpeakerChanged",
                ProtocolVersion::supports_speaker_changed,
                3,
            ),
            (
                "SupportsTransceiverReuse",
                ProtocolVersion::supports_transceiver_reuse,
                4,
            ),
            (
                "SupportsConnectionQuality",
                ProtocolVersion::supports_connection_quality,
                5,
            ),
            (
                "SupportsSessionMigrate",
                ProtocolVersion::supports_session_migrate,
                6,
            ),
            ("SupportsICELite", ProtocolVersion::supports_ice_lite, 6),
            ("SupportsUnpublish", ProtocolVersion::supports_unpublish, 7),
            ("SupportFastStart", ProtocolVersion::support_fast_start, 8),
            (
                "SupportsDisconnectedUpdate",
                ProtocolVersion::supports_disconnected_update,
                9,
            ),
            (
                "SupportsSyncStreamID",
                ProtocolVersion::supports_sync_stream_id,
                10,
            ),
            (
                "SupportsConnectionQualityLost",
                ProtocolVersion::supports_connection_quality_lost,
                11,
            ),
            (
                "SupportsAsyncRoomID",
                ProtocolVersion::supports_async_room_id,
                12,
            ),
            (
                "SupportsIdentityBasedReconnection",
                ProtocolVersion::supports_identity_based_reconnection,
                12,
            ),
            (
                "SupportsRegionsInLeaveRequest",
                ProtocolVersion::supports_regions_in_leave_request,
                13,
            ),
            (
                "SupportsNonErrorSignalResponse",
                ProtocolVersion::supports_non_error_signal_response,
                15,
            ),
            ("SupportsMoving", ProtocolVersion::supports_moving, 16),
            (
                "SupportsPacketTrailer",
                ProtocolVersion::supports_packet_trailer,
                17,
            ),
            (
                "SupportsNegotiatedDataChannels",
                ProtocolVersion::supports_negotiated_data_channels,
                18,
            ),
        ];

        for (name, supports, first) in table {
            for v in -2..=30 {
                assert_eq!(
                    v >= first,
                    supports(ProtocolVersion(v)),
                    "{name} at version {v}"
                );
            }
        }

        assert_eq!(18, CURRENT_PROTOCOL);
        // the current protocol supports everything, the one before it all but the newest
        for (name, supports, _) in table {
            assert!(supports(ProtocolVersion(CURRENT_PROTOCOL)), "{name}");
        }
        let before = ProtocolVersion(CURRENT_PROTOCOL - 1);
        assert!(before.supports_packet_trailer());
        assert!(!before.supports_negotiated_data_channels());
        assert!(!ProtocolVersion::default().supports_protobuf());
    }
}
