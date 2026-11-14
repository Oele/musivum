//! LiveKit's protocol definitions as Rust types.
//!
//! The types are generated at build time from the `.proto` files in `protobufs/`, which are
//! copies from `livekit/protocol` at the version the pinned LiveKit release is built with.
//! [`livekit`] is the counterpart of the Go package `github.com/livekit/protocol/livekit`.

#[allow(clippy::all, clippy::pedantic, missing_docs, rustdoc::all)]
pub mod livekit {
    include!(concat!(env!("OUT_DIR"), "/livekit.rs"));
    include!(concat!(env!("OUT_DIR"), "/livekit.serde.rs"));
}

#[allow(clippy::all, clippy::pedantic, missing_docs, rustdoc::all)]
pub mod logger {
    include!(concat!(env!("OUT_DIR"), "/logger.rs"));
}

pub use prost::Message;

/// `google.protobuf.Timestamp` as the messages have it in their fields: Go's
/// `timestamppb.Timestamp`.
pub use pbjson_types::Timestamp;

#[cfg(test)]
mod tests {
    use super::Message;
    use super::livekit;

    #[test]
    fn a_message_survives_the_wire_and_has_its_json_form() {
        let info = livekit::TrackInfo {
            sid: "TR_abc".into(),
            r#type: livekit::TrackType::Video as i32,
            muted: true,
            width: 1280,
            ..Default::default()
        };
        let back = livekit::TrackInfo::decode(info.encode_to_vec().as_slice()).unwrap();
        assert_eq!(info, back);

        // The JSON form is the one Protocol Buffers defines: lowerCamelCase names, enums by
        // name, defaults left out.
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"sid": "TR_abc", "type": "VIDEO", "muted": true, "width": 1280})
        );
        let from_json: livekit::TrackInfo = serde_json::from_value(json).unwrap();
        assert_eq!(info, from_json);
    }

    #[test]
    fn a_signal_request_carries_one_message() {
        let req = livekit::SignalRequest {
            message: Some(livekit::signal_request::Message::Ping(42)),
        };
        let back = livekit::SignalRequest::decode(req.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            back.message,
            Some(livekit::signal_request::Message::Ping(42))
        );
    }
}
