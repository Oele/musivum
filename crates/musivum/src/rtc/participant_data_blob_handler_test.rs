// Ported from LiveKit, pkg/rtc/participant_data_blob_handler_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Upstream's tests make a whole participant
// (`newParticipantForTest`, participant_internal_test.go), which is not ported yet. Here
// the participant is `TestParticipant`, which has what the methods use of it: the
// parameters the tests set, a store of blobs, a fake listener, and responses written to a
// fake sink as participant_signal.go writes them (through `Signalling`, with the checks of
// `sendRequestResponse` left out: they pass for the client of upstream's test, which has no
// client info, and for the reasons the tests send). The participant handed to the listener
// is a fake that stands for the test's participant; "the listener was given `p`" is that
// fake. `genericKey` of participant_data_blob_test.go is written out again here. The
// subtests are blocks that carry their names in the messages of their assertions.

use std::sync::Arc;

use super::*;
use crate::protocol::livekit::signal_response;
use crate::protocol::logger;
use crate::routing::{FakeMessageSink, Message, MessageSink};
use crate::rtc::ParticipantDataBlobParams;
use crate::rtc::signalling::{ParticipantSignalling, Signalling, SignallingParams};
use crate::rtc::types::{FakeLocalParticipant, FakeLocalParticipantListener, FakeParticipant};

use livekit::request_response::Reason;

fn generic_key(name: &str) -> livekit::DataBlobKey {
    livekit::DataBlobKey {
        key: Some(livekit::data_blob_key::Key::Generic(name.to_string())),
    }
}

struct TestParticipantParams {
    enable_participant_data_blob: bool,
    limit_config: config::LimitConfig,
    sink: Arc<FakeMessageSink>,
    participant_listener: Arc<FakeLocalParticipantListener>,
}

struct TestParticipant {
    params: TestParticipantParams,
    pub_logger: Logger,
    data_blob: ParticipantDataBlob,
    signalling: Arc<Signalling>,
    // `p` as the listener is handed it
    me: Arc<FakeLocalParticipant>,
}

impl TestParticipant {
    fn write_message(&self, msg: Option<Message>) -> Result<(), Error> {
        if let Some(msg) = msg {
            self.params.sink.write_message(msg)?;
        }
        Ok(())
    }

    fn is(&self, p: &Arc<dyn LocalParticipant>) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(p), Arc::as_ptr(&self.me))
    }
}

impl ParticipantDataBlobHandler for TestParticipant {
    fn data_blob_participant(&self) -> Arc<dyn LocalParticipant> {
        self.me.clone()
    }

    fn enable_participant_data_blob(&self) -> bool {
        self.params.enable_participant_data_blob
    }

    fn limit_config(&self) -> &config::LimitConfig {
        &self.params.limit_config
    }

    fn pub_logger(&self) -> &Logger {
        &self.pub_logger
    }

    fn data_blob(&self) -> &ParticipantDataBlob {
        &self.data_blob
    }

    fn listener(&self) -> Arc<dyn LocalParticipantListener> {
        self.params.participant_listener.clone()
    }

    fn send_request_response(
        &self,
        request_response: livekit::RequestResponse,
    ) -> Result<(), Error> {
        self.write_message(self.signalling.signal_request_response(request_response))
    }

    fn send_store_data_blob_response(
        &self,
        request_id: u32,
        key: Option<livekit::DataBlobKey>,
    ) -> Result<(), Error> {
        self.write_message(
            self.signalling
                .signal_store_data_blob_response(livekit::StoreDataBlobResponse {
                    request_id,
                    key,
                }),
        )
    }

    fn send_get_data_blob_response(
        &self,
        request_id: u32,
        data_blob: Option<livekit::DataBlob>,
    ) -> Result<(), Error> {
        self.write_message(self.signalling.signal_get_data_blob_response(
            livekit::GetDataBlobResponse {
                request_id,
                blob: data_blob,
            },
        ))
    }
}

fn new_participant_with_data_blob(
    enabled: bool,
    max_key_length: i64,
    max_size: u32,
) -> TestParticipant {
    TestParticipant {
        params: TestParticipantParams {
            enable_participant_data_blob: enabled,
            limit_config: config::LimitConfig {
                max_data_blob_key_length: max_key_length,
                max_data_blob_size: max_size,
                ..Default::default()
            },
            sink: Arc::new(FakeMessageSink::default()),
            participant_listener: Arc::new(FakeLocalParticipantListener::default()),
        },
        pub_logger: logger::get_logger(),
        data_blob: ParticipantDataBlob::new(ParticipantDataBlobParams {
            logger: logger::get_logger(),
        }),
        signalling: Signalling::new(SignallingParams {
            logger: logger::get_logger(),
        }),
        me: Arc::new(FakeLocalParticipant::default()),
    }
}

// What a response of the sink's write `idx` carries.
#[track_caller]
fn response_message(sink: &FakeMessageSink, idx: usize) -> signal_response::Message {
    let Message::SignalResponse(msg) = sink.write_message_args_for_call(idx) else {
        panic!("expected a SignalResponse");
    };
    msg.message.expect("a SignalResponse without a message")
}

#[track_caller]
fn last_request_response(sink: &FakeMessageSink, idx: usize) -> livekit::RequestResponse {
    match response_message(sink, idx) {
        signal_response::Message::RequestResponse(rr) => rr,
        other => panic!("expected SignalResponse_RequestResponse, got {other:?}"),
    }
}

fn blob(key: Option<livekit::DataBlobKey>, contents: &[u8]) -> livekit::DataBlob {
    livekit::DataBlob {
        key,
        contents: contents.to_vec(),
    }
}

#[test]
fn test_handle_store_data_blob_request() {
    {
        let name = "returns NOT_ALLOWED when feature not enabled";
        let p = new_participant_with_data_blob(false, 0, 0);
        let sink = &p.params.sink;

        let req = livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(generic_key("blob-1")), b"def")),
            ..Default::default()
        };
        p.handle_store_data_blob_request(&req);

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::NotAllowed, rr.reason(), "{name}");
        assert!(p.data_blob.get_all().is_empty(), "{name}");
    }

    {
        let name = "returns INVALID_REQUEST when blob is nil";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest::default());

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::InvalidRequest, rr.reason(), "{name}");
        assert!(p.data_blob.get_all().is_empty(), "{name}");
    }

    {
        let name = "returns INVALID_REQUEST when key is nil";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(None, b"def")),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::InvalidRequest, rr.reason(), "{name}");
    }

    {
        let name = "returns INVALID_REQUEST when key has no oneof set";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(livekit::DataBlobKey::default()), b"def")),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::InvalidRequest, rr.reason(), "{name}");
    }

    {
        let name = "returns INVALID_REQUEST when key exceeds length limit";
        let p = new_participant_with_data_blob(true, 5, 0);
        let sink = &p.params.sink;

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(generic_key(&"a".repeat(64))), b"def")),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::InvalidRequest, rr.reason(), "{name}");
    }

    {
        let name = "returns INVALID_REQUEST when generic key is empty";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(generic_key("")), b"def")),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::InvalidRequest, rr.reason(), "{name}");
        assert!(p.data_blob.get_all().is_empty(), "{name}");
    }

    {
        let name = "accepts a key exactly at the length limit";
        let p = new_participant_with_data_blob(true, 5, 0);
        let sink = &p.params.sink;

        let key = generic_key("abcde");
        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(key.clone()), b"def")),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let msg = response_message(sink, 0);
        assert!(
            matches!(msg, signal_response::Message::StoreDataBlobResponse(_)),
            "{name}: expected SignalResponse_StoreDataBlobResponse, got {msg:?}"
        );
        assert!(p.data_blob.get(Some(&key)).is_some(), "{name}");
    }

    {
        let name = "counts only key and contents bytes against the size limit";
        let p = new_participant_with_data_blob(true, 0, 9);
        let sink = &p.params.sink;

        let key = generic_key("abc");
        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(key.clone()), b"defghi")),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let msg = response_message(sink, 0);
        assert!(
            matches!(msg, signal_response::Message::StoreDataBlobResponse(_)),
            "{name}: expected SignalResponse_StoreDataBlobResponse, got {msg:?}"
        );
        assert!(p.data_blob.get(Some(&key)).is_some(), "{name}");
    }

    {
        let name = "returns INVALID_REQUEST when contents is empty";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(generic_key("blob-1")), b"")),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::InvalidRequest, rr.reason(), "{name}");
        assert!(p.data_blob.get_all().is_empty(), "{name}");
    }

    {
        let name = "returns LIMIT_EXCEEDED when adding would breach the limit";
        let p = new_participant_with_data_blob(true, 0, 16);
        let sink = &p.params.sink;

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            blob: Some(blob(Some(generic_key("blob-1")), "x".repeat(32).as_bytes())),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::LimitExceeded, rr.reason(), "{name}");
        assert!(p.data_blob.get_all().is_empty(), "{name}");
    }

    {
        let name = "stores a valid blob, notifies listener, and sends response";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;
        let listener = &p.params.participant_listener;

        let key = generic_key("blob-1");
        let contents = b"definition-bytes";
        let blob = blob(Some(key.clone()), contents);

        p.handle_store_data_blob_request(&livekit::StoreDataBlobRequest {
            request_id: 42,
            blob: Some(blob.clone()),
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let msg = response_message(sink, 0);
        let signal_response::Message::StoreDataBlobResponse(response) = msg else {
            panic!("{name}: expected SignalResponse_StoreDataBlobResponse, got {msg:?}");
        };
        assert_eq!(42, response.request_id, "{name}");
        assert_eq!(Some(key.clone()), response.key, "{name}");

        let stored = p.data_blob.get(Some(&key));
        assert!(stored.is_some(), "{name}");
        assert_eq!(&contents[..], &stored.unwrap().contents[..], "{name}");

        assert_eq!(1, listener.on_store_data_blob_call_count(), "{name}");
        let (got_participant, got_blob) = listener.on_store_data_blob_args_for_call(0);
        assert!(p.is(&got_participant), "{name}");
        assert_eq!(blob, got_blob, "{name}");
    }
}

#[test]
fn test_handle_get_data_blob_request() {
    {
        let name = "returns INVALID_REQUEST when key is missing";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        p.handle_get_data_blob_request(&livekit::GetDataBlobRequest {
            participant_identity: "other".to_string(),
            ..Default::default()
        });

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::InvalidRequest, rr.reason(), "{name}");
    }

    {
        let name = "forwards request to listener when key is provided";
        let p = new_participant_with_data_blob(true, 0, 0);
        let listener = &p.params.participant_listener;

        let req = livekit::GetDataBlobRequest {
            participant_identity: "other".to_string(),
            key: Some(generic_key("blob-1")),
            ..Default::default()
        };
        p.handle_get_data_blob_request(&req);

        assert_eq!(1, listener.on_get_data_blob_call_count(), "{name}");
        let (got_participant, got_req) = listener.on_get_data_blob_args_for_call(0);
        assert!(p.is(&got_participant), "{name}");
        assert_eq!(req, got_req, "{name}");
    }
}

#[test]
fn test_get_data_blob() {
    let p = new_participant_with_data_blob(true, 0, 0);

    let key = generic_key("blob-1");
    assert!(p.get_data_blob(Some(&key)).is_none());

    let blob = blob(Some(key.clone()), b"definition");
    p.data_blob.add(Arc::new(blob));
    let got = p.get_data_blob(Some(&key));
    assert!(got.is_some());
    let got = got.unwrap();
    // `key.String()`, the text of the message, is the same for keys that are equal
    assert_eq!(Some(&key), got.key.as_ref());
    assert_eq!(&b"definition"[..], &got.contents[..]);
}

#[test]
fn test_process_get_data_blob_request() {
    {
        let name = "returns NOT_FOUND when publisher is nil";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        p.process_get_data_blob_request(
            &livekit::GetDataBlobRequest {
                key: Some(generic_key("blob-1")),
                ..Default::default()
            },
            None,
        );

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::NotFound, rr.reason(), "{name}");
        assert!(rr.message.contains("participant"), "{name}");
    }

    {
        let name = "returns NOT_FOUND when publisher has no matching blob";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        let publisher = Arc::new(FakeParticipant::default());
        publisher.get_data_blob_returns(None);

        let req = livekit::GetDataBlobRequest {
            key: Some(generic_key("blob-1")),
            ..Default::default()
        };
        p.process_get_data_blob_request(&req, Some(publisher.clone()));

        assert_eq!(1, publisher.get_data_blob_call_count(), "{name}");
        assert_eq!(req.key, publisher.get_data_blob_args_for_call(0), "{name}");

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let rr = last_request_response(sink, 0);
        assert_eq!(Reason::NotFound, rr.reason(), "{name}");
    }

    {
        let name = "sends blob response when publisher has a matching blob";
        let p = new_participant_with_data_blob(true, 0, 0);
        let sink = &p.params.sink;

        let key = generic_key("blob-1");
        let blob = blob(Some(key.clone()), b"definition-bytes");

        let publisher = Arc::new(FakeParticipant::default());
        publisher.get_data_blob_returns(Some(Arc::new(blob.clone())));

        p.process_get_data_blob_request(
            &livekit::GetDataBlobRequest {
                request_id: 42,
                key: Some(key),
                ..Default::default()
            },
            Some(publisher),
        );

        assert_eq!(1, sink.write_message_call_count(), "{name}");
        let msg = response_message(sink, 0);
        let signal_response::Message::GetDataBlobResponse(response) = msg else {
            panic!("{name}: expected SignalResponse_GetDataBlobResponse, got {msg:?}");
        };
        assert_eq!(42, response.request_id, "{name}");
        assert_eq!(Some(blob), response.blob, "{name}");
    }
}
