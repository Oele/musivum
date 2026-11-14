// Ported from LiveKit, pkg/rtc/participant_data_blob_handler.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The functions of the original are methods of
// `ParticipantImpl` (participant.go), which is not ported yet. Here they are the provided
// methods of the trait `ParticipantDataBlobHandler`, whose required methods are what they
// use of the participant: its parameters (`EnableParticipantDataBlob`, `LimitConfig`), its
// `pubLogger` and store of blobs (`dataBlob`), its `listener()`, the participant itself as
// the listener is handed it, and the three responses of participant_signal.go. The
// participant implements the trait, and its `LocalParticipant` methods of the same names
// call these (`ParticipantDataBlobHandler::handle_store_data_blob_request(self, req)`).
// The blob of a request is stored as a copy in an `Arc` (`add_data_blob` takes the message
// by reference, as `types::LocalParticipant` has it), where the original stores the pointer
// it was given. The errors of the responses are dropped, as upstream. A request is logged
// by its `Debug`, where the original logs it through `logger.Proto`.

use std::sync::Arc;

use crate::config;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::Error;
use crate::rtc::ParticipantDataBlob;
use crate::rtc::types::{LocalParticipant, LocalParticipantListener, Participant};
use crate::warnw;

/// The methods of participant_data_blob_handler.go, on the participant they are methods of
/// upstream. New for musivum: see the header.
pub trait ParticipantDataBlobHandler: Send + Sync {
    // ---- what the methods of the original use of the participant

    /// The participant itself, as `p` is handed to its listener.
    fn data_blob_participant(&self) -> Arc<dyn LocalParticipant>;
    /// `p.params.EnableParticipantDataBlob`.
    fn enable_participant_data_blob(&self) -> bool;
    /// `p.params.LimitConfig`.
    fn limit_config(&self) -> &config::LimitConfig;
    /// `p.pubLogger`.
    fn pub_logger(&self) -> &Logger;
    /// `p.dataBlob`.
    fn data_blob(&self) -> &ParticipantDataBlob;
    /// `p.listener()` of participant.go.
    fn listener(&self) -> Arc<dyn LocalParticipantListener>;
    /// `p.sendRequestResponse` of participant_signal.go.
    fn send_request_response(
        &self,
        request_response: livekit::RequestResponse,
    ) -> Result<(), Error>;
    /// `p.sendStoreDataBlobResponse` of participant_signal.go.
    fn send_store_data_blob_response(
        &self,
        request_id: u32,
        key: Option<livekit::DataBlobKey>,
    ) -> Result<(), Error>;
    /// `p.sendGetDataBlobResponse` of participant_signal.go.
    fn send_get_data_blob_response(
        &self,
        request_id: u32,
        data_blob: Option<livekit::DataBlob>,
    ) -> Result<(), Error>;

    // ---- participant_data_blob_handler.go

    fn handle_store_data_blob_request(&self, req: &livekit::StoreDataBlobRequest) {
        if !self.enable_participant_data_blob() {
            warnw!(self.pub_logger(), "data blob not enabled", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::NotAllowed as i32,
                message: "data blob not enabled".to_string(),
                ..Default::default()
            });
            return;
        }

        let key = req.blob.as_ref().and_then(|blob| blob.key.as_ref());
        if req.blob.is_none()
            || key.is_none()
            || config::data_blob_key_length(key) == 0
            || !self.limit_config().check_data_blob_key_length(key)
        {
            warnw!(self.pub_logger(), "data blob is invalid", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::InvalidRequest as i32,
                message: "data blob is invalid".to_string(),
                ..Default::default()
            });
            return;
        }
        let Some(blob) = req.blob.as_ref() else {
            return;
        };

        // req.Blob.Key.GetSchemaId(): nil for a key that is not a schema id
        let schema_id = match key.and_then(|key| key.key.as_ref()) {
            Some(livekit::data_blob_key::Key::SchemaId(schema_id)) => Some(schema_id),
            _ => None,
        };
        if !self.limit_config().check_data_track_schema_id(schema_id) {
            warnw!(self.pub_logger(), "data blob key schema id is invalid", nil, "req" => req);
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::InvalidRequest as i32,
                message: "encoding identifier is empty or exceeds the maximum length".to_string(),
                ..Default::default()
            });
            return;
        }

        if blob.contents.is_empty() {
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::InvalidRequest as i32,
                message: "data blob is empty".to_string(),
                ..Default::default()
            });
            return;
        }

        if !self
            .limit_config()
            .can_add_data_blob(&self.data_blob().get_all(), blob)
        {
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::LimitExceeded as i32,
                message: "async attribute definition exceeds limit".to_string(),
                ..Default::default()
            });
            return;
        }

        ParticipantDataBlobHandler::add_data_blob(self, blob);
        self.listener()
            .on_store_data_blob(self.data_blob_participant(), blob);
        let _ = self.send_store_data_blob_response(req.request_id, blob.key.clone());
    }

    fn handle_get_data_blob_request(&self, req: &livekit::GetDataBlobRequest) {
        if req.key.is_none() {
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::InvalidRequest as i32,
                message: "data blob key is required".to_string(),
                ..Default::default()
            });
            return;
        }

        self.listener()
            .on_get_data_blob(self.data_blob_participant(), req);
    }

    fn add_data_blob(&self, data_blob: &livekit::DataBlob) {
        self.data_blob().add(Arc::new(data_blob.clone()));
    }

    fn get_data_blob(&self, key: Option<&livekit::DataBlobKey>) -> Option<Arc<livekit::DataBlob>> {
        self.data_blob().get(key)
    }

    fn process_get_data_blob_request(
        &self,
        req: &livekit::GetDataBlobRequest,
        publisher: Option<Arc<dyn Participant>>,
    ) {
        let Some(publisher) = publisher else {
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::NotFound as i32,
                message: "participant not found".to_string(),
                ..Default::default()
            });
            return;
        };

        let Some(data_blob) = publisher.get_data_blob(req.key.as_ref()) else {
            let _ = self.send_request_response(livekit::RequestResponse {
                request_id: req.request_id,
                reason: livekit::request_response::Reason::NotFound as i32,
                message: "data blob not found".to_string(),
                ..Default::default()
            });
            return;
        };

        let _ = self.send_get_data_blob_response(req.request_id, Some((*data_blob).clone()));
    }

    fn get_all_data_blob(&self) -> Vec<Arc<livekit::DataBlob>> {
        self.data_blob().get_all()
    }
}

#[cfg(test)]
#[path = "participant_data_blob_handler_test.rs"]
mod participant_data_blob_handler_test;
