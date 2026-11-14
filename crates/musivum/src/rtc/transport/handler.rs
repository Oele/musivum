// Ported from LiveKit, pkg/rtc/transport/handler.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, against musivum's connection layer: pion's
// `*webrtc.TrackRemote` and `*webrtc.RTPReceiver` are the layer's seams
// (`Arc<dyn webrtc::TrackRemote>`, `Arc<dyn webrtc::RtpReceiver>`, which the layer's
// `on_track` gives as its own types), `webrtc.ICECandidate` and `SessionDescription` are the
// layer's. The candidate, nil when gathering is done, is an `Option`; the connection info
// and the stream state update, pointers that are never nil, are references; the error a
// data channel could not send with is `rtc::Error`, as is what the methods return: the
// transport that calls them and the participant that implements them are of package `rtc`.
// The three errors of the package are `Error`, which `rtc::Error` holds.
// New: every method has a default body, which is what the method of the same name does in
// the original's `UnimplementedHandler`; Go embeds that struct to get the methods it does
// not define, here an implementation leaves those methods out, and `UnimplementedHandler`
// is the handler that has nothing but them. In the original its `OnFailed` takes only
// `isShortLived`, so `UnimplementedHandler` alone is no `Handler` there (whatever embeds it
// has to define `OnFailed`); here the default `on_failed` takes both and does nothing.

use std::collections::HashMap;
use std::sync::Arc;

use super::NegotiationState;
use crate::protocol::livekit;
use crate::rtc;
use crate::rtc::types;
use crate::sfu::streamallocator::StreamStateUpdate;
use crate::webrtc;
use crate::webrtc::peerconnection::{IceCandidate, SessionDescription};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("no ICE candidate handler")]
    NoIceCandidateHandler,
    #[error("no offer handler")]
    NoOfferHandler,
    #[error("no answer handler")]
    NoAnswerHandler,
}

#[cfg_attr(test, musivum_fakes::fake)]
#[allow(unused_variables)]
pub trait Handler: Send + Sync {
    fn on_ice_candidate(
        &self,
        c: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), rtc::Error> {
        Err(Error::NoIceCandidateHandler.into())
    }
    fn on_initial_connected(&self) {}
    fn on_fully_established(&self) {}
    fn on_failed(&self, is_short_lived: bool, ice_connection_info: &types::IceConnectionInfo) {}
    fn on_track(
        &self,
        track: Arc<dyn webrtc::TrackRemote>,
        rtp_receiver: Arc<dyn webrtc::RtpReceiver>,
    ) {
    }
    fn on_data_message(&self, kind: livekit::data_packet::Kind, data: &[u8]) {}
    fn on_data_message_unlabeled(&self, data: &[u8]) {}
    fn on_data_track_message(&self, data: &[u8], arrival_time: i64) {}
    fn on_data_send_error(&self, err: rtc::Error) {}
    fn on_offer(
        &self,
        sd: SessionDescription,
        offer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), rtc::Error> {
        Err(Error::NoOfferHandler.into())
    }
    fn on_set_remote_description_offer(&self) {}
    fn on_answer(
        &self,
        sd: SessionDescription,
        answer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), rtc::Error> {
        Err(Error::NoAnswerHandler.into())
    }
    fn on_negotiation_state_changed(&self, state: NegotiationState) {}
    fn on_negotiation_failed(&self) {}
    fn on_stream_state_change(&self, update: &StreamStateUpdate) -> Result<(), rtc::Error> {
        Ok(())
    }
    fn on_unmatched_media(&self, num_audios: u32, num_videos: u32) -> Result<(), rtc::Error> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UnimplementedHandler;

impl Handler for UnimplementedHandler {}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no test of this file. What the original's
    // `UnimplementedHandler` answers, and that the fake records and answers as configured.
    #[test]
    fn unimplemented_handler_answers_as_the_original() {
        let h = UnimplementedHandler;
        assert_eq!(
            Err(rtc::Error::Transport(Error::NoIceCandidateHandler)),
            h.on_ice_candidate(None, livekit::SignalTarget::Publisher)
        );
        let sd = SessionDescription::default();
        assert_eq!(
            Err(rtc::Error::Transport(Error::NoOfferHandler)),
            h.on_offer(sd.clone(), 1, &HashMap::new())
        );
        assert_eq!(
            Err(rtc::Error::Transport(Error::NoAnswerHandler)),
            h.on_answer(sd, 1, &HashMap::new())
        );
        assert_eq!(
            Ok(()),
            h.on_stream_state_change(&StreamStateUpdate::default())
        );
        assert_eq!(Ok(()), h.on_unmatched_media(1, 2));
        assert_eq!(
            "no ICE candidate handler",
            rtc::Error::from(Error::NoIceCandidateHandler).to_string()
        );
        assert_eq!("no offer handler", Error::NoOfferHandler.to_string());
        assert_eq!("no answer handler", Error::NoAnswerHandler.to_string());

        let fake = Arc::new(FakeHandler::default());
        let handler: Arc<dyn Handler> = fake.clone();
        assert_eq!(
            Ok(()),
            handler.on_ice_candidate(None, livekit::SignalTarget::Subscriber)
        );
        fake.on_unmatched_media_returns(Err(rtc::Error::TransportFailure));
        assert_eq!(
            Err(rtc::Error::TransportFailure),
            handler.on_unmatched_media(3, 4)
        );
        handler.on_data_track_message(&[1, 2], 7);
        assert_eq!(1, fake.on_ice_candidate_call_count());
        assert_eq!(
            (None, livekit::SignalTarget::Subscriber),
            fake.on_ice_candidate_args_for_call(0)
        );
        assert_eq!((3, 4), fake.on_unmatched_media_args_for_call(0));
        assert_eq!((vec![1, 2], 7), fake.on_data_track_message_args_for_call(0));
    }
}
