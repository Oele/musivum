// Ported from LiveKit, pkg/rtc/signalling/signallerasync.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The participant is the `LocalParticipant` of this
// package (`localparticipant.rs`) until `types.LocalParticipant` is ported. It is held
// weakly, since the participant holds its signaller: a participant that is gone is one that
// is disconnected. `NewSignallerAsync` returns the signaller in an `Arc`, not as the
// interface. The embedded base is a field, and its methods are handed on to it one by one.
// `OnHandshakeOpened` goes to the base and is not kept in the params here as well. Go's
// `%T` of what a response carries is the table in `signal_response_type`. A message is
// moved into the sink: what is logged of it after the write is taken before. `psrpc.Canceled`
// is not among the errors of a sink that are passed over quietly: it is the code of the
// errors of the relay between nodes, which is not ported, and a sink here returns a
// `routing::Error`, which has no such error. Nothing of psrpc is ported for this package.
// A response is logged by its `Debug`, where the original logs it through `logger.Proto`,
// which leaves out what the protocol marks as sensitive. The test at the end is new: the
// original has none.

use std::sync::{Arc, Weak};

use super::interfaces::ParticipantSignaller;
use super::localparticipant::LocalParticipant;
use super::signallerasyncbase::{SignallerAsyncBase, SignallerAsyncBaseParams};
use crate::protocol::livekit::signal_response;
use crate::protocol::logger::{Level, Logger};
use crate::protocol::utils::error_is_one_of;
use crate::routing::{self, Message, MessageSink};
use crate::rtc::types::SignallingCloseReason;
use crate::{debugw, infow, warnw};

pub struct SignallerAsyncParams {
    pub logger: Logger,
    pub participant: Weak<dyn LocalParticipant>,
    pub on_handshake_opened: Option<Box<dyn Fn() + Send + Sync>>,
}

pub struct SignallerAsync {
    params: SignallerAsyncParams,

    signaller_async_base: Arc<SignallerAsyncBase>,
}

impl SignallerAsync {
    pub fn new(mut params: SignallerAsyncParams) -> Arc<Self> {
        let signaller_async_base = SignallerAsyncBase::new(SignallerAsyncBaseParams {
            logger: params.logger.clone(),
            on_handshake_opened: params.on_handshake_opened.take(),
        });
        Arc::new(Self {
            params,
            signaller_async_base,
        })
    }
}

impl ParticipantSignaller for SignallerAsync {
    fn swap_response_sink(
        &self,
        sink: Option<Arc<dyn MessageSink>>,
        reason: SignallingCloseReason,
    ) {
        self.signaller_async_base.swap_response_sink(sink, reason);
    }

    fn get_response_sink(&self) -> Option<Arc<dyn MessageSink>> {
        self.signaller_async_base.get_response_sink()
    }

    fn close_signal_connection(&self, reason: SignallingCloseReason) {
        self.signaller_async_base.close_signal_connection(reason);
    }

    fn handshake_pending(&self) -> bool {
        self.signaller_async_base.handshake_pending()
    }

    fn open_handshake(&self) {
        self.signaller_async_base.open_handshake();
    }

    fn write_message(&self, msg: Option<Message>) -> Result<(), routing::Error> {
        let Some(msg) = msg else {
            return Ok(());
        };

        let get_message_type = |msg: &Message| -> &'static str {
            let mut message_type = "unknown";
            if let Message::SignalResponse(typed) = msg {
                message_type = signal_response_type(typed.message.as_ref());
            }
            message_type
        };

        let participant = match self.params.participant.upgrade() {
            Some(participant) if !participant.is_disconnected() => participant,
            _ => {
                debugw!(
                    self.params.logger,
                    "could not send message to participant, participant disconnected",
                    "messageType" => get_message_type(&msg),
                );
                return Ok(());
            }
        };

        // a signal connection opens with a join response, or with a reconnect response when
        // it is resumed or migrated in. The client reads that first message as the handshake,
        // so nothing may go out ahead of it.
        let mut is_handshake = false;
        let mut is_sdp = false;
        match &msg {
            Message::SignalResponse(typed) => {
                is_handshake = matches!(
                    typed.message,
                    Some(signal_response::Message::Join(_))
                        | Some(signal_response::Message::Reconnect(_))
                );
                is_sdp = matches!(
                    typed.message,
                    Some(signal_response::Message::Offer(_))
                        | Some(signal_response::Message::Answer(_))
                );
            }
            msg => {
                warnw!(
                    self.params.logger,
                    "unknown message type", nil,
                    "messageType" => msg.type_name(),
                );
            }
        }

        if !is_handshake && (!participant.is_ready() || self.handshake_pending()) {
            if is_sdp {
                // a dropped SDP leaves the negotiation waiting for the peer until the
                // negotiation state machine recovers it, so make it visible
                infow!(
                    self.params.logger,
                    "dropping message, connection has not opened yet",
                    "messageType" => get_message_type(&msg),
                );
            } else {
                debugw!(
                    self.params.logger,
                    "dropping message, connection has not opened yet",
                    "messageType" => get_message_type(&msg),
                );
            }
            return Ok(());
        }

        let Some(sink) = self.get_response_sink() else {
            debugw!(
                self.params.logger,
                "could not send message to participant, no sink",
                "messageType" => get_message_type(&msg),
            );
            return Ok(());
        };

        // The sink takes the message: what the lines below log of it is taken here.
        let message_type = get_message_type(&msg);
        let response = self
            .params
            .logger
            .enabled(Level::Debug)
            .then(|| msg.clone());
        if let Err(err) = sink.write_message(msg) {
            // The original also passes over `psrpc.Canceled`, see the header.
            if error_is_one_of(&err, &[routing::Error::ChannelClosed]) {
                debugw!(
                    self.params.logger,
                    "could not send message to participant",
                    "error" => format_args!("{err}"),
                    "messageType" => message_type,
                );
                return Ok(());
            } else {
                warnw!(
                    self.params.logger,
                    "could not send message to participant", err,
                    "messageType" => message_type,
                );
                return Err(err);
            }
        } else {
            if is_handshake {
                // the connection is open, hold nothing back any more
                self.open_handshake();
            }
            if let Some(response) = &response {
                debugw!(self.params.logger, "sent signal response", "response" => response);
            }
        }
        Ok(())
    }
}

// Go's `fmt.Sprintf("%T", typed.Message)`: the Go type of what a response carries.
fn signal_response_type(message: Option<&signal_response::Message>) -> &'static str {
    use signal_response::Message;

    let Some(message) = message else {
        return "<nil>";
    };
    match message {
        Message::Join(_) => "*livekit.SignalResponse_Join",
        Message::Answer(_) => "*livekit.SignalResponse_Answer",
        Message::Offer(_) => "*livekit.SignalResponse_Offer",
        Message::Trickle(_) => "*livekit.SignalResponse_Trickle",
        Message::Update(_) => "*livekit.SignalResponse_Update",
        Message::TrackPublished(_) => "*livekit.SignalResponse_TrackPublished",
        Message::Leave(_) => "*livekit.SignalResponse_Leave",
        Message::Mute(_) => "*livekit.SignalResponse_Mute",
        Message::SpeakersChanged(_) => "*livekit.SignalResponse_SpeakersChanged",
        Message::RoomUpdate(_) => "*livekit.SignalResponse_RoomUpdate",
        Message::ConnectionQuality(_) => "*livekit.SignalResponse_ConnectionQuality",
        Message::StreamStateUpdate(_) => "*livekit.SignalResponse_StreamStateUpdate",
        Message::SubscribedQualityUpdate(_) => "*livekit.SignalResponse_SubscribedQualityUpdate",
        Message::SubscriptionPermissionUpdate(_) => {
            "*livekit.SignalResponse_SubscriptionPermissionUpdate"
        }
        Message::RefreshToken(_) => "*livekit.SignalResponse_RefreshToken",
        Message::TrackUnpublished(_) => "*livekit.SignalResponse_TrackUnpublished",
        Message::Pong(_) => "*livekit.SignalResponse_Pong",
        Message::Reconnect(_) => "*livekit.SignalResponse_Reconnect",
        Message::PongResp(_) => "*livekit.SignalResponse_PongResp",
        Message::SubscriptionResponse(_) => "*livekit.SignalResponse_SubscriptionResponse",
        Message::RequestResponse(_) => "*livekit.SignalResponse_RequestResponse",
        Message::TrackSubscribed(_) => "*livekit.SignalResponse_TrackSubscribed",
        Message::RoomMoved(_) => "*livekit.SignalResponse_RoomMoved",
        Message::MediaSectionsRequirement(_) => "*livekit.SignalResponse_MediaSectionsRequirement",
        Message::SubscribedAudioCodecUpdate(_) => {
            "*livekit.SignalResponse_SubscribedAudioCodecUpdate"
        }
        Message::PublishDataTrackResponse(_) => "*livekit.SignalResponse_PublishDataTrackResponse",
        Message::UnpublishDataTrackResponse(_) => {
            "*livekit.SignalResponse_UnpublishDataTrackResponse"
        }
        Message::DataTrackSubscriberHandles(_) => {
            "*livekit.SignalResponse_DataTrackSubscriberHandles"
        }
        Message::StoreDataBlobResponse(_) => "*livekit.SignalResponse_StoreDataBlobResponse",
        Message::GetDataBlobResponse(_) => "*livekit.SignalResponse_GetDataBlobResponse",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::atomic;
    use crate::protocol::livekit;
    use crate::protocol::logger;
    use crate::routing::FakeMessageSink;
    use crate::rtc::signalling::FakeLocalParticipant;

    fn response(message: signal_response::Message) -> Message {
        livekit::SignalResponse {
            message: Some(message),
        }
        .into()
    }

    fn refresh_token() -> Message {
        response(signal_response::Message::RefreshToken("token".to_string()))
    }

    // New for musivum: the original has no test of the signaller.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_message_is_written_once_the_connection_has_opened() {
        let participant = Arc::new(FakeLocalParticipant::default());
        let p: Arc<dyn LocalParticipant> = participant.clone();
        let opened = Arc::new(atomic::Int32::new(0));
        let counter = opened.clone();
        let signaller = SignallerAsync::new(SignallerAsyncParams {
            logger: logger::get_logger(),
            participant: Arc::downgrade(&p),
            on_handshake_opened: Some(Box::new(move || {
                counter.inc();
            })),
        });

        // nothing to write
        assert_eq!(Ok(()), signaller.write_message(None));
        assert_eq!(0, participant.is_disconnected_call_count());

        // no connection
        participant.is_ready_returns(true);
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));

        let sink = Arc::new(FakeMessageSink::default());
        signaller.swap_response_sink(Some(sink.clone()), SignallingCloseReason::Unknown);

        // a participant that is not ready is sent the join response and nothing else
        participant.is_ready_returns(false);
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        assert_eq!(0, sink.write_message_call_count());
        let join = response(signal_response::Message::Join(
            livekit::JoinResponse::default(),
        ));
        assert_eq!(Ok(()), signaller.write_message(Some(join.clone())));
        assert_eq!(1, sink.write_message_call_count());
        assert_eq!(join, sink.write_message_args_for_call(0));

        participant.is_ready_returns(true);
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        assert_eq!(refresh_token(), sink.write_message_args_for_call(1));
        assert_eq!(0, opened.load());

        // a resumed connection opens with the reconnect response
        let resumed = Arc::new(FakeMessageSink::default());
        signaller.swap_response_sink(Some(resumed.clone()), SignallingCloseReason::Resume);
        assert_eq!(1, sink.close_call_count());
        assert!(signaller.handshake_pending());
        let offer = response(signal_response::Message::Offer(
            livekit::SessionDescription::default(),
        ));
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        assert_eq!(Ok(()), signaller.write_message(Some(offer)));
        assert_eq!(0, resumed.write_message_call_count());
        let reconnect = response(signal_response::Message::Reconnect(
            livekit::ReconnectResponse::default(),
        ));
        assert_eq!(Ok(()), signaller.write_message(Some(reconnect.clone())));
        assert_eq!(reconnect, resumed.write_message_args_for_call(0));
        assert!(!signaller.handshake_pending());
        assert_eq!(1, opened.load());
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        assert_eq!(2, resumed.write_message_call_count());

        // a message of the wrong kind is written all the same, as in the original
        let request = Message::from(livekit::SignalRequest::default());
        assert_eq!(Ok(()), signaller.write_message(Some(request.clone())));
        assert_eq!(request, resumed.write_message_args_for_call(2));

        // a sink that is closed is passed over, another error is returned
        resumed.write_message_returns(Err(routing::Error::ChannelClosed));
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        resumed.write_message_returns(Err(routing::Error::ChannelFull));
        assert_eq!(
            Err(routing::Error::ChannelFull),
            signaller.write_message(Some(refresh_token()))
        );
        resumed.write_message_returns(Ok(()));
        assert_eq!(5, resumed.write_message_call_count());

        // nothing is written for a participant that is disconnected
        participant.is_disconnected_returns(true);
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        assert_eq!(5, resumed.write_message_call_count());
        participant.is_disconnected_returns(false);

        signaller.close_signal_connection(SignallingCloseReason::ParticipantClose);
        assert_eq!(1, resumed.close_call_count());
        assert!(signaller.get_response_sink().is_none());
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        assert_eq!(5, resumed.write_message_call_count());

        // New: the participant is gone
        signaller.swap_response_sink(Some(resumed.clone()), SignallingCloseReason::Unknown);
        drop(p);
        drop(participant);
        assert_eq!(Ok(()), signaller.write_message(Some(refresh_token())));
        assert_eq!(5, resumed.write_message_call_count());
    }
}
