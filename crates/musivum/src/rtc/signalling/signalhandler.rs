// Ported from LiveKit, pkg/rtc/signalling/signalhandler.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The participant is the `LocalParticipant` of this
// package (`localparticipant.rs`) until `types.LocalParticipant` is ported. It is held
// weakly, since the participant holds its handler: a request for a participant that is gone
// is dropped. `NewSignalHandler` returns the handler in an `Arc`, not as the interface. A
// request is logged by its `Debug`, where the original logs it through `logger.Proto`, which
// leaves out what the protocol marks as sensitive. The test at the end is new: the original
// has none.

use std::sync::{Arc, Weak};

use super::errors::Error;
use super::interfaces::ParticipantSignalHandler;
use super::localparticipant::LocalParticipant;
use crate::protocol::livekit::{self, signal_request};
use crate::protocol::logger::Logger;
use crate::routing::Message;
use crate::rtc::types::ParticipantCloseReason;
use crate::{debugw, warnw};

pub struct SignalHandlerParams {
    pub logger: Logger,
    pub participant: Weak<dyn LocalParticipant>,
}

pub struct SignalHandler {
    params: SignalHandlerParams,
}

impl SignalHandler {
    pub fn new(params: SignalHandlerParams) -> Arc<Self> {
        Arc::new(Self { params })
    }
}

impl ParticipantSignalHandler for SignalHandler {
    fn handle_message(&self, msg: Message) -> Result<(), Error> {
        let req = match msg {
            Message::SignalRequest(req) => req,
            msg => {
                warnw!(
                    self.params.logger,
                    "unknown message type", nil,
                    "messageType" => msg.type_name(),
                );
                return Err(Error::InvalidMessageType);
            }
        };
        let Some(participant) = self.params.participant.upgrade() else {
            return Ok(());
        };
        participant.update_last_seen_signal();

        debugw!(self.params.logger, "handling signal request", "request" => req);
        match &req.message {
            Some(signal_request::Message::Offer(offer)) => {
                let _ = participant.handle_offer(offer);
            }

            Some(signal_request::Message::Answer(answer)) => {
                participant.handle_answer(answer);
            }

            Some(signal_request::Message::Trickle(trickle)) => {
                participant.handle_ice_trickle(trickle);
            }

            Some(signal_request::Message::AddTrack(add_track)) => {
                participant.add_track(add_track);
            }

            Some(signal_request::Message::Mute(mute)) => {
                participant.set_track_muted(mute, false);
            }

            Some(signal_request::Message::Subscription(subscription)) => {
                // allow participant to indicate their interest in the subscription
                // permission check happens later in SubscriptionManager
                participant.handle_update_subscriptions(
                    &livekit::strings_as_ids::<livekit::TrackId>(&subscription.track_sids),
                    &subscription.participant_tracks,
                    subscription.subscribe,
                );
            }

            Some(signal_request::Message::TrackSetting(track_setting)) => {
                for sid in livekit::strings_as_ids::<livekit::TrackId>(&track_setting.track_sids) {
                    participant.update_subscribed_track_settings(sid, track_setting);
                }
            }

            Some(signal_request::Message::Leave(leave)) => {
                let mut reason = ParticipantCloseReason::ClientRequestLeave;
                match leave.reason() {
                    livekit::DisconnectReason::ClientInitiated => {
                        reason = ParticipantCloseReason::ClientRequestLeave;
                    }
                    livekit::DisconnectReason::UserUnavailable => {
                        reason = ParticipantCloseReason::UserUnavailable;
                    }
                    livekit::DisconnectReason::UserRejected => {
                        reason = ParticipantCloseReason::UserRejected;
                    }
                    livekit::DisconnectReason::AgentError => {
                        reason = ParticipantCloseReason::AgentError;
                    }
                    livekit::DisconnectReason::SipTrunkFailure => {
                        reason = ParticipantCloseReason::SipTrunkFailure;
                    }
                    _ => {}
                }
                debugw!(
                    self.params.logger,
                    "client leaving room",
                    "reason" => format_args!("{reason}"),
                );
                participant.handle_leave_request(reason);
            }

            Some(signal_request::Message::SubscriptionPermission(subscription_permission)) => {
                if let Err(err) =
                    participant.handle_update_subscription_permission(subscription_permission)
                {
                    warnw!(
                        self.params.logger,
                        "could not update subscription permission", err,
                        "permissions" => subscription_permission,
                    );
                }
            }

            Some(signal_request::Message::SyncState(sync_state)) => {
                if let Err(err) = participant.handle_sync_state(sync_state) {
                    warnw!(
                        self.params.logger,
                        "could not sync state", err,
                        "state" => sync_state,
                    );
                }
            }

            Some(signal_request::Message::Simulate(simulate)) => {
                if let Err(err) = participant.handle_simulate_scenario(simulate) {
                    warnw!(
                        self.params.logger,
                        "could not simulate scenario", err,
                        "simulate" => simulate,
                    );
                }
            }

            Some(signal_request::Message::PingReq(ping_req)) => {
                if ping_req.rtt > 0 {
                    participant.update_signaling_rtt(ping_req.rtt as u32);
                }
            }

            Some(signal_request::Message::UpdateMetadata(update_metadata)) => {
                let _ = participant.update_metadata(update_metadata, false);
            }

            Some(signal_request::Message::UpdateAudioTrack(update_audio_track)) => {
                if let Err(err) = participant.update_audio_track(update_audio_track) {
                    warnw!(
                        self.params.logger,
                        "could not update audio track", err,
                        "update" => update_audio_track,
                    );
                }
            }

            Some(signal_request::Message::UpdateVideoTrack(update_video_track)) => {
                if let Err(err) = participant.update_video_track(update_video_track) {
                    warnw!(
                        self.params.logger,
                        "could not update video track", err,
                        "update" => update_video_track,
                    );
                }
            }

            Some(signal_request::Message::PublishDataTrackRequest(publish_data_track_request)) => {
                participant.handle_publish_data_track_request(publish_data_track_request);
            }

            Some(signal_request::Message::UnpublishDataTrackRequest(
                unpublish_data_track_request,
            )) => {
                participant.handle_unpublish_data_track_request(unpublish_data_track_request);
            }

            Some(signal_request::Message::UpdateDataSubscription(update_data_subscription)) => {
                participant.handle_update_data_subscription(update_data_subscription);
            }

            Some(signal_request::Message::StoreDataBlobRequest(store_data_blob_request)) => {
                participant.handle_store_data_blob_request(store_data_blob_request);
            }

            Some(signal_request::Message::GetDataBlobRequest(get_data_blob_request)) => {
                participant.handle_get_data_blob_request(get_data_blob_request);
            }

            // The original has no case for the others: `update_layers`, `ping`, and a
            // request without a message.
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::logger;
    use crate::rtc::signalling::FakeLocalParticipant;

    fn request(message: signal_request::Message) -> Message {
        livekit::SignalRequest {
            message: Some(message),
        }
        .into()
    }

    // New for musivum: the original has no test of the handler.
    #[test]
    fn a_request_is_handed_to_the_participant() {
        let participant = Arc::new(FakeLocalParticipant::default());
        let p: Arc<dyn LocalParticipant> = participant.clone();
        let handler = SignalHandler::new(SignalHandlerParams {
            logger: logger::get_logger(),
            participant: Arc::downgrade(&p),
        });

        // a response is not for the handler
        assert_eq!(
            Err(Error::InvalidMessageType),
            handler.handle_message(livekit::SignalResponse::default().into())
        );
        assert_eq!(0, participant.update_last_seen_signal_call_count());

        let offer = livekit::SessionDescription {
            r#type: "offer".to_string(),
            sdp: "v=0".to_string(),
            id: 2,
            ..Default::default()
        };
        assert_eq!(
            Ok(()),
            handler.handle_message(request(signal_request::Message::Offer(offer.clone())))
        );
        assert_eq!(offer, participant.handle_offer_args_for_call(0));
        assert_eq!(0, participant.handle_answer_call_count());

        // muted by the client, not by an admin
        let mute = livekit::MuteTrackRequest {
            sid: "TR_1".to_string(),
            muted: true,
        };
        handler
            .handle_message(request(signal_request::Message::Mute(mute.clone())))
            .unwrap();
        assert_eq!((mute, false), participant.set_track_muted_args_for_call(0));

        let subscription = livekit::UpdateSubscription {
            track_sids: vec!["TR_3".to_string()],
            subscribe: true,
            participant_tracks: vec![livekit::ParticipantTracks {
                participant_sid: "PA_2".to_string(),
                track_sids: vec!["TR_4".to_string()],
            }],
        };
        handler
            .handle_message(request(signal_request::Message::Subscription(
                subscription.clone(),
            )))
            .unwrap();
        assert_eq!(
            (
                vec![livekit::TrackId::from("TR_3")],
                subscription.participant_tracks.clone(),
                true
            ),
            participant.handle_update_subscriptions_args_for_call(0)
        );

        // the settings go to every track they name
        let track_setting = livekit::UpdateTrackSettings {
            track_sids: vec!["TR_1".to_string(), "TR_2".to_string()],
            disabled: true,
            ..Default::default()
        };
        handler
            .handle_message(request(signal_request::Message::TrackSetting(
                track_setting.clone(),
            )))
            .unwrap();
        assert_eq!(2, participant.update_subscribed_track_settings_call_count());
        assert_eq!(
            (livekit::TrackId::from("TR_2"), track_setting),
            participant.update_subscribed_track_settings_args_for_call(1)
        );

        let reasons = [
            (
                livekit::DisconnectReason::ClientInitiated,
                ParticipantCloseReason::ClientRequestLeave,
            ),
            (
                livekit::DisconnectReason::UserUnavailable,
                ParticipantCloseReason::UserUnavailable,
            ),
            (
                livekit::DisconnectReason::UserRejected,
                ParticipantCloseReason::UserRejected,
            ),
            (
                livekit::DisconnectReason::AgentError,
                ParticipantCloseReason::AgentError,
            ),
            (
                livekit::DisconnectReason::SipTrunkFailure,
                ParticipantCloseReason::SipTrunkFailure,
            ),
            // any other reason is a leave the client asked for
            (
                livekit::DisconnectReason::ServerShutdown,
                ParticipantCloseReason::ClientRequestLeave,
            ),
        ];
        for (i, (reason, expected)) in reasons.into_iter().enumerate() {
            handler
                .handle_message(request(signal_request::Message::Leave(
                    livekit::LeaveRequest {
                        reason: reason as i32,
                        ..Default::default()
                    },
                )))
                .unwrap();
            assert_eq!(
                expected,
                participant.handle_leave_request_args_for_call(i),
                "{reason:?}"
            );
        }

        // a round trip time is taken when the client has measured one
        for rtt in [0, 35] {
            handler
                .handle_message(request(signal_request::Message::PingReq(livekit::Ping {
                    timestamp: 1,
                    rtt,
                })))
                .unwrap();
        }
        assert_eq!(1, participant.update_signaling_rtt_call_count());
        assert_eq!(35, participant.update_signaling_rtt_args_for_call(0));

        let update_metadata = livekit::UpdateParticipantMetadata {
            name: "Alice".to_string(),
            ..Default::default()
        };
        handler
            .handle_message(request(signal_request::Message::UpdateMetadata(
                update_metadata.clone(),
            )))
            .unwrap();
        assert_eq!(
            (update_metadata, false),
            participant.update_metadata_args_for_call(0)
        );

        // what the participant makes of a request is logged, not returned
        participant.handle_sync_state_returns(Err("out of sync".into()));
        assert_eq!(
            Ok(()),
            handler.handle_message(request(signal_request::Message::SyncState(
                livekit::SyncState::default()
            )))
        );
        assert_eq!(1, participant.handle_sync_state_call_count());

        // a request the handler has no case for
        assert_eq!(
            Ok(()),
            handler.handle_message(request(signal_request::Message::Ping(1)))
        );

        // every request is a sign of life
        assert_eq!(15, participant.update_last_seen_signal_call_count());

        // New: the participant is gone
        drop(p);
        drop(participant);
        assert_eq!(
            Ok(()),
            handler.handle_message(request(signal_request::Message::Ping(1)))
        );
    }
}
