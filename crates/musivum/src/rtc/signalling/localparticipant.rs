//! The part of `types.LocalParticipant` that this package calls. Written new for musivum.
//!
//! `types.LocalParticipant` (LiveKit's `pkg/rtc/types/interfaces.go`, 142 methods and
//! everything they name) is not ported yet. The signal handler and the signaller, which are
//! given a `types.LocalParticipant` upstream, are given this until it is: exactly the methods
//! they call on it, under the original's names and with its signatures, in the order they
//! have there. When `types::LocalParticipant` exists it takes the place of this trait (or
//! this trait is implemented for whatever implements that one), and this file goes.
//!
//! A message of the protocol, a pointer upstream, is taken by reference. An `error` is a
//! [`LocalParticipantError`], any error: this package only logs what a participant returns,
//! and which errors a participant has is for the port of the participant to say.

use crate::protocol::livekit;
use crate::rtc::types::ParticipantCloseReason;

/// Go's `error`, as a method of a participant returns it.
pub type LocalParticipantError = Box<dyn std::error::Error + Send + Sync>;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait LocalParticipant: Send + Sync {
    // of types.Participant, which types.LocalParticipant embeds
    fn is_disconnected(&self) -> bool;

    fn is_ready(&self) -> bool;

    fn update_last_seen_signal(&self);

    // updates
    fn update_metadata(
        &self,
        update: &livekit::UpdateParticipantMetadata,
        from_admin: bool,
    ) -> Result<(), LocalParticipantError>;
    fn update_audio_track(
        &self,
        update: &livekit::UpdateLocalAudioTrack,
    ) -> Result<(), LocalParticipantError>;
    fn update_video_track(
        &self,
        update: &livekit::UpdateLocalVideoTrack,
    ) -> Result<(), LocalParticipantError>;

    // PeerConnection
    fn handle_ice_trickle(&self, trickle_request: &livekit::TrickleRequest);
    fn handle_offer(&self, sd: &livekit::SessionDescription) -> Result<(), LocalParticipantError>;
    fn add_track(&self, req: &livekit::AddTrackRequest);
    fn set_track_muted(
        &self,
        mute: &livekit::MuteTrackRequest,
        from_admin: bool,
    ) -> Option<livekit::TrackInfo>;

    fn handle_answer(&self, sd: &livekit::SessionDescription);

    // subscriptions
    fn update_subscribed_track_settings(
        &self,
        track_id: livekit::TrackId,
        settings: &livekit::UpdateTrackSettings,
    );

    fn update_signaling_rtt(&self, rtt: u32);

    fn handle_update_subscriptions(
        &self,
        track_ids: &[livekit::TrackId],
        participant_tracks: &[livekit::ParticipantTracks],
        subscribe: bool,
    );
    fn handle_update_subscription_permission(
        &self,
        subscription_permission: &livekit::SubscriptionPermission,
    ) -> Result<(), LocalParticipantError>;
    fn handle_sync_state(
        &self,
        sync_state: &livekit::SyncState,
    ) -> Result<(), LocalParticipantError>;
    fn handle_simulate_scenario(
        &self,
        simulate_scenario: &livekit::SimulateScenario,
    ) -> Result<(), LocalParticipantError>;
    fn handle_leave_request(&self, reason: ParticipantCloseReason);

    fn handle_publish_data_track_request(&self, req: &livekit::PublishDataTrackRequest);
    fn handle_unpublish_data_track_request(&self, req: &livekit::UnpublishDataTrackRequest);
    fn handle_update_data_subscription(&self, req: &livekit::UpdateDataSubscription);
    fn handle_store_data_blob_request(&self, req: &livekit::StoreDataBlobRequest);
    fn handle_get_data_blob_request(&self, req: &livekit::GetDataBlobRequest);
}
