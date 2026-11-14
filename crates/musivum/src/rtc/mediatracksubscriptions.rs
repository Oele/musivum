// Ported from LiveKit, pkg/rtc/mediatracksubscriptions.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `NewMediaTrackSubscriptions` is `MediaTrackSubscriptions::new` and gives an `Arc`; the
//   subscriptions keep a `Weak` of themselves, which is what the subscribed tracks they make
//   are told of a closed down track through.
// - Who holds whom. The media track owns its subscriptions (through its receiver), so they
//   hold it weakly, in an `Option` as the zero value of the params has none: a media track
//   that is gone is one that is not open (`add_subscriber` answers `Error::NotOpen`), mutes
//   nothing and has no `TrackInfo`. The subscriptions hold the subscribed tracks until
//   their down tracks close. What `add_subscriber` leaves with the down track to keep (the
//   callback of its binding) holds the subscribed track, the down track and the media track
//   weakly; what it leaves with the subscribed track (the first `on_close`) holds the
//   subscriber and the sender, which the subscribed track holds anyway.
// - The three callbacks, plain fields upstream that are set before the first subscriber
//   comes, are behind a lock each, as their setters take `&self`; a subscribed track is
//   given the ones there are when it is made, as upstream.
// - `dtState`, which the callback of the binding reads and `add_subscriber` writes once
//   after handing the callback over, is behind a lock of its own.
// - The two errors are variants of the package's `Error`.
// - `rtpSender.ReplaceTrack(downTrack)` on a cached transceiver whose sender has sent binds
//   the down track before it returns in pion, and so calls the callback of its binding from
//   inside `AddSubscriber`; the seam to the WebRTC library (`webrtc::RtpSender::replace_track`)
//   binds it later, on the connection's task, and a failure to bind is reported to that
//   callback, not returned (see `docs/webrtc-library.md`, "Third round"). The error of
//   `existingTransceiver.Stop()`, which the original drops, is dropped.
// - `GetAllSubscribersForMime` passes over a subscribed track without a down track, where
//   the original would dereference nil.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use super::{
    DirectionConfig, Error, OnDownTrackCreated, OnSubscriberAudioCodecChange,
    OnSubscriberMaxQualityChange, ReceiverConfig, SubscribedTrack, SubscribedTrackParams,
    WrappedReceiver, is_red_enabled,
};
use crate::go::atomic;
use crate::go::time;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::types;
use crate::sfu;
use crate::sfu::TrackReceiver as _;
use crate::webrtc;
use crate::{debugw, errorw, warnw};

// errAlreadySubscribed and errNotFound are `Error::AlreadySubscribed` and `Error::NotFound`.

// MediaTrackSubscriptions manages subscriptions of a media track
pub struct MediaTrackSubscriptions {
    params: MediaTrackSubscriptionsParams,

    subscribed_tracks_mu: RwLock<HashMap<livekit::ParticipantId, Arc<dyn types::SubscribedTrack>>>,

    on_down_track_created: RwLock<Option<OnDownTrackCreated>>,
    on_subscriber_max_quality_change: RwLock<Option<OnSubscriberMaxQualityChange>>,
    on_subscriber_audio_codec_change: RwLock<Option<OnSubscriberAudioCodecChange>>,

    // the subscriptions themselves, for the subscribed tracks they make
    this: Weak<MediaTrackSubscriptions>,
}

#[derive(Default)]
pub struct MediaTrackSubscriptionsParams {
    pub media_track: Option<Weak<dyn types::MediaTrack>>,
    pub is_relayed: bool,

    pub receiver_config: ReceiverConfig,
    pub subscriber_config: DirectionConfig,

    pub logger: Logger,
}

impl MediaTrackSubscriptions {
    pub fn new(params: MediaTrackSubscriptionsParams) -> Arc<MediaTrackSubscriptions> {
        Arc::new_cyclic(|this| MediaTrackSubscriptions {
            params,
            subscribed_tracks_mu: RwLock::new(HashMap::new()),
            on_down_track_created: RwLock::new(None),
            on_subscriber_max_quality_change: RwLock::new(None),
            on_subscriber_audio_codec_change: RwLock::new(None),
            this: this.clone(),
        })
    }

    pub fn on_down_track_created(&self, f: impl Fn(Arc<sfu::DownTrack>) + Send + Sync + 'static) {
        *self.on_down_track_created.write() = Some(Arc::new(f));
    }

    pub fn on_subscriber_max_quality_change(
        &self,
        f: impl Fn(&livekit::ParticipantId, mime::MimeType, i32) + Send + Sync + 'static,
    ) {
        *self.on_subscriber_max_quality_change.write() = Some(Arc::new(f));
    }

    pub fn on_subscriber_audio_codec_change(
        &self,
        f: impl Fn(&livekit::ParticipantId, mime::MimeType, bool) + Send + Sync + 'static,
    ) {
        *self.on_subscriber_audio_codec_change.write() = Some(Arc::new(f));
    }

    pub fn set_muted(&self, muted: bool) {
        // update mute of all subscribed tracks
        for st in self.get_all_subscribed_tracks() {
            st.set_publisher_muted(muted);
        }
    }

    pub fn is_subscriber(&self, sub_id: &livekit::ParticipantId) -> bool {
        let subscribed_tracks = self.subscribed_tracks_mu.read();

        subscribed_tracks.contains_key(sub_id)
    }

    // AddSubscriber subscribes sub to current mediaTrack
    pub fn add_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
        wr: Arc<WrappedReceiver>,
    ) -> Result<Arc<dyn types::SubscribedTrack>, Error> {
        let Some(media_track) = self.media_track() else {
            return Err(Error::NotOpen);
        };
        let track_id = media_track.id();
        let subscriber_id = sub.id();

        // don't subscribe to the same track multiple times
        let subscribed_tracks = self.subscribed_tracks_mu.write();
        if subscribed_tracks.contains_key(&subscriber_id) {
            return Err(Error::AlreadySubscribed);
        }
        drop(subscribed_tracks);

        let this = self.this.clone();
        let sub_track = SubscribedTrack::new(SubscribedTrackParams {
            receiver_config: self.params.receiver_config,
            subscriber_config: self.params.subscriber_config.clone(),
            subscriber: sub.clone(),
            media_track: media_track.clone(),
            adaptive_stream: sub.get_adaptive_stream(),
            enable_start_at_desired_quality: sub.get_enable_start_at_desired_quality(),
            telemetry_listener: sub.get_telemetry_listener(),
            wrapped_receiver: wr.clone(),
            is_relayed: self.params.is_relayed,
            on_down_track_created: self.on_down_track_created.read().clone(),
            on_down_track_closed: Some(Box::new(move |subscriber_id| {
                if let Some(t) = this.upgrade() {
                    let removed = t.subscribed_tracks_mu.write().remove(subscriber_id);
                    drop(removed);
                }
            })),
            on_subscriber_max_quality_change: self.on_subscriber_max_quality_change.read().clone(),
            on_subscriber_audio_codec_change: self.on_subscriber_audio_codec_change.read().clone(),
        })?;

        // Bind callback can happen from replaceTrack, so set it up early
        let reusing_transceiver = Arc::new(atomic::Bool::default());
        let dt_state = Arc::new(Mutex::new(sfu::DownTrackState::default()));
        let down_track = sub_track.dt().clone();
        {
            let sub_track = Arc::downgrade(&sub_track);
            let weak_down_track = Arc::downgrade(&down_track);
            let wr = wr.clone();
            let media_track = self.params.media_track.clone();
            let reusing_transceiver = reusing_transceiver.clone();
            let dt_state = dt_state.clone();
            let track_id = track_id.clone();
            down_track.on_binding(move |err| {
                // Both are there while the down track calls: it is the subscribed track's.
                let (Some(sub_track), Some(down_track)) =
                    (sub_track.upgrade(), weak_down_track.upgrade())
                else {
                    return;
                };
                if let Err(err) = err {
                    time::spawn(async move {
                        sub_track.bound(Err(err.into()));
                    });
                    return;
                }
                let sub = sub_track.subscriber();
                if reusing_transceiver.load() {
                    debugw!(sub.get_logger(), "seeding downtrack state", "trackID" => track_id);
                    let dt_state = dt_state.lock().clone();
                    down_track.seed_state(dt_state);
                }
                if let Err(err) = wr.add_down_track(down_track.clone())
                    && err != sfu::Error::ReceiverClosed
                {
                    errorw!(
                        sub.get_logger(),
                        "could not add down track",
                        err,
                        "publisher" => sub_track.publisher_identity(),
                        "publisherID" => sub_track.publisher_id(),
                        "trackID" => track_id,
                    );
                }

                {
                    let sub_track = sub_track.clone();
                    time::spawn(async move {
                        sub_track.bound(Ok(()));
                    });
                }

                let is_muted = media_track
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .is_some_and(|media_track| media_track.is_muted());
                sub_track.set_publisher_muted(is_muted);
            });
        }

        let mut transceiver: Option<Arc<dyn webrtc::RtpTransceiver>> = None;
        let mut sender: Option<Arc<dyn webrtc::RtpSender>> = None;

        // try cached RTP senders for a chance to replace track
        let mut replaced_track = false;
        let (existing_transceiver, cached_state) = sub.get_cached_down_track(&track_id);
        *dt_state.lock() = cached_state;
        if let Some(existing_transceiver) = existing_transceiver {
            debugw!(
                sub.get_logger(),
                "trying to use existing transceiver",
                "publisher" => sub_track.publisher_identity(),
                "publisherID" => sub_track.publisher_id(),
                "trackID" => track_id,
            );
            reusing_transceiver.store(true);
            let rtp_sender = existing_transceiver.sender();
            if let Some(rtp_sender) = rtp_sender {
                // replaced track will bind immediately without negotiation, SetTransceiver first before bind
                down_track.set_transceiver(existing_transceiver.clone());
                let err = rtp_sender.replace_track(Some(down_track.clone()));
                if err.is_ok() {
                    sender = Some(rtp_sender);
                    transceiver = Some(existing_transceiver.clone());
                    replaced_track = true;
                    debugw!(
                        sub.get_logger(),
                        "track replaced",
                        "publisher" => sub_track.publisher_identity(),
                        "publisherID" => sub_track.publisher_id(),
                        "trackID" => track_id,
                    );
                }
            }

            if !replaced_track {
                // Could not re-use cached transceiver for this track.
                // Stop the transceiver so that it is at least not active.
                // It is not usable once stopped,
                //
                // Adding down track will create a new transceiver (or re-use
                // an inactive existing one). In either case, a renegotiation
                // will happen and that will notify remote of this stopped
                // transceiver
                let _ = existing_transceiver.stop();
                reusing_transceiver.store(false);
            }
        }

        // if cannot replace, find an unused transceiver or add new one
        let (sender, transceiver) = match (sender, transceiver) {
            (Some(sender), Some(transceiver)) => (sender, transceiver),
            _ => {
                let info = media_track.to_proto();
                let mut add_track_params = types::AddTrackParams {
                    stereo: info
                        .audio_features
                        .contains(&(livekit::AudioTrackFeature::TfStereo as i32)),
                    red: is_red_enabled(&info),
                };
                let codecs = wr.codecs();
                if add_track_params.red
                    && (codecs.len() == 1 && mime::is_mime_type_string_opus(&codecs[0].mime_type))
                {
                    add_track_params.red = false;
                }

                sub.verify_subscribe_participant_info(
                    &sub_track.publisher_id(),
                    sub_track.publisher_version(),
                );
                if sub.supports_transceiver_reuse(media_track.clone()) {
                    //
                    // AddTrack will create a new transceiver or re-use an unused one
                    // if the attributes match. This prevents SDP from bloating
                    // because of dormant transceivers building up.
                    //
                    sub.add_track_local(down_track.clone(), add_track_params)?
                } else {
                    sub.add_transceiver_from_track_local(down_track.clone(), add_track_params)?
                }
            }
        };

        // whether re-using or stopping remove transceiver from cache
        // NOTE: safety net, if somehow a cached transceiver is re-used by a different track
        sub.uncache_down_track(transceiver.clone());

        // negotiation isn't required if we've replaced track
        // ONE-SHOT-SIGNALLING-MODE: this should not be needed, but that mode information is not available here,
        //   but it is not detrimental to set this, needs clean up when participants modes are separated out better.
        sub_track.set_needs_negotiation(!replaced_track);
        sub_track.set_rtp_sender(Some(sender.clone()));

        // it is possible that subscribed track is closed before subscription manager sets
        // the `OnClose` callback. That handler in subscription manager removes the track
        // from the peer connection.
        //
        // But, the subscription could be removed early if the published track is closed
        // while adding subscription. In those cases, subscription manager would not have set
        // the `OnClose` callback. So, set it here to handle cases of early close.
        // Subscription manager will reset this if this subscription proceeds till that point.
        {
            let sub = sub.clone();
            let logger = self.params.logger.clone();
            sub_track.on_close(Some(Box::new(move |is_expected_to_resume| {
                if !is_expected_to_resume && let Err(err) = sub.remove_track_local(sender.clone()) {
                    warnw!(logger, "could not remove track from peer connection", err);
                }
            })));
        }

        down_track.set_transceiver(transceiver);

        let mut subscribed_tracks = self.subscribed_tracks_mu.write();
        subscribed_tracks.insert(subscriber_id, sub_track.clone());
        drop(subscribed_tracks);

        Ok(sub_track)
    }

    // RemoveSubscriber removes participant from subscription
    // stop all forwarders to the client
    pub fn remove_subscriber(
        &self,
        subscriber_id: &livekit::ParticipantId,
        is_expected_to_resume: bool,
    ) -> Result<(), Error> {
        let Some(sub_track) = self.get_subscribed_track(subscriber_id) else {
            return Err(Error::NotFound);
        };

        debugw!(
            self.params.logger,
            "removing subscriber",
            "subscriberID" => subscriber_id,
            "isExpectedToResume" => is_expected_to_resume,
        );
        self.close_subscribed_track(sub_track, is_expected_to_resume);
        Ok(())
    }

    fn close_subscribed_track(
        &self,
        sub_track: Arc<dyn types::SubscribedTrack>,
        is_expected_to_resume: bool,
    ) {
        let Some(dt) = sub_track.down_track() else {
            return;
        };

        if is_expected_to_resume {
            dt.close_with_flush(false, false);
        } else {
            // flushing blocks, avoid blocking when publisher removes all its subscribers
            time::spawn(async move {
                dt.close_with_flush(true, true);
            });
        }
    }

    pub fn get_all_subscribers(&self) -> Vec<livekit::ParticipantId> {
        let subscribed_tracks = self.subscribed_tracks_mu.read();

        let mut subs = Vec::with_capacity(subscribed_tracks.len());
        for id in subscribed_tracks.keys() {
            subs.push(id.clone());
        }
        subs
    }

    pub fn get_all_subscribers_for_mime(
        &self,
        mime: mime::MimeType,
    ) -> Vec<livekit::ParticipantId> {
        let subscribed_tracks = self.subscribed_tracks_mu.read();

        let mut subs = Vec::with_capacity(subscribed_tracks.len());
        for (id, sub_track) in subscribed_tracks.iter() {
            if sub_track.down_track().is_none_or(|dt| dt.mime() != mime) {
                continue;
            }

            subs.push(id.clone());
        }
        subs
    }

    pub fn get_num_subscribers(&self) -> usize {
        let subscribed_tracks = self.subscribed_tracks_mu.read();

        subscribed_tracks.len()
    }

    pub fn update_video_layers(&self) {
        for st in self.get_all_subscribed_tracks() {
            st.update_video_layer();
        }
    }

    fn get_subscribed_track(
        &self,
        subscriber_id: &livekit::ParticipantId,
    ) -> Option<Arc<dyn types::SubscribedTrack>> {
        let subscribed_tracks = self.subscribed_tracks_mu.read();

        subscribed_tracks.get(subscriber_id).cloned()
    }

    pub(super) fn get_all_subscribed_tracks(&self) -> Vec<Arc<dyn types::SubscribedTrack>> {
        let subscribed_tracks = self.subscribed_tracks_mu.read();

        Self::get_all_subscribed_tracks_locked(&subscribed_tracks)
    }

    fn get_all_subscribed_tracks_locked(
        subscribed_tracks: &HashMap<livekit::ParticipantId, Arc<dyn types::SubscribedTrack>>,
    ) -> Vec<Arc<dyn types::SubscribedTrack>> {
        let mut sub_tracks = Vec::with_capacity(subscribed_tracks.len());
        for sub_track in subscribed_tracks.values() {
            sub_tracks.push(sub_track.clone());
        }
        sub_tracks
    }

    pub fn debug_info(&self) -> Vec<sfu::DebugInfo> {
        let mut subscribed_track_info = Vec::new();
        for val in self.get_all_subscribed_tracks() {
            let val: Arc<dyn Any + Send + Sync> = val;
            if let Ok(st) = val.downcast::<SubscribedTrack>() {
                subscribed_track_info.push(st.dt().debug_info());
            }
        }

        subscribed_track_info
    }

    // `t.params.MediaTrack`: the media track these are the subscriptions of, if it is there.
    fn media_track(&self) -> Option<Arc<dyn types::MediaTrack>> {
        self.params.media_track.as_ref().and_then(Weak::upgrade)
    }
}

#[cfg(test)]
#[path = "mediatracksubscriptions_test.rs"]
mod mediatracksubscriptions_test;
