// Ported from LiveKit, pkg/rtc/subscriptionmanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `NewSubscriptionManager` is `SubscriptionManager::new` and gives an `Arc`; the manager
//   keeps a `Weak` of itself for the callbacks it registers and the tasks it starts.
// - Who holds whom. The participant holds its manager, so the manager holds the participant
//   weakly (`params.participant` is a `Weak`): a call for a participant that is gone does
//   nothing, and the methods that the reconciler runs (`subscribe`, `subscribe_synchronous`,
//   `add_subscriber`, `subscribe_data_track`) are handed the participant their caller got
//   hold of, where the original reads it from the params. The worker, the observers on the
//   notifiers and the callbacks for "bound" and "stream started" hold the manager and the
//   subscription weakly. The two callbacks for "closed" (of a subscribed track and of a data
//   down track) hold the manager, the subscription and the participant strongly, as the
//   original's closures do: a participant that has closed must stay until its down tracks
//   have closed, because that is when its unsubscribes are told to telemetry. Such a
//   callback is let go of when its track has closed (`set_subscribed_track(None)` and
//   `set_data_down_track(None)` take it back with `on_close(None)`), so the cycle
//   participant, manager, subscription, track, callback lasts as long as the subscription.
// - The three times that the original keeps in variables "to override in tests" are
//   constants, with the values of the test file's `init()` in a test build, which like
//   `init()` holds for every test of the crate. The one test that changes the reconcile
//   interval for itself makes its manager with `new_with_reconcile_interval`: tests run at
//   the same time here. `subscriptionTimeout` is `iceFailedTimeoutTotal` of `transport.go`,
//   which is not ported yet: its value is written out as `ICE_FAILED_TIMEOUT_TOTAL`.
// - The reconciler is a task. The two queues are bounded tokio channels of which the worker
//   holds the receiving halves, `closeCh` is a `CancellationToken`, and `doneCh` is a flag
//   with a condition variable that the worker sets when it ends, also when it is dropped
//   without having run. `close` waits for it by blocking its thread, as the original blocks
//   its goroutine, and tells the runtime so when the thread is a worker of a multi-threaded
//   one (`tokio::task::block_in_place`). On a current-thread runtime it does not wait: the
//   caller is on the one thread the worker runs on, so the worker is at an await and not in
//   a pass, and every pass starts with `can_reconcile`, which a closed manager denies.
//   `wait_until_subscribed` sleeps the same way; on a current-thread runtime nothing is
//   reconciled while it does. A manager that is dropped without `close` ends its worker too.
// - `trackSubscription` is embedded in the two kinds of subscription and its lock also
//   guards fields of theirs: `TrackSubscription<T>` has those fields as `outer: T` inside
//   its lock, and the two kinds are reached through `Deref`. `MediaTrackSubscription::new`
//   and `DataTrackSubscription::new` give `Arc`s. The two times in atomic pointers are
//   `atomic::Time`, zero for nil (`maybe_record_success` reads the zero time where the
//   original would dereference nil); the kind in an atomic pointer is a `OnceLock`.
// - `notify_data_track_subscriber_handles` reads the publisher of a subscription under the
//   subscription's lock; the original reads the two fields without it.
// - A subscribed track without a media track (`media_track()` is `None`) and a data down
//   track without a data track: where the original calls a method on the nil track, nothing
//   is done for it here.
// - Errors: `ErrNotOpen` and `ErrNoReceiver` are of `mediatrackreceiver.go`, which is not
//   ported yet, and are variants of the package's `Error` already; `context.DeadlineExceeded`
//   is `Error::DeadlineExceeded`. `*rtcerr.InvalidStateError`, which pion's `RemoveTrack`
//   returns for a connection that is closed, and for nothing else, is
//   `Error::Webrtc(webrtc::Error::ConnectionClosed)`.
// - The callbacks of the params take the track id and the error by reference; the callback
//   of `on_subscribe_status_changed` takes the publisher's id by reference. The data track
//   resolver is not optional (the original's tests leave it nil where no data track is
//   subscribed).
// - The results of the resolvers have no `Debug` and are logged by the fields that have one.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::{Condvar, Mutex, RwLock};
use tokio::runtime::RuntimeFlavor;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::{Error, SubscribePacer};
use crate::go::atomic;
#[cfg(not(test))]
use crate::go::time::MINUTE;
use crate::go::time::{self, Duration, MILLISECOND, SECOND, Time};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils;
use crate::rtc::types::{
    ChangeNotifier, DataDownTrack, DataResolverResult, DataTrackResolver, LocalParticipant,
    MediaResolverResult, MediaTrack, MediaTrackResolver, ParticipantTelemetryListener,
    SubscribedTrack,
};
use crate::sfu;
use crate::telemetry::prometheus;
use crate::webrtc;
use crate::{debugw, infow, warnw};

// using var instead of const to override in tests
#[cfg(not(test))]
const RECONCILE_INTERVAL: Duration = Duration::from_secs(3);
// amount of time to give up if a track or publisher isn't found
// ensuring this is longer than iceFailedTimeout so we are certain the participant won't return
#[cfg(not(test))]
const NOT_FOUND_TIMEOUT: Duration = MINUTE;
// amount of time to try otherwise before flagging subscription as failed
#[cfg(not(test))]
const SUBSCRIPTION_TIMEOUT: Duration = ICE_FAILED_TIMEOUT_TOTAL;
const MAX_UNSUBSCRIBE_WAIT: Duration = SECOND;

// what the test file's `init()` sets the first three to
#[cfg(test)]
use subscriptionmanager_test::{NOT_FOUND_TIMEOUT, RECONCILE_INTERVAL, SUBSCRIPTION_TIMEOUT};

// `iceFailedTimeoutTotal` of transport.go, which is not ported yet: iceFailedTimeout (5 s)
// + iceDisconnectedTimeout (10 s), the total time between connecting and failure
#[cfg(not(test))]
const ICE_FAILED_TIMEOUT_TOTAL: Duration = Duration::from_secs(15);

const TRACK_ID_FOR_RECONCILE_SUBSCRIPTIONS: &str = "subscriptions_reconcile";

pub struct SubscriptionManagerParams {
    pub logger: Logger,
    pub participant: Weak<dyn LocalParticipant>,
    pub track_resolver: MediaTrackResolver,
    pub on_track_subscribed: Box<dyn Fn(Arc<dyn SubscribedTrack>) + Send + Sync>,
    pub on_track_unsubscribed: Box<dyn Fn(Arc<dyn SubscribedTrack>) + Send + Sync>,
    #[allow(clippy::type_complexity)]
    pub on_subscription_error: Box<dyn Fn(&livekit::TrackId, bool, &Error) + Send + Sync>,

    pub subscription_limit_video: i32,
    pub subscription_limit_audio: i32,

    pub subscribe_pacer: Option<Arc<SubscribePacer>>,

    pub data_track_resolver: DataTrackResolver,

    pub use_one_shot_signalling_mode: bool,
}

type OnSubscribeStatusChanged = Arc<dyn Fn(&livekit::ParticipantId, bool) + Send + Sync>;

// SubscriptionManager manages a participant's subscriptions
pub struct SubscriptionManager {
    params: SubscriptionManagerParams,
    this: Weak<SubscriptionManager>,
    lock: RwLock<SubscriptionManagerInner>,
    pending_unsubscribes: atomic::Int32,

    subscribed_video_count: atomic::Int32,
    subscribed_audio_count: atomic::Int32,

    reconcile_ch: mpsc::Sender<livekit::TrackId>,
    reconcile_data_track_ch: mpsc::Sender<livekit::TrackId>,
    close_ch: CancellationToken,
    done_ch: Arc<DoneCh>,

    // set when a queued reconcile is dropped, the worker then reconciles everything
    reconcile_overflow: atomic::Bool,
    reconcile_data_track_overflow: atomic::Bool,
}

// What `lock sync.RWMutex` guards upstream.
struct SubscriptionManagerInner {
    subscriptions: HashMap<livekit::TrackId, Arc<MediaTrackSubscription>>,

    subscribed_to: HashMap<livekit::ParticipantId, HashSet<livekit::TrackId>>,

    on_subscribe_status_changed: Option<OnSubscribeStatusChanged>,

    data_track_subscriptions: HashMap<livekit::TrackId, Arc<DataTrackSubscription>>,
}

impl SubscriptionManager {
    pub fn new(params: SubscriptionManagerParams) -> Arc<Self> {
        Self::new_with_reconcile_interval(params, RECONCILE_INTERVAL)
    }

    // The original's worker reads the variable `reconcileInterval` when it starts, and one
    // test changes it for the managers it makes.
    fn new_with_reconcile_interval(
        params: SubscriptionManagerParams,
        reconcile_interval: Duration,
    ) -> Arc<Self> {
        let (reconcile_ch, reconcile_rx) = mpsc::channel(50);
        let (reconcile_data_track_ch, reconcile_data_track_rx) = mpsc::channel(5);
        let m = Arc::new_cyclic(|this: &Weak<SubscriptionManager>| SubscriptionManager {
            params,
            this: this.clone(),
            lock: RwLock::new(SubscriptionManagerInner {
                subscriptions: HashMap::new(),
                subscribed_to: HashMap::new(),
                on_subscribe_status_changed: None,
                data_track_subscriptions: HashMap::new(),
            }),
            pending_unsubscribes: atomic::Int32::new(0),
            subscribed_video_count: atomic::Int32::new(0),
            subscribed_audio_count: atomic::Int32::new(0),
            reconcile_ch,
            reconcile_data_track_ch,
            close_ch: CancellationToken::new(),
            done_ch: Arc::new(DoneCh::default()),
            reconcile_overflow: atomic::Bool::new(false),
            reconcile_data_track_overflow: atomic::Bool::new(false),
        });

        time::spawn(Self::reconcile_worker(
            m.this.clone(),
            m.close_ch.clone(),
            reconcile_rx,
            reconcile_data_track_rx,
            CloseDoneCh(m.done_ch.clone()),
            reconcile_interval,
        ));
        m
    }

    pub fn close(&self, is_expected_to_resume: bool) {
        {
            let _m = self.lock.write();
            if self.is_closed() {
                return;
            }
            self.close_ch.cancel();
        }

        self.done_ch.wait();

        prometheus::record_track_subscribe_cancels(self.get_num_cancellations() as i32);

        // Remove observer closures from track change/remove notifiers to allow
        // this participant and its transports to be garbage collected.
        let (subs, data_track_subs) = {
            let m = self.lock.write();
            (m.subscriptions.clone(), m.data_track_subscriptions.clone())
        };
        for sub in subs.values() {
            sub.set_changed_notifier(None);
            sub.set_removed_notifier(None);
        }
        for data_track_sub in data_track_subs.values() {
            data_track_sub.set_changed_notifier(None);
            data_track_sub.set_removed_notifier(None);
        }

        let sub_tracks = self.get_subscribed_tracks();
        let mut down_tracks_to_close: Vec<Arc<sfu::DownTrack>> =
            Vec::with_capacity(sub_tracks.len());
        for st in &sub_tracks {
            self.set_desired(&st.id(), false);
            // nil check exists primarily for tests
            if let Some(dt) = st.down_track() {
                down_tracks_to_close.push(dt);
            }
        }

        if is_expected_to_resume {
            for dt in &down_tracks_to_close {
                dt.close_with_flush(false, true);
            }
        } else {
            // flush blocks, so execute in parallel
            for dt in down_tracks_to_close {
                time::spawn(async move {
                    dt.close_with_flush(true, true);
                });
            }
        }

        for (track_id, sub) in &data_track_subs {
            self.set_data_track_desired(track_id, false);

            let Some(data_down_track) = sub.get_data_down_track() else {
                // already unsubscribed
                continue;
            };

            let Some(data_track) = data_down_track.publish_data_track() else {
                continue;
            };

            data_track.remove_subscriber(&sub.subscriber_id);
        }
        self.notify_data_track_subscriber_handles();
    }

    fn is_closed(&self) -> bool {
        self.close_ch.is_cancelled()
    }

    pub fn subscribe_to_track(&self, track_id: &livekit::TrackId, is_sync: bool) {
        if self.is_closed() {
            return;
        }
        // held weakly, see the header: nobody is there to subscribe
        let Some(participant) = self.params.participant.upgrade() else {
            return;
        };

        if self.params.use_one_shot_signalling_mode || is_sync {
            let _ = self.subscribe_synchronous(&participant, track_id);
            return;
        }

        // find or create and set desired under one lock, so that a concurrent subscribe,
        // settings update or cleanup cannot replace or remove the subscription in between
        let (sub, desire_changed) = {
            let mut m = self.lock.write();
            let sub = match m.subscriptions.get(track_id) {
                Some(sub) => sub.clone(),
                None => {
                    let s_logger = self.params.logger.with_values(&[("trackID", track_id)]);
                    let sub =
                        MediaTrackSubscription::new(participant.id(), track_id.clone(), s_logger);
                    m.subscriptions.insert(track_id.clone(), sub.clone());
                    sub
                }
            };
            let desire_changed = sub.set_desired(true);
            (sub, desire_changed)
        };
        if desire_changed {
            debugw!(sub.logger, "subscribing to track");
        }

        // always reconcile, since SubscribeToTrack could be called when the track is ready
        self.queue_reconcile(track_id);
    }

    pub fn unsubscribe_from_track(&self, track_id: &livekit::TrackId) {
        if self.params.use_one_shot_signalling_mode {
            let _ = self.unsubscribe_synchronous(track_id);
            return;
        }

        let (sub, desire_changed) = self.set_desired(track_id, false);
        let Some(sub) = sub else {
            return;
        };
        if !desire_changed {
            return;
        }

        if sub.is_canceled() {
            prometheus::record_track_subscribe_cancels(1);
        }

        debugw!(sub.logger, "unsubscribing from track");
        self.queue_reconcile(track_id);
    }

    pub fn subscribe_to_data_track(&self, track_id: &livekit::TrackId) {
        if self.is_closed() {
            return;
        }
        // held weakly, see the header: nobody is there to subscribe
        let Some(participant) = self.params.participant.upgrade() else {
            return;
        };

        // same as SubscribeToTrack, find or create and set desired under one lock
        let (sub, desire_changed) = {
            let mut m = self.lock.write();
            let sub = match m.data_track_subscriptions.get(track_id) {
                Some(sub) => sub.clone(),
                None => {
                    let s_logger = self.params.logger.with_values(&[("trackID", track_id)]);
                    let sub =
                        DataTrackSubscription::new(participant.id(), track_id.clone(), s_logger);
                    m.data_track_subscriptions
                        .insert(track_id.clone(), sub.clone());
                    sub
                }
            };
            let desire_changed = sub.set_desired(true);
            (sub, desire_changed)
        };
        if desire_changed {
            debugw!(sub.logger, "subscribing to data track");
        }

        self.queue_reconcile_data_track(track_id);
    }

    pub fn unsubscribe_from_data_track(&self, track_id: &livekit::TrackId) {
        let (sub, desire_changed) = self.set_data_track_desired(track_id, false);
        let Some(sub) = sub else {
            return;
        };
        if !desire_changed {
            return;
        }

        debugw!(sub.logger, "unsubscribing from data track");
        self.queue_reconcile_data_track(track_id);
    }

    pub fn clear_all_subscriptions(&self) {
        debugw!(self.params.logger, "clearing all subscriptions");

        if self.params.use_one_shot_signalling_mode {
            for track in self.get_subscribed_tracks() {
                let _ = self.unsubscribe_synchronous(&track.id());
            }

            // no synchronous data tracks
        }

        let mut num_cancellations = 0;
        {
            let m = self.lock.read();
            for sub in m.subscriptions.values() {
                if sub.is_canceled() {
                    num_cancellations += 1;
                }
                sub.set_desired(false);
            }

            for sub in m.data_track_subscriptions.values() {
                sub.set_desired(false);
            }
        }
        prometheus::record_track_subscribe_cancels(num_cancellations);

        self.reconcile_all();
    }

    pub fn get_subscribed_tracks(&self) -> Vec<Arc<dyn SubscribedTrack>> {
        let m = self.lock.read();

        let mut tracks = Vec::with_capacity(m.subscriptions.len());
        for t in m.subscriptions.values() {
            if let Some(st) = t.get_subscribed_track() {
                tracks.push(st);
            }
        }
        tracks
    }

    pub fn get_subscribed_data_tracks(&self) -> Vec<Arc<dyn DataDownTrack>> {
        let m = self.lock.read();

        let mut tracks = Vec::with_capacity(m.data_track_subscriptions.len());
        for s in m.data_track_subscriptions.values() {
            if let Some(dt) = s.get_data_down_track() {
                tracks.push(dt);
            }
        }
        tracks
    }

    pub fn is_track_name_subscribed(
        &self,
        publisher_identity: &livekit::ParticipantIdentity,
        track_name: &str,
    ) -> bool {
        let m = self.lock.read();

        for s in m.subscriptions.values() {
            if let Some(st) = s.get_subscribed_track()
                && st.publisher_identity() == *publisher_identity
                && let Some(media_track) = st.media_track()
                && media_track.name() == track_name
            {
                return true;
            }
        }
        false
    }

    pub fn stop_and_get_subscribed_tracks_forwarder_state(
        &self,
    ) -> HashMap<livekit::TrackId, livekit::RtpForwarderState> {
        let m = self.lock.read();

        let mut states = HashMap::with_capacity(m.subscriptions.len());
        for (track_id, t) in &m.subscriptions {
            if let Some(st) = t.get_subscribed_track()
                && let Some(dt) = st.down_track()
            {
                let state = dt.stop_write_and_get_state();
                if let Some(forwarder_state) = state.forwarder_state {
                    states.insert(track_id.clone(), forwarder_state);
                }
            }
        }
        states
    }

    pub fn has_subscriptions(&self) -> bool {
        let m = self.lock.read();
        for s in m.subscriptions.values() {
            if s.is_desired() {
                return true;
            }
        }
        false
    }

    pub fn get_subscribed_participants(&self) -> Vec<livekit::ParticipantId> {
        let m = self.lock.read();

        m.subscribed_to.keys().cloned().collect()
    }

    pub fn is_subscribed_to(&self, participant_id: &livekit::ParticipantId) -> bool {
        let m = self.lock.read();

        m.subscribed_to.contains_key(participant_id)
    }

    pub fn update_subscribed_track_settings(
        &self,
        track_id: &livekit::TrackId,
        settings: &livekit::UpdateTrackSettings,
    ) {
        let sub = {
            let mut m = self.lock.write();
            let sub = match m.subscriptions.get(track_id) {
                Some(sub) => sub.clone(),
                None => {
                    // held weakly, see the header: nobody is there to keep settings for
                    let Some(participant) = self.params.participant.upgrade() else {
                        return;
                    };
                    let s_logger = self.params.logger.with_values(&[("trackID", track_id)]);
                    let sub =
                        MediaTrackSubscription::new(participant.id(), track_id.clone(), s_logger);
                    m.subscriptions.insert(track_id.clone(), sub.clone());
                    sub
                }
            };
            sub.keep_for_subscribe();
            sub
        };

        sub.set_settings(settings);
    }

    pub fn update_data_track_subscription_options(
        &self,
        track_id: &livekit::TrackId,
        subscription_options: Option<&livekit::DataTrackSubscriptionOptions>,
    ) {
        let sub = {
            let mut m = self.lock.write();
            let sub = match m.data_track_subscriptions.get(track_id) {
                Some(sub) => sub.clone(),
                None => {
                    // held weakly, see the header: nobody is there to keep options for
                    let Some(participant) = self.params.participant.upgrade() else {
                        return;
                    };
                    let s_logger = self.params.logger.with_values(&[("trackID", track_id)]);
                    let sub =
                        DataTrackSubscription::new(participant.id(), track_id.clone(), s_logger);
                    m.data_track_subscriptions
                        .insert(track_id.clone(), sub.clone());
                    sub
                }
            };
            sub.keep_for_subscribe();
            sub
        };

        sub.set_subscription_options(subscription_options);
    }

    // OnSubscribeStatusChanged callback will be notified when a participant subscribes or unsubscribes to another participant
    // it will only fire once per publisher. If current participant is subscribed to multiple tracks from another, this
    // callback will only fire once.
    #[allow(clippy::type_complexity)]
    pub fn on_subscribe_status_changed(
        &self,
        f: Box<dyn Fn(&livekit::ParticipantId, bool) + Send + Sync>,
    ) {
        let mut m = self.lock.write();
        m.on_subscribe_status_changed = Some(Arc::from(f));
    }

    pub fn wait_until_subscribed(&self, timeout: Duration) -> Result<(), Error> {
        let expires_at = time::now().add(timeout);
        while expires_at.after(time::now()) {
            let mut all_subscribed = true;
            {
                let m = self.lock.read();
                for sub in m.subscriptions.values() {
                    if sub.needs_subscribe() {
                        all_subscribed = false;
                        break;
                    }
                }
                if all_subscribed {
                    for sub in m.data_track_subscriptions.values() {
                        if sub.needs_subscribe() {
                            all_subscribed = false;
                            break;
                        }
                    }
                }
            }
            if all_subscribed {
                return Ok(());
            }
            blocking(|| std::thread::sleep((50 * MILLISECOND).to_std()));
        }

        Err(Error::DeadlineExceeded)
    }

    pub fn reconcile_all(&self) {
        let track_id = livekit::TrackId::from(TRACK_ID_FOR_RECONCILE_SUBSCRIPTIONS);
        self.queue_reconcile(&track_id);
        self.queue_reconcile_data_track(&track_id);
    }

    fn set_desired(
        &self,
        track_id: &livekit::TrackId,
        desired: bool,
    ) -> (Option<Arc<MediaTrackSubscription>>, bool) {
        let m = self.lock.read();

        let Some(sub) = m.subscriptions.get(track_id) else {
            return (None, false);
        };

        (Some(sub.clone()), sub.set_desired(desired))
    }

    fn set_data_track_desired(
        &self,
        track_id: &livekit::TrackId,
        desired: bool,
    ) -> (Option<Arc<DataTrackSubscription>>, bool) {
        let m = self.lock.read();

        let Some(sub) = m.data_track_subscriptions.get(track_id) else {
            return (None, false);
        };

        (Some(sub.clone()), sub.set_desired(desired))
    }

    fn can_reconcile(&self) -> bool {
        // held weakly, see the header: a participant that is gone is one that is closed
        let Some(p) = self.params.participant.upgrade() else {
            return false;
        };
        if self.is_closed() || p.is_closed() || p.is_disconnected() {
            return false;
        }
        true
    }

    fn reconcile_subscriptions(&self) {
        let mut needs_to_reconcile: Vec<Arc<MediaTrackSubscription>> = Vec::new();
        {
            let m = self.lock.read();
            for sub in m.subscriptions.values() {
                if sub.needs_subscribe()
                    || sub.needs_unsubscribe()
                    || sub.needs_bind()
                    || sub.needs_cleanup()
                {
                    needs_to_reconcile.push(sub.clone());
                }
            }
        }

        for s in &needs_to_reconcile {
            self.reconcile_subscription(s);
        }
    }

    fn reconcile_subscription(&self, s: &Arc<MediaTrackSubscription>) {
        if !self.can_reconcile() {
            return;
        }
        let Some(participant) = self.params.participant.upgrade() else {
            return;
        };
        if s.needs_subscribe() {
            if self.pending_unsubscribes.load() != 0
                && s.duration_since_start() < MAX_UNSUBSCRIBE_WAIT
            {
                // enqueue this in a bit, after pending unsubscribes are complete
                let this = self.this.clone();
                let track_id = s.track_id.clone();
                time::spawn(async move {
                    time::sleep(Duration(
                        (sfu::RTP_BLANK_FRAMES_CLOSE_SECONDS * SECOND.0 as f32) as i64,
                    ))
                    .await;
                    if let Some(m) = this.upgrade() {
                        m.queue_reconcile(&track_id);
                    }
                });
                return;
            }

            let num_attempts = s.get_num_attempts();
            if num_attempts == 0 {
                participant
                    .get_telemetry_listener()
                    .on_track_subscribe_requested(
                        &s.subscriber_id,
                        &livekit::TrackInfo {
                            sid: s.track_id.to_string(),
                            ..Default::default()
                        },
                    );
            }
            if let Err(err) = self.subscribe(&participant, s) {
                s.record_attempt(false);

                match err {
                    Error::NoTrackPermission
                    | Error::NoSubscribePermission
                    | Error::NoReceiver
                    | Error::NotOpen
                    | Error::SubscriptionLimitExceeded => {
                        // these are errors that are outside of our control, so we'll keep trying
                        // - ErrNoTrackPermission: publisher did not grant subscriber permission, may change any moment
                        // - ErrNoSubscribePermission: participant was not granted canSubscribe, may change any moment
                        // - ErrNoReceiver: Track is in the process of closing (another local track published to the same instance)
                        // - ErrNotOpen: Track is closing or already closed
                        // - ErrSubscriptionLimitExceeded: the participant have reached the limit of subscriptions, wait for the other subscription to be unsubscribed
                        // We'll still log an event to reflect this in telemetry since it's been too long
                        if s.duration_since_start() > SUBSCRIPTION_TIMEOUT {
                            s.maybe_record_error(participant.get_telemetry_listener(), &err, true);
                        }
                    }
                    Error::TrackNotFound => {
                        // source track was never published or closed
                        // if after timeout we'd unsubscribe from it.
                        // this is the *only* case we'd change desired state
                        if s.duration_since_start() > NOT_FOUND_TIMEOUT {
                            s.maybe_record_error(participant.get_telemetry_listener(), &err, true);
                            infow!(s.logger, "unsubscribing from track after notFoundTimeout", "error" => err);
                            s.set_desired(false);
                            self.queue_reconcile(&s.track_id);
                            (self.params.on_subscription_error)(&s.track_id, false, &err);
                        }
                    }
                    _ => {
                        // all other errors
                        if s.duration_since_start() > SUBSCRIPTION_TIMEOUT {
                            warnw!(
                                s.logger,
                                "failed to subscribe, triggering error handler", err,
                                "attempt" => s.get_num_attempts(),
                            );
                            s.maybe_record_error(participant.get_telemetry_listener(), &err, false);
                            (self.params.on_subscription_error)(&s.track_id, true, &err);
                        } else {
                            debugw!(
                                s.logger,
                                "failed to subscribe, retrying",
                                "error" => err,
                                "attempt" => s.get_num_attempts(),
                            );
                        }
                    }
                }
            } else {
                s.record_attempt(true);
            }

            return;
        }

        if s.needs_unsubscribe() {
            if let Err(err) = self.unsubscribe(s) {
                warnw!(s.logger, "failed to unsubscribe", err);
            }
            // do not remove subscription from map. Wait for subscribed track to close
            // and the callback (handleSubscribedTrackClose) to set the subscribedTrack to nil
            // and the clean up path to handle removing subscription from the subscription map.
            // It is possible that the track is re-published before subscribed track is closed.
            // That could create a new subscription and a duplicate entry in SDP.
            // Waiting for subscribed track close would ensure that the track is removed from
            // the peer connection before re-published track is re-subscribed and added back to the SDP.
            return;
        }

        if s.needs_bind() {
            // check bound status, notify error callback if it's not bound
            // if a publisher leaves or closes the source track, SubscribedTrack will be closed as well and it will go
            // back to needsSubscribe state
            let active_at = participant.active_at();
            if !active_at.is_zero() {
                let wait = time::since(active_at).min(s.duration_since_start());
                if wait > SUBSCRIPTION_TIMEOUT {
                    warnw!(s.logger, "track not bound after timeout", nil);
                    s.maybe_record_error(
                        participant.get_telemetry_listener(),
                        &Error::TrackNotBound,
                        false,
                    );
                    (self.params.on_subscription_error)(&s.track_id, true, &Error::TrackNotBound);
                }
            }
        }

        let mut m = self.lock.write();
        if m.subscriptions
            .get(&s.track_id)
            .is_some_and(|sub| Arc::ptr_eq(sub, s))
            && s.needs_cleanup()
        {
            debugw!(s.logger, "cleanup removing subscription");
            m.subscriptions.remove(&s.track_id);
        }
    }

    fn reconcile_data_track_subscriptions(&self) {
        let mut needs_to_reconcile: Vec<Arc<DataTrackSubscription>> = Vec::new();
        {
            let m = self.lock.read();
            for sub in m.data_track_subscriptions.values() {
                if sub.needs_subscribe() || sub.needs_unsubscribe() || sub.needs_cleanup() {
                    needs_to_reconcile.push(sub.clone());
                }
            }
        }

        for s in &needs_to_reconcile {
            self.reconcile_data_track_subscription(s);
        }
    }

    fn reconcile_data_track_subscription(&self, s: &Arc<DataTrackSubscription>) {
        if !self.can_reconcile() {
            return;
        }
        let Some(participant) = self.params.participant.upgrade() else {
            return;
        };
        if s.needs_subscribe() {
            if let Err(err) = self.subscribe_data_track(&participant, s) {
                s.record_attempt(false);

                match err {
                    Error::NoTrackPermission | Error::NoSubscribePermission => {
                        // these are errors that are outside of our control, so we'll keep trying
                        // - ErrNoTrackPermission: publisher did not grant subscriber permission, may change any moment
                        // - ErrNoSubscribePermission: participant was not granted canSubscribe, may change any moment
                    }
                    Error::TrackNotFound => {
                        // source track was never published or closed
                        // if after timeout we'd unsubscribe from it.
                        // this is the *only* case we'd change desired state
                        if s.duration_since_start() > NOT_FOUND_TIMEOUT {
                            infow!(s.logger, "unsubscribing from data track after notFoundTimeout", "error" => err);
                            s.set_desired(false);
                            self.queue_reconcile_data_track(&s.track_id);
                        }
                    }
                    _ => {
                        // all other errors
                        if s.duration_since_start() > SUBSCRIPTION_TIMEOUT {
                            warnw!(
                                s.logger,
                                "failed to subscribe, triggering error handler", err,
                                "attempt" => s.get_num_attempts(),
                            );
                        } else {
                            debugw!(
                                s.logger,
                                "failed to subscribe, retrying",
                                "error" => err,
                                "attempt" => s.get_num_attempts(),
                            );
                        }
                    }
                }
            } else {
                s.record_attempt(true);
                self.notify_data_track_subscriber_handles();
            }

            return;
        }

        if s.needs_unsubscribe() {
            if let Err(err) = self.unsubscribe_data_track(s) {
                warnw!(s.logger, "failed to unsubscribe", err);
            }

            // a subscribe during the removal sets the entry as desired again, keep it then
            let removed = {
                let mut m = self.lock.write();
                let removed = m
                    .data_track_subscriptions
                    .get(&s.track_id)
                    .is_some_and(|sub| Arc::ptr_eq(sub, s))
                    && !s.is_desired();
                if removed {
                    m.data_track_subscriptions.remove(&s.track_id);
                }
                removed
            };
            self.notify_data_track_subscriber_handles();
            if !removed {
                self.queue_reconcile_data_track(&s.track_id);
            }
            return;
        }

        let cleaned_up = {
            let mut m = self.lock.write();
            let cleaned_up = m
                .data_track_subscriptions
                .get(&s.track_id)
                .is_some_and(|sub| Arc::ptr_eq(sub, s))
                && s.needs_cleanup();
            if cleaned_up {
                debugw!(s.logger, "cleanup removing data track subscription");
                m.data_track_subscriptions.remove(&s.track_id);
            }
            cleaned_up
        };

        if cleaned_up {
            self.notify_data_track_subscriber_handles();
        }
    }

    // trigger an immediate reconciliation, when trackID is empty, will reconcile all subscriptions
    fn queue_reconcile(&self, track_id: &livekit::TrackId) {
        if self.reconcile_ch.try_send(track_id.clone()).is_err() {
            // queue is full, the worker reconciles everything after its next queued item
            self.reconcile_overflow.store(true);
        }
    }

    fn queue_reconcile_data_track(&self, track_id: &livekit::TrackId) {
        if self
            .reconcile_data_track_ch
            .try_send(track_id.clone())
            .is_err()
        {
            self.reconcile_data_track_overflow.store(true);
        }
    }

    // The worker holds the manager weakly and looks it up for every pass: it ends when the
    // manager is closed and when it is gone (the queues then have no sending half left).
    async fn reconcile_worker(
        this: Weak<SubscriptionManager>,
        close_ch: CancellationToken,
        mut reconcile_ch: mpsc::Receiver<livekit::TrackId>,
        mut reconcile_data_track_ch: mpsc::Receiver<livekit::TrackId>,
        done_ch: CloseDoneCh,
        reconcile_interval: Duration,
    ) {
        // defer close(m.doneCh)
        let _done_ch = done_ch;

        let mut reconcile_ticker = new_ticker(reconcile_interval);

        loop {
            tokio::select! {
                _ = close_ch.cancelled() => {
                    return;
                }

                _ = reconcile_ticker.tick() => {
                    let Some(m) = this.upgrade() else {
                        return;
                    };
                    m.reconcile_subscriptions();
                    m.reconcile_data_track_subscriptions();
                }

                track_id = reconcile_ch.recv() => {
                    let (Some(track_id), Some(m)) = (track_id, this.upgrade()) else {
                        return;
                    };
                    let s = m.lock.write().subscriptions.get(&track_id).cloned();
                    if let Some(s) = s {
                        m.reconcile_subscription(&s);
                    } else {
                        m.reconcile_subscriptions();
                    }
                    if m.reconcile_overflow.swap(false) {
                        m.reconcile_subscriptions();
                    }
                }

                track_id = reconcile_data_track_ch.recv() => {
                    let (Some(track_id), Some(m)) = (track_id, this.upgrade()) else {
                        return;
                    };
                    let s = m.lock.write().data_track_subscriptions.get(&track_id).cloned();
                    if let Some(s) = s {
                        m.reconcile_data_track_subscription(&s);
                    } else {
                        m.reconcile_data_track_subscriptions();
                    }
                    if m.reconcile_data_track_overflow.swap(false) {
                        m.reconcile_data_track_subscriptions();
                    }
                }
            }
        }
    }

    #[allow(clippy::collapsible_match)] // the shape of the original's switch
    fn has_capacity_for_subscription(&self, kind: livekit::TrackType) -> bool {
        match kind {
            livekit::TrackType::Video => {
                if self.params.subscription_limit_video > 0
                    && self.subscribed_video_count.load() >= self.params.subscription_limit_video
                {
                    debugw!(self.params.logger, "subscription limit exceeded for video", "limit" => self.params.subscription_limit_video, "subscriptions" => self.subscribed_video_count.load());
                    return false;
                }
            }

            livekit::TrackType::Audio => {
                if self.params.subscription_limit_audio > 0
                    && self.subscribed_audio_count.load() >= self.params.subscription_limit_audio
                {
                    debugw!(self.params.logger, "subscription limit exceeded for audio", "limit" => self.params.subscription_limit_audio, "subscriptions" => self.subscribed_audio_count.load());
                    return false;
                }
            }

            _ => {}
        }
        true
    }

    fn subscribe(
        &self,
        participant: &Arc<dyn LocalParticipant>,
        sub: &Arc<MediaTrackSubscription>,
    ) -> Result<(), Error> {
        debugw!(sub.logger, "executing subscribe");

        if !participant.can_subscribe() {
            return Err(Error::NoSubscribePermission);
        }

        if let (kind, true) = sub.get_kind()
            && !self.has_capacity_for_subscription(kind)
        {
            return Err(Error::SubscriptionLimitExceeded);
        }

        let track_id = &sub.track_id;
        let res = (self.params.track_resolver)(participant.clone(), track_id);
        debugw!(sub.logger, "resolved track", "result" => MediaResolverResultLog(&res));

        if let Some(notifier) = &res.track_changed_notifier
            && sub.set_changed_notifier(Some(notifier.clone()))
        {
            // set callback only when we haven't done it before
            // we set the observer before checking for existence of track, so that we may get notified
            // when the track becomes available
            let this = self.this.clone();
            let track_id = track_id.clone();
            notifier.add_observer(
                sub.subscriber_id.as_str(),
                Box::new(move || {
                    if let Some(m) = this.upgrade() {
                        m.queue_reconcile(&track_id);
                    }
                }),
            );
        }
        if let Some(notifier) = &res.track_removed_notifier
            && sub.set_removed_notifier(Some(notifier.clone()))
        {
            let this = self.this.clone();
            let track_id = track_id.clone();
            notifier.add_observer(
                sub.subscriber_id.as_str(),
                Box::new(move || {
                    let Some(m) = this.upgrade() else {
                        return;
                    };
                    let Some(participant) = m.params.participant.upgrade() else {
                        return;
                    };
                    // re-resolve the track in case the same track had been re-published
                    let res = (m.params.track_resolver)(participant, &track_id);
                    if res.track.is_some() {
                        // do not unsubscribe, track is still available
                        return;
                    }
                    m.handle_source_track_removed(&track_id);
                }),
            );
        }

        let Some(track) = res.track else {
            return Err(Error::TrackNotFound);
        };
        sub.try_set_kind(track.kind());
        if !self.has_capacity_for_subscription(track.kind()) {
            return Err(Error::SubscriptionLimitExceeded);
        }

        sub.set_publisher(&res.publisher_identity, &res.publisher_id);

        let perm_changed = sub.set_has_permission(res.has_permission);
        if perm_changed {
            let _ = participant.send_subscription_permission_update(
                &sub.get_publisher_id(),
                track_id,
                res.has_permission,
            );
        }
        if !res.has_permission {
            return Err(Error::NoTrackPermission);
        }

        self.add_subscriber(participant, sub, track, false)?;

        self.mark_subscribed_to(&sub.get_publisher_id(), track_id);
        Ok(())
    }

    fn subscribe_synchronous(
        &self,
        participant: &Arc<dyn LocalParticipant>,
        track_id: &livekit::TrackId,
    ) -> Result<(), Error> {
        debugw!(self.params.logger, "executing subscribe synchronous", "trackID" => track_id);

        if !participant.can_subscribe() {
            return Err(Error::NoSubscribePermission);
        }

        let res = (self.params.track_resolver)(participant.clone(), track_id);
        debugw!(self.params.logger, "resolved track", "trackID" => track_id, " result" => MediaResolverResultLog(&res));

        let Some(track) = res.track else {
            return Err(Error::TrackNotFound);
        };

        let sub = {
            let mut m = self.lock.write();
            match m.subscriptions.get(track_id) {
                Some(sub) => sub.clone(),
                None => {
                    let s_logger = self.params.logger.with_values(&[("trackID", track_id)]);
                    let sub =
                        MediaTrackSubscription::new(participant.id(), track_id.clone(), s_logger);
                    m.subscriptions.insert(track_id.clone(), sub.clone());
                    sub
                }
            }
        };
        sub.set_desired(true);

        self.add_subscriber(participant, &sub, track, true)
    }

    fn add_subscriber(
        &self,
        participant: &Arc<dyn LocalParticipant>,
        sub: &Arc<MediaTrackSubscription>,
        track: Arc<dyn MediaTrack>,
        is_sync: bool,
    ) -> Result<(), Error> {
        let track_id = track.id();
        // take the shadow token before setup, so tokens go out in arrival order
        let mut pace_shadow_delays: Option<HashMap<String, Duration>> = None;
        if track.kind() == livekit::TrackType::Video && !is_sync {
            pace_shadow_delays = self
                .params
                .subscribe_pacer
                .as_ref()
                .map(|pacer| pacer.delays(time::now()));
        }
        let sub_track = match track.add_subscriber(participant.clone()) {
            Ok(sub_track) => sub_track,
            Err(Error::AlreadySubscribed) => {
                debugw!(
                    sub.logger,
                    "already subscribed to track",
                    "subscribedAudioCount" => self.subscribed_audio_count.load(),
                    "subscribedVideoCount" => self.subscribed_video_count.load(),
                );
                None
            }
            Err(err) => {
                // ignore error(s): already subscribed
                if !utils::error_is_one_of(&err, &[Error::NoReceiver]) {
                    // as track resolution could take some time, not logging errors due to waiting for track resolution
                    warnw!(self.params.logger, "add subscriber failed", err, "trackID" => track_id);
                }
                return Err(err);
            }
        };
        // subTrack could be nil if already subscribed
        if let Some(sub_track) = sub_track {
            if let Some(pace_shadow_delays) = &pace_shadow_delays {
                prometheus::record_subscribe_pace_shadow(pace_shadow_delays);
            }
            {
                let sub = Arc::downgrade(sub);
                sub_track.on_subscribe_stream_started(Box::new(move |elapsed| {
                    if let Some(sub) = sub.upgrade() {
                        sub.record_stream_start_latency(elapsed);
                    }
                }));
            }
            if let Some(m) = self.this.upgrade() {
                let sub = sub.clone();
                let track_id = track_id.clone();
                // held until the track has closed, see the header
                let participant = participant.clone();
                sub_track.on_close(Some(Box::new(move |is_expected_to_resume| {
                    let _participant = &participant;
                    m.handle_subscribed_track_close(&sub, is_expected_to_resume);

                    if is_sync {
                        m.lock.write().subscriptions.remove(&track_id);
                    }
                })));
            }
            {
                let this = self.this.clone();
                let sub = Arc::downgrade(sub);
                let track_id = track_id.clone();
                sub_track.add_on_bind(Box::new(move |res| {
                    let (Some(m), Some(sub)) = (this.upgrade(), sub.upgrade()) else {
                        return;
                    };
                    let Some(participant) = m.params.participant.upgrade() else {
                        return;
                    };
                    if let Err(err) = res {
                        infow!(sub.logger, "failed to bind track", "err" => err);
                        sub.maybe_record_error(participant.get_telemetry_listener(), &err, true);
                        m.unsubscribe_from_track(&track_id);
                        (m.params.on_subscription_error)(&track_id, false, &err);
                        return;
                    }
                    sub.set_bound();
                    sub.maybe_record_success(participant.get_telemetry_listener());
                }));
            }
            sub.set_subscribed_track(Some(sub_track.clone()));

            match track.kind() {
                livekit::TrackType::Video => {
                    self.subscribed_video_count.inc();
                }
                livekit::TrackType::Audio => {
                    self.subscribed_audio_count.inc();
                }
                _ => {}
            }

            // a canSubscribe revoke only removes recorded subscriptions, so one that landed while
            // this subscribe was in flight missed it; now that it is recorded, check again
            if !participant.can_subscribe() {
                track.remove_subscriber(&participant.id(), false);
                return Err(Error::NoSubscribePermission);
            }

            if !is_sync && sub_track.needs_negotiation() {
                participant.negotiate(false);
            }

            if let Some(m) = self.this.upgrade() {
                let sub_track = sub_track.clone();
                time::spawn(async move {
                    (m.params.on_track_subscribed)(sub_track);
                });
            }

            debugw!(
                sub.logger,
                "subscribed to track",
                "subscribedAudioCount" => self.subscribed_audio_count.load(),
                "subscribedVideoCount" => self.subscribed_video_count.load(),
            );
        }
        Ok(())
    }

    fn unsubscribe(&self, s: &Arc<MediaTrackSubscription>) -> Result<(), Error> {
        debugw!(s.logger, "executing unsubscribe");

        let Some(sub_track) = s.get_subscribed_track() else {
            // already unsubscribed
            return Ok(());
        };

        let track = sub_track.media_track();
        let p_id = s.subscriber_id.clone();
        self.pending_unsubscribes.inc();
        let pending_unsubscribes = PendingUnsubscribesDec(self.this.clone());
        time::spawn(async move {
            // defer m.pendingUnsubscribes.Dec()
            let _pending_unsubscribes = pending_unsubscribes;
            if let Some(track) = track {
                track.remove_subscriber(&p_id, false);
            }
        });

        Ok(())
    }

    fn unsubscribe_synchronous(&self, track_id: &livekit::TrackId) -> Result<(), Error> {
        let sub = self.lock.write().subscriptions.remove(track_id);
        let Some(sub) = sub else {
            // already unsubscribed or not subscribed
            return Ok(());
        };

        debugw!(sub.logger, "executing unsubscribe synchronous");

        if sub.is_canceled() {
            prometheus::record_track_subscribe_cancels(1);
        }

        let Some(sub_track) = sub.get_subscribed_track() else {
            // already unsubscribed
            return Ok(());
        };

        if let Some(track) = sub_track.media_track() {
            track.remove_subscriber(&sub.subscriber_id, false);
        }
        Ok(())
    }

    fn handle_source_track_removed(&self, track_id: &livekit::TrackId) {
        let sub = self.lock.write().subscriptions.get(track_id).cloned();

        if let Some(sub) = sub {
            if sub.is_canceled() {
                prometheus::record_track_subscribe_cancels(1);
            }
            sub.handle_source_track_removed();
        }
    }

    // DownTrack closing is how the publisher signifies that the subscription is no longer fulfilled
    // this could be due to a few reasons:
    // - subscriber-initiated unsubscribe
    // - UpTrack was closed
    // - publisher revoked permissions for the participant
    fn handle_subscribed_track_close(
        &self,
        s: &Arc<MediaTrackSubscription>,
        is_expected_to_resume: bool,
    ) {
        debugw!(
            s.logger,
            "subscribed track closed",
            "isExpectedToResume" => is_expected_to_resume,
        );
        let was_bound = s.is_bound();
        let Some(sub_track) = s.get_subscribed_track() else {
            return;
        };
        s.set_subscribed_track(None);
        s.set_changed_notifier(None);
        s.set_removed_notifier(None);

        let kind = sub_track.media_track().map(|track| track.kind());
        let mut relieve_from_limits = false;
        match kind {
            Some(livekit::TrackType::Video) => {
                let video_count = self.subscribed_video_count.dec();
                relieve_from_limits = self.params.subscription_limit_video > 0
                    && video_count == self.params.subscription_limit_video - 1;
            }
            Some(livekit::TrackType::Audio) => {
                let audio_count = self.subscribed_audio_count.dec();
                relieve_from_limits = self.params.subscription_limit_audio > 0
                    && audio_count == self.params.subscription_limit_audio - 1;
            }
            _ => {}
        }

        self.unmark_subscribed_to(&s.get_publisher_id(), &s.track_id);

        if let Some(m) = self.this.upgrade() {
            let sub_track = sub_track.clone();
            time::spawn(async move {
                (m.params.on_track_unsubscribed)(sub_track);
            });
        }

        // held weakly, see the header; the callback that leads here holds it until now
        let participant = self.params.participant.upgrade();

        // trigger to decrement unsubscribed counter as long as track has been bound
        // Only log an analytics event when
        // * the participant isn't closing
        // * it's not a migration
        if was_bound && let Some(participant) = &participant {
            participant.get_telemetry_listener().on_track_unsubscribed(
                &s.subscriber_id,
                &livekit::TrackInfo {
                    sid: s.track_id.to_string(),
                    r#type: kind.unwrap_or_default() as i32,
                    ..Default::default()
                },
                !is_expected_to_resume,
            );

            if let Some(dt) = sub_track.down_track()
                && let Some(stats) = dt.get_track_stats()
            {
                participant
                    .get_telemetry_listener()
                    .on_track_subscribe_rtp_stats(&s.subscriber_id, &s.track_id, dt.mime(), &stats);
            }
        }

        if !is_expected_to_resume {
            if let Some(participant) = &participant {
                if let Some(sender) = sub_track.rtp_sender() {
                    debugw!(s.logger, "removing PeerConnection track",
                        "kind" => kind,
                    );

                    if let Err(err) = participant.remove_track_local(sender)
                        && err != Error::Webrtc(webrtc::Error::ConnectionClosed)
                    {
                        // most of these are safe to ignore, since the track state might have already
                        // been set to Inactive
                        debugw!(self.params.logger, "could not remove remoteTrack from forwarder",
                            "error" => err,
                            "publisher" => sub_track.publisher_identity(),
                            "publisherID" => sub_track.publisher_id(),
                        );
                    }
                }

                participant.negotiate(false);
            }
        } else {
            s.subscribe_at.store(time::now());
        }
        if !self.params.use_one_shot_signalling_mode {
            if relieve_from_limits {
                self.queue_reconcile(&livekit::TrackId::from(
                    TRACK_ID_FOR_RECONCILE_SUBSCRIPTIONS,
                ));
            } else {
                self.queue_reconcile(&s.track_id);
            }
        }
    }

    fn subscribe_data_track(
        &self,
        participant: &Arc<dyn LocalParticipant>,
        sub: &Arc<DataTrackSubscription>,
    ) -> Result<(), Error> {
        debugw!(sub.logger, "executing subscribe");

        if !participant.can_subscribe() {
            return Err(Error::NoSubscribePermission);
        }

        let track_id = &sub.track_id;
        let res = (self.params.data_track_resolver)(participant.clone(), track_id);
        debugw!(sub.logger, "resolved data track", "result" => DataResolverResultLog(&res));

        if let Some(notifier) = &res.track_changed_notifier
            && sub.set_changed_notifier(Some(notifier.clone()))
        {
            // set callback only when we haven't done it before
            // we set the observer before checking for existence of track, so that we may get notified
            // when the track becomes available
            let this = self.this.clone();
            let track_id = track_id.clone();
            notifier.add_observer(
                sub.subscriber_id.as_str(),
                Box::new(move || {
                    if let Some(m) = this.upgrade() {
                        m.queue_reconcile_data_track(&track_id);
                    }
                }),
            );
        }
        if let Some(notifier) = &res.track_removed_notifier
            && sub.set_removed_notifier(Some(notifier.clone()))
        {
            let this = self.this.clone();
            let track_id = track_id.clone();
            notifier.add_observer(
                sub.subscriber_id.as_str(),
                Box::new(move || {
                    let Some(m) = this.upgrade() else {
                        return;
                    };
                    let Some(participant) = m.params.participant.upgrade() else {
                        return;
                    };
                    // re-resolve the track in case the same track had been re-published
                    let res = (m.params.data_track_resolver)(participant, &track_id);
                    if res.data_track.is_some() {
                        // do not unsubscribe, track is still available
                        return;
                    }
                    m.handle_source_data_track_removed(&track_id);
                }),
            );
        }

        let Some(data_track) = res.data_track else {
            return Err(Error::TrackNotFound);
        };

        sub.set_publisher(&res.publisher_identity, &res.publisher_id);

        let perm_changed = sub.set_has_permission(res.has_permission);
        if perm_changed {
            let _ = participant.send_subscription_permission_update(
                &sub.get_publisher_id(),
                track_id,
                res.has_permission,
            );
        }
        if !res.has_permission {
            return Err(Error::NoTrackPermission);
        }

        let data_down_track = match data_track.add_subscriber(participant.clone()) {
            Ok(data_down_track) => data_down_track,
            Err(Error::AlreadySubscribed) => {
                debugw!(sub.logger, "already subscribed to data track");
                None
            }
            Err(err) => {
                return Err(err);
            }
        };
        // subTrack could be nil if already subscribed
        if let Some(data_down_track) = data_down_track {
            if let Some(m) = self.this.upgrade() {
                let sub = sub.clone();
                // held until the track has closed, see the header
                let participant = participant.clone();
                data_down_track.on_close(Some(Box::new(move || {
                    let _participant = &participant;
                    m.handle_data_down_track_close(&sub);
                })));
            }
            sub.set_data_down_track(Some(data_down_track));

            // a canSubscribe revoke only removes recorded subscriptions, so one that landed while
            // this subscribe was in flight missed it; now that it is recorded, check again
            if !participant.can_subscribe() {
                data_track.remove_subscriber(&participant.id());
                return Err(Error::NoSubscribePermission);
            }
            debugw!(sub.logger, "subscribed to data track");
        }

        self.mark_subscribed_to(&sub.get_publisher_id(), track_id);
        Ok(())
    }

    fn unsubscribe_data_track(&self, s: &Arc<DataTrackSubscription>) -> Result<(), Error> {
        debugw!(s.logger, "executing unsubscribe");

        let Some(data_down_track) = s.get_data_down_track() else {
            // already unsubscribed
            return Ok(());
        };

        if let Some(data_track) = data_down_track.publish_data_track() {
            data_track.remove_subscriber(&s.subscriber_id);
        }

        Ok(())
    }

    // DataDownTrack closing is how the publisher signifies that the subscription is no longer fulfilled
    // this could be due to a few reasons:
    // - subscriber-initiated unsubscribe
    // - data track was unpublished
    // - publisher revoked permissions for the participant
    fn handle_data_down_track_close(&self, s: &Arc<DataTrackSubscription>) {
        debugw!(s.logger, "data down track closed");

        if s.get_data_down_track().is_none() {
            return;
        }
        s.set_data_down_track(None);
        s.set_changed_notifier(None);
        s.set_removed_notifier(None);

        self.unmark_subscribed_to(&s.get_publisher_id(), &s.track_id);
        self.notify_data_track_subscriber_handles();
        self.queue_reconcile_data_track(&s.track_id);
    }

    fn notify_data_track_subscriber_handles(&self) {
        let handles = {
            let m = self.lock.write();
            let mut handles = HashMap::with_capacity(m.data_track_subscriptions.len());
            for sub in m.data_track_subscriptions.values() {
                if !sub.is_desired() {
                    continue;
                }
                let Some(data_down_track) = sub.get_data_down_track() else {
                    continue;
                };
                let handle = data_down_track.handle() as u32;
                let s = sub.lock.read();
                handles.insert(
                    handle,
                    livekit::data_track_subscriber_handles::PublishedDataTrack {
                        publisher_identity: s.publisher_identity.to_string(),
                        publisher_sid: s.publisher_id.to_string(),
                        track_sid: sub.track_id.to_string(),
                    },
                );
            }
            handles
        };

        // held weakly, see the header: nobody is there to be told
        if let Some(participant) = self.params.participant.upgrade() {
            let _ = participant.send_data_track_subscriber_handles(&handles);
        }
    }

    fn handle_source_data_track_removed(&self, track_id: &livekit::TrackId) {
        let sub = self
            .lock
            .write()
            .data_track_subscriptions
            .get(track_id)
            .cloned();

        if let Some(sub) = sub {
            sub.handle_source_track_removed();
        }
    }

    fn mark_subscribed_to(
        &self,
        publisher_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
    ) {
        // add mark the participant as someone we've subscribed to
        let mut first_subscribe = false;
        let changed_cb = {
            let mut m = self.lock.write();
            let changed_cb = m.on_subscribe_status_changed.clone();
            if !m.subscribed_to.contains_key(publisher_id) {
                m.subscribed_to.insert(publisher_id.clone(), HashSet::new());
                first_subscribe = true;
            }
            if let Some(p_tracks) = m.subscribed_to.get_mut(publisher_id) {
                p_tracks.insert(track_id.clone());
            }
            changed_cb
        };

        if let Some(changed_cb) = changed_cb
            && first_subscribe
        {
            changed_cb(publisher_id, true);
        }
    }

    fn unmark_subscribed_to(
        &self,
        publisher_id: &livekit::ParticipantId,
        track_id: &livekit::TrackId,
    ) {
        // remove from subscribedTo
        let mut last_subscription = false;
        let changed_cb = {
            let mut m = self.lock.write();
            let changed_cb = m.on_subscribe_status_changed.clone();
            if let Some(p_tracks) = m.subscribed_to.get_mut(publisher_id) {
                p_tracks.remove(track_id);
                if p_tracks.is_empty() {
                    m.subscribed_to.remove(publisher_id);
                    last_subscription = true;
                }
            }
            changed_cb
        };
        if let Some(changed_cb) = changed_cb
            && last_subscription
        {
            changed_cb(publisher_id, false);
        }
    }

    fn get_num_cancellations(&self) -> usize {
        let m = self.lock.read();

        let mut num_cancellations = 0;
        for sub in m.subscriptions.values() {
            if sub.is_canceled() {
                num_cancellations += 1;
            }
        }
        num_cancellations
    }
}

// --------------------------------------------------------------------------------------

// `T` is what the lock guards of the struct this one is embedded in.
#[derive(Default)]
struct TrackSubscription<T> {
    subscriber_id: livekit::ParticipantId,
    track_id: livekit::TrackId,
    logger: Logger,

    lock: RwLock<TrackSubscriptionInner<T>>,

    num_attempts: atomic::Int32,

    // the later of when subscription was requested OR when the first failure was encountered OR when permission is granted
    // this timestamp determines when failures are reported
    sub_started_at: atomic::Time,

    // the timestamp when the subscription was started, will be reset when downtrack is closed with expected resume
    subscribe_at: atomic::Time,
}

// What `lock sync.RWMutex` guards upstream.
#[derive(Default)]
struct TrackSubscriptionInner<T> {
    desired: bool,
    publisher_id: livekit::ParticipantId,
    publisher_identity: livekit::ParticipantIdentity,
    changed_notifier: Option<Arc<dyn ChangeNotifier>>,
    removed_notifier: Option<Arc<dyn ChangeNotifier>>,
    has_permission_initialized: bool,
    has_permission: bool,

    // an entry that is not desired is not cleaned up before this time,
    // so that settings sent before a subscribe are there when the subscribe comes
    keep_until: Time,

    // the fields of the embedding struct
    outer: T,
}

impl<T> TrackSubscription<T> {
    // set permission and return true if it has changed
    fn set_has_permission(&self, perm: bool) -> bool {
        let mut s = self.lock.write();
        if s.has_permission_initialized && s.has_permission == perm {
            return false;
        }

        s.has_permission_initialized = true;
        s.has_permission = perm;
        if s.has_permission {
            // when permission is granted, reset the timer so it has sufficient time to reconcile
            let t = time::now();
            self.sub_started_at.store(t);
            self.subscribe_at.store(t);
        }
        true
    }

    #[allow(dead_code)] // called by the tests only upstream too
    fn get_has_permission(&self) -> bool {
        let s = self.lock.read();
        s.has_permission
    }

    fn set_publisher(
        &self,
        publisher_identity: &livekit::ParticipantIdentity,
        publisher_id: &livekit::ParticipantId,
    ) {
        let mut s = self.lock.write();

        s.publisher_id = publisher_id.clone();
        s.publisher_identity = publisher_identity.clone();
    }

    fn get_publisher_id(&self) -> livekit::ParticipantId {
        let s = self.lock.read();
        s.publisher_id.clone()
    }

    fn set_desired(&self, desired: bool) -> bool {
        let mut s = self.lock.write();

        if desired {
            // as long as user explicitly set it to desired
            // we'll reset the timer so it has sufficient time to reconcile
            let t = time::now();
            self.sub_started_at.store(t);
            self.subscribe_at.store(t);
            s.keep_until = Time::default();
        }

        if s.desired == desired {
            return false;
        }
        s.desired = desired;

        // when no longer desired, we no longer care about change notifications
        if desired {
            // reset attempts
            self.num_attempts.store(0);
        } else {
            self.set_changed_notifier_locked(&mut s, None);
            self.set_removed_notifier_locked(&mut s, None);
        }
        true
    }

    fn is_desired(&self) -> bool {
        let s = self.lock.read();
        s.desired
    }

    fn record_attempt(&self, success: bool) {
        if !success {
            if self.num_attempts.load() == 0 {
                // on first failure, we'd want to start the timer
                let t = time::now();
                self.sub_started_at.store(t);
            }
            self.num_attempts.add(1);
        } else {
            self.num_attempts.store(0);
        }
    }

    fn get_num_attempts(&self) -> i32 {
        self.num_attempts.load()
    }

    fn keep_for_subscribe(&self) {
        let mut s = self.lock.write();
        if !s.desired {
            s.keep_until = time::now().add(NOT_FOUND_TIMEOUT);
        }
    }

    fn duration_since_start(&self) -> Duration {
        let t = self.sub_started_at.load();
        if t.is_zero() {
            return Duration::ZERO;
        }
        time::since(t)
    }

    fn set_changed_notifier(&self, notifier: Option<Arc<dyn ChangeNotifier>>) -> bool {
        let mut s = self.lock.write();
        self.set_changed_notifier_locked(&mut s, notifier)
    }

    fn set_changed_notifier_locked(
        &self,
        s: &mut TrackSubscriptionInner<T>,
        notifier: Option<Arc<dyn ChangeNotifier>>,
    ) -> bool {
        if same_notifier(&s.changed_notifier, &notifier) {
            return false;
        }

        let existing = std::mem::replace(&mut s.changed_notifier, notifier);

        if let Some(existing) = existing {
            let subscriber_id = self.subscriber_id.clone();
            time::spawn(async move {
                existing.remove_observer(subscriber_id.as_str());
            });
        }
        true
    }

    fn set_removed_notifier(&self, notifier: Option<Arc<dyn ChangeNotifier>>) -> bool {
        let mut s = self.lock.write();
        self.set_removed_notifier_locked(&mut s, notifier)
    }

    fn set_removed_notifier_locked(
        &self,
        s: &mut TrackSubscriptionInner<T>,
        notifier: Option<Arc<dyn ChangeNotifier>>,
    ) -> bool {
        if same_notifier(&s.removed_notifier, &notifier) {
            return false;
        }

        let existing = std::mem::replace(&mut s.removed_notifier, notifier);

        if let Some(existing) = existing {
            let subscriber_id = self.subscriber_id.clone();
            time::spawn(async move {
                existing.remove_observer(subscriber_id.as_str());
            });
        }
        true
    }

    fn handle_source_track_removed(&self) {
        let mut s = self.lock.write();

        // source track removed, we would unsubscribe
        debugw!(
            self.logger,
            "unsubscribing from track since source track was removed"
        );
        s.desired = false;

        self.set_changed_notifier_locked(&mut s, None);
        self.set_removed_notifier_locked(&mut s, None);
    }
}

impl<T> TrackSubscriptionInner<T> {
    fn is_kept_locked(&self) -> bool {
        time::now().before(self.keep_until)
    }
}

// --------------------------------------------------------------------------------------

#[derive(Default)]
struct MediaTrackSubscription {
    track_subscription: TrackSubscription<MediaTrackSubscriptionInner>,

    event_sent: atomic::Bool,
    kind: OnceLock<livekit::TrackType>,

    succ_record_counter: atomic::Int32,
}

// The fields of `mediaTrackSubscription` that the lock of the embedded struct guards.
#[derive(Default)]
struct MediaTrackSubscriptionInner {
    settings: Option<livekit::UpdateTrackSettings>,
    subscribed_track: Option<Arc<dyn SubscribedTrack>>,
    bound: bool,
}

impl Deref for MediaTrackSubscription {
    type Target = TrackSubscription<MediaTrackSubscriptionInner>;

    fn deref(&self) -> &Self::Target {
        &self.track_subscription
    }
}

impl MediaTrackSubscription {
    fn new(
        subscriber_id: livekit::ParticipantId,
        track_id: livekit::TrackId,
        l: Logger,
    ) -> Arc<Self> {
        let s = MediaTrackSubscription {
            track_subscription: TrackSubscription {
                subscriber_id,
                track_id,
                logger: l,
                ..Default::default()
            },
            ..Default::default()
        };
        let t = time::now();
        s.subscribe_at.store(t);
        Arc::new(s)
    }

    fn set_subscribed_track(&self, track: Option<Arc<dyn SubscribedTrack>>) {
        let (old_track, settings) = {
            let mut s = self.lock.write();
            let old_track = std::mem::replace(&mut s.outer.subscribed_track, track.clone());
            s.outer.bound = false;
            (old_track, s.outer.settings.clone())
        };

        if let Some(settings) = &settings
            && let Some(track) = &track
        {
            debugw!(self.logger, "restoring subscriber settings", "settings" => settings);
            track.update_subscriber_settings(settings, true);
        }
        if let Some(old_track) = old_track {
            old_track.on_close(None);
        }
    }

    fn get_subscribed_track(&self) -> Option<Arc<dyn SubscribedTrack>> {
        let s = self.lock.read();
        s.outer.subscribed_track.clone()
    }

    fn try_set_kind(&self, kind: livekit::TrackType) {
        let _ = self.kind.set(kind);
    }

    fn get_kind(&self) -> (livekit::TrackType, bool) {
        match self.kind.get() {
            None => (livekit::TrackType::Audio, false),
            Some(kind) => (*kind, true),
        }
    }

    fn set_settings(&self, settings: &livekit::UpdateTrackSettings) {
        let sub_track = {
            let mut s = self.lock.write();
            s.outer.settings = Some(settings.clone());
            s.outer.subscribed_track.clone()
        };
        if let Some(sub_track) = sub_track {
            sub_track.update_subscriber_settings(settings, false);
        }
    }

    // mark the subscription as bound - when we've received the client's answer
    fn set_bound(&self) {
        let mut s = self.lock.write();
        s.outer.bound = true;
    }

    fn is_bound(&self) -> bool {
        let s = self.lock.read();
        s.outer.bound
    }

    fn maybe_record_error(
        &self,
        tl: Arc<dyn ParticipantTelemetryListener>,
        err: &Error,
        is_user_error: bool,
    ) {
        if self.event_sent.swap(true) {
            return;
        }

        tl.on_track_subscribe_failed(&self.subscriber_id, &self.track_id, err, is_user_error);
    }

    fn maybe_record_success(&self, tl: Arc<dyn ParticipantTelemetryListener>) {
        let Some(sub_track) = self.get_subscribed_track() else {
            return;
        };
        let Some(media_track) = sub_track.media_track() else {
            return;
        };

        let d = time::since(self.subscribe_at.load());
        debugw!(self.logger, "track subscribed", "cost" => d.milliseconds());
        let subscriber = sub_track.subscriber();
        prometheus::record_subscribe_time(
            &subscriber.get_country(),
            media_track.source(),
            media_track.kind(),
            d,
            subscriber
                .get_client_info()
                .map(|client_info| client_info.sdk())
                .unwrap_or_default(),
            subscriber.kind(),
            self.succ_record_counter.inc() as i64,
        );

        let event_sent = self.event_sent.swap(true);

        let pi = livekit::ParticipantInfo {
            identity: sub_track.publisher_identity().to_string(),
            sid: sub_track.publisher_id().to_string(),
            ..Default::default()
        };
        tl.on_track_subscribed(
            &self.subscriber_id,
            &media_track.to_proto(),
            &pi,
            !event_sent,
        );
    }

    fn record_stream_start_latency(&self, elapsed: Duration) {
        let Some(sub_track) = self.get_subscribed_track() else {
            return;
        };
        let Some(media_track) = sub_track.media_track() else {
            return;
        };

        debugw!(self.logger, "track subscribe stream started", "cost" => elapsed.milliseconds());
        let subscriber = sub_track.subscriber();
        prometheus::record_subscribe_stream_start_time(
            &subscriber.get_country(),
            media_track.source(),
            media_track.kind(),
            elapsed,
            subscriber
                .get_client_info()
                .map(|client_info| client_info.sdk())
                .unwrap_or_default(),
            subscriber.kind(),
        );
    }

    fn is_canceled(&self) -> bool {
        !self.event_sent.load() && self.is_desired()
    }

    fn needs_subscribe(&self) -> bool {
        let s = self.lock.read();
        s.desired && s.outer.subscribed_track.is_none()
    }

    fn needs_unsubscribe(&self) -> bool {
        let s = self.lock.read();
        !s.desired && s.outer.subscribed_track.is_some()
    }

    fn needs_bind(&self) -> bool {
        let s = self.lock.read();
        s.desired && s.outer.subscribed_track.is_some() && !s.outer.bound
    }

    fn needs_cleanup(&self) -> bool {
        let s = self.lock.read();
        !s.desired && s.outer.subscribed_track.is_none() && !s.is_kept_locked()
    }
}

// -----------------------------------------------------------------

#[derive(Default)]
struct DataTrackSubscription {
    track_subscription: TrackSubscription<DataTrackSubscriptionInner>,
}

// The fields of `dataTrackSubscription`, which the lock of the embedded struct guards.
#[derive(Default)]
struct DataTrackSubscriptionInner {
    subscription_options: Option<livekit::DataTrackSubscriptionOptions>,

    data_down_track: Option<Arc<dyn DataDownTrack>>,
}

impl Deref for DataTrackSubscription {
    type Target = TrackSubscription<DataTrackSubscriptionInner>;

    fn deref(&self) -> &Self::Target {
        &self.track_subscription
    }
}

impl DataTrackSubscription {
    fn new(
        subscriber_id: livekit::ParticipantId,
        track_id: livekit::TrackId,
        l: Logger,
    ) -> Arc<Self> {
        let s = DataTrackSubscription {
            track_subscription: TrackSubscription {
                subscriber_id,
                track_id,
                logger: l,
                ..Default::default()
            },
        };
        let t = time::now();
        s.subscribe_at.store(t);
        Arc::new(s)
    }

    fn needs_subscribe(&self) -> bool {
        let s = self.lock.read();
        s.desired && s.outer.data_down_track.is_none()
    }

    fn needs_unsubscribe(&self) -> bool {
        let s = self.lock.read();
        !s.desired && s.outer.data_down_track.is_some()
    }

    fn needs_cleanup(&self) -> bool {
        let s = self.lock.read();
        !s.desired && s.outer.data_down_track.is_none() && !s.is_kept_locked()
    }

    fn set_data_down_track(&self, data_down_track: Option<Arc<dyn DataDownTrack>>) {
        let (old_data_down_track, subscription_options) = {
            let mut s = self.lock.write();
            let old_data_down_track =
                std::mem::replace(&mut s.outer.data_down_track, data_down_track.clone());
            (old_data_down_track, s.outer.subscription_options)
        };

        if let Some(data_down_track) = &data_down_track {
            debugw!(self.logger, "restoring data track subscription options", "subscriptionOptions" => subscription_options);
            data_down_track.update_subscription_options(subscription_options.as_ref());
        }
        if let Some(old_data_down_track) = old_data_down_track {
            old_data_down_track.on_close(None);
        }
    }

    fn get_data_down_track(&self) -> Option<Arc<dyn DataDownTrack>> {
        let s = self.lock.read();
        s.outer.data_down_track.clone()
    }

    fn set_subscription_options(
        &self,
        subscription_options: Option<&livekit::DataTrackSubscriptionOptions>,
    ) {
        let data_down_track = {
            let mut s = self.lock.write();
            s.outer.subscription_options = subscription_options.copied();
            s.outer.data_down_track.clone()
        };
        if let Some(data_down_track) = data_down_track {
            data_down_track.update_subscription_options(subscription_options);
        }
    }
}

// -----------------------------------------------------------------

// `doneCh`, the channel that the worker closes when it ends.
#[derive(Default)]
struct DoneCh {
    closed: Mutex<bool>,
    cond: Condvar,
}

impl DoneCh {
    // `<-m.doneCh`. It blocks the thread, as the original blocks its goroutine; on a
    // current-thread runtime it does not wait, see the header.
    fn wait(&self) {
        if matches!(
            tokio::runtime::Handle::try_current(),
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::CurrentThread
        ) {
            return;
        }
        blocking(|| {
            let mut closed = self.closed.lock();
            while !*closed {
                self.cond.wait(&mut closed);
            }
        });
    }
}

// `defer close(m.doneCh)` of the worker: a guard, so that it also happens when the worker is
// dropped before it has ended.
struct CloseDoneCh(Arc<DoneCh>);

impl Drop for CloseDoneCh {
    fn drop(&mut self) {
        *self.0.closed.lock() = true;
        self.0.cond.notify_all();
    }
}

// `defer m.pendingUnsubscribes.Dec()`
struct PendingUnsubscribesDec(Weak<SubscriptionManager>);

impl Drop for PendingUnsubscribesDec {
    fn drop(&mut self) {
        if let Some(m) = self.0.upgrade() {
            m.pending_unsubscribes.dec();
        }
    }
}

// Runs what blocks the thread, as the original blocks its goroutine, and tells the runtime
// so when the thread is a worker of a multi-threaded one.
fn blocking<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

// Go's `time.NewTicker(d)`: the first tick comes after `d`, and ticks that the receiver is
// too slow for are dropped.
fn new_ticker(d: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + d.to_std(), d.to_std());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

// `a == b` of two notifiers as the original compares them: the same one, or none both times.
fn same_notifier(a: &Option<Arc<dyn ChangeNotifier>>, b: &Option<Arc<dyn ChangeNotifier>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b)),
        _ => false,
    }
}

// What is logged of a resolver's result: the fields that have a `Debug`, and of the others
// whether they are there.
struct MediaResolverResultLog<'a>(&'a MediaResolverResult);

impl fmt::Debug for MediaResolverResultLog<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaResolverResult")
            .field(
                "TrackChangedNotifier",
                &self.0.track_changed_notifier.is_some(),
            )
            .field(
                "TrackRemovedNotifier",
                &self.0.track_removed_notifier.is_some(),
            )
            .field("Track", &self.0.track.as_ref().map(|track| track.id()))
            .field("HasPermission", &self.0.has_permission)
            .field("PublisherID", &self.0.publisher_id)
            .field("PublisherIdentity", &self.0.publisher_identity)
            .finish()
    }
}

struct DataResolverResultLog<'a>(&'a DataResolverResult);

impl fmt::Debug for DataResolverResultLog<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataResolverResult")
            .field(
                "TrackChangedNotifier",
                &self.0.track_changed_notifier.is_some(),
            )
            .field(
                "TrackRemovedNotifier",
                &self.0.track_removed_notifier.is_some(),
            )
            .field(
                "DataTrack",
                &self.0.data_track.as_ref().map(|track| track.id()),
            )
            .field("HasPermission", &self.0.has_permission)
            .field("PublisherID", &self.0.publisher_id)
            .field("PublisherIdentity", &self.0.publisher_identity)
            .finish()
    }
}

#[cfg(test)]
#[path = "subscriptionmanager_test.rs"]
mod subscriptionmanager_test;
