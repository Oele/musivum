// Ported from LiveKit, pkg/rtc/dynacast/dynacastqualityvideo.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `new` gives an `Arc`, which is a `DynacastQuality`;
// the methods are the type's own, in the order of the original, and the trait is implemented
// on top of those at the end, with the two calls the original leaves to the
// `dynacastQualityNull` it embeds passed on to one. The listener is the manager that owns
// the quality and is held as a `Weak`; the function of the max quality timer holds a `Weak`
// of the quality too. So a quality whose manager is gone notifies nobody, where upstream a
// running timer keeps both alive until it has fired. `regress_to` with `None`: the original
// panics on a nil interface there, after the same changes of state.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::debugw;
use crate::go::time::{self, Duration, Timer};
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;

use super::{DynacastQuality, DynacastQualityListener, DynacastQualityNull};

pub(super) const INITIAL_QUALITY_UPDATE_WAIT: Duration = Duration::from_secs(10);

pub(super) struct DynacastQualityVideoParams {
    pub mime_type: MimeType,
    pub listener: Weak<dyn DynacastQualityListener>,
    pub logger: Logger,
}

// dynacastQualityVideo manages max subscribed quality of a single receiver of a media track
pub(super) struct DynacastQualityVideo {
    params: DynacastQualityVideoParams,

    // quality level enable/disable
    lock: RwLock<DynacastQualityVideoInner>,

    dynacast_quality_null: DynacastQualityNull,

    // the quality itself, for the function of the max quality timer
    this: Weak<DynacastQualityVideo>,
}

// What `lock` guards.
struct DynacastQualityVideoInner {
    initialized: bool,
    max_subscriber_quality: HashMap<livekit::ParticipantId, livekit::VideoQuality>,
    max_subscriber_node_quality: HashMap<livekit::NodeId, livekit::VideoQuality>,
    max_subscribed_quality: livekit::VideoQuality,
    max_quality_timer: Option<Timer>,
    regress_to: Option<Arc<dyn DynacastQuality>>,
}

impl DynacastQualityVideo {
    pub(super) fn new(params: DynacastQualityVideoParams) -> Arc<DynacastQualityVideo> {
        Arc::new_cyclic(|this| DynacastQualityVideo {
            params,
            lock: RwLock::new(DynacastQualityVideoInner {
                initialized: false,
                max_subscriber_quality: HashMap::new(),
                max_subscriber_node_quality: HashMap::new(),
                // Go's zero value
                max_subscribed_quality: livekit::VideoQuality::Low,
                max_quality_timer: None,
                regress_to: None,
            }),
            dynacast_quality_null: DynacastQualityNull::default(),
            this: this.clone(),
        })
    }

    fn start(&self) {
        self.reset();
    }

    fn restart(&self) {
        self.reset();
    }

    fn stop(&self) {
        self.stop_max_quality_timer();
    }

    fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        quality: livekit::VideoQuality,
    ) {
        debugw!(
            self.params.logger,
            "setting subscriber max quality",
            "mime" => self.params.mime_type,
            "subscriberID" => subscriber_id,
            "quality" => quality.as_str_name(),
        );

        let mut inner = self.lock.write();
        if let Some(r) = inner.regress_to.clone() {
            drop(inner);
            r.notify_subscriber_max_quality(subscriber_id, quality);
            return;
        }

        if quality == livekit::VideoQuality::Off {
            inner.max_subscriber_quality.remove(subscriber_id);
        } else {
            inner
                .max_subscriber_quality
                .insert(subscriber_id.clone(), quality);
        }
        drop(inner);

        self.update_quality_change(false);
    }

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        quality: livekit::VideoQuality,
    ) {
        debugw!(
            self.params.logger,
            "setting subscriber node max quality",
            "mime" => self.params.mime_type,
            "subscriberNodeID" => node_id,
            "quality" => quality.as_str_name(),
        );

        let mut inner = self.lock.write();
        if let Some(r) = &inner.regress_to {
            // the downstream node will synthesize correct quality notify (its dynacast manager has codec regression), just ignore it
            debugw!(
                self.params.logger,
                "ignoring node quality change, regressed to another dynacast quality",
                "mime" => self.params.mime_type,
                "regressedMime" => r.mime(),
            );
            return;
        }

        if quality == livekit::VideoQuality::Off {
            inner.max_subscriber_node_quality.remove(node_id);
        } else {
            inner
                .max_subscriber_node_quality
                .insert(node_id.clone(), quality);
        }
        drop(inner);

        self.update_quality_change(false);
    }

    fn clear_subscriber_nodes(&self) {
        let mut inner = self.lock.write();
        inner.max_subscriber_node_quality = HashMap::new();
        drop(inner);

        self.update_quality_change(false);
    }

    fn mime(&self) -> MimeType {
        self.params.mime_type
    }

    fn regress_to(&self, other: Option<Arc<dyn DynacastQuality>>) {
        let mut inner = self.lock.write();
        inner.regress_to = other.clone();
        let max_subscriber_quality = std::mem::take(&mut inner.max_subscriber_quality);
        let max_subscriber_node_quality = std::mem::take(&mut inner.max_subscriber_node_quality);
        drop(inner);

        if let Some(other) = other {
            other.replace(max_subscriber_quality, max_subscriber_node_quality);
        }
    }

    fn replace(
        &self,
        max_subscriber_quality: HashMap<livekit::ParticipantId, livekit::VideoQuality>,
        max_subscriber_node_quality: HashMap<livekit::NodeId, livekit::VideoQuality>,
    ) {
        let mut inner = self.lock.write();
        for (sub_id, quality) in max_subscriber_quality {
            if let Some(old_quality) = inner.max_subscriber_quality.get(&sub_id).copied() {
                // no QUALITY_OFF in the map
                if quality > old_quality {
                    inner.max_subscriber_quality.insert(sub_id, quality);
                }
            } else {
                inner.max_subscriber_quality.insert(sub_id, quality);
            }
        }

        for (node_id, quality) in max_subscriber_node_quality {
            if let Some(old_quality) = inner.max_subscriber_node_quality.get(&node_id).copied() {
                // no QUALITY_OFF in the map
                if quality > old_quality {
                    inner.max_subscriber_node_quality.insert(node_id, quality);
                }
            } else {
                inner.max_subscriber_node_quality.insert(node_id, quality);
            }
        }
        drop(inner);

        self.restart();
    }

    fn reset(&self) {
        let mut inner = self.lock.write();
        inner.initialized = false;
        drop(inner);

        self.start_max_quality_timer();
    }

    fn update_quality_change(&self, force: bool) {
        let mut inner = self.lock.write();
        let mut max_subscribed_quality = livekit::VideoQuality::Off;
        for &sub_quality in inner.max_subscriber_quality.values() {
            if max_subscribed_quality == livekit::VideoQuality::Off
                || (sub_quality != livekit::VideoQuality::Off
                    && sub_quality > max_subscribed_quality)
            {
                max_subscribed_quality = sub_quality;
            }
        }
        for &node_quality in inner.max_subscriber_node_quality.values() {
            if max_subscribed_quality == livekit::VideoQuality::Off
                || (node_quality != livekit::VideoQuality::Off
                    && node_quality > max_subscribed_quality)
            {
                max_subscribed_quality = node_quality;
            }
        }

        if max_subscribed_quality == inner.max_subscribed_quality && inner.initialized && !force {
            return;
        }

        inner.initialized = true;
        inner.max_subscribed_quality = max_subscribed_quality;
        debugw!(
            self.params.logger,
            "notifying quality change",
            "mime" => self.params.mime_type,
            "maxSubscriberQuality" => inner.max_subscriber_quality,
            "maxSubscriberNodeQuality" => inner.max_subscriber_node_quality,
            "maxSubscribedQuality" => inner.max_subscribed_quality,
            "force" => force,
        );
        drop(inner);

        if let Some(listener) = self.params.listener.upgrade() {
            listener.on_update_max_quality_for_mime(self.params.mime_type, max_subscribed_quality);
        }
    }

    fn start_max_quality_timer(&self) {
        let mut inner = self.lock.write();

        if let Some(max_quality_timer) = inner.max_quality_timer.take() {
            max_quality_timer.stop();
        }

        let d = self.this.clone();
        inner.max_quality_timer = Some(time::after_func(INITIAL_QUALITY_UPDATE_WAIT, move || {
            if let Some(d) = d.upgrade() {
                d.stop_max_quality_timer();
                d.update_quality_change(true);
            }
        }));
    }

    fn stop_max_quality_timer(&self) {
        let mut inner = self.lock.write();

        if let Some(max_quality_timer) = inner.max_quality_timer.take() {
            max_quality_timer.stop();
        }
    }
}

// The interface, on top of the methods above and of the `DynacastQualityNull` for the two
// that the original leaves to the `dynacastQualityNull` it embeds.
impl DynacastQuality for DynacastQualityVideo {
    fn start(&self) {
        DynacastQualityVideo::start(self)
    }

    fn restart(&self) {
        DynacastQualityVideo::restart(self)
    }

    fn stop(&self) {
        DynacastQualityVideo::stop(self)
    }

    fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        quality: livekit::VideoQuality,
    ) {
        DynacastQualityVideo::notify_subscriber_max_quality(self, subscriber_id, quality)
    }

    fn notify_subscription(&self, subscriber_id: &livekit::ParticipantId, enabled: bool) {
        self.dynacast_quality_null
            .notify_subscription(subscriber_id, enabled)
    }

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        quality: livekit::VideoQuality,
    ) {
        DynacastQualityVideo::notify_subscriber_node_max_quality(self, node_id, quality)
    }

    fn notify_subscription_node(&self, node_id: &livekit::NodeId, enabled: bool) {
        self.dynacast_quality_null
            .notify_subscription_node(node_id, enabled)
    }

    fn clear_subscriber_nodes(&self) {
        DynacastQualityVideo::clear_subscriber_nodes(self)
    }

    fn replace(
        &self,
        max_subscriber_quality: HashMap<livekit::ParticipantId, livekit::VideoQuality>,
        max_subscriber_node_quality: HashMap<livekit::NodeId, livekit::VideoQuality>,
    ) {
        DynacastQualityVideo::replace(self, max_subscriber_quality, max_subscriber_node_quality)
    }

    fn mime(&self) -> MimeType {
        DynacastQualityVideo::mime(self)
    }

    fn regress_to(&self, other: Option<Arc<dyn DynacastQuality>>) {
        DynacastQualityVideo::regress_to(self, other)
    }
}
