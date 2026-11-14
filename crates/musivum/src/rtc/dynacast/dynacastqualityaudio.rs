// Ported from LiveKit, pkg/rtc/dynacast/dynacastqualityaudio.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `new` gives an `Arc`, which is a `DynacastQuality`;
// the methods are the type's own, in the order of the original, and the trait is implemented
// on top of those at the end, with the three calls the original leaves to the
// `dynacastQualityNull` it embeds passed on to one. The listener is the manager that owns
// the quality and is held as a `Weak`: a quality whose manager is gone notifies nobody.
// `regress_to` with `None`: the original panics on a nil interface there, after the same
// change of state.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::debugw;
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;

use super::{DynacastQuality, DynacastQualityListener, DynacastQualityNull};

pub(super) struct DynacastQualityAudioParams {
    pub mime_type: MimeType,
    pub listener: Weak<dyn DynacastQualityListener>,
    pub logger: Logger,
}

// dynacastQualityAudio manages enable a single receiver of a media track
pub(super) struct DynacastQualityAudio {
    params: DynacastQualityAudioParams,

    // quality level enable/disable
    lock: RwLock<DynacastQualityAudioInner>,

    dynacast_quality_null: DynacastQualityNull,
}

// What `lock` guards.
struct DynacastQualityAudioInner {
    initialized: bool,
    subscriber_enables: HashMap<livekit::ParticipantId, bool>,
    subscriber_node_enables: HashMap<livekit::NodeId, bool>,
    enabled: bool,
    regress_to: Option<Arc<dyn DynacastQuality>>,
}

impl DynacastQualityAudio {
    pub(super) fn new(params: DynacastQualityAudioParams) -> Arc<DynacastQualityAudio> {
        Arc::new(DynacastQualityAudio {
            params,
            lock: RwLock::new(DynacastQualityAudioInner {
                initialized: false,
                subscriber_enables: HashMap::new(),
                subscriber_node_enables: HashMap::new(),
                enabled: false,
                regress_to: None,
            }),
            dynacast_quality_null: DynacastQualityNull::default(),
        })
    }

    fn start(&self) {
        self.reset();
    }

    fn restart(&self) {
        self.reset();
    }

    fn stop(&self) {}

    fn notify_subscription(&self, subscriber_id: &livekit::ParticipantId, enabled: bool) {
        debugw!(
            self.params.logger,
            "setting subscriber codec enable",
            "mime" => self.params.mime_type,
            "subscriberID" => subscriber_id,
            "enabled" => enabled,
        );

        let mut inner = self.lock.write();
        if inner.regress_to.is_some() {
            return;
        }

        if !enabled {
            inner.subscriber_enables.remove(subscriber_id);
        } else {
            inner.subscriber_enables.insert(subscriber_id.clone(), true);
        }
        drop(inner);

        self.update_quality_change(false);
    }

    fn notify_subscription_node(&self, node_id: &livekit::NodeId, enabled: bool) {
        debugw!(
            self.params.logger,
            "setting subscriber node codec enabled",
            "mime" => self.params.mime_type,
            "subscriberNodeID" => node_id,
            "enabled" => enabled,
        );

        let mut inner = self.lock.write();
        if let Some(r) = &inner.regress_to {
            // the downstream node will synthesize correct enable (its dynacast manager has codec regression), just ignore it
            debugw!(
                self.params.logger,
                "ignoring node codec change, regressed to another dynacast quality",
                "mime" => self.params.mime_type,
                "regressedMime" => r.mime(),
            );
            return;
        }

        if !enabled {
            inner.subscriber_node_enables.remove(node_id);
        } else {
            inner.subscriber_node_enables.insert(node_id.clone(), true);
        }
        drop(inner);

        self.update_quality_change(false);
    }

    fn clear_subscriber_nodes(&self) {
        let mut inner = self.lock.write();
        inner.subscriber_node_enables = HashMap::new();
        drop(inner);

        self.update_quality_change(false);
    }

    fn mime(&self) -> MimeType {
        self.params.mime_type
    }

    fn regress_to(&self, other: Option<Arc<dyn DynacastQuality>>) {
        let mut inner = self.lock.write();
        inner.regress_to = other.clone();
        drop(inner);

        if let Some(other) = other {
            other.restart();
        }
    }

    fn reset(&self) {
        let mut inner = self.lock.write();
        inner.initialized = false;
    }

    fn update_quality_change(&self, force: bool) {
        let mut inner = self.lock.write();
        let enabled =
            !inner.subscriber_enables.is_empty() || !inner.subscriber_node_enables.is_empty();
        if enabled == inner.enabled && inner.initialized && !force {
            return;
        }

        inner.initialized = true;
        inner.enabled = enabled;
        debugw!(
            self.params.logger,
            "notifying enabled change",
            "mime" => self.params.mime_type,
            "enabled" => inner.enabled,
            "subscriberNodeEnables" => inner.subscriber_node_enables,
            "subscribedEnables" => inner.subscriber_enables,
            "force" => force,
        );
        drop(inner);

        if let Some(listener) = self.params.listener.upgrade() {
            listener.on_update_audio_codec_for_mime(self.params.mime_type, enabled);
        }
    }
}

// The interface, on top of the methods above and of the `DynacastQualityNull` for the three
// that the original leaves to the `dynacastQualityNull` it embeds.
impl DynacastQuality for DynacastQualityAudio {
    fn start(&self) {
        DynacastQualityAudio::start(self)
    }

    fn restart(&self) {
        DynacastQualityAudio::restart(self)
    }

    fn stop(&self) {
        DynacastQualityAudio::stop(self)
    }

    fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        quality: livekit::VideoQuality,
    ) {
        self.dynacast_quality_null
            .notify_subscriber_max_quality(subscriber_id, quality)
    }

    fn notify_subscription(&self, subscriber_id: &livekit::ParticipantId, enabled: bool) {
        DynacastQualityAudio::notify_subscription(self, subscriber_id, enabled)
    }

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        quality: livekit::VideoQuality,
    ) {
        self.dynacast_quality_null
            .notify_subscriber_node_max_quality(node_id, quality)
    }

    fn notify_subscription_node(&self, node_id: &livekit::NodeId, enabled: bool) {
        DynacastQualityAudio::notify_subscription_node(self, node_id, enabled)
    }

    fn clear_subscriber_nodes(&self) {
        DynacastQualityAudio::clear_subscriber_nodes(self)
    }

    fn replace(
        &self,
        max_subscriber_quality: HashMap<livekit::ParticipantId, livekit::VideoQuality>,
        max_subscriber_node_quality: HashMap<livekit::NodeId, livekit::VideoQuality>,
    ) {
        self.dynacast_quality_null
            .replace(max_subscriber_quality, max_subscriber_node_quality)
    }

    fn mime(&self) -> MimeType {
        DynacastQualityAudio::mime(self)
    }

    fn regress_to(&self, other: Option<Arc<dyn DynacastQuality>>) {
        DynacastQualityAudio::regress_to(self, other)
    }
}
