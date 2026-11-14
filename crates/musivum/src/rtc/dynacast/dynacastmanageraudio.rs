// Ported from LiveKit, pkg/rtc/dynacast/dynacastmanageraudio.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewDynacastManagerAudio` is
// `DynacastManagerAudio::new` and gives an `Arc` of the manager itself, which is a
// `DynacastManager` (the original returns the interface). The fields that the lock of the
// embedded base guards upstream are a struct of their own, `DynacastManagerAudioInner`,
// which the base keeps under that lock (see `dynacastmanagerbase.rs`); the function the
// original calls with the lock held takes it. The base is a field and does not hold the
// manager: the functions handed to it hold a `Weak` of the manager. The listener is the
// owner of the manager (the media track), so it is held as a `Weak` too, and a listener that
// is gone is not notified. A logger cannot be nil: `Default` of the parameters gives the one
// of `logger.GetLogger()`. The methods are the type's own, in the order of the original, and
// the two traits are implemented on top of those at the end, with the calls the original
// leaves to what it embeds passed on. `is_closed` is never set, as upstream, where the field
// hides the one of the base.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::debugw;
use crate::protocol::codecs::mime::{self, MimeType};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::types;

use super::{
    DynacastManager, DynacastManagerBase, DynacastManagerBaseParams, DynacastManagerListener,
    DynacastQuality, DynacastQualityAudio, DynacastQualityAudioParams, DynacastQualityListener,
};

#[derive(Clone, Default)]
pub struct DynacastManagerAudioParams {
    pub listener: Option<Weak<dyn DynacastManagerListener>>,
    pub logger: Logger,
}

pub struct DynacastManagerAudio {
    params: DynacastManagerAudioParams,

    // `subscribed_codecs` to `is_closed` are in `DynacastManagerAudioInner`
    pub(super) dynacast_manager_base: DynacastManagerBase<DynacastManagerAudioInner>,
}

// What the lock of the base guards of the manager.
#[derive(Default)]
pub(super) struct DynacastManagerAudioInner {
    subscribed_codecs: HashMap<MimeType, bool>,
    committed_subscribed_codecs: HashMap<MimeType, bool>,

    is_closed: bool,
}

impl DynacastManagerAudio {
    pub fn new(params: DynacastManagerAudioParams) -> Arc<DynacastManagerAudio> {
        Arc::new_cyclic(|this: &Weak<DynacastManagerAudio>| {
            let dynacast_manager_base = DynacastManagerBase::new(DynacastManagerBaseParams {
                logger: params.logger.clone(),
                ops_queue_depth: 4,
                on_restart: Box::new(|d: &mut DynacastManagerAudioInner| {
                    d.committed_subscribed_codecs = HashMap::new();
                }),
                on_dynacast_quality_create: Box::new({
                    let d = this.clone();
                    let logger = params.logger.clone();
                    move |mime_type: MimeType| -> Arc<dyn DynacastQuality> {
                        DynacastQualityAudio::new(DynacastQualityAudioParams {
                            mime_type,
                            listener: d.clone(),
                            logger: logger.clone(),
                        })
                    }
                }),
                on_regress_codec: Box::new(
                    |d: &mut DynacastManagerAudioInner, from_mime: MimeType, to_mime: MimeType| {
                        d.subscribed_codecs.insert(from_mime, false);

                        // if the new codec is not added, notify the publisher to start publishing
                        d.subscribed_codecs.entry(to_mime).or_insert(true);
                    },
                ),
                on_update_needed: Box::new({
                    let d = this.clone();
                    move |force: bool| {
                        if let Some(d) = d.upgrade() {
                            d.update(force);
                        }
                    }
                }),
            });
            DynacastManagerAudio {
                params,
                dynacast_manager_base,
            }
        })
    }

    // It is possible for tracks to be in pending close state. When track
    // is waiting to be closed, a node is not streaming a track. This can
    // be used to force an update announcing that subscribed codec is disabled,
    // i.e. indicating not pulling track any more.
    pub fn force_enable(&self, enabled: bool) {
        let mut inner = self.dynacast_manager_base.lock.write();
        let d = &mut inner.manager;

        for committed_enabled in d.committed_subscribed_codecs.values_mut() {
            *committed_enabled = enabled;
        }

        self.enqueue_subscribed_change(d);
    }

    pub fn notify_subscription(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        enabled: bool,
    ) {
        let dq = self
            .dynacast_manager_base
            .get_or_create_dynacast_quality(mime);
        if let Some(dq) = dq {
            dq.notify_subscription(subscriber_id, enabled);
        }
    }

    pub fn notify_subscription_node(
        &self,
        node_id: &livekit::NodeId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) {
        for codec in codecs {
            let dq = self
                .dynacast_manager_base
                .get_or_create_dynacast_quality(mime::normalize_mime_type(&codec.codec));
            if let Some(dq) = dq {
                dq.notify_subscription_node(node_id, codec.enabled);
            }
        }
    }

    fn on_update_audio_codec_for_mime(&self, mime: MimeType, enabled: bool) {
        let mut inner = self.dynacast_manager_base.lock.write();
        if !inner.regressed_codec.contains(&mime) {
            inner.manager.subscribed_codecs.insert(mime, enabled);
        }
        drop(inner);

        self.update(false);
    }

    fn update(&self, force: bool) {
        let mut inner = self.dynacast_manager_base.lock.write();
        let d = &mut inner.manager;

        debugw!(
            self.params.logger,
            "processing subscribed codec change",
            "force" => force,
            "committedSubscribedCodecs" => d.committed_subscribed_codecs,
            "subscribedCodecs" => d.subscribed_codecs,
        );

        if d.subscribed_codecs.is_empty() {
            // no mime has been added, nothing to update
            return;
        }

        // add or remove of a mime triggers an update
        let mut changed = d.subscribed_codecs.len() != d.committed_subscribed_codecs.len();
        if !changed {
            for (mime, &enabled) in &d.subscribed_codecs {
                if let Some(&ce) = d.committed_subscribed_codecs.get(mime)
                    && ce != enabled
                {
                    changed = true;
                    break;
                }
            }
        }

        if !force && !changed {
            return;
        }

        debugw!(
            self.params.logger,
            "committing subscribed codec change",
            "force" => force,
            "committedSubscribedCoecs" => d.committed_subscribed_codecs,
            "subscribedcodecs" => d.subscribed_codecs,
        );

        // commit change
        d.committed_subscribed_codecs = HashMap::with_capacity(d.subscribed_codecs.len());
        for (&mime, &enabled) in &d.subscribed_codecs {
            d.committed_subscribed_codecs.insert(mime, enabled);
        }

        self.enqueue_subscribed_change(d);
    }

    fn enqueue_subscribed_change(&self, d: &DynacastManagerAudioInner) {
        if d.is_closed {
            return;
        }
        let Some(listener) = self.params.listener.clone() else {
            return;
        };

        let mut subscribed_codecs: Vec<livekit::SubscribedAudioCodec> =
            Vec::with_capacity(d.committed_subscribed_codecs.len());
        for (&mime, &enabled) in &d.committed_subscribed_codecs {
            subscribed_codecs.push(livekit::SubscribedAudioCodec {
                codec: mime.to_string(),
                enabled,
            });
        }

        debugw!(
            self.params.logger,
            "subscribedAudioCodecChange",
            "subscribedCodecs" => subscribed_codecs,
        );
        self.dynacast_manager_base
            .notify_ops_queue
            .enqueue(move || {
                if let Some(listener) = listener.upgrade() {
                    listener.on_dynacast_subscribed_audio_codec_change(subscribed_codecs);
                }
            });
    }
}

// The interface, on top of the methods above, of those of the base, and of the
// `DynacastManagerNull` in the base for the four that the original leaves to it.
impl DynacastManager for DynacastManagerAudio {
    fn add_codec(&self, mime: MimeType) {
        self.dynacast_manager_base.add_codec(mime)
    }

    fn handle_codec_regression(&self, from_mime: MimeType, to_mime: MimeType) {
        self.dynacast_manager_base
            .handle_codec_regression(from_mime, to_mime)
    }

    fn restart(&self) {
        self.dynacast_manager_base.restart()
    }

    fn close(&self) {
        self.dynacast_manager_base.close()
    }

    fn force_update(&self) {
        self.dynacast_manager_base.force_update()
    }

    fn resend_committed_quality(&self) {
        self.dynacast_manager_base
            .dynacast_manager_null
            .resend_committed_quality()
    }

    fn force_quality(&self, quality: livekit::VideoQuality) {
        self.dynacast_manager_base
            .dynacast_manager_null
            .force_quality(quality)
    }

    fn force_enable(&self, enabled: bool) {
        DynacastManagerAudio::force_enable(self, enabled)
    }

    fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        quality: livekit::VideoQuality,
    ) {
        self.dynacast_manager_base
            .dynacast_manager_null
            .notify_subscriber_max_quality(subscriber_id, mime, quality)
    }

    fn notify_subscription(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        enabled: bool,
    ) {
        DynacastManagerAudio::notify_subscription(self, subscriber_id, mime, enabled)
    }

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        qualities: &[types::SubscribedCodecQuality],
    ) {
        self.dynacast_manager_base
            .dynacast_manager_null
            .notify_subscriber_node_max_quality(node_id, qualities)
    }

    fn notify_subscription_node(
        &self,
        node_id: &livekit::NodeId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) {
        DynacastManagerAudio::notify_subscription_node(self, node_id, codecs)
    }

    fn clear_subscriber_nodes(&self) {
        self.dynacast_manager_base.clear_subscriber_nodes()
    }
}

// The listener of its qualities, with the call for video passed on to the
// `DynacastQualityListenerNull` in the base.
impl DynacastQualityListener for DynacastManagerAudio {
    fn on_update_max_quality_for_mime(
        &self,
        mime_type: MimeType,
        max_quality: livekit::VideoQuality,
    ) {
        self.dynacast_manager_base
            .dynacast_quality_listener_null
            .on_update_max_quality_for_mime(mime_type, max_quality)
    }

    fn on_update_audio_codec_for_mime(&self, mime_type: MimeType, enabled: bool) {
        DynacastManagerAudio::on_update_audio_codec_for_mime(self, mime_type, enabled)
    }
}
