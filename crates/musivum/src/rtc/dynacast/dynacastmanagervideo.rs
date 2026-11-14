// Ported from LiveKit, pkg/rtc/dynacast/dynacastmanagervideo.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewDynacastManagerVideo` is
// `DynacastManagerVideo::new` and gives an `Arc` of the manager itself, which is a
// `DynacastManager` (the original returns the interface, and its test asserts the type
// back). The fields that the lock of the embedded base guards upstream are a struct of their
// own, `DynacastManagerVideoInner`, which the base keeps under that lock (see
// `dynacastmanagerbase.rs`); the functions the original calls with the lock held take it.
// The base is a field and does not hold the manager: the functions handed to it, and the one
// handed to the debounce, hold a `Weak` of the manager. The listener is the owner of the
// manager (the media track), so it is held as a `Weak` too, and a listener that is gone is
// not notified. The debounce of `bep/debounce` is `sfu::utils::Debouncer`. A logger cannot
// be nil: `Default` of the parameters gives the one of `logger.GetLogger()`. The methods are
// the type's own, in the order of the original, and the two traits are implemented on top
// of those at the end, with the calls the original leaves to what it embeds passed on.
// `is_closed` is never set, as upstream, where the field hides the one of the base.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::debugw;
use crate::go::time::Duration;
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::types;
use crate::sfu::utils::Debouncer;

use super::{
    DynacastManager, DynacastManagerBase, DynacastManagerBaseParams, DynacastManagerListener,
    DynacastQuality, DynacastQualityListener, DynacastQualityVideo, DynacastQualityVideoParams,
};

#[derive(Clone, Default)]
pub struct DynacastManagerVideoParams {
    pub dynacast_pause_delay: Duration,
    pub listener: Option<Weak<dyn DynacastManagerListener>>,
    pub logger: Logger,
}

pub struct DynacastManagerVideo {
    params: DynacastManagerVideoParams,

    // `max_subscribed_quality` to `is_closed`, but for the debounce, are in
    // `DynacastManagerVideoInner`
    max_subscribed_quality_debounce: Option<Debouncer>,

    pub(super) dynacast_manager_base: DynacastManagerBase<DynacastManagerVideoInner>,

    // the manager itself, for the function it hands to the debounce
    this: Weak<DynacastManagerVideo>,
}

// What the lock of the base guards of the manager.
#[derive(Default)]
pub(super) struct DynacastManagerVideoInner {
    pub(super) max_subscribed_quality: HashMap<MimeType, livekit::VideoQuality>,
    committed_max_subscribed_quality: HashMap<MimeType, livekit::VideoQuality>,

    max_subscribed_quality_debounce_pending: bool,

    is_closed: bool,
}

impl DynacastManagerVideo {
    pub fn new(params: DynacastManagerVideoParams) -> Arc<DynacastManagerVideo> {
        Arc::new_cyclic(|this: &Weak<DynacastManagerVideo>| {
            let max_subscribed_quality_debounce = if params.dynacast_pause_delay > Duration::ZERO {
                Some(Debouncer::new(params.dynacast_pause_delay))
            } else {
                None
            };
            let dynacast_manager_base = DynacastManagerBase::new(DynacastManagerBaseParams {
                logger: params.logger.clone(),
                ops_queue_depth: 64,
                on_restart: Box::new(|d: &mut DynacastManagerVideoInner| {
                    d.committed_max_subscribed_quality = HashMap::new();
                }),
                on_dynacast_quality_create: Box::new({
                    let d = this.clone();
                    let logger = params.logger.clone();
                    move |mime_type: MimeType| -> Arc<dyn DynacastQuality> {
                        DynacastQualityVideo::new(DynacastQualityVideoParams {
                            mime_type,
                            listener: d.clone(),
                            logger: logger.clone(),
                        })
                    }
                }),
                on_regress_codec: Box::new(
                    |d: &mut DynacastManagerVideoInner, from_mime: MimeType, to_mime: MimeType| {
                        d.max_subscribed_quality
                            .insert(from_mime, livekit::VideoQuality::Off);

                        // if the new codec is not added, notify the publisher to start publishing
                        d.max_subscribed_quality
                            .entry(to_mime)
                            .or_insert(livekit::VideoQuality::High);
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
            DynacastManagerVideo {
                params,
                max_subscribed_quality_debounce,
                dynacast_manager_base,
                this: this.clone(),
            }
        })
    }

    // It is possible for tracks to be in pending close state. When track
    // is waiting to be closed, a node is not streaming a track. This can
    // be used to force an update announcing that subscribed quality is OFF,
    // i.e. indicating not pulling track any more.
    pub fn force_quality(&self, quality: livekit::VideoQuality) {
        let mut inner = self.dynacast_manager_base.lock.write();
        let d = &mut inner.manager;

        for committed_quality in d.committed_max_subscribed_quality.values_mut() {
            *committed_quality = quality;
        }

        self.enqueue_subscribed_quality_change(d);
    }

    // ResendCommittedQuality notifies the listener again of the last committed subscribed qualities,
    // for a publisher that may have lost them. Nothing new is committed: a debounced downgrade stays pending.
    // Only needed if some layer is paused, i.e. some committed quality is not HIGH.
    pub fn resend_committed_quality(&self) {
        let inner = self.dynacast_manager_base.lock.write();
        let d = &inner.manager;

        for &quality in d.committed_max_subscribed_quality.values() {
            if quality != livekit::VideoQuality::High {
                self.enqueue_subscribed_quality_change(d);
                return;
            }
        }
    }

    pub fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        quality: livekit::VideoQuality,
    ) {
        let dq = self
            .dynacast_manager_base
            .get_or_create_dynacast_quality(mime);
        if let Some(dq) = dq {
            dq.notify_subscriber_max_quality(subscriber_id, quality);
        }
    }

    pub fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        qualities: &[types::SubscribedCodecQuality],
    ) {
        for quality in qualities {
            let dq = self
                .dynacast_manager_base
                .get_or_create_dynacast_quality(quality.codec_mime);
            if let Some(dq) = dq {
                dq.notify_subscriber_node_max_quality(node_id, quality.quality);
            }
        }
    }

    fn on_update_max_quality_for_mime(&self, mime: MimeType, max_quality: livekit::VideoQuality) {
        let mut inner = self.dynacast_manager_base.lock.write();
        if !inner.regressed_codec.contains(&mime) {
            inner
                .manager
                .max_subscribed_quality
                .insert(mime, max_quality);
        }
        drop(inner);

        self.update(false);
    }

    fn update(&self, force: bool) {
        let mut inner = self.dynacast_manager_base.lock.write();
        let d = &mut inner.manager;

        debugw!(
            self.params.logger,
            "processing quality change",
            "force" => force,
            "committedMaxSubscribedQuality" => d.committed_max_subscribed_quality,
            "maxSubscribedQuality" => d.max_subscribed_quality,
        );

        if d.max_subscribed_quality.is_empty() {
            // no mime has been added, nothing to update
            return;
        }

        // add or remove of a mime triggers an update
        let mut changed =
            d.max_subscribed_quality.len() != d.committed_max_subscribed_quality.len();
        let mut downgrades_only = !changed;
        if !changed {
            for (mime, &quality) in &d.max_subscribed_quality {
                if let Some(&cq) = d.committed_max_subscribed_quality.get(mime) {
                    if cq != quality {
                        changed = true;
                    }

                    if (cq == livekit::VideoQuality::Off && quality != livekit::VideoQuality::Off)
                        || (cq != livekit::VideoQuality::Off
                            && quality != livekit::VideoQuality::Off
                            && cq < quality)
                    {
                        downgrades_only = false;
                    }
                }
            }
        }

        if !force {
            if !changed {
                return;
            }

            if downgrades_only
                && let Some(max_subscribed_quality_debounce) = &self.max_subscribed_quality_debounce
            {
                if !d.max_subscribed_quality_debounce_pending {
                    debugw!(
                        self.params.logger,
                        "debouncing quality downgrade",
                        "committedMaxSubscribedQuality" => d.committed_max_subscribed_quality,
                        "maxSubscribedQuality" => d.max_subscribed_quality,
                    );
                    let this = self.this.clone();
                    max_subscribed_quality_debounce.add(move || {
                        if let Some(d) = this.upgrade() {
                            d.update(true);
                        }
                    });
                    d.max_subscribed_quality_debounce_pending = true;
                } else {
                    debugw!(
                        self.params.logger,
                        "quality downgrade waiting for debounce",
                        "committedMaxSubscribedQuality" => d.committed_max_subscribed_quality,
                        "maxSubscribedQuality" => d.max_subscribed_quality,
                    );
                }
                return;
            }
        }

        // clear debounce on send
        if let Some(max_subscribed_quality_debounce) = &self.max_subscribed_quality_debounce {
            max_subscribed_quality_debounce.add(|| {});
            d.max_subscribed_quality_debounce_pending = false;
        }

        debugw!(
            self.params.logger,
            "committing quality change",
            "force" => force,
            "committedMaxSubscribedQuality" => d.committed_max_subscribed_quality,
            "maxSubscribedQuality" => d.max_subscribed_quality,
        );

        // commit change
        d.committed_max_subscribed_quality = d.max_subscribed_quality.clone();

        self.enqueue_subscribed_quality_change(d);
    }

    fn enqueue_subscribed_quality_change(&self, d: &DynacastManagerVideoInner) {
        if d.is_closed {
            return;
        }
        let Some(listener) = self.params.listener.clone() else {
            return;
        };

        let mut subscribed_codecs: Vec<livekit::SubscribedCodec> =
            Vec::with_capacity(d.committed_max_subscribed_quality.len());
        let mut max_subscribed_qualities: Vec<types::SubscribedCodecQuality> =
            Vec::with_capacity(d.committed_max_subscribed_quality.len());
        for (&mime, &quality) in &d.committed_max_subscribed_quality {
            max_subscribed_qualities.push(types::SubscribedCodecQuality {
                codec_mime: mime,
                quality,
            });

            if quality == livekit::VideoQuality::Off {
                subscribed_codecs.push(livekit::SubscribedCodec {
                    codec: mime.to_string(),
                    qualities: vec![
                        livekit::SubscribedQuality {
                            quality: livekit::VideoQuality::Low as i32,
                            enabled: false,
                        },
                        livekit::SubscribedQuality {
                            quality: livekit::VideoQuality::Medium as i32,
                            enabled: false,
                        },
                        livekit::SubscribedQuality {
                            quality: livekit::VideoQuality::High as i32,
                            enabled: false,
                        },
                    ],
                });
            } else {
                let mut subscribed_qualities: Vec<livekit::SubscribedQuality> = Vec::new();
                // `for q := livekit.VideoQuality_LOW; q <= livekit.VideoQuality_HIGH; q++`
                for q in [
                    livekit::VideoQuality::Low,
                    livekit::VideoQuality::Medium,
                    livekit::VideoQuality::High,
                ] {
                    subscribed_qualities.push(livekit::SubscribedQuality {
                        quality: q as i32,
                        enabled: q <= quality,
                    });
                }
                subscribed_codecs.push(livekit::SubscribedCodec {
                    codec: mime.to_string(),
                    qualities: subscribed_qualities,
                });
            }
        }

        debugw!(
            self.params.logger,
            "subscribedMaxQualityChange",
            "subscribedCodecs" => subscribed_codecs,
            "maxSubscribedQualities" => max_subscribed_qualities,
        );
        self.dynacast_manager_base
            .notify_ops_queue
            .enqueue(move || {
                if let Some(listener) = listener.upgrade() {
                    listener.on_dynacast_subscribed_max_quality_change(
                        subscribed_codecs,
                        max_subscribed_qualities,
                    );
                }
            });
    }
}

// The interface, on top of the methods above, of those of the base, and of the
// `DynacastManagerNull` in the base for the three that the original leaves to it.
impl DynacastManager for DynacastManagerVideo {
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
        DynacastManagerVideo::resend_committed_quality(self)
    }

    fn force_quality(&self, quality: livekit::VideoQuality) {
        DynacastManagerVideo::force_quality(self, quality)
    }

    fn force_enable(&self, enabled: bool) {
        self.dynacast_manager_base
            .dynacast_manager_null
            .force_enable(enabled)
    }

    fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        quality: livekit::VideoQuality,
    ) {
        DynacastManagerVideo::notify_subscriber_max_quality(self, subscriber_id, mime, quality)
    }

    fn notify_subscription(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        enabled: bool,
    ) {
        self.dynacast_manager_base
            .dynacast_manager_null
            .notify_subscription(subscriber_id, mime, enabled)
    }

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        qualities: &[types::SubscribedCodecQuality],
    ) {
        DynacastManagerVideo::notify_subscriber_node_max_quality(self, node_id, qualities)
    }

    fn notify_subscription_node(
        &self,
        node_id: &livekit::NodeId,
        codecs: &[livekit::SubscribedAudioCodec],
    ) {
        self.dynacast_manager_base
            .dynacast_manager_null
            .notify_subscription_node(node_id, codecs)
    }

    fn clear_subscriber_nodes(&self) {
        self.dynacast_manager_base.clear_subscriber_nodes()
    }
}

// The listener of its qualities, with the call for audio passed on to the
// `DynacastQualityListenerNull` in the base.
impl DynacastQualityListener for DynacastManagerVideo {
    fn on_update_max_quality_for_mime(
        &self,
        mime_type: MimeType,
        max_quality: livekit::VideoQuality,
    ) {
        DynacastManagerVideo::on_update_max_quality_for_mime(self, mime_type, max_quality)
    }

    fn on_update_audio_codec_for_mime(&self, mime_type: MimeType, enabled: bool) {
        self.dynacast_manager_base
            .dynacast_quality_listener_null
            .on_update_audio_codec_for_mime(mime_type, enabled)
    }
}
