// Ported from LiveKit, pkg/rtc/dynacast/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the interfaces are traits, and each "null" type
// implements its trait, as `var _ X = (*xNull)(nil)` says upstream. A listener is given the
// lists it is notified of to keep (`Vec`), identifiers are taken by reference. `regress_to`
// takes an `Option`: the original can be handed a nil interface there (see
// `handle_codec_regression` of `dynacastmanagerbase.rs`).

use std::collections::HashMap;
use std::sync::Arc;

use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;
use crate::rtc::types;

pub trait DynacastManagerListener: Send + Sync {
    fn on_dynacast_subscribed_max_quality_change(
        &self,
        subscribed_qualities: Vec<livekit::SubscribedCodec>,
        max_subscribed_qualities: Vec<types::SubscribedCodecQuality>,
    );

    fn on_dynacast_subscribed_audio_codec_change(&self, codecs: Vec<livekit::SubscribedAudioCodec>);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DynacastManagerListenerNull {}

impl DynacastManagerListener for DynacastManagerListenerNull {
    fn on_dynacast_subscribed_max_quality_change(
        &self,
        _subscribed_qualities: Vec<livekit::SubscribedCodec>,
        _max_subscribed_qualities: Vec<types::SubscribedCodecQuality>,
    ) {
    }
    fn on_dynacast_subscribed_audio_codec_change(
        &self,
        _codecs: Vec<livekit::SubscribedAudioCodec>,
    ) {
    }
}

// -----------------------------------------

pub trait DynacastManager: Send + Sync {
    fn add_codec(&self, mime: MimeType);
    fn handle_codec_regression(&self, from_mime: MimeType, to_mime: MimeType);
    fn restart(&self);
    fn close(&self);
    fn force_update(&self);
    fn resend_committed_quality(&self);
    fn force_quality(&self, quality: livekit::VideoQuality);
    fn force_enable(&self, enabled: bool);

    fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        quality: livekit::VideoQuality,
    );
    fn notify_subscription(
        &self,
        subscriber_id: &livekit::ParticipantId,
        mime: MimeType,
        enabled: bool,
    );

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        qualities: &[types::SubscribedCodecQuality],
    );
    fn notify_subscription_node(
        &self,
        node_id: &livekit::NodeId,
        codecs: &[livekit::SubscribedAudioCodec],
    );
    fn clear_subscriber_nodes(&self);
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DynacastManagerNull {}

impl DynacastManager for DynacastManagerNull {
    fn add_codec(&self, _mime: MimeType) {}
    fn handle_codec_regression(&self, _from_mime: MimeType, _to_mime: MimeType) {}
    fn restart(&self) {}
    fn close(&self) {}
    fn force_update(&self) {}
    fn resend_committed_quality(&self) {}
    fn force_quality(&self, _quality: livekit::VideoQuality) {}
    fn force_enable(&self, _enabled: bool) {}
    fn notify_subscriber_max_quality(
        &self,
        _subscriber_id: &livekit::ParticipantId,
        _mime: MimeType,
        _quality: livekit::VideoQuality,
    ) {
    }
    fn notify_subscription(
        &self,
        _subscriber_id: &livekit::ParticipantId,
        _mime: MimeType,
        _enabled: bool,
    ) {
    }
    fn notify_subscriber_node_max_quality(
        &self,
        _node_id: &livekit::NodeId,
        _qualities: &[types::SubscribedCodecQuality],
    ) {
    }
    fn notify_subscription_node(
        &self,
        _node_id: &livekit::NodeId,
        _codecs: &[livekit::SubscribedAudioCodec],
    ) {
    }
    fn clear_subscriber_nodes(&self) {}
}

// ------------------------------------------------

pub(super) trait DynacastQualityListener: Send + Sync {
    fn on_update_max_quality_for_mime(
        &self,
        mime_type: MimeType,
        max_quality: livekit::VideoQuality,
    );
    fn on_update_audio_codec_for_mime(&self, mime_type: MimeType, enabled: bool);
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DynacastQualityListenerNull {}

impl DynacastQualityListener for DynacastQualityListenerNull {
    fn on_update_max_quality_for_mime(
        &self,
        _mime_type: MimeType,
        _max_quality: livekit::VideoQuality,
    ) {
    }

    fn on_update_audio_codec_for_mime(&self, _mime_type: MimeType, _enabled: bool) {}
}

// ------------------------------------------------

pub(super) trait DynacastQuality: Send + Sync {
    fn start(&self);
    fn restart(&self);
    fn stop(&self);

    fn notify_subscriber_max_quality(
        &self,
        subscriber_id: &livekit::ParticipantId,
        quality: livekit::VideoQuality,
    );
    fn notify_subscription(&self, subscriber_id: &livekit::ParticipantId, enabled: bool);

    fn notify_subscriber_node_max_quality(
        &self,
        node_id: &livekit::NodeId,
        quality: livekit::VideoQuality,
    );
    fn notify_subscription_node(&self, node_id: &livekit::NodeId, enabled: bool);
    fn clear_subscriber_nodes(&self);

    fn replace(
        &self,
        max_subscriber_quality: HashMap<livekit::ParticipantId, livekit::VideoQuality>,
        max_subscriber_node_quality: HashMap<livekit::NodeId, livekit::VideoQuality>,
    );

    fn mime(&self) -> MimeType;
    fn regress_to(&self, other: Option<Arc<dyn DynacastQuality>>);
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DynacastQualityNull {}

impl DynacastQuality for DynacastQualityNull {
    fn start(&self) {}
    fn restart(&self) {}
    fn stop(&self) {}
    fn notify_subscriber_max_quality(
        &self,
        _subscriber_id: &livekit::ParticipantId,
        _quality: livekit::VideoQuality,
    ) {
    }
    fn notify_subscription(&self, _subscriber_id: &livekit::ParticipantId, _enabled: bool) {}
    fn notify_subscriber_node_max_quality(
        &self,
        _node_id: &livekit::NodeId,
        _quality: livekit::VideoQuality,
    ) {
    }
    fn notify_subscription_node(&self, _node_id: &livekit::NodeId, _enabled: bool) {}
    fn clear_subscriber_nodes(&self) {}
    fn replace(
        &self,
        _max_subscriber_quality: HashMap<livekit::ParticipantId, livekit::VideoQuality>,
        _max_subscriber_node_quality: HashMap<livekit::NodeId, livekit::VideoQuality>,
    ) {
    }
    fn mime(&self) -> MimeType {
        MimeType::Unknown
    }
    fn regress_to(&self, _other: Option<Arc<dyn DynacastQuality>>) {}
}
