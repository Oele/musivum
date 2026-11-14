// Ported from LiveKit, pkg/sfu/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The interfaces are traits, with fakes for tests where
// the original has counterfeiter make them. `DownTrackStreamAllocatorListener` is not here
// yet: its methods take a `*DownTrack`, and it is ported with the down track. A callback
// handed over (`AddOnReady`, `AddOnCodecStateChange`) is a boxed closure. `DebugInfo`'s
// `map[string]any` is a JSON object. A `*livekit.TrackInfo` that is handed out is shared
// (`Arc`), one that is handed in is borrowed. `REDTransformer` repeats `GetDownTracks`, which
// it has from `TrackReceiver`. At the end, a track sender is made a member of the down
// track spreader's two traits, which upstream is a matter of having the methods.

use std::sync::Arc;

use crate::go::time::Duration;
use crate::mediatransportutil::codec;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::sfu::buffer;
use crate::sfu::utils as sfuutils;
use crate::sfu::{Bitrates, Error, ReceiverCodecState};
use crate::webrtc;

/// Go's `map[string]any` of the `DebugInfo()` methods.
pub type DebugInfo = serde_json::Map<String, serde_json::Value>;

// TrackReceiver defines an interface receive media from remote peer
#[cfg_attr(test, musivum_fakes::fake)]
pub trait TrackReceiver: Send + Sync {
    fn track_id(&self) -> livekit::TrackId;
    fn stream_id(&self) -> String;

    // returns the initial codec of the receiver, it is determined by the track's codec
    // and will not change if the codec changes during the session (publisher changes codec)
    fn codec(&self) -> webrtc::RtpCodecParameters;
    fn mime(&self) -> mime::MimeType;
    fn video_layer_mode(&self) -> livekit::video_layer::Mode;
    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter>;
    fn is_closed(&self) -> bool;

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, Error>;
    fn get_layered_bitrate(&self) -> (Vec<i32>, Bitrates);

    fn get_audio_level(&self) -> (f64, bool);

    fn send_pli(&self, layer: i32, force: bool);

    fn set_max_expected_spatial_layer(&self, layer: i32);

    fn add_down_track(&self, track: Arc<dyn TrackSender>) -> Result<(), Error>;
    fn delete_down_track(&self, participant_id: &livekit::ParticipantId);
    fn get_down_tracks(&self) -> Vec<Arc<dyn TrackSender>>;

    fn debug_info(&self) -> DebugInfo;

    fn track_info(&self) -> Arc<livekit::TrackInfo>;
    fn update_track_info(&self, ti: &livekit::TrackInfo);

    // Get primary receiver if this receiver represents a RED codec; otherwise it will return itself
    fn get_primary_receiver_for_red(&self) -> Arc<dyn TrackReceiver>;

    // Get red receiver for primary codec, used by forward red encodings for opus only codec
    fn get_red_receiver(&self) -> Arc<dyn TrackReceiver>;

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32>;

    fn get_track_stats(&self) -> Option<livekit::RtpStats>;

    // AddOnReady adds a function to be called when the receiver is ready, the callback
    // could be called immediately if the receiver is ready when the callback is added
    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>);

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, ReceiverCodecState) + Send + Sync>,
    );
    fn codec_state(&self) -> ReceiverCodecState;

    // VideoSizes returns the video size parsed from rtp packet for each spatial layer.
    fn video_sizes(&self) -> Vec<codec::VideoSize>;

    // closes all associated buffers and issues a resync to all attached downtracks so that
    // they can resync and have proper sequncing without gaps in sequence numbers / timestamps
    fn restart(&self, reason: &str);
}

// --------------------------------------

#[cfg_attr(test, musivum_fakes::fake)]
pub trait RedTransformer: TrackReceiver {
    fn set_lb_threshold(&self, lb_threshold: usize);
    fn forward_rtp(&self, pkt: &buffer::ExtPacket, spatial_layer: i32) -> i32;
    fn forward_rtcp_sender_report(
        &self,
        payload_type: webrtc::PayloadType,
        layer: i32,
        publisher_sr_data: &livekit::RtcpSenderReportState,
    );
    fn resync_down_tracks(&self);
    fn on_stream_restart(&self);
    fn can_close(&self) -> bool;
    fn close(&self);
}

// --------------------------------------

// TrackSender defines an interface send media to remote peer
#[cfg_attr(test, musivum_fakes::fake)]
pub trait TrackSender: Send + Sync {
    fn up_track_layers_change(&self);
    fn up_track_bitrate_availability_change(&self);
    fn up_track_max_published_layer_change(&self, max_published_layer: i32);
    fn up_track_max_temporal_layer_seen_change(&self, max_temporal_layer_seen: i32);
    fn up_track_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates);
    fn write_rtp(&self, p: &buffer::ExtPacket, layer: i32) -> i32;
    fn close(&self);
    fn is_closed(&self) -> bool;
    // ID is the globally unique identifier for this Track.
    fn id(&self) -> String;
    fn subscriber_id(&self) -> livekit::ParticipantId;
    fn handle_rtcp_sender_report_data(
        &self,
        payload_type: webrtc::PayloadType,
        layer: i32,
        publisher_sr_data: &livekit::RtcpSenderReportState,
    ) -> Result<(), Error>;
    fn resync(&self);
    fn set_receiver(&self, r: Arc<dyn TrackReceiver>);
    fn receiver_restart(&self, r: Arc<dyn TrackReceiver>);
}

// -------------------------------------------------------------------

#[cfg_attr(test, musivum_fakes::fake)]
pub trait DownTrackListener: Send + Sync {
    fn on_bind_and_connected(&self);
    fn on_stats_update(&self, stat: &livekit::AnalyticsStat);
    fn on_max_subscribed_layer_changed(&self, layer: i32);
    fn on_rtt_update(&self, rtt: u32);
    fn on_codec_negotiated(&self, codec: webrtc::RtpCodecCapability);
    fn on_down_track_close(&self, is_expected_to_resume: bool);
    fn on_stream_started(&self, elapsed: Duration);
}

// -------------------------------------------------------------------

// What the down track spreader (`sfu/utils`) asks of the down tracks it holds: upstream any
// type with these methods will do, here a track sender is one by these two lines.

impl sfuutils::Sender for dyn TrackSender {
    fn subscriber_id(&self) -> livekit::ParticipantId {
        TrackSender::subscriber_id(self)
    }
}

impl sfuutils::RtpWriter<buffer::ExtPacket> for dyn TrackSender {
    fn write_rtp(&self, pkt: &buffer::ExtPacket, layer: i32) -> i32 {
        TrackSender::write_rtp(self, pkt, layer)
    }
}
