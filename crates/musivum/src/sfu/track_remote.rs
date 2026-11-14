// Ported from LiveKit, pkg/sfu/track_remote.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. pion's `*webrtc.TrackRemote` is the trait
// `webrtc::TrackRemote` of the connection layer, held in an `Arc`. `TrackRemoteFromSdp`
// embeds it upstream and so has its methods; here it has it as a field and hands each call
// on. Everything of the interface has a counterpart in the layer's trait. `new` gives an
// `Arc`, as the track is used through the interface. The interface has a fake for tests,
// which the original does without (its tests make no receiver from a track).

use std::sync::Arc;

use crate::webrtc;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait TrackRemote: Send + Sync {
    fn id(&self) -> String;
    fn rid(&self) -> String;
    fn msid(&self) -> String;
    fn ssrc(&self) -> webrtc::Ssrc;
    fn rtx_ssrc(&self) -> webrtc::Ssrc;
    fn stream_id(&self) -> String;
    fn kind(&self) -> webrtc::RtpCodecType;
    fn codec(&self) -> webrtc::RtpCodecParameters;
    fn rtc_track(&self) -> Arc<dyn webrtc::TrackRemote>;
}

// TrackRemoteFromSdp represents a remote track that could be created by the sdp.
// It is a wrapper around the webrtc.TrackRemote and return the Codec from sdp
// before the first RTP packet is received.
pub struct TrackRemoteFromSdp {
    // the `*webrtc.TrackRemote` that the original embeds
    track_remote: Arc<dyn webrtc::TrackRemote>,
    sdp_codec: webrtc::RtpCodecParameters,
}

impl TrackRemoteFromSdp {
    pub fn new(
        track: Arc<dyn webrtc::TrackRemote>,
        codec: webrtc::RtpCodecParameters,
    ) -> Arc<TrackRemoteFromSdp> {
        Arc::new(TrackRemoteFromSdp {
            track_remote: track,
            sdp_codec: codec,
        })
    }
}

impl TrackRemote for TrackRemoteFromSdp {
    fn id(&self) -> String {
        self.track_remote.id()
    }

    fn rid(&self) -> String {
        self.track_remote.rid()
    }

    fn msid(&self) -> String {
        self.track_remote.msid()
    }

    fn ssrc(&self) -> webrtc::Ssrc {
        self.track_remote.ssrc()
    }

    fn rtx_ssrc(&self) -> webrtc::Ssrc {
        self.track_remote.rtx_ssrc()
    }

    fn stream_id(&self) -> String {
        self.track_remote.stream_id()
    }

    fn kind(&self) -> webrtc::RtpCodecType {
        self.track_remote.kind()
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        self.sdp_codec.clone()
    }

    fn rtc_track(&self) -> Arc<dyn webrtc::TrackRemote> {
        self.track_remote.clone()
    }
}
