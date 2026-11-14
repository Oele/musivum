// Ported from pion/interceptor, streaminfo.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only `RTPHeaderExtension` is here, which LiveKit's
// `pkg/sfu/utils` takes. `StreamInfo` and `RTCPFeedback` belong to pion's interceptors, which
// are not ported.

/// RTPHeaderExtension represents a negotiated RFC5285 RTP header extension.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RtpHeaderExtension {
    pub uri: String,
    pub id: i32,
}
