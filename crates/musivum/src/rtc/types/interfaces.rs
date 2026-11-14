// Ported from LiveKit, pkg/rtc/types/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only `SubscribedCodecQuality` is here so far; the
// interfaces follow when what they name of `pkg/sfu` is ported.

use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscribedCodecQuality {
    pub codec_mime: MimeType,
    pub quality: livekit::VideoQuality,
}
