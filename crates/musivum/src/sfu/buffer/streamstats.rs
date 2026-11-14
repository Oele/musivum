// Ported from LiveKit, pkg/sfu/buffer/streamstats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; a delta info, which the original points to from
// `RTPStats` and from `Layers` at once, is held by `Arc`, and one that is nil is `None`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sfu::rtpstats;

#[derive(Clone, Debug, Default)]
pub struct StreamStatsWithLayers {
    pub rtp_stats: Option<Arc<rtpstats::RtpDeltaInfo>>,
    pub layers: HashMap<i32, Option<Arc<rtpstats::RtpDeltaInfo>>>,

    pub rtp_stats_remote_view: Option<Arc<rtpstats::RtpDeltaInfo>>,
}
