// Ported from LiveKit, pkg/sfu/buffer/videolayerutils.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; a track info that may be nil is an `Option`; the rids
// of a `VideoLayersRid` are `String`s, so the three sets of them are made on first use and a
// function is given a set by reference; each `switch` over which qualities are present is a
// `match` on the three flags, with one pattern for each `case` of the original, in its
// order; a quality or a layer mode that is returned as it is in the track info goes through
// the accessor of the generated message, which gives the default (`LOW`, `MODE_UNUSED`) for
// a number that is not a value of the enumeration, where the original returns the number.

use std::sync::LazyLock;

use super::{DEFAULT_MAX_LAYER_SPATIAL, INVALID_LAYER_SPATIAL};
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::{errorw, infow, warnw};

const QUARTER_RESOLUTION_Q: &str = "q";
const HALF_RESOLUTION_H: &str = "h";
const FULL_RESOLUTION_F: &str = "f";

const QUARTER_RESOLUTION_2: &str = "2";
const HALF_RESOLUTION_1: &str = "1";
const FULL_RESOLUTION_0: &str = "0";

pub type VideoLayersRid = [String; DEFAULT_MAX_LAYER_SPATIAL as usize + 1];

static VIDEO_LAYERS_RID_QHF: LazyLock<VideoLayersRid> = LazyLock::new(|| {
    [
        QUARTER_RESOLUTION_Q.to_string(),
        HALF_RESOLUTION_H.to_string(),
        FULL_RESOLUTION_F.to_string(),
    ]
});
static VIDEO_LAYERS_RID_210: LazyLock<VideoLayersRid> = LazyLock::new(|| {
    [
        QUARTER_RESOLUTION_2.to_string(),
        HALF_RESOLUTION_1.to_string(),
        FULL_RESOLUTION_0.to_string(),
    ]
});
pub static DEFAULT_VIDEO_LAYERS_RID: LazyLock<VideoLayersRid> =
    LazyLock::new(|| VIDEO_LAYERS_RID_QHF.clone());

pub fn layer_presence_from_track_info(
    mime_type: mime::MimeType,
    track_info: Option<&livekit::TrackInfo>,
) -> Option<[bool; livekit::VideoQuality::High as usize + 1]> {
    let track_info = track_info?;

    let layers = get_video_layers_for_mime_type(mime_type, Some(track_info));
    if layers.is_empty() {
        return None;
    }

    let mut layer_presence = [false; livekit::VideoQuality::High as usize + 1];
    for layer in layers {
        // WARNING: comparing protobuf enum
        if layer.quality <= livekit::VideoQuality::High as i32 {
            layer_presence[layer.quality as usize] = true;
        } else {
            warnw!(
                logger::get_logger(),
                "unexpected quality in track info",
                nil,
                "trackID" => track_info.sid,
                "trackInfo" => track_info,
            );
        }
    }

    Some(layer_presence)
}

pub fn rid_to_spatial_layer(
    mime_type: mime::MimeType,
    rid: &str,
    track_info: Option<&livekit::TrackInfo>,
    rid_space: &VideoLayersRid,
) -> i32 {
    let (Some(lp), Some(track_info)) = (
        layer_presence_from_track_info(mime_type, track_info),
        track_info,
    ) else {
        return match rid {
            QUARTER_RESOLUTION_Q => 0,
            HALF_RESOLUTION_H => 1,
            FULL_RESOLUTION_F => 2,
            _ => 0,
        };
    };
    let low = lp[livekit::VideoQuality::Low as usize];
    let medium = lp[livekit::VideoQuality::Medium as usize];
    let high = lp[livekit::VideoQuality::High as usize];

    if rid == rid_space[0] {
        match (low, medium, high) {
            (true, true, true) | (true, true, _) | (true, _, true) | (_, true, true) => 0,

            // only one quality published, could be any
            _ => 0,
        }
    } else if rid == rid_space[1] {
        match (low, medium, high) {
            (true, true, true) | (true, true, _) | (true, _, true) | (_, true, true) => 1,

            // only one quality published, could be any
            _ => 0,
        }
    } else if rid == rid_space[2] {
        match (low, medium, high) {
            (true, true, true) => 2,

            (true, true, _) => {
                warnw!(
                    logger::get_logger(),
                    "unexpected rid with only two qualities, low and medium",
                    nil,
                    "trackID" => track_info.sid,
                    "trackInfo" => track_info,
                    "rid" => rid_space[2],
                );
                1
            }
            (true, _, true) => {
                warnw!(
                    logger::get_logger(),
                    "unexpected rid with only two qualities, low and high",
                    nil,
                    "trackID" => track_info.sid,
                    "trackInfo" => track_info,
                    "rid" => rid_space[2],
                );
                1
            }
            (_, true, true) => {
                warnw!(
                    logger::get_logger(),
                    "unexpected rid with only two qualities, medium and high",
                    nil,
                    "trackID" => track_info.sid,
                    "trackInfo" => track_info,
                    "rid" => rid_space[2],
                );
                1
            }

            // only one quality published, could be any
            _ => 0,
        }
    } else {
        // no rid, should be single layer
        0
    }
}

pub fn spatial_layer_to_rid(
    mime_type: mime::MimeType,
    layer: i32,
    track_info: Option<&livekit::TrackInfo>,
    rid_space: &VideoLayersRid,
) -> String {
    let (Some(lp), Some(track_info)) = (
        layer_presence_from_track_info(mime_type, track_info),
        track_info,
    ) else {
        return match layer {
            0 => QUARTER_RESOLUTION_Q,
            1 => HALF_RESOLUTION_H,
            2 => FULL_RESOLUTION_F,
            _ => QUARTER_RESOLUTION_Q,
        }
        .to_string();
    };
    let low = lp[livekit::VideoQuality::Low as usize];
    let medium = lp[livekit::VideoQuality::Medium as usize];
    let high = lp[livekit::VideoQuality::High as usize];

    match layer {
        0 => match (low, medium, high) {
            (true, true, true) | (true, true, _) | (true, _, true) | (_, true, true) => {
                rid_space[0].clone()
            }

            _ => rid_space[0].clone(),
        },

        1 => match (low, medium, high) {
            (true, true, true) | (true, true, _) | (true, _, true) | (_, true, true) => {
                rid_space[1].clone()
            }

            _ => rid_space[0].clone(),
        },

        2 => match (low, medium, high) {
            (true, true, true) => rid_space[2].clone(),

            (true, true, _) => {
                warnw!(
                    logger::get_logger(),
                    "unexpected layer 2 with only two qualities, low and medium",
                    nil,
                    "trackID" => track_info.sid,
                    "trackInfo" => track_info,
                );
                rid_space[1].clone()
            }
            (true, _, true) => {
                warnw!(
                    logger::get_logger(),
                    "unexpected layer 2 with only two qualities, low and high",
                    nil,
                    "trackID" => track_info.sid,
                    "trackInfo" => track_info,
                );
                rid_space[1].clone()
            }
            (_, true, true) => {
                warnw!(
                    logger::get_logger(),
                    "unexpected layer 2 with only two qualities, medium and high",
                    nil,
                    "trackID" => track_info.sid,
                    "trackInfo" => track_info,
                );
                rid_space[1].clone()
            }

            _ => rid_space[0].clone(),
        },

        _ => rid_space[0].clone(),
    }
}

pub fn video_quality_to_rid(
    mime_type: mime::MimeType,
    quality: livekit::VideoQuality,
    track_info: Option<&livekit::TrackInfo>,
    rid_space: &VideoLayersRid,
) -> String {
    spatial_layer_to_rid(
        mime_type,
        video_quality_to_spatial_layer(mime_type, quality, track_info),
        track_info,
        rid_space,
    )
}

pub fn spatial_layer_to_video_quality(
    mime_type: mime::MimeType,
    layer: i32,
    track_info: Option<&livekit::TrackInfo>,
) -> livekit::VideoQuality {
    let (Some(lp), Some(track_info)) = (
        layer_presence_from_track_info(mime_type, track_info),
        track_info,
    ) else {
        return match layer {
            0 => livekit::VideoQuality::Low,
            1 => livekit::VideoQuality::Medium,
            2 => livekit::VideoQuality::High,
            _ => livekit::VideoQuality::Off,
        };
    };
    let low = lp[livekit::VideoQuality::Low as usize];
    let medium = lp[livekit::VideoQuality::Medium as usize];
    let high = lp[livekit::VideoQuality::High as usize];

    match layer {
        0 => match (low, medium, high) {
            (true, true, true) | (true, true, _) | (true, _, true) | (true, _, _) => {
                livekit::VideoQuality::Low
            }

            (_, true, true) | (_, true, _) => livekit::VideoQuality::Medium,

            _ => livekit::VideoQuality::High,
        },

        1 => match (low, medium, high) {
            (true, true, true) | (true, true, _) => livekit::VideoQuality::Medium,

            (true, _, true) | (_, true, true) => livekit::VideoQuality::High,

            _ => {
                errorw!(
                    logger::get_logger(),
                    "invalid layer",
                    nil,
                    "trackID" => track_info.sid,
                    "layer" => layer,
                    "trackInfo" => track_info,
                );
                livekit::VideoQuality::High
            }
        },

        2 => match (low, medium, high) {
            (true, true, true) => livekit::VideoQuality::High,

            _ => {
                errorw!(
                    logger::get_logger(),
                    "invalid layer",
                    nil,
                    "trackID" => track_info.sid,
                    "layer" => layer,
                    "trackInfo" => track_info,
                );
                livekit::VideoQuality::High
            }
        },

        _ => livekit::VideoQuality::Off,
    }
}

pub fn video_quality_to_spatial_layer(
    mime_type: mime::MimeType,
    quality: livekit::VideoQuality,
    track_info: Option<&livekit::TrackInfo>,
) -> i32 {
    let Some(lp) = layer_presence_from_track_info(mime_type, track_info) else {
        return match quality {
            livekit::VideoQuality::Low => 0,
            livekit::VideoQuality::Medium => 1,
            livekit::VideoQuality::High => 2,
            _ => INVALID_LAYER_SPATIAL,
        };
    };
    let low = lp[livekit::VideoQuality::Low as usize];
    let medium = lp[livekit::VideoQuality::Medium as usize];
    let high = lp[livekit::VideoQuality::High as usize];

    match quality {
        livekit::VideoQuality::Low => match (low, medium, high) {
            (true, true, true) | (true, true, _) | (true, _, true) | (_, true, true) => 0,
            _ => 0, // only one quality published, could be any
        },

        livekit::VideoQuality::Medium => match (low, medium, high) {
            (true, true, true) | (true, true, _) | (true, _, true) => 1,

            (_, true, true) => 0,

            _ => 0, // only one quality published, could be any
        },

        livekit::VideoQuality::High => match (low, medium, high) {
            (true, true, true) => 2,

            (true, true, _) | (true, _, true) | (_, true, true) => 1,

            _ => 0, // only one quality published, could be any
        },

        _ => INVALID_LAYER_SPATIAL,
    }
}

pub fn get_video_layer_mode_for_mime_type(
    mime_type: mime::MimeType,
    ti: Option<&livekit::TrackInfo>,
) -> livekit::video_layer::Mode {
    if let Some(ti) = ti {
        for codec in &ti.codecs {
            if mime::normalize_mime_type(&codec.mime_type) == mime_type {
                return codec.video_layer_mode();
            }
        }
    }

    livekit::video_layer::Mode::Unused
}

pub fn get_video_layers_for_mime_type(
    mime_type: mime::MimeType,
    ti: Option<&livekit::TrackInfo>,
) -> &[livekit::VideoLayer] {
    let mut layers: &[livekit::VideoLayer] = &[];
    if let Some(ti) = ti {
        for codec in &ti.codecs {
            if mime::normalize_mime_type(&codec.mime_type) == mime_type {
                layers = &codec.layers;
                break;
            }
        }
        if layers.is_empty() {
            layers = &ti.layers;
        }
    }
    layers
}

pub fn get_spatial_layer_for_rid(
    mime_type: mime::MimeType,
    rid: &str,
    ti: Option<&livekit::TrackInfo>,
) -> i32 {
    let Some(ti) = ti else {
        return INVALID_LAYER_SPATIAL;
    };

    if rid.is_empty() {
        // single layer without RID
        return 0;
    }

    let layers = get_video_layers_for_mime_type(mime_type, Some(ti));
    for layer in layers {
        if layer.rid == rid {
            return layer.spatial_layer;
        }
    }

    if !layers.is_empty() {
        // RID present in codec, but may not be specified via signalling
        // (happens with older browsers setting a rid for SVC codecs)
        let mut has_rid = false;
        for layer in layers {
            if !layer.rid.is_empty() {
                has_rid = true;
                break;
            }
        }
        if !has_rid {
            return 0;
        }
    }

    // SIMULCAST-CODEC-TODO - ideally should return invalid, but there are
    // VP9 publishers using rid = f, if there are only two layers
    // in TrackInfo, that will be q;h and f will become invalid.
    //
    // Actually, there should be no rids for VP9 in SDP and hence
    // the above check should take effect. However, as simulcast
    // codec/back up codec does not update rids from SDP,
    // the default rids are used when vp9 (primary codec)
    // is published. Due to that the above check gets bypassed.
    //
    // The full proper sequence would be
    // 1. For primary codec using SVC, there will be no rids.
    //    The above check should take effect and it should
    //    return 0 even if some publisher uses a rid like `f`.
    // 2. When secondary codec is published, rids for the codec
    //    corresponding to the back up codec mime type should
    //    be updated in `TrackInfo`. This is a bit tricky
    //    for a couple of cases
    //    a. Browsers like Firefox use a different CID every time.
    //       So, it cannot be matched between `AddTrack` and SDP.
    //       One option is to look for a published track with
    //       back up codec and apply it there. But, that becomes
    //       a challenge if there are multiple published tracks
    //       with pending back up codec.
    //    b. The back up codec publish SDP will have the full
    //       codec list. It should be okay to assume that the
    //       codec that will be published is the back up codec,
    //       but just something to be aware of.
    // 3. Use of this function with proper mime so that proper
    //    codec section can be looked up in `TrackInfo`.
    // return InvalidLayerSpatial
    infow!(
        logger::get_logger(),
        "invalid layer for rid, returning default",
        "trackID" => ti.sid,
        "rid" => rid,
        "mimeType" => mime_type,
        "trackInfo" => ti,
    );
    0
}

pub fn get_spatial_layer_for_video_quality(
    mime_type: mime::MimeType,
    quality: livekit::VideoQuality,
    ti: Option<&livekit::TrackInfo>,
) -> i32 {
    let Some(ti) = ti else {
        return INVALID_LAYER_SPATIAL;
    };
    if quality == livekit::VideoQuality::Off {
        return INVALID_LAYER_SPATIAL;
    }

    let layers = get_video_layers_for_mime_type(mime_type, Some(ti));
    for layer in layers {
        if layer.quality == quality as i32 {
            return layer.spatial_layer;
        }
    }

    if layers.is_empty() {
        // single layer
        return 0;
    }

    // requested quality is higher than available layers, return the highest available layer
    video_quality_to_spatial_layer(mime_type, quality, Some(ti))
}

pub fn get_video_quality_for_spatial_layer(
    mime_type: mime::MimeType,
    spatial_layer: i32,
    ti: Option<&livekit::TrackInfo>,
) -> livekit::VideoQuality {
    if spatial_layer == INVALID_LAYER_SPATIAL || ti.is_none() {
        return livekit::VideoQuality::Off;
    }

    let layers = get_video_layers_for_mime_type(mime_type, ti);
    for layer in layers {
        if layer.spatial_layer == spatial_layer {
            return layer.quality();
        }
    }

    livekit::VideoQuality::Off
}

fn is_video_layers_rid_known(rids: &VideoLayersRid, known_rids: &VideoLayersRid) -> bool {
    for rid in rids {
        if rid.is_empty() {
            continue;
        }

        if !known_rids.contains(rid) {
            return false;
        }
    }

    true
}

pub fn normalize_video_layers_rid(rids: &VideoLayersRid) -> VideoLayersRid {
    let mut out = rids.clone();

    let mut normalize = |known_rids: &VideoLayersRid| {
        let mut idx = 0;
        for known in known_rids {
            if rids.contains(known) {
                out[idx] = known.clone();
                idx += 1;
            }
        }
    };

    if is_video_layers_rid_known(rids, &VIDEO_LAYERS_RID_QHF) {
        normalize(&VIDEO_LAYERS_RID_QHF);
    }

    if is_video_layers_rid_known(rids, &VIDEO_LAYERS_RID_210) {
        normalize(&VIDEO_LAYERS_RID_210);
    }

    out
}

#[cfg(test)]
#[path = "videolayerutils_test.rs"]
mod videolayerutils_test;
