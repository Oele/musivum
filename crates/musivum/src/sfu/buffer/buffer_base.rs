// Ported from LiveKit, pkg/sfu/buffer/buffer_base.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only `ExtPacket` is here so far; the buffer itself
// follows. The packet holds its parts by value, absent where the original's pointer is nil
// (the original points into backing storage inside the same struct and takes packets from a
// pool, both to keep the receive path from allocating: `ExtPacketFactory` and
// `ReleaseExtPacket` have no counterpart, a packet is dropped when its owner is done with
// it). `packet` is never nil in the original and is a plain value here. The embedded
// `VideoLayer` is the field `video_layer`.

use bytes::Bytes;

use super::{ExtDependencyDescriptor, VideoLayer};
use crate::mediatransportutil::codec;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::webrtc::rtp;
use crate::webrtc::rtp::codecs;

// --------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ExtPacket {
    pub video_layer: VideoLayer,
    pub arrival: i64,
    pub ext_sequence_number: u64,
    pub ext_timestamp: u64,
    pub packet: rtp::Packet,
    pub vp8: Option<codec::Vp8>,        // None when not parsed
    pub vp9: Option<codecs::Vp9Packet>, // None when not parsed
    pub is_key_frame: bool,
    pub raw_packet: Bytes,
    pub dependency_descriptor: Option<ExtDependencyDescriptor>,
    pub abs_capture_time_ext: Option<act::AbsCaptureTime>,
    pub is_out_of_order: bool,
    pub is_buffered: bool,
}
