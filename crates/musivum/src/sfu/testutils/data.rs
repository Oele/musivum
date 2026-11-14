// Ported from LiveKit, pkg/sfu/testutils/data.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the two test codecs are functions, since a codec
// capability owns its strings and cannot be a constant.

use std::sync::Arc;

use bytes::Bytes;

use crate::go::time::Time;
use crate::mediatransportutil::codec;
use crate::sfu::buffer;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::webrtc;
use crate::webrtc::rtp;

// -----------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct TestExtPacketParams {
    pub marker: bool,
    pub is_key_frame: bool,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub sn_cycles: i64,
    pub timestamp: u32,
    pub ts_cycles: i64,
    pub ssrc: u32,
    pub payload_size: usize,
    pub padding_size: u8,
    pub arrival_time: Time,
    pub video_layer: buffer::VideoLayer,
    pub is_out_of_order: bool,
}

// -----------------------------------------------------------

pub fn get_test_ext_packet(params: &TestExtPacketParams) -> Result<buffer::ExtPacket, rtp::Error> {
    let packet = rtp::Packet {
        header: rtp::Header {
            version: 2,
            padding: params.padding_size != 0,
            marker: params.marker,
            payload_type: params.payload_type,
            sequence_number: params.sequence_number,
            timestamp: params.timestamp,
            ssrc: params.ssrc,
            padding_size: params.padding_size,
            ..Default::default()
        },
        payload: Bytes::from(vec![0u8; params.payload_size]),
        ..Default::default()
    };

    let raw = packet.marshal()?;

    let ep = buffer::ExtPacket {
        video_layer: params.video_layer,
        ext_sequence_number: ((params.sn_cycles << 16) as u64)
            .wrapping_add(params.sequence_number as u64),
        ext_timestamp: ((params.ts_cycles << 32) as u64).wrapping_add(params.timestamp as u64),
        arrival: params.arrival_time.unix_nano(),
        packet,
        is_key_frame: params.is_key_frame,
        raw_packet: Bytes::from(raw),
        is_out_of_order: params.is_out_of_order,
        ..Default::default()
    };

    Ok(ep)
}

// --------------------------------------

pub fn get_test_ext_packet_vp8(
    params: &TestExtPacketParams,
    vp8: &codec::Vp8,
) -> Result<buffer::ExtPacket, rtp::Error> {
    let mut ep = get_test_ext_packet(params)?;

    ep.is_key_frame = vp8.is_key_frame;
    let vp8_copy = *vp8; // keep the packet independent of later changes to vp8
    ep.vp8 = Some(vp8_copy);
    if ep.dependency_descriptor.is_none() {
        ep.video_layer.temporal = vp8.tid as i32;
    }
    Ok(ep)
}

// --------------------------------------

#[derive(Clone, Debug, Default)]
pub struct TestDdParams {
    pub first_packet_in_frame: bool,
    pub ext_frame_num: u64,
    pub temporal_id: i64,
    // decode target indications as one character per decode target: S, R, D or -
    pub dtis: String,
    pub frame_diffs: Vec<i64>,
    pub decode_targets: Vec<buffer::DependencyDescriptorDecodeTarget>,
}

// GetTestExtPacketVP8DD returns a VP8 packet with a dependency descriptor and the
// minimized VP8 payload descriptor that libwebrtc sends with it: no TID, no Y bit and
// no picture ID.
pub fn get_test_ext_packet_vp8_dd(
    params: &TestExtPacketParams,
    dd_params: &TestDdParams,
) -> Result<buffer::ExtPacket, rtp::Error> {
    let mut ep = get_test_ext_packet_vp8(
        params,
        &codec::Vp8 {
            s: dd_params.first_packet_in_frame,
            is_key_frame: params.is_key_frame,
            ..Default::default()
        },
    )?;

    let mut dtis = Vec::with_capacity(dd_params.dtis.len());
    for c in dd_params.dtis.chars() {
        match c {
            'S' => dtis.push(dd::DecodeTargetIndication::Switch),
            'R' => dtis.push(dd::DecodeTargetIndication::Required),
            'D' => dtis.push(dd::DecodeTargetIndication::Discardable),
            _ => dtis.push(dd::DecodeTargetIndication::NotPresent),
        }
    }
    ep.video_layer.temporal = dd_params.temporal_id as i32;
    ep.dependency_descriptor = Some(buffer::ExtDependencyDescriptor {
        descriptor: Some(dd::DependencyDescriptor {
            first_packet_in_frame: dd_params.first_packet_in_frame,
            last_packet_in_frame: params.marker,
            frame_dependencies: Some(Arc::new(dd::FrameDependencyTemplate {
                temporal_id: dd_params.temporal_id,
                decode_target_indications: dtis,
                frame_diffs: dd_params.frame_diffs.clone(),
                ..Default::default()
            })),
            ..Default::default()
        }),
        decode_targets: dd_params.decode_targets.clone(),
        ext_frame_num: dd_params.ext_frame_num,
        // all decode targets active, as the parser reports after a structure
        active_decode_targets_mask: (1u32
            .checked_shl(dd_params.decode_targets.len() as u32)
            .unwrap_or(0))
        .wrapping_sub(1),
        ..Default::default()
    });
    Ok(ep)
}

// TestVP8DecodeTargets are the decode targets of libwebrtc's three layer VP8 structure,
// sorted from high to low.
pub const TEST_VP8_DECODE_TARGETS: [buffer::DependencyDescriptorDecodeTarget; 3] = [
    buffer::DependencyDescriptorDecodeTarget {
        target: 2,
        layer: buffer::VideoLayer {
            spatial: 0,
            temporal: 2,
        },
    },
    buffer::DependencyDescriptorDecodeTarget {
        target: 1,
        layer: buffer::VideoLayer {
            spatial: 0,
            temporal: 1,
        },
    },
    buffer::DependencyDescriptorDecodeTarget {
        target: 0,
        layer: buffer::VideoLayer {
            spatial: 0,
            temporal: 0,
        },
    },
];

// --------------------------------------

pub fn test_vp8_codec() -> webrtc::RtpCodecCapability {
    webrtc::RtpCodecCapability {
        mime_type: "video/vp8".to_string(),
        clock_rate: 90000,
        ..Default::default()
    }
}

pub fn test_opus_codec() -> webrtc::RtpCodecCapability {
    webrtc::RtpCodecCapability {
        mime_type: "audio/opus".to_string(),
        clock_rate: 48000,
        ..Default::default()
    }
}

// --------------------------------------
