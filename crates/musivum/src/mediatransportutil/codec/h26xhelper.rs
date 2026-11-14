// Ported from LiveKit's mediatransportutil, pkg/codec/h26xhelper.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are variants of the package's `Error`
// (vp8.rs); `BitReader` borrows its data; Go's `uint` is `u64` and the number of bits to read
// is a `usize`, so the original's check for a negative number is not there.

use super::{Error, VideoSize};

// SPSInfo holds parsed SPS parameters
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpsInfo {
    pub chroma_format_idc: u64,
    pub pic_width_in_luma_samples: u64,
    pub pic_height_in_luma_samples: u64,
    pub conformance_window_flag: bool,
    pub conf_win_left_offset: u64,
    pub conf_win_right_offset: u64,
    pub conf_win_top_offset: u64,
    pub conf_win_bottom_offset: u64,
    // Raw coded resolution
    pub coded_width: u64,
    pub coded_height: u64,
    // Resolution after conformance window cropping
    pub display_width: u64,
    pub display_height: u64,
}

// -------- BitReader --------
#[derive(Clone, Debug)]
pub struct BitReader<'a> {
    data: &'a [u8],
    pos: usize, // bit position
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn left(&self) -> usize {
        self.data.len() * 8 - self.pos
    }

    pub fn read_bits(&mut self, n: usize) -> Result<u64, Error> {
        // n < 0 || br.left() < n
        if self.left() < n {
            return Err(Error::NotEnoughBits);
        }
        let mut v: u64 = 0;
        for _ in 0..n {
            let byte_pos = self.pos / 8;
            let bit_pos = 7 - (self.pos % 8);
            let bit = (self.data[byte_pos] >> bit_pos) & 1;
            v = (v << 1) | bit as u64;
            self.pos += 1;
        }
        Ok(v)
    }

    pub fn read_flag(&mut self) -> Result<bool, Error> {
        let b = self.read_bits(1)?;
        Ok(b == 1)
    }

    pub fn read_ue(&mut self) -> Result<u64, Error> {
        // Unsigned Exp-Golomb
        let mut zeros: usize = 0;
        loop {
            let bit = self.read_bits(1)?;
            if bit == 0 {
                zeros += 1;
                continue;
            }
            break; // hit the stop bit '1'
        }
        if zeros == 0 {
            return Ok(0);
        }
        let info = self.read_bits(zeros)?;
        // (1<<zeros - 1) + info, where a shift by 64 or more gives zero in Go
        let one_shifted = u32::try_from(zeros)
            .ok()
            .and_then(|zeros| 1u64.checked_shl(zeros))
            .unwrap_or(0);
        Ok(one_shifted.wrapping_sub(1).wrapping_add(info))
    }

    pub fn read_se(&mut self) -> Result<i64, Error> {
        let ue_val = self.read_ue()?;
        let k = ue_val as i64;
        let val = if k % 2 == 0 {
            (k / 2).wrapping_neg()
        } else {
            k.wrapping_add(1) / 2
        };
        Ok(val)
    }
}

// ------------------------- H265 -------------------------

// stripStartCode removes 00 00 01 or 00 00 00 01 if present.
pub(super) fn strip_start_code(b: &[u8]) -> &[u8] {
    if b.len() >= 4 && b[0] == 0x00 && b[1] == 0x00 && b[2] == 0x00 && b[3] == 0x01 {
        return &b[4..];
    }
    if b.len() >= 3 && b[0] == 0x00 && b[1] == 0x00 && b[2] == 0x01 {
        return &b[3..];
    }
    b
}

// removeEmulationPreventionBytes removes 0x03 after 0x0000
pub(super) fn remove_emulation_prevention_bytes(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        if i > 1 && data[i] == 0x03 && data[i - 1] == 0x00 && data[i - 2] == 0x00 {
            continue;
        }
        out.push(data[i]);
    }
    out
}

// parseH265SPS parses a full H.265 SPS NAL unit
pub(super) fn parse_h265_sps(nal: &[u8]) -> Result<SpsInfo, Error> {
    // Optional start code
    let nal = strip_start_code(nal);

    // Remove emulation prevention bytes across the NAL
    let rbsp = remove_emulation_prevention_bytes(nal);

    let mut br = BitReader::new(&rbsp);

    // ---- NAL header (16 bits): forbidden_zero_bit(1), nal_unit_type(6), nuh_layer_id(6), nuh_temporal_id_plus1(3)
    br.read_bits(1)?; // forbidden_zero_bit
    let nal_unit_type = br.read_bits(6)?;
    br.read_bits(6)?; // nuh_layer_id
    br.read_bits(3)?; // nuh_temporal_id_plus1
    // 33 = SPS
    if nal_unit_type != 33 {
        return Err(Error::NotAnHevcSpsNal(nal_unit_type));
    }

    // ---- sps_video_parameter_set_id u(4), sps_max_sub_layers_minus1 u(3), sps_temporal_id_nesting_flag u(1)
    br.read_bits(4)?;
    let max_sub_layers_minus1 = br.read_bits(3)? as usize;
    br.read_bits(1)?;

    // ---- profile_tier_level(1, max_sub_layers_minus1)
    // general_profile_space u(2), general_tier_flag u(1), general_profile_idc u(5)
    br.read_bits(2 + 1 + 5)?;
    // general_profile_compatibility_flags u(32)
    br.read_bits(32)?;
    // general_constraint_indicator_flags u(48)
    br.read_bits(16)?;
    br.read_bits(32)?;
    // general_level_idc u(8)
    br.read_bits(8)?;

    let mut sub_layer_profile_present_flag = vec![false; max_sub_layers_minus1];
    let mut sub_layer_level_present_flag = vec![false; max_sub_layers_minus1];
    for i in 0..max_sub_layers_minus1 {
        let f1 = br.read_flag()?;
        let f2 = br.read_flag()?;
        sub_layer_profile_present_flag[i] = f1;
        sub_layer_level_present_flag[i] = f2;
    }
    if max_sub_layers_minus1 > 0 {
        // reserved_zero_2bits for i = maxSubLayersMinus1 .. 7
        for _ in max_sub_layers_minus1..8 {
            br.read_bits(2)?;
        }
    }
    for i in 0..max_sub_layers_minus1 {
        if sub_layer_profile_present_flag[i] {
            br.read_bits(2 + 1 + 5)?;
            br.read_bits(32)?;
            br.read_bits(48)?;
        }
        if sub_layer_level_present_flag[i] {
            br.read_bits(8)?;
        }
    }

    // ---- Now the core SPS fields we need
    br.read_ue()?; // sps_seq_parameter_set_id

    let chroma_format_idc = br.read_ue()?;
    if chroma_format_idc == 3 {
        // separate_colour_plane_flag u(1)
        br.read_flag()?;
    }

    let pic_w = br.read_ue()?; // pic_width_in_luma_samples
    let pic_h = br.read_ue()?; // pic_height_in_luma_samples

    let conf_flag = br.read_flag()?;
    let (mut l, mut r, mut t, mut b) = (0u64, 0u64, 0u64, 0u64);
    if conf_flag {
        l = br.read_ue()?;
        r = br.read_ue()?;
        t = br.read_ue()?;
        b = br.read_ue()?;
    }

    // crop unit size depends on chroma_format_idc
    let (sub_width_c, sub_height_c) = (
        get_sub_width_c(chroma_format_idc),
        get_sub_height_c(chroma_format_idc),
    );

    let mut info = SpsInfo {
        chroma_format_idc,
        pic_width_in_luma_samples: pic_w,
        pic_height_in_luma_samples: pic_h,
        conformance_window_flag: conf_flag,
        conf_win_left_offset: l,
        conf_win_right_offset: r,
        conf_win_top_offset: t,
        conf_win_bottom_offset: b,
        coded_width: pic_w,
        coded_height: pic_h,
        ..Default::default()
    };

    if conf_flag {
        // int(picW) - int(l+r)*int(subWidthC)
        let mut w = (pic_w as i64)
            .wrapping_sub((l.wrapping_add(r) as i64).wrapping_mul(sub_width_c as i64));
        let mut h = (pic_h as i64)
            .wrapping_sub((t.wrapping_add(b) as i64).wrapping_mul(sub_height_c as i64));
        if w < 0 {
            w = 0;
        }
        if h < 0 {
            h = 0;
        }
        info.display_width = w as u64;
        info.display_height = h as u64;
    } else {
        info.display_width = pic_w;
        info.display_height = pic_h;
    }

    Ok(info)
}

fn get_sub_width_c(chroma_format_idc: u64) -> u64 {
    if chroma_format_idc == 1 || chroma_format_idc == 2 {
        return 2;
    }
    1
}

fn get_sub_height_c(chroma_format_idc: u64) -> u64 {
    if chroma_format_idc == 1 {
        return 2;
    }
    1
}

pub fn extract_h265_video_size(payload: &[u8]) -> VideoSize {
    if payload.len() < 2 {
        return VideoSize::default();
    }
    let nal_type = (payload[0] >> 1) & 0x3F;

    let mut sps_nalu: &[u8] = &[];
    match nal_type {
        33 => {
            // SPS
            sps_nalu = payload;
        }
        48 => {
            // Aggregation Packet (AP)
            // skip 2-byte header
            let mut i = 2;
            while i + 2 <= payload.len() {
                let nal_size = ((payload[i] as usize) << 8) | payload[i + 1] as usize;
                i += 2;
                if i + nal_size > payload.len() {
                    break;
                }
                if nal_size == 0 {
                    continue;
                }
                let nal_unit = &payload[i..i + nal_size];
                let nt = (nal_unit[0] >> 1) & 0x3F;
                if nt == 33 {
                    sps_nalu = nal_unit;
                    break;
                }
                i += nal_size;
            }
        }
        _ => {}
    }

    if !sps_nalu.is_empty() {
        let info = match parse_h265_sps(sps_nalu) {
            Ok(info) => info,
            Err(_) => return VideoSize::default(),
        };
        return VideoSize {
            width: info.display_width as u32,
            height: info.display_height as u32,
        };
    }

    VideoSize::default()
}

// ------------------------- H264 -------------------------

// parseH264SPS parses a full H.264 SPS NAL unit into SPSInfo
pub(super) fn parse_h264_sps(nal: &[u8]) -> Result<SpsInfo, Error> {
    if nal.is_empty() {
        return Err(Error::EmptySpsNal);
    }
    let nal = strip_start_code(nal);
    if nal.is_empty() {
        return Err(Error::EmptySpsNalAfterStrippingStartCode);
    }
    let nal_type = nal[0] & 0x1F;
    if nal_type != 7 {
        return Err(Error::NotAnSpsNal(nal_type));
    }

    let rbsp = remove_emulation_prevention_bytes(&nal[1..]); // skip NAL header
    let mut br = BitReader::new(&rbsp);

    // The original does not look at the errors of the reads from here on: a read that fails
    // gives zero (false for a flag).
    let profile_idc = br.read_bits(8).unwrap_or(0);
    let _ = br.read_bits(8); // constraint flags
    let _ = br.read_bits(8); // level_idc
    let _ = br.read_ue(); // seq_parameter_set_id

    let mut chroma_format_idc = 1u64;
    if profile_idc == 100
        || profile_idc == 110
        || profile_idc == 122
        || profile_idc == 244
        || profile_idc == 44
        || profile_idc == 83
        || profile_idc == 86
        || profile_idc == 118
        || profile_idc == 128
    {
        chroma_format_idc = br.read_ue().unwrap_or(0);
        if chroma_format_idc == 3 {
            let _ = br.read_flag(); // separate_colour_plane_flag
        }
        let _ = br.read_ue(); // bit_depth_luma_minus8
        let _ = br.read_ue(); // bit_depth_chroma_minus8
        let _ = br.read_flag(); // qpprime_y_zero_transform_bypass_flag
        if br.read_flag().unwrap_or(false) {
            // seq_scaling_matrix_present_flag
            for _ in 0..8 {
                let _ = br.read_flag();
            }
        }
    }

    let _ = br.read_ue(); // log2_max_frame_num_minus4
    let poc_type = br.read_ue().unwrap_or(0);
    match poc_type {
        0 => {
            let _ = br.read_ue();
        }
        1 => {
            let _ = br.read_flag();
            let _ = br.read_se();
            let _ = br.read_se();
            let cnt = br.read_ue().unwrap_or(0);
            if cnt > 255 {
                return Err(Error::SpsNumRefFramesInPicOrderCntCycleTooLarge);
            }
            for _ in 0..cnt {
                let _ = br.read_se();
            }
        }
        _ => {}
    }

    let _ = br.read_ue(); // max_num_ref_frames
    let _ = br.read_flag(); // gaps_in_frame_num_value_allowed_flag

    let w_mbs = br.read_ue().unwrap_or(0);
    let h_map_units = br.read_ue().unwrap_or(0);
    let frame_mbs_only = br.read_flag().unwrap_or(false);
    if !frame_mbs_only {
        let _ = br.read_flag(); // mb_adaptive_frame_field_flag
    }
    let _ = br.read_flag(); // direct_8x8_inference_flag

    let (mut crop_left, mut crop_right, mut crop_top, mut crop_bottom) = (0u64, 0u64, 0u64, 0u64);
    if br.read_flag().unwrap_or(false) {
        // frameCropping
        crop_left = br.read_ue().unwrap_or(0);
        crop_right = br.read_ue().unwrap_or(0);
        crop_top = br.read_ue().unwrap_or(0);
        crop_bottom = br.read_ue().unwrap_or(0);
    }

    let width = w_mbs.wrapping_add(1).wrapping_mul(16);
    let mut height = h_map_units.wrapping_add(1).wrapping_mul(16);
    if !frame_mbs_only {
        height = height.wrapping_mul(2);
    }

    let sub_width_c = get_sub_width_c(chroma_format_idc);
    let sub_height_c = get_sub_height_c(chroma_format_idc);
    let mut crop_unit_x = sub_width_c;
    let mut crop_unit_y = sub_height_c;
    if chroma_format_idc == 0 {
        crop_unit_x = 1;
        if !frame_mbs_only {
            crop_unit_y = 2;
        } else {
            crop_unit_y = 1;
        }
    } else if !frame_mbs_only {
        crop_unit_y = crop_unit_y.wrapping_mul(2);
    }

    let info = SpsInfo {
        chroma_format_idc,
        pic_width_in_luma_samples: width,
        pic_height_in_luma_samples: height,
        conformance_window_flag: crop_left
            .wrapping_add(crop_right)
            .wrapping_add(crop_top)
            .wrapping_add(crop_bottom)
            > 0,
        conf_win_left_offset: crop_left,
        conf_win_right_offset: crop_right,
        conf_win_top_offset: crop_top,
        conf_win_bottom_offset: crop_bottom,
        coded_width: width,
        coded_height: height,
        display_width: width
            .wrapping_sub(crop_left.wrapping_add(crop_right).wrapping_mul(crop_unit_x)),
        display_height: height
            .wrapping_sub(crop_top.wrapping_add(crop_bottom).wrapping_mul(crop_unit_y)),
    };

    Ok(info)
}

// ExtractH264VideoSize extracts resolution from H.264 RTP payload
pub fn extract_h264_video_size(payload: &[u8]) -> VideoSize {
    if payload.is_empty() {
        return VideoSize::default();
    }

    let parse_nal = |nal: &[u8]| -> VideoSize {
        let info = match parse_h264_sps(nal) {
            Ok(info) => info,
            Err(_) => return VideoSize::default(),
        };
        VideoSize {
            width: info.display_width as u32,
            height: info.display_height as u32,
        }
    };

    let nal_type = payload[0] & 0x1F;

    match nal_type {
        7 => {
            // SPS NAL
            parse_nal(payload)
        }

        28 => {
            // FU-A
            if payload.len() < 2 {
                return VideoSize::default();
            }
            let start = (payload[1] & 0x80) != 0;
            if !start {
                return VideoSize::default();
            }
            let nal_header = (payload[0] & 0xE0) | (payload[1] & 0x1F);
            let mut sps = vec![nal_header];
            sps.extend_from_slice(&payload[2..]);
            parse_nal(&sps)
        }

        24..=27 => {
            // STAP-A/B, MTAP16, MTAP24
            let mut offset = 1;
            match nal_type {
                25 => {
                    // STAP-B has 16-bit DON
                    offset += 2;
                }
                26 => {
                    // MTAP16
                    offset += 3;
                }
                27 => {
                    // MTAP24
                    offset += 4;
                }
                _ => {}
            }

            while offset + 2 <= payload.len() {
                let nalu_size = ((payload[offset] as usize) << 8) | payload[offset + 1] as usize;
                offset += 2;
                if offset + nalu_size > payload.len() {
                    break;
                }
                if nalu_size == 0 {
                    continue;
                }
                let nalu = &payload[offset..offset + nalu_size];
                if nalu[0] & 0x1F == 7 {
                    // SPS
                    return parse_nal(nalu);
                }
                offset += nalu_size;
            }
            VideoSize::default()
        }

        _ => VideoSize::default(),
    }
}

#[cfg(test)]
#[path = "h26xhelper_test.rs"]
mod h26xhelper_test;
