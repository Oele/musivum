// Ported from LiveKit, pkg/sfu/codecmunger/vp8.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the ordered maps of the original
// (github.com/elliotchance/orderedmap/v3) are the `OrderedMap` of `orderedmap.rs`, whose
// `delete_front` is the original's `Front()` and `Delete(el.Key)`; `NewVP8FromOther` finds
// the type of the other munger through `as_any`; the methods of the interface are in the
// block that implements the trait; with an error `update_and_get` returns no sizes and no
// header; the size of a header is unsigned (`codec::Vp8`'s `header_size`), so the size of
// the munged header is added up signed and converted.

use std::any::Any;

use crate::mediatransportutil::codec;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;

use super::orderedmap::OrderedMap;
use super::{CodecMunger, Error, MAX_HEADER_SIZE, Null, State};

const MISSING_PICTURE_IDS_THRESHOLD: usize = 50;
const DROPPED_PICTURE_IDS_THRESHOLD: usize = 20;
const EXEMPTED_PICTURE_IDS_THRESHOLD: usize = 20;

// -----------------------------------------------------------

pub struct Vp8 {
    #[allow(dead_code)] // not read upstream either
    logger: Logger,

    picture_id_wrap_handler: Vp8PictureIdWrapHandler,
    ext_last_picture_id: i32,
    picture_id_offset: i32,
    picture_id_used: bool,
    last_tl0_pic_idx: u8,
    tl0_pic_idx_offset: u8,
    tl0_pic_idx_used: bool,
    tid_used: bool,
    last_key_idx: u8,
    key_idx_offset: u8,
    key_idx_used: bool,

    missing_picture_ids: OrderedMap<i32, i32>,
    dropped_picture_ids: OrderedMap<i32, bool>,
    exempted_picture_ids: OrderedMap<i32, bool>,
}

impl Vp8 {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            picture_id_wrap_handler: Vp8PictureIdWrapHandler::default(),
            ext_last_picture_id: 0,
            picture_id_offset: 0,
            picture_id_used: false,
            last_tl0_pic_idx: 0,
            tl0_pic_idx_offset: 0,
            tl0_pic_idx_used: false,
            tid_used: false,
            last_key_idx: 0,
            key_idx_offset: 0,
            key_idx_used: false,
            missing_picture_ids: OrderedMap::new(),
            dropped_picture_ids: OrderedMap::new(),
            exempted_picture_ids: OrderedMap::new(),
        }
    }

    pub fn new_from_other(cm: &dyn CodecMunger, logger: Logger) -> Self {
        let mut v = Self::new(logger);
        let cm = cm.as_any();
        if let Some(cm) = cm.downcast_ref::<Null>() {
            v.seed_state(cm.get_seeded_state());
        } else if let Some(cm) = cm.downcast_ref::<Vp8>() {
            v.seed_state(cm.get_state());
        }
        v
    }
}

impl CodecMunger for Vp8 {
    fn get_state(&self) -> Option<State> {
        Some(State::Vp8MungerState(livekit::Vp8MungerState {
            ext_last_picture_id: self.ext_last_picture_id,
            picture_id_used: self.picture_id_used,
            last_tl0_pic_idx: self.last_tl0_pic_idx as u32,
            tl0_pic_idx_used: self.tl0_pic_idx_used,
            tid_used: self.tid_used,
            last_key_idx: self.last_key_idx as u32,
            key_idx_used: self.key_idx_used,
        }))
    }

    fn seed_state(&mut self, seed: Option<State>) {
        let state = match seed {
            Some(State::CodecMunger(livekit::rtp_forwarder_state::CodecMunger::Vp8Munger(cm))) => {
                Some(cm)
            }
            Some(State::Vp8MungerState(cm)) => Some(cm),
            None => None,
        };
        if let Some(state) = state {
            self.ext_last_picture_id = state.ext_last_picture_id;
            self.picture_id_used = state.picture_id_used;
            self.last_tl0_pic_idx = state.last_tl0_pic_idx as u8;
            self.tl0_pic_idx_used = state.tl0_pic_idx_used;
            self.tid_used = state.tid_used;
            self.last_key_idx = state.last_key_idx as u8;
            self.key_idx_used = state.key_idx_used;
        }
    }

    fn set_last(&mut self, ext_pkt: &buffer::ExtPacket) {
        let Some(vp8) = &ext_pkt.vp8 else {
            return;
        };

        self.picture_id_used = vp8.i;
        if self.picture_id_used {
            self.picture_id_wrap_handler
                .init(vp8.picture_id as i32 - 1, vp8.m);
            self.ext_last_picture_id = vp8.picture_id as i32;
        }

        self.tl0_pic_idx_used = vp8.l;
        if self.tl0_pic_idx_used {
            self.last_tl0_pic_idx = vp8.tl0picidx;
        }

        self.tid_used = vp8.t;

        self.key_idx_used = vp8.k;
        if self.key_idx_used {
            self.last_key_idx = vp8.keyidx;
        }
    }

    fn update_offsets(&mut self, ext_pkt: &buffer::ExtPacket) {
        let Some(vp8) = &ext_pkt.vp8 else {
            return;
        };

        if self.picture_id_used {
            self.picture_id_wrap_handler
                .init(vp8.picture_id as i32 - 1, vp8.m);
            self.picture_id_offset = (vp8.picture_id as i32)
                .wrapping_sub(self.ext_last_picture_id)
                .wrapping_sub(1);
        }

        if self.tl0_pic_idx_used {
            self.tl0_pic_idx_offset = vp8
                .tl0picidx
                .wrapping_sub(self.last_tl0_pic_idx)
                .wrapping_sub(1);
        }

        if self.key_idx_used {
            self.key_idx_offset = vp8.keyidx.wrapping_sub(self.last_key_idx).wrapping_sub(1) & 0x1f;
        }

        // clear picture id caches on layer switch
        self.missing_picture_ids = OrderedMap::new();
        self.dropped_picture_ids = OrderedMap::new();
        self.exempted_picture_ids = OrderedMap::new();
    }

    fn update_and_get(
        &mut self,
        ext_pkt: &buffer::ExtPacket,
        sn_out_of_order: bool,
        sn_has_gap: bool,
        max_temporal_layer: i32,
    ) -> Result<(usize, [u8; MAX_HEADER_SIZE], usize), Error> {
        let mut hdr = [0u8; MAX_HEADER_SIZE];
        let Some(vp8) = &ext_pkt.vp8 else {
            return Err(Error::NotVp8);
        };

        let ext_picture_id = self.picture_id_wrap_handler.unwrap(vp8.picture_id, vp8.m);

        // if out-of-order, look up missing picture id cache
        if sn_out_of_order {
            let (picture_id_offset, ok) = self.missing_picture_ids.get(ext_picture_id);
            if !ok {
                return Err(Error::OutOfOrderVp8PictureIdCacheMiss);
            }

            // the out-of-order picture id cannot be deleted from the cache
            // as there could more than one packet in a picture and more
            // than one packet of a picture could come out-of-order.
            // To prevent picture id cache from growing, it is truncated
            // when it reaches a certain size.

            let munged_picture_id =
                (ext_picture_id.wrapping_sub(picture_id_offset) & 0x7fff) as u16;
            let vp8_packet = codec::Vp8 {
                first_byte: vp8.first_byte,
                i: vp8.i,
                m: munged_picture_id > 127,
                picture_id: munged_picture_id,
                l: vp8.l,
                tl0picidx: vp8.tl0picidx.wrapping_sub(self.tl0_pic_idx_offset),
                t: vp8.t,
                tid: vp8.tid,
                y: vp8.y,
                k: vp8.k,
                keyidx: vp8.keyidx.wrapping_sub(self.key_idx_offset),
                is_key_frame: vp8.is_key_frame,
                header_size: (vp8.header_size as i64).wrapping_add(codec::vpx_picture_id_size_diff(
                    munged_picture_id > 127,
                    vp8.m,
                )) as usize,
                ..Default::default()
            };
            let n = vp8_packet.marshal_to(&mut hdr)?;
            return Ok((vp8.header_size, hdr, n));
        }

        let prev_max_picture_id = self.picture_id_wrap_handler.max_picture_id();
        self.picture_id_wrap_handler
            .update_max_picture_id(ext_picture_id, vp8.m);

        // if there is a gap in sequence number, record possible pictures that
        // the missing packets can belong to in missing picture id cache.
        // The missing picture cache should contain the previous picture id
        // and the current picture id and all the intervening pictures.
        // This is to handle a scenario as follows
        //   o Packet 10 -> Picture ID 10
        //   o Packet 11 -> missing
        //   o Packet 12 -> Picture ID 11
        // In this case, Packet 11 could belong to either Picture ID 10 (last packet of that picture)
        // or Picture ID 11 (first packet of the current picture). Although in this simple case,
        // it is possible to deduce that (for example by looking at previous packet's RTP marker
        // and check if that was the last packet of Picture 10), it could get complicated when
        // the gap is larger.
        if sn_has_gap {
            let mut lost_picture_id = prev_max_picture_id;
            while lost_picture_id <= ext_picture_id {
                // Record missing only if picture id was not dropped. This is to avoid a subsequent packet of dropped frame going through.
                // A sequence like this
                //   o Packet 10 - Picture 11 - TID that should be dropped
                //   o Packet 11 - missing - belongs to Picture 11 still
                //   o Packet 12 - Picture 12 - will be reported as GAP, so missing picture id mapping will be set up for Picture 11 also.
                //   o Next packet - Packet 11 - this will use the wrong offset from missing pictures cache
                let (_, ok) = self.dropped_picture_ids.get(lost_picture_id);
                if !ok {
                    self.missing_picture_ids
                        .set(lost_picture_id, self.picture_id_offset);
                }
                lost_picture_id = lost_picture_id.wrapping_add(1);
            }

            // trim cache if necessary
            while self.missing_picture_ids.len() > MISSING_PICTURE_IDS_THRESHOLD {
                self.missing_picture_ids.delete_front();
            }

            // if there is a gap, packet is forwarded irrespective of temporal layer as it cannot be determined
            // which layer the missing packets belong to. A layer could have multiple packets. So, keep track
            // of pictures that are forwarded even though they will be filtered out based on temporal layer
            // requirements. That allows forwarding of the complete picture.
            if ext_pkt.video_layer.temporal > max_temporal_layer {
                self.exempted_picture_ids.set(ext_picture_id, true);
                // trim cache if necessary
                while self.exempted_picture_ids.len() > EXEMPTED_PICTURE_IDS_THRESHOLD {
                    self.exempted_picture_ids.delete_front();
                }
            }
        } else {
            if ext_pkt.video_layer.temporal > max_temporal_layer {
                // drop only if not exempted
                let (_, ok) = self.exempted_picture_ids.get(ext_picture_id);
                if !ok {
                    // adjust only once per picture as a picture could have multiple packets
                    if vp8.i && prev_max_picture_id != ext_picture_id {
                        // keep track of dropped picture ids so that they do not get into the missing picture cache
                        self.dropped_picture_ids.set(ext_picture_id, true);
                        // trim cache if necessary
                        while self.dropped_picture_ids.len() > DROPPED_PICTURE_IDS_THRESHOLD {
                            self.dropped_picture_ids.delete_front();
                        }

                        self.picture_id_offset = self.picture_id_offset.wrapping_add(1);
                    }
                    return Err(Error::FilteredTemporalLayer);
                }
            }
        }

        // in-order incoming sequence number, may or may not be contiguous.
        // In the case of loss (i.e. incoming sequence number is not contiguous),
        // forward even if it is a filtered layer. With temporal scalability,
        // it is unclear if the current packet should be dropped if it is not
        // contiguous. Hence, forward anything that is not contiguous.
        // Reference: http://www.rtcbits.com/2017/04/howto-implement-temporal-scalability.html
        let ext_munged_picture_id = ext_picture_id.wrapping_sub(self.picture_id_offset);
        let munged_picture_id = (ext_munged_picture_id & 0x7fff) as u16;
        let munged_tl0_pic_idx = vp8.tl0picidx.wrapping_sub(self.tl0_pic_idx_offset);
        let munged_key_idx = vp8.keyidx.wrapping_sub(self.key_idx_offset) & 0x1f;

        self.ext_last_picture_id = ext_munged_picture_id;
        self.last_tl0_pic_idx = munged_tl0_pic_idx;
        self.last_key_idx = munged_key_idx;

        let vp8_packet = codec::Vp8 {
            first_byte: vp8.first_byte,
            i: vp8.i,
            m: munged_picture_id > 127,
            picture_id: munged_picture_id,
            l: vp8.l,
            tl0picidx: munged_tl0_pic_idx,
            t: vp8.t,
            tid: vp8.tid,
            y: vp8.y,
            k: vp8.k,
            keyidx: munged_key_idx,
            is_key_frame: vp8.is_key_frame,
            header_size: (vp8.header_size as i64).wrapping_add(codec::vpx_picture_id_size_diff(
                munged_picture_id > 127,
                vp8.m,
            )) as usize,
            ..Default::default()
        };
        let n = vp8_packet.marshal_to(&mut hdr)?;
        Ok((vp8.header_size, hdr, n))
    }

    fn update_and_get_padding(&mut self, new_picture: bool) -> Result<Vec<u8>, Error> {
        let mut offset = 0i32;
        if new_picture {
            offset = 1;
        }

        let mut header_size = 1usize;
        if self.picture_id_used || self.tl0_pic_idx_used || self.tid_used || self.key_idx_used {
            header_size += 1;
        }

        let mut ext_picture_id = self.ext_last_picture_id;
        if self.picture_id_used {
            ext_picture_id = self.ext_last_picture_id.wrapping_add(offset);
            self.ext_last_picture_id = ext_picture_id;
            self.picture_id_offset = self.picture_id_offset.wrapping_sub(offset);
            if (ext_picture_id & 0x7fff) > 127 {
                header_size += 2;
            } else {
                header_size += 1;
            }
        }
        let picture_id = (ext_picture_id & 0x7fff) as u16;

        let mut tl0_pic_idx = 0u8;
        if self.tl0_pic_idx_used {
            tl0_pic_idx = self.last_tl0_pic_idx.wrapping_add(offset as u8);
            self.last_tl0_pic_idx = tl0_pic_idx;
            self.tl0_pic_idx_offset = self.tl0_pic_idx_offset.wrapping_sub(offset as u8);
            header_size += 1;
        }

        if self.tid_used || self.key_idx_used {
            header_size += 1;
        }

        let mut key_idx = 0u8;
        if self.key_idx_used {
            key_idx = self.last_key_idx.wrapping_add(offset as u8) & 0x1f;
            self.last_key_idx = key_idx;
            self.key_idx_offset = self.key_idx_offset.wrapping_sub(offset as u8);
        }

        let vp8_packet = codec::Vp8 {
            first_byte: 0x10, // partition 0, start of VP8 Partition, reference frame
            i: self.picture_id_used,
            m: picture_id > 127,
            picture_id,
            l: self.tl0_pic_idx_used,
            tl0picidx: tl0_pic_idx,
            t: self.tid_used,
            tid: 0,
            y: true,
            k: self.key_idx_used,
            keyidx: key_idx,
            is_key_frame: true,
            header_size,
            ..Default::default()
        };
        Ok(vp8_packet.marshal()?)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Vp8 {
    // for testing only
    pub fn picture_id_offset(&self, ext_picture_id: i32) -> (i32, bool) {
        self.missing_picture_ids.get(ext_picture_id)
    }
}

// -----------------------------

// VP8PictureIdWrapHandler
fn is_wrapping_7bit(val1: i32, val2: i32) -> bool {
    val2 < val1 && val1.wrapping_sub(val2) > (1 << 6)
}

fn is_wrapping_15bit(val1: i32, val2: i32) -> bool {
    val2 < val1 && val1.wrapping_sub(val2) > (1 << 14)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Vp8PictureIdWrapHandler {
    max_picture_id: i32,
    max_m_bit: bool,
    total_wrap: i32,
    last_wrap: i32,
}

impl Vp8PictureIdWrapHandler {
    pub fn init(&mut self, ext_picture_id: i32, m_bit: bool) {
        self.max_picture_id = ext_picture_id;
        self.max_m_bit = m_bit;
        self.total_wrap = 0;
        self.last_wrap = 0;
    }

    pub fn max_picture_id(&self) -> i32 {
        self.max_picture_id
    }

    // unwrap picture id and update the maxPictureId. return unwrapped value
    #[allow(clippy::collapsible_if)] // the conditions as the original has them
    pub fn unwrap(&mut self, picture_id: u16, m_bit: bool) -> i32 {
        //
        // VP8 Picture ID is specified very flexibly.
        //
        // Reference: https://datatracker.ietf.org/doc/html/draft-ietf-payload-vp8
        //
        // Quoting from the RFC
        // ----------------------------
        // PictureID:  7 or 15 bits (shown left and right, respectively, in
        //    Figure 2) not including the M bit.  This is a running index of
        //    the frames, which MAY start at a random value, MUST increase by
        //    1 for each subsequent frame, and MUST wrap to 0 after reaching
        //    the maximum ID (all bits set).  The 7 or 15 bits of the
        //    PictureID go from most significant to least significant,
        //    beginning with the first bit after the M bit.  The sender
        //    chooses a 7 or 15 bit index and sets the M bit accordingly.
        //    The receiver MUST NOT assume that the number of bits in
        //    PictureID stay the same through the session.  Having sent a
        //    7-bit PictureID with all bits set to 1, the sender may either
        //    wrap the PictureID to 0, or extend to 15 bits and continue
        //    incrementing
        // ----------------------------
        //
        // While in practice, senders may not switch between modes indiscriminately,
        // it is possible that small picture ids are sent in 7 bits and then switch
        // to 15 bits. But, to ensure correctness, this code keeps track of how much
        // quantity has wrapped and uses that to figure out if the incoming picture id
        // is newer OR out-of-order.
        //
        let mut max_picture_id = self.max_picture_id;
        // maxPictureId can be -1 at the start
        if max_picture_id > 0 {
            if self.max_m_bit {
                max_picture_id = self.max_picture_id & 0x7fff;
            } else {
                max_picture_id = self.max_picture_id & 0x7f;
            }
        }

        let mut new_picture_id: i32;
        if m_bit {
            new_picture_id = (picture_id & 0x7fff) as i32;
        } else {
            new_picture_id = (picture_id & 0x7f) as i32;
        }

        //
        // if the new picture id is too far ahead of max, i.e. more than half of last wrap,
        // it is out-of-order, unwrap backwards
        //
        if self.total_wrap > 0 {
            if self.max_picture_id.wrapping_add(self.last_wrap >> 1)
                < new_picture_id.wrapping_add(self.total_wrap)
            {
                return new_picture_id
                    .wrapping_add(self.total_wrap)
                    .wrapping_sub(self.last_wrap);
            }
        }

        //
        // check for wrap around based on mode of previous picture id.
        // There are three cases here
        //   1. Wrapping from 15-bit -> 8-bit (32767 -> 0)
        //   2. Wrapping from 15-bit -> 15-bit (32767 -> 0)
        //   3. Wrapping from 8-bit -> 8-bit (127 -> 0)
        // In all cases, looking at the mode of previous picture id will
        // ensure that we are calculating the wrap properly.
        //
        let mut wrap = 0i32;
        if self.max_m_bit {
            if is_wrapping_15bit(max_picture_id, new_picture_id) {
                wrap = 1 << 15;
            }
        } else {
            if is_wrapping_7bit(max_picture_id, new_picture_id) {
                wrap = 1 << 7;
            }
        }

        self.total_wrap = self.total_wrap.wrapping_add(wrap);
        if wrap != 0 {
            self.last_wrap = wrap;
        }
        new_picture_id = new_picture_id.wrapping_add(self.total_wrap);

        new_picture_id
    }

    pub fn update_max_picture_id(&mut self, ext_picture_id: i32, m_bit: bool) {
        self.max_picture_id = ext_picture_id;
        self.max_m_bit = m_bit;
    }
}

#[cfg(test)]
#[path = "vp8_test.rs"]
mod vp8_test;
