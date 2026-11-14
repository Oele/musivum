// Ported from LiveKit, pkg/sfu/redreceiver.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The `TrackReceiver` that the original embeds is a
// field, and the methods of `impl TrackReceiver` that only hand a call on to it are what the
// embedding was. The original's methods are in the two trait impls: those of `RedTransformer`
// in the original's order, those of `TrackReceiver` in the trait's. `new` gives an `Arc` of
// the receiver itself where the original gives the interface, and the receiver keeps a `Weak`
// of itself, which is what `on_stream_restart` hands to the down tracks. The history, which
// `ForwardRTP` changes from the one goroutine that calls it, is behind a lock, since the
// method takes `&self`; the lock is not held while the packet is written to the down tracks.
// `pktBuff` holds slots of `histPkts` where the original holds pointers into it. A payload is
// a `Bytes`, which cannot be written over, so the history keeps a slice of it where the
// original copies it out of the read buffer into `histPayloads`. The RED payload is encoded
// into a buffer on the stack and copied into a `Bytes` for each packet, and the forwarded
// packet is a local, where the original reuses `redPayloadBuf`, `redExtPkt` and `redRtpPkt`.
// The error that `fmt.Errorf` makes is a variant of the package's `Error`.

use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::go::atomic;
use crate::mediatransportutil::bucket;
use crate::mediatransportutil::codec;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::receiver_base::close_track_senders;
use crate::sfu::utils as sfuutils;
use crate::sfu::{
    Bitrates, DebugInfo, Error, ReceiverCodecState, RedTransformer, TrackReceiver, TrackSender,
};
use crate::webrtc;
use crate::webrtc::rtp;
use crate::{debugw, errorw, infow};

const MAX_RED_COUNT: usize = 2;
const MTU_SIZE: usize = 1500;
const MAX_RED_PAYLOAD: usize = 1 << 10; // fit into 10 bits length field

// the RedReceiver is only for chrome / native webrtc now, we always negotiate opus payload to 111 with those clients,
// so it is safe to use a fixed payload 111 here for performance(avoid encoding red blocks for each downtrack that
// have a different opus payload type).
pub(super) const OPUS_PT: u8 = 111;
const OPUS_RED_PT: u8 = 63;

pub struct RedReceiver {
    // the `TrackReceiver` that the original embeds
    track_receiver: Arc<dyn TrackReceiver>,
    down_track_spreader: sfuutils::DownTrackSpreader<Arc<dyn TrackSender>>,
    logger: Logger,
    closed: atomic::Bool,

    // what `ForwardRTP` changes, upstream without a lock
    inner: Mutex<RedReceiverInner>,

    // the receiver itself, for the down tracks that are told of a restart
    this: Weak<RedReceiver>,
}

#[derive(Default)]
struct RedReceiverInner {
    // slots of hist_pkts
    pkt_buff: [Option<usize>; MAX_RED_COUNT],
    // the packets that pktBuff points at (copies upstream, as a forwarded packet's payload is in a read buffer
    // reused for the next packet there),
    // one more slot than pktBuff so that a new copy never overwrites a redundant block of the packet being encoded
    hist_pkts: [rtp::Packet; MAX_RED_COUNT + 1],
}

impl RedReceiver {
    pub fn new(
        receiver: Arc<dyn TrackReceiver>,
        dsp: sfuutils::DownTrackSpreaderParams,
    ) -> Arc<RedReceiver> {
        Arc::new_cyclic(|this| RedReceiver {
            track_receiver: receiver,
            logger: dsp.logger.clone(),
            down_track_spreader: sfuutils::DownTrackSpreader::new(dsp),
            closed: atomic::Bool::default(),
            inner: Mutex::new(RedReceiverInner::default()),
            this: this.clone(),
        })
    }
}

impl RedTransformer for RedReceiver {
    fn set_lb_threshold(&self, lb_threshold: usize) {
        self.down_track_spreader.set_threshold(lb_threshold);
    }

    fn forward_rtp(&self, pkt: &buffer::ExtPacket, spatial_layer: i32) -> i32 {
        // encode RED payload from primary payload and forward to downtracks
        if self.down_track_spreader.down_track_count() == 0 {
            return 0;
        }

        // fallback to primary codec if payload size exceeds redundant block length
        if pkt.packet.payload.len() >= MAX_RED_PAYLOAD {
            return sfuutils::broadcast_rtp(&self.down_track_spreader, pkt, spatial_layer);
        }

        let mut red_payload_buf = [0u8; MTU_SIZE];
        let encoded = self
            .inner
            .lock()
            .encode_red_for_primary(&pkt.packet, &mut red_payload_buf);
        let red_len = match encoded {
            Ok(red_len) => red_len,
            Err(err) => {
                errorw!(self.logger, "red encoding failed", err);
                return 0;
            }
        };

        let mut p_pkt = pkt.clone();
        p_pkt.packet.payload_type = OPUS_RED_PT;
        p_pkt.packet.payload = Bytes::copy_from_slice(&red_payload_buf[..red_len]);

        // not modify the ExtPacket.RawPacket here for performance since it is not used by the DownTrack,
        // otherwise it should be set to the correct value (marshal the primary rtp packet)
        sfuutils::broadcast_rtp(&self.down_track_spreader, &p_pkt, spatial_layer)
    }

    fn forward_rtcp_sender_report(
        &self,
        payload_type: webrtc::PayloadType,
        layer: i32,
        publisher_sr_data: &livekit::RtcpSenderReportState,
    ) {
        self.down_track_spreader.broadcast(|dt| {
            let _ = dt.handle_rtcp_sender_report_data(payload_type, layer, publisher_sr_data);
        });
    }

    fn resync_down_tracks(&self) {
        self.down_track_spreader.broadcast(|dt| {
            dt.resync();
        });
    }

    fn on_stream_restart(&self) {
        // The receiver is alive, as it is being called.
        let Some(r) = self.this.upgrade() else {
            return;
        };
        let r: Arc<dyn TrackReceiver> = r;
        self.down_track_spreader.broadcast(|dt| {
            dt.receiver_restart(r.clone());
        });
    }

    fn can_close(&self) -> bool {
        self.closed.load() || self.down_track_spreader.down_track_count() == 0
    }

    fn close(&self) {
        self.closed.store(true);
        close_track_senders(&self.down_track_spreader.close_and_get_down_tracks());
    }
}

impl TrackReceiver for RedReceiver {
    fn track_id(&self) -> livekit::TrackId {
        self.track_receiver.track_id()
    }

    fn stream_id(&self) -> String {
        self.track_receiver.stream_id()
    }

    fn codec(&self) -> webrtc::RtpCodecParameters {
        self.track_receiver.codec()
    }

    fn mime(&self) -> mime::MimeType {
        self.track_receiver.mime()
    }

    fn video_layer_mode(&self) -> livekit::video_layer::Mode {
        self.track_receiver.video_layer_mode()
    }

    fn header_extensions(&self) -> Vec<webrtc::RtpHeaderExtensionParameter> {
        self.track_receiver.header_extensions()
    }

    fn is_closed(&self) -> bool {
        self.closed.load()
    }

    fn read_rtp(&self, _buf: &mut [u8], _layer: u8, _esn: u64) -> Result<usize, Error> {
        // red encoding doesn't support nack
        Err(bucket::Error::PacketMismatch(String::new()).into())
    }

    fn get_layered_bitrate(&self) -> (Vec<i32>, Bitrates) {
        self.track_receiver.get_layered_bitrate()
    }

    fn get_audio_level(&self) -> (f64, bool) {
        self.track_receiver.get_audio_level()
    }

    fn send_pli(&self, layer: i32, force: bool) {
        self.track_receiver.send_pli(layer, force);
    }

    fn set_max_expected_spatial_layer(&self, layer: i32) {
        self.track_receiver.set_max_expected_spatial_layer(layer);
    }

    fn add_down_track(&self, track: Arc<dyn TrackSender>) -> Result<(), Error> {
        if self.closed.load() {
            return Err(Error::ReceiverClosed);
        }

        if self
            .down_track_spreader
            .has_down_track(&track.subscriber_id())
        {
            infow!(
                self.logger,
                "subscriberID already exists, replacing downtrack",
                "subscriberID" => track.subscriber_id()
            );
        }

        if !self.down_track_spreader.try_store(track.clone()) {
            return Err(Error::ReceiverClosed);
        }
        debugw!(
            self.logger,
            "red receiver downtrack added",
            "subscriberID" => track.subscriber_id()
        );
        Ok(())
    }

    fn delete_down_track(&self, subscriber_id: &livekit::ParticipantId) {
        if self.closed.load() {
            return;
        }

        self.down_track_spreader.free(subscriber_id);
        debugw!(
            self.logger,
            "red receiver downtrack deleted",
            "subscriberID" => subscriber_id
        );
    }

    fn get_down_tracks(&self) -> Vec<Arc<dyn TrackSender>> {
        self.down_track_spreader.get_down_tracks().to_vec()
    }

    fn debug_info(&self) -> DebugInfo {
        self.track_receiver.debug_info()
    }

    fn track_info(&self) -> Arc<livekit::TrackInfo> {
        self.track_receiver.track_info()
    }

    fn update_track_info(&self, ti: &livekit::TrackInfo) {
        self.track_receiver.update_track_info(ti);
    }

    fn get_primary_receiver_for_red(&self) -> Arc<dyn TrackReceiver> {
        self.track_receiver.get_primary_receiver_for_red()
    }

    fn get_red_receiver(&self) -> Arc<dyn TrackReceiver> {
        self.track_receiver.get_red_receiver()
    }

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        self.track_receiver
            .get_temporal_layer_fps_for_spatial(layer)
    }

    fn get_track_stats(&self) -> Option<livekit::RtpStats> {
        self.track_receiver.get_track_stats()
    }

    fn add_on_ready(&self, f: Box<dyn Fn() + Send + Sync>) {
        self.track_receiver.add_on_ready(f);
    }

    fn add_on_codec_state_change(
        &self,
        f: Box<dyn Fn(webrtc::RtpCodecParameters, ReceiverCodecState) + Send + Sync>,
    ) {
        self.track_receiver.add_on_codec_state_change(f);
    }

    fn codec_state(&self) -> ReceiverCodecState {
        self.track_receiver.codec_state()
    }

    fn video_sizes(&self) -> Vec<codec::VideoSize> {
        self.track_receiver.video_sizes()
    }

    fn restart(&self, reason: &str) {
        self.track_receiver.restart(reason);
    }
}

impl RedReceiverInner {
    fn encode_red_for_primary(
        &mut self,
        pkt: &rtp::Packet,
        red_payload: &mut [u8],
    ) -> Result<usize, Error> {
        let red_length = self.pkt_buff.len();
        // the slots of the packets, of which the packets are taken once the history is written
        let mut red_pkts: Vec<usize> = Vec::with_capacity(red_length + 1);
        let mut last_nil_pkt: i64 = -1;
        for i in (0..red_length).rev() {
            if self.pkt_buff[i].is_none() {
                last_nil_pkt = i as i64;
                break;
            }
        }

        for &prev_slot in self.pkt_buff[(last_nil_pkt + 1) as usize..]
            .iter()
            .flatten()
        {
            let prev = &self.hist_pkts[prev_slot];
            if pkt.sequence_number == prev.sequence_number
                || pkt.sequence_number.wrapping_sub(prev.sequence_number) > red_length as u16
                || pkt.timestamp.wrapping_sub(prev.timestamp) >= (1 << 14)
            {
                continue;
            }
            red_pkts.push(prev_slot);
        }

        // insert a copy of the primary packet in history buffer
        for i in (0..red_length).rev() {
            if self.pkt_buff[i].is_none_or(|prev_slot| {
                // history is empty, or
                // received packet has more recent sequence number
                pkt.sequence_number
                    .wrapping_sub(self.hist_pkts[prev_slot].sequence_number)
                    < (1 << 15)
            }) {
                // pick the slot before aging out, as the aged out packet can be a redundant block of this packet
                let slot = self.free_hist_slot();
                // age out older ones
                for j in 0..i {
                    self.pkt_buff[j] = self.pkt_buff[j + 1];
                }
                let n = pkt.payload.len().min(MAX_RED_PAYLOAD);
                self.hist_pkts[slot] = rtp::Packet {
                    header: rtp::Header {
                        sequence_number: pkt.sequence_number,
                        timestamp: pkt.timestamp,
                        ..Default::default()
                    },
                    payload: pkt.payload.slice(..n),
                    ..Default::default()
                };
                self.pkt_buff[i] = Some(slot);
                break;
            }
        }

        let red_pkts: Vec<&rtp::Packet> =
            red_pkts.iter().map(|&slot| &self.hist_pkts[slot]).collect();
        encode_red_for_primary(&red_pkts, pkt, red_payload)
    }

    // freeHistSlot returns a history slot that pktBuff does not point at, there is always one as there is a spare slot
    fn free_hist_slot(&self) -> usize {
        for slot in 0..self.hist_pkts.len() {
            if !self.pkt_buff.contains(&Some(slot)) {
                return slot;
            }
        }
        0
    }
}

fn encode_red_for_primary(
    mut red_pkts: &[&rtp::Packet],
    primary: &rtp::Packet,
    red_payload: &mut [u8],
) -> Result<usize, Error> {
    let mut payload_size = primary.payload.len() + 1;
    for p in red_pkts {
        payload_size += p.payload.len() + 4;
    }

    // if required payload size is larger than the redPayload buffer, encode the primary packet only
    if payload_size > red_payload.len() {
        red_pkts = &red_pkts[..0];
    }

    let mut index: usize = 0;
    for p in red_pkts {
        /* RED payload https://datatracker.ietf.org/doc/html/rfc2198#section-3
            0                   1                    2                   3
            0 1 2 3 4 5 6 7 8 9 0 1 2 3  4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           |F|   block PT  |  timestamp offset         |   block length    |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           F: 1 bit First bit in header indicates whether another header block
               follows.  If 1 further header blocks follow, if 0 this is the
               last header block.
        */
        let mut header = (0x80 | OPUS_PT) as u32;
        header <<= 14;
        header |= primary.timestamp.wrapping_sub(p.timestamp) & 0x3FFF;
        header <<= 10;
        header |= (p.payload.len() as u32) & 0x3FF;
        red_payload[index..index + 4].copy_from_slice(&header.to_be_bytes());
        index += 4;
    }
    // last block header
    red_payload[index] = OPUS_PT;
    index += 1;

    // append data blocks
    for p in red_pkts.iter().copied().chain(std::iter::once(primary)) {
        let dst = &mut red_payload[index..];
        let n = dst.len().min(p.payload.len());
        dst[..n].copy_from_slice(&p.payload[..n]);
        if n < p.payload.len() {
            return Err(Error::RedPayloadNoSpace(p.payload.len()));
        }
        index += p.payload.len();
    }
    Ok(index)
}

#[cfg(test)]
#[path = "redreceiver_test.rs"]
mod redreceiver_test;
