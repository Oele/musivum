// Ported from LiveKit, pkg/sfu/redprimaryreceiver.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The `TrackReceiver` that the original embeds is a
// field, and the methods of `impl TrackReceiver` that only hand a call on to it are what the
// embedding was. The original's methods are in the two trait impls: those of `RedTransformer`
// in the original's order, those of `TrackReceiver` in the trait's. `new` gives an `Arc` of
// the receiver itself where the original gives the interface, and the receiver keeps a `Weak`
// of itself, which is what `on_stream_restart` hands to the down tracks. The depacketizer and
// the highest forwarded sequence number and timestamp, which `ForwardRTP` changes from the
// one goroutine that calls it, are behind a lock, since the method takes `&self`; the lock is
// not held while a packet is written to the down tracks. The forwarded packet is a local
// where the original reuses `sendExtPkt`. `read_rtp` gives no count with an error, and parses
// the packet from a copy of what was read, as it writes the packet back to where it was read.

use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::go::atomic;
use crate::mediatransportutil::codec;
use crate::mediatransportutil::red;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::redreceiver::close_track_senders;
use crate::sfu::utils as sfuutils;
use crate::sfu::{
    Bitrates, DebugInfo, Error, ReceiverCodecState, RedTransformer, TrackReceiver, TrackSender,
};
use crate::webrtc;
use crate::webrtc::rtp;
use crate::{debugw, errorw, infow, warnw};

pub struct RedPrimaryReceiver {
    // the `TrackReceiver` that the original embeds
    track_receiver: Arc<dyn TrackReceiver>,
    down_track_spreader: sfuutils::DownTrackSpreader<Arc<dyn TrackSender>>,
    logger: Logger,
    closed: atomic::Bool,
    red_pt: u8,

    // what `ForwardRTP` changes, upstream without a lock
    inner: Mutex<RedPrimaryReceiverInner>,

    // the receiver itself, for the down tracks that are told of a restart
    this: Weak<RedPrimaryReceiver>,
}

#[derive(Default)]
struct RedPrimaryReceiverInner {
    depacketizer: red::Depacketizer,
    highest_forwarded_ext_sequence_number: u64,
    highest_forwarded_ext_timestamp: u64,
}

impl RedPrimaryReceiver {
    pub fn new(
        receiver: Arc<dyn TrackReceiver>,
        dsp: sfuutils::DownTrackSpreaderParams,
    ) -> Arc<RedPrimaryReceiver> {
        let red_pt = receiver.codec().payload_type;
        Arc::new_cyclic(|this| RedPrimaryReceiver {
            track_receiver: receiver,
            logger: dsp.logger.clone(),
            down_track_spreader: sfuutils::DownTrackSpreader::new(dsp),
            closed: atomic::Bool::default(),
            red_pt,
            inner: Mutex::new(RedPrimaryReceiverInner::default()),
            this: this.clone(),
        })
    }
}

impl RedTransformer for RedPrimaryReceiver {
    fn set_lb_threshold(&self, lb_threshold: usize) {
        self.down_track_spreader.set_threshold(lb_threshold);
    }

    fn forward_rtp(&self, pkt: &buffer::ExtPacket, spatial_layer: i32) -> i32 {
        // extract primary payload from RED and forward to downtracks
        if self.down_track_spreader.down_track_count() == 0 {
            return 0;
        }

        if pkt.packet.payload_type != self.red_pt {
            // forward non-red packet directly
            return sfuutils::broadcast_rtp(&self.down_track_spreader, pkt, spatial_layer);
        }

        let depacketized = self.inner.lock().depacketizer.depacketize(&pkt.packet);
        let pkts = match depacketized {
            Ok(pkts) => pkts,
            Err(err) => {
                errorw!(
                    self.logger,
                    "get encoding for red failed",
                    err,
                    "payloadtype" => pkt.packet.payload_type
                );
                return 0;
            }
        };

        let mut write_count: i32 = 0;
        for (i, send_pkt) in pkts.iter().enumerate() {
            let mut p_pkt = pkt.clone();
            if i != pkts.len() - 1 {
                // patch extended sequence number and time stamp for all but the last packet,
                // last packet is the primary payload
                p_pkt.ext_sequence_number = p_pkt.ext_sequence_number.wrapping_sub(
                    pkts[pkts.len() - 1]
                        .sequence_number
                        .wrapping_sub(pkts[i].sequence_number) as u64,
                );
                p_pkt.ext_timestamp = p_pkt.ext_timestamp.wrapping_sub(
                    pkts[pkts.len() - 1]
                        .timestamp
                        .wrapping_sub(pkts[i].timestamp) as u64,
                );
            }
            p_pkt.packet = send_pkt.clone();

            {
                let mut inner = self.inner.lock();
                if inner.highest_forwarded_ext_timestamp != 0
                    && ((p_pkt.ext_sequence_number > inner.highest_forwarded_ext_sequence_number
                        && p_pkt.ext_timestamp < inner.highest_forwarded_ext_timestamp)
                        || (p_pkt.ext_sequence_number
                            < inner.highest_forwarded_ext_sequence_number
                            && p_pkt.ext_timestamp > inner.highest_forwarded_ext_timestamp))
                {
                    warnw!(
                        self.logger,
                        "sequence number OR timestamp inversion, dropping", nil,
                        "numPackets" => pkts.len(),
                        "primaryIncomingSN" => pkt.packet.header.sequence_number,
                        "primaryIncomingTS" => pkt.packet.header.timestamp,
                        "primaryExtractedSN" => pkts[pkts.len() - 1].sequence_number,
                        "primaryExtractedTS" => pkts[pkts.len() - 1].timestamp,
                        "primaryESN" => pkt.ext_sequence_number,
                        "primaryETS" => pkt.ext_timestamp,
                        "packetIndex" => i,
                        "packetExtractedSN" => pkts[i].sequence_number,
                        "packetESN" => p_pkt.ext_sequence_number,
                        "packetExtractedTS" => pkts[i].timestamp,
                        "packetETS" => p_pkt.ext_timestamp,
                        "redHeader" => &pkt.packet.payload[..pkt.packet.payload.len().min(10)],
                        "payloadSize" => pkt.packet.payload.len(),
                        "highestFowardedExtSequenceNumber" => inner.highest_forwarded_ext_sequence_number,
                        "highestFowardedExtTimestamp" => inner.highest_forwarded_ext_timestamp,
                    );
                    continue; // drop the packet which causes the inversion
                }
                inner.highest_forwarded_ext_sequence_number = inner
                    .highest_forwarded_ext_sequence_number
                    .max(p_pkt.ext_sequence_number);
                inner.highest_forwarded_ext_timestamp = inner
                    .highest_forwarded_ext_timestamp
                    .max(p_pkt.ext_timestamp);
            }

            // not modify the ExtPacket.RawPacket here for performance since it is not used by the DownTrack,
            // otherwise it should be set to the correct value (marshal the primary rtp packet)
            write_count = write_count.wrapping_add(sfuutils::broadcast_rtp(
                &self.down_track_spreader,
                &p_pkt,
                spatial_layer,
            ));
        }
        write_count
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

impl TrackReceiver for RedPrimaryReceiver {
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

    fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, Error> {
        let n = self.track_receiver.read_rtp(buf, layer, esn)?;

        let mut pkt = rtp::Packet::default();
        let raw = Bytes::copy_from_slice(&buf[..n]);
        // The original does not look at the error either.
        let _ = pkt.unmarshal(&raw);
        let payload = red::extract_primary_encoding(&pkt.payload)?;
        pkt.payload = pkt.payload.slice_ref(payload);

        Ok(pkt.marshal_to(buf)?)
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
            "red primary receiver downtrack added",
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
            "red primary receiver downtrack deleted",
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
