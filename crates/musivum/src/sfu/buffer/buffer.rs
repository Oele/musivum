// Ported from LiveKit, pkg/sfu/buffer/buffer.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `Buffer` embeds `*BufferBase` and uses its lock. Here the lock is `Buffer`'s and guards
//   `BufferInner`: the base's state (`buffer_base`) and the fields of `Buffer`. The functions
//   the original calls with the lock held are methods of `BufferInner`; the methods of the
//   base that take the lock are in `buffer_base.rs`, whose header has the rest. `Buffer` is
//   made as an `Arc` (the factory, a repair stream's buffer and the tasks of the base hold
//   it), and implements `BufferProvider` at the end of this file.
// - pion's seams. `Write` takes the packet as `Bytes` and a packet that waits for the bind
//   keeps those bytes where the original copies them. Like the original it reports 0 bytes
//   written. `write_at` is new: `Write` with the arrival time given, where the original has
//   only its own clock because pion hands it no time; the body of the original's `Write` is
//   `write_at_or_now`, which both call (the clock is read where the original reads it).
//   `Read`, with `lastPacketRead`, has no counterpart: it is the `io.Reader` side
//   that pion reads a stream through (and the pending packets with it, to find out what the
//   stream is), and nothing of LiveKit's calls it.
// - `sendPLI` is `send_pli_packet` (`send_pli` is the base's `SendPLI`), and
//   `on_stream_restart_locked` is what the base's `OnStreamRestartLocked` was set to: see the
//   header of `buffer_base.rs`.
// - `rtxPktBuf` and `fecPktBuf`, the reused scratch, are arrays on the stack. The FEC
//   decoder is handed the lookup of media packets with every call (see `flexfec/decoder.rs`),
//   so `get_fec_media_packet_locked` takes the base's state and not the whole buffer, and
//   its errors are the package's.
// - `StreamInfoProbe` is owned by the buffer it is installed on, and its counter is not seen
//   by whoever made it. The strings it finds are made from the bytes of the extensions as
//   they are, with the replacement character for what is not UTF-8.
// - The callbacks are held by `Arc`, so that they can be called after the lock is given up
//   as the original does; RTCP feedback is handed over as a list of boxed packets, as the
//   transport-wide feedback responder does it; the counts of `cbFECRecovery` are `usize`.
// - `fecRecoveryDelta.invoke` takes the callback as an `Option`.

use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::RwLock;

use super::buffer_base::{BufferBase, BufferBaseParams, BufferBaseShared};
use super::{BufferProvider, Error, ExtPacket, ReadExtendedFuture, StreamStatsWithLayers};
use crate::go::time::{self, Time};
use crate::mediatransportutil::bucket;
use crate::mediatransportutil::codec;
use crate::mediatransportutil::twcc;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::audio;
use crate::sfu::flexfec;
use crate::sfu::rtpstats;
use crate::utils as sutils;
use crate::webrtc;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_nack::{
    NackPair, TransportLayerNack,
};
use crate::webrtc::rtp;
use crate::{debugw, errorw, infow, warnw};

const RTCP_RECEIVER_REPORT_DELTA: i64 = 1_000_000_000; // 1e9

pub const INIT_PACKET_BUFFER_SIZE_VIDEO: usize = 300;
pub const INIT_PACKET_BUFFER_SIZE_AUDIO: usize = 70;

// An unpaired FEC repair stream only needs a short bridge until its primary
// stream appears. The decoder itself retains at most this many FEC states.
pub(super) const MAX_PENDING_FEC_REPAIR_PACKETS: usize = 100;

// errInvalidCodec and errFECMediaPacketNotFound are `Error::InvalidCodec` and
// `Error::FecMediaPacketNotFound`

// var _ BufferProvider = (*Buffer)(nil): the implementation is at the end of this file

pub(super) struct PendingPacket {
    arrival_time: i64,
    pub(super) packet: Bytes,
}

#[derive(Clone, Copy, Debug, Default)]
struct FecRecoveryDelta {
    received: usize,
    recovered: usize,
    discarded: usize,
    bytes_received: usize,
}

impl FecRecoveryDelta {
    fn invoke(self, callback: Option<CbFecRecovery>) {
        if let Some(callback) = callback
            && (self.recovered > 0 || self.received > 0 || self.discarded > 0)
        {
            callback(
                self.received,
                self.recovered,
                self.discarded,
                self.bytes_received,
            );
        }
    }
}

// func(received int, recovered int, discarded int, bytesReceived int)
type CbFecRecovery = Arc<dyn Fn(usize, usize, usize, usize) + Send + Sync>;

type OnClose = Arc<dyn Fn() + Send + Sync>;
type OnRtcpFeedback = Arc<dyn Fn(Vec<Box<dyn rtcp::Packet>>) + Send + Sync>;
type OnFinalRtpStats = Arc<dyn Fn(livekit::RtpStats) + Send + Sync>;
type OnNotifyRtx = Arc<dyn Fn(u32, u32, &str) + Send + Sync>;
// func(ssrc uint32, mid, rid, rsid string)
type OnFound = Arc<dyn Fn(u32, &str, &str, &str) + Send + Sync>;

// Buffer contains all packets
pub struct Buffer {
    // `*BufferBase`: what the base uses without its lock
    pub(super) buffer_base: Arc<BufferBaseShared>,

    // `sync.RWMutex` of `BufferBase`
    pub(super) lock: RwLock<BufferInner>,
}

// What the lock guards: the base's state and the fields of `Buffer` upstream.
pub(super) struct BufferInner {
    pub(super) buffer_base: BufferBase,

    pub(super) p_packets: Vec<PendingPacket>,
    last_report_at: i64,
    is_bound: bool,

    twcc: Option<Arc<twcc::Responder>>,
    twcc_ext_id: u8,

    enable_audio_loss_proxying: bool,
    last_fraction_lost_to_report: u8, // Last fraction lost from subscribers, should report to publisher; Audio only

    // callbacks
    on_close: Option<OnClose>,
    on_rtcp_feedback: Option<OnRtcpFeedback>,
    on_final_rtp_stats: Option<OnFinalRtpStats>,
    on_notify_rtx: Option<OnNotifyRtx>,

    primary_buffer_for_rtx: Option<Arc<Buffer>>,

    primary_buffer_for_fec: Option<Arc<Buffer>>,
    is_fec_repair: bool,
    fec_ssrc: u32,
    pub(super) fec_decoder: Option<flexfec::Decoder>,
    on_fec_recovery: Option<CbFecRecovery>,

    stream_info_probe: Option<StreamInfoProbe>,
    warned_pending_overflow: bool,
}

// StreamInfoProbe identifies a stream from the mid/rid/rsid header extensions of its
// packets. It runs on the write path, i. e. as SRTP pushes into this buffer, because
// nothing reads remote streams through pion's interceptor chain.
#[derive(Clone)]
pub struct StreamInfoProbe {
    pub mid_ext_id: u8,
    pub rid_ext_id: u8,
    pub rsid_ext_id: u8,

    // Tries bounds how many packets are inspected before giving up.
    pub tries: i64,

    // OnFound is called at most once, in a goroutine, as it can re-enter this buffer.
    pub on_found: OnFound,
}

impl Buffer {
    pub fn new(ssrc: u32, max_video_pkts: usize, max_audio_pkts: usize) -> Arc<Buffer> {
        Arc::new_cyclic(|this: &Weak<Buffer>| {
            let buffer_base = BufferBase::new(
                BufferBaseParams {
                    ssrc,
                    max_video_pkts,
                    max_audio_pkts,
                    logger_components: vec![
                        sutils::COMPONENT_PUB.to_string(),
                        sutils::COMPONENT_SFU.to_string(),
                    ],
                    // SendPLI: b.sendPLI, OnStreamRestartLocked: b.recreateFECDecoderLocked:
                    // see the header of `buffer_base.rs`
                    is_reporting_enabled: true,
                    ..Default::default()
                },
                this.clone(),
            );
            Buffer {
                buffer_base: buffer_base.shared.clone(),
                lock: RwLock::new(BufferInner {
                    buffer_base,
                    p_packets: Vec::new(),
                    last_report_at: 0,
                    is_bound: false,
                    twcc: None,
                    twcc_ext_id: 0,
                    enable_audio_loss_proxying: false,
                    last_fraction_lost_to_report: 0,
                    on_close: None,
                    on_rtcp_feedback: None,
                    on_final_rtp_stats: None,
                    on_notify_rtx: None,
                    primary_buffer_for_rtx: None,
                    primary_buffer_for_fec: None,
                    is_fec_repair: false,
                    fec_ssrc: 0,
                    fec_decoder: None,
                    on_fec_recovery: None,
                    stream_info_probe: None,
                    warned_pending_overflow: false,
                }),
            }
        })
    }

    pub fn set_twcc_and_ext_id(&self, twcc: Option<Arc<twcc::Responder>>, ext_id: u8) {
        let mut b = self.lock.write();

        b.twcc = twcc;
        b.twcc_ext_id = ext_id;
    }

    pub fn set_audio_loss_proxying(&self, enable: bool) {
        let mut b = self.lock.write();

        b.enable_audio_loss_proxying = enable;
    }

    pub fn bind(
        &self,
        params: webrtc::RtpParameters,
        codec: &webrtc::RtpCodecCapability,
        bitrates: i64,
    ) -> Result<(), Error> {
        let mut lock = self.lock.write();
        let b = &mut *lock;
        if b.is_bound {
            return Ok(());
        }

        b.buffer_base.bind_locked(params, codec, bitrates)?;

        let now = mono::unix_nano();
        b.last_report_at = now;

        if !b.p_packets.is_empty() {
            debugw!(b.buffer_base.logger, "releasing queued packets on bind", "count" => b.p_packets.len());
        }
        for pp in std::mem::take(&mut b.p_packets) {
            b.ingest_locked(&pp.packet, None, pp.arrival_time, true, false);
        }

        let rtcp_packets = b.get_rtcp_packets(now);

        b.is_bound = true;
        b.maybe_create_fec_decoder_locked();
        drop(lock);

        self.packet_post_process(rtcp_packets, FecRecoveryDelta::default(), None);
        Ok(())
    }

    // Write adds an RTP Packet, ordering is not guaranteed, newer packets may arrive later
    pub fn write(&self, pkt: Bytes) -> Result<usize, Error> {
        self.write_at_or_now(pkt, None)
    }

    // (not in the original) Write with the time the packet arrived at, in the unit and of the
    // clock `write` stamps it with (`mono::unix_nano`), for a caller that has it from the
    // socket.
    pub fn write_at(&self, pkt: Bytes, arrival_time: i64) -> Result<usize, Error> {
        self.write_at_or_now(pkt, Some(arrival_time))
    }

    // (the body of the original's Write)
    fn write_at_or_now(&self, pkt: Bytes, arrival_time: Option<i64>) -> Result<usize, Error> {
        let mut rtp_packet = rtp::Packet::default();
        rtp_packet.unmarshal(&pkt)?;

        let mut lock = self.lock.write();
        let b = &mut *lock;
        if self.is_closed() {
            return Err(Error::Eof);
        }

        let now = arrival_time.unwrap_or_else(mono::unix_nano);
        if let Some(twcc) = &b.twcc
            && b.twcc_ext_id != 0
            && let Some(ext) = rtp_packet.get_extension(b.twcc_ext_id)
            && ext.len() >= 2
        {
            twcc.push(
                rtp_packet.ssrc,
                u16::from_be_bytes([ext[0], ext[1]]),
                now,
                rtp_packet.marker,
            );
        }

        // libwebrtc will use 0 ssrc for probing, don't push the packet to pending queue to avoid memory increasing since
        // the Bind will not be called to consume the pending packets. More details in https://github.com/pion/webrtc/pull/2816
        if rtp_packet.ssrc == 0 {
            return Ok(0);
        }

        if b.stream_info_probe.is_some() {
            b.probe_stream_info_locked(&rtp_packet);
        }

        // handle RTX packet
        if let Some(pb) = b.primary_buffer_for_rtx.clone() {
            drop(lock);

            // skip padding only packets
            if rtp_packet.padding && rtp_packet.payload.is_empty() {
                return Ok(0);
            }

            pb.write_rtx(&rtp_packet, now);
            return Ok(0);
        }

        // handle FlexFEC packet
        if let Some(pb) = b.primary_buffer_for_fec.clone() {
            drop(lock);

            // skip padding only packets
            if rtp_packet.padding && rtp_packet.payload.is_empty() {
                return Ok(0);
            }

            pb.write_fec(&rtp_packet, now);
            return Ok(0);
        }

        if !b.is_bound {
            if b.p_packets.is_empty() {
                debugw!(b.buffer_base.logger, "received first packet");
            }

            let mut pending_limit = b
                .buffer_base
                .max_video_pkts()
                .max(b.buffer_base.max_audio_pkts());
            if b.is_fec_repair {
                pending_limit = pending_limit.min(MAX_PENDING_FEC_REPAIR_PACKETS);
            }
            pending_limit = pending_limit.max(1);
            let overflow = b.p_packets.len() as i64 - pending_limit as i64 + 1;
            if overflow > 0 {
                b.p_packets.drain(..overflow as usize);

                // a stream that keeps arriving but never binds drops every packet from here
                // on; for an RTX stream it means the pairing was never established
                if !b.warned_pending_overflow {
                    b.warned_pending_overflow = true;
                    warnw!(
                        b.buffer_base.logger,
                        "unbound buffer overflowing, dropping packets", nil,
                        "ssrc" => b.buffer_base.ssrc(),
                        "pending" => b.p_packets.len(),
                    );
                }
            }

            b.p_packets.push(PendingPacket {
                packet: pkt,
                arrival_time: now,
            });

            b.buffer_base.notify_read();
            return Ok(0);
        }

        b.ingest_locked(&pkt, Some(&mut rtp_packet), now, false, false);

        let mut fec_delta = FecRecoveryDelta::default();
        let mut on_fec_recovery = None;
        if b.fec_decoder.is_some() {
            // feed media into the FEC decoder, a media arrival can complete a
            // previously unrecoverable FEC window. Recovered packets update NACK
            // state before feedback is generated below, so a packet repaired by
            // FEC is not needlessly NACKed.
            (fec_delta, on_fec_recovery) = b.feed_fec_locked(&rtp_packet, now);
        }

        let rtcp_packets = b.get_rtcp_packets(now);
        drop(lock);

        self.packet_post_process(rtcp_packets, fec_delta, on_fec_recovery);
        Ok(0)
    }

    // SetStreamInfoProbe installs probe and runs it over packets already queued.
    pub fn set_stream_info_probe(&self, probe: Option<StreamInfoProbe>) {
        let mut lock = self.lock.write();
        let b = &mut *lock;

        b.stream_info_probe = probe;

        for i in 0..b.p_packets.len() {
            if b.stream_info_probe.is_none() {
                return;
            }

            let mut rtp_packet = rtp::Packet::default();
            if rtp_packet.unmarshal(&b.p_packets[i].packet).is_err() {
                continue;
            }
            b.probe_stream_info_locked(&rtp_packet);
        }
    }
}

impl BufferInner {
    // probeStreamInfoLocked inspects one packet, clearing the probe once the stream is
    // identified or the try budget runs out.
    fn probe_stream_info_locked(&mut self, rtp_packet: &rtp::Packet) {
        let Some(probe) = self.stream_info_probe.as_mut() else {
            return;
        };

        let (mut mid, mut rid, mut rsid) = (String::new(), String::new(), String::new());
        if let Some(ext) = rtp_packet.get_extension(probe.mid_ext_id) {
            mid = String::from_utf8_lossy(&ext).into_owned();
        }
        if let Some(ext) = rtp_packet.get_extension(probe.rid_ext_id) {
            rid = String::from_utf8_lossy(&ext).into_owned();
        }
        if let Some(ext) = rtp_packet.get_extension(probe.rsid_ext_id) {
            rsid = String::from_utf8_lossy(&ext).into_owned();
        }

        if !mid.is_empty() && (!rid.is_empty() || !rsid.is_empty()) {
            let on_found = probe.on_found.clone();
            self.stream_info_probe = None;
            debugw!(self.buffer_base.logger, "stream found", "ssrc" => rtp_packet.ssrc, "mid" => mid, "rid" => rid, "rsid" => rsid);
            let ssrc = rtp_packet.ssrc;
            time::spawn(async move { on_found(ssrc, &mid, &rid, &rsid) });
            return;
        }

        // ignore padding only packets for probe count
        if rtp_packet.padding && rtp_packet.payload.is_empty() {
            return;
        }

        probe.tries -= 1;
        if probe.tries <= 0 {
            self.stream_info_probe = None;
        }
    }
}

impl Buffer {
    pub fn set_primary_buffer_for_rtx(&self, primary_buffer: Arc<Buffer>) {
        let pkts = {
            let mut b = self.lock.write();
            b.primary_buffer_for_rtx = Some(primary_buffer.clone());
            std::mem::take(&mut b.p_packets)
        };

        for pp in pkts {
            let mut rtp_packet = rtp::Packet::default();
            if rtp_packet.unmarshal(&pp.packet).is_err() {
                continue;
            }
            if rtp_packet.padding && rtp_packet.payload.is_empty() {
                continue;
            }
            primary_buffer.write_rtx(&rtp_packet, pp.arrival_time);
        }
    }

    pub fn notify_rtx(&self, ssrc: u32, repair_ssrc: u32, rsid: &str) {
        if let Some(on_notify_rtx) = self.get_on_notify_rtx() {
            on_notify_rtx(ssrc, repair_ssrc, rsid);
        }
    }

    pub(super) fn write_rtx(&self, rtx_pkt: &rtp::Packet, arrival_time: i64) {
        let mut lock = self.lock.write();
        let b = &mut *lock;
        if !b.is_bound {
            return;
        }

        if rtx_pkt.payload_type != b.buffer_base.rtx_payload_type {
            debugw!(b.buffer_base.logger, "unexpected rtx payload type", "expected" => b.buffer_base.rtx_payload_type, "actual" => rtx_pkt.payload_type);
            return;
        }

        let mut rtx_pkt_buf = [0u8; bucket::RTP_MAX_PKT_SIZE];

        if rtx_pkt.payload.len() < 2 {
            warnw!(b.buffer_base.logger, "rtx payload too short", nil, "size" => rtx_pkt.payload.len());
            return;
        }

        let mut repaired_pkt = rtx_pkt.clone();
        repaired_pkt.payload_type = b.buffer_base.payload_type;
        repaired_pkt.sequence_number = u16::from_be_bytes([rtx_pkt.payload[0], rtx_pkt.payload[1]]);
        repaired_pkt.ssrc = b.buffer_base.ssrc();
        repaired_pkt.payload = rtx_pkt.payload.slice(2..);
        let n = match repaired_pkt.marshal_to(&mut rtx_pkt_buf) {
            Ok(n) => n,
            Err(err) => {
                errorw!(b.buffer_base.logger, "could not marshal repaired packet", err, "ssrc" => b.buffer_base.ssrc(), "sn" => repaired_pkt.sequence_number);
                return;
            }
        };

        b.ingest_locked(
            &rtx_pkt_buf[..n],
            Some(&mut repaired_pkt),
            arrival_time,
            false,
            true,
        );

        let mut fec_delta = FecRecoveryDelta::default();
        let mut on_fec_recovery = None;
        if b.fec_decoder.is_some() {
            (fec_delta, on_fec_recovery) = b.feed_fec_locked(&repaired_pkt, arrival_time);
        }

        let rtcp_packets = b.get_rtcp_packets(arrival_time);
        drop(lock);

        self.packet_post_process(rtcp_packets, fec_delta, on_fec_recovery);
    }

    pub fn set_primary_buffer_for_fec(&self, primary_buffer: Arc<Buffer>) {
        let (pkts, ssrc) = {
            let mut b = self.lock.write();
            b.primary_buffer_for_fec = Some(primary_buffer.clone());
            (std::mem::take(&mut b.p_packets), b.buffer_base.ssrc())
        };

        // Let the primary know the repair stream SSRC so its decoder is ready
        // before the first FEC packet arrives.
        primary_buffer.set_fec_ssrc(ssrc);

        for pp in pkts {
            let mut rtp_packet = rtp::Packet::default();
            if rtp_packet.unmarshal(&pp.packet).is_err() {
                continue;
            }
            if rtp_packet.padding && rtp_packet.payload.is_empty() {
                continue;
            }
            primary_buffer.write_fec(&rtp_packet, pp.arrival_time);
        }
    }

    pub(super) fn set_fec_repair_buffer_limit(&self) {
        let mut b = self.lock.write();

        b.is_fec_repair = true;
        if b.p_packets.len() <= MAX_PENDING_FEC_REPAIR_PACKETS {
            return;
        }

        // Pairing can be announced after repair packets start arriving. Compact the
        // retained tail so both the dropped packet bytes and oversized slice backing
        // array become collectible.
        let start = b.p_packets.len() - MAX_PENDING_FEC_REPAIR_PACKETS;
        let mut retained = Vec::with_capacity(MAX_PENDING_FEC_REPAIR_PACKETS);
        retained.extend(b.p_packets.drain(start..));
        b.p_packets = retained;
    }

    fn set_fec_ssrc(&self, ssrc: u32) {
        let mut b = self.lock.write();
        b.fec_ssrc = ssrc;
        b.maybe_create_fec_decoder_locked();
    }
}

impl BufferInner {
    // maybeCreateFECDecoderLocked creates the FEC decoder as soon as the repair
    // stream SSRC is known and the buffer is bound with a negotiated flexfec
    // payload type. Protected media is read from the primary RTP packet bucket.
    fn maybe_create_fec_decoder_locked(&mut self) {
        if self.fec_decoder.is_some()
            || self.fec_ssrc == 0
            || !self.is_bound
            || self.buffer_base.fec_payload_type == 0
        {
            return;
        }

        // b.getFECMediaPacketLocked is handed to the decoder with every call: see the header
        self.fec_decoder = Some(flexfec::Decoder::new(
            self.fec_ssrc,
            self.buffer_base.ssrc(),
            self.buffer_base.logger.clone(),
        ));
        debugw!(self.buffer_base.logger, "flexfec decoder created", "fecSSRC" => self.fec_ssrc, "mediaSSRC" => self.buffer_base.ssrc());
    }

    // recreateFECDecoderLocked drops FlexFEC state from the previous sequence
    // space. Stream restart resyncs the packet bucket and sequence map, so repair
    // packets retained by the old decoder would recover against the wrong media
    // or be treated as duplicates of the new repair stream.
    fn recreate_fec_decoder_locked(&mut self) {
        if self.fec_decoder.is_none() {
            return;
        }

        self.fec_decoder = None;
        self.maybe_create_fec_decoder_locked();
    }

    // `OnStreamRestartLocked` of the base's parameters, which `NewBuffer` sets to
    // `b.recreateFECDecoderLocked`: to be called after every call into the base that can
    // restart the stream, with the lock still held. See the header of `buffer_base.rs`.
    pub(super) fn on_stream_restart_locked(&mut self) {
        if std::mem::take(&mut self.buffer_base.stream_restarted) {
            self.recreate_fec_decoder_locked();
        }
    }

    fn get_fec_media_packet_locked(
        b: &BufferBase,
        sequence_number: u16,
        dst: &mut [u8],
    ) -> Result<usize, Error> {
        let (Some(bucket), Some(rtp_stats)) = (&b.bucket, &b.rtp_stats) else {
            return Err(Error::FecMediaPacketNotFound);
        };

        // FlexFEC masks use the publisher's sequence-number space. BufferBase
        // removes padding-only packets from the downstream space, so resolve the
        // original extended sequence number and apply the same adjustment used
        // when the packet was inserted into the bucket.
        let highest_sequence_number = rtp_stats.extended_highest_sequence_number();
        let extended_sequence_number = (highest_sequence_number as i64).wrapping_add(
            sequence_number.wrapping_sub(highest_sequence_number as u16) as i16 as i64,
        );
        if extended_sequence_number < 0 {
            return Err(Error::FecMediaPacketNotFound);
        }

        let extended_sn = extended_sequence_number as u64;
        let sequence_number_adjustment = match b.sn_range_map.get_value(extended_sn) {
            Ok(sequence_number_adjustment) if sequence_number_adjustment <= extended_sn => {
                sequence_number_adjustment
            }
            _ => return Err(Error::FecMediaPacketNotFound),
        };

        Ok(bucket.get_packet(dst, extended_sn - sequence_number_adjustment)?)
    }
}

impl Buffer {
    // OnFECRecovery is called with counter deltas whenever FEC packets are
    // processed since the previous callback: FEC packets received, media packets
    // recovered, FEC packets discarded and FEC bytes received.
    pub fn on_fec_recovery(&self, f: impl Fn(usize, usize, usize, usize) + Send + Sync + 'static) {
        let mut b = self.lock.write();
        b.on_fec_recovery = Some(Arc::new(f));
    }

    // FECDecoderStats returns cumulative FlexFEC decode counters of the buffer.
    pub fn fec_decoder_stats(&self) -> flexfec::DecoderStats {
        let b = self.lock.read();

        let Some(fec_decoder) = &b.fec_decoder else {
            return flexfec::DecoderStats::default();
        };
        fec_decoder.stats()
    }

    // writeFEC handles a packet of the coupled FlexFEC repair stream, recovered
    // media packets are injected into the regular packet pipeline.
    pub(super) fn write_fec(&self, fec_pkt: &rtp::Packet, arrival_time: i64) {
        let mut lock = self.lock.write();
        let b = &mut *lock;
        if !b.is_bound {
            return;
        }

        // the FEC stream is never bound in pion, run its TWCC accounting here so
        // publisher send side BWE sees the FEC packets acked. The repair stream
        // shares the media m-line, extension ids match the primary stream.
        if let Some(twcc) = &b.twcc
            && b.twcc_ext_id != 0
            && let Some(ext) = fec_pkt.get_extension(b.twcc_ext_id)
            && ext.len() >= 2
        {
            twcc.push(
                fec_pkt.ssrc,
                u16::from_be_bytes([ext[0], ext[1]]),
                arrival_time,
                fec_pkt.marker,
            );
        }

        if b.buffer_base.fec_payload_type == 0
            || fec_pkt.payload_type != b.buffer_base.fec_payload_type
        {
            debugw!(b.buffer_base.logger, "unexpected fec payload type", "expected" => b.buffer_base.fec_payload_type, "actual" => fec_pkt.payload_type);
            return;
        }

        if b.fec_decoder.is_none() {
            // normally created when the pair is declared, fall back to the
            // observed repair stream SSRC
            b.fec_ssrc = fec_pkt.ssrc;
            b.maybe_create_fec_decoder_locked();
            if b.fec_decoder.is_none() {
                infow!(b.buffer_base.logger, "flexfec decoder unavailable after creation attempt", "fecSSRC" => b.fec_ssrc, "mediaSSRC" => b.buffer_base.ssrc(), "payloadType" => b.buffer_base.fec_payload_type);
                return;
            }
        }

        let (fec_delta, on_fec_recovery) = b.feed_fec_locked(fec_pkt, arrival_time);
        let rtcp_packets = b.get_rtcp_packets(arrival_time);
        drop(lock);

        self.packet_post_process(rtcp_packets, fec_delta, on_fec_recovery);
    }
}

impl BufferInner {
    // feedFECLocked runs a media or FEC packet through the FEC decoder and
    // injects recovered packets into the packet pipeline. Must be called with the
    // buffer lock held and a non-nil decoder.
    fn feed_fec_locked(
        &mut self,
        pkt: &rtp::Packet,
        arrival_time: i64,
    ) -> (FecRecoveryDelta, Option<CbFecRecovery>) {
        let buffer_base = &self.buffer_base;
        let fec_decoder = self
            .fec_decoder
            .as_mut()
            .expect("the FEC decoder is there when it is fed");
        let stats_before = fec_decoder.stats();
        let recovered = fec_decoder.decode_fec(
            pkt,
            Some(&|sequence_number: u16, dst: &mut [u8]| {
                Ok(Self::get_fec_media_packet_locked(
                    buffer_base,
                    sequence_number,
                    dst,
                )?)
            }),
        );
        let stats_after = fec_decoder.stats();

        let fec_packets = stats_after
            .fec_packets_received
            .wrapping_sub(stats_before.fec_packets_received);
        let fec_bytes = stats_after
            .fec_bytes_received
            .wrapping_sub(stats_before.fec_bytes_received);
        let fec_packets_discarded = stats_after
            .fec_packets_discarded
            .wrapping_sub(stats_before.fec_packets_discarded);
        let fec_packets_recovered = stats_after
            .packets_recovered
            .wrapping_sub(stats_before.packets_recovered);
        if let Some(rtp_stats) = &self.buffer_base.rtp_stats {
            rtp_stats.update_fec(
                fec_packets,
                fec_bytes,
                fec_packets_discarded,
                fec_packets_recovered,
            );
        }

        for mut rp in recovered {
            let mut fec_pkt_buf = [0u8; bucket::RTP_MAX_PKT_SIZE];
            let n = match rp.marshal_to(&mut fec_pkt_buf) {
                Ok(n) => n,
                Err(err) => {
                    warnw!(self.buffer_base.logger, "could not marshal fec recovered packet", err, "ssrc" => self.buffer_base.ssrc(), "sn" => rp.sequence_number);
                    continue;
                }
            };

            // recovered packets flow through the regular pipeline: they are
            // forwarded downstream and stop NACKs for the lost sequence numbers.
            // They do not re-enter the decoder because chained recovery already
            // completed within DecodeFEC. RTCP is generated once by the caller
            // after all recovered packets have updated NACK state.
            self.ingest_locked(&fec_pkt_buf[..n], Some(&mut rp), arrival_time, false, true);
        }

        if let Some(cb) = &self.on_fec_recovery {
            return (
                FecRecoveryDelta {
                    received: fec_packets as usize,
                    recovered: fec_packets_recovered as usize,
                    discarded: fec_packets_discarded as usize,
                    bytes_received: fec_bytes as usize,
                },
                Some(cb.clone()),
            );
        }

        (FecRecoveryDelta::default(), None)
    }
}

impl Buffer {
    // Read: no counterpart, see the header

    pub fn close(&self) -> Result<(), Error> {
        let stats = self.close_with_reason("close")?;

        if let Some(stats) = stats
            && let Some(cb) = self.get_on_final_rtp_stats()
        {
            cb(stats);
        }

        if let Some(cb) = self.get_on_close() {
            cb();
        }

        Ok(())
    }

    pub fn on_close(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut b = self.lock.write();
        b.on_close = Some(Arc::new(f));
    }

    fn get_on_close(&self) -> Option<OnClose> {
        let b = self.lock.read();

        b.on_close.clone()
    }

    // sendPLI
    pub(super) fn send_pli_packet(&self) {
        let ssrc = self.ssrc();
        if ssrc == 0 {
            return;
        }

        let logger = self.lock.read().buffer_base.logger.clone();
        debugw!(logger, "send pli", "mediaSSRC" => ssrc);
        let pli: Vec<Box<dyn rtcp::Packet>> = vec![Box::new(PictureLossIndication {
            sender_ssrc: ssrc,
            media_ssrc: ssrc,
        })];

        if let Some(cb) = self.get_on_rtcp_feedback() {
            cb(pli);
        }
    }
}

impl BufferInner {
    // ingestLocked runs a packet through the incoming packet pipeline, updating RTP
    // stats, NACK state and the packet bucket. It intentionally does not generate
    // RTCP so that FEC recovery can update NACK state before feedback is generated
    // once. Must be called with the buffer lock held.
    fn ingest_locked(
        &mut self,
        raw_pkt: &[u8],
        rtp_packet: Option<&mut rtp::Packet>,
        arrival_time: i64,
        is_buffered: bool,
        is_rtx: bool,
    ) {
        let _ = self.buffer_base.handle_incoming_packet_locked(
            raw_pkt,
            rtp_packet,
            arrival_time,
            is_buffered,
            is_rtx,
            &[],
            0,
        );
        self.on_stream_restart_locked();
    }
}

impl Buffer {
    // packetPostProcess runs processing after a packet is handled.
    // `rtcpPackets` should be RTCP feedback from the current NACK state, which
    // already reflects any FEC recovery performed during this write.
    // This is to ensure that packets recovered via FEC is not NACKed.
    fn packet_post_process(
        &self,
        rtcp_packets: Vec<Box<dyn rtcp::Packet>>,
        fec_delta: FecRecoveryDelta,
        on_fec_recovery: Option<CbFecRecovery>,
    ) {
        fec_delta.invoke(on_fec_recovery);

        if !rtcp_packets.is_empty()
            && let Some(cb) = self.get_on_rtcp_feedback()
        {
            cb(rtcp_packets);
        }
    }
}

impl BufferInner {
    fn get_rtcp_packets(&mut self, arrival_time: i64) -> Vec<Box<dyn rtcp::Packet>> {
        let mut pkts: Vec<Box<dyn rtcp::Packet>> = Vec::new();
        if let Some(nack_pkt) = self.get_nack_packet() {
            pkts.push(Box::new(nack_pkt));
        }
        if let Some(receiver_report) = self.get_rtcp_receiver_report(arrival_time) {
            pkts.push(Box::new(receiver_report));
        }

        pkts
    }

    fn get_nack_packet(&mut self) -> Option<TransportLayerNack> {
        let nacks = self.buffer_base.get_nack_pairs_locked();
        if !nacks.is_empty() {
            let ssrc = self.buffer_base.ssrc();
            return Some(TransportLayerNack {
                sender_ssrc: ssrc,
                media_ssrc: ssrc,
                nacks,
            });
        }
        None
    }

    fn get_rtcp_receiver_report(&mut self, arrival_time: i64) -> Option<ReceiverReport> {
        if arrival_time.wrapping_sub(self.last_report_at) < RTCP_RECEIVER_REPORT_DELTA {
            return None;
        }
        self.last_report_at = arrival_time;

        let mut proxy_loss = self.last_fraction_lost_to_report;
        if self.buffer_base.codec_type == webrtc::RtpCodecType::Audio
            && !self.enable_audio_loss_proxying
        {
            proxy_loss = 0;
        }

        if let Some(reception_report) = self
            .buffer_base
            .get_rtcp_reception_report_locked(proxy_loss)
        {
            return Some(ReceiverReport {
                ssrc: self.buffer_base.ssrc(),
                reports: vec![reception_report],
                ..Default::default()
            });
        }

        None
    }
}

impl Buffer {
    pub fn set_last_fraction_lost_report(&self, lost: u8) {
        let mut b = self.lock.write();

        b.last_fraction_lost_to_report = lost;
    }

    pub fn on_rtcp_feedback(&self, f: impl Fn(Vec<Box<dyn rtcp::Packet>>) + Send + Sync + 'static) {
        let mut b = self.lock.write();
        b.on_rtcp_feedback = Some(Arc::new(f));
    }

    fn get_on_rtcp_feedback(&self) -> Option<OnRtcpFeedback> {
        let b = self.lock.read();

        b.on_rtcp_feedback.clone()
    }

    pub fn on_final_rtp_stats(&self, f: impl Fn(livekit::RtpStats) + Send + Sync + 'static) {
        let mut b = self.lock.write();
        b.on_final_rtp_stats = Some(Arc::new(f));
    }

    fn get_on_final_rtp_stats(&self) -> Option<OnFinalRtpStats> {
        let b = self.lock.read();

        b.on_final_rtp_stats.clone()
    }

    pub fn on_notify_rtx(&self, f: impl Fn(u32, u32, &str) + Send + Sync + 'static) {
        let mut b = self.lock.write();
        b.on_notify_rtx = Some(Arc::new(f));
    }

    fn get_on_notify_rtx(&self) -> Option<OnNotifyRtx> {
        let b = self.lock.read();

        b.on_notify_rtx.clone()
    }
}

// var _ BufferProvider = (*Buffer)(nil)
impl BufferProvider for Buffer {
    fn set_logger(&self, lgr: Logger) {
        Buffer::set_logger(self, lgr)
    }

    fn set_audio_level_config(&self, audio_level_config: audio::AudioLevelConfig) {
        Buffer::set_audio_level_config(self, audio_level_config)
    }

    fn set_stream_restart_detection(&self, enable: bool) {
        Buffer::set_stream_restart_detection(self, enable)
    }

    fn set_pli_throttle(&self, duration: i64) {
        Buffer::set_pli_throttle(self, duration)
    }

    fn set_rtt(&self, rtt: u32) {
        Buffer::set_rtt(self, rtt)
    }

    fn set_paused(&self, paused: bool) {
        Buffer::set_paused(self, paused)
    }

    fn send_pli(&self, force: bool) {
        Buffer::send_pli(self, force)
    }

    fn read_extended(&self, buf: &mut [u8]) -> Result<Option<ExtPacket>, Error> {
        Buffer::read_extended(self, buf)
    }

    fn read_extended_async<'a>(&'a self, buf: &'a mut [u8]) -> ReadExtendedFuture<'a> {
        Box::pin(Buffer::read_extended_async(self, buf))
    }

    fn get_packet(&self, buf: &mut [u8], esn: u64) -> Result<usize, Error> {
        Buffer::get_packet(self, buf, esn)
    }

    fn get_audio_level(&self) -> (f64, bool) {
        Buffer::get_audio_level(self)
    }

    fn get_temporal_layer_fps_for_spatial(&self, layer: i32) -> Vec<f32> {
        Buffer::get_temporal_layer_fps_for_spatial(self, layer)
    }

    fn get_stats(&self) -> Option<livekit::RtpStats> {
        Buffer::get_stats(self)
    }

    fn get_delta_stats(&self) -> Option<StreamStatsWithLayers> {
        Buffer::get_delta_stats(self)
    }

    fn get_delta_stats_lite(&self) -> Option<rtpstats::RtpDeltaInfoLite> {
        Buffer::get_delta_stats_lite(self)
    }

    fn get_last_sender_report_time(&self) -> Time {
        Buffer::get_last_sender_report_time(self)
    }

    fn get_nack_pairs(&self) -> Vec<NackPair> {
        Buffer::get_nack_pairs(self)
    }

    fn set_sender_report_data(&self, sr_data: Option<&livekit::RtcpSenderReportState>) {
        Buffer::set_sender_report_data(self, sr_data)
    }

    fn get_sender_report_data(&self) -> Option<livekit::RtcpSenderReportState> {
        Buffer::get_sender_report_data(self)
    }

    fn on_rtcp_sender_report(&self, f: Box<dyn Fn() + Send + Sync>) {
        Buffer::on_rtcp_sender_report(self, f)
    }

    fn on_fps_changed(&self, f: Box<dyn Fn() + Send + Sync>) {
        Buffer::on_fps_changed(self, f)
    }

    fn on_video_size_changed(&self, f: Box<dyn Fn(Vec<codec::VideoSize>) + Send + Sync>) {
        Buffer::on_video_size_changed(self, f)
    }

    fn on_codec_change(&self, f: Box<dyn Fn(webrtc::RtpCodecParameters) + Send + Sync>) {
        Buffer::on_codec_change(self, f)
    }

    fn on_stream_restart(&self, f: Box<dyn Fn(&str) + Send + Sync>) {
        Buffer::on_stream_restart(self, f)
    }

    fn start_key_frame_seeder(&self) {
        Buffer::start_key_frame_seeder(self)
    }

    fn stop_key_frame_seeder(&self) {
        Buffer::stop_key_frame_seeder(self)
    }

    fn handle_incoming_packet(
        &self,
        raw_pkt: &[u8],
        rtp_packet: Option<&mut rtp::Packet>,
        arrival_time: i64,
        is_buffered: bool,
        is_rtx: bool,
        skipped_seqs: &[u16],
        oob_sequence_number: u16,
    ) -> Result<u64, Error> {
        Buffer::handle_incoming_packet(
            self,
            raw_pkt,
            rtp_packet,
            arrival_time,
            is_buffered,
            is_rtx,
            skipped_seqs,
            oob_sequence_number,
        )
    }

    fn mark_for_restart_stream(&self, reason: &str) {
        Buffer::mark_for_restart_stream(self, reason)
    }

    fn restart_stream(&self, reason: &str) {
        Buffer::restart_stream(self, reason)
    }

    fn close_with_reason(&self, reason: &str) -> Result<Option<livekit::RtpStats>, Error> {
        Buffer::close_with_reason(self, reason)
    }
}

#[cfg(test)]
#[path = "buffer_test.rs"]
mod buffer_test;

#[cfg(test)]
#[path = "buffer_fec_test.rs"]
mod buffer_fec_test;

#[cfg(test)]
#[path = "buffer_reference_test.rs"]
mod buffer_reference_test;
