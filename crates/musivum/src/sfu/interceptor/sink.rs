//! The sink of one connection: what stands between musivum's peer connection and the buffers.
//!
//! pion gave LiveKit every received packet through two doors: a buffer factory, which its
//! SRTP sessions ask for a buffer per SSRC, and interceptors, which it tells about every
//! remote stream. musivum's connection has one door, [`PacketSink`], and
//! [`BufferFactorySink`] is what LiveKit's side of both doors becomes behind it: it feeds the
//! connection's [`buffer::Factory`] as pion's sessions did, and runs what LiveKit's
//! interceptors did ([`RtxInfoExtractor`], [`RttFromXr`]).
//!
//! # RTP
//!
//! Every packet goes into the buffer of its SSRC (`Buffer::write`). The buffer is asked of
//! the factory once (`get_or_new`, which takes the factory's lock for writing) and the handle
//! kept. Together with it the RTCP reader of the SSRC is made, as pion opens the RTCP stream
//! that goes with an RTP stream, so that `Factory::get_buffer_pair` has both when the track
//! is announced. A buffer is made
//!
//! - when the connection announces the stream (`bind_remote_stream`, which comes before the
//!   stream's first packet), as pion has opened a stream's buffers when it binds it;
//! - at the first packet of an SSRC that was not announced. That is every SSRC `rtc` cannot
//!   place, and of those the one that matters is the retransmission stream of a simulcast
//!   layer (see below).
//!
//! Until somebody binds a buffer (`Buffer::bind`, by the receiver of the track) it keeps the
//! packets that come, the newest `max(video, audio)` packets of the factory's sizes; a
//! retransmission stream's buffer keeps them until it is paired, and then hands them to the
//! buffer it repairs, which drops them unless it is bound. A buffer its receiver has closed
//! stays here as it is (packets for it are dropped, as with pion) until the stream is
//! unbound. A packet is written with the arrival time the connection gives, which is when its
//! datagram was read from the socket (`Buffer::write_at`); upstream's buffer has only its own
//! clock at the moment of the write, because pion hands it no time.
//!
//! # RTCP
//!
//! As pion's SRTCP session does it: every packet of a compound packet is marshalled once, on
//! its own, and written to the RTCP reader of every SSRC it is addressed to
//! (`destination_ssrc`); one that names no SSRC (a receiver reference time report, a goodbye)
//! goes to every SSRC the compound packet is addressed to. A reader is asked of the factory
//! on first use and kept, whether it is that of a stream the connection receives or that of a
//! stream it sends (which a down track has asked of the same factory). A packet that cannot be
//! marshalled again is left out and the others still go, as with pion; that can happen to
//! transport-wide feedback (`docs/port-status.md`, on `rtc-rtcp`). Unlike pion, a reader
//! that is closed does not keep the rest of the compound packet from the other readers.
//!
//! # Which stream repairs which
//!
//! - A retransmission stream the description names (`a=ssrc-group:FID`) is in the
//!   announcement of the stream it repairs (`StreamInfo::ssrc_retransmission`), and is paired
//!   then and there (`Factory::set_rtx_pair`). Upstream `pkg/rtc/transport.go` reads the
//!   same from the SDP.
//! - With simulcast the streams are known by their rid only. [`RtxInfoExtractor`] has a probe
//!   put on a stream's buffer that reads mid and rid, or mid and repaired rid, from the
//!   stream's first packets; when it knows both of a pair it says so, and the sink tells the
//!   factory. `rtc` announces the layers but not their retransmission streams, so for an SSRC
//!   that was not announced the sink does what pion did for it: it gives it to the extractor
//!   at its first packet, with the header extensions the connection's streams were announced
//!   with (they are the same for every stream of a bundle), or as soon as those are known.
//!   Once such a stream is paired the sink announces it (`on_stream_found`, as `video/rtx`
//!   with the header extensions of the stream it repairs), which is how it gets its
//!   transport-wide feedback set up. Its first packets, before the pairing, are not in that
//!   feedback.
//!
//! # When buffers are closed
//!
//! - `unbind_remote_stream`: the buffer and the RTCP reader of the stream, and those of a
//!   retransmission stream that was paired with it, as pion's `RTPReceiver.Stop` closes
//!   them.
//! - [`BufferFactorySink::close`]: everything the sink has a handle of, as closing pion's
//!   SRTP sessions does. The connection calls it when it has closed (`closed`, from its own
//!   task, after everything else it had to hand over); whoever closes the connection may call
//!   it too, sooner, and what the connection still hands over then goes nowhere. Closing
//!   twice is nothing. Dropping the sink closes it as well, for a sink that never was a
//!   connection's.
//!
//! # The streams the connection sends
//!
//! The connection tells its sink when the track of a sender is bound and when it is unbound
//! (`bind_local_stream`, `unbind_local_stream`). That goes to the connection's [`RttFromXr`],
//! which answers a receiver's reference time report in the name of every such stream. pion
//! told its interceptors when a sender started and when it stopped; a track that takes the
//! place of another on a sender is, here, the end of one stream and the beginning of the next.
//!
//! # Where it runs
//!
//! The connection calls its sink from its own task, one call at a time, with no lock of its
//! own held. The sink holds its lock only to look something up: buffers, readers and
//! callbacks are called without it. What a buffer calls back when a packet is written (RTCP
//! feedback, which goes back into the connection) therefore runs on the connection's task. A
//! pairing that a probe finds is reported from a task of its own, as upstream from a
//! goroutine.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use parking_lot::Mutex;

use super::{
    RtcpWriter, RttFromXr, RttFromXrFactory, RtxInfoExtractor, RtxInfoExtractorFactory,
    SDES_REPAIR_RTP_STREAM_ID_URI, SimulcastTrackInfo,
};
use crate::debugw;
use crate::go::time::Time;
use crate::protocol::logger::Logger;
use crate::sfu::buffer::{self, Buffer, BufferPacketType, RtcpReader};
use crate::sfu::utils;
use crate::webrtc::interceptor::RtpHeaderExtension;
use crate::webrtc::rtcp;
use crate::webrtc::sdp;
use crate::webrtc::{MIME_TYPE_RTX, PacketSink, RtpHeaderExtensionCapability, Ssrc, StreamInfo};

/// What a sink calls for a stream that was found: `func(*interceptor.StreamInfo)` upstream.
pub type OnStreamFound = Box<dyn Fn(&StreamInfo) + Send + Sync>;

/// What a [`BufferFactorySink`] is made from: what `newPeerConnection` of upstream's
/// `pkg/rtc/transport.go` gives pion as buffer factory and as interceptors.
pub struct BufferFactorySinkParams {
    /// `params.Config.BufferFactory`. A participant's two connections share one.
    pub buffer_factory: Arc<buffer::Factory>,
    /// The first argument of `NewRTXInfoExtractorFactory` (`setTWCCForVideo`): called for
    /// every stream the connection announces, before the stream's first packet, and for a
    /// retransmission stream it does not announce once that was paired.
    pub on_stream_found: Option<OnStreamFound>,
    /// `params.SimTracks`: what is known of a migrated publisher's simulcast streams. The
    /// pairings in it are taken over.
    pub sim_tracks: HashMap<u32, SimulcastTrackInfo>,
    /// `Some` where upstream adds `NewRTTFromXRFactory` to the registry (`!params.IsOfferer`).
    pub rtt_from_xr: Option<RttFromXrFactory>,
    pub logger: Logger,
}

/// The [`PacketSink`] of one connection. See the module's description.
pub struct BufferFactorySink {
    buffer_factory: Arc<buffer::Factory>,
    on_stream_found: Option<OnStreamFound>,
    logger: Logger,
    // Set once in `new`: the factory's callbacks point back to the sink.
    rtx_info_extractor: OnceLock<(Arc<RtxInfoExtractorFactory>, RtxInfoExtractor)>,
    rtt_from_xr: Option<Arc<RttFromXr>>,
    lock: Mutex<Inner>,
}

// What the lock guards.
#[derive(Default)]
struct Inner {
    closed: bool,
    // The streams the connection receives, by SSRC: pion's read streams of the SRTP session.
    streams: HashMap<u32, Stream>,
    // The RTCP readers that were written to, by SSRC: those of the streams and those of what
    // the connection sends.
    rtcp_readers: HashMap<u32, Arc<RtcpReader>>,
    // The header extensions of the first stream that was announced with the three the probe
    // reads. Empty until then.
    header_extensions: Vec<RtpHeaderExtension>,
    // Retransmission stream -> the stream it repairs, as far as known.
    repairs: HashMap<u32, u32>,
}

struct Stream {
    buffer: Arc<Buffer>,
    // What the connection announced the stream with; `None` for one it did not announce.
    info: Option<StreamInfo>,
    // Of the stream's first packet, where the stream was not announced.
    payload_type: u8,
    // The extractor was given the stream.
    probed: bool,
    // `on_stream_found` was called for the stream.
    found: bool,
}

impl BufferFactorySink {
    pub fn new(params: BufferFactorySinkParams) -> Arc<BufferFactorySink> {
        let sink = Arc::new(BufferFactorySink {
            buffer_factory: params.buffer_factory.clone(),
            on_stream_found: params.on_stream_found,
            logger: params.logger.clone(),
            rtx_info_extractor: OnceLock::new(),
            rtt_from_xr: params
                .rtt_from_xr
                .map(|factory| factory.new_interceptor("")),
            lock: Mutex::new(Inner::default()),
        });

        // The pairings of `sim_tracks` are reported while the factory is made.
        let factory = RtxInfoExtractorFactory::new(
            {
                let sink = Arc::downgrade(&sink);
                move |info| {
                    if let Some(sink) = sink.upgrade() {
                        sink.stream_found(info);
                    }
                }
            },
            {
                let sink = Arc::downgrade(&sink);
                move |repair, base, rsid| {
                    if let Some(sink) = sink.upgrade() {
                        sink.rtx_pair_found(repair, base, rsid);
                    }
                }
            },
            params.buffer_factory,
            &params.sim_tracks,
            params.logger,
        );
        let extractor = factory.new_interceptor("");
        let _ = sink.rtx_info_extractor.set((factory, extractor));
        sink
    }

    /// The factory of the connection's [`RtxInfoExtractor`], which upstream's transport keeps
    /// to tell it about a published stream (`RTPStreamPublished`: `set_stream_info(ssrc, mid,
    /// rid, "")`).
    pub fn rtx_info_extractor_factory(&self) -> &Arc<RtxInfoExtractorFactory> {
        &self.extractor().0
    }

    /// The connection's [`RttFromXr`], if it has one. It is told about the streams the
    /// connection sends by the sink, which the connection tells.
    pub fn rtt_from_xr(&self) -> Option<&Arc<RttFromXr>> {
        self.rtt_from_xr.as_ref()
    }

    /// pion's `BindRTCPWriter`: where [`RttFromXr`] writes its extended reports, which is the
    /// connection (`PeerConnection::write_rtcp`). The sink lets go of the writer when it is
    /// closed.
    pub fn bind_rtcp_writer(&self, writer: Arc<dyn RtcpWriter>) {
        if let Some(rtt_from_xr) = &self.rtt_from_xr {
            rtt_from_xr.bind_rtcp_writer(writer);
        }
    }

    /// Closes the buffers and RTCP readers of the connection, and ends what runs for it. The
    /// connection does this when it has closed (`closed`); whoever closes the connection may
    /// do it as well, before or after. Only the first call does anything.
    pub fn close(&self) {
        let (streams, rtcp_readers) = {
            let mut inner = self.lock.lock();
            if inner.closed {
                return;
            }
            inner.closed = true;
            inner.repairs.clear();
            (
                std::mem::take(&mut inner.streams),
                std::mem::take(&mut inner.rtcp_readers),
            )
        };
        if let Some(rtt_from_xr) = &self.rtt_from_xr {
            rtt_from_xr.close();
        }
        for stream in streams.into_values() {
            let _ = stream.buffer.close();
        }
        for reader in rtcp_readers.into_values() {
            let _ = reader.close();
        }
    }

    fn extractor(&self) -> &(Arc<RtxInfoExtractorFactory>, RtxInfoExtractor) {
        self.rtx_info_extractor
            .get()
            .expect("the extractor is set when the sink is made")
    }

    /// The buffer of an SSRC, made if the sink does not have it yet, with the RTCP reader
    /// that goes with it. `info` is what the connection announced the stream with. `None`
    /// when the sink is closed.
    fn open(&self, ssrc: u32, info: Option<&StreamInfo>, payload_type: u8) -> Option<Arc<Buffer>> {
        {
            let mut guard = self.lock.lock();
            let inner = &mut *guard;
            if inner.closed {
                return None;
            }
            if let Some(info) = info
                && inner.header_extensions.is_empty()
                && has_probe_extensions(info)
            {
                inner.header_extensions = info.rtp_header_extensions.clone();
            }
            if let Some(stream) = inner.streams.get_mut(&ssrc) {
                if let Some(info) = info {
                    stream.info = Some(info.clone());
                    stream.found = true;
                }
                return Some(stream.buffer.clone());
            }
        }

        // Not with the sink's lock held: the factory calls into buffers, which call out.
        let buffer = self
            .buffer_factory
            .get_or_new(BufferPacketType::Rtp, ssrc)
            .into_buffer()?;
        let reader = self
            .buffer_factory
            .get_or_new(BufferPacketType::Rtcp, ssrc)
            .into_rtcp_reader()?;

        let mut inner = self.lock.lock();
        if inner.closed {
            // closed in between
            drop(inner);
            let _ = buffer.close();
            let _ = reader.close();
            return None;
        }
        inner.streams.insert(
            ssrc,
            Stream {
                buffer: buffer.clone(),
                info: info.cloned(),
                payload_type,
                probed: false,
                found: info.is_some(),
            },
        );
        inner.rtcp_readers.entry(ssrc).or_insert(reader);
        Some(buffer)
    }

    /// The RTCP reader of an SSRC, asked of the factory if the sink does not have it yet.
    fn rtcp_reader(&self, ssrc: u32) -> Option<Arc<RtcpReader>> {
        {
            let inner = self.lock.lock();
            if inner.closed {
                return None;
            }
            if let Some(reader) = inner.rtcp_readers.get(&ssrc) {
                return Some(reader.clone());
            }
        }

        let reader = self
            .buffer_factory
            .get_or_new(BufferPacketType::Rtcp, ssrc)
            .into_rtcp_reader()?;

        let mut inner = self.lock.lock();
        if inner.closed {
            drop(inner);
            let _ = reader.close();
            return None;
        }
        inner.rtcp_readers.insert(ssrc, reader.clone());
        Some(reader)
    }

    /// What the extractor reports as a stream it was given: passed on, unless it is a stream
    /// the sink gave it without knowing what it is (see `settle`).
    fn stream_found(&self, info: &StreamInfo) {
        if info.mime_type.is_empty() {
            return;
        }
        if let Some(on_stream_found) = &self.on_stream_found {
            on_stream_found(info);
        }
    }

    /// What the extractor reports as a pair: the second argument of
    /// `NewRTXInfoExtractorFactory` in upstream's transport.
    fn rtx_pair_found(&self, repair: u32, base: u32, rsid: &str) {
        {
            let mut inner = self.lock.lock();
            if inner.closed {
                return;
            }
            inner.repairs.insert(repair, base);
        }
        debugw!(self.logger, "rtx pair found from extension", "repair" => repair, "base" => base, "rsid" => rsid);
        self.buffer_factory.set_rtx_pair(repair, base, rsid);
        self.settle(None);
    }

    /// Does for the streams the connection did not announce what can be done by now:
    ///
    /// - one that is not known to repair another is given to the extractor, which puts its
    ///   probe on the stream's buffer, once the connection's header extensions are known.
    ///   pion did that for every SSRC it found, with the codec of the first packet's payload
    ///   type; which codec that is, the sink cannot know, so the stream goes without one and
    ///   is not passed on as found;
    /// - one that repairs a stream the connection announced is passed on as found, as a
    ///   retransmission stream with that stream's header extensions.
    ///
    /// Called whenever something was learned: the first packet of a stream (with that
    /// stream, the only one it can be about), an announcement, a pair.
    fn settle(&self, only: Option<u32>) {
        let mut to_probe = Vec::new();
        let mut to_announce = Vec::new();
        {
            let mut guard = self.lock.lock();
            let inner = &mut *guard;
            if inner.closed {
                return;
            }
            let unannounced: Vec<u32> = match only {
                Some(ssrc) => vec![ssrc],
                None => inner.streams.keys().copied().collect(),
            }
            .into_iter()
            .filter(|ssrc| {
                *ssrc != 0
                    && inner
                        .streams
                        .get(ssrc)
                        .is_some_and(|stream| stream.info.is_none())
            })
            .collect();
            for ssrc in unannounced {
                let base_info = match inner.repairs.get(&ssrc) {
                    Some(base) => Some(inner.streams.get(base).and_then(|b| b.info.clone())),
                    None => None,
                };
                let Some(stream) = inner.streams.get_mut(&ssrc) else {
                    continue;
                };
                match base_info {
                    None => {
                        if !stream.probed && !inner.header_extensions.is_empty() {
                            stream.probed = true;
                            to_probe.push(StreamInfo {
                                ssrc,
                                payload_type: stream.payload_type,
                                rtp_header_extensions: inner.header_extensions.clone(),
                                ..Default::default()
                            });
                        }
                    }
                    Some(Some(base_info)) => {
                        if !stream.found {
                            stream.found = true;
                            to_announce.push(StreamInfo {
                                ssrc,
                                payload_type: stream.payload_type,
                                rtp_header_extensions: base_info.rtp_header_extensions,
                                mime_type: MIME_TYPE_RTX.to_string(),
                                clock_rate: base_info.clock_rate,
                                ..Default::default()
                            });
                        }
                    }
                    // the stream it repairs was not announced (yet)
                    Some(None) => {}
                }
            }
        }

        if let Some((_, extractor)) = self.rtx_info_extractor.get() {
            for info in &to_probe {
                extractor.bind_remote_stream(info);
            }
        }
        if let Some(on_stream_found) = &self.on_stream_found {
            for info in &to_announce {
                on_stream_found(info);
            }
        }
    }
}

impl PacketSink for BufferFactorySink {
    fn on_rtp(&self, ssrc: Ssrc, packet: Bytes, arrival: Time) {
        let buffer = {
            let inner = self.lock.lock();
            if inner.closed {
                return;
            }
            inner.streams.get(&ssrc).map(|stream| stream.buffer.clone())
        };
        let buffer = match buffer {
            Some(buffer) => buffer,
            None => {
                // The first packet of an SSRC that was not announced.
                let payload_type = packet.get(1).map_or(0, |b| b & 0x7f);
                let Some(buffer) = self.open(ssrc, None, payload_type) else {
                    return;
                };
                self.settle(Some(ssrc));
                buffer
            }
        };
        // What cannot be read as RTP, and what comes for a buffer that was closed, is dropped.
        // The buffer's clock is the one the connection's times are of (`mono::unix_nano`).
        let _ = buffer.write_at(packet, arrival.unix_nano());
    }

    fn on_rtcp(&self, packets: &[Box<dyn rtcp::Packet>], _arrival: Time) {
        if self.lock.lock().closed {
            return;
        }
        if let Some(rtt_from_xr) = &self.rtt_from_xr {
            rtt_from_xr.read_rtcp(packets);
        }

        let mut compound_ssrcs: Option<Vec<u32>> = None;
        for packet in packets {
            let marshaled = match rtcp::packet::marshal(std::slice::from_ref(packet)) {
                Ok(marshaled) => marshaled,
                Err(err) => {
                    // Skip the packet that failed to remarshal so the remaining
                    // packets in the compound are still forwarded.
                    debugw!(self.logger, "could not marshal RTCP packet", "error" => err.to_string());
                    continue;
                }
            };

            let mut destinations = destination_ssrc(std::slice::from_ref(packet));
            if destinations.is_empty() {
                // Packets without a destination of their own (e.g. unknown packet
                // types parsed as rtcp.RawPacket) are delivered to every stream the
                // compound packet is addressed to instead of being dropped.
                destinations = compound_ssrcs
                    .get_or_insert_with(|| destination_ssrc(packets))
                    .clone();
            }

            for ssrc in destinations {
                if let Some(reader) = self.rtcp_reader(ssrc) {
                    let _ = reader.write(&marshaled);
                }
            }
        }
    }

    fn bind_remote_stream(&self, info: &StreamInfo) {
        if self
            .open(info.ssrc, Some(info), info.payload_type)
            .is_none()
        {
            return;
        }

        // What the description says about the stream's retransmission stream.
        if info.ssrc_retransmission != 0 {
            self.lock
                .lock()
                .repairs
                .insert(info.ssrc_retransmission, info.ssrc);
            self.buffer_factory
                .set_rtx_pair(info.ssrc_retransmission, info.ssrc, "");
        }

        // In the order upstream's transport adds the interceptors to its registry.
        if let Some(rtt_from_xr) = &self.rtt_from_xr {
            rtt_from_xr.bind_remote_stream(info);
        }
        self.extractor().1.bind_remote_stream(info);

        self.settle(None);
    }

    fn unbind_remote_stream(&self, info: &StreamInfo) {
        if let Some(rtt_from_xr) = &self.rtt_from_xr {
            rtt_from_xr.unbind_remote_stream(info);
        }

        let (buffers, readers) = {
            let mut guard = self.lock.lock();
            let inner = &mut *guard;
            let mut ssrcs = vec![info.ssrc];
            inner.repairs.retain(|&repair, &mut base| {
                if base == info.ssrc {
                    ssrcs.push(repair);
                }
                base != info.ssrc && repair != info.ssrc
            });
            let mut buffers = Vec::new();
            let mut readers = Vec::new();
            for ssrc in ssrcs {
                buffers.extend(inner.streams.remove(&ssrc).map(|stream| stream.buffer));
                readers.extend(inner.rtcp_readers.remove(&ssrc));
            }
            (buffers, readers)
        };
        for reader in readers {
            let _ = reader.close();
        }
        for buffer in buffers {
            let _ = buffer.close();
        }
    }

    fn bind_local_stream(&self, info: &StreamInfo) {
        if let Some(rtt_from_xr) = &self.rtt_from_xr {
            rtt_from_xr.bind_local_stream(info);
        }
    }

    fn unbind_local_stream(&self, info: &StreamInfo) {
        if let Some(rtt_from_xr) = &self.rtt_from_xr {
            rtt_from_xr.unbind_local_stream(info);
        }
    }

    fn closed(&self) {
        self.close();
    }
}

impl Drop for BufferFactorySink {
    fn drop(&mut self) {
        self.close();
    }
}

/// Whether a stream was announced with the three header extensions the probe reads.
fn has_probe_extensions(info: &StreamInfo) -> bool {
    [
        sdp::SDES_MID_URI,
        sdp::SDES_RTP_STREAM_ID_URI,
        SDES_REPAIR_RTP_STREAM_ID_URI,
    ]
    .into_iter()
    .all(|uri| {
        utils::get_header_extension_id(
            &info.rtp_header_extensions,
            &RtpHeaderExtensionCapability {
                uri: uri.to_string(),
            },
        ) != 0
    })
}

/// `destinationSSRC` of pion's SRTCP session: the SSRCs the packets are addressed to, each
/// once.
fn destination_ssrc(packets: &[Box<dyn rtcp::Packet>]) -> Vec<u32> {
    let mut out = Vec::new();
    for packet in packets {
        for ssrc in packet.destination_ssrc() {
            if !out.contains(&ssrc) {
                out.push(ssrc);
            }
        }
    }
    out
}

#[cfg(test)]
#[path = "sink_test.rs"]
mod sink_test;
