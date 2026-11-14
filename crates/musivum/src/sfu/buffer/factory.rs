// Ported from LiveKit, pkg/sfu/buffer/factory.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; pion's `packetio.BufferPacketType` is the enum
// `BufferPacketType`, and the `io.ReadWriteCloser` that `GetOrNew` returns, which is a
// `*Buffer` or a `*RTCPReader` by the packet type, is the enum `ReadWriteCloser` (it has
// `write` and `close`, and `into_buffer` and `into_rtcp_reader` where the original asserts
// the type; neither a buffer nor an RTCP reader has pion's `Read`); a factory is made as an
// `Arc`, and the buffers and readers it makes point back to it weakly from their `OnClose`;
// what the lock guards is `FactoryInner`.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::RwLock;

use super::{Buffer, Error, RtcpReader, StreamInfoProbe};

/// pion's `packetio.BufferPacketType`: the type of the packets a buffer of the factory is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BufferPacketType {
    // RTPBufferPacket indicates the Buffer that is handling RTP packets
    Rtp,
    // RTCPBufferPacket indicates the Buffer that is handling RTCP packets
    Rtcp,
}

/// What `Factory::get_or_new` gives: Go's `io.ReadWriteCloser`, which is a buffer for RTP and
/// an RTCP reader for RTCP.
#[derive(Clone)]
pub enum ReadWriteCloser {
    Buffer(Arc<Buffer>),
    RtcpReader(Arc<RtcpReader>),
}

impl ReadWriteCloser {
    pub fn write(&self, p: Bytes) -> Result<usize, Error> {
        match self {
            ReadWriteCloser::Buffer(buffer) => buffer.write(p),
            ReadWriteCloser::RtcpReader(reader) => reader.write(&p),
        }
    }

    pub fn close(&self) -> Result<(), Error> {
        match self {
            ReadWriteCloser::Buffer(buffer) => buffer.close(),
            ReadWriteCloser::RtcpReader(reader) => reader.close(),
        }
    }

    /// `.(*Buffer)`
    pub fn into_buffer(self) -> Option<Arc<Buffer>> {
        match self {
            ReadWriteCloser::Buffer(buffer) => Some(buffer),
            ReadWriteCloser::RtcpReader(_) => None,
        }
    }

    /// `.(*RTCPReader)`
    pub fn into_rtcp_reader(self) -> Option<Arc<RtcpReader>> {
        match self {
            ReadWriteCloser::Buffer(_) => None,
            ReadWriteCloser::RtcpReader(reader) => Some(reader),
        }
    }
}

pub struct FactoryOfBufferFactory {
    tracking_packets_video: usize,
    tracking_packets_audio: usize,
}

impl FactoryOfBufferFactory {
    pub fn new(tracking_packets_video: usize, tracking_packets_audio: usize) -> Self {
        Self {
            tracking_packets_video,
            tracking_packets_audio,
        }
    }

    pub fn create_buffer_factory(&self) -> Arc<Factory> {
        Arc::new_cyclic(|this| Factory {
            this: this.clone(),
            tracking_packets_video: self.tracking_packets_video,
            tracking_packets_audio: self.tracking_packets_audio,
            lock: RwLock::new(FactoryInner {
                rtp_buffers: HashMap::new(),
                rtcp_readers: HashMap::new(),
                rtx_pair: HashMap::new(),
                fec_pair: HashMap::new(),
            }),
        })
    }
}

pub struct Factory {
    this: Weak<Factory>,
    tracking_packets_video: usize,
    tracking_packets_audio: usize,
    lock: RwLock<FactoryInner>,
}

// What the lock guards.
struct FactoryInner {
    rtp_buffers: HashMap<u32, Arc<Buffer>>,
    rtcp_readers: HashMap<u32, Arc<RtcpReader>>,
    rtx_pair: HashMap<u32, u32>, // repair -> base
    fec_pair: HashMap<u32, u32>, // fec -> base
}

impl Factory {
    pub fn get_or_new(&self, packet_type: BufferPacketType, ssrc: u32) -> ReadWriteCloser {
        let mut f = self.lock.write();
        match packet_type {
            BufferPacketType::Rtcp => {
                if let Some(reader) = f.rtcp_readers.get(&ssrc) {
                    return ReadWriteCloser::RtcpReader(reader.clone());
                }
                let reader = RtcpReader::new(ssrc);
                f.rtcp_readers.insert(ssrc, reader.clone());
                let this = self.this.clone();
                reader.on_close(move || {
                    if let Some(this) = this.upgrade() {
                        let mut f = this.lock.write();
                        f.rtcp_readers.remove(&ssrc);
                    }
                });
                ReadWriteCloser::RtcpReader(reader)
            }
            BufferPacketType::Rtp => {
                if let Some(reader) = f.rtp_buffers.get(&ssrc) {
                    return ReadWriteCloser::Buffer(reader.clone());
                }
                let buffer = Buffer::new(
                    ssrc,
                    self.tracking_packets_video,
                    self.tracking_packets_audio,
                );
                f.rtp_buffers.insert(ssrc, buffer.clone());
                for (&repair, &base) in &f.rtx_pair {
                    if repair == ssrc {
                        if let Some(base_buffer) = f.rtp_buffers.get(&base) {
                            buffer.set_primary_buffer_for_rtx(base_buffer.clone());
                        }
                        break;
                    } else if base == ssrc {
                        if let Some(repair_buffer) = f.rtp_buffers.get(&repair) {
                            repair_buffer.set_primary_buffer_for_rtx(buffer.clone());
                        }
                        break;
                    }
                }
                for (&fec, &base) in &f.fec_pair {
                    if fec == ssrc {
                        buffer.set_fec_repair_buffer_limit();
                        if let Some(base_buffer) = f.rtp_buffers.get(&base) {
                            buffer.set_primary_buffer_for_fec(base_buffer.clone());
                        }
                        break;
                    } else if base == ssrc {
                        if let Some(fec_buffer) = f.rtp_buffers.get(&fec) {
                            fec_buffer.set_primary_buffer_for_fec(buffer.clone());
                        }
                        break;
                    }
                }
                let this = self.this.clone();
                buffer.on_close(move || {
                    if let Some(this) = this.upgrade() {
                        let mut f = this.lock.write();
                        f.rtp_buffers.remove(&ssrc);
                        f.rtx_pair.remove(&ssrc);
                        f.fec_pair.remove(&ssrc);
                    }
                });
                ReadWriteCloser::Buffer(buffer)
            }
        }
    }

    pub fn get_buffer_pair(&self, ssrc: u32) -> (Option<Arc<Buffer>>, Option<Arc<RtcpReader>>) {
        let f = self.lock.read();
        (
            f.rtp_buffers.get(&ssrc).cloned(),
            f.rtcp_readers.get(&ssrc).cloned(),
        )
    }

    pub fn get_buffer(&self, ssrc: u32) -> Option<Arc<Buffer>> {
        let f = self.lock.read();
        f.rtp_buffers.get(&ssrc).cloned()
    }

    pub fn get_rtcp_reader(&self, ssrc: u32) -> Option<Arc<RtcpReader>> {
        let f = self.lock.read();
        f.rtcp_readers.get(&ssrc).cloned()
    }

    // SetStreamInfoProbe installs probe on the buffer of ssrc, reporting whether that
    // buffer exists. False means the stream can never be identified.
    pub fn set_stream_info_probe(&self, ssrc: u32, probe: Option<StreamInfoProbe>) -> bool {
        let buffer = {
            let f = self.lock.read();
            f.rtp_buffers.get(&ssrc).cloned()
        };

        let Some(buffer) = buffer else {
            return false;
        };

        buffer.set_stream_info_probe(probe);
        true
    }

    pub fn set_rtx_pair(&self, repair: u32, base: u32, rsid: &str) {
        let (repair_buffer, base_buffer) = {
            let mut f = self.lock.write();
            let (repair_buffer, base_buffer) = (
                f.rtp_buffers.get(&repair).cloned(),
                f.rtp_buffers.get(&base).cloned(),
            );
            if repair_buffer.is_none() || base_buffer.is_none() {
                f.rtx_pair.insert(repair, base);
            }
            (repair_buffer, base_buffer)
        };
        if let (Some(repair_buffer), Some(base_buffer)) = (repair_buffer, base_buffer) {
            repair_buffer.set_primary_buffer_for_rtx(base_buffer.clone());
            if !rsid.is_empty() {
                base_buffer.notify_rtx(base, repair, rsid);
            }
        }
    }

    pub fn set_fec_pair(&self, fec: u32, base: u32) {
        let (fec_buffer, base_buffer) = {
            let mut f = self.lock.write();
            let (fec_buffer, base_buffer) = (
                f.rtp_buffers.get(&fec).cloned(),
                f.rtp_buffers.get(&base).cloned(),
            );
            if fec_buffer.is_none() || base_buffer.is_none() {
                f.fec_pair.insert(fec, base);
            }
            (fec_buffer, base_buffer)
        };
        if let (Some(fec_buffer), None) = (&fec_buffer, &base_buffer) {
            fec_buffer.set_fec_repair_buffer_limit();
        }
        if let (Some(fec_buffer), Some(base_buffer)) = (fec_buffer, base_buffer) {
            fec_buffer.set_primary_buffer_for_fec(base_buffer);
        }
    }
}
