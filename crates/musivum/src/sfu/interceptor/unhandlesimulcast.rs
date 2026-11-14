// Ported from LiveKit, pkg/sfu/interceptor/unhandlesimulcast.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - Upstream this is a pion interceptor that wraps the reader pion probes a stream through,
//   so that pion finds a mid and a rid on the first packets of a stream whose SSRC is known
//   from elsewhere (a migrated publisher) and places it in its simulcast track. Nothing can
//   call it to that effect here: `rtc` decides which stream a packet belongs to from the
//   packet as it was decrypted, before any interceptor or the connection's sink sees it. The
//   logic is ported as it is, on a buffer the caller has read a packet into, for the day the
//   library has a place for it. What the connection's sink does with simulcast streams is in
//   `sink.rs`.
// - `BindRemoteStream(info, reader)`, which returned a reader that wraps the one it was given
//   or that one itself, is `bind_remote_stream(info)`, which returns the reader or `None`.
//   `unhandleSimulcastRTPReader.Read(b, a)`, which read from the wrapped reader into `b` and
//   then worked on `b`, is `read(b, n)` for a caller that has read `n` bytes into `b`; like
//   the original it returns how many bytes of `b` the packet has afterwards. The reader type
//   is public, since the caller holds it. pion's attributes and the rest of
//   `interceptor.NoOp` have no counterpart.
// - An option cannot fail (the one there is never does), so `new` of the factory and
//   `new_interceptor` return the value. `MarshalLogObject` is `Debug`.

use std::collections::HashMap;

use bytes::Bytes;

use crate::debugw;
use crate::protocol::logger::Logger;
use crate::sfu::utils;
use crate::webrtc;
use crate::webrtc::rtp;
use crate::webrtc::sdp;

const SIMULCAST_PROBE_COUNT: i64 = 10;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimulcastTrackInfo {
    pub mid: String,
    pub stream_id: String,
    pub repair_ssrc: u32, // set only when `IsRepairStream: false`, i. e. RTX SSRC for the primary stream
    pub is_repair_stream: bool,
}

// -------------------------------------------------------------------

pub type UnhandleSimulcastOption = Box<dyn Fn(&mut UnhandleSimulcastInterceptor) + Send + Sync>;

pub fn unhandle_simulcast_tracks(
    logger: Logger,
    tracks: HashMap<u32, SimulcastTrackInfo>,
) -> UnhandleSimulcastOption {
    Box::new(move |u| {
        u.logger = logger.clone();
        u.sim_tracks = tracks.clone();
    })
}

pub struct UnhandleSimulcastInterceptorFactory {
    opts: Vec<UnhandleSimulcastOption>,
}

impl UnhandleSimulcastInterceptorFactory {
    pub fn new_interceptor(&self, _id: &str) -> UnhandleSimulcastInterceptor {
        let mut i = UnhandleSimulcastInterceptor {
            logger: Logger::default(),
            sim_tracks: HashMap::new(),
        };
        for o in &self.opts {
            o(&mut i);
        }
        i
    }

    pub fn new(opts: Vec<UnhandleSimulcastOption>) -> Self {
        Self { opts }
    }
}

pub struct UnhandleSimulcastRtpReader {
    simulcast_track_info: SimulcastTrackInfo,
    logger: Logger,
    try_times: i64,
    mid_ext_id: u8,
    rid_ext_id: u8,
    rsid_ext_id: u8,
}

impl UnhandleSimulcastRtpReader {
    pub fn read(&mut self, b: &mut [u8], n: usize) -> usize {
        if self.try_times < 0 {
            return n;
        }

        let mut header = rtp::Header::default();
        let Ok(hsize) = header.unmarshal(&Bytes::copy_from_slice(&b[..n])) else {
            return n;
        };
        let (mut mid, mut rid, rsid) = (String::new(), String::new(), String::new());
        if let Some(payload) = header.get_extension(self.mid_ext_id) {
            mid = String::from_utf8_lossy(&payload).into_owned();
        }

        if let Some(payload) = header.get_extension(self.rid_ext_id) {
            rid = String::from_utf8_lossy(&payload).into_owned();
        }

        // (the original puts the repair stream's id into `rid` as well, and `rsid` stays
        // empty)
        if let Some(payload) = header.get_extension(self.rsid_ext_id) {
            rid = String::from_utf8_lossy(&payload).into_owned();
        }

        if !mid.is_empty() && (!rid.is_empty() || !rsid.is_empty()) {
            debugw!(
                self.logger,
                "unhandle stream found",
                "mid" => mid,
                "rid" => rid,
                "rsid" => rsid,
                "ssrc" => header.ssrc,
                "simulcastTrackInfo" => self.simulcast_track_info,
            );
            self.try_times = -1;
            return n;
        } else {
            // ignore padding only packet for probe count
            if !header.padding || n as i64 - header.marshal_size() as i64 - b[n - 1] as i64 != 0 {
                self.try_times -= 1;
            }
        }

        // (an extension that cannot be set is left out, as the original ignores the error)
        if mid.is_empty() {
            let _ = header.set_extension(
                self.mid_ext_id,
                Bytes::copy_from_slice(self.simulcast_track_info.mid.as_bytes()),
            );
        }
        if rid.is_empty() && !self.simulcast_track_info.is_repair_stream {
            let _ = header.set_extension(
                self.rid_ext_id,
                Bytes::copy_from_slice(self.simulcast_track_info.stream_id.as_bytes()),
            );
        }
        if rsid.is_empty() && self.simulcast_track_info.is_repair_stream {
            let _ = header.set_extension(
                self.rsid_ext_id,
                Bytes::copy_from_slice(self.simulcast_track_info.stream_id.as_bytes()),
            );
        }

        let hsize2 = header.marshal_size();

        if hsize2 as i64 - hsize as i64 + n as i64 > b.len() as i64 {
            // no enough buf to set extension
            return n;
        }
        b.copy_within(hsize..n, hsize2);
        let _ = header.marshal_to(b);
        debugw!(
            self.logger,
            "unhandle stream injecting",
            "mid" => mid,
            "rid" => rid,
            "rsid" => rsid,
            "ssrc" => header.ssrc,
            "simulcastTrackInfo" => self.simulcast_track_info,
        );
        (hsize2 as i64 - hsize as i64 + n as i64) as usize
    }
}

pub struct UnhandleSimulcastInterceptor {
    logger: Logger,
    sim_tracks: HashMap<u32, SimulcastTrackInfo>,
}

impl UnhandleSimulcastInterceptor {
    pub fn bind_remote_stream(
        &self,
        info: &webrtc::StreamInfo,
    ) -> Option<UnhandleSimulcastRtpReader> {
        if let Some(t) = self.sim_tracks.get(&info.ssrc) {
            let mid_extension_id = utils::get_header_extension_id(
                &info.rtp_header_extensions,
                &webrtc::RtpHeaderExtensionCapability {
                    uri: sdp::SDES_MID_URI.to_string(),
                },
            );
            let stream_id_extension_id = utils::get_header_extension_id(
                &info.rtp_header_extensions,
                &webrtc::RtpHeaderExtensionCapability {
                    uri: sdp::SDES_RTP_STREAM_ID_URI.to_string(),
                },
            );
            let repair_stream_id_extension_id = utils::get_header_extension_id(
                &info.rtp_header_extensions,
                &webrtc::RtpHeaderExtensionCapability {
                    uri: sdp::SDES_REPAIR_RTP_STREAM_ID_URI.to_string(),
                },
            );
            if mid_extension_id == 0
                || stream_id_extension_id == 0
                || repair_stream_id_extension_id == 0
            {
                return None;
            }

            return Some(UnhandleSimulcastRtpReader {
                simulcast_track_info: t.clone(),
                logger: self.logger.clone(),
                try_times: SIMULCAST_PROBE_COUNT,
                mid_ext_id: mid_extension_id as u8,
                rid_ext_id: stream_id_extension_id as u8,
                rsid_ext_id: repair_stream_id_extension_id as u8,
            });
        }
        None
    }
}

#[cfg(test)]
#[path = "unhandlesimulcast_test.rs"]
mod unhandlesimulcast_test;
