// Ported from LiveKit, pkg/sfu/interceptor/rtx.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - Upstream these are a factory of pion interceptors and the interceptor it makes for a
//   connection. Here nothing registers them: the connection's sink (`sink.rs`) calls them.
//   `NewInterceptor(id)` cannot fail and returns the extractor itself. `BindRemoteStream(info,
//   reader)`, which returned the reader it was given, is `bind_remote_stream(info)`. The rest of
//   pion's `interceptor.NoOp` has no counterpart.
// - pion's `interceptor.StreamInfo` is `webrtc::StreamInfo`; the unexported `streamInfo` of
//   this file keeps its name, in Rust's spelling.
// - The factory is made as an `Arc` (the extractor and the sink hold it), and what its lock
//   guards is `RtxInfoExtractorFactoryInner`. `paired`, a set, is a `HashSet`.
// - The probe is handed `SetStreamInfo` as its `OnFound`. Here it holds the factory weakly:
//   the probe lives in a buffer of the buffer factory, which this factory holds as its prober.
// - `StreamInfoProber` is implemented for `buffer::Factory` here, where Go needs no word for
//   it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::SimulcastTrackInfo;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::utils;
use crate::warnw;
use crate::webrtc;
use crate::webrtc::sdp;

pub const SDES_REPAIR_RTP_STREAM_ID_URI: &str =
    "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id";

const RTX_PROBE_COUNT: i64 = 10;

// StreamInfoProber installs a bounded probe that identifies a remote stream from the
// mid/rid/rsid header extensions of its packets. Implemented by buffer.Factory.
pub trait StreamInfoProber: Send + Sync {
    fn set_stream_info_probe(&self, ssrc: u32, probe: Option<buffer::StreamInfoProbe>) -> bool;
}

impl StreamInfoProber for buffer::Factory {
    fn set_stream_info_probe(&self, ssrc: u32, probe: Option<buffer::StreamInfoProbe>) -> bool {
        buffer::Factory::set_stream_info_probe(self, ssrc, probe)
    }
}

struct StreamInfo {
    mid: String,
    rid: String,
    rsid: String,
}

type OnStreamFound = Box<dyn Fn(&webrtc::StreamInfo) + Send + Sync>;
// func(repair, base uint32, rsid string)
type OnRtxPairFound = Box<dyn Fn(u32, u32, &str) + Send + Sync>;

pub struct RtxInfoExtractorFactory {
    this: Weak<RtxInfoExtractorFactory>,
    on_stream_found: OnStreamFound,
    on_rtx_pair_found: OnRtxPairFound,
    prober: Arc<dyn StreamInfoProber>,
    lock: Mutex<RtxInfoExtractorFactoryInner>,
    logger: Logger,
}

// What the lock guards.
struct RtxInfoExtractorFactoryInner {
    streams: HashMap<u32, StreamInfo>,
    paired: HashSet<u32>,
}

impl RtxInfoExtractorFactory {
    pub fn new(
        on_stream_found: impl Fn(&webrtc::StreamInfo) + Send + Sync + 'static,
        on_rtx_pair_found: impl Fn(u32, u32, &str) + Send + Sync + 'static, // repair, base, rsid
        prober: Arc<dyn StreamInfoProber>,
        sim_tracks: &HashMap<u32, SimulcastTrackInfo>,
        logger: Logger,
    ) -> Arc<Self> {
        let f = Arc::new_cyclic(|this| Self {
            this: this.clone(),
            on_stream_found: Box::new(on_stream_found),
            on_rtx_pair_found: Box::new(on_rtx_pair_found),
            prober,
            lock: Mutex::new(RtxInfoExtractorFactoryInner {
                streams: HashMap::new(),
                paired: HashSet::new(),
            }),
            logger,
        });
        f.seed_simulcast_tracks(sim_tracks);
        f
    }

    // seedSimulcastTracks pairs migrated streams from the migration info. A migrated client
    // is mid-stream and stops sending rid/rsid, so the extensions never appear on the wire
    // and the pairing has to come from what is already known about the tracks.
    fn seed_simulcast_tracks(&self, sim_tracks: &HashMap<u32, SimulcastTrackInfo>) {
        for (&ssrc, info) in sim_tracks {
            if info.mid.is_empty() || info.stream_id.is_empty() {
                continue;
            }

            if info.is_repair_stream {
                self.set_stream_info(ssrc, &info.mid, "", &info.stream_id);
                continue;
            }

            self.set_stream_info(ssrc, &info.mid, &info.stream_id, "");
            if info.repair_ssrc != 0 {
                self.set_stream_info(info.repair_ssrc, &info.mid, "", &info.stream_id);
            }
        }
    }

    pub fn new_interceptor(&self, _id: &str) -> RtxInfoExtractor {
        RtxInfoExtractor {
            factory: self
                .this
                .upgrade()
                .expect("the factory is alive while it is called"),
            logger: self.logger.clone(),
        }
    }

    pub fn set_stream_info(&self, ssrc: u32, mid: &str, rid: &str, rsid: &str) {
        let (mut repair_ssrc, mut base_ssrc) = (0u32, 0u32);
        let mut repair_sid = "";
        {
            let mut f = self.lock.lock();

            if mid.is_empty() || (rid.is_empty() && rsid.is_empty()) {
                return;
            }

            // the same stream can be reported by both the packet probe and the migration info
            if f.paired.contains(&ssrc) {
                return;
            }

            // (the entry that is found is taken out after the search, where Go deletes it
            // from the map it is ranging over)
            if !rsid.is_empty() {
                // repair stream found, find base stream
                let found = f
                    .streams
                    .iter()
                    .find(|(_, info)| info.mid == mid && info.rid == rsid)
                    .map(|(&base, _)| base);
                if let Some(base) = found {
                    repair_ssrc = ssrc;
                    base_ssrc = base;
                    repair_sid = rsid;
                    f.streams.remove(&base);
                }
            } else {
                // base stream found, find repair stream
                let found = f
                    .streams
                    .iter()
                    .find(|(_, info)| info.mid == mid && info.rsid == rid)
                    .map(|(&repair, _)| repair);
                if let Some(repair) = found {
                    repair_ssrc = repair;
                    base_ssrc = ssrc;
                    repair_sid = rid;
                    f.streams.remove(&repair);
                }
            }

            // no rtx pair found, save it for later
            if repair_ssrc == 0 || base_ssrc == 0 {
                f.streams.insert(
                    ssrc,
                    StreamInfo {
                        mid: mid.to_string(),
                        rid: rid.to_string(),
                        rsid: rsid.to_string(),
                    },
                );
                return;
            }

            f.paired.insert(repair_ssrc);
            f.paired.insert(base_ssrc);
        }

        (self.on_rtx_pair_found)(repair_ssrc, base_ssrc, repair_sid);
    }
}

// ------------------------------------------

pub struct RtxInfoExtractor {
    factory: Arc<RtxInfoExtractorFactory>,
    logger: Logger,
}

impl RtxInfoExtractor {
    pub fn bind_remote_stream(&self, info: &webrtc::StreamInfo) {
        (self.factory.on_stream_found)(info);

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
                uri: SDES_REPAIR_RTP_STREAM_ID_URI.to_string(),
            },
        );
        if mid_extension_id == 0
            || stream_id_extension_id == 0
            || repair_stream_id_extension_id == 0
        {
            return;
        }

        // Probe on the buffer write path rather than by wrapping this reader. Remote streams
        // are consumed through SettingEngine.BufferFactory, so nothing here reads the
        // interceptor chain. pion used to drive the repair stream reader, but since
        // pion/webrtc#3470 it only does so when the application reads the TrackRemote or
        // when no BufferFactory is set, neither of which holds.
        let factory = self.factory.this.clone();
        let ok = self.factory.prober.set_stream_info_probe(
            info.ssrc,
            Some(buffer::StreamInfoProbe {
                mid_ext_id: mid_extension_id as u8,
                rid_ext_id: stream_id_extension_id as u8,
                rsid_ext_id: repair_stream_id_extension_id as u8,
                tries: RTX_PROBE_COUNT,
                on_found: Arc::new(move |ssrc, mid, rid, rsid| {
                    if let Some(factory) = factory.upgrade() {
                        factory.set_stream_info(ssrc, mid, rid, rsid);
                    }
                }),
            }),
        );
        if !ok {
            warnw!(
                self.logger,
                "could not install stream info probe, rtx pairing will not work", nil,
                "ssrc" => info.ssrc,
                "mime" => info.mime_type,
            );
        }
    }
}

#[cfg(test)]
#[path = "rtx_test.rs"]
mod rtx_test;
