// Ported from LiveKit, pkg/sfu/pacer/base.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `send_packet` takes the packet, which owns its header
// and its payload, and drops it when it is done, where the original hands the header, the
// payload and the packet back to their pools; the two extensions it patches are made as
// `Bytes` for the header to hold, where the original marshals them into scratch buffers of
// the packet (so a send allocates for each of them, which the original's does not); the
// estimator that may be nil is an `Option`; the embedded `ProbeObserver` is a field; what
// `send_packet` fails with, an error of the header's extensions or of the write, is one
// `Error` enum; a size is a `usize`.

use std::sync::Arc;

use bytes::Bytes;

use crate::errorw;
use crate::go::atomic;
use crate::go::time::Duration;
use crate::mediatransportutil;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::bwe::Bwe;
use crate::webrtc;
use crate::webrtc::rtp;

use super::{Packet, ProbeObserver};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// What setting an extension on the header failed with.
    #[error(transparent)]
    Rtp(#[from] rtp::Error),
    /// What the write stream failed with.
    #[error(transparent)]
    Write(#[from] webrtc::Error),
}

pub struct Base {
    logger: Logger,

    bwe: Option<Arc<dyn Bwe>>,

    last_packet_sent_at: atomic::Int64,

    pub probe_observer: ProbeObserver,
}

impl Base {
    pub fn new(logger: Logger, bwe: Option<Arc<dyn Bwe>>) -> Base {
        Base {
            logger: logger.clone(),
            bwe,
            last_packet_sent_at: atomic::Int64::new(0),
            probe_observer: ProbeObserver::new(logger),
        }
    }

    pub fn set_interval(&self, _interval: Duration) {}

    pub fn set_bitrate(&self, _bitrate: i64) {}

    pub fn time_since_last_sent_packet(&self) -> Duration {
        Duration(mono::unix_nano().wrapping_sub(self.last_packet_sent_at.load()))
    }

    pub fn send_packet(&self, mut p: Packet) -> Result<usize, Error> {
        // The original's deferred function hands the header, the payload and the packet back
        // to their pools: here they are dropped with `p` when this returns.

        if let Err(err) = self.patch_rtp_header_extensions(&mut p) {
            errorw!(self.logger, "patching rtp header extensions err", err);
            return Err(err.into());
        }

        let written = match p.write_stream.write_rtp(&p.header, &p.payload) {
            Ok(written) => written,
            Err(err) => {
                if err != webrtc::Error::ClosedPipe {
                    errorw!(self.logger, "write rtp packet failed", err);
                }
                return Err(err.into());
            }
        };

        Ok(written)
    }

    // patch just abs-send-time and transport-cc extensions if applicable
    fn patch_rtp_header_extensions(&self, p: &mut Packet) -> Result<(), rtp::Error> {
        let sending_at = mono::now();
        if p.abs_send_time_ext_id != 0 {
            let abs_send_time_ext = rtp::AbsSendTimeExtension {
                timestamp: mediatransportutil::to_ntp_time(sending_at).0 >> 14,
            };
            let mut abs_send_time_buf = [0u8; 3];
            abs_send_time_ext.marshal_to(&mut abs_send_time_buf)?;

            p.header.set_extension(
                p.abs_send_time_ext_id,
                Bytes::copy_from_slice(&abs_send_time_buf),
            )?;

            self.last_packet_sent_at.store(sending_at.unix_nano());
        }

        let packet_size = p.header_size + p.payload.len() + p.header.padding_size as usize;
        if p.transport_wide_ext_id != 0
            && let Some(bwe) = &self.bwe
        {
            let twcc_sn = bwe.record_packet_send_and_get_sequence_number(
                sending_at.unix_micro(),
                packet_size,
                p.is_rtx,
                p.probe_cluster_id,
                p.is_probe,
            );
            let twcc_ext = rtp::TransportCcExtension {
                transport_sequence: twcc_sn,
            };
            let mut twcc_buf = [0u8; 2];
            twcc_ext.marshal_to(&mut twcc_buf)?;

            p.header
                .set_extension(p.transport_wide_ext_id, Bytes::copy_from_slice(&twcc_buf))?;

            self.last_packet_sent_at.store(sending_at.unix_nano());
        }

        self.probe_observer
            .record_packet(packet_size, p.is_rtx, p.probe_cluster_id, p.is_probe);
        Ok(())
    }
}

// ------------------------------------------------

#[cfg(test)]
#[path = "base_test.rs"]
pub(super) mod base_test;
