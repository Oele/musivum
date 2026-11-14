// Ported from LiveKit, pkg/sfu/pacer/pacer.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A `Packet` owns its header and its payload, so the
// pools are gone: no `PacketFactory`, no `HeaderPool`, `Pool` or `PoolEntity`, and nothing
// is handed back after a send. The scratch buffers are gone with them: an extension of the
// header is a `Bytes` that the header holds itself. `hold_extension` therefore gives the
// copy of the extension back as `Bytes`, for the caller to set on the header, and refuses
// what the original refuses (more than two per packet, or one longer than
// `MAX_HELD_EXTENSION_SIZE`); the abs-send-time and transport-cc extensions are made where
// they are patched (base.rs). The count of held extensions is private, so a packet is made
// with `Packet::new`, which takes the write stream (the one field without a zero value),
// and its other fields are set after that. `PacerBehavior`, a string type, is an enum whose
// `Display` and `FromStr` have the texts. The interfaces are traits whose methods take
// `&self`; a listener that may be nil is an `Option`. The tests at the end are new.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;

use crate::go::time::Duration;
use crate::sfu::ccutils;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::webrtc;
use crate::webrtc::rtp;

// --------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PacerBehavior {
    PassThrough,
    NoQueue,
    Leakybucket,
}

impl PacerBehavior {
    pub fn as_str(&self) -> &'static str {
        match self {
            PacerBehavior::PassThrough => "pass-through",
            PacerBehavior::NoQueue => "no-queue",
            PacerBehavior::Leakybucket => "leaky-bucket",
        }
    }
}

impl fmt::Display for PacerBehavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Go's `PacerBehavior(s)` of a string from the configuration. Any other text, the empty one
// too, is an error here, where the original has a value that equals none of the constants.
impl FromStr for PacerBehavior {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pass-through" => Ok(PacerBehavior::PassThrough),
            "no-queue" => Ok(PacerBehavior::NoQueue),
            "leaky-bucket" => Ok(PacerBehavior::Leakybucket),
            _ => Err(()),
        }
    }
}

// largest extension HoldExtension copies, an inline dependency descriptor or an abs-capture-time
pub(super) const MAX_HELD_EXTENSION_SIZE: usize = if dd::MAX_INLINE_EXTENSION_SIZE > act::MAX_SIZE {
    dd::MAX_INLINE_EXTENSION_SIZE
} else {
    act::MAX_SIZE
};

// `len(p.extBufs)`: how many extensions the original's scratch has room for
const NUM_HELD_EXTENSIONS: usize = 2;

#[derive(Clone)]
pub struct Packet {
    pub header: rtp::Header,
    pub header_size: usize,
    pub payload: Bytes,
    pub is_rtx: bool,
    pub probe_cluster_id: ccutils::ProbeClusterId,
    pub is_probe: bool,
    pub abs_send_time_ext_id: u8,
    pub transport_wide_ext_id: u8,
    pub write_stream: Arc<dyn webrtc::TrackLocalWriter>,

    // how many header extensions are held before Enqueue,
    // the Packet is owned by one send until SendPacket returns
    num_ext_held: usize,
}

impl Packet {
    // Go's `Packet{WriteStream: write_stream}`: every other field has its zero value.
    pub fn new(write_stream: Arc<dyn webrtc::TrackLocalWriter>) -> Packet {
        Packet {
            header: rtp::Header::default(),
            header_size: 0,
            payload: Bytes::new(),
            is_rtx: false,
            probe_cluster_id: ccutils::PROBE_CLUSTER_ID_INVALID,
            is_probe: false,
            abs_send_time_ext_id: 0,
            transport_wide_ext_id: 0,
            write_stream,
            num_ext_held: 0,
        }
    }

    // HoldExtension copies ext into the next free slot, which the header can hold until SendPacket returns.
    // It returns None when ext does not fit or every slot is held.
    pub fn hold_extension(&mut self, ext: &[u8]) -> Option<Bytes> {
        if self.num_ext_held == NUM_HELD_EXTENSIONS || ext.len() > MAX_HELD_EXTENSION_SIZE {
            return None;
        }
        self.num_ext_held += 1;
        Some(Bytes::copy_from_slice(ext))
    }
}

#[cfg_attr(test, musivum_fakes::fake)]
pub trait Pacer: Send + Sync {
    fn enqueue(&self, p: Packet);
    fn stop(&self);

    fn set_interval(&self, interval: Duration);
    fn set_bitrate(&self, bitrate: i64);

    fn time_since_last_sent_packet(&self) -> Duration;

    fn set_pacer_probe_observer_listener(
        &self,
        listener: Option<Arc<dyn PacerProbeObserverListener>>,
    );
    fn start_probe_cluster(&self, pci: ccutils::ProbeClusterInfo);
    fn end_probe_cluster(
        &self,
        probe_cluster_id: ccutils::ProbeClusterId,
    ) -> ccutils::ProbeClusterInfo;
}

pub trait PacerProbeObserverListener: Send + Sync {
    fn on_pacer_probe_observer_cluster_complete(&self, probe_cluster_id: ccutils::ProbeClusterId);
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::base::base_test::{RecWriter, plain_packet};
    use super::*;

    // New for musivum: the original has no tests of this file. The texts are the
    // original's, which the configuration names a pacer by.
    #[test]
    fn the_behaviors_have_the_texts_of_the_original() {
        for (behavior, text) in [
            (PacerBehavior::PassThrough, "pass-through"),
            (PacerBehavior::NoQueue, "no-queue"),
            (PacerBehavior::Leakybucket, "leaky-bucket"),
        ] {
            assert_eq!(text, behavior.to_string());
            assert_eq!(Ok(behavior), text.parse());
        }
        assert_eq!(Err(()), "".parse::<PacerBehavior>());
        assert_eq!(Err(()), "leaky".parse::<PacerBehavior>());
    }

    // New for musivum: the original's test of `HoldExtension` stops at the size. A packet
    // holds two extensions, as the original's scratch does, an empty one counting as one.
    #[test]
    fn a_packet_holds_two_extensions() {
        let mut p = Packet::new(RecWriter::new());
        assert_eq!(Some(Bytes::new()), p.hold_extension(&[]));
        assert_eq!(Some(Bytes::from_static(&[1])), p.hold_extension(&[1]));
        assert_eq!(None, p.hold_extension(&[2]));
        assert_eq!(None, p.hold_extension(&[]));
        // a copy of the packet holds what the packet held
        assert_eq!(None, p.clone().hold_extension(&[2]));
    }

    // New for musivum: the fake of `Pacer`, which the tests of what sends through a pacer
    // use, records the packets it is given.
    #[test]
    fn a_fake_pacer_records_what_it_is_given() {
        let fake = Arc::new(FakePacer::default());
        let p: Arc<dyn Pacer> = fake.clone();

        p.enqueue(plain_packet(7, &RecWriter::new()));
        assert_eq!(1, fake.enqueue_call_count());
        let packet = fake.enqueue_args_for_call(0);
        assert_eq!(7, packet.header.sequence_number);
        assert_eq!(488, packet.payload.len());

        assert_eq!(Duration::ZERO, p.time_since_last_sent_packet());
        fake.end_probe_cluster_returns(ccutils::ProbeClusterInfo {
            id: ccutils::ProbeClusterId(3),
            ..Default::default()
        });
        let pci = p.end_probe_cluster(ccutils::ProbeClusterId(3));
        assert_eq!(ccutils::ProbeClusterId(3), pci.id);
        assert_eq!(
            ccutils::ProbeClusterId(3),
            fake.end_probe_cluster_args_for_call(0)
        );
        p.stop();
        assert_eq!(1, fake.stop_call_count());
    }
}
