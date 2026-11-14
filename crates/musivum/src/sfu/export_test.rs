// Ported from LiveKit, pkg/sfu/export_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The file is a module of downtrack.rs, compiled for
// tests only, and what it adds to `DownTrack` is seen by the tests of that file (the
// original needs it because its tests are in another package). `SetRTXBudgetForTest` is
// not here: the budget of a down track is a field that nothing changes after `new`, so the
// test that wants another budget makes its down track with it (`DownTrackParams.rtx_budget`),
// which gives the same budget.

use crate::go::time::Duration;
use crate::sfu::buffer;
use crate::sfu::sequencer::{ExtPacketMeta, IGNORE_RETRANSMISSION, PacketMeta};

use super::{DROPPED_SLOT_PADDING_SIZE, DownTrack, Error, KEY_FRAME_INTERVAL_MAX};

// Test-only seams that expose DownTrack internals to the external sfu_test package.
// This file is only compiled into the test binary (its name ends in _test.go), so it
// does not affect the production API. The external test package is required because the
// integration tests import the streamallocator package, which itself imports sfu (an
// import cycle that an in-package test cannot resolve).

pub(super) const DROPPED_SLOT_PADDING_SIZE_FOR_TEST: u8 = DROPPED_SLOT_PADDING_SIZE;

// NACKIgnoreWindowForTest is the longest time the sequencer ignores a repeat NACK for a packet.
pub(super) const NACK_IGNORE_WINDOW_FOR_TEST: Duration =
    Duration::from_millis(IGNORE_RETRANSMISSION);

pub(super) const KEY_FRAME_INTERVAL_MAX_FOR_TEST: Duration =
    Duration::from_millis(KEY_FRAME_INTERVAL_MAX as i64);

impl DownTrack {
    pub(super) fn is_writable_for_test(&self) -> bool {
        self.writable.load()
    }

    pub(super) fn payload_type_rtx_for_test(&self) -> u32 {
        self.payload_type_rtx.load()
    }

    pub(super) fn rtp_stats_active_for_test(&self) -> bool {
        self.rtp_stats.is_active()
    }

    pub(super) fn write_dropped_slot_padding_for_test(
        &self,
        ext_sequence_number: u64,
        ext_timestamp: u64,
    ) {
        self.write_dropped_slot_padding(ext_sequence_number, ext_timestamp, false);
    }

    pub(super) fn last_receiver_report_time_for_test(&self) -> i64 {
        self.rtp_stats.last_receiver_report_time()
    }

    // (absSendTime, transportWide)
    pub(super) fn ext_ids_for_test(&self) -> (i32, i32) {
        (
            self.abs_send_time_ext_id.load(),
            self.transport_wide_ext_id.load(),
        )
    }

    pub(super) fn handle_rtcp_for_test(&self, pkt: &[u8]) {
        self.handle_rtcp(pkt);
    }

    pub(super) fn set_rtp_header_extensions_for_test(&self) {
        self.set_rtp_header_extensions();
    }

    pub(super) fn set_abs_capture_time_ext_id_for_test(&self, id: i32) {
        self.abs_capture_time_ext_id.store(id);
    }

    // ForceForwardLayerForTest forces the forwarder onto a valid current/target layer so a
    // (non-simulcast) video packet will be forwarded without driving full allocation.
    pub(super) fn force_forward_layer_for_test(&self, layer: buffer::VideoLayer) {
        self.forwarder.force_layer_for_test(layer);
    }

    // RetransmitForTest drives the (unexported) retransmitPacket path with an in-package
    // extPacketMeta built from exported values, so the external test can exercise the
    // retransmit path.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn retransmit_for_test(
        &self,
        source_seq_no: u64,
        target_seq_no: u16,
        timestamp: u32,
        ext_timestamp: u64,
        layer: i8,
        source_pkt: &[u8],
        trailer_stripped: u8,
    ) -> Result<usize, Error> {
        let epm = ExtPacketMeta {
            packet_meta: PacketMeta {
                source_seq_no,
                target_seq_no,
                timestamp,
                layer,
                trailer_stripped,
                ..Default::default()
            },
            ext_sequence_number: target_seq_no as u64,
            ext_timestamp,
            ..Default::default()
        };
        self.retransmit_packet(&epm, source_pkt, false)
    }

    // RTXCappedForTest returns the NACKed packets the budget has refused.
    pub(super) fn rtx_capped_for_test(&self) -> u64 {
        self.rtx_budget
            .as_ref()
            .map_or(0, |rtx_budget| rtx_budget.num_capped.load())
    }

    pub(super) fn key_frame_requester_wakeups_for_test(&self) -> u32 {
        self.key_frame_requester_wakeups.load()
    }
}
