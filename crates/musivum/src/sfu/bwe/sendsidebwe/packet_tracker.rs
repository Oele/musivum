// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/packet_tracker.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; what the lock guards is `PacketTrackerInner`, which has
// the two functions the original calls with the lock held (at the end of the file); the
// pointers into the array of packet infos (`piLastRecv`, and what `getPacketInfo` and
// `getPacketInfoExisting` give) are indices into it, `None` where the original has nil; the
// array is a `Vec` of `PACKET_INFOS_LEN` entries, so that a tracker is not 64 kB on the stack.

use parking_lot::Mutex;
use rand::RngExt;

use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::ccutils;

use super::packet_info::PacketInfo;

// -------------------------------------------------------------------------------

const PACKET_INFOS_LEN: usize = 2048;

#[derive(Clone, Debug, Default)]
pub(super) struct PacketTrackerParams {
    #[allow(dead_code)] // not read upstream either
    pub(super) logger: Logger,
}

pub(super) struct PacketTracker {
    #[allow(dead_code)] // not read upstream either
    params: PacketTrackerParams,

    pub(super) lock: Mutex<PacketTrackerInner>,
}

// What `lock` guards.
pub(super) struct PacketTrackerInner {
    pub(super) sequence_number: u64,

    base_send_time: i64,
    pub(super) packet_infos: Vec<PacketInfo>,

    base_recv_time: i64,
    pi_last_recv: Option<usize>,

    probe_cluster_id: ccutils::ProbeClusterId,
    probe_max_sequence_number: u64,
}

impl PacketTracker {
    pub(super) fn new(params: PacketTrackerParams) -> Self {
        Self {
            params,
            lock: Mutex::new(PacketTrackerInner {
                // a random number in third quartile of sequence number space
                sequence_number: rand::rng().random_range(0..(1u64 << 14)) + (1u64 << 15),
                base_send_time: 0,
                packet_infos: vec![PacketInfo::default(); PACKET_INFOS_LEN],
                base_recv_time: 0,
                pi_last_recv: None,
                probe_cluster_id: ccutils::PROBE_CLUSTER_ID_INVALID,
                probe_max_sequence_number: 0,
            }),
        }
    }

    pub(super) fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ccutils::ProbeClusterId,
        is_probe: bool,
    ) -> u16 {
        let mut p = self.lock.lock();

        if p.base_send_time == 0 {
            p.base_send_time = at_micro;
        }

        let pi = p.get_packet_info(p.sequence_number as u16);
        p.packet_infos[pi] = PacketInfo {
            sequence_number: p.sequence_number,
            send_time: at_micro.wrapping_sub(p.base_send_time),
            size: size as u16,
            is_rtx,
            probe_cluster_id,
            is_probe,
            ..Default::default()
        };

        p.sequence_number = p.sequence_number.wrapping_add(1);

        // extreme case of wrap around before receiving any feedback
        if Some(pi) == p.pi_last_recv {
            p.pi_last_recv = None;
        }

        let pi = p.packet_infos[pi];
        if p.probe_cluster_id != ccutils::PROBE_CLUSTER_ID_INVALID
            && p.probe_cluster_id == pi.probe_cluster_id
            && pi.sequence_number > p.probe_max_sequence_number
        {
            p.probe_max_sequence_number = pi.sequence_number;
        }

        pi.sequence_number as u16
    }

    pub(super) fn base_send_time_threshold(&self, threshold: i64) -> (i64, bool) {
        let p = self.lock.lock();

        if p.base_send_time == 0 {
            return (0, false);
        }

        (
            mono::unix_micro()
                .wrapping_sub(p.base_send_time)
                .wrapping_sub(threshold),
            true,
        )
    }

    pub(super) fn record_packet_indication_from_remote(
        &self,
        sn: u16,
        recv_time: i64,
    ) -> (PacketInfo, i64, i64) {
        let mut p = self.lock.lock();

        let Some(pi) = p.get_packet_info_existing(sn) else {
            return (PacketInfo::default(), 0, 0);
        };

        if recv_time == 0 {
            // maybe lost OR already received but reported lost in a later report
            return (p.packet_infos[pi], 0, 0);
        }

        if p.base_recv_time == 0 {
            p.base_recv_time = recv_time;
            p.pi_last_recv = Some(pi);
        }

        p.packet_infos[pi].recv_time = recv_time.wrapping_sub(p.base_recv_time);
        let pi_recv = p.packet_infos[pi];
        let (mut send_delta, mut recv_delta) = (0, 0);
        if let Some(pi_last_recv) = p.pi_last_recv {
            let pi_last_recv = p.packet_infos[pi_last_recv];
            (send_delta, recv_delta) = (
                pi_recv.send_time.wrapping_sub(pi_last_recv.send_time),
                pi_recv.recv_time.wrapping_sub(pi_last_recv.recv_time),
            );
        }
        p.pi_last_recv = Some(pi);
        (pi_recv, send_delta, recv_delta)
    }

    pub(super) fn probe_cluster_starting(&self, probe_cluster_id: ccutils::ProbeClusterId) {
        let mut p = self.lock.lock();

        p.probe_cluster_id = probe_cluster_id;
    }

    pub(super) fn probe_cluster_done(&self, probe_cluster_id: ccutils::ProbeClusterId) {
        let mut p = self.lock.lock();

        if p.probe_cluster_id == probe_cluster_id {
            p.probe_cluster_id = ccutils::PROBE_CLUSTER_ID_INVALID;
        }
    }

    pub(super) fn probe_max_sequence_number(&self) -> u64 {
        let p = self.lock.lock();

        p.probe_max_sequence_number
    }
}

impl PacketTrackerInner {
    // The index of the packet info the original gives a pointer to.
    fn get_packet_info(&self, sn: u16) -> usize {
        sn as usize % self.packet_infos.len()
    }

    // The index of the packet info the original gives a pointer to, `None` for its nil.
    pub(super) fn get_packet_info_existing(&self, sn: u16) -> Option<usize> {
        let pi = sn as usize % self.packet_infos.len();
        if self.packet_infos[pi].sequence_number as u16 == sn {
            return Some(pi);
        }

        None
    }
}
