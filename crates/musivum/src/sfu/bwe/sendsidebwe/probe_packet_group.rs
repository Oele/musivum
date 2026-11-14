// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/probe_packet_group.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the `*packetGroup` the original embeds is the field
// `packet_group`, through which the methods are called that the original has from it
// (`ppg.Traffic()` is `ppg.packet_group.traffic()`); `MarshalLogObject` is `Debug`; the `yaml`
// tags of the configuration are doc comments on its fields, as nothing reads a configuration
// file yet.

use std::fmt;

use crate::go::time::{self, Duration, Time};
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::ccutils;

use super::packet_group::{Error, PacketGroup, PacketGroupConfig, PacketGroupParams};
use super::packet_info::PacketInfo;
use super::traffic_stats::WeightedLossConfig;

// -------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProbePacketGroupConfig {
    /// `packet_group` in YAML.
    pub packet_group: PacketGroupConfig,

    /// `settle_wait_num_rtt` in YAML.
    pub settle_wait_num_rtt: u32,
    /// `settle_wait_min` in YAML.
    pub settle_wait_min: Duration,
    /// `settle_wait_max` in YAML.
    pub settle_wait_max: Duration,
}

// large numbers to treat a probe packet group as one
pub(super) const DEFAULT_PROBE_PACKET_GROUP_CONFIG: ProbePacketGroupConfig =
    ProbePacketGroupConfig {
        packet_group: PacketGroupConfig {
            min_packets: 16384,
            max_window_duration: Duration::from_secs(60),
        },

        settle_wait_num_rtt: 5,
        settle_wait_min: Duration::from_millis(250),
        settle_wait_max: Duration::from_secs(5),
    };

// -------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct ProbePacketGroupParams {
    pub(super) config: ProbePacketGroupConfig,
    pub(super) weighted_loss: WeightedLossConfig,
    pub(super) logger: Logger,
}

pub(super) struct ProbePacketGroup {
    params: ProbePacketGroupParams,
    pub(super) pci: ccutils::ProbeClusterInfo,
    pub(super) packet_group: PacketGroup,
    pub(super) max_sequence_number: u64,
    pub(super) done_at: Time,
}

impl ProbePacketGroup {
    pub(super) fn new(params: ProbePacketGroupParams, pci: ccutils::ProbeClusterInfo) -> Self {
        Self {
            pci,
            packet_group: PacketGroup::new(
                PacketGroupParams {
                    config: params.config.packet_group,
                    weighted_loss: params.weighted_loss,
                    logger: params.logger.clone(),
                },
                0,
            ),
            params,
            max_sequence_number: 0,
            done_at: Time::ZERO,
        }
    }

    pub(super) fn probe_cluster_done(&mut self, pci: ccutils::ProbeClusterInfo) {
        if self.pci.id != pci.id {
            return;
        }

        self.pci.result = pci.result;
        self.done_at = mono::now();
    }

    pub(super) fn probe_cluster_info(&self) -> ccutils::ProbeClusterInfo {
        self.pci
    }

    pub(super) fn maybe_finalize_probe(
        &self,
        max_sequence_number: u64,
        rtt: f64,
    ) -> (ccutils::ProbeClusterInfo, bool) {
        if self.done_at.is_zero() {
            return (ccutils::PROBE_CLUSTER_INFO_INVALID, false);
        }

        if max_sequence_number != 0 && self.max_sequence_number >= max_sequence_number {
            return (self.pci, true);
        }

        let mut settle_wait = Duration(
            (self.params.config.settle_wait_num_rtt as f64 * rtt * time::SECOND.0 as f64) as i64,
        );
        if settle_wait < self.params.config.settle_wait_min {
            settle_wait = self.params.config.settle_wait_min;
        }
        if settle_wait > self.params.config.settle_wait_max {
            settle_wait = self.params.config.settle_wait_max;
        }
        if time::since(self.done_at) < settle_wait {
            return (ccutils::PROBE_CLUSTER_INFO_INVALID, false);
        }

        (self.pci, true)
    }

    pub(super) fn add(
        &mut self,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
    ) -> Result<(), Error> {
        if pi.probe_cluster_id != self.pci.id {
            return Ok(());
        }

        self.max_sequence_number = self.max_sequence_number.max(pi.sequence_number);

        self.packet_group.add(pi, send_delta, recv_delta, is_lost)
    }
}

impl fmt::Debug for ProbePacketGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbePacketGroup")
            .field("pci", &self.pci)
            .field("packet_group", &self.packet_group)
            .field("max_sequence_number", &self.max_sequence_number)
            .field("done_at", &self.done_at)
            .finish()
    }
}
