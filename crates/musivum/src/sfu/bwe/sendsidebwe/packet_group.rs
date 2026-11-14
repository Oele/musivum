// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/packet_group.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the two errors are the variants of `Error`;
// `MarshalLogObject` is `Debug` (derived for `Stat` and `ClassStat`, written to read like a
// derived one for the packet group, with the values the original adds to the fields); the
// counts of packets and bytes are `i64`, as the original's `int` can go below zero when a
// packet reported lost is received after all; the `yaml` tags of the configuration are doc
// comments on its fields, as nothing reads a configuration file yet.

use std::fmt;

use crate::go::time::Duration;
use crate::protocol::logger::Logger;
use crate::protocol::utils::Bitmap;

use super::packet_info::PacketInfo;
use super::traffic_stats::{TrafficStats, TrafficStatsParams, WeightedLossConfig};

// -------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum Error {
    #[error("packet group is finalized")]
    GroupFinalized,
    #[error("packet is older than packet group start")]
    OldPacket,
}

// -------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PacketGroupConfig {
    /// `min_packets` in YAML.
    pub min_packets: usize,
    /// `max_window_duration` in YAML.
    pub max_window_duration: Duration,
}

pub(super) const DEFAULT_PACKET_GROUP_CONFIG: PacketGroupConfig = PacketGroupConfig {
    min_packets: 30,
    max_window_duration: Duration::from_millis(500),
};

// -------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Stat {
    pub(super) num_packets: i64,
    pub(super) num_bytes: i64,
}

impl Stat {
    fn add(&mut self, size: i64) {
        self.num_packets += 1;
        self.num_bytes += size;
    }

    fn remove(&mut self, size: i64) {
        self.num_packets -= 1;
        self.num_bytes -= size;
    }

    fn get_num_packets(&self) -> i64 {
        self.num_packets
    }

    fn get_num_bytes(&self) -> i64 {
        self.num_bytes
    }
}

// -------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ClassStat {
    pub(super) primary: Stat,
    pub(super) rtx: Stat,
    pub(super) probe: Stat,
}

impl ClassStat {
    fn add(&mut self, size: i64, is_rtx: bool, is_probe: bool) {
        if is_rtx {
            self.rtx.add(size);
        } else if is_probe {
            self.probe.add(size);
        } else {
            self.primary.add(size);
        }
    }

    fn remove(&mut self, size: i64, is_rtx: bool, is_probe: bool) {
        if is_rtx {
            self.rtx.remove(size);
        } else if is_probe {
            self.probe.remove(size);
        } else {
            self.primary.remove(size);
        }
    }

    pub(super) fn num_packets(&self) -> i64 {
        self.primary.get_num_packets() + self.rtx.get_num_packets() + self.probe.get_num_packets()
    }

    pub(super) fn num_bytes(&self) -> i64 {
        self.primary.get_num_bytes() + self.rtx.get_num_bytes() + self.probe.get_num_bytes()
    }
}

// -------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct PacketGroupParams {
    pub(super) config: PacketGroupConfig,
    pub(super) weighted_loss: WeightedLossConfig,
    pub(super) logger: Logger,
}

pub(super) struct PacketGroup {
    params: PacketGroupParams,

    pub(super) min_sequence_number: u64,
    pub(super) max_sequence_number: u64,

    pub(super) min_send_time: i64,
    pub(super) max_send_time: i64,

    pub(super) min_recv_time: i64, // for information only
    pub(super) max_recv_time: i64, // for information only

    pub(super) acked: ClassStat,
    pub(super) lost: ClassStat,
    sn_bitmap: Bitmap<u64>,

    pub(super) aggregate_send_delta: i64,
    pub(super) aggregate_recv_delta: i64,
    pub(super) inherited_queuing_delay: i64,

    is_finalized: bool,
}

impl PacketGroup {
    pub(super) fn new(params: PacketGroupParams, inherited_queuing_delay: i64) -> Self {
        Self {
            inherited_queuing_delay,
            sn_bitmap: Bitmap::new(params.config.min_packets),
            params,
            min_sequence_number: 0,
            max_sequence_number: 0,
            min_send_time: 0,
            max_send_time: 0,
            min_recv_time: 0,
            max_recv_time: 0,
            acked: ClassStat::default(),
            lost: ClassStat::default(),
            aggregate_send_delta: 0,
            aggregate_recv_delta: 0,
            is_finalized: false,
        }
    }

    pub(super) fn add(
        &mut self,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
    ) -> Result<(), Error> {
        if is_lost {
            return self.lost_packet(pi);
        }

        self.in_group(pi.sequence_number)?;

        if self.min_sequence_number == 0 || pi.sequence_number < self.min_sequence_number {
            self.min_sequence_number = pi.sequence_number;
        }
        self.max_sequence_number = self.max_sequence_number.max(pi.sequence_number);

        if self.min_send_time == 0 || pi.send_time.wrapping_sub(send_delta) < self.min_send_time {
            self.min_send_time = pi.send_time.wrapping_sub(send_delta);
        }
        self.max_send_time = self.max_send_time.max(pi.send_time);

        if self.min_recv_time == 0 || pi.recv_time.wrapping_sub(recv_delta) < self.min_recv_time {
            self.min_recv_time = pi.recv_time.wrapping_sub(recv_delta);
        }
        self.max_recv_time = self.max_recv_time.max(pi.recv_time);

        self.acked.add(pi.size as i64, pi.is_rtx, pi.is_probe);
        let offset = pi.sequence_number.wrapping_sub(self.min_sequence_number);
        // int(offset) < p.snBitmap.Len() of the original is a signed comparison
        if (offset as i64) < self.sn_bitmap.len() as i64 && self.sn_bitmap.is_set(offset) {
            // an earlier packet reported as lost has been received
            self.sn_bitmap.clear(offset);
            self.lost.remove(pi.size as i64, pi.is_rtx, pi.is_probe);
        }

        // note that out-of-order deliveries will amplify the queueing delay.
        // for e.g. a, b, c getting delivered as a, c, b.
        // let us say packets are delivered with interval of `x`
        //   send delta aggregate will go up by x((a, c) = 2x + (c, b) -1x)
        //   recv delta aggregate will go up by 3x((a, c) = 2x + (c, b) 1x)
        self.aggregate_send_delta = self.aggregate_send_delta.wrapping_add(send_delta);
        self.aggregate_recv_delta = self.aggregate_recv_delta.wrapping_add(recv_delta);

        if self.acked.num_packets() == self.params.config.min_packets as i64
            || pi.send_time.wrapping_sub(self.min_send_time)
                > self.params.config.max_window_duration.microseconds()
        {
            self.is_finalized = true;
        }
        Ok(())
    }

    fn lost_packet(&mut self, pi: &PacketInfo) -> Result<(), Error> {
        if pi.recv_time != 0 {
            // previously received packet, so not lost
            return Ok(());
        }

        self.in_group(pi.sequence_number)?;

        if self.min_sequence_number == 0 || pi.sequence_number < self.min_sequence_number {
            self.min_sequence_number = pi.sequence_number;
        }
        self.max_sequence_number = self.max_sequence_number.max(pi.sequence_number);
        self.sn_bitmap
            .set(pi.sequence_number.wrapping_sub(self.min_sequence_number));

        self.lost.add(pi.size as i64, pi.is_rtx, pi.is_probe);
        Ok(())
    }

    pub(super) fn min_send_time(&self) -> i64 {
        self.min_send_time
    }

    pub(super) fn send_window(&self) -> (i64, i64) {
        (self.min_send_time, self.max_send_time)
    }

    pub(super) fn propagated_queuing_delay(&self) -> i64 {
        let delay = self
            .inherited_queuing_delay
            .wrapping_add(self.aggregate_recv_delta)
            .wrapping_sub(self.aggregate_send_delta);
        if delay > 0 {
            return delay;
        }

        0.max(
            self.aggregate_recv_delta
                .wrapping_sub(self.aggregate_send_delta),
        )
    }

    pub(super) fn finalized_propagated_queuing_delay(&self) -> (i64, bool) {
        if !self.is_finalized {
            return (0, false);
        }

        (self.propagated_queuing_delay(), true)
    }

    pub(super) fn is_finalized(&self) -> bool {
        self.is_finalized
    }

    pub(super) fn traffic(&self) -> TrafficStats {
        TrafficStats {
            min_send_time: self.min_send_time,
            max_send_time: self.max_send_time,
            send_delta: self.aggregate_send_delta,
            recv_delta: self.aggregate_recv_delta,
            acked_packets: self.acked.num_packets(),
            acked_bytes: self.acked.num_bytes(),
            lost_packets: self.lost.num_packets(),
            lost_bytes: self.lost.num_bytes(),
            ..Default::default()
        }
    }
}

impl fmt::Debug for PacketGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("PacketGroup");
        s.field("min_sequence_number", &self.min_sequence_number);
        s.field("max_sequence_number", &self.max_sequence_number);
        s.field("acked", &self.acked);
        s.field("lost", &self.lost);

        s.field("min_recv_time", &self.min_recv_time);
        s.field("max_recv_time", &self.max_recv_time);
        let recv_duration = Duration(
            self.max_recv_time
                .wrapping_sub(self.min_recv_time)
                .wrapping_mul(1000),
        );
        s.field("recv_duration", &recv_duration);

        if recv_duration != Duration::ZERO {
            let recv_bitrate =
                self.acked.num_bytes().wrapping_mul(8) as f64 / recv_duration.seconds();
            s.field("recv_bitrate", &recv_bitrate);
        }

        let mut ts = TrafficStats::new(TrafficStatsParams {
            config: self.params.weighted_loss,
            logger: self.params.logger.clone(),
        });
        ts.merge(&self.traffic());
        s.field("traffic_stats", &ts);
        s.field("inherited_queuing_delay", &self.inherited_queuing_delay);
        s.field("propagated_queuing_delay", &self.propagated_queuing_delay());

        s.field("is_finalized", &self.is_finalized);
        s.finish()
    }
}

impl PacketGroup {
    fn in_group(&self, sequence_number: u64) -> Result<(), Error> {
        if self.is_finalized && sequence_number > self.max_sequence_number {
            return Err(Error::GroupFinalized);
        }

        if sequence_number < self.min_sequence_number {
            return Err(Error::OldPacket);
        }

        Ok(())
    }
}
