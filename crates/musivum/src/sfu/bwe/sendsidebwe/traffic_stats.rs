// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/traffic_stats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `MarshalLogObject` is `Debug`, written to read like a
// derived one, with the values the original adds to the fields; the counts of packets and
// bytes are `i64`, as the original's `int` can go below zero in a packet group; `math.Exp` and
// `math.Log10` are `f64::exp` and `f64::log10`, which may differ from Go's in the last bit
// (the session of reference_test.rs gives the same weighted losses to nine decimals and the
// same loss penalties and bitrates); the `yaml` tags of the configuration are doc comments on
// its fields, as nothing reads a configuration file yet.

use std::fmt;

use crate::go::time::Duration;
use crate::protocol::logger::Logger;

// -----------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct WeightedLossConfig {
    /// `min_duration_for_loss_validity` in YAML.
    pub min_duration_for_loss_validity: Duration,
    /// `base_duration` in YAML.
    pub base_duration: Duration,
    /// `base_pps` in YAML.
    pub base_pps: i64,
    /// `loss_penalty_factor` in YAML.
    pub loss_penalty_factor: f64,
}

pub(super) const DEFAULT_WEIGHTED_LOSS_CONFIG: WeightedLossConfig = WeightedLossConfig {
    min_duration_for_loss_validity: Duration::from_millis(100),
    base_duration: Duration::from_millis(500),
    base_pps: 30,
    loss_penalty_factor: 0.25,
};

// -----------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct TrafficStatsParams {
    pub(super) config: WeightedLossConfig,
    #[allow(dead_code)] // not read upstream either
    pub(super) logger: Logger,
}

#[derive(Clone, Default)]
pub(super) struct TrafficStats {
    pub(super) params: TrafficStatsParams,

    pub(super) min_send_time: i64,
    pub(super) max_send_time: i64,
    pub(super) send_delta: i64,
    pub(super) recv_delta: i64,
    pub(super) acked_packets: i64,
    pub(super) acked_bytes: i64,
    pub(super) lost_packets: i64,
    pub(super) lost_bytes: i64,
}

impl TrafficStats {
    pub(super) fn new(params: TrafficStatsParams) -> Self {
        Self {
            params,
            ..Default::default()
        }
    }

    pub(super) fn merge(&mut self, rhs: &TrafficStats) {
        if self.min_send_time == 0 || rhs.min_send_time < self.min_send_time {
            self.min_send_time = rhs.min_send_time;
        }
        if rhs.max_send_time > self.max_send_time {
            self.max_send_time = rhs.max_send_time;
        }
        self.send_delta = self.send_delta.wrapping_add(rhs.send_delta);
        self.recv_delta = self.recv_delta.wrapping_add(rhs.recv_delta);
        self.acked_packets = self.acked_packets.wrapping_add(rhs.acked_packets);
        self.acked_bytes = self.acked_bytes.wrapping_add(rhs.acked_bytes);
        self.lost_packets = self.lost_packets.wrapping_add(rhs.lost_packets);
        self.lost_bytes = self.lost_bytes.wrapping_add(rhs.lost_bytes);
    }

    #[allow(dead_code)] // not called upstream either
    pub(super) fn num_bytes(&self) -> i64 {
        self.acked_bytes.wrapping_add(self.lost_bytes)
    }

    pub(super) fn duration(&self) -> i64 {
        self.max_send_time.wrapping_sub(self.min_send_time)
    }

    pub(super) fn acknowledged_bitrate(&self) -> i64 {
        let duration = self.duration();
        if duration == 0 {
            return 0;
        }

        let acked_bitrate = self.acked_bytes as f64 * 8.0 * 1e6 / self.duration() as f64;
        (acked_bitrate * self.captured_traffic_ratio()) as i64
    }

    pub(super) fn captured_traffic_ratio(&self) -> f64 {
        if self.recv_delta == 0 {
            return 0.0;
        }

        // apply a penalty for lost packets,
        // the rationale being packet dropping is a strategy to relieve congestion
        // and if they were not dropped, they would have increased queuing delay,
        // as it is not possible to know the reason for the losses,
        // apply a small penalty to receive delta aggregate to simulate those packets
        // building up queuing delay.
        go_min(
            1.0,
            self.send_delta as f64 / self.recv_delta.wrapping_add(self.loss_penalty()) as f64,
        )
    }

    pub(super) fn weighted_loss(&self) -> f64 {
        let duration_micro = self.duration();
        if Duration(duration_micro.wrapping_mul(1000))
            < self.params.config.min_duration_for_loss_validity
        {
            return 0.0;
        }

        let total_packets = self.lost_packets.wrapping_add(self.acked_packets) as f64;
        let pps = total_packets * 1e6 / duration_micro as f64;

        // longer duration, i. e. more time resolution, lower pps is acceptable as the measurement is more stable
        let mut delta_duration = Duration(
            duration_micro
                .wrapping_mul(1000)
                .wrapping_sub(self.params.config.base_duration.0),
        );
        if delta_duration < Duration::ZERO {
            delta_duration = Duration::ZERO;
        }
        let threshold = (-delta_duration.seconds()).exp() * self.params.config.base_pps as f64;
        if pps < threshold {
            return 0.0;
        }

        let mut loss_ratio = 0.0;
        if total_packets != 0.0 {
            loss_ratio = self.lost_packets as f64 / total_packets;
        }

        // Log10 is used to give higher weight for the same loss ratio at higher packet rates,
        // for e.g.
        //    - 10% loss at 20 pps = 0.1 * log10(20) = 0.130
        //    - 10% loss at 100 pps = 0.1 * log10(100) = 0.2
        //    - 10% loss at 1000 pps = 0.1 * log10(1000) = 0.3
        loss_ratio * pps.log10()
    }

    pub(super) fn loss_penalty(&self) -> i64 {
        (self.recv_delta as f64 * self.weighted_loss() * self.params.config.loss_penalty_factor)
            as i64
    }
}

// Go's builtin `min` of two floats: a NaN if either is one, which `f64::min` does not give.
fn go_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::NAN;
    }
    a.min(b)
}

impl fmt::Debug for TrafficStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("TrafficStats");
        s.field("min_send_time", &self.min_send_time);
        s.field("max_send_time", &self.max_send_time);
        let duration = Duration(self.duration().wrapping_mul(1000));
        s.field("duration", &duration);

        s.field("acked_packets", &self.acked_packets);
        s.field("acked_bytes", &self.acked_bytes);
        s.field("lost_packets", &self.lost_packets);
        s.field("lost_bytes", &self.lost_bytes);

        let mut bitrate = 0.0;
        if duration != Duration::ZERO {
            bitrate = self.acked_bytes.wrapping_mul(8) as f64 / duration.seconds();
            s.field("bitrate", &bitrate);
        }

        s.field("send_delta", &self.send_delta);
        s.field("recv_delta", &self.recv_delta);
        s.field(
            "group_delay",
            &self.recv_delta.wrapping_sub(self.send_delta),
        );

        let total_packets = self.lost_packets.wrapping_add(self.acked_packets);
        if duration != Duration::ZERO {
            s.field("pps", &(total_packets as f64 / duration.seconds()));
        }
        if total_packets != 0 {
            s.field(
                "raw_loss",
                &(self.lost_packets as f64 / total_packets as f64),
            );
        }
        s.field("weighted_loss", &self.weighted_loss());
        s.field("loss_penalty", &self.loss_penalty());

        let captured_traffic_ratio = self.captured_traffic_ratio();
        s.field("captured_traffic_ratio", &captured_traffic_ratio);
        s.field(
            "estimated_available_channel_capacity",
            &(bitrate * captured_traffic_ratio),
        );
        s.finish()
    }
}
