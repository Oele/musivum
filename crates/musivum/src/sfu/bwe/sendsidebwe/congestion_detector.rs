// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/congestion_detector.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `queuingRegion` and `congestionReason` are enums and
// their `String()` is `Display`; `MarshalLogObject` is `Debug`, written to read like a derived
// one, with the values the original adds to the fields; what the lock guards is
// `CongestionDetectorInner`, which the functions the original calls with its lock held take as
// an argument; the `*packetTracker` the original embeds is the field `packet_tracker`, outside
// the lock as it has its own, through which the methods are called that the original has from
// it (`c.RecordPacketSendAndGetSequenceNumber(..)` is
// `c.packet_tracker.record_packet_send_and_get_sequence_number(..)`); the two closures of
// `HandleTWCCFeedback` are the method `track_packet_group`, in which `pg` of the original is
// the last of the packet groups (which it always is there), and a closure that is handed the
// sequence number it advances; the measurements, the probe packet group and what belongs to
// the trend of the captured traffic ratio are `Option`s where the original has nil pointers;
// the indices and counts of groups are `usize`, so the `max(0, ..)` of `GroupRange` is not
// needed; the `yaml` tags of the configuration are doc comments on its fields, as nothing
// reads a configuration file yet.

use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time::{Duration, Time};
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::bwe;
use crate::sfu::ccutils;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::{
    PacketStatusChunk, SymbolTypeTcc, TransportLayerCc,
};
use crate::{infow, warnw};

use super::packet_group::{
    DEFAULT_PACKET_GROUP_CONFIG, Error, PacketGroup, PacketGroupConfig, PacketGroupParams,
};
use super::packet_info::PacketInfo;
use super::packet_tracker::{PacketTracker, PacketTrackerParams};
use super::probe_packet_group::{
    DEFAULT_PROBE_PACKET_GROUP_CONFIG, ProbePacketGroup, ProbePacketGroupConfig,
    ProbePacketGroupParams,
};
use super::traffic_stats::{
    DEFAULT_WEIGHTED_LOSS_CONFIG, TrafficStats, TrafficStatsParams, WeightedLossConfig,
};
use super::twcc_feedback::{TwccFeedback, TwccFeedbackParams};

// -------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CongestionSignalConfig {
    /// `min_number_of_groups` in YAML.
    pub min_number_of_groups: usize,
    /// `min_duration` in YAML.
    pub min_duration: Duration,
}

impl CongestionSignalConfig {
    pub fn is_triggered(&self, num_groups: usize, duration: i64) -> bool {
        num_groups >= self.min_number_of_groups && duration >= self.min_duration.microseconds()
    }
}

pub(super) const DEFAULT_QUEUING_DELAY_EARLY_WARNING_JQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 2,
        min_duration: Duration::from_millis(200),
    };

pub(super) const DEFAULT_QUEUING_DELAY_EARLY_WARNING_DQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 3,
        min_duration: Duration::from_millis(300),
    };

pub(super) const DEFAULT_LOSS_EARLY_WARNING_JQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 3,
        min_duration: Duration::from_millis(300),
    };

pub(super) const DEFAULT_LOSS_EARLY_WARNING_DQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 4,
        min_duration: Duration::from_millis(400),
    };

pub(super) const DEFAULT_QUEUING_DELAY_CONGESTED_JQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 4,
        min_duration: Duration::from_millis(400),
    };

pub(super) const DEFAULT_QUEUING_DELAY_CONGESTED_DQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 5,
        min_duration: Duration::from_millis(500),
    };

pub(super) const DEFAULT_LOSS_CONGESTED_JQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 6,
        min_duration: Duration::from_millis(600),
    };

pub(super) const DEFAULT_LOSS_CONGESTED_DQR_CONFIG: CongestionSignalConfig =
    CongestionSignalConfig {
        min_number_of_groups: 6,
        min_duration: Duration::from_millis(600),
    };

// -------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ProbeSignalConfig {
    /// `min_bytes_ratio` in YAML.
    pub min_bytes_ratio: f64,
    /// `min_duration_ratio` in YAML.
    pub min_duration_ratio: f64,

    /// `jqr_min_delay` in YAML.
    pub jqr_min_delay: Duration,
    /// `dqr_max_delay` in YAML.
    pub dqr_max_delay: Duration,

    /// `weighted_loss` in YAML.
    pub weighted_loss: WeightedLossConfig,
    /// `jqr_min_weighted_loss` in YAML.
    pub jqr_min_weighted_loss: f64,
    /// `dqr_max_weighted_loss` in YAML.
    pub dqr_max_weighted_loss: f64,
}

impl ProbeSignalConfig {
    pub fn is_valid(&self, pci: ccutils::ProbeClusterInfo) -> bool {
        pci.result.bytes() > (self.min_bytes_ratio * pci.goal.desired_bytes as f64) as i64
            && pci.result.duration()
                > Duration((self.min_duration_ratio * pci.goal.duration.0 as f64) as i64)
    }

    pub(super) fn probe_signal(&self, ppg: &ProbePacketGroup) -> (ccutils::ProbeSignal, i64) {
        let mut ts = TrafficStats::new(TrafficStatsParams {
            config: self.weighted_loss,
            ..Default::default()
        });
        ts.merge(&ppg.packet_group.traffic());

        let pqd = ppg.packet_group.propagated_queuing_delay();
        if pqd > self.jqr_min_delay.microseconds()
            || ts.weighted_loss() > self.jqr_min_weighted_loss
        {
            return (ccutils::ProbeSignal::Congesting, ts.acknowledged_bitrate());
        }

        if pqd < self.dqr_max_delay.microseconds()
            && ts.weighted_loss() < self.dqr_max_weighted_loss
        {
            return (
                ccutils::ProbeSignal::NotCongesting,
                ts.acknowledged_bitrate(),
            );
        }

        (
            ccutils::ProbeSignal::Inconclusive,
            ts.acknowledged_bitrate(),
        )
    }
}

pub(super) const DEFAULT_PROBE_SIGNAL_CONFIG: ProbeSignalConfig = ProbeSignalConfig {
    min_bytes_ratio: 0.5,
    min_duration_ratio: 0.5,

    jqr_min_delay: Duration::from_millis(50),
    dqr_max_delay: Duration::from_millis(20),

    weighted_loss: DEFAULT_WEIGHTED_LOSS_CONFIG,
    jqr_min_weighted_loss: 0.25,
    dqr_max_weighted_loss: 0.1,
};

// -------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) enum QueuingRegion {
    #[default]
    Dqr,
    Indeterminate,
    Jqr,
}

impl fmt::Display for QueuingRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            QueuingRegion::Dqr => "DQR",
            QueuingRegion::Indeterminate => "INDETERMINATE",
            QueuingRegion::Jqr => "JQR",
        })
    }
}

// -------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) enum CongestionReason {
    #[default]
    None,
    QueuingDelay,
    Loss,
}

impl fmt::Display for CongestionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CongestionReason::None => "NONE",
            CongestionReason::QueuingDelay => "QUEUING_DELAY",
            CongestionReason::Loss => "LOSS",
        })
    }
}

// -------------------------------------------------------------------------------

pub(super) struct QdMeasurement {
    jqr_config: CongestionSignalConfig,
    dqr_config: CongestionSignalConfig,
    jqr_min_delay: i64,
    jqr_min_trend_coefficient: f64,
    dqr_max_delay: i64,

    num_groups: usize,
    num_jqr_groups: usize,
    num_dqr_groups: usize,
    min_send_time: i64,
    max_send_time: i64,
    propagated_queuing_delays: Vec<i64>,

    is_sealed: bool,
    min_group_idx: usize,
    max_group_idx: usize,

    queuing_region: QueuingRegion,
}

impl QdMeasurement {
    pub(super) fn new(
        jqr_config: CongestionSignalConfig,
        dqr_config: CongestionSignalConfig,
        jqr_min_delay: i64,
        jqr_min_trend_coefficient: f64,
        dqr_max_delay: i64,
    ) -> Self {
        Self {
            jqr_config,
            dqr_config,
            jqr_min_delay,
            jqr_min_trend_coefficient,
            dqr_max_delay,
            num_groups: 0,
            num_jqr_groups: 0,
            num_dqr_groups: 0,
            min_send_time: 0,
            max_send_time: 0,
            propagated_queuing_delays: Vec::new(),
            is_sealed: false,
            min_group_idx: 0,
            max_group_idx: 0,
            queuing_region: QueuingRegion::Indeterminate,
        }
    }

    pub(super) fn process_packet_group(&mut self, pg: &PacketGroup, group_idx: usize) {
        if self.is_sealed {
            return;
        }

        let (pqd, pqd_ok) = pg.finalized_propagated_queuing_delay();
        if !pqd_ok {
            return;
        }

        self.num_groups += 1;
        if self.min_group_idx == 0 || self.min_group_idx > group_idx {
            self.min_group_idx = group_idx;
        }
        self.max_group_idx = self.max_group_idx.max(group_idx);

        let (min_send_time, max_send_time) = pg.send_window();
        if self.min_send_time == 0 || min_send_time < self.min_send_time {
            self.min_send_time = min_send_time;
        }
        self.max_send_time = self.max_send_time.max(max_send_time);

        self.propagated_queuing_delays.push(pqd);

        if pqd < self.dqr_max_delay {
            self.num_dqr_groups += 1;
            if self.num_jqr_groups > 0 {
                // broken continuity, seal
                self.is_sealed = true;
            } else if self.dqr_config.is_triggered(
                self.num_dqr_groups,
                self.max_send_time.wrapping_sub(self.min_send_time),
            ) {
                self.is_sealed = true;
                self.queuing_region = QueuingRegion::Dqr;
            }
        } else if pqd > self.jqr_min_delay {
            self.num_jqr_groups += 1;
            if self.num_dqr_groups > 0 {
                // broken continuity, seal
                self.is_sealed = true;
            } else if self.jqr_config.is_triggered(
                self.num_jqr_groups,
                self.max_send_time.wrapping_sub(self.min_send_time),
            ) && self.trend_coefficient() > self.jqr_min_trend_coefficient
            {
                self.is_sealed = true;
                self.queuing_region = QueuingRegion::Jqr;
            }
        } else if self.num_dqr_groups > 0 || self.num_jqr_groups > 0 {
            // broken continuity, seal
            self.is_sealed = true;
        }
    }

    pub(super) fn is_sealed(&self) -> bool {
        self.is_sealed
    }

    pub(super) fn queuing_region(&self) -> QueuingRegion {
        self.queuing_region
    }

    pub(super) fn group_range(&self) -> (usize, usize) {
        (self.min_group_idx, self.max_group_idx)
    }
}

impl fmt::Debug for QdMeasurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QdMeasurement")
            .field("num_groups", &self.num_groups)
            .field("num_jqr_groups", &self.num_jqr_groups)
            .field("num_dqr_groups", &self.num_dqr_groups)
            .field("min_send_time", &self.min_send_time)
            .field("max_send_time", &self.max_send_time)
            .field("propagated_queuing_delays", &self.propagated_queuing_delays)
            .field("trend_coefficient", &self.trend_coefficient())
            .field(
                "duration",
                &Duration(
                    self.max_send_time
                        .wrapping_sub(self.min_send_time)
                        .wrapping_mul(1000),
                ),
            )
            .field("is_sealed", &self.is_sealed)
            .field("min_group_idx", &self.min_group_idx)
            .field("max_group_idx", &self.max_group_idx)
            .field("queuing_region", &format_args!("{}", self.queuing_region))
            .finish()
    }
}

impl QdMeasurement {
    fn trend_coefficient(&self) -> f64 {
        let mut concordant_pairs = 0i64;
        let mut discordant_pairs = 0i64;

        // the packet groups are processed from newest to oldest,
        // so a concordant pair is when the value drops,
        // i. e. the propagated queuing delay is increasing if newer (earlier entity in slice) is higher than older (later entity in slice)
        //
        // i < len(q.propagatedQueuingDelays)-1 of the original is a signed comparison: no
        // pairs of no delays
        for i in 0..self.propagated_queuing_delays.len().saturating_sub(1) {
            for j in i + 1..self.propagated_queuing_delays.len() {
                if self.propagated_queuing_delays[i] > self.propagated_queuing_delays[j] {
                    concordant_pairs += 1;
                } else if self.propagated_queuing_delays[i] < self.propagated_queuing_delays[j] {
                    discordant_pairs += 1;
                }
            }
        }

        if (concordant_pairs + discordant_pairs) == 0 {
            // if the min requirements is only one sample, trend calculation is not possible, declare highest trend value
            if self.propagated_queuing_delays.len() == 1 {
                return 1.0;
            }

            return 0.0;
        }

        (concordant_pairs as f64 - discordant_pairs as f64)
            / (concordant_pairs as f64 + discordant_pairs as f64)
    }
}

// -------------------------------------------------------------------------------

pub(super) struct LossMeasurement {
    jqr_config: CongestionSignalConfig,
    dqr_config: CongestionSignalConfig,
    jqr_min_loss: f64,
    dqr_max_loss: f64,

    num_groups: usize,
    ts: TrafficStats,

    is_jqr_sealed: bool,
    is_dqr_sealed: bool,
    min_group_idx: usize,
    max_group_idx: usize,

    weighted_loss: f64,

    queuing_region: QueuingRegion,
}

impl LossMeasurement {
    pub(super) fn new(
        jqr_config: CongestionSignalConfig,
        dqr_config: CongestionSignalConfig,
        weighted_loss_config: WeightedLossConfig,
        jqr_min_loss: f64,
        dqr_max_loss: f64,
        logger: Logger,
    ) -> Self {
        Self {
            jqr_config,
            dqr_config,
            jqr_min_loss,
            dqr_max_loss,
            num_groups: 0,
            ts: TrafficStats::new(TrafficStatsParams {
                config: weighted_loss_config,
                logger,
            }),
            is_jqr_sealed: false,
            is_dqr_sealed: false,
            min_group_idx: 0,
            max_group_idx: 0,
            weighted_loss: 0.0,
            queuing_region: QueuingRegion::Indeterminate,
        }
    }

    pub(super) fn process_packet_group(&mut self, pg: &PacketGroup, group_idx: usize) {
        if (self.is_jqr_sealed && self.is_dqr_sealed) || !pg.is_finalized() {
            return;
        }

        self.num_groups += 1;
        if self.min_group_idx == 0 || self.min_group_idx > group_idx {
            self.min_group_idx = group_idx;
        }
        self.max_group_idx = self.max_group_idx.max(group_idx);

        self.ts.merge(&pg.traffic());

        if !self.is_jqr_sealed
            && self
                .jqr_config
                .is_triggered(self.num_groups, self.ts.duration())
        {
            self.is_jqr_sealed = true;

            let weighted_loss = self.ts.weighted_loss();
            if weighted_loss > self.jqr_min_loss {
                self.weighted_loss = weighted_loss;
                self.queuing_region = QueuingRegion::Jqr;
                self.is_dqr_sealed = true; // seal DQR also as queuing region has been determined
                return;
            }
        }

        if !self.is_dqr_sealed
            && self
                .dqr_config
                .is_triggered(self.num_groups, self.ts.duration())
        {
            self.is_dqr_sealed = true;

            let weighted_loss = self.ts.weighted_loss();
            if weighted_loss < self.dqr_max_loss {
                self.weighted_loss = weighted_loss;
                self.queuing_region = QueuingRegion::Dqr;
                self.is_jqr_sealed = true; // seal JQR also as queuing region has been determined
            }
        }
    }

    pub(super) fn is_sealed(&self) -> bool {
        self.is_jqr_sealed && self.is_dqr_sealed
    }

    pub(super) fn queuing_region(&self) -> QueuingRegion {
        self.queuing_region
    }

    pub(super) fn group_range(&self) -> (usize, usize) {
        (self.min_group_idx, self.max_group_idx)
    }
}

impl fmt::Debug for LossMeasurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LossMeasurement")
            .field("num_groups", &self.num_groups)
            .field("ts", &self.ts)
            .field("is_jqr_sealed", &self.is_jqr_sealed)
            .field("is_dqr_sealed", &self.is_dqr_sealed)
            .field("min_group_idx", &self.min_group_idx)
            .field("max_group_idx", &self.max_group_idx)
            .field("weighted_loss", &self.weighted_loss)
            .field("queuing_region", &format_args!("{}", self.queuing_region))
            .finish()
    }
}

// -------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CongestionDetectorConfig {
    /// `packet_group` in YAML.
    pub packet_group: PacketGroupConfig,
    /// `packet_group_max_age` in YAML.
    pub packet_group_max_age: Duration,

    /// `probe_packet_group` in YAML.
    pub probe_packet_group: ProbePacketGroupConfig,
    /// `probe_regulator` in YAML.
    pub probe_regulator: ccutils::ProbeRegulatorConfig,
    /// `probe_signal` in YAML.
    pub probe_signal: ProbeSignalConfig,

    /// `jqr_min_delay` in YAML.
    pub jqr_min_delay: Duration,
    /// `jqr_min_trend_coefficient` in YAML.
    pub jqr_min_trend_coefficient: f64,
    /// `dqr_max_delay` in YAML.
    pub dqr_max_delay: Duration,

    /// `weighted_loss` in YAML.
    pub weighted_loss: WeightedLossConfig,
    /// `jqr_min_weighted_loss` in YAML.
    pub jqr_min_weighted_loss: f64,
    /// `dqr_max_weighted_loss` in YAML.
    pub dqr_max_weighted_loss: f64,

    /// `queuing_delay_early_warning_jqr` in YAML.
    pub queuing_delay_early_warning_jqr: CongestionSignalConfig,
    /// `queuing_delay_early_warning_dqr` in YAML.
    pub queuing_delay_early_warning_dqr: CongestionSignalConfig,
    /// `loss_early_warning_jqr` in YAML.
    pub loss_early_warning_jqr: CongestionSignalConfig,
    /// `loss_early_warning_dqr` in YAML.
    pub loss_early_warning_dqr: CongestionSignalConfig,

    /// `queuing_delay_congested_jqr` in YAML.
    pub queuing_delay_congested_jqr: CongestionSignalConfig,
    /// `queuing_delay_congested_dqr` in YAML.
    pub queuing_delay_congested_dqr: CongestionSignalConfig,
    /// `loss_congested_jqr` in YAML.
    pub loss_congested_jqr: CongestionSignalConfig,
    /// `loss_congested_dqr` in YAML.
    pub loss_congested_dqr: CongestionSignalConfig,

    /// `congested_ctr_trend` in YAML.
    pub congested_ctr_trend: ccutils::TrendDetectorConfig,
    /// `congested_ctr_epsilon` in YAML.
    pub congested_ctr_epsilon: f64,
    /// `congested_packet_group` in YAML.
    pub congested_packet_group: PacketGroupConfig,

    /// `estimaton_window_duration` in YAML (spelled so upstream).
    pub estimation_window_duration: Duration,
}

pub(super) const DEFAULT_TREND_DETECTOR_CONFIG_CONGESTED_CTR: ccutils::TrendDetectorConfig =
    ccutils::TrendDetectorConfig {
        required_samples: 4,
        required_samples_min: 2,
        downward_trend_threshold: -0.5,
        downward_trend_max_wait: Duration::from_secs(2),
        collapse_threshold: Duration::from_millis(500),
        validity_window: Duration::from_secs(10),
    };

pub(super) const DEFAULT_CONGESTED_PACKET_GROUP_CONFIG: PacketGroupConfig = PacketGroupConfig {
    min_packets: 20,
    max_window_duration: Duration::from_millis(150),
};

pub(super) const DEFAULT_CONGESTION_DETECTOR_CONFIG: CongestionDetectorConfig =
    CongestionDetectorConfig {
        packet_group: DEFAULT_PACKET_GROUP_CONFIG,
        packet_group_max_age: Duration::from_secs(10),

        probe_packet_group: DEFAULT_PROBE_PACKET_GROUP_CONFIG,
        probe_regulator: ccutils::DEFAULT_PROBE_REGULATOR_CONFIG,
        probe_signal: DEFAULT_PROBE_SIGNAL_CONFIG,

        jqr_min_delay: Duration::from_millis(50),
        jqr_min_trend_coefficient: 0.8,
        dqr_max_delay: Duration::from_millis(20),

        weighted_loss: DEFAULT_WEIGHTED_LOSS_CONFIG,
        jqr_min_weighted_loss: 0.25,
        dqr_max_weighted_loss: 0.1,

        queuing_delay_early_warning_jqr: DEFAULT_QUEUING_DELAY_EARLY_WARNING_JQR_CONFIG,
        queuing_delay_early_warning_dqr: DEFAULT_QUEUING_DELAY_EARLY_WARNING_DQR_CONFIG,
        loss_early_warning_jqr: DEFAULT_LOSS_EARLY_WARNING_JQR_CONFIG,
        loss_early_warning_dqr: DEFAULT_LOSS_EARLY_WARNING_DQR_CONFIG,

        queuing_delay_congested_jqr: DEFAULT_QUEUING_DELAY_CONGESTED_JQR_CONFIG,
        queuing_delay_congested_dqr: DEFAULT_QUEUING_DELAY_CONGESTED_DQR_CONFIG,
        loss_congested_jqr: DEFAULT_LOSS_CONGESTED_JQR_CONFIG,
        loss_congested_dqr: DEFAULT_LOSS_CONGESTED_DQR_CONFIG,

        congested_ctr_trend: DEFAULT_TREND_DETECTOR_CONFIG_CONGESTED_CTR,
        congested_ctr_epsilon: 0.05,
        congested_packet_group: DEFAULT_CONGESTED_PACKET_GROUP_CONFIG,

        estimation_window_duration: Duration::from_secs(1),
    };

// -------------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct CongestionDetectorParams {
    pub(super) config: CongestionDetectorConfig,
    pub(super) logger: Logger,
}

pub(super) struct CongestionDetector {
    params: CongestionDetectorParams,

    lock: Mutex<CongestionDetectorInner>,

    pub(super) packet_tracker: PacketTracker,
}

// What `lock` guards.
struct CongestionDetectorInner {
    rtt: f64,

    twcc_feedback: TwccFeedback,

    packet_groups: Vec<PacketGroup>,

    probe_packet_group: Option<ProbePacketGroup>,
    probe_regulator: ccutils::ProbeRegulator,

    estimated_available_channel_capacity: i64,
    estimate_traffic_stats: Option<TrafficStats>,

    congestion_state: bwe::CongestionState,
    #[allow(dead_code)] // not read upstream either
    congestion_state_switched_at: Time,

    congested_ctr_trend: Option<ccutils::TrendDetector<f64>>,
    congested_traffic_stats: Option<TrafficStats>,
    congested_packet_group: Option<PacketGroup>,

    congestion_reason: CongestionReason,
    qd_measurement: Option<QdMeasurement>,
    loss_measurement: Option<LossMeasurement>,

    bwe_listener: Option<Arc<dyn bwe::BweListener>>,
}

impl CongestionDetector {
    pub(super) fn new(params: CongestionDetectorParams) -> Self {
        let c = Self {
            packet_tracker: PacketTracker::new(PacketTrackerParams {
                logger: params.logger.clone(),
            }),
            lock: Mutex::new(CongestionDetectorInner {
                rtt: 0.0,
                twcc_feedback: TwccFeedback::new(TwccFeedbackParams {
                    logger: params.logger.clone(),
                }),
                packet_groups: Vec::new(),
                probe_packet_group: None,
                // cannot be nil here: `reset` makes it again
                probe_regulator: ccutils::ProbeRegulator::new(ccutils::ProbeRegulatorParams {
                    config: params.config.probe_regulator,
                    logger: params.logger.clone(),
                }),
                estimated_available_channel_capacity: 0,
                estimate_traffic_stats: None,
                congestion_state: bwe::CongestionState::None,
                congestion_state_switched_at: Time::ZERO,
                congested_ctr_trend: None,
                congested_traffic_stats: None,
                congested_packet_group: None,
                congestion_reason: CongestionReason::None,
                qd_measurement: None,
                loss_measurement: None,
                bwe_listener: None,
            }),
            params,
        };
        c.reset();

        c
    }

    pub(super) fn reset(&self) {
        let mut inner = self.lock.lock();
        let inner = &mut *inner;

        inner.rtt = bwe::DEFAULT_RTT;

        inner.packet_groups = Vec::new();

        inner.probe_packet_group = None;
        inner.probe_regulator = ccutils::ProbeRegulator::new(ccutils::ProbeRegulatorParams {
            config: self.params.config.probe_regulator,
            logger: self.params.logger.clone(),
        });

        inner.estimated_available_channel_capacity = 100_000_000;
        inner.estimate_traffic_stats = None;

        inner.congestion_state = bwe::CongestionState::None;
        inner.congestion_state_switched_at = mono::now();

        self.clear_ctr_trend(inner);

        inner.congestion_reason = CongestionReason::None;
        inner.qd_measurement = None;
        inner.loss_measurement = None;
    }

    pub(super) fn set_bwe_listener(&self, bwe_listener: Option<Arc<dyn bwe::BweListener>>) {
        let mut inner = self.lock.lock();

        inner.bwe_listener = bwe_listener;
    }

    fn get_bwe_listener(&self) -> Option<Arc<dyn bwe::BweListener>> {
        let inner = self.lock.lock();

        inner.bwe_listener.clone()
    }

    pub(super) fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        let mut guard = self.lock.lock();
        let inner = &mut *guard;
        let (mut recv_ref_time, _) = inner.twcc_feedback.process_report(report, mono::now());

        if inner.packet_groups.is_empty() {
            inner.packet_groups.push(PacketGroup::new(
                PacketGroupParams {
                    config: self.params.config.packet_group,
                    weighted_loss: self.params.config.weighted_loss,
                    logger: self.params.logger.clone(),
                },
                0,
            ));
        }

        // 1. go through the TWCC feedback report and record receive time as reported by remote
        // 2. process acknowledged packet and group them
        //
        // losses are not recorded if a feedback report is completely lost.
        // RFC recommends treating lost reports by ignoring packets that would have been in it.
        // -----------------------------------------------------------------------------------
        // | From a congestion control perspective, lost feedback messages are               |
        // | handled by ignoring packets which would have been reported as lost or           |
        // | received in the lost feedback messages.  This behavior is similar to            |
        // | how a lost RTCP receiver report is handled.                                     |
        // -----------------------------------------------------------------------------------
        // Reference: https://datatracker.ietf.org/doc/html/draft-holmer-rmcat-transport-wide-cc-extensions-01#page-4
        let mut sequence_number = report.base_sequence_number;
        let end_sequence_number_exclusive =
            sequence_number.wrapping_add(report.packet_status_count);
        let mut delta_idx = 0usize;
        let mut process_symbol = |inner: &mut CongestionDetectorInner,
                                  sequence_number: &mut u16,
                                  symbol: SymbolTypeTcc| {
            let mut recv_time = 0i64;
            let mut is_lost = false;
            if symbol == SymbolTypeTcc::PacketNotReceived {
                is_lost = true;
            } else if (symbol == SymbolTypeTcc::PacketReceivedSmallDelta
                || symbol == SymbolTypeTcc::PacketReceivedLargeDelta)
                && delta_idx < report.recv_deltas.len()
            {
                recv_ref_time = recv_ref_time.wrapping_add(report.recv_deltas[delta_idx].delta);
                delta_idx += 1;

                recv_time = recv_ref_time;
            } else {
                // the reserved symbol carries no receive delta, nothing to record
                *sequence_number = sequence_number.wrapping_add(1);
                return;
            }
            let (pi, send_delta, recv_delta) = self
                .packet_tracker
                .record_packet_indication_from_remote(*sequence_number, recv_time);
            if pi.send_time != 0 {
                self.track_packet_group(inner, &pi, send_delta, recv_delta, is_lost);
            }
            *sequence_number = sequence_number.wrapping_add(1);
        };
        for chunk in &report.packet_chunks {
            if sequence_number == end_sequence_number_exclusive {
                break;
            }

            match chunk {
                PacketStatusChunk::RunLengthChunk(chunk) => {
                    for _ in 0..chunk.run_length {
                        if sequence_number == end_sequence_number_exclusive {
                            break;
                        }

                        process_symbol(inner, &mut sequence_number, chunk.packet_status_symbol);
                    }
                }

                PacketStatusChunk::StatusVectorChunk(chunk) => {
                    for symbol in &chunk.symbol_list {
                        if sequence_number == end_sequence_number_exclusive {
                            break;
                        }

                        process_symbol(inner, &mut sequence_number, *symbol);
                    }
                }
            }
        }

        self.prune_packet_groups(inner);
        let (should_notify, from_state, to_state, committed_channel_capacity) =
            self.congestion_detection_state_machine(inner);
        drop(guard);

        if should_notify && let Some(bwe_listener) = self.get_bwe_listener() {
            bwe_listener.on_congestion_state_change(
                from_state,
                to_state,
                committed_channel_capacity,
            );
        }
    }

    // `trackPacketGroup` of the original, a closure in `HandleTWCCFeedback`.
    fn track_packet_group(
        &self,
        inner: &mut CongestionDetectorInner,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
    ) {
        self.update_ctr_trend(inner, pi, send_delta, recv_delta, is_lost);

        if let Some(probe_packet_group) = inner.probe_packet_group.as_mut() {
            let _ = probe_packet_group.add(pi, send_delta, recv_delta, is_lost);
        }

        let Some(pg) = inner.packet_groups.last_mut() else {
            return;
        };
        let err = match pg.add(pi, send_delta, recv_delta, is_lost) {
            Ok(()) => return,
            Err(err) => err,
        };

        if err == Error::GroupFinalized {
            // previous group ended, start a new group
            let pg = PacketGroup::new(
                PacketGroupParams {
                    config: self.params.config.packet_group,
                    weighted_loss: self.params.config.weighted_loss,
                    logger: self.params.logger.clone(),
                },
                pg.propagated_queuing_delay(),
            );
            inner.packet_groups.push(pg);

            if let Some(pg) = inner.packet_groups.last_mut()
                && let Err(err) = pg.add(pi, send_delta, recv_delta, is_lost)
            {
                warnw!(
                    self.params.logger,
                    "send side bwe: could not add packet to new packet group",
                    err,
                    "packetInfo" => pi,
                    "packetGroup" => pg,
                );
            }
            return;
        }

        // try an older group
        for idx in (0..inner.packet_groups.len() - 1).rev() {
            let opg = &mut inner.packet_groups[idx];
            match opg.add(pi, send_delta, recv_delta, is_lost) {
                Ok(()) => return,
                Err(Error::GroupFinalized) => {
                    infow!(
                        self.params.logger,
                        "send side bwe: unexpected finalized group",
                        "packetInfo" => pi,
                        "packetGroup" => opg,
                    );
                }
                Err(_) => {}
            }
        }
    }

    pub(super) fn update_rtt(&self, rtt: f64) {
        let mut inner = self.lock.lock();

        if rtt == 0.0 {
            inner.rtt = bwe::DEFAULT_RTT;
        } else if inner.rtt == 0.0 {
            inner.rtt = rtt;
        } else {
            inner.rtt =
                bwe::RTT_SMOOTHING_FACTOR * rtt + (1.0 - bwe::RTT_SMOOTHING_FACTOR) * inner.rtt;
        }
    }

    pub(super) fn congestion_state(&self) -> bwe::CongestionState {
        let inner = self.lock.lock();

        inner.congestion_state
    }

    pub(super) fn can_probe(&self) -> bool {
        let inner = self.lock.lock();

        inner.congestion_state == bwe::CongestionState::None
            && inner.probe_packet_group.is_none()
            && inner.probe_regulator.can_probe()
    }

    pub(super) fn probe_duration(&self) -> Duration {
        let inner = self.lock.lock();

        inner.probe_regulator.probe_duration()
    }

    pub(super) fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
        let mut inner = self.lock.lock();

        inner.probe_packet_group = Some(ProbePacketGroup::new(
            ProbePacketGroupParams {
                config: self.params.config.probe_packet_group,
                weighted_loss: self.params.config.weighted_loss,
                logger: self.params.logger.clone(),
            },
            pci,
        ));

        self.packet_tracker.probe_cluster_starting(pci.id);
    }

    pub(super) fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
        let mut inner = self.lock.lock();

        self.packet_tracker.probe_cluster_done(pci.id);
        if let Some(probe_packet_group) = inner.probe_packet_group.as_mut() {
            probe_packet_group.probe_cluster_done(pci);
        }
    }

    pub(super) fn probe_cluster_is_goal_reached(&self) -> bool {
        let inner = self.lock.lock();

        let Some(probe_packet_group) = inner.probe_packet_group.as_ref() else {
            return false;
        };
        if inner.congestion_state != bwe::CongestionState::None {
            return false;
        }

        let pci = probe_packet_group.probe_cluster_info();
        if !self.params.config.probe_signal.is_valid(pci) {
            return false;
        }

        let (probe_signal, estimated_available_channel_capacity) = self
            .params
            .config
            .probe_signal
            .probe_signal(probe_packet_group);
        probe_signal != ccutils::ProbeSignal::NotCongesting
            && estimated_available_channel_capacity > pci.goal.desired_bps
    }

    pub(super) fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        let mut inner = self.lock.lock();
        let inner = &mut *inner;

        let Some(probe_packet_group) = inner.probe_packet_group.as_ref() else {
            return (ccutils::ProbeSignal::Inconclusive, 0, false);
        };

        let (pci, is_finalized) = probe_packet_group
            .maybe_finalize_probe(self.packet_tracker.probe_max_sequence_number(), inner.rtt);
        if !is_finalized {
            return (ccutils::ProbeSignal::Inconclusive, 0, is_finalized);
        }

        let is_signal_valid = self.params.config.probe_signal.is_valid(pci);
        infow!(
            self.params.logger,
            "send side bwe: probe finalized",
            "isSignalValid" => is_signal_valid,
            "probeClusterInfo" => pci,
            "probePacketGroup" => probe_packet_group,
            "congestionState" => inner.congestion_state,
            "rtt" => inner.rtt,
        );

        // if congestion signal changed during probe, defer to that signal
        if inner.congestion_state != bwe::CongestionState::None {
            let probe_signal = ccutils::ProbeSignal::Congesting;
            inner
                .probe_regulator
                .probe_signal(probe_signal, pci.created_at);
            inner.probe_packet_group = None;
            return (
                probe_signal,
                inner.estimated_available_channel_capacity,
                true,
            );
        }

        let (probe_signal, estimated_available_channel_capacity) = self
            .params
            .config
            .probe_signal
            .probe_signal(probe_packet_group);
        if probe_signal == ccutils::ProbeSignal::NotCongesting
            && estimated_available_channel_capacity > inner.estimated_available_channel_capacity
        {
            inner.estimated_available_channel_capacity = estimated_available_channel_capacity;
        }

        inner
            .probe_regulator
            .probe_signal(probe_signal, pci.created_at);
        inner.probe_packet_group = None;
        (
            probe_signal,
            inner.estimated_available_channel_capacity,
            true,
        )
    }

    fn prune_packet_groups(&self, inner: &mut CongestionDetectorInner) {
        if inner.packet_groups.is_empty() {
            return;
        }

        let (threshold, ok) = self
            .packet_tracker
            .base_send_time_threshold(self.params.config.packet_group_max_age.microseconds());
        if !ok {
            return;
        }

        for (idx, pg) in inner.packet_groups.iter().enumerate() {
            let mst = pg.min_send_time();
            if mst > threshold {
                inner.packet_groups.drain(..idx);
                return;
            }
        }
    }

    fn update_congestion_signal(
        &self,
        inner: &mut CongestionDetectorInner,
        qd_jqr_config: CongestionSignalConfig,
        qd_dqr_config: CongestionSignalConfig,
        loss_jqr_config: CongestionSignalConfig,
        loss_dqr_config: CongestionSignalConfig,
    ) -> QueuingRegion {
        let mut qd_measurement = QdMeasurement::new(
            qd_jqr_config,
            qd_dqr_config,
            self.params.config.jqr_min_delay.microseconds(),
            self.params.config.jqr_min_trend_coefficient,
            self.params.config.dqr_max_delay.microseconds(),
        );
        let mut loss_measurement = LossMeasurement::new(
            loss_jqr_config,
            loss_dqr_config,
            self.params.config.weighted_loss,
            self.params.config.jqr_min_weighted_loss,
            self.params.config.dqr_max_weighted_loss,
            self.params.logger.clone(),
        );

        for idx in (0..inner.packet_groups.len()).rev() {
            let pg = &inner.packet_groups[idx];
            qd_measurement.process_packet_group(pg, idx);
            loss_measurement.process_packet_group(pg, idx);

            // if both measurements have enough data to make a decision, stop processing groups
            if qd_measurement.is_sealed() && loss_measurement.is_sealed() {
                break;
            }
        }

        let mut qr = QueuingRegion::Indeterminate;
        let qd_queuing_region = qd_measurement.queuing_region();
        let loss_queuing_region = loss_measurement.queuing_region();
        if qd_queuing_region == QueuingRegion::Jqr {
            qr = QueuingRegion::Jqr;
            inner.congestion_reason = CongestionReason::QueuingDelay;
        } else if loss_queuing_region == QueuingRegion::Jqr {
            qr = QueuingRegion::Jqr;
            inner.congestion_reason = CongestionReason::Loss;
        } else if qd_queuing_region == QueuingRegion::Dqr
            && loss_queuing_region == QueuingRegion::Dqr
        {
            qr = QueuingRegion::Dqr;
            inner.congestion_reason = CongestionReason::None;
        }

        inner.qd_measurement = Some(qd_measurement);
        inner.loss_measurement = Some(loss_measurement);

        qr
    }

    fn update_early_warning_signal(&self, inner: &mut CongestionDetectorInner) -> QueuingRegion {
        self.update_congestion_signal(
            inner,
            self.params.config.queuing_delay_early_warning_jqr,
            self.params.config.queuing_delay_early_warning_dqr,
            self.params.config.loss_early_warning_jqr,
            self.params.config.loss_early_warning_dqr,
        )
    }

    fn update_congested_signal(&self, inner: &mut CongestionDetectorInner) -> QueuingRegion {
        self.update_congestion_signal(
            inner,
            self.params.config.queuing_delay_congested_jqr,
            self.params.config.queuing_delay_congested_dqr,
            self.params.config.loss_congested_jqr,
            self.params.config.loss_congested_dqr,
        )
    }

    fn congestion_detection_state_machine(
        &self,
        inner: &mut CongestionDetectorInner,
    ) -> (bool, bwe::CongestionState, bwe::CongestionState, i64) {
        let mut from_state = inner.congestion_state;
        let mut to_state = inner.congestion_state;

        match from_state {
            bwe::CongestionState::None => {
                if self.update_early_warning_signal(inner) == QueuingRegion::Jqr {
                    to_state = bwe::CongestionState::EarlyWarning;
                }
            }

            bwe::CongestionState::EarlyWarning => {
                if self.update_congested_signal(inner) == QueuingRegion::Jqr {
                    to_state = bwe::CongestionState::Congested;
                } else if self.update_early_warning_signal(inner) == QueuingRegion::Dqr {
                    to_state = bwe::CongestionState::None;
                }
            }

            bwe::CongestionState::Congested => {
                if self.update_congested_signal(inner) == QueuingRegion::Dqr {
                    to_state = bwe::CongestionState::None;
                }
            }
        }

        let mut should_notify = false;
        if to_state != from_state {
            self.estimate_available_channel_capacity(inner);
            (from_state, to_state) = self.update_congestion_state(inner, to_state);
            should_notify = true;
        }

        if let Some(congested_ctr_trend) = inner.congested_ctr_trend.as_ref()
            && congested_ctr_trend.get_direction() == ccutils::TrendDirection::Downward
        {
            let congested_acked_bitrate = inner
                .congested_traffic_stats
                .as_ref()
                .map_or(0, TrafficStats::acknowledged_bitrate);
            if congested_acked_bitrate < inner.estimated_available_channel_capacity {
                inner.estimated_available_channel_capacity = congested_acked_bitrate;

                infow!(
                    self.params.logger,
                    "send side bwe: captured traffic ratio is trending downward",
                    "channel" => congested_ctr_trend,
                    "trafficStats" => inner.congested_traffic_stats,
                    "estimatedAvailableChannelCapacity" => inner.estimated_available_channel_capacity,
                );

                should_notify = true;
            }

            // reset to get new set of samples for next trend
            self.reset_ctr_trend(inner);
        }

        (
            should_notify,
            from_state,
            to_state,
            inner.estimated_available_channel_capacity,
        )
    }

    fn create_ctr_trend(&self, inner: &mut CongestionDetectorInner) {
        self.reset_ctr_trend(inner);
        inner.congested_packet_group = None;
    }

    fn reset_ctr_trend(&self, inner: &mut CongestionDetectorInner) {
        inner.congested_ctr_trend =
            Some(ccutils::TrendDetector::new(ccutils::TrendDetectorParams {
                name: "ssbwe-ctr".to_string(),
                logger: self.params.logger.clone(),
                config: self.params.config.congested_ctr_trend,
            }));
        inner.congested_traffic_stats = Some(TrafficStats::new(TrafficStatsParams {
            config: self.params.config.weighted_loss,
            logger: self.params.logger.clone(),
        }));
    }

    fn clear_ctr_trend(&self, inner: &mut CongestionDetectorInner) {
        inner.congested_ctr_trend = None;
        inner.congested_traffic_stats = None;
        inner.congested_packet_group = None;
    }

    fn update_ctr_trend(
        &self,
        inner: &mut CongestionDetectorInner,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
    ) {
        let Some(congested_ctr_trend) = inner.congested_ctr_trend.as_mut() else {
            return;
        };

        let congested_packet_group = inner.congested_packet_group.get_or_insert_with(|| {
            PacketGroup::new(
                PacketGroupParams {
                    config: self.params.config.congested_packet_group,
                    weighted_loss: self.params.config.weighted_loss,
                    logger: self.params.logger.clone(),
                },
                0,
            )
        });

        if congested_packet_group.add(pi, send_delta, recv_delta, is_lost)
            == Err(Error::GroupFinalized)
        {
            // progressively keep increasing the window and make measurements over longer windows,
            // if congestion is not relieving, CTR will trend down
            if let Some(congested_traffic_stats) = inner.congested_traffic_stats.as_mut() {
                congested_traffic_stats.merge(&congested_packet_group.traffic());
            }

            let mut ts = TrafficStats::new(TrafficStatsParams {
                config: self.params.config.weighted_loss,
                logger: self.params.logger.clone(),
            });
            ts.merge(&congested_packet_group.traffic());
            let ctr = ts.captured_traffic_ratio();

            // quantise CTR to filter out small changes
            congested_ctr_trend.add_value(
                ((ctr + (self.params.config.congested_ctr_epsilon / 2.0))
                    / self.params.config.congested_ctr_epsilon) as i64 as f64
                    * self.params.config.congested_ctr_epsilon,
            );

            *congested_packet_group = PacketGroup::new(
                PacketGroupParams {
                    config: self.params.config.congested_packet_group,
                    weighted_loss: self.params.config.weighted_loss,
                    logger: self.params.logger.clone(),
                },
                congested_packet_group.propagated_queuing_delay(),
            );
        }
    }

    fn estimate_available_channel_capacity(&self, inner: &mut CongestionDetectorInner) {
        inner.estimate_traffic_stats = None;
        if inner.packet_groups.is_empty() || inner.probe_packet_group.is_some() {
            return;
        }

        // when congested, use contributing groups,
        // else use a time windowed measurement
        let mut use_window = false;
        let mut is_agg_valid = true;
        let mut min_group_idx = 0;
        let mut max_group_idx = inner.packet_groups.len() - 1;
        match (
            inner.congestion_reason,
            &inner.qd_measurement,
            &inner.loss_measurement,
        ) {
            (CongestionReason::QueuingDelay, Some(qd_measurement), _) => {
                (min_group_idx, max_group_idx) = qd_measurement.group_range();
            }
            (CongestionReason::Loss, _, Some(loss_measurement)) => {
                (min_group_idx, max_group_idx) = loss_measurement.group_range();
            }
            _ => {
                use_window = true;
                is_agg_valid = false;
            }
        }

        let mut agg = TrafficStats::new(TrafficStatsParams {
            config: self.params.config.weighted_loss,
            logger: self.params.logger.clone(),
        });
        for idx in (min_group_idx..=max_group_idx).rev() {
            let pg = &inner.packet_groups[idx];
            if !pg.is_finalized() {
                continue;
            }

            agg.merge(&pg.traffic());
            if use_window
                && agg.duration() > self.params.config.estimation_window_duration.microseconds()
            {
                is_agg_valid = true;
                break;
            }
        }

        if is_agg_valid {
            inner.estimated_available_channel_capacity = agg.acknowledged_bitrate();
            inner.estimate_traffic_stats = Some(agg);
        }
    }

    fn update_congestion_state(
        &self,
        inner: &mut CongestionDetectorInner,
        state: bwe::CongestionState,
    ) -> (bwe::CongestionState, bwe::CongestionState) {
        let (mut min_group_idx, mut max_group_idx) = (0, 0);
        match (
            inner.congestion_reason,
            &inner.qd_measurement,
            &inner.loss_measurement,
        ) {
            (CongestionReason::QueuingDelay, Some(qd_measurement), _) => {
                (min_group_idx, max_group_idx) = qd_measurement.group_range();
            }
            (CongestionReason::Loss, _, Some(loss_measurement)) => {
                (min_group_idx, max_group_idx) = loss_measurement.group_range();
            }
            _ => {}
        }
        if inner.congestion_reason != CongestionReason::None {
            infow!(
                self.params.logger,
                "send side bwe: congestion state change",
                "from" => inner.congestion_state,
                "to" => state,
                "congestionReason" => inner.congestion_reason,
                "qdMeasurement" => inner.qd_measurement,
                "lossMeasurement" => inner.loss_measurement,
                "numPacketGroups" => inner.packet_groups.len(),
                "estimatedAvailableChannelCapacity" => inner.estimated_available_channel_capacity,
                "estimateTrafficStats" => inner.estimate_traffic_stats,
                "contributingGroups" => &inner.packet_groups[min_group_idx..max_group_idx + 1],
            );
        } else {
            infow!(
                self.params.logger,
                "send side bwe: congestion state change",
                "from" => inner.congestion_state,
                "to" => state,
                "congestionReason" => inner.congestion_reason,
                "qdMeasurement" => inner.qd_measurement,
                "lossMeasurement" => inner.loss_measurement,
                "numPacketGroups" => inner.packet_groups.len(),
                "estimatedAvailableChannelCapacity" => inner.estimated_available_channel_capacity,
                "estimateTrafficStats" => inner.estimate_traffic_stats,
            );
        }

        if state != inner.congestion_state {
            inner.congestion_state_switched_at = mono::now();
        }

        let from_state = inner.congestion_state;
        inner.congestion_state = state;

        // when in congested state, monitor changes in captured traffic ratio (CTR)
        // to ensure allocations are in line with latest estimates, it is possible that
        // the estimate is incorrect when congestion starts and the allocation may be
        // sub-optimal and not enough to reduce/relieve congestion, by monitoring CTR
        // on a continuous basis allocations can be adjusted in the direction of
        // reducing/relieving congestion
        if state == bwe::CongestionState::Congested && from_state != bwe::CongestionState::Congested
        {
            self.create_ctr_trend(inner);
        } else if state != bwe::CongestionState::Congested {
            self.clear_ctr_trend(inner);
        }

        (from_state, inner.congestion_state)
    }
}

#[cfg(test)]
#[path = "congestion_detector_test.rs"]
mod congestion_detector_test;

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
