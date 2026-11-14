// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/send_side_bwe.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewSendSideBWE` is `SendSideBwe::new` and gives an
// `Arc`, as the estimator is shared; the methods are the type's own, in the order of the
// original, and the `Bwe` trait is implemented on top of those at the end, with the one call
// the original leaves to the `bwe.NullBWE` it embeds (`HandleREMB`) passed on to a `NullBwe`;
// the `*congestionDetector` the original embeds is the field `congestion_detector`; the `yaml`
// tag of the configuration is a doc comment on its field, as nothing reads a configuration
// file yet. The test at the end is new: the original has none.

use std::sync::Arc;

use crate::go::time::Duration;
use crate::protocol::logger::Logger;
use crate::sfu::bwe;
use crate::sfu::ccutils;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;

use super::congestion_detector::{
    CongestionDetector, CongestionDetectorConfig, CongestionDetectorParams,
    DEFAULT_CONGESTION_DETECTOR_CONFIG,
};

//
// Based on a simplified/modified version of JitterPath paper
// (https://homepage.iis.sinica.edu.tw/papers/lcs/2114-F.pdf)
//
// TWCC feedback is used to calculate delta one-way-delay.
// It is accumulated/propagated to determine in which region
// groups of packets are operating in.
//
// In simplified terms,
//   o JQR (Join Queuing Region) is when channel is congested.
//   o DQR (Disjoint Queuing Region) is when channel is not.
//
// Packets are grouped and thresholds applied to smooth over
// small variations. For example, in the paper,
//    if propagated_queuing_delay + delta_one_way_delay > 0 {
//       possibly_operating_in_jqr
//    }
// But, in this implementation it is checked at packet group level,
// i. e. using queuing delay and aggregated delta one-way-delay of
// the group and a minimum value threshold is applied before declaring
// that a group is in JQR.
//
// There is also hysteresis to make transisitons smoother, i.e. if the
// metric is above a certain threshold, it is JQR and it is DQR only if it
// is below a certain value and the gap in between those two thresholds
// are treated as interdeterminate groups.
//

// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SendSideBweConfig {
    /// `congestion_detector` in YAML.
    pub congestion_detector: CongestionDetectorConfig,
}

pub const DEFAULT_SEND_SIDE_BWE_CONFIG: SendSideBweConfig = SendSideBweConfig {
    congestion_detector: DEFAULT_CONGESTION_DETECTOR_CONFIG,
};

// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct SendSideBweParams {
    pub config: SendSideBweConfig,
    pub logger: Logger,
}

pub struct SendSideBwe {
    null_bwe: bwe::NullBwe,

    #[allow(dead_code)] // not read upstream either
    params: SendSideBweParams,

    pub(super) congestion_detector: CongestionDetector,
}

impl SendSideBwe {
    pub fn new(params: SendSideBweParams) -> Arc<Self> {
        Arc::new(Self {
            null_bwe: bwe::NullBwe::default(),
            congestion_detector: CongestionDetector::new(CongestionDetectorParams {
                config: params.config.congestion_detector,
                logger: params.logger.clone(),
            }),
            params,
        })
    }

    pub fn r#type(&self) -> bwe::BweType {
        bwe::BweType::SendSide
    }

    pub fn set_bwe_listener(&self, bwe_listener: Option<Arc<dyn bwe::BweListener>>) {
        self.congestion_detector.set_bwe_listener(bwe_listener);
    }

    pub fn reset(&self) {
        self.congestion_detector.reset();
    }

    pub fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ccutils::ProbeClusterId,
        is_probe: bool,
    ) -> u16 {
        self.congestion_detector
            .packet_tracker
            .record_packet_send_and_get_sequence_number(
                at_micro,
                size,
                is_rtx,
                probe_cluster_id,
                is_probe,
            )
    }

    pub fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        self.congestion_detector.handle_twcc_feedback(report);
    }

    pub fn update_rtt(&self, rtt: f64) {
        self.congestion_detector.update_rtt(rtt);
    }

    pub fn congestion_state(&self) -> bwe::CongestionState {
        self.congestion_detector.congestion_state()
    }

    pub fn can_probe(&self) -> bool {
        self.congestion_detector.can_probe()
    }

    pub fn probe_duration(&self) -> Duration {
        self.congestion_detector.probe_duration()
    }

    pub fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
        self.congestion_detector.probe_cluster_starting(pci);
    }

    pub fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
        self.congestion_detector.probe_cluster_done(pci);
    }

    pub fn probe_cluster_is_goal_reached(&self) -> bool {
        self.congestion_detector.probe_cluster_is_goal_reached()
    }

    pub fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        self.congestion_detector.probe_cluster_finalize()
    }
}

// ------------------------------------------------

// The interface, on top of the methods above and of the `NullBwe` for the one that the
// original leaves to the `bwe.NullBWE` it embeds.
impl bwe::Bwe for SendSideBwe {
    fn r#type(&self) -> bwe::BweType {
        SendSideBwe::r#type(self)
    }

    fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn bwe::BweListener>>) {
        SendSideBwe::set_bwe_listener(self, bwe_listner)
    }

    fn reset(&self) {
        SendSideBwe::reset(self)
    }

    fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    ) {
        self.null_bwe.handle_remb(
            received_estimate,
            expected_bandwidth_usage,
            sent_packets,
            repeated_nacks,
        )
    }

    fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ccutils::ProbeClusterId,
        is_probe: bool,
    ) -> u16 {
        SendSideBwe::record_packet_send_and_get_sequence_number(
            self,
            at_micro,
            size,
            is_rtx,
            probe_cluster_id,
            is_probe,
        )
    }

    fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        SendSideBwe::handle_twcc_feedback(self, report)
    }

    fn update_rtt(&self, rtt: f64) {
        SendSideBwe::update_rtt(self, rtt)
    }

    fn congestion_state(&self) -> bwe::CongestionState {
        SendSideBwe::congestion_state(self)
    }

    fn can_probe(&self) -> bool {
        SendSideBwe::can_probe(self)
    }

    fn probe_duration(&self) -> Duration {
        SendSideBwe::probe_duration(self)
    }

    fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
        SendSideBwe::probe_cluster_starting(self, pci)
    }

    fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
        SendSideBwe::probe_cluster_done(self, pci)
    }

    fn probe_cluster_is_goal_reached(&self) -> bool {
        SendSideBwe::probe_cluster_is_goal_reached(self)
    }

    fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        SendSideBwe::probe_cluster_finalize(self)
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::time;
    use crate::sfu::bwe::Bwe;

    // New for musivum: the original has no test of this file. The estimator behind the
    // trait, as the rest of the server holds it: the calls reach the congestion detector,
    // and the one the original leaves to the bwe.NullBWE it embeds does nothing.
    #[test]
    fn the_estimator_behind_the_trait_is_the_congestion_detector() {
        let s = SendSideBwe::new(SendSideBweParams {
            config: DEFAULT_SEND_SIDE_BWE_CONFIG,
            ..Default::default()
        });
        let bwe: Arc<dyn Bwe> = s.clone();
        assert_eq!(bwe.r#type(), bwe::BweType::SendSide);

        // a random number in third quartile of sequence number space, and the one after it
        let sn = bwe.record_packet_send_and_get_sequence_number(
            1_000_000,
            1200,
            false,
            ccutils::PROBE_CLUSTER_ID_INVALID,
            false,
        );
        assert!((1 << 15..(1 << 15) + (1 << 14)).contains(&sn), "{sn}");
        assert_eq!(
            sn + 1,
            bwe.record_packet_send_and_get_sequence_number(
                1_001_000,
                1200,
                false,
                ccutils::PROBE_CLUSTER_ID_INVALID,
                false,
            )
        );

        bwe.handle_remb(1_000_000, 2_000_000, 100, 50);
        assert_eq!(bwe.congestion_state(), bwe::CongestionState::None);
        assert_eq!(bwe.probe_duration(), 200 * time::MILLISECOND);
        assert!(!bwe.probe_cluster_is_goal_reached());
        assert_eq!(
            bwe.probe_cluster_finalize(),
            (ccutils::ProbeSignal::Inconclusive, 0, false)
        );

        // a probe that is starting is the detector's
        bwe.probe_cluster_starting(ccutils::ProbeClusterInfo {
            id: ccutils::ProbeClusterId(1),
            ..Default::default()
        });
        assert!(!bwe.can_probe());
        bwe.reset();
        assert_eq!(bwe.congestion_state(), bwe::CongestionState::None);
    }
}
