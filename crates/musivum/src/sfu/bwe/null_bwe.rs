// Ported from LiveKit, pkg/sfu/bwe/null_bwe.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. As in the original, `NullBwe` has every method of
// `Bwe` but `type`, and so is not a `Bwe` itself: the original embeds it in an estimator for
// the methods that estimator does not have, and an estimator here holds one and passes those
// calls on. The test at the end is new: the original has none.

use std::sync::Arc;

use crate::go::time::Duration;
use crate::sfu::ccutils;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;

use super::{BweListener, CongestionState};

#[derive(Clone, Copy, Debug, Default)]
pub struct NullBwe {}

impl NullBwe {
    pub fn set_bwe_listener(&self, _bwe_listener: Option<Arc<dyn BweListener>>) {}

    pub fn reset(&self) {}

    pub fn record_packet_send_and_get_sequence_number(
        &self,
        _at_micro: i64,
        _size: usize,
        _is_rtx: bool,
        _probe_cluster_id: ccutils::ProbeClusterId,
        _is_probe: bool,
    ) -> u16 {
        0
    }

    pub fn handle_remb(
        &self,
        _received_estimate: i64,
        _expected_bandwidth_usage: i64,
        _sent_packets: u32,
        _repeated_nacks: u32,
    ) {
    }

    pub fn handle_twcc_feedback(&self, _report: &TransportLayerCc) {}

    pub fn update_rtt(&self, _rtt: f64) {}

    pub fn congestion_state(&self) -> CongestionState {
        CongestionState::None
    }

    pub fn can_probe(&self) -> bool {
        false
    }

    pub fn probe_duration(&self) -> Duration {
        Duration::ZERO
    }

    pub fn probe_cluster_starting(&self, _pci: ccutils::ProbeClusterInfo) {}

    pub fn probe_cluster_done(&self, _pci: ccutils::ProbeClusterInfo) {}

    pub fn probe_cluster_is_goal_reached(&self) -> bool {
        false
    }

    pub fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        (ccutils::ProbeSignal::Inconclusive, 0, false)
    }
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sfu::bwe::{Bwe, BweType};

    // What the original's tests do where they need an estimator that does nothing
    // (`nullBWE` in pkg/sfu/pacer/base_test.go): NullBWE, and the method it lacks.
    #[derive(Default)]
    struct NullBweWithType {
        null_bwe: NullBwe,
    }

    impl Bwe for NullBweWithType {
        fn r#type(&self) -> BweType {
            BweType::None
        }

        fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn BweListener>>) {
            self.null_bwe.set_bwe_listener(bwe_listner);
        }

        fn reset(&self) {
            self.null_bwe.reset();
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
            );
        }

        fn record_packet_send_and_get_sequence_number(
            &self,
            at_micro: i64,
            size: usize,
            is_rtx: bool,
            probe_cluster_id: ccutils::ProbeClusterId,
            is_probe: bool,
        ) -> u16 {
            self.null_bwe.record_packet_send_and_get_sequence_number(
                at_micro,
                size,
                is_rtx,
                probe_cluster_id,
                is_probe,
            )
        }

        fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
            self.null_bwe.handle_twcc_feedback(report);
        }

        fn update_rtt(&self, rtt: f64) {
            self.null_bwe.update_rtt(rtt);
        }

        fn congestion_state(&self) -> CongestionState {
            self.null_bwe.congestion_state()
        }

        fn can_probe(&self) -> bool {
            self.null_bwe.can_probe()
        }

        fn probe_duration(&self) -> Duration {
            self.null_bwe.probe_duration()
        }

        fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo) {
            self.null_bwe.probe_cluster_starting(pci);
        }

        fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo) {
            self.null_bwe.probe_cluster_done(pci);
        }

        fn probe_cluster_is_goal_reached(&self) -> bool {
            self.null_bwe.probe_cluster_is_goal_reached()
        }

        fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
            self.null_bwe.probe_cluster_finalize()
        }
    }

    struct Listener;

    impl BweListener for Listener {
        fn on_congestion_state_change(
            &self,
            _from_state: CongestionState,
            _to_state: CongestionState,
            _estimated_available_channel_capacity: i64,
        ) {
            panic!("the null estimator has no congestion to tell of");
        }
    }

    // New for musivum: the original has no tests. An estimator is held as `Arc<dyn Bwe>`;
    // the null one behind that does nothing and answers with zero values.
    #[test]
    fn the_null_estimator_does_nothing_behind_the_trait() {
        let bwe: Arc<dyn Bwe> = Arc::new(NullBweWithType::default());
        assert_eq!(bwe.r#type(), BweType::None);

        bwe.set_bwe_listener(Some(Arc::new(Listener)));
        bwe.reset();
        bwe.handle_remb(1_000_000, 2_000_000, 100, 50);
        assert_eq!(
            bwe.record_packet_send_and_get_sequence_number(
                1_000,
                1200,
                false,
                ccutils::PROBE_CLUSTER_ID_INVALID,
                false
            ),
            0
        );
        bwe.handle_twcc_feedback(&TransportLayerCc::default());
        bwe.update_rtt(0.2);
        assert_eq!(bwe.congestion_state(), CongestionState::None);

        assert!(!bwe.can_probe());
        assert_eq!(bwe.probe_duration(), Duration::ZERO);
        bwe.probe_cluster_starting(ccutils::PROBE_CLUSTER_INFO_INVALID);
        bwe.probe_cluster_done(ccutils::PROBE_CLUSTER_INFO_INVALID);
        assert!(!bwe.probe_cluster_is_goal_reached());
        assert_eq!(
            bwe.probe_cluster_finalize(),
            (ccutils::ProbeSignal::Inconclusive, 0, false)
        );
    }
}
