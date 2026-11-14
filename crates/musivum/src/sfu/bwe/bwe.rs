// Ported from LiveKit, pkg/sfu/bwe/bwe.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `BWEType` and `CongestionState` are enums and their
// `String()` is `Display`; the two interfaces are traits, whose methods take `&self`: an
// estimator is shared and guards its own state. A listener that may be nil is an `Option`.
// The tests at the end are new: the original has none.

use std::fmt;
use std::sync::Arc;

use crate::go::time::Duration;
use crate::sfu::ccutils;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;

// ------------------------------------------------

pub const DEFAULT_RTT: f64 = 0.070; // 70 ms
pub const RTT_SMOOTHING_FACTOR: f64 = 0.5;

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum BweType {
    #[default]
    None,
    Remote,
    SendSide,
}

impl fmt::Display for BweType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BweType::None => "NONE",
            BweType::Remote => "REMOTE",
            BweType::SendSide => "SEND_SIDE",
        })
    }
}

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CongestionState {
    #[default]
    None,
    EarlyWarning,
    Congested,
}

impl fmt::Display for CongestionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CongestionState::None => "NONE",
            CongestionState::EarlyWarning => "EARLY_WARNING",
            CongestionState::Congested => "CONGESTED",
        })
    }
}

// ------------------------------------------------

pub trait Bwe: Send + Sync {
    fn r#type(&self) -> BweType;

    fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn BweListener>>);

    fn reset(&self);

    fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    );

    // TWCC sequence number
    fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ccutils::ProbeClusterId,
        is_probe: bool,
    ) -> u16;

    fn handle_twcc_feedback(&self, report: &TransportLayerCc);

    fn update_rtt(&self, rtt: f64);

    fn congestion_state(&self) -> CongestionState;

    fn can_probe(&self) -> bool;
    fn probe_duration(&self) -> Duration;
    fn probe_cluster_starting(&self, pci: ccutils::ProbeClusterInfo);
    fn probe_cluster_done(&self, pci: ccutils::ProbeClusterInfo);
    fn probe_cluster_is_goal_reached(&self) -> bool;
    fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool);
}

// ------------------------------------------------

pub trait BweListener: Send + Sync {
    fn on_congestion_state_change(
        &self,
        from_state: CongestionState,
        to_state: CongestionState,
        estimated_available_channel_capacity: i64,
    );
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The texts are the original's.
    #[test]
    fn the_types_and_states_have_the_texts_of_the_original() {
        assert_eq!(BweType::None.to_string(), "NONE");
        assert_eq!(BweType::Remote.to_string(), "REMOTE");
        assert_eq!(BweType::SendSide.to_string(), "SEND_SIDE");
        assert_eq!(BweType::default(), BweType::None);

        assert_eq!(CongestionState::None.to_string(), "NONE");
        assert_eq!(CongestionState::EarlyWarning.to_string(), "EARLY_WARNING");
        assert_eq!(CongestionState::Congested.to_string(), "CONGESTED");
        assert_eq!(CongestionState::default(), CongestionState::None);
    }
}
