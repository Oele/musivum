//! How the parts of the configuration that are defined elsewhere (`mediatransportutil`'s
//! `rtcconfig`, `sfu` and what is under it) are read from YAML: for each struct its fields
//! with the names the original's `yaml` tags give them, and for the types that have an
//! `UnmarshalYAML` or are strings in the original, what those do.

use crate::mediatransportutil::rtcconfig;
use crate::sfu;
use crate::sfu::bwe::{remotebwe, sendsidebwe};
use crate::sfu::{audio, ccutils, streamallocator, streamtracker};

#[cfg(test)]
use super::yaml::Dump;
use super::yaml::{Decoder, Node, Yaml, yaml_struct};

// ---------------------------------
// mediatransportutil/pkg/rtcconfig

yaml_struct!(rtcconfig::RtcConfig, "rtcconfig.RTCConfig", {
    udp_port: "udp_port",
    tcp_port: "tcp_port",
    ice_port_range_start: "port_range_start",
    ice_port_range_end: "port_range_end",
    use_stun_port_as_ice: "use_stun_port_as_ice",
    node_ip: "node_ip",
    node_ip_auto_generated: skip,
    stun_servers: "stun_servers",
    use_external_ip: "use_external_ip",
    require_ipv4: "require_ipv4",
    skip_external_ip_validation: "skip_external_ip_validation",
    advertise_internal_ip: "advertise_internal_ip",
    use_ice_lite: "use_ice_lite",
    interfaces: "interfaces",
    ips: "ips",
    enable_loopback_candidate: "enable_loopback_candidate",
    use_mdns: "use_mdns",
    external_ip_only: "external_ip_only",
    batch_io: "batch_io",
    sctp_min_cwnd: "sctp_min_cwnd",
    sctp_fast_rtx_wnd: "sctp_fast_rtx_wnd",
    sctp_cwnd_ca_step: "sctp_cwnd_ca_step",
    host_acceptance_min_wait: "host_acceptance_min_wait",
    srflx_acceptance_min_wait: "srflx_acceptance_min_wait",
    prflx_acceptance_min_wait: "prflx_acceptance_min_wait",
    relay_acceptance_min_wait: "relay_acceptance_min_wait",
    force_tcp: "force_tcp",
});
yaml_struct!(rtcconfig::InterfacesConfig, "rtcconfig.InterfacesConfig", {
    includes: "includes",
    excludes: "excludes",
});
yaml_struct!(rtcconfig::IpsConfig, "rtcconfig.IPsConfig", {
    includes: "includes",
    excludes: "excludes",
});
yaml_struct!(rtcconfig::BatchIoConfig, "rtcconfig.BatchIOConfig", {
    batch_size: "batch_size",
    max_flush_interval: "max_flush_interval",
});

// UnmarshalYAML: the text of a scalar, which is empty for a mapping and a sequence; a null
// does not get that far. Its error stops the decoding.
impl Yaml for rtcconfig::PortRange {
    fn type_name() -> String {
        "rtcconfig.PortRange".to_string()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        if n.is_null() {
            return false;
        }
        if let Err(err) = self.unmarshal_string(&n.value) {
            d.fail(err);
            return false;
        }
        true
    }
}

// UnmarshalYAML
impl Yaml for rtcconfig::NodeIp {
    fn type_name() -> String {
        "rtcconfig.NodeIP".to_string()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        if n.is_null() {
            return false;
        }
        if let Err(err) = self.unmarshal_string(&n.value) {
            d.fail(err);
            return false;
        }
        true
    }
}

#[cfg(test)]
impl Dump for rtcconfig::PortRange {
    fn dump(&self, path: &str, out: &mut Vec<String>) {
        out.push(format!("{path}.start={}", self.start));
        out.push(format!("{path}.end={}", self.end));
    }
}

#[cfg(test)]
impl Dump for rtcconfig::NodeIp {
    fn dump(&self, path: &str, out: &mut Vec<String>) {
        out.push(format!("{path}.ipv4={:?}", self.v4));
        out.push(format!("{path}.ipv6={:?}", self.v6));
    }
}

// ---------------------------------
// pkg/sfu

yaml_struct!(sfu::PliThrottleConfig, "sfu.PLIThrottleConfig", {
    low_quality: "low_quality",
    mid_quality: "mid_quality",
    high_quality: "high_quality",
});
yaml_struct!(sfu::AudioConfig, "sfu.AudioConfig", {
    audio_level_config: inline,
    active_red_encoding: "active_red_encoding",
    enable_loss_proxying: "enable_loss_proxying",
});
yaml_struct!(audio::AudioLevelConfig, "audio.AudioLevelConfig", {
    active_level: "active_level",
    min_percentile: "min_percentile",
    update_interval: "update_interval",
    smooth_intervals: "smooth_intervals",
});
yaml_struct!(sfu::RtxBudgetConfig, "sfu.RTXBudgetConfig", {
    audio: "audio",
    video: "video",
});
yaml_struct!(sfu::RtxBudgetLimit, "sfu.RTXBudgetLimit", {
    max_ratio: "max_ratio",
    burst_bytes: "burst_bytes",
});
yaml_struct!(sfu::StreamTrackerManagerConfig, "sfu.StreamTrackerManagerConfig", {
    video: "video",
    screenshare: "screenshare",
});
yaml_struct!(sfu::StreamTrackerConfig, "sfu.StreamTrackerConfig", {
    stream_tracker_type: "stream_tracker_type",
    bitrate_report_interval: "bitrate_report_interval",
    packet_tracker: "packet_tracker",
    frame_tracker: "frame_tracker",
});
yaml_struct!(streamtracker::StreamTrackerPacketConfig, "streamtracker.StreamTrackerPacketConfig", {
    samples_required: "samples_required",
    cycles_required: "cycles_required",
    cycle_duration: "cycle_duration",
});
yaml_struct!(streamtracker::StreamTrackerFrameConfig, "streamtracker.StreamTrackerFrameConfig", {
    min_fps: "min_fps",
});

// A string in the original, which keeps any text. The port has an enum, so a text that
// names no kind of tracker is `Unknown` here and cannot be read back.
impl Yaml for sfu::StreamTrackerType {
    fn type_name() -> String {
        "sfu.StreamTrackerType".to_string()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        let mut text = String::new();
        if d.scalar::<Self>(n).is_none() || !text.unmarshal(d, n) {
            return false;
        }
        *self = match text.as_str() {
            "packet" => sfu::StreamTrackerType::Packet,
            "frame" => sfu::StreamTrackerType::Frame,
            _ => sfu::StreamTrackerType::Unknown,
        };
        true
    }
}

#[cfg(test)]
impl Dump for sfu::StreamTrackerType {
    fn dump(&self, path: &str, out: &mut Vec<String>) {
        let text = match self {
            sfu::StreamTrackerType::Packet => "packet",
            sfu::StreamTrackerType::Frame => "frame",
            sfu::StreamTrackerType::Unknown => "",
        };
        out.push(format!("{path}={text:?}"));
    }
}

// ---------------------------------
// pkg/sfu/streamallocator

yaml_struct!(streamallocator::StreamAllocatorConfig, "streamallocator.StreamAllocatorConfig", {
    min_channel_capacity: "min_channel_capacity",
    // spelled so upstream
    disable_estimation_unmanaged_tracks: "disable_etimation_unmanaged_tracks",
    probe_mode: "probe_mode",
    probe_overage_pct: "probe_overage_pct",
    probe_min_bps: "probe_min_bps",
    paused_min_wait: "paused_min_wait",
});

// A string in the original, which keeps any text and probes in neither way if the text
// names no mode. The port has an enum of the two modes, so such a text is an error here.
impl Yaml for streamallocator::ProbeMode {
    fn type_name() -> String {
        "streamallocator.ProbeMode".to_string()
    }

    fn unmarshal(&mut self, d: &mut Decoder, n: &Node) -> bool {
        let mut text = String::new();
        if d.scalar::<Self>(n).is_none() || !text.unmarshal(d, n) {
            return false;
        }
        match text.parse() {
            Ok(probe_mode) => {
                *self = probe_mode;
                true
            }
            Err(()) => {
                d.terror(n, &Self::type_name());
                false
            }
        }
    }
}

#[cfg(test)]
impl Dump for streamallocator::ProbeMode {
    fn dump(&self, path: &str, out: &mut Vec<String>) {
        out.push(format!("{path}={:?}", self.as_str()));
    }
}

// ---------------------------------
// pkg/sfu/ccutils

yaml_struct!(ccutils::ProbeRegulatorConfig, "ccutils.ProbeRegulatorConfig", {
    base_interval: "base_interval",
    backoff_factor: "backoff_factor",
    max_interval: "max_interval",
    min_duration: "min_duration",
    max_duration: "max_duration",
    duration_increase_factor: "duration_increase_factor",
});
yaml_struct!(ccutils::TrendDetectorConfig, "ccutils.TrendDetectorConfig", {
    required_samples: "required_samples",
    required_samples_min: "required_samples_min",
    downward_trend_threshold: "downward_trend_threshold",
    downward_trend_max_wait: "downward_trend_max_wait",
    collapse_threshold: "collapse_threshold",
    validity_window: "validity_window",
});

// ---------------------------------
// pkg/sfu/bwe/remotebwe

yaml_struct!(remotebwe::RemoteBweConfig, "remotebwe.RemoteBWEConfig", {
    nack_ratio_attenuator: "nack_ratio_attenuator",
    expected_usage_threshold: "expected_usage_threshold",
    channel_observer_probe: "channel_observer_probe",
    channel_observer_non_probe: "channel_observer_non_probe",
    probe_controller: "probe_controller",
});
yaml_struct!(remotebwe::ChannelObserverConfig, "remotebwe.ChannelObserverConfig", {
    estimate: "estimate",
    nack: "nack",
});
yaml_struct!(remotebwe::NackTrackerConfig, "remotebwe.NackTrackerConfig", {
    window_min_duration: "window_min_duration",
    window_max_duration: "window_max_duration",
    ratio_threshold: "ratio_threshold",
});
yaml_struct!(remotebwe::ProbeControllerConfig, "remotebwe.ProbeControllerConfig", {
    probe_regulator: "probe_regulator",
    settle_wait_num_rtt: "settle_wait_num_rtt",
    settle_wait_min: "settle_wait_min",
    settle_wait_max: "settle_wait_max",
});

// ---------------------------------
// pkg/sfu/bwe/sendsidebwe

yaml_struct!(sendsidebwe::SendSideBweConfig, "sendsidebwe.SendSideBWEConfig", {
    congestion_detector: "congestion_detector",
});
yaml_struct!(sendsidebwe::CongestionDetectorConfig, "sendsidebwe.CongestionDetectorConfig", {
    packet_group: "packet_group",
    packet_group_max_age: "packet_group_max_age",
    probe_packet_group: "probe_packet_group",
    probe_regulator: "probe_regulator",
    probe_signal: "probe_signal",
    jqr_min_delay: "jqr_min_delay",
    jqr_min_trend_coefficient: "jqr_min_trend_coefficient",
    dqr_max_delay: "dqr_max_delay",
    weighted_loss: "weighted_loss",
    jqr_min_weighted_loss: "jqr_min_weighted_loss",
    dqr_max_weighted_loss: "dqr_max_weighted_loss",
    queuing_delay_early_warning_jqr: "queuing_delay_early_warning_jqr",
    queuing_delay_early_warning_dqr: "queuing_delay_early_warning_dqr",
    loss_early_warning_jqr: "loss_early_warning_jqr",
    loss_early_warning_dqr: "loss_early_warning_dqr",
    queuing_delay_congested_jqr: "queuing_delay_congested_jqr",
    queuing_delay_congested_dqr: "queuing_delay_congested_dqr",
    loss_congested_jqr: "loss_congested_jqr",
    loss_congested_dqr: "loss_congested_dqr",
    congested_ctr_trend: "congested_ctr_trend",
    congested_ctr_epsilon: "congested_ctr_epsilon",
    congested_packet_group: "congested_packet_group",
    // spelled so upstream
    estimation_window_duration: "estimaton_window_duration",
});
yaml_struct!(sendsidebwe::PacketGroupConfig, "sendsidebwe.PacketGroupConfig", {
    min_packets: "min_packets",
    max_window_duration: "max_window_duration",
});
yaml_struct!(sendsidebwe::ProbePacketGroupConfig, "sendsidebwe.ProbePacketGroupConfig", {
    packet_group: "packet_group",
    settle_wait_num_rtt: "settle_wait_num_rtt",
    settle_wait_min: "settle_wait_min",
    settle_wait_max: "settle_wait_max",
});
yaml_struct!(sendsidebwe::CongestionSignalConfig, "sendsidebwe.CongestionSignalConfig", {
    min_number_of_groups: "min_number_of_groups",
    min_duration: "min_duration",
});
yaml_struct!(sendsidebwe::ProbeSignalConfig, "sendsidebwe.ProbeSignalConfig", {
    min_bytes_ratio: "min_bytes_ratio",
    min_duration_ratio: "min_duration_ratio",
    jqr_min_delay: "jqr_min_delay",
    dqr_max_delay: "dqr_max_delay",
    weighted_loss: "weighted_loss",
    jqr_min_weighted_loss: "jqr_min_weighted_loss",
    dqr_max_weighted_loss: "dqr_max_weighted_loss",
});
yaml_struct!(sendsidebwe::WeightedLossConfig, "sendsidebwe.WeightedLossConfig", {
    min_duration_for_loss_validity: "min_duration_for_loss_validity",
    base_duration: "base_duration",
    base_pps: "base_pps",
    loss_penalty_factor: "loss_penalty_factor",
});
