// Ported from LiveKit, pkg/config/config.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The `yaml` tags are doc comments on the fields, and the names `yaml_struct!` lists for
//   each struct; the decoding is `yaml.rs`. The original starts from its defaults by writing
//   them as YAML and reading that into an empty struct; here it starts from a copy of
//   `DEFAULT_CONFIG`, which is the same but for what YAML cannot tell apart.
// - The sections for what musivum leaves out (`IgnoredSection`: Redis, the node selector,
//   the relay and RPC between nodes, ingress, SIP, agents, the debug listener, tracing,
//   WebTransport, node statistics, and `room.room_configurations`) take any YAML and are not
//   read; `Config::new` logs each one that a file has. Their structs and defaults
//   (`WebTransportConfig`, `NodeSelectorConfig`, `SignalRelayConfig`, `RegionConfig`,
//   `IngressConfig`, `SIPConfig`, `DebugHandlerConfig`, `TracingConfig`, `NodeStatsConfig`)
//   are not ported.
// - The command line is not ported: `GenerateCLIFlags`, `ToCLIFlagNames`, `cliDefaultText`,
//   and the first half of `updateFromCLI`, which sets any key from a generated flag or its
//   environment variable. Its second half, the flags `cmd/server` defines by hand, is kept
//   as data (`BaseFlags`): `Config::new` is `NewConfig` without a command line, and
//   `Config::new_with_flags` is `NewConfig` with one.
// - `SetLogger` and `InitLoggerFromConfig` wait for `crate::protocol::logger` to have what
//   they call.
// - `homedir.Expand` and `os.ExpandEnv`, for the path of the key file, are functions at the
//   end, written new for what a path needs of them.
// - The package's errors are `Error`, with the texts of the original; an error of the system
//   has the text Rust gives it.
// - An `int` is an `i64`. A slice that is nil and one that is empty are the same `Vec`, so
//   `bind_addresses: []` is `bind_addresses` left out.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use crate::go::time::{self, Duration};
use crate::mediatransportutil::rtcconfig;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger as protocol_logger;
use crate::sfu;
use crate::sfu::bwe::{remotebwe, sendsidebwe};
use crate::sfu::{pacer, streamallocator};
use crate::{errorw, warnw};

use super::yaml::{self, AnyString, IgnoredSection, yaml_struct};
use super::{logger, metric, webhook};

const TURN_CREDENTIALS_INLINE_WARNING: &str = "both static credentials and credentials_file are set for TURN server, the inline values will be used";

// TURNMaxTTLSeconds is the operational maximum for TURN credential TTLs (24 hours).
// It bounds credential lifetime and prevents time.Duration overflow when multiplying
// by time.Second (values above ~9.2e9 seconds wrap on 64-bit builds).
pub const TURN_MAX_TTL_SECONDS: i64 = 24 * 60 * 60;

// DefaultTURNTTLSeconds is the default TTL for embedded TURN credentials, and the
// fallback used when a configured TTL is <= 0.
pub const DEFAULT_TURN_TTL_SECONDS: i64 = 300;

// DefaultExternalTURNTTLSeconds is the default TTL applied to external TURN
// (static-auth-secret) credentials when the configured TTL is left at 0.
pub const DEFAULT_EXTERNAL_TURN_TTL_SECONDS: i64 = 14400;

// DefaultTURNPerUserRelayAllocationLimit bounds how many concurrent relay
// allocations a single participant credential may hold on the embedded TURN
// server. It prevents one authenticated participant from exhausting the
// shared relay-port range for everyone else. A value <= 0 disables the quota.
pub const DEFAULT_TURN_PER_USER_RELAY_ALLOCATION_LIMIT: i64 = 12;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("key file others permissions must be set to 0")]
    KeyFileIncorrectPermission,
    #[error("turn credentials file others permissions must be set to 0")]
    TurnCredentialFileIncorrectPermission,
    #[error("turn secret file others permissions must be set to 0")]
    TurnSecretFileIncorrectPermission,
    #[error("one of key-file or keys must be provided")]
    KeysNotSet,
    #[error("turn secret is empty")]
    TurnSecretEmpty,
    #[error(
        "turn server has no usable credentials: set a non-empty secret/secret_file for dynamic auth, or username and credential for static auth"
    )]
    TurnServerNoCredentials,

    // The errors the original makes with `fmt.Errorf` and `errors.New` where they arise.
    #[error("could not parse config: {0}")]
    CouldNotParseConfig(#[source] yaml::Error),
    #[error("could not validate RTC config: {0}")]
    CouldNotValidateRtcConfig(#[source] rtcconfig::Error),
    #[error("could not validate congestion control config: {0}")]
    CouldNotValidateCongestionControlConfig(#[source] Box<Error>),
    #[error(
        "invalid send_side_bwe_pacer {pacer:?}, must be one of {:?}, {:?}, {:?}",
        pacer::PacerBehavior::PassThrough.as_str(),
        pacer::PacerBehavior::NoQueue.as_str(),
        pacer::PacerBehavior::Leakybucket.as_str()
    )]
    InvalidSendSideBwePacer { pacer: String },
    #[error("reading turn secret file {0:?}: {1}")]
    ReadingTurnSecretFile(String, #[source] std::io::Error),
    #[error("turn server {0:?} secret file {1:?}: {2}")]
    TurnServerSecretFile(String, String, #[source] Box<Error>),
    #[error("reading turn credentials file {0:?}: {1}")]
    ReadingTurnCredentialsFile(String, #[source] std::io::Error),
    #[error("invalid YAML in turn credentials file {0:?}")]
    InvalidYamlInTurnCredentialsFile(String),
    #[error("turn server {0:?}: {1}")]
    TurnServer(String, #[source] Box<Error>),
    #[error("Could not parse keys, it needs to be exactly, \"key: secret\", including the space")]
    CouldNotParseKeys,
    /// The error of `homedir.Expand`.
    #[error("{0}")]
    HomeDir(&'static str),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Yaml(#[from] yaml::Error),
}

impl Error {
    /// Go's `errors.Is(err, target)` for the errors of this package that have no fields.
    pub fn is(&self, target: &Error) -> bool {
        if std::mem::discriminant(self) == std::mem::discriminant(target) {
            return true;
        }
        match self {
            Error::CouldNotValidateCongestionControlConfig(err)
            | Error::TurnServerSecretFile(_, _, err)
            | Error::TurnServer(_, err) => err.is(target),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Config {
    /// `port` in YAML.
    pub port: u32,
    /// `bind_addresses` in YAML.
    pub bind_addresses: Vec<String>,
    // PrometheusPort is deprecated
    /// `prometheus_port` in YAML.
    pub prometheus_port: u32,
    /// `prometheus` in YAML.
    pub prometheus: PrometheusConfig,
    /// `debug_handler` in YAML.
    pub debug_handler: IgnoredSection,
    /// `webtransport` in YAML.
    pub web_transport: IgnoredSection,
    /// `rtc` in YAML.
    pub rtc: RtcConfig,
    /// `redis` in YAML.
    pub redis: IgnoredSection,
    /// `audio` in YAML.
    pub audio: sfu::AudioConfig,
    /// `video` in YAML.
    pub video: VideoConfig,
    /// `room` in YAML.
    pub room: RoomConfig,
    /// `turn` in YAML.
    pub turn: TurnConfig,
    /// `ingress` in YAML.
    pub ingress: IgnoredSection,
    /// `sip` in YAML.
    pub sip: IgnoredSection,
    /// `webhook` in YAML.
    pub web_hook: webhook::WebHookConfig,
    /// `node_selector` in YAML.
    pub node_selector: IgnoredSection,
    /// `key_file` in YAML.
    pub key_file: String,
    /// `keys` in YAML.
    pub keys: HashMap<String, String>,
    /// `region` in YAML.
    pub region: String,
    /// `signal_relay` in YAML.
    pub signal_relay: IgnoredSection,
    /// `psrpc` in YAML.
    pub psrpc: IgnoredSection,
    // Deprecated: LogLevel is deprecated
    /// `log_level` in YAML.
    pub log_level: String,
    /// `logging` in YAML.
    pub logging: LoggingConfig,
    /// `limit` in YAML.
    pub limit: LimitConfig,
    /// `agents` in YAML.
    pub agents: IgnoredSection,

    /// `development` in YAML.
    pub development: bool,

    /// `metric` in YAML.
    pub metric: metric::MetricConfig,
    /// `trace` in YAML.
    pub trace: IgnoredSection,

    /// `node_stats` in YAML.
    pub node_stats: IgnoredSection,

    /// `enable_data_tracks` in YAML.
    pub enable_data_tracks: bool,

    /// `enable_participant_data_blob` in YAML.
    pub enable_participant_data_blob: bool,

    /// `api` in YAML.
    pub api: ApiConfig,
}

yaml_struct!(Config, "config.Config", {
    port: "port",
    bind_addresses: "bind_addresses",
    prometheus_port: "prometheus_port",
    prometheus: "prometheus",
    debug_handler: "debug_handler",
    web_transport: "webtransport",
    rtc: "rtc",
    redis: "redis",
    audio: "audio",
    video: "video",
    room: "room",
    turn: "turn",
    ingress: "ingress",
    sip: "sip",
    web_hook: "webhook",
    node_selector: "node_selector",
    key_file: "key_file",
    keys: "keys",
    region: "region",
    signal_relay: "signal_relay",
    psrpc: "psrpc",
    log_level: "log_level",
    logging: "logging",
    limit: "limit",
    agents: "agents",
    development: "development",
    metric: "metric",
    trace: "trace",
    node_stats: "node_stats",
    enable_data_tracks: "enable_data_tracks",
    enable_participant_data_blob: "enable_participant_data_blob",
    api: "api",
});

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RtcConfig {
    /// Inline in YAML: its fields are fields of this section.
    pub rtc_config: rtcconfig::RtcConfig,

    /// `turn_servers` in YAML.
    pub turn_servers: Vec<TurnServer>,

    // EnableWarp turns on WARP = SPED (DTLS-in-STUN, saves DTLS round-trip) +
    // SNAP (SCTP INIT in SDP, saves SCTP/data-channel init round-trip).
    // Off by default; experimental.
    /// `enable_warp` in YAML.
    pub enable_warp: bool,

    // Deprecated
    /// `strict_acks` in YAML.
    pub strict_acks: bool,

    // Deprecated: use PacketBufferSizeVideo and PacketBufferSizeAudio
    /// `packet_buffer_size` in YAML.
    pub packet_buffer_size: i64,
    // Number of packets to buffer for NACK - video
    /// `packet_buffer_size_video` in YAML.
    pub packet_buffer_size_video: i64,
    // Number of packets to buffer for NACK - audio
    /// `packet_buffer_size_audio` in YAML.
    pub packet_buffer_size_audio: i64,

    // Throttle periods for pli/fir rtcp packets
    /// `pli_throttle` in YAML.
    pub pli_throttle: sfu::PliThrottleConfig,

    /// `rtx_budget` in YAML.
    pub rtx_budget: sfu::RtxBudgetConfig,

    /// `congestion_control` in YAML.
    pub congestion_control: CongestionControlConfig,

    /// `flexfec` in YAML.
    pub flex_fec: FlexFecConfig,

    // allow TCP and TURN/TLS fallback
    /// `allow_tcp_fallback` in YAML.
    pub allow_tcp_fallback: Option<bool>,

    // Signaling RTT threshold (in milliseconds) governing ICE/TCP fallback. On a UDP
    // failure, ICE/TCP is attempted only while the measured signaling RTT is below this
    // value; at or above it, supporting clients fall back directly to TURN/TLS. When 0
    // (the default), the RTT check is disabled and ICE/TCP is always attempted (for
    // clients that support it). A positive value also gates allow_udp_unstable_fallback.
    /// `tcp_fallback_rtt_threshold` in YAML.
    pub tcp_fallback_rtt_threshold: i64,

    // When enabled, an established UDP connection reporting sustained high packet loss is
    // migrated to ICE/TCP or TURN/TLS. Requires tcp_fallback_rtt_threshold to be set
    // (> 0). Disabled by default.
    /// `allow_udp_unstable_fallback` in YAML.
    pub allow_udp_unstable_fallback: bool,

    // force a reconnect on a publication error
    /// `reconnect_on_publication_error` in YAML.
    pub reconnect_on_publication_error: Option<bool>,

    // force a reconnect on a subscription error
    /// `reconnect_on_subscription_error` in YAML.
    pub reconnect_on_subscription_error: Option<bool>,

    // force a reconnect on a data channel error
    /// `reconnect_on_data_channel_error` in YAML.
    pub reconnect_on_data_channel_error: Option<bool>,

    // Deprecated
    /// `data_channel_max_buffered_amount` in YAML.
    pub data_channel_max_buffered_amount: u64,

    // Threshold of data channel writing to be considered too slow, data packet could
    // be dropped for a slow data channel to avoid blocking the room.
    /// `datachannel_slow_threshold` in YAML.
    pub datachannel_slow_threshold: i64,

    // Target latency for lossy data channels, used to drop packets to reduce latency.
    /// `datachannel_lossy_target_latency` in YAML.
    pub datachannel_lossy_target_latency: Duration,

    // Target latency for data track channels, used to drop packets to keep the
    // per-subscriber buffer bounded under downlink congestion. Data tracks are a
    // low-latency lossy transport, so a stale queued frame is worse than a dropped
    // one. Set to 0 to disable buffering control (unbounded buffering).
    /// `datachannel_data_track_target_latency` in YAML.
    pub datachannel_data_track_target_latency: Duration,

    /// `forward_stats` in YAML.
    pub forward_stats: ForwardStatsConfig,

    // enable rtp stream restart detection for published tracks
    /// `enable_rtp_stream_restart_detection` in YAML.
    pub enable_rtp_stream_restart_detection: bool,
}

yaml_struct!(RtcConfig, "config.RTCConfig", {
    rtc_config: inline,
    turn_servers: "turn_servers",
    enable_warp: "enable_warp",
    strict_acks: "strict_acks",
    packet_buffer_size: "packet_buffer_size",
    packet_buffer_size_video: "packet_buffer_size_video",
    packet_buffer_size_audio: "packet_buffer_size_audio",
    pli_throttle: "pli_throttle",
    rtx_budget: "rtx_budget",
    congestion_control: "congestion_control",
    flex_fec: "flexfec",
    allow_tcp_fallback: "allow_tcp_fallback",
    tcp_fallback_rtt_threshold: "tcp_fallback_rtt_threshold",
    allow_udp_unstable_fallback: "allow_udp_unstable_fallback",
    reconnect_on_publication_error: "reconnect_on_publication_error",
    reconnect_on_subscription_error: "reconnect_on_subscription_error",
    reconnect_on_data_channel_error: "reconnect_on_data_channel_error",
    data_channel_max_buffered_amount: "data_channel_max_buffered_amount",
    datachannel_slow_threshold: "datachannel_slow_threshold",
    datachannel_lossy_target_latency: "datachannel_lossy_target_latency",
    datachannel_data_track_target_latency: "datachannel_data_track_target_latency",
    forward_stats: "forward_stats",
    enable_rtp_stream_restart_detection: "enable_rtp_stream_restart_detection",
});

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnServer {
    /// `host` in YAML.
    pub host: String,
    /// `port` in YAML.
    pub port: i64,
    /// `protocol` in YAML.
    pub protocol: String,
    /// `username` in YAML.
    pub username: String,
    /// `credential` in YAML.
    pub credential: String,
    // YAML file containing username and credential. Inline static credentials take precedence.
    /// `credentials_file` in YAML.
    pub credentials_file: String,
    // Secret is used for TURN static auth secrets mechanism. When provided,
    // dynamic credentials are generated using HMAC-SHA1 instead of static Username/Credential
    /// `secret` in YAML.
    pub secret: String,
    // File containing the secret
    /// `secret_file` in YAML.
    pub secret_file: String,
    // TTL is the time-to-live in seconds for generated credentials when using Secret.
    // Defaults to 14400 seconds (4 hours) when 0. Negative values fall back to the 5m
    // default and large values are capped at TURNMaxTTLSeconds.
    /// `ttl` in YAML.
    pub ttl: i64,
}

yaml_struct!(TurnServer, "config.TURNServer", {
    host: "host",
    port: "port",
    protocol: "protocol",
    username: "username",
    credential: "credential",
    credentials_file: "credentials_file",
    secret: "secret",
    secret_file: "secret_file",
    ttl: "ttl",
});

#[derive(Clone, Debug, PartialEq)]
pub struct CongestionControlConfig {
    /// `enabled` in YAML.
    pub enabled: bool,
    /// `allow_pause` in YAML.
    pub allow_pause: bool,

    /// `stream_allocator` in YAML.
    pub stream_allocator: streamallocator::StreamAllocatorConfig,

    /// `remote_bwe` in YAML.
    pub remote_bwe: remotebwe::RemoteBweConfig,

    /// `use_send_side_bwe_interceptor` in YAML.
    pub use_send_side_bwe_interceptor: bool,

    /// `use_send_side_bwe` in YAML.
    pub use_send_side_bwe: bool,
    /// `send_side_bwe_pacer` in YAML.
    pub send_side_bwe_pacer: String,
    /// `send_side_bwe` in YAML.
    pub send_side_bwe: sendsidebwe::SendSideBweConfig,
}

// Go's zero value, but for the probe mode of the stream allocator: the port has an enum
// for it, which cannot be the empty string that is neither mode.
impl Default for CongestionControlConfig {
    fn default() -> Self {
        CongestionControlConfig {
            enabled: false,
            allow_pause: false,
            stream_allocator: streamallocator::StreamAllocatorConfig {
                min_channel_capacity: 0,
                disable_estimation_unmanaged_tracks: false,
                probe_mode: streamallocator::ProbeMode::Padding,
                probe_overage_pct: 0,
                probe_min_bps: 0,
                paused_min_wait: Duration::ZERO,
            },
            remote_bwe: Default::default(),
            use_send_side_bwe_interceptor: false,
            use_send_side_bwe: false,
            send_side_bwe_pacer: String::new(),
            send_side_bwe: Default::default(),
        }
    }
}

yaml_struct!(CongestionControlConfig, "config.CongestionControlConfig", {
    enabled: "enabled",
    allow_pause: "allow_pause",
    stream_allocator: "stream_allocator",
    remote_bwe: "remote_bwe",
    use_send_side_bwe_interceptor: "use_send_side_bwe_interceptor",
    use_send_side_bwe: "use_send_side_bwe",
    send_side_bwe_pacer: "send_side_bwe_pacer",
    send_side_bwe: "send_side_bwe",
});

impl CongestionControlConfig {
    // Validate reports configuration that would otherwise be ignored at runtime.
    pub fn validate(&self) -> Result<(), Error> {
        if self.send_side_bwe_pacer.is_empty()
            || pacer::PacerBehavior::from_str(&self.send_side_bwe_pacer).is_ok()
        {
            return Ok(());
        }
        Err(Error::InvalidSendSideBwePacer {
            pacer: self.send_side_bwe_pacer.clone(),
        })
    }
}

// FlexFECConfig controls FlexFEC-03 recovery on the publisher -> SFU leg.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlexFecConfig {
    // negotiate flexfec-03 with publishers and use it to recover lost upstream packets
    /// `upstream_enabled` in YAML.
    pub upstream_enabled: bool,
}

yaml_struct!(FlexFecConfig, "config.FlexFECConfig", {
    upstream_enabled: "upstream_enabled",
});

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlayoutDelayConfig {
    /// `enabled` in YAML.
    pub enabled: bool,
    /// `min` in YAML.
    pub min: i64,
    /// `max` in YAML.
    pub max: i64,
}

yaml_struct!(PlayoutDelayConfig, "config.PlayoutDelayConfig", {
    enabled: "enabled",
    min: "min",
    max: "max",
});

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VideoConfig {
    /// `dynacast_pause_delay` in YAML.
    pub dynacast_pause_delay: Duration,
    /// `stream_tracker_manager` in YAML.
    pub stream_tracker_manager: sfu::StreamTrackerManagerConfig,

    /// `codec_regression_threshold` in YAML.
    pub codec_regression_threshold: i64,
}

yaml_struct!(VideoConfig, "config.VideoConfig", {
    dynacast_pause_delay: "dynacast_pause_delay",
    stream_tracker_manager: "stream_tracker_manager",
    codec_regression_threshold: "codec_regression_threshold",
});

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoomConfig {
    // enable rooms to be automatically created
    /// `auto_create` in YAML.
    pub auto_create: bool,
    /// `enabled_codecs` in YAML.
    pub enabled_codecs: Vec<CodecSpec>,
    /// `max_participants` in YAML.
    pub max_participants: u32,
    /// `empty_timeout` in YAML.
    pub empty_timeout: u32,
    /// `departure_timeout` in YAML.
    pub departure_timeout: u32,
    /// `enable_remote_unmute` in YAML.
    pub enable_remote_unmute: bool,
    /// `playout_delay` in YAML.
    pub playout_delay: PlayoutDelayConfig,
    /// `sync_streams` in YAML.
    pub sync_streams: bool,
    /// `create_room_timeout` in YAML.
    pub create_room_timeout: Duration,
    /// `create_room_attempts` in YAML.
    pub create_room_attempts: i64,
    // include room metadata in track webhook events instead of just room SID and name
    /// `enable_full_room_in_webhooks` in YAML.
    pub enable_full_room_in_webhooks: bool,
    // target room participant update batch chunk size in bytes
    /// `update_batch_target_size` in YAML.
    pub update_batch_target_size: i64,
    // deprecated, moved to limits
    /// `max_metadata_size` in YAML.
    pub max_metadata_size: u32,
    // deprecated, moved to limits
    /// `max_room_name_length` in YAML.
    pub max_room_name_length: i64,
    // deprecated, moved to limits
    /// `max_participant_identity_length` in YAML.
    pub max_participant_identity_length: i64,
    /// `room_configurations` in YAML: room settings by name, with what they say about
    /// egress and agents. The original reads them into messages of the protocol.
    pub room_configurations: IgnoredSection,
}

yaml_struct!(RoomConfig, "config.RoomConfig", {
    auto_create: "auto_create",
    enabled_codecs: "enabled_codecs",
    max_participants: "max_participants",
    empty_timeout: "empty_timeout",
    departure_timeout: "departure_timeout",
    enable_remote_unmute: "enable_remote_unmute",
    playout_delay: "playout_delay",
    sync_streams: "sync_streams",
    create_room_timeout: "create_room_timeout",
    create_room_attempts: "create_room_attempts",
    enable_full_room_in_webhooks: "enable_full_room_in_webhooks",
    update_batch_target_size: "update_batch_target_size",
    max_metadata_size: "max_metadata_size",
    max_room_name_length: "max_room_name_length",
    max_participant_identity_length: "max_participant_identity_length",
    room_configurations: "room_configurations",
});

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CodecSpec {
    /// `mime` in YAML.
    pub mime: String,
    /// `fmtp_line` in YAML.
    pub fmtp_line: String,
}

yaml_struct!(CodecSpec, "config.CodecSpec", {
    mime: "mime",
    fmtp_line: "fmtp_line",
});

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoggingConfig {
    /// Inline in YAML: its fields are fields of this section.
    pub config: logger::Config,
    /// `pion_level` in YAML.
    pub pion_level: String,
}

yaml_struct!(LoggingConfig, "config.LoggingConfig", {
    config: inline,
    pion_level: "pion_level",
});

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnConfig {
    /// `enabled` in YAML.
    pub enabled: bool,
    /// `domain` in YAML.
    pub domain: String,
    /// `cert_file` in YAML.
    pub cert_file: String,
    /// `key_file` in YAML.
    pub key_file: String,
    /// `tls_port` in YAML.
    pub tls_port: i64,
    /// `udp_port` in YAML.
    pub udp_port: i64,
    /// `relay_range_start` in YAML.
    pub relay_port_range_start: u16,
    /// `relay_range_end` in YAML.
    pub relay_port_range_end: u16,
    /// `external_tls` in YAML.
    pub external_tls: bool,
    /// `bind_addresses` in YAML.
    pub bind_addresses: Vec<String>,
    // ProxyProtocol makes the TURN TCP listener require a PROXY protocol (v1 or v2)
    // header on every connection and use the client address it carries. Needed when
    // a proxy in front of TURN dials from its own address (a TLS-terminating L4
    // proxy, a TCP reverse proxy): TURN echoes the address it sees back to the
    // client as XOR-MAPPED-ADDRESS, and browsers such as Firefox reject a loopback
    // or wildcard address there and abandon the allocation. Connections without
    // the header are rejected.
    /// `proxy_protocol` in YAML.
    pub proxy_protocol: bool,
    // ProxyProtocolTrustedCIDRs lists the proxies whose PROXY header is believed.
    // Connections from any other address are closed, so a client that reaches
    // tls_port directly cannot claim an arbitrary source address. Defaults to
    // loopback only, for a proxy on the same host.
    /// `proxy_protocol_trusted_cidrs` in YAML.
    pub proxy_protocol_trusted_cidrs: Vec<String>,
    // PerUserRelayAllocationLimit caps the number of concurrent relay allocations
    // a single participant credential may hold, keyed by the participant ID. This
    // stops one authenticated participant from consuming the shared relay-port
    // range and denying TURN relays to others. Defaults to
    // DefaultTURNPerUserRelayAllocationLimit; a value <= 0 disables the quota.
    /// `per_user_relay_allocation_limit` in YAML.
    pub per_user_relay_allocation_limit: i64,
    // TTL of the TURN credentials in seconds - defaults to 300. Values <= 0 fall back to the
    // 300s (5m) default and large values are capped at TURNMaxTTLSeconds.
    /// `ttl_seconds` in YAML.
    pub ttl_seconds: i64,
    // list of restricted peer CIDRs (loopback, link-local (unicast, multicast), multicast, private, unspecified) to allow access to.
    // By default (i. e. empty list), all restricted peer CIDRs are denied access.
    // When not empty, only the specified CIDRs are allowed access.
    // Note that this check is applied to restricted peer CIDRs only.
    /// `allow_restricted_peer_cidrs` in YAML.
    pub allow_restricted_peer_cidrs: Vec<String>,
    // list of peer CIDRs to deny access to
    // This applies to all peer CIDRs, including restricted ones.
    // Deny list takes precedence over allow list.
    /// `deny_peer_cidrs` in YAML.
    pub deny_peer_cidrs: Vec<String>,
}

yaml_struct!(TurnConfig, "config.TURNConfig", {
    enabled: "enabled",
    domain: "domain",
    cert_file: "cert_file",
    key_file: "key_file",
    tls_port: "tls_port",
    udp_port: "udp_port",
    relay_port_range_start: "relay_range_start",
    relay_port_range_end: "relay_range_end",
    external_tls: "external_tls",
    bind_addresses: "bind_addresses",
    proxy_protocol: "proxy_protocol",
    proxy_protocol_trusted_cidrs: "proxy_protocol_trusted_cidrs",
    per_user_relay_allocation_limit: "per_user_relay_allocation_limit",
    ttl_seconds: "ttl_seconds",
    allow_restricted_peer_cidrs: "allow_restricted_peer_cidrs",
    deny_peer_cidrs: "deny_peer_cidrs",
});

// NodeSelectorConfig, SignalRelayConfig and RegionConfig are not ported: `node_selector`
// and `signal_relay` are for a cluster.

// ---------------------------------

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LimitConfig {
    /// `num_tracks` in YAML.
    pub num_tracks: i32,
    /// `bytes_per_sec` in YAML.
    pub bytes_per_sec: f32,
    /// `subscription_limit_video` in YAML.
    pub subscription_limit_video: i32,
    /// `subscription_limit_audio` in YAML.
    pub subscription_limit_audio: i32,
    /// `max_metadata_size` in YAML.
    pub max_metadata_size: u32,
    // total size of all attributes on a participant
    /// `max_attributes_size` in YAML.
    pub max_attributes_size: u32,
    /// `max_room_name_length` in YAML.
    pub max_room_name_length: i64,
    /// `max_participant_identity_length` in YAML.
    pub max_participant_identity_length: i64,
    /// `max_participant_name_length` in YAML.
    pub max_participant_name_length: i64,

    /// `max_data_blob_key_length` in YAML.
    pub max_data_blob_key_length: i64,
    /// `max_data_blobs_size` in YAML.
    pub max_data_blob_size: u32,

    /// `max_data_track_custom_encoding_length` in YAML.
    pub max_data_track_custom_encoding_length: i64,

    // video subscribes per second on this node at which to measure, in shadow, how long
    // pacing would delay subscribes; empty turns the measurement off
    /// `subscribe_pace_shadow_rates` in YAML.
    pub subscribe_pace_shadow_rates: Vec<f64>,

    // maximum size (in bytes) of a single message read off a client-facing
    // signalling WebSocket connection. Frames larger than this are rejected by the
    // transport before being buffered, guarding against single-frame memory
    // exhaustion. A value of 0 disables the limit (unbounded).
    /// `signal_message_size_limit` in YAML.
    pub signal_message_size_limit: i64,
    // same as SignalMessageSizeLimit, but for agent worker WebSocket connections.
    /// `agent_signal_message_size_limit` in YAML.
    pub agent_signal_message_size_limit: i64,

    // maximum size (in bytes) of an HTTP request body accepted on the main API
    // listener (Twirp room/egress/ingress/SIP routes, etc). Requests larger than
    // this are rejected before their body is decoded, so large messages cannot
    // exhaust memory. A value of 0 disables the limit (unbounded).
    /// `max_api_request_body_size` in YAML.
    pub max_api_request_body_size: i64,
}

yaml_struct!(LimitConfig, "config.LimitConfig", {
    num_tracks: "num_tracks",
    bytes_per_sec: "bytes_per_sec",
    subscription_limit_video: "subscription_limit_video",
    subscription_limit_audio: "subscription_limit_audio",
    max_metadata_size: "max_metadata_size",
    max_attributes_size: "max_attributes_size",
    max_room_name_length: "max_room_name_length",
    max_participant_identity_length: "max_participant_identity_length",
    max_participant_name_length: "max_participant_name_length",
    max_data_blob_key_length: "max_data_blob_key_length",
    max_data_blob_size: "max_data_blobs_size",
    max_data_track_custom_encoding_length: "max_data_track_custom_encoding_length",
    subscribe_pace_shadow_rates: "subscribe_pace_shadow_rates",
    signal_message_size_limit: "signal_message_size_limit",
    agent_signal_message_size_limit: "agent_signal_message_size_limit",
    max_api_request_body_size: "max_api_request_body_size",
});

impl LimitConfig {
    pub fn check_room_name_length(&self, name: &str) -> bool {
        self.max_room_name_length == 0 || name.len() as i64 <= self.max_room_name_length
    }

    pub fn check_participant_identity_length(&self, identity: &str) -> bool {
        self.max_participant_identity_length == 0
            || identity.len() as i64 <= self.max_participant_identity_length
    }

    pub fn check_participant_name_length(&self, name: &str) -> bool {
        self.max_participant_name_length == 0
            || name.len() as i64 <= self.max_participant_name_length
    }

    pub fn check_metadata_size(&self, metadata: &str) -> bool {
        self.max_metadata_size == 0 || metadata.len() as u32 <= self.max_metadata_size
    }

    pub fn check_attributes_size(&self, attributes: &HashMap<String, String>) -> bool {
        if self.max_attributes_size == 0 {
            return true;
        }

        let mut total = 0;
        for (k, v) in attributes {
            total += k.len() + v.len();
        }
        total as u32 <= self.max_attributes_size
    }
}

// DataBlobKeyLength returns the length of the identifying content of a data blob key.
// The text form of the key message is not used, as it adds field names and quotes.
pub fn data_blob_key_length(key: Option<&livekit::DataBlobKey>) -> usize {
    match key.and_then(|key| key.key.as_ref()) {
        Some(livekit::data_blob_key::Key::Generic(generic)) => generic.len(),
        Some(livekit::data_blob_key::Key::SchemaId(schema_id)) => {
            let custom = match schema_id.encoding.as_ref().and_then(|e| e.value.as_ref()) {
                Some(livekit::data_track_schema_encoding::Value::Custom(custom)) => custom.len(),
                _ => 0,
            };
            schema_id.name.len() + custom
        }
        None => 0,
    }
}

impl LimitConfig {
    pub fn check_data_blob_key_length(&self, key: Option<&livekit::DataBlobKey>) -> bool {
        self.max_data_blob_key_length == 0
            || data_blob_key_length(key) as i64 <= self.max_data_blob_key_length
    }

    pub fn check_data_track_custom_encoding_length(&self, identifier: &str) -> bool {
        self.max_data_track_custom_encoding_length == 0
            || identifier.len() as i64 <= self.max_data_track_custom_encoding_length
    }

    pub fn check_data_track_frame_encoding(
        &self,
        encoding: Option<&livekit::DataTrackFrameEncoding>,
    ) -> bool {
        let Some(livekit::data_track_frame_encoding::Value::Custom(custom)) =
            encoding.and_then(|e| e.value.as_ref())
        else {
            return true;
        };
        !custom.is_empty() && self.check_data_track_custom_encoding_length(custom)
    }

    pub fn check_data_track_schema_id(&self, schema: Option<&livekit::DataTrackSchemaId>) -> bool {
        let Some(livekit::data_track_schema_encoding::Value::Custom(custom)) = schema
            .and_then(|s| s.encoding.as_ref())
            .and_then(|e| e.value.as_ref())
        else {
            return true;
        };
        !custom.is_empty() && self.check_data_track_custom_encoding_length(custom)
    }

    pub fn check_data_blobs_size(&self, data_blobs: &[Arc<livekit::DataBlob>]) -> bool {
        if self.max_data_blob_size == 0 {
            return true;
        }

        let mut total = 0;
        for data_blob in data_blobs {
            total += data_blob_key_length(data_blob.key.as_ref()) + data_blob.contents.len();
        }
        total as u32 <= self.max_data_blob_size
    }

    pub fn can_add_data_blob(
        &self,
        data_blobs: &[Arc<livekit::DataBlob>],
        to_add: &livekit::DataBlob,
    ) -> bool {
        if self.max_data_blob_size == 0 {
            return true;
        }

        let mut total = 0;
        for data_blob in data_blobs {
            total += data_blob_key_length(data_blob.key.as_ref()) + data_blob.contents.len();
        }
        (total + data_blob_key_length(to_add.key.as_ref()) + to_add.contents.len()) as u32
            <= self.max_data_blob_size
    }
}

// ---------------------------------

// IngressConfig and SIPConfig are not ported: ingress and SIP are left out.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApiConfig {
    // amount of time to wait for API to execute, default 2s
    /// `execution_timeout` in YAML.
    pub execution_timeout: Duration,

    // min amount of time to wait before checking for operation complete
    /// `check_interval` in YAML.
    pub check_interval: Duration,

    // max amount of time to wait before checking for operation complete
    /// `max_check_interval` in YAML.
    pub max_check_interval: Duration,

    // Backwards compatibility for room service api calls, will enable by default and remove in a future release
    /// `enable_psrpc_for_get_list_participants` in YAML.
    pub enable_psrpc_for_get_list_particpants: bool,
}

yaml_struct!(ApiConfig, "config.APIConfig", {
    execution_timeout: "execution_timeout",
    check_interval: "check_interval",
    max_check_interval: "max_check_interval",
    enable_psrpc_for_get_list_particpants: "enable_psrpc_for_get_list_participants",
});

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrometheusConfig {
    /// `port` in YAML.
    pub port: u32,
    /// `username` in YAML.
    pub username: String,
    /// `password` in YAML.
    pub password: String,
}

yaml_struct!(PrometheusConfig, "config.PrometheusConfig", {
    port: "port",
    username: "username",
    password: "password",
});

// DebugHandlerConfig is not ported: the listener it is for is left out.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ForwardStatsConfig {
    /// `summary_interval` in YAML.
    pub summary_interval: Duration,
    /// `report_interval` in YAML.
    pub report_interval: Duration,
    /// `report_window` in YAML.
    pub report_window: Duration,
}

yaml_struct!(ForwardStatsConfig, "config.ForwardStatsConfig", {
    summary_interval: "summary_interval",
    report_interval: "report_interval",
    report_window: "report_window",
});

// TracingConfig is not ported: tracing is left out.

pub fn default_api_config() -> ApiConfig {
    ApiConfig {
        execution_timeout: 2 * time::SECOND,
        check_interval: 100 * time::MILLISECOND,
        max_check_interval: 300 * time::SECOND,
        ..Default::default()
    }
}

// NodeStatsConfig and DefaultNodeStatsConfig are not ported: the statistics of a node are
// what a cluster chooses a node by.

pub static DEFAULT_CONFIG: LazyLock<Config> = LazyLock::new(|| Config {
    port: 7880,
    rtc: RtcConfig {
        rtc_config: rtcconfig::RtcConfig {
            use_external_ip: false,
            tcp_port: 7881,
            ice_port_range_start: 0,
            ice_port_range_end: 0,
            stun_servers: Vec::new(),
            ..Default::default()
        },
        packet_buffer_size: 500,
        packet_buffer_size_video: 500,
        packet_buffer_size_audio: 200,
        pli_throttle: sfu::DEFAULT_PLI_THROTTLE_CONFIG,
        rtx_budget: sfu::DEFAULT_RTX_BUDGET_CONFIG,
        datachannel_data_track_target_latency: 100 * time::MILLISECOND,
        congestion_control: CongestionControlConfig {
            enabled: true,
            allow_pause: false,
            stream_allocator: streamallocator::DEFAULT_STREAM_ALLOCATOR_CONFIG,
            remote_bwe: remotebwe::DEFAULT_REMOTE_BWE_CONFIG,
            use_send_side_bwe_interceptor: false,
            use_send_side_bwe: false,
            send_side_bwe_pacer: pacer::PacerBehavior::NoQueue.as_str().to_string(),
            send_side_bwe: sendsidebwe::DEFAULT_SEND_SIDE_BWE_CONFIG,
        },
        ..Default::default()
    },
    audio: sfu::DEFAULT_AUDIO_CONFIG,
    video: VideoConfig {
        dynacast_pause_delay: 5 * time::SECOND,
        stream_tracker_manager: sfu::DEFAULT_STREAM_TRACKER_MANAGER_CONFIG.clone(),
        codec_regression_threshold: 5,
    },
    room: RoomConfig {
        auto_create: true,
        enabled_codecs: [
            mime::MimeType::Pcmu,
            mime::MimeType::Pcma,
            mime::MimeType::Opus,
            mime::MimeType::Red,
            mime::MimeType::Vp8,
            mime::MimeType::H264,
            mime::MimeType::Vp9,
            mime::MimeType::Av1,
            mime::MimeType::H265,
            mime::MimeType::Rtx,
        ]
        .map(|mime_type| CodecSpec {
            mime: mime_type.as_str().to_string(),
            ..Default::default()
        })
        .to_vec(),
        empty_timeout: 5 * 60,
        departure_timeout: 20,
        create_room_timeout: 10 * time::SECOND,
        create_room_attempts: 3,
        update_batch_target_size: 128 * 1024,
        ..Default::default()
    },
    limit: LimitConfig {
        max_metadata_size: 512 * 1024,
        max_attributes_size: 64 * 1024,
        max_room_name_length: 256,
        max_participant_identity_length: 256,
        max_participant_name_length: 256,
        max_data_blob_key_length: 256,
        max_data_blob_size: 64000,
        max_data_track_custom_encoding_length: 32,
        signal_message_size_limit: 2 << 20,       // 2 MiB
        agent_signal_message_size_limit: 2 << 20, // 2 MiB
        max_api_request_body_size: 10 << 20,      // 10 MiB
        ..Default::default()
    },
    logging: LoggingConfig {
        pion_level: "error".to_string(),
        ..Default::default()
    },
    turn: TurnConfig {
        enabled: false,
        bind_addresses: vec!["0.0.0.0".to_string()],
        proxy_protocol_trusted_cidrs: vec!["127.0.0.0/8".to_string(), "::1/128".to_string()],
        ttl_seconds: DEFAULT_TURN_TTL_SECONDS,
        per_user_relay_allocation_limit: DEFAULT_TURN_PER_USER_RELAY_ALLOCATION_LIMIT,
        ..Default::default()
    },
    // NodeSelector, SignalRelay, Agents, PSRPC and NodeStats: not ported
    keys: HashMap::new(),
    metric: metric::DEFAULT_METRIC_CONFIG,
    web_hook: webhook::DEFAULT_WEB_HOOK_CONFIG,
    api: default_api_config(),
    enable_data_tracks: true,
    ..Default::default()
});

/// The flags of `cmd/server` that the second half of the original's `updateFromCLI` reads
/// by name, each `None` where the original has `!c.IsSet(..)`. `redis-host` and
/// `redis-password` are left out with Redis. The names and the environment variables are
/// the server's: `--dev`, `--key-file`, `--keys` (`LIVEKIT_KEYS`), `--region`
/// (`LIVEKIT_REGION`), `--turn-cert` (`LIVEKIT_TURN_CERT`), `--turn-key`
/// (`LIVEKIT_TURN_KEY`), `--node-ip` (`NODE_IP`), `--udp-port` (`UDP_PORT`), `--bind`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BaseFlags {
    pub dev: Option<bool>,
    pub key_file: Option<String>,
    pub keys: Option<String>,
    pub region: Option<String>,
    pub turn_cert: Option<String>,
    pub turn_key: Option<String>,
    pub node_ip: Option<String>,
    pub udp_port: Option<String>,
    pub bind: Option<Vec<String>>,
}

impl Config {
    pub fn new(conf_string: &str, strict_mode: bool) -> Result<Config, Error> {
        Config::new_with_flags(conf_string, strict_mode, None)
    }

    /// `NewConfig` of the original with a command line.
    pub fn new_with_flags(
        conf_string: &str,
        strict_mode: bool,
        c: Option<&BaseFlags>,
    ) -> Result<Config, Error> {
        // start with defaults
        let mut conf = DEFAULT_CONFIG.clone();

        if !conf_string.is_empty() {
            yaml::decode(conf_string, strict_mode, &mut conf)
                .map_err(Error::CouldNotParseConfig)?;
        }

        if let Some(c) = c {
            conf.update_from_cli(c)?;
        }

        conf.rtc
            .rtc_config
            .validate(conf.development)
            .map_err(Error::CouldNotValidateRtcConfig)?;

        conf.rtc
            .congestion_control
            .validate()
            .map_err(|err| Error::CouldNotValidateCongestionControlConfig(Box::new(err)))?;

        conf.normalize_turn_ttls();

        // expand env vars in filenames
        conf.key_file = homedir_expand(&expand_env(&conf.key_file))?;

        // set defaults for Turn relay if none are set
        if conf.turn.relay_port_range_start == 0 || conf.turn.relay_port_range_end == 0 {
            // to make it easier to run in dev mode/docker, default to two ports
            if conf.development {
                conf.turn.relay_port_range_start = 30000;
                conf.turn.relay_port_range_end = 30002;
            } else {
                conf.turn.relay_port_range_start = 30000;
                conf.turn.relay_port_range_end = 40000;
            }
        }

        if !conf.log_level.is_empty() {
            conf.logging.config.level = conf.log_level.clone();
        }
        if conf.logging.config.level.is_empty() && conf.development {
            conf.logging.config.level = "debug".to_string();
        }
        if !conf.logging.pion_level.is_empty() {
            let pion_level = conf.logging.pion_level.clone();
            conf.logging
                .config
                .component_levels
                .insert("transport.pion".to_string(), pion_level.clone());
            conf.logging
                .config
                .component_levels
                .insert("pion".to_string(), pion_level);
        }

        // copy over legacy limits
        if conf.room.max_metadata_size != 0 {
            conf.limit.max_metadata_size = conf.room.max_metadata_size;
        }
        if conf.room.max_participant_identity_length != 0 {
            conf.limit.max_participant_identity_length = conf.room.max_participant_identity_length;
        }
        if conf.room.max_room_name_length != 0 {
            conf.limit.max_room_name_length = conf.room.max_room_name_length;
        }

        // not in the original: what the file sets that musivum does not read
        for (section, ignored) in conf.ignored_sections() {
            if ignored.is_present() {
                warnw!(
                    protocol_logger::get_logger(),
                    "configuration section has no effect in musivum",
                    nil,
                    "section" => section,
                );
            }
        }

        Ok(conf)
    }

    /// The parts of the configuration that are taken from a file and not read, by their
    /// names in YAML.
    pub fn ignored_sections(&self) -> [(&'static str, &IgnoredSection); 12] {
        [
            ("debug_handler", &self.debug_handler),
            ("webtransport", &self.web_transport),
            ("redis", &self.redis),
            ("ingress", &self.ingress),
            ("sip", &self.sip),
            ("node_selector", &self.node_selector),
            ("signal_relay", &self.signal_relay),
            ("psrpc", &self.psrpc),
            ("agents", &self.agents),
            ("trace", &self.trace),
            ("node_stats", &self.node_stats),
            ("room.room_configurations", &self.room.room_configurations),
        ]
    }

    pub fn is_turns_enabled(&self) -> bool {
        if self.turn.enabled && self.turn.tls_port != 0 {
            return true;
        }
        for s in &self.rtc.turn_servers {
            if s.protocol == "tls" {
                return true;
            }
        }
        false
    }

    // configNode and ToCLIFlagNames are not ported, see above.

    pub fn validate_keys(&mut self) -> Result<(), Error> {
        // prefer keyfile if set
        if !self.key_file.is_empty() {
            let other_filter = 0o007;
            let st = std::fs::metadata(&self.key_file)?;
            if perm(&st) & other_filter != 0o000 {
                return Err(Error::KeyFileIncorrectPermission);
            }
            let f = std::fs::read_to_string(&self.key_file)?;
            self.keys = HashMap::new();
            yaml::decode(&f, false, &mut self.keys)?;
        }

        if self.keys.is_empty() {
            return Err(Error::KeysNotSet);
        }

        if !self.development {
            for (key, secret) in &self.keys {
                if secret.len() < 32 {
                    errorw!(
                        protocol_logger::get_logger(),
                        "secret is too short, should be at least 32 characters for security",
                        nil,
                        "apiKey" => key,
                    );
                }
            }
        }
        Ok(())
    }

    pub fn load_turn_secrets(&mut self) -> Result<(), Error> {
        let other_filter = 0o007;
        for s in self.rtc.turn_servers.iter_mut() {
            // trim first so a blank inline secret falls back to secret_file rather than being treated as set
            let inline_secret = s.secret.trim().to_string();
            if !s.secret_file.is_empty() && !inline_secret.is_empty() {
                warnw!(
                    protocol_logger::get_logger(),
                    "both secret and secret_file are set for TURN server, the hardcoded secret will be used",
                    nil,
                    "host" => s.host,
                    "port" => s.port,
                );
                s.secret = inline_secret;
            } else if !s.secret_file.is_empty() {
                let st = std::fs::metadata(&s.secret_file)?;
                if perm(&st) & other_filter != 0o000 {
                    return Err(Error::TurnSecretFileIncorrectPermission);
                }
                let data = std::fs::read(&s.secret_file)
                    .map_err(|err| Error::ReadingTurnSecretFile(s.secret_file.clone(), err))?;
                let data = String::from_utf8_lossy(&data);
                let secret = data.trim();
                if secret.is_empty() {
                    return Err(Error::TurnServerSecretFile(
                        s.host.clone(),
                        s.secret_file.clone(),
                        Box::new(Error::TurnSecretEmpty),
                    ));
                }
                s.secret = secret.to_string();
            } else {
                s.secret = inline_secret;
            }

            // Keep the static pair together; never combine one inline value with a
            // credential from a different source.
            if !s.credentials_file.is_empty() {
                if !s.username.is_empty() || !s.credential.is_empty() {
                    warnw!(
                        protocol_logger::get_logger(),
                        TURN_CREDENTIALS_INLINE_WARNING,
                        nil,
                        "host" => s.host,
                        "port" => s.port,
                    );
                } else {
                    let st = std::fs::metadata(&s.credentials_file)?;
                    if perm(&st) & other_filter != 0o000 {
                        return Err(Error::TurnCredentialFileIncorrectPermission);
                    }
                    let data = std::fs::read(&s.credentials_file).map_err(|err| {
                        Error::ReadingTurnCredentialsFile(s.credentials_file.clone(), err)
                    })?;
                    let mut credentials = TurnCredentials::default();
                    if yaml::unmarshal(&String::from_utf8_lossy(&data), &mut credentials).is_err() {
                        // YAML type errors may contain scalar values from the secret file.
                        return Err(Error::InvalidYamlInTurnCredentialsFile(
                            s.credentials_file.clone(),
                        ));
                    }
                    s.username = credentials.username;
                    s.credential = credentials.credential;
                }
            }

            // validate credential mode as an explicit union rather than inferring it from a
            // post-load empty secret (which would silently fall back to static credentials)
            let has_dynamic = !s.secret.is_empty();
            let has_static = !s.username.is_empty() && !s.credential.is_empty();
            if !has_dynamic && !has_static {
                return Err(Error::TurnServer(
                    s.host.clone(),
                    Box::new(Error::TurnServerNoCredentials),
                ));
            }
        }
        Ok(())
    }
}

// The struct that `LoadTURNSecrets` of the original reads a credentials file into.
#[derive(Default)]
struct TurnCredentials {
    username: String,
    credential: String,
}

yaml_struct!(TurnCredentials, "struct { Username string; Credential string }", {
    username: "username",
    credential: "credential",
});

// ClampTURNTTLSeconds bounds a TURN credential TTL to [DefaultTURNTTLSeconds, TURNMaxTTLSeconds]:
// non-positive values (which would otherwise produce already-expired credentials) fall back to
// the 5m default and overflowing values clamp to the max. The second return value reports whether
// the input was out of range.
pub fn clamp_turn_ttl_seconds(ttl_seconds: i64) -> (i64, bool) {
    if ttl_seconds <= 0 {
        (DEFAULT_TURN_TTL_SECONDS, true)
    } else if ttl_seconds > TURN_MAX_TTL_SECONDS {
        (TURN_MAX_TTL_SECONDS, true)
    } else {
        (ttl_seconds, false)
    }
}

impl Config {
    // NormalizeTURNTTLs clamps configured TURN TTLs to the safe range, warning on any
    // adjustment. Safe to call more than once.
    pub fn normalize_turn_ttls(&mut self) {
        let (clamped, changed) = clamp_turn_ttl_seconds(self.turn.ttl_seconds);
        if changed {
            warnw!(
                protocol_logger::get_logger(),
                "turn.ttl_seconds out of range, using safe value",
                nil,
                "configured" => self.turn.ttl_seconds,
                "clamped" => clamped,
                "max" => TURN_MAX_TTL_SECONDS,
            );
            self.turn.ttl_seconds = clamped;
        }
        for s in self.rtc.turn_servers.iter_mut() {
            // external TTL of 0 means "use the default", so only cap the upper bound here;
            // non-positive values fall back to the default when credentials are generated
            let ttl = s.ttl;
            if ttl > TURN_MAX_TTL_SECONDS {
                warnw!(
                    protocol_logger::get_logger(),
                    "turn_servers ttl out of range, using safe value",
                    nil,
                    "host" => s.host,
                    "configured" => ttl,
                    "clamped" => TURN_MAX_TTL_SECONDS,
                    "max" => TURN_MAX_TTL_SECONDS,
                );
                s.ttl = TURN_MAX_TTL_SECONDS;
            }
        }
    }

    // GenerateCLIFlags and cliDefaultText are not ported, see above.

    // The second half of the original's function: the flags the server defines by hand.
    // The first half, which sets any key from the flag that is generated for it, is not
    // ported.
    fn update_from_cli(&mut self, c: &BaseFlags) -> Result<(), Error> {
        if let Some(dev) = c.dev {
            self.development = dev;
        }
        if let Some(key_file) = &c.key_file {
            self.key_file = key_file.clone();
        }
        if let Some(keys) = &c.keys
            && self.unmarshal_keys(keys).is_err()
        {
            return Err(Error::CouldNotParseKeys);
        }
        if let Some(region) = &c.region {
            self.region = region.clone();
        }
        if let Some(turn_cert) = &c.turn_cert {
            self.turn.cert_file = turn_cert.clone();
        }
        if let Some(turn_key) = &c.turn_key {
            self.turn.key_file = turn_key.clone();
        }
        if let Some(node_ip) = &c.node_ip {
            // the original does not look at the error either
            let _ = self.rtc.rtc_config.node_ip.unmarshal_string(node_ip);
        }
        if let Some(udp_port) = &c.udp_port {
            let _ = self.rtc.rtc_config.udp_port.unmarshal_string(udp_port);
        }
        if let Some(bind) = &c.bind {
            self.bind_addresses = bind.clone();
        }
        Ok(())
    }

    pub fn unmarshal_keys(&mut self, keys: &str) -> Result<(), Error> {
        let mut temp: HashMap<String, AnyString> = HashMap::new();
        yaml::unmarshal(keys, &mut temp)?;

        self.keys = HashMap::with_capacity(temp.len());

        for (key, val) in temp {
            if let AnyString(Some(secret)) = val {
                self.keys.insert(key, secret);
            }
        }
        Ok(())
    }
}

// SetLogger and InitLoggerFromConfig are not ported yet: `crate::protocol::logger` has no
// `SetLogger` and no `InitFromConfig` for them to call.

// The permission bits of a file, Go's `Mode().Perm()`.
#[cfg(unix)]
fn perm(st: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    st.permissions().mode() & 0o777
}

// Go gives every file the bits of others there.
#[cfg(not(unix))]
fn perm(st: &std::fs::Metadata) -> u32 {
    if st.permissions().readonly() {
        0o444
    } else {
        0o666
    }
}

// What Go's `os.ExpandEnv` does for the path of a file, written new from what its
// documentation says: `$NAME` and `${NAME}` are replaced by what the environment has for
// the name, and by nothing if it has nothing. A `$` that no name follows stays here, also
// where that function takes it out (`${}`) or reads a name of the shell's own (`$1`, `$?`).
fn expand_env(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        let after = &rest[dollar + 1..];
        let (name, taken) = match after.strip_prefix('{') {
            Some(braced) => match braced.find('}') {
                Some(end) => (&braced[..end], end + 2),
                None => ("", 0),
            },
            None => {
                let len = after
                    .bytes()
                    .take_while(|b| *b == b'_' || b.is_ascii_alphanumeric())
                    .count();
                (&after[..len], len)
            }
        };
        if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
            out.push('$');
            rest = after;
            continue;
        }
        out.push_str(&std::env::var(name).unwrap_or_default());
        rest = &after[taken..];
    }
    out.push_str(rest);
    out
}

// `homedir.Expand` (mitchellh/go-homedir): a path that starts with `~` or `~/` has the
// home directory of the user for it; `~name` is an error. The home directory is what the
// environment says (`HOME`), where the original asks the system if that is empty.
fn homedir_expand(path: &str) -> Result<String, Error> {
    if path.is_empty() {
        return Ok(String::new());
    }

    if !path.starts_with('~') {
        return Ok(path.to_string());
    }

    if path.len() > 1 && !path[1..].starts_with('/') && !path[1..].starts_with('\\') {
        return Err(Error::HomeDir("cannot expand user-specific home dir"));
    }

    let dir = std::env::var("HOME").unwrap_or_default();
    if dir.is_empty() {
        return Err(Error::HomeDir("blank output when reading home directory"));
    }

    // `filepath.Join`, without the cleaning of the path it also does
    let rest = path[1..].trim_start_matches(['/', '\\']);
    if rest.is_empty() {
        return Ok(dir);
    }
    Ok(format!("{}/{}", dir.trim_end_matches('/'), rest))
}

#[cfg(test)]
#[path = "config_test.rs"]
mod config_test;
