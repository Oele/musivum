// Ported from LiveKit, pkg/rtc/transport.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, against musivum's connection layer
// (`crate::webrtc::peerconnection`, docs/webrtc-library.md) where the original is written
// against pion. The file is `transport_impl.rs`, because `rtc::transport` is the subpackage
// `pkg/rtc/transport`; `PCTransport` is `rtc::PcTransport`. What else differs:
//
// - Making the connection. `newPeerConnection` makes the media engine as upstream, the
//   layer's setting engine from the node's (`web_rtc_config.setting_engine.build()`) with
//   the settings of this file, and one `sfu::interceptor::BufferFactorySink` where upstream
//   registers its interceptors and pion its buffer factory (the RTX info extractor, the RTT
//   from XR responder with its empty callback, the pairings of `SimTracks`). The settings
//   `rtc` does not have are comments at their places (the layer's settings.rs lists them):
//   the DTLS curves, the HelloVerify skip, the DTLS handshake time limit, close by DTLS, SRTP replay protection, SCTP zero checksum, warp, detached
//   channels and blocking writes (the writers wait on the buffered amount instead,
//   `block_write`), firing `OnTrack` before the first packet. The address rewrite rules and
//   IP filter for a client that cannot do prflx over relay are applied to a copy of the
//   node's settings, whose addresses (`host_candidate_ips`) the connection advertises; when
//   they cannot be worked out that is logged as the rules not being set. pion's GCC
//   estimator (`UseSendSideBWEInterceptor`, off by default) is not carried over: with that
//   switch the remote estimator is used, and a warning says so (new). The unhandle-simulcast
//   interceptor has nothing to be installed in (sfu/interceptor/unhandlesimulcast.rs); the
//   sink takes the pairings of `SimTracks`. Without a buffer factory in the configuration
//   (`WebRTCConfig{}` of the tests) there is no sink, and the RTX and FEC pairs of a
//   description go nowhere, where the original calls a nil factory.
// - The connection is on a `Node` (one UDP socket for all connections), which
//   `TransportParams` has in addition: whoever owns the transports makes it once.
// - `iceTransport` is not there: the layer tells the selected candidate pair on the
//   connection (`on_selected_candidate_pair_change`), and `GetRTT` reads the round-trip time
//   of that pair from the connection's statistics (there only where the ICE agent sends
//   checks of its own, which an ICE-lite one does not). `ErrNoICETransport` cannot happen.
// - Gathering is over when a local description has been set: there are only host
//   candidates, the node's. `GatheringCompletePromise` has nothing to wait for, and the
//   gathering state is never "gathering" when the events worker looks, so the paths that
//   defer an ICE restart until gathering is done are kept and not taken.
// - TURN is not in the layer: the paths for relay candidates are ported as logic and reach
//   nothing. TCP candidates exist when the node has a TCP listener (`rtc.tcp_port`), and
//   "prefer TCP" and the TCP ICE connect timer act on them. mDNS candidates are filtered as
//   upstream.
// - Data channels are not detached: the writers of `sfu::datachannel` take the layer's
//   channel, and the reading goroutine is the channel's `on_message`, set when it opens. A
//   message longer than the original's read buffer ends the reading with the warning and
//   the closing of the channel that a failed read has.
// - Migration between nodes. `SetMid` and `SetSender` are not in the layer, and a single
//   node never takes a participant's connection over from another. `SetPreviousSdp` is
//   kept; `initPCWithPreviousAnswer` stops where it would set the mid of a media section
//   with an error, which makes `SetPreviousSdp` ask for a full reconnect, as it does when
//   there is no previous answer. `preparePC` (data channel only) is kept whole; reusing a
//   sender after migration (`AddTrack`) replaces its track where pion sets the sender.
// - The events queue is `utils::TypedOpsQueue`, as upstream: its worker is a thread
//   (utils/opsqueue.rs), so a handler may block as a goroutine may. An event holds the
//   transport weakly where Go embeds the pointer. The fields upstream touches only on that
//   worker are in a mutex of their own (`events`), held for each read or write only, never
//   across a call out, as upstream holds no lock there.
// - `errors.Wrap` is `Error::Wrap`; `errors.Is(err, webrtc.ErrUnsupportedCodec)` looks
//   through it. Errors of the connection are `Error::Webrtc`, of the SDP helpers
//   `Error::Sdp`. The checks for `ErrConnectionClosed` compare with the layer's.
// - `MarshalLogObject` and `MarshalLogArray` are `Debug`. The statistics of a candidate pair
//   that are logged when ICE may have failed are those `rtc` has: no time stamps of the
//   first and last requests and responses.
// - `CreateDataChannelIfEmpty` gives the id 0 where the original dereferences a nil id.
//
// Who holds whom: the transport holds its connection (the only handle), its sink, stream
// allocator, estimator, pacer, the writers of its data channels and, in its parameters, its
// handler. Everything the connection, its data channels, the timers, the debouncer, the
// events, the sink's RTCP writer and the stream allocator's RTT getter call back holds the
// transport weakly. The stream allocator's callback for stream state changes holds the
// handler strongly, as the parameters do: a handler that holds its transport strongly makes
// a cycle that its owner breaks when it lets go of the transport.

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use rand::RngExt;

use crate::config;
use crate::go::atomic;
use crate::go::time::{self, Duration, Time, Timer};
use crate::mediatransportutil::rtcconfig;
use crate::mediatransportutil::twcc;
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::sdp as lksdp;
use crate::protocol::utils::mono;
use crate::rtc::transport::{Handler, NegotiationState};
use crate::rtc::types;
use crate::rtc::{
    ClientInfo, DirectionConfig, Error, RtcpFeedbackConfig, WebRtcConfig, create_media_engine,
    filter_codecs, is_ipv6, maybe_truncate_ip, merge_codecs_by_mime,
};
use crate::sfu::buffer;
use crate::sfu::bwe::{self, remotebwe, sendsidebwe};
use crate::sfu::datachannel::{BLOCK_WRITE_BUFFERED_AMOUNT, DataChannelWriter};
use crate::sfu::interceptor::{
    BufferFactorySink, BufferFactorySinkParams, OnStreamFound, RtcpWriter, RttFromXrFactory,
    SimulcastTrackInfo,
};
use crate::sfu::pacer::{self, PacerBehavior};
use crate::sfu::rtpextension::playoutdelay as pd;
use crate::sfu::streamallocator::{self, StreamAllocator, StreamAllocatorParams};
use crate::sfu::utils as sfuutils;
use crate::telemetry::prometheus;
use crate::utils;
use crate::webrtc;
use crate::webrtc::ice;
use crate::webrtc::peerconnection::{
    Configuration, DataChannel, DataChannelInit, IceCandidate, IceCandidateInit, IceCandidatePair,
    IceCandidateStats, IceConnectionState, IceGatheringState, IceProtocol, MediaEngine, Node,
    OfferOptions, PeerConnection, PeerConnectionState, RtpReceiver, RtpSender, RtpTransceiver,
    RtpTransceiverDirection, RtpTransceiverInit, SdpType, SessionDescription, SettingEngine,
    StatsReportEntry,
};
use crate::webrtc::rtcp;
use crate::webrtc::sdp;
use crate::webrtc::{PacketSink, PayloadType, RtpCodecType, StreamInfo};
// For the tests, which take it from here.
#[cfg(test)]
use crate::webrtc::RtpCodecParameters;
use crate::{debugw, errorw, infow, warnw};

pub const LOSSY_DATA_CHANNEL: &str = "_lossy";
pub const RELIABLE_DATA_CHANNEL: &str = "_reliable";
pub const DATA_TRACK_DATA_CHANNEL: &str = "_data_track";

const FAST_NEGOTIATION_FREQUENCY: Duration = Duration::from_millis(10);
const NEGOTIATION_FREQUENCY: Duration = Duration::from_millis(150);
pub const NEGOTIATION_FAILED_TIMEOUT: Duration = Duration::from_secs(15);
const DTLS_RETRANSMISSION_INTERVAL: Duration = Duration::from_millis(100);
// `dtlsHandshakeTimeout` (a minute): `rtc` has no such setting (see the header)

const ICE_DISCONNECTED_TIMEOUT: Duration = Duration::from_secs(10); // compatible for ice-lite with firefox client
pub const ICE_FAILED_TIMEOUT: Duration = Duration::from_secs(5); // time between disconnected and failed
pub const ICE_FAILED_TIMEOUT_TOTAL: Duration =
    Duration::from_nanos(ICE_FAILED_TIMEOUT.nanoseconds() + ICE_DISCONNECTED_TIMEOUT.nanoseconds()); // total time between connecting and failure
const ICE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(2); // pion's default

const MIN_TCP_ICE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TCP_ICE_CONNECT_TIMEOUT: Duration = Duration::from_secs(12); // js-sdk has a default 15s timeout for first connection, let server detect failure earlier before that

const MIN_CONNECT_TIMEOUT_AFTER_ICE: Duration = Duration::from_secs(10);
const MAX_CONNECT_TIMEOUT_AFTER_ICE: Duration = Duration::from_secs(20); // max duration for waiting pc to connect after ICE is connected

const SHORT_CONNECTION_THRESHOLD: Duration = Duration::from_secs(90);

const DATA_CHANNEL_BUFFER_SIZE: usize = 65535;
const LOSSY_DATA_CHANNEL_MIN_BUFFERED_AMOUNT: u64 = 8 * 1024;

// `errors.Wrap(err, message)`
fn wrap(err: impl Into<Error>, message: &'static str) -> Error {
    Error::Wrap {
        message,
        cause: Box::new(err.into()),
    }
}

// `errors.Is(err, target)` for an error of the connection, through `errors.Wrap`
fn is_webrtc_error(err: &Error, target: &webrtc::Error) -> bool {
    match err {
        Error::Webrtc(err) => err == target,
        Error::Wrap { cause, .. } => is_webrtc_error(cause, target),
        _ => false,
    }
}

// -------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Signal {
    IceGatheringComplete,
    LocalIceCandidate,
    RemoteIceCandidate,
    SendOffer,
    RemoteDescriptionReceived,
    IceRestart,
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Signal::IceGatheringComplete => "ICE_GATHERING_COMPLETE",
            Signal::LocalIceCandidate => "LOCAL_ICE_CANDIDATE",
            Signal::RemoteIceCandidate => "REMOTE_ICE_CANDIDATE",
            Signal::SendOffer => "SEND_OFFER",
            Signal::RemoteDescriptionReceived => "REMOTE_DESCRIPTION_RECEIVED",
            Signal::IceRestart => "ICE_RESTART",
        })
    }
}

// -------------------------------------------------------

// `data any` of the original's event
#[derive(Debug)]
enum EventData {
    None,
    LocalIceCandidate(Option<IceCandidate>),
    RemoteIceCandidate(IceCandidateInit),
    RemoteDescription(Box<RemoteDescriptionData>),
}

struct Event {
    // `*PCTransport` embedded upstream
    pc_transport: Weak<PcTransport>,
    signal: Signal,
    data: EventData,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PCTransport:Event{{signal: {}, data: {:?}}}",
            self.signal, self.data
        )
    }
}

// -------------------------------------------------------

struct WrappedIceCandidatePairLogger<'a> {
    pair: Option<&'a IceCandidatePair>,
}

impl fmt::Debug for WrappedIceCandidatePairLogger<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(pair) = self.pair else {
            return f.write_str("{}");
        };

        let mut e = f.debug_struct("");
        let local = pair.local();
        e.field("localProtocol", &local.protocol.to_string());
        e.field("localCandidateType", &local.typ.to_string());
        e.field("localAddress", &local.address);
        e.field("localPort", &local.port);
        let remote = pair.remote();
        e.field("remoteProtocol", &remote.protocol.to_string());
        e.field("remoteCandidateType", &remote.typ.to_string());
        e.field("remoteAddress", &maybe_truncate_ip(&remote.address));
        e.field("remotePort", &remote.port);
        if !remote.related_address.is_empty() {
            e.field(
                "relatedAddress",
                &maybe_truncate_ip(&remote.related_address),
            );
            e.field("relatedPort", &remote.related_port);
        }
        e.finish()
    }
}

// -------------------------------------------------------------------

#[derive(Clone)]
struct TrackDescription {
    mid: String,
    sender: Option<Arc<RtpSender>>,
}

impl fmt::Debug for TrackDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut e = f.debug_struct("");
        e.field("mid", &self.mid);
        if let Some(sender) = &self.sender
            && let Some(track) = sender.track()
        {
            e.field("trackID", &track.id());
        }
        e.finish()
    }
}

// -------------------------------------------------------------------

// PCTransport is a wrapper around PeerConnection, with some helper methods
pub struct PcTransport {
    params: TransportParams,
    pc: PeerConnection,
    // `iceTransport`: see the header
    me: MediaEngine,
    // what upstream registers as interceptors and pion's buffer factory, when the
    // configuration has a buffer factory
    sink: Option<Arc<BufferFactorySink>>,

    // what `lock sync.RWMutex` guards upstream
    lock: RwLock<PcTransportInner>,

    reset_short_conn_on_ice_restart: atomic::Bool,
    signaling_rtt: atomic::Uint32, // milliseconds

    debounced_negotiate: sfuutils::Debouncer,

    // `rtxInfoExtractorFactory`: the sink's

    // stream allocator for subscriber PC
    stream_allocator: Option<Arc<StreamAllocator>>,

    // only for subscriber PC
    // (`bwe`: nothing reads it after `NewPCTransport` has given it to the stream allocator
    // and the pacer, which hold it)
    pacer: Option<Arc<dyn pacer::Pacer>>,

    // transceivers (senders) waiting for SetRemoteDescription (offer) to happen before
    // SetCodecPreferences can be invoked on them.
    // Pion adapts codecs/payload types from remote description.
    // If SetCodecPreferences are done before the remote description is processed,
    // it is possible that the transceiver gets payload types from media engine.
    // Subssequently if the peer sends an offer with different payload type for the
    // same codec, there could be two payload types for the same codec and the wrong
    // one could be used in the forwarding path. So, wait for `SetRemoteDescription`
    // to happen so that remote side payload types are adapted.
    senders_pending_config_mu: Mutex<Vec<ConfigureSenderParams>>,

    prefer_tcp: atomic::Bool,
    is_closed: atomic::Bool,

    // used to check for offer/answer pairing,
    // i. e. every offer should have an answer before another offer can be sent
    local_offer_id: atomic::Uint32,
    remote_answer_id: atomic::Uint32,

    remote_offer_id: atomic::Uint32,
    local_answer_id: atomic::Uint32,

    events_queue: utils::TypedOpsQueue<Event>,

    connection_details: types::IceConnectionDetails,
    selected_pair: atomic::Pointer<IceCandidatePair>,

    // the following should be accessed only in event processing go routine
    events: Mutex<EventsState>,
    negotiate_counter: atomic::Int32,

    this: Weak<PcTransport>,
}

// What `lock` guards.
#[derive(Default)]
struct PcTransportInner {
    first_offer_received: bool,
    first_offer_no_data_channel: bool,
    reliable_dc: Option<Arc<DataChannelWriter<DataChannel>>>,
    reliable_dc_opened: bool,
    lossy_dc: Option<Arc<DataChannelWriter<DataChannel>>>,
    lossy_dc_opened: bool,
    data_track_dc: Option<Arc<DataChannelWriter<DataChannel>>>,
    unlabeled_data_channels: Vec<Arc<DataChannelWriter<DataChannel>>>,

    ice_first_started_at: Time,
    ice_first_connected_at: Time,
    peer_connection_first_connected_at: Time,
    peer_connection_lastconnected_at: Time,
    tcp_ice_timer: Option<Timer>,
    connect_after_ice_timer: Option<Timer>, // timer to wait for pc to connect after ice connected

    has_fully_established_recorded: bool,

    debounce_pending: bool,
    last_negotiate: Time,

    on_negotiation_state_changed: Option<Arc<dyn Fn(NegotiationState) + Send + Sync>>,

    previous_answer: Option<SessionDescription>,
    // track id -> description map in previous offer sdp
    previous_track_description: HashMap<String, TrackDescription>,
    can_reuse_transceiver: bool,

    may_failed_ice_stats: Vec<IceCandidatePairStats>,
    may_failed_ice_stats_timer: Option<Timer>,

    num_outstanding_audios: u32,
    num_request_sent_audios: u32,
    num_outstanding_videos: u32,
    num_request_sent_videos: u32,

    // parsed forms kept so that a remote description is parsed once per negotiation
    remote_offer_parsed: Option<Arc<sdp::SessionDescription>>,
}

// What upstream touches only on the events worker.
#[derive(Default)]
struct EventsState {
    cache_local_candidates: bool,
    cached_local_candidates: Vec<Option<IceCandidate>>,
    pending_remote_candidates: Vec<IceCandidateInit>,
    restart_after_gathering: bool,
    restart_at_next_offer: bool,
    negotiation_state: NegotiationState,
    signal_state_check_timer: Option<Timer>,
    current_offer_ice_credential: String, // ice user:pwd, for publish side ice restart checking
    pending_restart_ice_offer: Option<SessionDescription>,
    // parsed forms kept so that a remote description is parsed once per negotiation
    pending_restart_ice_offer_parsed: Option<sdp::SessionDescription>,
}

#[derive(Clone)]
pub struct TransportParams {
    pub handler: Arc<dyn Handler>,
    pub protocol_version: types::ProtocolVersion,
    pub config: Arc<WebRtcConfig>,
    pub twcc: Option<Arc<twcc::Responder>>,
    pub direction_config: DirectionConfig,
    pub congestion_control_config: config::CongestionControlConfig,
    pub enabled_publish_codecs: Vec<livekit::Codec>,
    pub enabled_subscribe_codecs: Vec<livekit::Codec>,
    pub logger: Logger,
    pub transport: livekit::SignalTarget,
    pub sim_tracks: HashMap<u32, SimulcastTrackInfo>,
    pub client_info: ClientInfo,
    pub is_offerer: bool,
    pub is_send_side: bool,
    pub allow_playout_delay: bool,
    pub use_one_shot_signalling_mode: bool,
    pub exclude_ipv6_local_candidates: bool,
    pub fire_on_track_by_sdp: bool,
    pub data_channel_max_buffered_amount: u64,
    pub datachannel_slow_threshold: i64,
    pub datachannel_lossy_target_latency: Duration,
    pub datachannel_data_track_target_latency: Duration,

    // for development test
    pub datachannel_max_receiver_buffer_size: i64,

    pub enable_data_tracks: bool,
    pub enable_warp: bool,

    // New: the node the connection is on (see the header).
    pub node: Node,
}

fn new_peer_connection(
    params: &TransportParams,
) -> Result<(PeerConnection, MediaEngine, Option<Arc<BufferFactorySink>>), Error> {
    let mut direction_config = params.direction_config.clone();
    if params.allow_playout_delay {
        direction_config
            .rtp_header_extension
            .video
            .push(pd::PLAYOUT_DELAY_URI.to_string());
    }

    // Some of the browser clients do not handle H.264 High Profile in signalling properly.
    // They still decode if the actual stream is H.264 High Profile, but do not handle it well in signalling.
    // So, disable H.264 High Profile for SUBSCRIBER peer connection to ensure it is not offered.
    //
    // Single-PC mode registers the union of publish and subscribe codecs so subscriptions
    // can negotiate subscribe-only codecs; per-direction filtering happens at the transceiver
    // level (configureSenderCodecs, restrictReceiverCodecsToPublishList).
    let me = create_media_engine(
        &merge_codecs_by_mime(
            &params.enabled_publish_codecs,
            &params.enabled_subscribe_codecs,
        ),
        &direction_config,
        params.is_offerer,
    )?;

    let mut se = params.config.web_rtc_config.setting_engine.build();
    // `SetAnsweringDTLSRole` of config.go, on the node's engine upstream
    se.answering_dtls_role = params.config.answering_dtls_role;
    // `se.DisableMediaEngineCopy(true)`: a connection of the layer has the media engine it
    // is given
    // simulcast layer disable/enable signalled via signalling channel,
    // so disable rid pause in SDP
    se.ignore_rid_pause_for_recv = true;

    // Change elliptic curve to improve connectivity
    // https://github.com/pion/dtls/pull/474
    // (`se.SetDTLSEllipticCurves(elliptic.X25519, elliptic.P384, elliptic.P256)`: not in `rtc`)

    // Disable close by dtls to avoid peerconnection close too early in migration
    // https://github.com/pion/webrtc/pull/2961
    // (`se.DisableCloseByDTLS(true)`: not in `rtc`)

    // `se.DetachDataChannels()`, and `se.EnableDataChannelBlockWrite(true)` when
    // `DatachannelSlowThreshold` is above zero: the writers wait on the buffered amount
    // (`block_write`)
    if params.datachannel_max_receiver_buffer_size > 0 {
        se.sctp_max_receive_buffer_size = params.datachannel_max_receiver_buffer_size as u32;
    }
    // `se.SetFireOnTrackBeforeFirstRTP(true)` when `FireOnTrackBySdp`: the layer announces a
    // track with its first packet

    // `se.EnableSCTPZeroChecksum(true)` when the client supports it: not in `rtc`

    if params.enable_warp {
        debugw!(params.logger, "enable warp");
        // `se.EnableSped(true)`, `se.EnableSctpSnap(true)`: not in `rtc`
    }

    //
    // Disable SRTP replay protection (https://datatracker.ietf.org/doc/html/rfc3711#page-15).
    // Needed due to lack of RTX stream support in Pion.
    //
    // When clients probe for bandwidth, there are several possible approaches
    //   1. Use padding packet (Chrome uses this)
    //   2. Use an older packet (Firefox uses this)
    // Typically, these are sent over the RTX stream and hence SRTP replay protection will not
    // trigger. As Pion does not support RTX, when firefox uses older packet for probing, they
    // trigger the replay protection.
    //
    // That results in two issues
    //   - Firefox bandwidth probing is not successful
    //   - Pion runs out of read buffer capacity - this potentially looks like a Pion issue
    //
    // NOTE: It is not required to disable RTCP replay protection, but doing it to be symmetric.
    //
    // (`se.DisableSRTPReplayProtection(true)`, `se.DisableSRTCPReplayProtection(true)`: `rtc`
    // can size the window, not switch it off)
    if !params.protocol_version.supports_ice_lite()
        || !params.client_info.supports_prflx_over_relay()
    {
        // if client don't support prflx over relay which is only Firefox, disable ICE Lite to ensure that
        // aggressive nomination is handled properly. Firefox does aggressive nomination even if peer is
        // ICE Lite (see comment as to historical reasons: https://github.com/pion/ice/pull/739#issuecomment-2452245066).
        // pion/ice (as of v2.3.37) will accept all use-candidate switches when in ICE Lite mode.
        // That combined with aggressive nomination from Firefox could potentially lead to the two ends
        // ending up with different candidates.
        // As Firefox does not support migration, ICE Lite can be disabled.
        se.lite = false;
    }
    // ICE checks already prove the remote address, so the HelloVerifyRequest cookie only adds a round trip.
    // pion applies this only when this side ends up as the DTLS server.
    // (`se.SetDTLSInsecureSkipHelloVerify(true)`, `se.SetDTLSConnectContextMaker`: not in
    // `rtc`)
    se.set_dtls_retransmission_interval(DTLS_RETRANSMISSION_INTERVAL);
    se.set_ice_timeouts(
        ICE_DISCONNECTED_TIMEOUT,
        ICE_FAILED_TIMEOUT,
        ICE_KEEPALIVE_INTERVAL,
    );

    // if client don't support prflx over relay, we should not expose private address to it, use single external ip as host candidate
    if !params.client_info.supports_prflx_over_relay()
        && !params.config.web_rtc_config.nat_1to1_ips.is_empty()
    {
        let mut nat1to1_ips = Vec::new();
        let mut include_ips = Vec::new();
        for mapping in &params.config.web_rtc_config.nat_1to1_ips {
            let ips: Vec<&str> = mapping.split('/').collect();
            if ips.len() == 2 && ips[0] != ips[1] {
                nat1to1_ips.push(mapping.clone());
                include_ips.push(ips[1].to_string());
            }
        }
        if !nat1to1_ips.is_empty() {
            infow!(params.logger, "client doesn't support prflx over relay, use external ip only as host candidate", "ips" => nat1to1_ips);
            // the rules and the filter are set on a copy of the node's settings, whose
            // addresses the connection then advertises (see the header)
            let mut copy = params.config.web_rtc_config.setting_engine.clone();
            let mut res =
                rtcconfig::set_nat_1to1_address_rewrite_rules(&mut copy, &nat1to1_ips, false);
            copy.ip_filter = Some(Arc::new(move |ip: IpAddr| {
                // `ip.To4() == nil`
                let v4 = match ip {
                    IpAddr::V4(v4) => Some(v4),
                    IpAddr::V6(v6) => v6.to_ipv4_mapped(),
                };
                let Some(v4) = v4 else {
                    return true;
                };
                let ipstr = v4.to_string();
                include_ips.contains(&ipstr)
            }));
            if res.is_ok() {
                match copy.host_candidate_ips() {
                    Ok(ips) => se.host_candidate_ips = Some(ips),
                    Err(err) => res = Err(err),
                }
            }
            if let Err(err) = res {
                warnw!(params.logger, "failed to set ICE address rewrite rules", err, "ips" => nat1to1_ips);
            }
        }
    }

    // `se.LoggerFactory = pionlogger.NewLoggerFactory(params.Logger)`: the connection's
    // logger

    // The interceptors, which the sink is (see the header).
    if params.is_send_side
        && params
            .congestion_control_config
            .use_send_side_bwe_interceptor
        && !params.congestion_control_config.use_send_side_bwe
    {
        // pion's GCC estimator with the transport-wide header extension interceptor: not
        // carried over
        warnw!(
            params.logger,
            "send side BWE interceptor is not available, using remote BWE",
            nil
        );
    }
    let mut sink = None;
    if let Some(buffer_factory) = &params.config.buffer_factory {
        let rtt_from_xr = if !params.is_offerer {
            // sfu only use interceptor to send XR but don't read response from it (use buffer instead),
            // so use a empty callback here
            Some(RttFromXrFactory::new(|_rtt| {}))
        } else {
            None
        };
        // `NewUnhandleSimulcastInterceptorFactory(UnhandleSimulcastTracks(params.Logger,
        // params.SimTracks))` when there are simulcast tracks: see the header

        let set_twcc_for_video: OnStreamFound = {
            let buffer_factory = buffer_factory.clone();
            let twcc = params.twcc.clone();
            let logger = params.logger.clone();
            Box::new(move |info: &StreamInfo| {
                if !mime::is_mime_type_string_video(&info.mime_type) {
                    return;
                }
                // rtx stream don't have rtcp feedback, always set twcc for rtx stream
                let mut twcc_fb =
                    mime::get_mime_type_codec(&info.mime_type) == mime::MimeTypeCodec::Rtx;
                if !twcc_fb {
                    for fb in &info.rtcp_feedback {
                        if fb.r#type == webrtc::TYPE_RTCP_FB_TRANSPORT_CC {
                            twcc_fb = true;
                            break;
                        }
                    }
                }
                if !twcc_fb {
                    return;
                }

                let twcc_ext_id = sfuutils::get_header_extension_id(
                    &info.rtp_header_extensions,
                    &webrtc::RtpHeaderExtensionCapability {
                        uri: sdp::TRANSPORT_CC_URI.to_string(),
                    },
                );
                if twcc_ext_id != 0 {
                    if let Some(buffer) = buffer_factory.get_buffer(info.ssrc) {
                        debugw!(
                            logger,
                            "set twcc and ext id",
                            "ssrc" => info.ssrc,
                            "isRTX" => mime::get_mime_type_codec(&info.mime_type) == mime::MimeTypeCodec::Rtx,
                            "twccExtID" => twcc_ext_id,
                        );
                        buffer.set_twcc_and_ext_id(twcc.clone(), twcc_ext_id as u8);
                    } else {
                        warnw!(logger, "failed to get buffer for stream", nil, "ssrc" => info.ssrc);
                    }
                }
            })
        };
        // put rtx interceptor behind unhandle simulcast interceptor so it can get the correct mid & rid
        // (the RTX info extractor and its callback for a pair, `SetRTXPair` with a debug line,
        // are the sink's)
        // (pion binds the interceptors' RTCP writer to the connection: `create_peer_connection`
        // binds the sink's to the transport's)
        sink = Some(BufferFactorySink::new(BufferFactorySinkParams {
            buffer_factory: buffer_factory.clone(),
            on_stream_found: Some(set_twcc_for_video),
            sim_tracks: params.sim_tracks.clone(),
            rtt_from_xr,
            logger: params.logger.clone(),
        }));
    }

    let pc = PeerConnection::new(
        &params.node,
        Configuration {
            media_engine: me.clone(),
            setting_engine: se,
            ice_servers: params.config.web_rtc_config.ice_servers.clone(),
            packet_sink: sink.clone().map(|s| s as Arc<dyn PacketSink>),
            logger: params.logger.clone(),
        },
    )?;
    Ok((pc, me, sink))
}

impl PcTransport {
    pub fn new(params: TransportParams) -> Result<Arc<PcTransport>, Error> {
        // `if params.Logger == nil`: a `Logger` is never nil here

        let mut bwe: Option<Arc<dyn bwe::Bwe>> = None;
        let mut pacer: Option<Arc<dyn pacer::Pacer>> = None;
        if params.is_send_side {
            if params.congestion_control_config.use_send_side_bwe {
                infow!(params.logger, "using send side BWE", "pacerBehavior" => params.congestion_control_config.send_side_bwe_pacer);
                let b: Arc<dyn bwe::Bwe> =
                    sendsidebwe::SendSideBwe::new(sendsidebwe::SendSideBweParams {
                        config: params.congestion_control_config.send_side_bwe,
                        logger: params.logger.clone(),
                    });
                let behavior = params
                    .congestion_control_config
                    .send_side_bwe_pacer
                    .as_str();
                pacer = Some(match behavior.parse::<PacerBehavior>() {
                    Ok(PacerBehavior::PassThrough) => {
                        pacer::PassThrough::new(params.logger.clone(), Some(b.clone()))
                    }
                    Ok(PacerBehavior::NoQueue) => {
                        pacer::NoQueue::new(params.logger.clone(), Some(b.clone()))
                    }
                    _ => {
                        // leaky-bucket lands here: it is a recognized value, but LeakyBucket paces
                        // to a bitrate set through Pacer.SetBitrate and nothing drives that from
                        // the bandwidth estimate, so it would pace at a fixed rate and ignore
                        // congestion. Unknown values are rejected by CongestionControlConfig.
                        // Validate, so they only reach here from callers building TransportParams
                        // directly.
                        if !behavior.is_empty() {
                            warnw!(
                                params.logger,
                                "send side BWE pacer unavailable, falling back", nil,
                                "pacerBehavior" => behavior,
                                "fallback" => PacerBehavior::NoQueue.as_str(),
                            );
                        }
                        pacer::NoQueue::new(params.logger.clone(), Some(b.clone()))
                    }
                });
                bwe = Some(b);
            } else {
                bwe = Some(remotebwe::RemoteBwe::new(remotebwe::RemoteBweParams {
                    config: params.congestion_control_config.remote_bwe,
                    logger: params.logger.clone(),
                }));
                pacer = Some(pacer::PassThrough::new(params.logger.clone(), None));
            }
        }

        // `createPeerConnection`, the part before the callbacks
        let (pc, me, sink) = new_peer_connection(&params)?;

        let t = Arc::new_cyclic(|this: &Weak<PcTransport>| {
            let stream_allocator = match (&bwe, &pacer) {
                (Some(bwe), Some(pacer)) => {
                    let this = this.clone();
                    Some(StreamAllocator::new(
                        StreamAllocatorParams {
                            config: params.congestion_control_config.stream_allocator,
                            bwe: bwe.clone(),
                            pacer: pacer.clone(),
                            rtt_getter: Some(Box::new(move || match this.upgrade() {
                                Some(t) => t.get_rtt(),
                                None => (0.0, false),
                            })),
                            logger: params
                                .logger
                                .with_component(utils::COMPONENT_CONGESTION_CONTROL),
                        },
                        params.congestion_control_config.enabled,
                        params.congestion_control_config.allow_pause,
                    ))
                }
                _ => None,
            };

            PcTransport {
                connection_details: types::IceConnectionDetails::new(
                    params.transport,
                    params.logger.clone(),
                ),
                events_queue: utils::TypedOpsQueue::new(utils::OpsQueueParams {
                    name: "transport".to_string(),
                    min_size: 64,
                    logger: params.logger.clone(),
                    ..Default::default()
                }),
                pc,
                me,
                sink,
                lock: RwLock::new(PcTransportInner {
                    can_reuse_transceiver: true,
                    ..Default::default()
                }),
                reset_short_conn_on_ice_restart: atomic::Bool::new(false),
                signaling_rtt: atomic::Uint32::new(0),
                debounced_negotiate: sfuutils::Debouncer::new(NEGOTIATION_FREQUENCY),
                stream_allocator,
                pacer: pacer.clone(),
                senders_pending_config_mu: Mutex::new(Vec::new()),
                prefer_tcp: atomic::Bool::new(false),
                is_closed: atomic::Bool::new(false),
                local_offer_id: atomic::Uint32::new(0),
                remote_answer_id: atomic::Uint32::new(0),
                remote_offer_id: atomic::Uint32::new(0),
                local_answer_id: atomic::Uint32::new(0),
                selected_pair: atomic::Pointer::new(None),
                events: Mutex::new(EventsState {
                    negotiation_state: NegotiationState::None,
                    ..Default::default()
                }),
                negotiate_counter: atomic::Int32::new(0),
                this: this.clone(),
                params: params.clone(),
            }
        });
        t.local_offer_id
            .store(rand::rng().random_range(0..1u32 << 8) + 1);

        t.create_peer_connection();

        if let Some(stream_allocator) = &t.stream_allocator {
            let handler = t.params.handler.clone();
            stream_allocator.on_stream_state_change(move |update| {
                handler
                    .on_stream_state_change(update)
                    .map_err(|err| Box::new(err) as streamallocator::StreamStateChangeError)
            });
            stream_allocator.start();

            // `SetSendSideBWEInterceptor(bwe)`: not carried over (see the header)
        }

        t.events_queue.start();

        Ok(t)
    }

    // `createPeerConnection`, from where the connection is made: the callbacks, and the RTCP
    // writer of the interceptors (the sink), which writes on the connection.
    fn create_peer_connection(&self) {
        if let Some(sink) = &self.sink {
            let this = self.this.clone();
            let writer: Arc<dyn RtcpWriter> = Arc::new(
                move |pkts: &[Box<dyn rtcp::Packet>]| -> Result<(), webrtc::Error> {
                    match this.upgrade() {
                        Some(t) => t.pc.write_rtcp(pkts),
                        None => Err(webrtc::Error::ClosedPipe),
                    }
                },
            );
            sink.bind_rtcp_writer(writer);
        }

        if !self.params.use_one_shot_signalling_mode {
            // one shot signalling mode gathers all candidates and sends in answer
            self.pc
                .on_ice_gathering_state_change(self.weak(Self::on_ice_gathering_state_change));
            self.pc
                .on_ice_candidate(self.weak(Self::on_ice_candidate_trickle));
        }
        self.pc
            .on_ice_connection_state_change(self.weak(Self::on_ice_connection_state_change));
        self.pc
            .on_connection_state_change(self.weak(Self::on_peer_connection_state_change));

        self.pc.on_data_channel(self.weak(Self::on_data_channel));
        {
            let this = self.this.clone();
            self.pc.on_track(move |track, receiver| {
                if let Some(t) = this.upgrade() {
                    t.params.handler.on_track(track, receiver);
                }
            });
        }

        // `t.iceTransport = t.pc.SCTP().Transport().ICETransport()`: the layer tells the
        // selected pair on the connection
        self.pc.on_selected_candidate_pair_change(self.weak(
            |t: &PcTransport, pair: IceCandidatePair| {
                debugw!(t.params.logger, "selected ICE candidate pair changed", "pair" => WrappedIceCandidatePairLogger { pair: Some(&pair) });
                t.connection_details.set_selected_pair(&pair);
                let existing_pair = t.selected_pair.load();
                if let Some(existing_pair) = &existing_pair {
                    infow!(
                        t.params.logger,
                        "ice reconnected or switched pair",
                        "existingPair" => WrappedIceCandidatePairLogger { pair: Some(existing_pair) },
                        "newPair" => WrappedIceCandidatePairLogger { pair: Some(&pair) },
                    );
                }
                t.selected_pair.store(Some(Arc::new(pair)));
            },
        ));
    }

    // A callback that holds the transport weakly.
    fn weak<A: 'static>(
        &self,
        f: impl Fn(&PcTransport, A) + Send + Sync + 'static,
    ) -> impl Fn(A) + Send + Sync + 'static {
        let this = self.this.clone();
        move |a| {
            if let Some(t) = this.upgrade() {
                f(&t, a);
            }
        }
    }

    // The function of a timer, which holds the transport weakly.
    fn weak_timer(
        &self,
        f: impl Fn(&PcTransport) + Send + Sync + 'static,
    ) -> impl Fn() + Send + Sync + 'static {
        let this = self.this.clone();
        move || {
            if let Some(t) = this.upgrade() {
                f(&t);
            }
        }
    }

    pub fn rtp_stream_published(&self, ssrc: u32, mid: &str, rid: &str) {
        if let Some(sink) = &self.sink {
            sink.rtx_info_extractor_factory()
                .set_stream_info(ssrc, mid, rid, "");
        }
    }

    pub fn get_pacer(&self) -> Option<Arc<dyn pacer::Pacer>> {
        self.pacer.clone()
    }

    pub fn set_signaling_rtt(&self, rtt: u32) {
        self.signaling_rtt.store(rtt);
    }

    fn set_ice_started_at(&self, at: Time) {
        let mut inner = self.lock.write();
        if inner.ice_first_started_at.is_zero() {
            inner.ice_first_started_at = at;

            // checklist of ice agent will be cleared on ice failed, get stats before that
            inner.may_failed_ice_stats_timer = Some(time::after_func(
                ICE_FAILED_TIMEOUT_TOTAL - time::SECOND,
                self.weak_timer(|t| t.log_may_failed_ice_stats()),
            ));

            // set failure timer for tcp ice connection based on signaling RTT
            if self.prefer_tcp.load() {
                let signaling_rtt = self.signaling_rtt.load();
                if signaling_rtt < 1000 {
                    let mut tcp_ice_timeout =
                        Duration::from_millis(signaling_rtt.wrapping_mul(8) as i64);
                    if tcp_ice_timeout < MIN_TCP_ICE_CONNECT_TIMEOUT {
                        tcp_ice_timeout = MIN_TCP_ICE_CONNECT_TIMEOUT;
                    } else if tcp_ice_timeout > MAX_TCP_ICE_CONNECT_TIMEOUT {
                        tcp_ice_timeout = MAX_TCP_ICE_CONNECT_TIMEOUT;
                    }
                    debugw!(self.params.logger, "set TCP ICE connect timer", "timeout" => tcp_ice_timeout, "signalRTT" => signaling_rtt);
                    inner.tcp_ice_timer = Some(time::after_func(
                        tcp_ice_timeout,
                        self.weak_timer(move |t| {
                            if t.pc.ice_connection_state() == IceConnectionState::Checking {
                                infow!(t.params.logger, "TCP ICE connect timeout", "timeout" => tcp_ice_timeout, "signalRTT" => signaling_rtt);
                                t.log_may_failed_ice_stats();
                                t.handle_connection_failed(true);
                            }
                        }),
                    ));
                }
            }
        }
    }

    fn set_ice_connected_at(&self, at: Time) {
        let mut inner = self.lock.write();
        if inner.ice_first_connected_at.is_zero() {
            //
            // Record initial connection time.
            // This prevents reset of iceFirstConnectedAt if ICE goes `Connected` -> `Disconnected` -> `Connected`.
            //
            inner.ice_first_connected_at = at;

            // set failure timer for dtls handshake
            let ice_duration = at.sub(inner.ice_first_started_at);
            let conn_timeout_after_ice = MIN_CONNECT_TIMEOUT_AFTER_ICE
                .max(Duration::from_nanos(
                    ice_duration.nanoseconds().wrapping_mul(3),
                ))
                .min(MAX_CONNECT_TIMEOUT_AFTER_ICE);
            debugw!(self.params.logger, "setting connection timer after ICE connected", "timeout" => conn_timeout_after_ice, "iceDuration" => ice_duration);
            inner.connect_after_ice_timer = Some(time::after_func(
                conn_timeout_after_ice,
                self.weak_timer(move |t| {
                    let state = t.pc.connection_state();
                    // if pc is still checking or connected but not fully established after timeout, then fire connection fail
                    if state != PeerConnectionState::Closed
                        && state != PeerConnectionState::Failed
                        && !t.is_fully_established()
                    {
                        infow!(t.params.logger, "connect timeout after ICE connected", "timeout" => conn_timeout_after_ice, "iceDuration" => ice_duration);
                        t.handle_connection_failed(false);
                    }
                }),
            ));

            // clear tcp ice connect timer
            if let Some(timer) = inner.tcp_ice_timer.take() {
                timer.stop();
            }

            prometheus::record_peer_connection_state(self.params.transport, "ice_connected");
        }

        if let Some(timer) = inner.may_failed_ice_stats_timer.take() {
            timer.stop();
        }
        inner.may_failed_ice_stats = Vec::new();
    }

    fn log_may_failed_ice_stats(&self) {
        if self.pc.connection_state() == PeerConnectionState::Closed {
            return;
        }

        let pair_stats = self.pc.get_stats();
        let mut candidate_pair_stats = Vec::new();
        let mut candidate_stats = HashMap::new();
        for stat in pair_stats.iter() {
            match stat {
                StatsReportEntry::IceCandidatePair(stat) => candidate_pair_stats.push(stat),
                StatsReportEntry::LocalCandidate(stat)
                | StatsReportEntry::RemoteCandidate(stat) => {
                    candidate_stats.insert(stat.stats.id.clone(), stat);
                }
                _ => {}
            }
        }

        let candidate_to_string = |c: &IceCandidateStats, truncate: bool| {
            let mut ip = c.address.clone().unwrap_or_default();
            if truncate {
                ip = maybe_truncate_ip(&ip);
            }
            format!(
                "{}:{} {} type({}/{:?}), priority({})",
                ip, c.port, c.protocol, c.candidate_type, c.relay_protocol, c.priority
            )
        };
        let mut ice_stats = Vec::with_capacity(candidate_pair_stats.len());
        for pair_stat in candidate_pair_stats {
            let mut ice_stat = IceCandidatePairStats {
                state: format!("{:?}", pair_stat.state),
                nominated: pair_stat.nominated,
                requests_sent: pair_stat.requests_sent,
                responses_received: pair_stat.responses_received,
                requests_received: pair_stat.requests_received,
                responses_sent: pair_stat.responses_sent,
                ..Default::default()
            };
            if let Some(local) = candidate_stats.get(&pair_stat.local_candidate_id) {
                ice_stat.local = candidate_to_string(local, false);
            }
            if let Some(remote) = candidate_stats.get(&pair_stat.remote_candidate_id) {
                ice_stat.remote = candidate_to_string(remote, true);
            }
            ice_stats.push(ice_stat);
        }

        self.lock.write().may_failed_ice_stats = ice_stats;
    }

    fn reset_short_conn(&self) {
        infow!(
            self.params.logger,
            "resetting short connection on ICE restart"
        );
        let mut inner = self.lock.write();
        inner.ice_first_started_at = Time::ZERO;
        inner.ice_first_connected_at = Time::ZERO;
        inner.peer_connection_lastconnected_at = Time::ZERO;
        if let Some(timer) = inner.connect_after_ice_timer.take() {
            timer.stop();
        }
        if let Some(timer) = inner.tcp_ice_timer.take() {
            timer.stop();
        }
    }

    pub fn is_short_connection(&self, at: Time) -> (bool, Duration) {
        let inner = self.lock.read();

        if inner.ice_first_connected_at.is_zero() {
            return (false, Duration::ZERO);
        }

        let duration = at.sub(inner.ice_first_connected_at);
        (duration < SHORT_CONNECTION_THRESHOLD, duration)
    }

    fn set_peer_connection_connected_at(&self, at: Time) -> bool {
        let mut inner = self.lock.write();
        inner.peer_connection_lastconnected_at = at;
        if !inner.peer_connection_first_connected_at.is_zero() {
            return false;
        }

        inner.peer_connection_first_connected_at = at;
        prometheus::record_service_operation_success("peer_connection");
        prometheus::record_peer_connection_state(self.params.transport, "connected");
        true
    }

    fn on_ice_gathering_state_change(&self, state: IceGatheringState) {
        debugw!(self.params.logger, "ice gathering state change", "state" => state.to_string());
        if state != IceGatheringState::Complete {
            return;
        }

        self.post_event(Signal::IceGatheringComplete, EventData::None);
    }

    fn on_ice_candidate_trickle(&self, c: Option<IceCandidate>) {
        self.post_event(Signal::LocalIceCandidate, EventData::LocalIceCandidate(c));
    }

    fn handle_connection_failed(&self, force_short_conn: bool) {
        let mut is_short = force_short_conn;
        if !is_short {
            let duration;
            (is_short, duration) = self.is_short_connection(time::now());
            if is_short {
                let pair = self.selected_pair.load();
                debugw!(self.params.logger, "short ICE connection", "pair" => WrappedIceCandidatePairLogger { pair: pair.as_deref() }, "duration" => duration);
            }
        }

        self.params
            .handler
            .on_failed(is_short, &self.get_ice_connection_info());
    }

    fn on_ice_connection_state_change(&self, state: IceConnectionState) {
        debugw!(self.params.logger, "ice connection state change", "state" => state.to_string());
        match state {
            IceConnectionState::Connected => self.set_ice_connected_at(time::now()),
            IceConnectionState::Checking => self.set_ice_started_at(time::now()),
            _ => {}
        }
    }

    fn on_peer_connection_state_change(&self, state: PeerConnectionState) {
        debugw!(self.params.logger, "peer connection state change", "state" => state.to_string());
        match state {
            PeerConnectionState::Connected => {
                self.clear_conn_timer();
                let is_initial_connection = self.set_peer_connection_connected_at(time::now());
                if is_initial_connection {
                    self.params.handler.on_initial_connected();

                    self.maybe_notify_fully_established();
                }
            }
            PeerConnectionState::Failed => {
                self.clear_conn_timer();
                self.handle_connection_failed(false);
            }
            _ => {}
        }
    }

    fn on_data_channel(&self, dc: Arc<DataChannel>) {
        let this = self.this.clone();
        let weak_dc = Arc::downgrade(&dc);
        dc.on_open(move || {
            let (Some(t), Some(dc)) = (this.upgrade(), weak_dc.upgrade()) else {
                return;
            };
            t.on_data_channel_open(&dc);
        });
    }

    // The function the original gives `dc.OnOpen` in `onDataChannel`.
    fn on_data_channel_open(&self, dc: &Arc<DataChannel>) {
        debugw!(
            self.params.logger,
            &format!("{} data channel open", dc.label())
        );
        let mut kind = livekit::data_packet::Kind::Reliable;
        let mut is_data_track = false;
        let mut is_unlabeled = false;
        match dc.label() {
            RELIABLE_DATA_CHANNEL => kind = livekit::data_packet::Kind::Reliable,

            LOSSY_DATA_CHANNEL => kind = livekit::data_packet::Kind::Lossy,

            DATA_TRACK_DATA_CHANNEL => is_data_track = true,

            _ => {
                infow!(self.params.logger, "unlabeled datachannel added", "label" => dc.label());
                is_unlabeled = true;
            }
        }

        // `rawDC, err := dc.DetachWithDeadline()`: the writers take the channel itself

        let mut is_handled = true;
        {
            let mut inner = self.lock.write();
            if is_unlabeled {
                inner
                    .unlabeled_data_channels
                    .push(Arc::new(DataChannelWriter::new_reliable(
                        dc.clone(),
                        self.block_write(),
                        self.params.datachannel_slow_threshold,
                    )));
            } else if is_data_track {
                if !self.params.enable_data_tracks {
                    debugw!(self.params.logger, "data tracks not enabled");
                    is_handled = false;
                } else {
                    if let Some(dc) = &inner.data_track_dc {
                        let _ = dc.close();
                    }
                    inner.data_track_dc = Some(Arc::new(DataChannelWriter::new_unreliable(
                        dc.clone(),
                        self.block_write(),
                        self.params.datachannel_data_track_target_latency,
                        LOSSY_DATA_CHANNEL_MIN_BUFFERED_AMOUNT,
                    )));
                }
            } else if kind == livekit::data_packet::Kind::Reliable {
                if let Some(dc) = &inner.reliable_dc {
                    let _ = dc.close();
                }
                inner.reliable_dc = Some(Arc::new(DataChannelWriter::new_reliable(
                    dc.clone(),
                    self.block_write(),
                    self.params.datachannel_slow_threshold,
                )));
                inner.reliable_dc_opened = true;
            } else if kind == livekit::data_packet::Kind::Lossy {
                if let Some(dc) = &inner.lossy_dc {
                    let _ = dc.close();
                }
                inner.lossy_dc = Some(Arc::new(DataChannelWriter::new_unreliable(
                    dc.clone(),
                    self.block_write(),
                    self.params.datachannel_lossy_target_latency,
                    LOSSY_DATA_CHANNEL_MIN_BUFFERED_AMOUNT,
                )));
                inner.lossy_dc_opened = true;
            }
        }

        if !is_handled {
            let _ = dc.close();
            return;
        }

        self.read_data_channel(
            dc,
            if is_unlabeled {
                DataChannelReader::Unlabeled
            } else if is_data_track {
                DataChannelReader::DataTrack
            } else {
                DataChannelReader::Kind(kind)
            },
        );

        self.maybe_notify_fully_established();
    }

    // `EnableDataChannelBlockWrite(true)` when `DatachannelSlowThreshold` is above zero: the
    // writers wait for the channel's buffered amount to come down (see the header)
    fn block_write(&self) -> Option<u64> {
        (self.params.datachannel_slow_threshold > 0).then_some(BLOCK_WRITE_BUFFERED_AMOUNT)
    }

    // The goroutine that reads a data channel upstream: the channel's message callback (see
    // the header).
    fn read_data_channel(&self, dc: &Arc<DataChannel>, reader: DataChannelReader) {
        let this = self.this.clone();
        let weak_dc = Arc::downgrade(dc);
        let label = dc.label().to_string();
        let stopped = atomic::Bool::new(false);
        dc.on_message(move |message| {
            let Some(t) = this.upgrade() else {
                return;
            };
            if stopped.load() {
                return;
            }
            if message.data.len() > DATA_CHANNEL_BUFFER_SIZE {
                // `io.ErrShortBuffer`, which ends the reading; the channel is closed on the way
                // out (`defer rawDC.Close()`)
                stopped.store(true);
                warnw!(t.params.logger, "error reading data channel", "short buffer", "label" => label);
                if let Some(dc) = weak_dc.upgrade() {
                    let _ = dc.close();
                }
                return;
            }

            match reader {
                DataChannelReader::Unlabeled => {
                    t.params.handler.on_data_message_unlabeled(&message.data);
                }

                DataChannelReader::DataTrack => {
                    t.params
                        .handler
                        .on_data_track_message(&message.data, mono::unix_nano());
                }

                DataChannelReader::Kind(kind) => {
                    t.params.handler.on_data_message(kind, &message.data);
                }
            }
        });
    }

    fn maybe_notify_fully_established(&self) {
        if self.is_fully_established() {
            self.params.handler.on_fully_established();

            let mut inner = self.lock.write();
            if !inner.has_fully_established_recorded {
                inner.has_fully_established_recorded = true;
                prometheus::record_peer_connection_state(
                    self.params.transport,
                    "fully_established",
                );
            }
        }
    }

    fn is_fully_established(&self) -> bool {
        let inner = self.lock.read();

        let data_channel_ready = self.params.use_one_shot_signalling_mode
            || inner.first_offer_no_data_channel
            || (inner.reliable_dc_opened && inner.lossy_dc_opened);

        data_channel_ready && !inner.peer_connection_lastconnected_at.is_zero()
    }

    pub fn set_prefer_tcp(&self, prefer_tcp: bool) {
        self.prefer_tcp.store(prefer_tcp);
    }

    pub fn add_ice_candidate(&self, candidate: IceCandidateInit) {
        self.post_event(
            Signal::RemoteIceCandidate,
            EventData::RemoteIceCandidate(candidate),
        );
    }

    fn queue_or_configure_sender(
        &self,
        transceiver: Arc<RtpTransceiver>,
        enabled_codecs: &[livekit::Codec],
        rtcp_feedback_config: &RtcpFeedbackConfig,
        enable_audio_stereo: bool,
        enable_audio_nack: bool,
    ) {
        let params = ConfigureSenderParams {
            transceiver,
            enabled_codecs: enabled_codecs.to_vec(),
            rtcp_feedback_config: rtcp_feedback_config.clone(),
            filter_out_h264_high_profile: !self.params.is_offerer,
            enable_audio_stereo,
            enable_audio_nack,
            enable_flex_fec: self.params.direction_config.flex_fec.enabled,
        };
        if !self.params.is_offerer {
            self.senders_pending_config_mu.lock().push(params);
            return;
        }

        // Offerer: no remote offer to echo payload types from.
        configure_sender(&params, None);
    }

    // processSendersPendingConfig configures the senders queued while answering the
    // remote's offer (single peer connection mode). offerAudioPT (mime type -> the
    // payload type the offer assigned) is parsed from the offer by the caller before
    // SetRemoteDescription, so the answer echoes the offered payload types for audio
    // codecs (and stays consistent with the forwarded RTP). It is nil when there is
    // nothing to echo.
    fn process_senders_pending_config(
        &self,
        offer_audio_pt: Option<&HashMap<mime::MimeType, PayloadType>>,
    ) {
        let pending = std::mem::take(&mut *self.senders_pending_config_mu.lock());

        let mut unprocessed = Vec::new();
        for p in pending {
            if p.transceiver.mid().is_empty() {
                unprocessed.push(p);
                continue;
            }

            configure_sender(&p, offer_audio_pt);
        }

        if !unprocessed.is_empty() {
            self.senders_pending_config_mu.lock().extend(unprocessed);
        }
    }

    pub fn add_track(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
        enabled_codecs: &[livekit::Codec],
        rtcp_feedback_config: &RtcpFeedbackConfig,
    ) -> Result<(Arc<RtpSender>, Arc<RtpTransceiver>), Error> {
        let (can_reuse, td) = {
            let mut inner = self.lock.write();
            let can_reuse = inner.can_reuse_transceiver;
            let td = inner.previous_track_description.remove(&track_local.id());
            (can_reuse, td)
        };

        // keep track use same mid after migration if possible
        if let Some(td) = &td
            && let Some(sender) = &td.sender
        {
            for tr in self.pc.get_transceivers() {
                if tr.mid() == td.mid {
                    // `tr.SetSender(td.sender, trackLocal)`: the sender is the transceiver's,
                    // and is given the track (see the header)
                    sender.replace_track(Some(track_local))?;
                    return Ok((sender.clone(), tr));
                }
            }
        }

        // if never negotiated with client, can't reuse transceiver for track not subscribed before migration
        if !can_reuse {
            return self.add_transceiver_from_track(
                track_local,
                params,
                enabled_codecs,
                rtcp_feedback_config,
            );
        }

        let sender = self.pc.add_track(track_local)?;

        let mut transceiver = None;
        for tr in self.pc.get_transceivers() {
            if tr.sender().is_some_and(|s| Arc::ptr_eq(&s, &sender)) {
                transceiver = Some(tr);
                break;
            }
        }

        let Some(transceiver) = transceiver else {
            return Err(Error::NoTransceiver);
        };

        self.queue_or_configure_sender(
            transceiver.clone(),
            enabled_codecs,
            rtcp_feedback_config,
            params.stereo,
            !params.red || !self.params.client_info.supports_audio_red(),
        );

        self.adjust_num_outstanding_media(&transceiver);
        Ok((sender, transceiver))
    }

    pub fn add_transceiver_from_track(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
        enabled_codecs: &[livekit::Codec],
        rtcp_feedback_config: &RtcpFeedbackConfig,
    ) -> Result<(Arc<RtpSender>, Arc<RtpTransceiver>), Error> {
        let transceiver = self.pc.add_transceiver_from_track(track_local, None)?;

        let Some(sender) = transceiver.sender() else {
            return Err(Error::NoSender);
        };

        self.queue_or_configure_sender(
            transceiver.clone(),
            enabled_codecs,
            rtcp_feedback_config,
            params.stereo,
            !params.red || !self.params.client_info.supports_audio_red(),
        );

        self.adjust_num_outstanding_media(&transceiver);
        Ok((sender, transceiver))
    }

    pub fn add_transceiver_from_kind(
        &self,
        kind: RtpCodecType,
        init: RtpTransceiverInit,
    ) -> Result<Arc<RtpTransceiver>, Error> {
        Ok(self.pc.add_transceiver_from_kind(kind, Some(init))?)
    }

    pub fn remove_track(&self, sender: &Arc<RtpSender>) -> Result<(), Error> {
        Ok(self.pc.remove_track(sender)?)
    }

    pub fn current_local_description(&self) -> Option<SessionDescription> {
        self.pc.current_local_description()
    }

    pub fn current_remote_description(&self) -> Option<SessionDescription> {
        self.pc.current_remote_description()
    }

    pub fn pending_remote_description(&self) -> Option<SessionDescription> {
        self.pc.pending_remote_description()
    }

    pub fn get_mid(&self, rtp_receiver: &RtpReceiver) -> String {
        if let Some(tr) = rtp_receiver.rtp_transceiver() {
            return tr.mid();
        }

        String::new()
    }

    pub fn get_rtp_transceiver(&self, mid: &str) -> Option<Arc<RtpTransceiver>> {
        self.pc
            .get_transceivers()
            .into_iter()
            .find(|tr| tr.mid() == mid)
    }

    pub fn get_rtp_receiver(&self, mid: &str) -> Option<Arc<RtpReceiver>> {
        for tr in self.pc.get_transceivers() {
            if tr.mid() == mid {
                return tr.receiver();
            }
        }

        None
    }

    fn get_num_unmatched_transceivers(&self) -> (u32, u32) {
        if self.is_closed.load() || self.pc.connection_state() == PeerConnectionState::Closed {
            return (0, 0);
        }

        let mut num_audios = 0u32;
        let mut num_videos = 0u32;
        for tr in self.pc.get_transceivers() {
            if !tr.mid().is_empty() {
                continue;
            }

            match tr.direction() {
                RtpTransceiverDirection::Sendonly | RtpTransceiverDirection::Sendrecv => {}
                _ => continue,
            }

            match tr.kind() {
                RtpCodecType::Audio => num_audios += 1,

                RtpCodecType::Video => num_videos += 1,

                _ => {}
            }
        }

        (num_audios, num_videos)
    }

    pub fn create_data_channel(
        &self,
        label: &str,
        dci: Option<DataChannelInit>,
    ) -> Result<(), Error> {
        if label == DATA_TRACK_DATA_CHANNEL && !self.params.enable_data_tracks {
            debugw!(self.params.logger, "data tracks not enabled");
            return Ok(());
        }

        let dc = self.pc.create_data_channel(label, dci)?;
        let slot;
        let mut is_data_track = false;
        let mut is_unlabeled = false;
        let mut kind = livekit::data_packet::Kind::Reliable;
        match dc.label() {
            RELIABLE_DATA_CHANNEL => {
                slot = Some(DataChannelSlot::Reliable);
                kind = livekit::data_packet::Kind::Reliable;
            }

            LOSSY_DATA_CHANNEL => {
                slot = Some(DataChannelSlot::Lossy);
                kind = livekit::data_packet::Kind::Lossy;
            }

            DATA_TRACK_DATA_CHANNEL => {
                slot = Some(DataChannelSlot::DataTrack);
                is_data_track = true;
            }

            _ => {
                slot = None;
                is_unlabeled = true;
                infow!(self.params.logger, "unlabeled datachannel added", "label" => dc.label());
            }
        }

        let this = self.this.clone();
        let weak_dc = Arc::downgrade(&dc);
        dc.on_open(move || {
            let (Some(t), Some(dc)) = (this.upgrade(), weak_dc.upgrade()) else {
                return;
            };
            // `rawDC, err := dc.DetachWithDeadline()`: the writers take the channel itself

            let mut slow_threshold = 0;
            if dc.label() == RELIABLE_DATA_CHANNEL || is_unlabeled {
                slow_threshold = t.params.datachannel_slow_threshold;
            }

            {
                let mut guard = t.lock.write();
                let inner = &mut *guard;
                match slot {
                    None => {
                        inner.unlabeled_data_channels.push(Arc::new(
                            DataChannelWriter::new_reliable(
                                dc.clone(),
                                t.block_write(),
                                slow_threshold,
                            ),
                        ));
                    }
                    Some(slot) => {
                        let (dc_ptr, dc_ready) = match slot {
                            DataChannelSlot::Reliable => {
                                (&mut inner.reliable_dc, Some(&mut inner.reliable_dc_opened))
                            }
                            DataChannelSlot::Lossy => {
                                (&mut inner.lossy_dc, Some(&mut inner.lossy_dc_opened))
                            }
                            DataChannelSlot::DataTrack => (&mut inner.data_track_dc, None),
                        };
                        if let Some(dc) = dc_ptr {
                            let _ = dc.close();
                        }
                        *dc_ptr = Some(Arc::new(match slot {
                            DataChannelSlot::Reliable => DataChannelWriter::new_reliable(
                                dc.clone(),
                                t.block_write(),
                                slow_threshold,
                            ),
                            DataChannelSlot::Lossy => DataChannelWriter::new_unreliable(
                                dc.clone(),
                                t.block_write(),
                                t.params.datachannel_lossy_target_latency,
                                LOSSY_DATA_CHANNEL_MIN_BUFFERED_AMOUNT,
                            ),
                            DataChannelSlot::DataTrack => DataChannelWriter::new_unreliable(
                                dc.clone(),
                                t.block_write(),
                                t.params.datachannel_data_track_target_latency,
                                LOSSY_DATA_CHANNEL_MIN_BUFFERED_AMOUNT,
                            ),
                        }));
                        if let Some(dc_ready) = dc_ready {
                            *dc_ready = true;
                        }
                    }
                }
            }
            debugw!(
                t.params.logger,
                &format!("{} data channel open", dc.label())
            );

            t.read_data_channel(
                &dc,
                if is_unlabeled {
                    DataChannelReader::Unlabeled
                } else if is_data_track {
                    DataChannelReader::DataTrack
                } else {
                    DataChannelReader::Kind(kind)
                },
            );

            t.maybe_notify_fully_established();
        });

        Ok(())
    }

    // for testing only
    pub fn create_readable_data_channel(
        &self,
        label: &str,
        dci: Option<DataChannelInit>,
    ) -> Result<(), Error> {
        let dc = self.pc.create_data_channel(label, dci)?;

        let this = self.this.clone();
        let weak_dc = Arc::downgrade(&dc);
        dc.on_open(move || {
            let (Some(t), Some(dc)) = (this.upgrade(), weak_dc.upgrade()) else {
                return;
            };
            debugw!(
                t.params.logger,
                &format!("{} data channel open", dc.label())
            );
            // `rawDC, err := dc.DetachWithDeadline()`: the writer takes the channel itself

            t.lock
                .write()
                .unlabeled_data_channels
                .push(Arc::new(DataChannelWriter::new_reliable(
                    dc.clone(),
                    t.block_write(),
                    t.params.datachannel_slow_threshold,
                )));

            t.read_data_channel(&dc, DataChannelReader::Unlabeled);
        });
        Ok(())
    }

    pub fn create_data_channel_if_empty(
        &self,
        dc_label: &str,
        dci: Option<DataChannelInit>,
    ) -> Result<(String, u16, bool), Error> {
        // `label`, the first result: empty until the end, which is what the warning below
        // logs, as the original's does
        let label = String::new();
        if dc_label == DATA_TRACK_DATA_CHANNEL && !self.params.enable_data_tracks {
            debugw!(self.params.logger, "data tracks not enabled");
            return Err(Error::DataTracksNotEnabled);
        }

        let dcw = {
            let inner = self.lock.read();
            match dc_label {
                RELIABLE_DATA_CHANNEL => inner.reliable_dc.clone(),
                LOSSY_DATA_CHANNEL => inner.lossy_dc.clone(),
                DATA_TRACK_DATA_CHANNEL => inner.data_track_dc.clone(),
                _ => {
                    warnw!(self.params.logger, "unknown data channel label", nil, "label" => label);
                    return Err(Error::UnknownDataChannelLabel);
                }
            }
        };

        if let Some(dcw) = dcw {
            let dc = dcw.buffered_amount_getter();
            return Ok((dc.label().to_string(), dc.id().unwrap_or_default(), true));
        }

        let dc = self.pc.create_data_channel(dc_label, dci)?;

        self.on_data_channel(dc.clone());
        Ok((dc.label().to_string(), dc.id().unwrap_or_default(), false))
    }

    pub fn get_rtt(&self) -> (f64, bool) {
        // `iceTransport.GetSelectedCandidatePairStats()`: the numbers of the selected pair are
        // in the connection's statistics
        let stats = self.pc.get_stats();
        let Some(transport) = stats.transport() else {
            return (0.0, false);
        };
        match stats.get(&transport.selected_candidate_pair_id) {
            Some(StatsReportEntry::IceCandidatePair(scps)) => (scps.current_round_trip_time, true),
            _ => (0.0, false),
        }
    }

    pub fn is_established(&self) -> bool {
        self.pc.connection_state() != PeerConnectionState::New
    }

    pub fn ice_has_ever_connected(&self) -> bool {
        !self.lock.read().ice_first_connected_at.is_zero()
    }

    pub fn peer_connection_has_ever_connected(&self) -> bool {
        !self
            .lock
            .read()
            .peer_connection_first_connected_at
            .is_zero()
    }

    pub fn peer_connection_first_connected_at(&self) -> Time {
        self.lock.read().peer_connection_first_connected_at
    }

    pub fn get_ice_connection_info(&self) -> types::IceConnectionInfo {
        self.connection_details.get_info()
    }

    pub fn get_ice_connection_type(&self) -> types::IceConnectionType {
        self.connection_details.get_connection_type()
    }

    pub fn write_rtcp(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), Error> {
        Ok(self.pc.write_rtcp(pkts)?)
    }

    pub fn send_data_message(
        &self,
        kind: livekit::data_packet::Kind,
        data: &[u8],
    ) -> Result<(), Error> {
        let mut convert_from_user_packet = false;
        let dc = {
            let inner = self.lock.read();
            if self.params.use_one_shot_signalling_mode {
                convert_from_user_packet = true;
                // use the first unlabeled to send
                inner.unlabeled_data_channels.first().cloned()
            } else if kind == livekit::data_packet::Kind::Reliable {
                inner.reliable_dc.clone()
            } else {
                inner.lossy_dc.clone()
            }
        };

        if convert_from_user_packet {
            use musivum_protocol::Message;
            let dp = livekit::DataPacket::decode(data)
                .map_err(|err| Error::ProtoUnmarshal(err.to_string()))?;

            return match &dp.value {
                Some(livekit::data_packet::Value::User(user)) => {
                    self.send_data_message_to(dc.as_deref(), &user.payload)
                }
                _ => Err(Error::CannotForwardNonUserDataPacket),
            };
        }

        self.send_data_message_to(dc.as_deref(), data)
    }

    pub fn send_data_message_unlabeled(
        &self,
        data: &[u8],
        use_raw: bool,
        sender: &livekit::ParticipantIdentity,
    ) -> Result<(), Error> {
        let mut convert_to_user_packet = false;
        let dc = {
            let inner = self.lock.read();
            if self.params.use_one_shot_signalling_mode || use_raw {
                // use the first unlabeled to send
                inner.unlabeled_data_channels.first().cloned()
            } else {
                convert_to_user_packet = true;
                if inner.reliable_dc.is_some() {
                    inner.reliable_dc.clone()
                } else {
                    inner.lossy_dc.clone()
                }
            }
        };

        if convert_to_user_packet {
            use musivum_protocol::Message;
            let dp_data = livekit::DataPacket {
                participant_identity: sender.to_string(),
                value: Some(livekit::data_packet::Value::User(livekit::UserPacket {
                    payload: data.to_vec(),
                    ..Default::default()
                })),
                ..Default::default()
            }
            .encode_to_vec();
            return self.send_data_message_to(dc.as_deref(), &dp_data);
        }

        self.send_data_message_to(dc.as_deref(), data)
    }

    pub fn send_data_track_message(&self, data: &[u8]) -> Result<(), Error> {
        let dc = self.lock.read().data_track_dc.clone();

        self.send_data_message_to(dc.as_deref(), data)
    }

    // `sendDataMessage`
    fn send_data_message_to(
        &self,
        dc: Option<&DataChannelWriter<DataChannel>>,
        data: &[u8],
    ) -> Result<(), Error> {
        let Some(dc) = dc else {
            return Err(Error::DataChannelUnavailable);
        };

        if self.pc.connection_state() == PeerConnectionState::Failed {
            return Err(Error::TransportFailure);
        }

        if self.params.datachannel_slow_threshold == 0
            && self.params.data_channel_max_buffered_amount > 0
            && dc.buffered_amount_getter().buffered_amount()
                > self.params.data_channel_max_buffered_amount
        {
            return Err(Error::DataChannelBufferFull);
        }
        dc.write(data)?;
        Ok(())
    }

    pub fn close(&self) {
        if self.is_closed.swap(true) {
            return;
        }

        if let Err(err) = self.pc.close() {
            warnw!(self.params.logger, "unclean close of peer connection", err);
        }
        // the buffers of the connection, after the connection (sfu/interceptor/sink.rs)
        if let Some(sink) = &self.sink {
            sink.close();
        }

        self.events_queue.stop().wait();
        self.clear_signal_state_check_timer();

        if let Some(stream_allocator) = &self.stream_allocator {
            stream_allocator.stop();
        }

        if let Some(pacer) = &self.pacer {
            pacer.stop();
        }

        self.clear_conn_timer();

        {
            let mut inner = self.lock.write();
            if let Some(timer) = inner.may_failed_ice_stats_timer.take() {
                timer.stop();
            }

            if let Some(dc) = inner.reliable_dc.take() {
                let _ = dc.close();
            }

            if let Some(dc) = inner.lossy_dc.take() {
                let _ = dc.close();
            }

            if let Some(dc) = inner.data_track_dc.take() {
                let _ = dc.close();
            }

            for dc in &inner.unlabeled_data_channels {
                let _ = dc.close();
            }
            inner.unlabeled_data_channels = Vec::new();
        }

        self.output_and_clear_ice_stats();
    }

    fn clear_conn_timer(&self) {
        let mut inner = self.lock.write();

        if let Some(timer) = inner.connect_after_ice_timer.take() {
            timer.stop();
        }

        if let Some(timer) = inner.tcp_ice_timer.take() {
            timer.stop();
        }
    }

    pub fn handle_remote_description(
        &self,
        sd: SessionDescription,
        remote_id: u32,
    ) -> Result<(), Error> {
        let parsed = sd.unmarshal();
        if self.params.use_one_shot_signalling_mode {
            if sd.r#type == SdpType::Offer {
                let remote_offer_id = self.remote_offer_id.load();
                if remote_offer_id != 0 && remote_offer_id != self.local_answer_id.load() {
                    warnw!(
                        self.params.logger,
                        "sdp state: multiple offers without answer", nil,
                        "remoteOfferId" => remote_offer_id,
                        "localAnswerId" => self.local_answer_id.load(),
                        "receivedRemoteOfferId" => remote_id,
                    );
                }
                self.remote_offer_id.store(remote_id);
            } else {
                if remote_id != 0 && remote_id != self.local_offer_id.load() {
                    warnw!(self.params.logger, "sdp state: answer id mismatch", nil, "expected" => self.local_offer_id.load(), "got" => remote_id);
                }
                self.remote_answer_id.store(remote_id);
            }

            // add remote candidates to ICE connection details
            if let Ok(parsed) = &parsed {
                let add_remote_ice_candidates = |attrs: &[sdp::Attribute]| {
                    for a in attrs {
                        if a.key == sdp::ATTR_KEY_CANDIDATE {
                            let Ok(c) = ice::unmarshal_candidate(a.value.as_deref().unwrap_or(""))
                            else {
                                continue;
                            };
                            self.connection_details.add_remote_ice_candidate(
                                Some(&c),
                                false,
                                false,
                                false,
                            );
                        }
                    }
                };

                add_remote_ice_candidates(&parsed.attributes);
                for m in &parsed.media_descriptions {
                    add_remote_ice_candidates(&m.attributes);
                }
            }

            if let Err(err) = self.pc.set_remote_description(sd.clone()) {
                errorw!(
                    self.params.logger,
                    "could not set remote description on synchronous mode peer connection",
                    err
                );
                return Err(err.into());
            }
            self.set_remote_offer_parsed(sd.r#type, parsed.as_ref().ok().cloned().map(Arc::new));
            if sd.r#type == SdpType::Offer {
                self.params.handler.on_set_remote_description_offer();
            }

            // the original reads a description that does not parse as nil here; `rtc` has
            // refused it above
            let Ok(parsed) = &parsed else {
                return Ok(());
            };
            let rtx_repairs = non_simulcast_rtx_repairs_from_sdp(parsed, &self.params.logger);
            if !rtx_repairs.is_empty() {
                debugw!(self.params.logger, "rtx pairs found from sdp", "ssrcs" => rtx_repairs);
                if let Some(buffer_factory) = &self.params.config.buffer_factory {
                    for (repair, base) in &rtx_repairs {
                        buffer_factory.set_rtx_pair(*repair, *base, "");
                    }
                }
            }

            if self.params.direction_config.flex_fec.enabled {
                let fec_flows = fec_pairs_from_sdp(parsed, &self.params.logger);
                if !fec_flows.is_empty() {
                    debugw!(self.params.logger, "fec pairs found from sdp", "ssrcs" => fec_flows);
                    if let Some(buffer_factory) = &self.params.config.buffer_factory {
                        for (fec, base) in &fec_flows {
                            buffer_factory.set_fec_pair(*fec, *base);
                        }
                    }
                }
            }
            return Ok(());
        }

        self.post_event(
            Signal::RemoteDescriptionReceived,
            EventData::RemoteDescription(Box::new(RemoteDescriptionData {
                session_description: sd,
                parsed,
                remote_id,
            })),
        );
        Ok(())
    }

    pub fn get_answer(&self) -> Result<(SessionDescription, u32), Error> {
        if !self.params.use_one_shot_signalling_mode {
            return Err(Error::NotSynchronousLocalCandidatesMode);
        }

        let prd = self.pc.pending_remote_description();
        if prd.is_none_or(|prd| prd.r#type != SdpType::Offer) {
            return Err(Error::NoRemoteDescription);
        }

        let answer = self.pc.create_answer()?;

        self.pc.set_local_description(answer)?;

        // wait for gathering to complete to include all candidates in the answer
        // (`<-webrtc.GatheringCompletePromise(t.pc)`: it is, see the header)

        let cld = self
            .pc
            .current_local_description()
            .ok_or(Error::NoLocalDescription)?;

        let prefer_tcp = self.prefer_tcp.load();
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local answer (unfiltered)", "sdp" => cld.sdp);
        }

        //
        // Filter after setting local description as pion expects the answer
        // to match between CreateAnswer and SetLocalDescription.
        // Filtered answer is sent to remote so that remote does not
        // see filtered candidates.
        //
        let filtered_answer = self.filter_candidates(cld, prefer_tcp, true);
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local answer (filtered)", "sdp" => filtered_answer.sdp);
        }

        let answer_id = self.remote_offer_id.load();
        self.local_answer_id.store(answer_id);

        Ok((filtered_answer, answer_id))
    }

    pub fn get_ice_session_ufrag(&self) -> Result<String, Error> {
        let cld = self
            .pc
            .current_local_description()
            .ok_or(Error::NoLocalDescription)?;

        let parsed = cld.unmarshal()?;

        let (ufrag, _) = lksdp::extract_ice_credential(&parsed)?;

        Ok(ufrag)
    }

    // Handles SDP Fragment for ICE Trickle in WHIP
    pub fn handle_ice_trickle_sdp_fragment(&self, sdp_fragment: &str) -> Result<(), Error> {
        if !self.params.use_one_shot_signalling_mode {
            return Err(Error::NotSynchronousLocalCandidatesMode);
        }

        let mut parsed_fragment = lksdp::SdpFragment::default();
        if let Err(err) = parsed_fragment.unmarshal(sdp_fragment) {
            warnw!(self.params.logger, "could not parse SDP fragment", err, "sdpFragment" => sdp_fragment);
            return Err(Error::InvalidSdpFragment);
        }

        let Some(crd) = self.pc.current_remote_description() else {
            warnw!(self.params.logger, "no remote description", nil);
            return Err(Error::NoRemoteDescription);
        };

        let parsed_remote = match crd.unmarshal() {
            Ok(parsed_remote) => parsed_remote,
            Err(err) => {
                warnw!(self.params.logger, "could not parse remote description", err, "offer" => crd);
                return Err(err.into());
            }
        };

        // check if BUNDLE mid matches the "mid" in the SDP fragment
        let Some(bundle_mid) = lksdp::get_bundle_mid(&parsed_remote) else {
            return Err(Error::NoBundleMid);
        };

        if parsed_fragment.mid() != bundle_mid {
            warnw!(self.params.logger, "incorrect mid", nil, "sdpFragment" => sdp_fragment);
            return Err(Error::MidMismatch);
        }

        let (fragment_ice_ufrag, fragment_ice_pwd) = match parsed_fragment.extract_ice_credential()
        {
            Ok(credential) => credential,
            Err(err) => {
                warnw!(
                    self.params.logger,
                    "could not get ICE credential from fragment", err,
                    "sdpFragment" => sdp_fragment,
                );
                return Err(Error::InvalidSdpFragment);
            }
        };
        let (remote_ice_ufrag, remote_ice_pwd) = match lksdp::extract_ice_credential(&parsed_remote)
        {
            Ok(credential) => credential,
            Err(err) => {
                warnw!(self.params.logger, "could not get ICE credential from remote description", err, "sdpFragment" => sdp_fragment, "remoteDescription" => crd);
                return Err(err.into());
            }
        };
        if !fragment_ice_ufrag.is_empty() && fragment_ice_ufrag != remote_ice_ufrag {
            warnw!(
                self.params.logger,
                "ice ufrag mismatch", nil,
                "remoteICEUfrag" => remote_ice_ufrag,
                "fragmentICEUfrag" => fragment_ice_ufrag,
                "sdpFragment" => sdp_fragment,
                "remoteDescription" => crd,
            );
            return Err(Error::IceCredentialMismatch);
        }
        if !fragment_ice_pwd.is_empty() && fragment_ice_pwd != remote_ice_pwd {
            warnw!(
                self.params.logger,
                "ice pwd mismatch", nil,
                "remoteICEPwd" => remote_ice_pwd,
                "fragmentICEPwd" => fragment_ice_pwd,
                "sdpFragment" => sdp_fragment,
                "remoteDescription" => crd,
            );
            return Err(Error::IceCredentialMismatch);
        }

        // add candidates from media description
        for ic in parsed_fragment.candidates() {
            if let Ok(c) = ice::unmarshal_candidate(ic) {
                self.connection_details
                    .add_remote_ice_candidate(Some(&c), false, false, false);
            }

            let candidate = IceCandidateInit {
                candidate: ic.clone(),
                ..Default::default()
            };
            if let Err(err) = self.pc.add_ice_candidate(candidate.clone()) {
                warnw!(self.params.logger, "failed to add ICE candidate", err, "candidate" => candidate);
            } else {
                debugw!(self.params.logger, "added ICE candidate", "candidate" => candidate);
            }
        }
        Ok(())
    }

    // Handles SDP Fragment for ICE Restart in WHIP
    pub fn handle_ice_restart_sdp_fragment(&self, sdp_fragment: &str) -> Result<String, Error> {
        if !self.params.use_one_shot_signalling_mode {
            return Err(Error::NotSynchronousLocalCandidatesMode);
        }

        let mut parsed_fragment = lksdp::SdpFragment::default();
        if let Err(err) = parsed_fragment.unmarshal(sdp_fragment) {
            warnw!(self.params.logger, "could not parse SDP fragment", err, "sdpFragment" => sdp_fragment);
            return Err(Error::InvalidSdpFragment);
        }

        let Some(crd) = self.pc.current_remote_description() else {
            warnw!(self.params.logger, "no remote description", nil);
            return Err(Error::NoRemoteDescription);
        };

        let mut parsed_remote = match crd.unmarshal() {
            Ok(parsed_remote) => parsed_remote,
            Err(err) => {
                warnw!(self.params.logger, "could not parse remote description", err, "offer" => crd);
                return Err(err.into());
            }
        };

        if let Err(err) =
            parsed_fragment.patch_ice_credential_and_candidates_into_sdp(&mut parsed_remote)
        {
            warnw!(self.params.logger, "could not patch SDP fragment into remote description", err, "offer" => crd, "sdpFragment" => sdp_fragment);
            return Err(err.into());
        }

        // `parsedRemote.Marshal()`, which cannot fail here
        let bytes = parsed_remote.marshal();
        let sd = SessionDescription {
            sdp: bytes,
            r#type: SdpType::Offer,
        };
        if let Err(err) = self.pc.set_remote_description(sd.clone()) {
            warnw!(self.params.logger, "could not set remote description", err);
            return Err(err.into());
        }
        self.set_remote_offer_parsed(sd.r#type, Some(Arc::new(parsed_remote)));

        // clear out connection details on ICE restart and re-populate
        self.connection_details.clear();
        for candidate in parsed_fragment.candidates() {
            let Ok(c) = ice::unmarshal_candidate(candidate) else {
                continue;
            };
            self.connection_details
                .add_remote_ice_candidate(Some(&c), false, false, false);
        }

        let answer = match self.pc.create_answer() {
            Ok(answer) => answer,
            Err(err) => {
                warnw!(self.params.logger, "could not create answer", err);
                return Err(err.into());
            }
        };

        if let Err(err) = self.pc.set_local_description(answer) {
            warnw!(self.params.logger, "could not set local description", err);
            return Err(err.into());
        }

        // wait for gathering to complete to include all candidates in the answer
        // (`<-webrtc.GatheringCompletePromise(t.pc)`: it is, see the header)

        let cld = self
            .pc
            .current_local_description()
            .ok_or(Error::NoLocalDescription)?;

        let prefer_tcp = self.prefer_tcp.load();
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local answer (unfiltered)", "sdp" => cld.sdp);
        }

        //
        // Filter after setting local description as pion expects the answer
        // to match between CreateAnswer and SetLocalDescription.
        // Filtered answer is sent to remote so that remote does not
        // see filtered candidates.
        //
        let filtered_answer = self.filter_candidates(cld, prefer_tcp, true);
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local answer (filtered)", "sdp" => filtered_answer.sdp);
        }

        // add local candidates to ICE connection details
        let parsed_filtered_answer = match filtered_answer.unmarshal() {
            Ok(parsed) => parsed,
            Err(err) => {
                warnw!(self.params.logger, "could not parse local description", err);
                return Err(err.into());
            }
        };

        let parsed_fragment_answer = match lksdp::extract_sdp_fragment(&parsed_filtered_answer) {
            Ok(fragment) => fragment,
            Err(err) => {
                warnw!(self.params.logger, "could not extract SDP fragment", err);
                return Err(err.into());
            }
        };

        let answer_fragment = match parsed_fragment_answer.marshal() {
            Ok(fragment) => fragment,
            Err(err) => {
                warnw!(
                    self.params.logger,
                    "could not marshal answer SDP fragment",
                    err
                );
                return Err(err.into());
            }
        };

        Ok(answer_fragment)
    }

    pub fn on_negotiation_state_changed(
        &self,
        f: Option<Box<dyn Fn(NegotiationState) + Send + Sync>>,
    ) {
        self.lock.write().on_negotiation_state_changed = f.map(Arc::from);
    }

    fn get_on_negotiation_state_changed(
        &self,
    ) -> Option<Arc<dyn Fn(NegotiationState) + Send + Sync>> {
        self.lock.read().on_negotiation_state_changed.clone()
    }

    pub fn negotiate(&self, force: bool) {
        if self.is_closed.load() {
            return;
        }

        let mut post_event = false;
        {
            let mut inner = self.lock.write();
            if force {
                self.debounced_negotiate.add(|| {
                    // no op to cancel pending negotiation
                });
                inner.debounce_pending = false;
                inner.update_last_negotiate_locked();

                post_event = true;
            } else if !inner.debounce_pending {
                if time::since(inner.last_negotiate) > NEGOTIATION_FREQUENCY {
                    self.debounced_negotiate
                        .set_duration(FAST_NEGOTIATION_FREQUENCY);
                } else {
                    self.debounced_negotiate.set_duration(NEGOTIATION_FREQUENCY);
                }

                let this = self.this.clone();
                self.debounced_negotiate.add(move || {
                    let Some(t) = this.upgrade() else {
                        return;
                    };
                    {
                        let mut inner = t.lock.write();
                        inner.debounce_pending = false;
                        inner.update_last_negotiate_locked();
                    }

                    t.post_event(Signal::SendOffer, EventData::None);
                });
                inner.debounce_pending = true;
            }
        }

        if post_event {
            self.post_event(Signal::SendOffer, EventData::None);
        }
    }

    pub fn ice_restart(&self) -> Result<(), Error> {
        if self.pc.connection_state() == PeerConnectionState::Closed {
            warnw!(
                self.params.logger,
                "trying to restart ICE on closed peer connection",
                nil
            );
            return Err(Error::IceRestartOnClosedPeerConnection);
        }

        self.post_event(Signal::IceRestart, EventData::None);
        Ok(())
    }

    pub fn reset_short_conn_on_ice_restart(&self) {
        self.reset_short_conn_on_ice_restart.store(true);
    }

    pub fn add_track_to_stream_allocator(&self, sub_track: &dyn types::SubscribedTrack) {
        let Some(stream_allocator) = &self.stream_allocator else {
            return;
        };

        // the original calls methods of both without looking: nil only from a fake that was
        // given neither
        let (Some(down_track), Some(media_track)) =
            (sub_track.down_track(), sub_track.media_track())
        else {
            return;
        };
        let ti = media_track.to_proto();
        let layers = buffer::get_video_layers_for_mime_type(down_track.mime(), Some(&ti));
        stream_allocator.add_track(
            down_track,
            streamallocator::AddTrackParams {
                source: media_track.source(),
                is_multi_layered: layers.len() > 1,
                publisher_id: media_track.publisher_id(),
                ..Default::default()
            },
        );
    }

    pub fn remove_track_from_stream_allocator(&self, sub_track: &dyn types::SubscribedTrack) {
        let Some(stream_allocator) = &self.stream_allocator else {
            return;
        };

        if let Some(down_track) = sub_track.down_track() {
            stream_allocator.remove_track(&down_track);
        }
    }

    pub fn set_allow_pause_of_stream_allocator(&self, allow_pause: bool) {
        let Some(stream_allocator) = &self.stream_allocator else {
            return;
        };

        stream_allocator.set_allow_pause(allow_pause);
    }

    pub fn set_channel_capacity_of_stream_allocator(&self, channel_capacity: i64) {
        let Some(stream_allocator) = &self.stream_allocator else {
            return;
        };

        stream_allocator.set_channel_capacity(channel_capacity);
    }

    fn prepare_pc(&self, previous_answer: &SessionDescription) -> Result<(), Error> {
        // sticky data channel to first m-lines, if someday we don't send sdp without media streams to
        // client's subscribe pc after joining, should change this step
        let parsed = previous_answer.unmarshal()?;
        let (fp, fp_hahs) = lksdp::extract_fingerprint(&parsed)?;

        let offer = self.pc.create_offer(None)?;
        self.pc.set_local_description(offer.clone())?;

        //
        // Simulate client side peer connection and set DTLS role from previous answer.
        // Role needs to be set properly (one side needs to be server and the other side
        // needs to be the client) for DTLS connection to form properly. As this is
        // trying to replicate previous setup, read from previous answer and use that role.
        //
        let mut se = SettingEngine::default();
        let _ = se.set_answering_dtls_role(lksdp::extract_dtls_role(&parsed));
        se.ignore_rid_pause_for_recv = true;
        let pc2 = PeerConnection::new(
            &self.params.node,
            Configuration {
                media_engine: self.me.clone(),
                setting_engine: se,
                logger: self.params.logger.clone(),
                ..Default::default()
            },
        )?;
        // `defer pc2.Close()`
        let ans = (|| -> Result<SessionDescription, Error> {
            pc2.set_remote_description(offer)?;
            let mut ans = pc2.create_answer()?;

            // replace client's fingerprint into dummy pc's answer, for pion's dtls process, it will
            // keep the fingerprint at first call of SetRemoteDescription, if dummy pc and client pc use
            // different fingerprint, that will cause pion denied dtls data after handshake with client
            // complete (can't pass fingerprint change).
            // in this step, we don't established connection with dummy pc(no candidate swap), just use
            // sdp negotiation to sticky data channel and keep client's fingerprint
            // (the original ignores an error of parsing here, and then fails on nil)
            let mut parsed_ans = ans.unmarshal()?;
            let fp_line = format!("{fp_hahs} {fp}");
            let replace_fp = |attrs: &mut Vec<sdp::Attribute>, fp_line: &str| {
                for attr in attrs.iter_mut() {
                    if attr.key == "fingerprint" {
                        attr.value = Some(fp_line.to_string());
                    }
                }
            };
            replace_fp(&mut parsed_ans.attributes, &fp_line);
            for m in &mut parsed_ans.media_descriptions {
                replace_fp(&mut m.attributes, &fp_line);
            }
            ans.sdp = parsed_ans.marshal();
            Ok(ans)
        })();
        let _ = pc2.close();
        let ans = ans?;

        Ok(self.pc.set_remote_description(ans)?)
    }

    fn init_pc_with_previous_answer(
        &self,
        previous_answer: &SessionDescription,
    ) -> Result<HashMap<String, Arc<RtpSender>>, Error> {
        let senders: HashMap<String, Arc<RtpSender>> = HashMap::new();
        let parsed = previous_answer.unmarshal()?;
        for m in &parsed.media_descriptions {
            let codec_type = match m.media_name.media.as_str() {
                "video" => RtpCodecType::Video,
                "audio" => RtpCodecType::Audio,
                "application" => {
                    if self.params.is_offerer {
                        // for pion generate unmatched sdp, it always appends data channel to last m-lines,
                        // that not consistent with our previous answer that data channel might at middle-line
                        // because sdp can negotiate multi times before migration.(it will sticky to the last m-line at first negotiate)
                        // so use a dummy pc to negotiate sdp to fixed the datachannel's mid at same position with previous answer
                        if let Err(err) = self.prepare_pc(previous_answer) {
                            warnw!(self.params.logger, "prepare pc for migration failed", err);
                            return Err(err);
                        }
                    }
                    continue;
                }
                _ => continue,
            };

            if !self.params.is_offerer {
                // `sendrecv` or `sendonly` means this transceiver is used for sending

                // Note that a transceiver previously used to send could be `inactive`.
                // Let those transceivers be created when remote description is set.
                let ok1 = sdp::attribute(&m.attributes, sdp::ATTR_KEY_SEND_RECV).is_some();
                let ok2 = sdp::attribute(&m.attributes, sdp::ATTR_KEY_SEND_ONLY).is_some();
                if !ok1 && !ok2 {
                    continue;
                }
            }

            self.pc.add_transceiver_from_kind(
                codec_type,
                Some(RtpTransceiverInit {
                    direction: RtpTransceiverDirection::Sendonly,
                }),
            )?;
            let mid = lksdp::get_mid_value(m);
            if mid.is_empty() {
                warnw!(self.params.logger, "cannot set up peer connection with previous answer, mid not found", nil, "senders" => senders.keys().collect::<Vec<_>>());
                return Err(Error::MidNotFound);
            }
            // `tr.SetMid(mid)`, then the sender is kept for migration reuse
            // (`senders[mid] = tr.Sender()`) and the transceiver made inactive
            // (`tr.SetSender(sender, nil)`): the layer has neither, see the header
            return Err(webrtc::Error::Other(
                "RTPTransceiver.SetMid is not in the connection layer".to_string(),
            )
            .into());
        }
        debugw!(self.params.logger, "set up peer connection with previous answer", "senders" => senders.keys().collect::<Vec<_>>());
        Ok(senders)
    }

    pub fn set_previous_sdp(
        &self,
        local_description: Option<&SessionDescription>,
        remote_description: Option<&SessionDescription>,
    ) {
        // when there is no answer, cannot migrate, force a full reconnect
        if (self.params.is_offerer && remote_description.is_none())
            || (!self.params.is_offerer && local_description.is_none())
        {
            self.on_negotiation_failed(true, "no previous answer");
            return;
        }

        let mut inner = self.lock.write();
        let mut senders = HashMap::new();
        let mut res = Ok(());
        let mut parse_mids = false;
        if self.params.is_offerer {
            if self.pc.remote_description().is_none() && inner.previous_answer.is_none() {
                inner.previous_answer = remote_description.cloned();
                if let Some(remote_description) = remote_description {
                    match self.init_pc_with_previous_answer(remote_description) {
                        Ok(s) => senders = s,
                        Err(err) => res = Err(err),
                    }
                }
                parse_mids = true;
            }
        } else if self.pc.local_description().is_none() {
            if let Some(local_description) = local_description {
                match self.init_pc_with_previous_answer(local_description) {
                    Ok(s) => senders = s,
                    Err(err) => res = Err(err),
                }
            }
            parse_mids = true;
        }
        if let Err(err) = res {
            drop(inner);
            self.on_negotiation_failed(
                true,
                &format!("initPCWithPreviousAnswer failed, error: {err}"),
            );
            return;
        }

        if let Some(local_description) = local_description
            && parse_mids
        {
            // in migration case, can't reuse transceiver before negotiating expected tracks
            // that were subscribed at previous node
            inner.can_reuse_transceiver = false;
            if let Err(err) = self.parse_track_mid(&mut inner, local_description, &senders) {
                warnw!(
                    self.params.logger,
                    "parse previous local description failed", err,
                    "localDescription" => local_description.sdp,
                );
            }
        }

        if self.params.is_offerer {
            // disable fast negotiation temporarily after migration to avoid sending offer
            // contains part of subscribed tracks before migration, let the subscribed track
            // resume at the same time.
            inner.last_negotiate = time::now().add(ICE_FAILED_TIMEOUT_TOTAL);
        }
    }

    fn set_remote_offer_parsed(
        &self,
        sd_type: SdpType,
        parsed: Option<Arc<sdp::SessionDescription>>,
    ) {
        if sd_type != SdpType::Offer {
            return;
        }
        self.lock.write().remote_offer_parsed = parsed;
    }

    // RemoteOfferParsed returns the parsed form of the last remote offer set on the peer connection,
    // candidates filtered the same way pion saw them
    pub fn remote_offer_parsed(&self) -> Option<Arc<sdp::SessionDescription>> {
        self.lock.read().remote_offer_parsed.clone()
    }

    fn parse_track_mid(
        &self,
        inner: &mut PcTransportInner,
        sd: &SessionDescription,
        senders: &HashMap<String, Arc<RtpSender>>,
    ) -> Result<(), Error> {
        let parsed = sd.unmarshal()?;

        inner.previous_track_description = HashMap::new();
        for m in &parsed.media_descriptions {
            let Some(msid) = sdp::attribute(&m.attributes, sdp::ATTR_KEY_MSID) else {
                continue;
            };

            let split: Vec<&str> = msid.split(' ').collect();
            if split.len() == 2 {
                let track_id = split[1];
                let mid = lksdp::get_mid_value(m);
                if mid.is_empty() {
                    return Err(Error::MidNotFound);
                }
                if let Some(sender) = senders.get(&mid) {
                    inner.previous_track_description.insert(
                        track_id.to_string(),
                        TrackDescription {
                            mid,
                            sender: Some(sender.clone()),
                        },
                    );
                }
            }
        }
        if !inner.previous_track_description.is_empty() {
            debugw!(self.params.logger, "previous track description", "previousTrackDescription" => inner.previous_track_description);
        }
        Ok(())
    }

    fn post_event(&self, signal: Signal, data: EventData) {
        let e = Event {
            pc_transport: self.this.clone(),
            signal,
            data,
        };
        self.events_queue.enqueue(
            |e: Event| {
                let Some(t) = e.pc_transport.upgrade() else {
                    return;
                };
                let res = match e.signal {
                    Signal::IceGatheringComplete => t.handle_ice_gathering_complete(&e),
                    Signal::LocalIceCandidate => t.handle_local_ice_candidate(&e),
                    Signal::RemoteIceCandidate => t.handle_remote_ice_candidate(&e),
                    Signal::SendOffer => t.handle_send_offer(&e),
                    Signal::RemoteDescriptionReceived => t.handle_remote_description_received(&e),
                    Signal::IceRestart => t.handle_ice_restart(&e),
                };
                if let Err(err) = res
                    && !t.is_closed.load()
                {
                    t.on_negotiation_failed(
                        true,
                        &format!("error handling event. err: {err}, event: {e}"),
                    );
                }
            },
            e,
        );
    }

    fn handle_ice_gathering_complete(&self, _e: &Event) -> Result<(), Error> {
        if self.params.is_offerer {
            self.handle_ice_gathering_complete_offerer()
        } else {
            self.handle_ice_gathering_complete_answerer()
        }
    }

    fn handle_ice_gathering_complete_offerer(&self) -> Result<(), Error> {
        {
            let mut events = self.events.lock();
            if !events.restart_after_gathering {
                return Ok(());
            }
            events.restart_after_gathering = false;
        }

        debugw!(self.params.logger, "restarting ICE after ICE gathering");
        self.do_ice_restart()
    }

    fn handle_ice_gathering_complete_answerer(&self) -> Result<(), Error> {
        let (offer, parsed) = {
            let mut events = self.events.lock();
            let Some(offer) = events.pending_restart_ice_offer.take() else {
                return Ok(());
            };
            (offer, events.pending_restart_ice_offer_parsed.take())
        };
        // set with the offer
        let mut parsed = parsed.unwrap_or_default();

        debugw!(
            self.params.logger,
            "accept remote restart ice offer after ICE gathering"
        );

        self.set_remote_description(&offer, &mut parsed)?;
        self.params.handler.on_set_remote_description_offer();
        self.process_senders_pending_config(offer_audio_payload_types(Some(&parsed)).as_ref());

        self.create_and_send_answer()
    }

    fn local_description_sent(&self) -> Result<(), Error> {
        let cached_local_candidates = {
            let mut events = self.events.lock();
            if !events.cache_local_candidates {
                return Ok(());
            }

            events.cache_local_candidates = false;

            std::mem::take(&mut events.cached_local_candidates)
        };

        for c in &cached_local_candidates {
            if let Err(err) = self
                .params
                .handler
                .on_ice_candidate(c.as_ref(), self.params.transport)
            {
                warnw!(self.params.logger, "failed to send cached ICE candidate", err, "candidate" => c);
                return Err(err);
            }
        }
        Ok(())
    }

    fn clear_local_description_sent(&self) {
        {
            let mut events = self.events.lock();
            events.cache_local_candidates = true;
            events.cached_local_candidates = Vec::new();
        }
        self.connection_details.clear();
    }

    fn handle_local_ice_candidate(&self, e: &Event) -> Result<(), Error> {
        let EventData::LocalIceCandidate(c) = &e.data else {
            return Ok(());
        };

        let mut filtered = false;
        if let Some(c) = c {
            if self.prefer_tcp.load() && c.protocol != IceProtocol::Tcp {
                debugw!(self.params.logger, "filtering out local candidate, TCP preferred", "candidate" => c.to_string());
                filtered = true;
            }
            if !filtered && self.params.exclude_ipv6_local_candidates && is_ipv6(&c.address) {
                debugw!(self.params.logger, "filtering out local candidate, IPv6 excluded", "candidate" => c.to_string());
                filtered = true;
            }
            self.connection_details
                .add_local_candidate(c, filtered, true);
        }

        if filtered {
            return Ok(());
        }

        {
            let mut events = self.events.lock();
            if events.cache_local_candidates {
                events.cached_local_candidates.push(c.clone());
                return Ok(());
            }
        }

        if let Err(err) = self
            .params
            .handler
            .on_ice_candidate(c.as_ref(), self.params.transport)
        {
            warnw!(self.params.logger, "failed to send ICE candidate", err, "candidate" => c);
            return Err(err);
        }

        Ok(())
    }

    fn handle_remote_ice_candidate(&self, e: &Event) -> Result<(), Error> {
        let EventData::RemoteIceCandidate(c) = &e.data else {
            return Ok(());
        };

        let mut filtered = false;
        if self.prefer_tcp.load() && !c.candidate.to_lowercase().contains("tcp") {
            debugw!(self.params.logger, "filtering out remote candidate", "candidate" => c.candidate);
            filtered = true;
        }

        if !self.params.config.web_rtc_config.use_mdns && types::is_candidate_mdns(c) {
            debugw!(self.params.logger, "ignoring mDNS candidate", "candidate" => c.candidate);
            filtered = true;
        }

        self.connection_details
            .add_remote_candidate(c, filtered, true, false);
        if filtered {
            return Ok(());
        }

        if self.pc.remote_description().is_none() {
            self.events.lock().pending_remote_candidates.push(c.clone());
            return Ok(());
        }

        if let Err(err) = self.pc.add_ice_candidate(c.clone()) {
            warnw!(self.params.logger, "failed to add ICE candidate", err, "candidate" => c);
            // ignore ParseAddr error as it does not affect ICE connectivity
            if !err.to_string().contains("ParseAddr") {
                return Err(wrap(err, "add ice candidate failed"));
            }
        } else {
            debugw!(self.params.logger, "added ICE candidate", "candidate" => c);
        }

        Ok(())
    }

    fn set_negotiation_state(&self, state: NegotiationState) {
        self.events.lock().negotiation_state = state;
        if let Some(on_negotiation_state_changed) = self.get_on_negotiation_state_changed() {
            on_negotiation_state_changed(state);
        }
    }

    // `t.negotiationState` where the original reads it
    fn negotiation_state(&self) -> NegotiationState {
        self.events.lock().negotiation_state
    }

    fn is_candidate_filter_active(&self, prefer_tcp: bool) -> bool {
        prefer_tcp || self.params.exclude_ipv6_local_candidates
    }

    fn filter_candidates(
        &self,
        sd: SessionDescription,
        prefer_tcp: bool,
        is_local: bool,
    ) -> SessionDescription {
        self.filter_candidates_parsed(sd, None, prefer_tcp, is_local)
    }

    // filterCandidatesParsed filters using an already parsed description when available,
    // the parsed description is modified in place to match the returned SDP
    fn filter_candidates_parsed(
        &self,
        mut sd: SessionDescription,
        parsed: Option<&mut sdp::SessionDescription>,
        prefer_tcp: bool,
        is_local: bool,
    ) -> SessionDescription {
        let mut own;
        let parsed = match parsed {
            Some(parsed) => parsed,
            None => match sd.unmarshal() {
                Ok(parsed) => {
                    own = parsed;
                    &mut own
                }
                Err(err) => {
                    warnw!(
                        self.params.logger,
                        "could not unmarshal SDP to filter candidates",
                        err
                    );
                    return sd;
                }
            },
        };

        self.filter_attributes(&mut parsed.attributes, &sd.sdp, prefer_tcp, is_local);
        for m in &mut parsed.media_descriptions {
            self.filter_attributes(&mut m.attributes, &sd.sdp, prefer_tcp, is_local);
        }

        // `parsed.Marshal()`, which cannot fail here
        sd.sdp = parsed.marshal();
        sd
    }

    // `filterAttributes` of `filterCandidatesParsed`
    fn filter_attributes(
        &self,
        attrs: &mut Vec<sdp::Attribute>,
        sdp_text: &str,
        prefer_tcp: bool,
        is_local: bool,
    ) {
        let mut filtered_attrs = Vec::with_capacity(attrs.len());
        for a in attrs.drain(..) {
            if a.key == sdp::ATTR_KEY_CANDIDATE {
                let c = match ice::unmarshal_candidate(a.value.as_deref().unwrap_or("")) {
                    Ok(c) => c,
                    Err(err) => {
                        errorw!(self.params.logger, "failed to unmarshal candidate in sdp", err, "isLocal" => is_local, "sdp" => sdp_text);
                        filtered_attrs.push(a);
                        continue;
                    }
                };
                // pion's `NetworkType.IsIPv6()`
                let is_ipv6 = matches!(
                    c.network_type(),
                    ice::NetworkType::Udp6 | ice::NetworkType::Tcp6
                );
                let excluded = (prefer_tcp && !c.network_type().is_tcp())
                    || (self.params.exclude_ipv6_local_candidates && is_local && is_ipv6)
                    || (!self.params.config.web_rtc_config.use_mdns
                        && types::is_ice_candidate_mdns(Some(&c)));
                if !excluded {
                    filtered_attrs.push(a);
                }

                if is_local {
                    self.connection_details
                        .add_local_ice_candidate(&c, excluded, false);
                } else {
                    self.connection_details.add_remote_ice_candidate(
                        Some(&c),
                        excluded,
                        false,
                        false,
                    );
                }
            } else {
                filtered_attrs.push(a);
            }
        }

        *attrs = filtered_attrs;
    }

    fn clear_signal_state_check_timer(&self) {
        if let Some(timer) = self.events.lock().signal_state_check_timer.take() {
            timer.stop();
        }
    }

    fn setup_signal_state_check_timer(&self) {
        self.clear_signal_state_check_timer();

        let negotiate_version = self.negotiate_counter.inc();
        let timer = time::after_func(
            NEGOTIATION_FAILED_TIMEOUT,
            self.weak_timer(move |t| {
                t.clear_signal_state_check_timer();

                let failed = t.negotiation_state() != NegotiationState::None;

                if t.negotiate_counter.load() == negotiate_version
                    && failed
                    && t.pc.connection_state() == PeerConnectionState::Connected
                {
                    t.on_negotiation_failed(false, "negotiation timed out");
                }
            }),
        );
        self.events.lock().signal_state_check_timer = Some(timer);
    }

    fn adjust_num_outstanding_media(&self, transceiver: &RtpTransceiver) {
        if !transceiver.mid().is_empty() {
            return;
        }

        let mut inner = self.lock.write();
        if transceiver.kind() == RtpCodecType::Audio {
            inner.num_outstanding_audios = inner.num_outstanding_audios.wrapping_add(1);
        } else {
            inner.num_outstanding_videos = inner.num_outstanding_videos.wrapping_add(1);
        }
    }

    fn send_unmatched_media_requirement(&self, force: bool) -> Result<(), Error> {
        // if there are unmatched media sections, notify remote peer to generate offer with
        // enough media section in subsequent offers
        let (num_audios, num_videos) = {
            let mut inner = self.lock.write();
            let num_audios = inner
                .num_outstanding_audios
                .wrapping_sub(inner.num_request_sent_audios);
            inner.num_request_sent_audios = inner.num_request_sent_audios.wrapping_add(num_audios);

            let num_videos = inner
                .num_outstanding_videos
                .wrapping_sub(inner.num_request_sent_videos);
            inner.num_request_sent_videos = inner.num_request_sent_videos.wrapping_add(num_videos);
            (num_audios, num_videos)
        };

        if force || num_audios.wrapping_add(num_videos) != 0 {
            self.params
                .handler
                .on_unmatched_media(num_audios, num_videos)
                .map_err(|err| wrap(err, "could not send unmatched media requirements"))?;
        }

        Ok(())
    }

    fn create_and_send_offer(&self, mut options: Option<OfferOptions>) -> Result<(), Error> {
        if self.pc.connection_state() == PeerConnectionState::Closed {
            warnw!(
                self.params.logger,
                "trying to send offer on closed peer connection",
                nil
            );
            return Ok(());
        }

        // when there's an ongoing negotiation, let it finish and not disrupt its state
        match self.negotiation_state() {
            NegotiationState::Remote => {
                debugw!(
                    self.params.logger,
                    "skipping negotiation, trying again later"
                );
                self.set_negotiation_state(NegotiationState::Retry);
                return Ok(());
            }
            NegotiationState::Retry => {
                // already set to retry, we can safely skip this attempt
                return Ok(());
            }
            NegotiationState::None => {}
        }

        let ensure_ice_restart = |options: Option<OfferOptions>| {
            let mut options = options.unwrap_or_default();
            options.ice_restart = true;
            Some(options)
        };

        {
            let mut inner = self.lock.write();
            if inner.previous_answer.is_some() {
                inner.previous_answer = None;
                options = ensure_ice_restart(options);
                infow!(self.params.logger, "ice restart due to previous answer");
            }
        }

        let restart_at_next_offer = std::mem::take(&mut self.events.lock().restart_at_next_offer);
        if restart_at_next_offer {
            options = ensure_ice_restart(options);
            infow!(self.params.logger, "ice restart at next offer");
        }

        if options.is_some_and(|options| options.ice_restart) {
            self.clear_local_description_sent();
        }

        let offer = match self.pc.create_offer(options) {
            Ok(offer) => offer,
            Err(err) => {
                if err == webrtc::Error::ConnectionClosed {
                    warnw!(
                        self.params.logger,
                        "trying to create offer on closed peer connection",
                        nil
                    );
                    return Ok(());
                }

                prometheus::record_service_operation_error("offer", "create");
                return Err(wrap(err, "create offer failed"));
            }
        };

        let prefer_tcp = self.prefer_tcp.load();
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local offer (unfiltered)", "sdp" => offer.sdp);
        }

        let is_start_of_connection_sequence = self.pc.local_description().is_none();

        if let Err(err) = self.pc.set_local_description(offer.clone()) {
            if err == webrtc::Error::ConnectionClosed {
                warnw!(
                    self.params.logger,
                    "trying to set local description on closed peer connection",
                    nil
                );
                return Ok(());
            }

            prometheus::record_service_operation_error("offer", "local_description");
            return Err(wrap(err, "setting local description failed"));
        }

        if is_start_of_connection_sequence {
            prometheus::record_peer_connection_state(self.params.transport, "started");
        }

        //
        // Filter after setting local description as pion expects the offer
        // to match between CreateOffer and SetLocalDescription.
        // Filtered offer is sent to remote so that remote does not
        // see filtered candidates.
        //
        let offer = self.filter_candidates(offer, prefer_tcp, true);
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local offer (filtered)", "sdp" => offer.sdp);
        }

        // indicate waiting for remote
        self.set_negotiation_state(NegotiationState::Remote);

        self.setup_signal_state_check_timer();

        let remote_answer_id = self.remote_answer_id.load();
        if remote_answer_id != 0
            && remote_answer_id != self.local_offer_id.load()
            && !options.is_some_and(|options| options.ice_restart)
        {
            warnw!(
                self.params.logger,
                "sdp state: sending offer before receiving answer", nil,
                "localOfferId" => self.local_offer_id.load(),
                "remoteAnswerId" => remote_answer_id,
            );
        }

        if let Err(err) = self.params.handler.on_offer(
            offer,
            self.local_offer_id.inc(),
            &self.get_mid_to_track_id_mapping(),
        ) {
            prometheus::record_service_operation_error("offer", "write_message");
            return Err(wrap(err, "could not send offer"));
        }
        prometheus::record_service_operation_success("offer");

        self.local_description_sent()
    }

    fn handle_send_offer(&self, _e: &Event) -> Result<(), Error> {
        if !self.params.is_offerer {
            return self.send_unmatched_media_requirement(true);
        }

        self.create_and_send_offer(None)
    }

    fn handle_remote_description_received(&self, e: &Event) -> Result<(), Error> {
        let EventData::RemoteDescription(rdd) = &e.data else {
            return Ok(());
        };
        let parsed = match &rdd.parsed {
            Ok(parsed) => parsed.clone(),
            Err(err) => return Err(err.clone().into()),
        };
        if rdd.session_description.r#type == SdpType::Offer {
            self.handle_remote_offer_received(&rdd.session_description, parsed, rdd.remote_id)
        } else {
            self.handle_remote_answer_received(&rdd.session_description, parsed, rdd.remote_id)
        }
    }

    fn is_remote_offer_restart_ice(
        &self,
        parsed: &sdp::SessionDescription,
    ) -> Result<(String, bool), Error> {
        let (user, pwd) = lksdp::extract_ice_credential(parsed)?;

        let credential = format!("{user}:{pwd}");
        // ice credential changed, remote offer restart ice
        let current_offer_ice_credential = self.events.lock().current_offer_ice_credential.clone();
        let restart_ice =
            !current_offer_ice_credential.is_empty() && current_offer_ice_credential != credential;
        Ok((credential, restart_ice))
    }

    fn set_remote_description(
        &self,
        sd: &SessionDescription,
        parsed: &mut sdp::SessionDescription,
    ) -> Result<(), Error> {
        // filter before setting remote description so that pion does not see filtered remote candidates
        let prefer_tcp = self.prefer_tcp.load();
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "remote description (unfiltered)", "type" => sd.r#type, "sdp" => sd.sdp);
        }
        let sd = self.filter_candidates_parsed(sd.clone(), Some(parsed), prefer_tcp, false);
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "remote description (filtered)", "type" => sd.r#type, "sdp" => sd.sdp);
        }

        if let Err(err) = self.pc.set_remote_description(sd.clone()) {
            if err == webrtc::Error::ConnectionClosed {
                warnw!(
                    self.params.logger,
                    "trying to set remote description on closed peer connection",
                    nil
                );
                return Ok(());
            }

            let sdp_type = if sd.r#type == SdpType::Answer {
                "answer"
            } else {
                "offer"
            };
            prometheus::record_service_operation_error(sdp_type, "remote_description");
            return Err(wrap(err, "setting remote description failed"));
        }
        self.set_remote_offer_parsed(sd.r#type, Some(Arc::new(parsed.clone())));
        if sd.r#type == SdpType::Answer {
            let mut inner = self.lock.write();
            if !inner.can_reuse_transceiver {
                inner.can_reuse_transceiver = true;
                inner.previous_track_description = HashMap::new();
                debugw!(self.params.logger, "enabling transceiver reuse");
            }
        }

        let pending_remote_candidates = self.events.lock().pending_remote_candidates.clone();
        for c in &pending_remote_candidates {
            if let Err(err) = self.pc.add_ice_candidate(c.clone()) {
                warnw!(self.params.logger, "failed to add cached ICE candidate", err, "candidate" => c);
                return Err(wrap(err, "add ice candidate failed"));
            } else {
                debugw!(self.params.logger, "added cached ICE candidate", "candidate" => c);
            }
        }
        self.events.lock().pending_remote_candidates = Vec::new();

        Ok(())
    }

    fn create_and_send_answer(&self) -> Result<(), Error> {
        let (num_outstanding_audios, num_outstanding_videos) =
            self.get_num_unmatched_transceivers();
        {
            let mut inner = self.lock.write();
            (inner.num_outstanding_audios, inner.num_outstanding_videos) =
                (num_outstanding_audios, num_outstanding_videos);
            (inner.num_request_sent_audios, inner.num_request_sent_videos) = (0, 0);
        }

        self.restrict_receiver_codecs_to_publish_list();

        let answer = match self.pc.create_answer() {
            Ok(answer) => answer,
            Err(err) => {
                if err == webrtc::Error::ConnectionClosed {
                    warnw!(
                        self.params.logger,
                        "trying to create answer on closed peer connection",
                        nil
                    );
                    return Ok(());
                }

                prometheus::record_service_operation_error("answer", "create");
                return Err(wrap(err, "create answer failed"));
            }
        };

        let prefer_tcp = self.prefer_tcp.load();
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local answer (unfiltered)", "sdp" => answer.sdp);
        }

        if let Err(err) = self.pc.set_local_description(answer.clone()) {
            prometheus::record_service_operation_error("answer", "local_description");
            return Err(wrap(err, "setting local description failed"));
        }

        //
        // Filter after setting local description as pion expects the answer
        // to match between CreateAnswer and SetLocalDescription.
        // Filtered answer is sent to remote so that remote does not
        // see filtered candidates.
        //
        let answer = self.filter_candidates(answer, prefer_tcp, true);
        if self.is_candidate_filter_active(prefer_tcp) {
            debugw!(self.params.logger, "local answer (filtered)", "sdp" => answer.sdp);
        }

        let local_answer_id = self.local_answer_id.load();
        if local_answer_id != 0 && local_answer_id >= self.remote_offer_id.load() {
            warnw!(
                self.params.logger,
                "sdp state: duplicate answer", nil,
                "localAnswerId" => local_answer_id,
                "remoteOfferId" => self.remote_offer_id.load(),
            );
        }

        let answer_id = self.remote_offer_id.load();

        if let Err(err) =
            self.params
                .handler
                .on_answer(answer, answer_id, &self.get_mid_to_track_id_mapping())
        {
            prometheus::record_service_operation_error("answer", "write_message");
            return Err(wrap(err, "could not send answer"));
        }
        self.local_answer_id.store(answer_id);
        prometheus::record_service_operation_success("answer");

        self.send_unmatched_media_requirement(false)?;

        {
            let mut inner = self.lock.write();
            if !inner.can_reuse_transceiver {
                inner.can_reuse_transceiver = true;
                inner.previous_track_description = HashMap::new();
                debugw!(self.params.logger, "enabling transceiver reuse");
            }
        }

        self.local_description_sent()
    }

    fn handle_remote_offer_received(
        &self,
        sd: &SessionDescription,
        mut parsed: sdp::SessionDescription,
        offer_id: u32,
    ) -> Result<(), Error> {
        debugw!(self.params.logger, "processing offer", "offerId" => offer_id);
        let remote_offer_id = self.remote_offer_id.load();
        if remote_offer_id != 0 && remote_offer_id != self.local_answer_id.load() {
            warnw!(
                self.params.logger,
                "sdp state: multiple offers without answer", nil,
                "remoteOfferId" => remote_offer_id,
                "localAnswerId" => self.local_answer_id.load(),
                "receivedRemoteOfferId" => offer_id,
            );
        }
        self.remote_offer_id.store(offer_id);

        {
            let mut inner = self.lock.write();
            if !inner.first_offer_received {
                inner.first_offer_received = true;
                let mut data_channel_found = false;
                for media in &parsed.media_descriptions {
                    if media.media_name.media.eq_ignore_ascii_case("application") {
                        data_channel_found = true;
                        break;
                    }
                }
                inner.first_offer_no_data_channel = !data_channel_found;
            }
        }

        let (ice_credential, offer_restart_ice) = self
            .is_remote_offer_restart_ice(&parsed)
            .map_err(|err| wrap(err, "check remote offer restart ice failed"))?;

        if offer_restart_ice && self.events.lock().pending_restart_ice_offer.is_none() {
            self.clear_local_description_sent();
        }

        if offer_restart_ice && self.pc.ice_gathering_state() == IceGatheringState::Gathering {
            debugw!(
                self.params.logger,
                "remote offer restart ice while ice gathering"
            );
            let mut events = self.events.lock();
            events.pending_restart_ice_offer = Some(sd.clone());
            events.pending_restart_ice_offer_parsed = Some(parsed);
            return Ok(());
        }

        if offer_restart_ice && self.reset_short_conn_on_ice_restart.cas(true, false) {
            self.reset_short_conn();
        }

        if offer_restart_ice {
            self.output_and_clear_ice_stats();
        }

        let is_start_of_connection_sequence = self.pc.remote_description().is_none();

        self.set_remote_description(sd, &mut parsed)?;

        if is_start_of_connection_sequence {
            prometheus::record_peer_connection_state(self.params.transport, "started");
        }

        self.params.handler.on_set_remote_description_offer();
        self.process_senders_pending_config(offer_audio_payload_types(Some(&parsed)).as_ref());

        let rtx_repairs = non_simulcast_rtx_repairs_from_sdp(&parsed, &self.params.logger);
        if !rtx_repairs.is_empty() {
            debugw!(self.params.logger, "rtx pairs found from sdp", "ssrcs" => rtx_repairs);
            if let Some(buffer_factory) = &self.params.config.buffer_factory {
                for (repair, base) in &rtx_repairs {
                    buffer_factory.set_rtx_pair(*repair, *base, "");
                }
            }
        }

        if self.params.direction_config.flex_fec.enabled {
            let fec_flows = fec_pairs_from_sdp(&parsed, &self.params.logger);
            if !fec_flows.is_empty() {
                debugw!(self.params.logger, "fec pairs found from sdp", "ssrcs" => fec_flows);
                if let Some(buffer_factory) = &self.params.config.buffer_factory {
                    for (fec, base) in &fec_flows {
                        buffer_factory.set_fec_pair(*fec, *base);
                    }
                }
            }
        }

        {
            let mut events = self.events.lock();
            if events.current_offer_ice_credential.is_empty() || offer_restart_ice {
                events.current_offer_ice_credential = ice_credential;
            }
        }

        self.create_and_send_answer()
    }

    fn handle_remote_answer_received(
        &self,
        sd: &SessionDescription,
        mut parsed: sdp::SessionDescription,
        answer_id: u32,
    ) -> Result<(), Error> {
        debugw!(self.params.logger, "processing answer", "answerId" => answer_id);
        if answer_id != 0 && answer_id != self.local_offer_id.load() {
            warnw!(
                self.params.logger,
                "sdp state: answer id mismatch", nil,
                "expected" => self.local_offer_id.load(),
                "got" => answer_id,
            );
        }
        self.remote_answer_id.store(answer_id);

        self.clear_signal_state_check_timer();

        if let Err(err) = self.set_remote_description(sd, &mut parsed) {
            // Pion will call RTPSender.Send method for each new added Downtrack, and return error if the DownTrack.Bind
            // returns error. In case of Downtrack.Bind returns ErrUnsupportedCodec, the signal state will be stable as negotiation is already completed
            // before startRTPSenders, and the peerconnection state can be recovered by next negotiation which will be triggered
            // by the SubscriptionManager unsubscribe the failure DownTrack. So don't treat this error as negotiation failure.
            // (The layer binds tracks after the call has returned and logs what `bind` says:
            // this error does not come here.)
            if !is_webrtc_error(&err, &webrtc::Error::UnsupportedCodec) {
                return Err(err);
            }
        }

        if self.negotiation_state() == NegotiationState::Retry {
            self.set_negotiation_state(NegotiationState::None);

            debugw!(self.params.logger, "re-negotiate after receiving answer");
            return self.create_and_send_offer(None);
        }

        self.set_negotiation_state(NegotiationState::None);
        Ok(())
    }

    fn do_ice_restart(&self) -> Result<(), Error> {
        if self.pc.connection_state() == PeerConnectionState::Closed {
            warnw!(
                self.params.logger,
                "trying to restart ICE on closed peer connection",
                nil
            );
            return Ok(());
        }

        // if restart is requested, but negotiation never started
        let ice_gathering_state = self.pc.ice_gathering_state();
        if ice_gathering_state == IceGatheringState::New {
            debugw!(
                self.params.logger,
                "skipping ICE restart on not yet started peer connection"
            );
            return Ok(());
        }

        // if restart is requested, and we are not ready, then continue afterwards
        if ice_gathering_state == IceGatheringState::Gathering {
            debugw!(
                self.params.logger,
                "deferring ICE restart to after gathering"
            );
            self.events.lock().restart_after_gathering = true;
            return Ok(());
        }

        if self.reset_short_conn_on_ice_restart.cas(true, false) {
            self.reset_short_conn();
        }

        if self.negotiation_state() == NegotiationState::None {
            self.output_and_clear_ice_stats();
            return self.create_and_send_offer(Some(OfferOptions { ice_restart: true }));
        }

        let current_remote_description = self.pc.current_remote_description();
        match current_remote_description {
            None => {
                // restart without current remote description, send current local description again to try recover
                let offer = self.pc.local_description();
                match offer {
                    None => {
                        // it should not happen, log just in case
                        warnw!(self.params.logger, "ice restart without local offer", nil);
                        Err(Error::IceRestartWithoutLocalSdp)
                    }
                    Some(offer) => {
                        infow!(self.params.logger, "deferring ice restart to next offer");
                        self.set_negotiation_state(NegotiationState::Retry);
                        self.events.lock().restart_at_next_offer = true;

                        let remote_answer_id = self.remote_answer_id.load();
                        if remote_answer_id != 0 && remote_answer_id != self.local_offer_id.load() {
                            warnw!(
                                self.params.logger,
                                "sdp state: answer not received in ICE restart", nil,
                                "localOfferId" => self.local_offer_id.load(),
                                "remoteAnswerId" => remote_answer_id,
                            );
                        }

                        let result = self.params.handler.on_offer(
                            offer,
                            self.local_offer_id.inc(),
                            &self.get_mid_to_track_id_mapping(),
                        );
                        if result.is_err() {
                            prometheus::record_service_operation_error("offer", "write_message");
                        } else {
                            prometheus::record_service_operation_success("offer");
                        }
                        result
                    }
                }
            }
            Some(current_remote_description) => {
                // recover by re-applying the last answer
                infow!(
                    self.params.logger,
                    "recovering from client negotiation state on ICE restart"
                );
                if let Err(err) = self.pc.set_remote_description(current_remote_description) {
                    prometheus::record_service_operation_error("offer", "remote_description");
                    Err(wrap(err, "set remote description failed"))
                } else {
                    self.set_negotiation_state(NegotiationState::None);
                    self.output_and_clear_ice_stats();
                    self.create_and_send_offer(Some(OfferOptions { ice_restart: true }))
                }
            }
        }
    }

    fn handle_ice_restart(&self, _e: &Event) -> Result<(), Error> {
        self.do_ice_restart()
    }

    fn on_negotiation_failed(&self, warning: bool, reason: &str) {
        let local_current = self.pc.current_local_description();
        let local_pending = self.pc.pending_local_description();
        let remote_current = self.pc.current_remote_description();
        let remote_pending = self.pc.pending_remote_description();
        if warning {
            warnw!(
                self.params.logger,
                "negotiation failed",
                nil,
                "reason" => reason,
                "localCurrent" => local_current,
                "localPending" => local_pending,
                "remoteCurrent" => remote_current,
                "remotePending" => remote_pending,
            );
        } else {
            infow!(
                self.params.logger,
                "negotiation failed",
                "reason" => reason,
                "localCurrent" => local_current,
                "localPending" => local_pending,
                "remoteCurrent" => remote_current,
                "remotePending" => remote_pending,
            );
        }
        self.params.handler.on_negotiation_failed();
    }

    fn output_and_clear_ice_stats(&self) {
        let stats = std::mem::take(&mut self.lock.write().may_failed_ice_stats);

        if !stats.is_empty() {
            infow!(self.params.logger, "ICE candidate pair stats", "stats" => IceCandidatePairStatsEncoder { stats: &stats });
        }
    }

    fn get_mid_to_track_id_mapping(&self) -> HashMap<String, String> {
        let transceivers = self.pc.get_transceivers();
        let mut mid_to_track_id = HashMap::with_capacity(transceivers.len());
        for tr in transceivers {
            let mid = tr.mid();
            if !mid.is_empty()
                && let Some(sender) = tr.sender()
                && let Some(track) = sender.track()
            {
                mid_to_track_id.insert(mid, track.id());
            }
        }
        mid_to_track_id
    }
}

impl PcTransportInner {
    fn update_last_negotiate_locked(&mut self) {
        let now = time::now();
        if now.after(self.last_negotiate) {
            self.last_negotiate = now;
        }
    }
}

// The slot of a data channel this side makes, where the original keeps a pointer to the
// field of the transport.
#[derive(Clone, Copy)]
enum DataChannelSlot {
    Reliable,
    Lossy,
    DataTrack,
}

// Which of the handler's methods a data channel's messages go to.
#[derive(Clone, Copy)]
enum DataChannelReader {
    Unlabeled,
    DataTrack,
    Kind(livekit::data_packet::Kind),
}

#[derive(Debug)]
struct RemoteDescriptionData {
    session_description: SessionDescription,
    // `parsed` with `parseErr`, which is set when parsed is nil
    parsed: Result<sdp::SessionDescription, webrtc::Error>,
    remote_id: u32,
}

// ----------------------

#[derive(Clone)]
struct ConfigureSenderParams {
    transceiver: Arc<RtpTransceiver>,
    enabled_codecs: Vec<livekit::Codec>,
    rtcp_feedback_config: RtcpFeedbackConfig,
    filter_out_h264_high_profile: bool,
    enable_audio_stereo: bool,
    enable_audio_nack: bool,
    enable_flex_fec: bool,
}

fn configure_sender(
    params: &ConfigureSenderParams,
    offer_audio_pt: Option<&HashMap<mime::MimeType, PayloadType>>,
) {
    configure_sender_codecs(
        &params.transceiver,
        &params.enabled_codecs,
        &params.rtcp_feedback_config,
        params.filter_out_h264_high_profile,
        params.enable_flex_fec,
    );

    if params.transceiver.kind() == RtpCodecType::Audio {
        configure_sender_audio(
            &params.transceiver,
            params.enable_audio_stereo,
            params.enable_audio_nack,
            offer_audio_pt,
        );
    }
}

// configure subscriber transceiver for audio stereo and nack
// pion doesn't support per transciver codec configuration, so the nack of this session will be disabled
// forever once it is first disabled by a transceiver.
fn configure_sender_audio(
    tr: &RtpTransceiver,
    stereo: bool,
    nack: bool,
    offer_audio_pt: Option<&HashMap<mime::MimeType, PayloadType>>,
) {
    let Some(sender) = tr.sender() else {
        return;
    };

    // enable stereo
    let codecs = sender.get_parameters().rtp_parameters.codecs;
    let mut config_codecs = Vec::with_capacity(codecs.len());
    for mut c in codecs {
        if mime::is_mime_type_string_opus(&c.rtp_codec_capability.mime_type) {
            c.rtp_codec_capability.sdp_fmtp_line = c
                .rtp_codec_capability
                .sdp_fmtp_line
                .replace(";sprop-stereo=1", "");
            if stereo {
                c.rtp_codec_capability.sdp_fmtp_line += ";sprop-stereo=1";
            }
            if !nack
                && let Some(i) = c
                    .rtp_codec_capability
                    .rtcp_feedback
                    .iter()
                    .position(|fb| fb.r#type == webrtc::TYPE_RTCP_FB_NACK)
            {
                c.rtp_codec_capability.rtcp_feedback.remove(i);
            }
        }
        // When answering a subscriber's offer (single peer connection mode), echo
        // the payload type the offer assigned for this codec instead of the server's
        // MediaEngine payload type. Otherwise the answer can advertise e.g. Opus on a
        // PT that was never offered, which Firefox rejects (received packets decode to
        // 0 samples / silence). The forwarded RTP already uses the offered PT.
        if let Some(offer_audio_pt) = offer_audio_pt
            && !offer_audio_pt.is_empty()
            && let Some(pt) = offer_audio_pt.get(&mime::normalize_mime_type(
                &c.rtp_codec_capability.mime_type,
            ))
        {
            c.payload_type = *pt;
        }
        config_codecs.push(c);
    }

    let _ = tr.set_codec_preferences(config_codecs);
}

// offerAudioPayloadTypes returns mime type -> payload type for the audio codecs
// in a remote offer, so the subscriber answer can echo the offered payload types
// (RFC 3264 6.1). The caller parses the offer before SetRemoteDescription, so this
// does not race with pion's use of the same description.
fn offer_audio_payload_types(
    parsed: Option<&sdp::SessionDescription>,
) -> Option<HashMap<mime::MimeType, PayloadType>> {
    let parsed = parsed?;
    let mut out = HashMap::new();
    for md in &parsed.media_descriptions {
        if !md.media_name.media.eq_ignore_ascii_case("audio") {
            continue;
        }
        for a in &md.attributes {
            if a.key != "rtpmap" {
                continue;
            }
            // value e.g. "109 opus/48000/2"
            let fields: Vec<&str> = a
                .value
                .as_deref()
                .unwrap_or("")
                .split_whitespace()
                .collect();
            if fields.len() < 2 {
                continue;
            }
            let Ok(pt) = parse_uint(fields[0], u8::MAX as u64) else {
                continue;
            };
            let mut codec_name = fields[1];
            if let Some(i) = codec_name.find('/') {
                codec_name = &codec_name[..i];
            }
            let mt = mime::normalize_mime_type_codec(codec_name).to_mime_type();
            if mt == mime::MimeType::Unknown {
                continue;
            }
            out.insert(mt, pt as PayloadType);
        }
    }
    if out.is_empty() {
        return None;
    }
    Some(out)
}

// Go's `strconv.ParseUint(s, 10, bits)` for a maximum of `max`, with its errors as text.
fn parse_uint(s: &str, max: u64) -> Result<u64, String> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.ParseUint: parsing {s:?}: invalid syntax"));
    }
    match s.parse::<u64>() {
        Ok(v) if v <= max => Ok(v),
        _ => Err(format!(
            "strconv.ParseUint: parsing {s:?}: value out of range"
        )),
    }
}

// In single peer connection mode, set up enabled codecs for sender.
// The config provides config of direction.
// For publisher peer connection those are publish enabled codecs
// and for subscriber peer connection those are subscribe enabled codecs.
//
// But, in single peer connection mode, if setting up a transceiver where the media is
// flowing in the other direction, the other direction codec config needs to be set.
fn configure_sender_codecs(
    tr: &RtpTransceiver,
    enabled_codecs: &[livekit::Codec],
    rtcp_feedback_config: &RtcpFeedbackConfig,
    filter_out_h264_high_profile: bool,
    enable_flex_fec: bool,
) {
    if enabled_codecs.is_empty() {
        return;
    }

    let Some(sender) = tr.sender() else {
        return;
    };

    let filtered_codecs = filter_codecs(
        &sender.get_parameters().rtp_parameters.codecs,
        enabled_codecs,
        rtcp_feedback_config,
        filter_out_h264_high_profile,
        enable_flex_fec,
    );
    let _ = tr.set_codec_preferences(filtered_codecs);
}

impl PcTransport {
    // restrictReceiverCodecsToPublishList narrows recv-side transceiver codec
    // preferences to the publish list, so the answer doesn't advertise
    // subscribe-only codecs as receivable. No-op in dual-PC mode.
    fn restrict_receiver_codecs_to_publish_list(&self) {
        for tr in self.pc.get_transceivers() {
            if tr.direction() != RtpTransceiverDirection::Recvonly
                && tr.direction() != RtpTransceiverDirection::Sendrecv
            {
                continue;
            }
            // a mid-less sender is not negotiated yet, see sendersPendingConfig
            if tr.mid().is_empty() {
                continue;
            }
            let Some(receiver) = tr.receiver() else {
                continue;
            };
            let filtered = filter_codecs(
                &receiver.get_parameters().codecs,
                &self.params.enabled_publish_codecs,
                &self.params.direction_config.rtcp_feedback,
                false,
                self.params.direction_config.flex_fec.enabled,
            );
            if filtered.is_empty() {
                continue;
            }
            if let Err(err) = tr.set_codec_preferences(filtered) {
                warnw!(self.params.logger, "failed to set recv codec preferences", err, "mid" => tr.mid());
            }
        }
    }
}

pub fn configure_receiver_codecs(
    tr: &RtpTransceiver,
    preferred_mime_type: &str,
    complies_with_codec_order_in_sdp_answer: bool,
) {
    let Some(receiver) = tr.receiver() else {
        return;
    };

    let mut preferred_codecs = Vec::new();
    let mut left_codecs = Vec::new();
    for mut c in receiver.get_parameters().codecs {
        if tr.kind() == RtpCodecType::Audio {
            let mut nack_found = false;
            for fb in &c.rtp_codec_capability.rtcp_feedback {
                if fb.r#type == webrtc::TYPE_RTCP_FB_NACK {
                    nack_found = true;
                    break;
                }
            }

            if !nack_found {
                c.rtp_codec_capability
                    .rtcp_feedback
                    .push(webrtc::RtcpFeedback {
                        r#type: webrtc::TYPE_RTCP_FB_NACK.to_string(),
                        parameter: String::new(),
                    });
            }
        }

        if mime::get_mime_type_codec(preferred_mime_type)
            == mime::get_mime_type_codec(&c.rtp_codec_capability.mime_type)
        {
            preferred_codecs.push(c);
        } else {
            left_codecs.push(c);
        }
    }
    if preferred_codecs.is_empty() {
        return;
    }

    let mut reordered_codecs = preferred_codecs;
    if tr.kind() == RtpCodecType::Video {
        // if the client don't comply with codec order in SDP answer, only keep preferred codecs to force client to use it
        if complies_with_codec_order_in_sdp_answer {
            reordered_codecs.extend(left_codecs);
        } else {
            // flexfec applies to whichever video codec is negotiated, retain
            // it even when trimming to the preferred codec
            for c in left_codecs {
                if mime::is_mime_type_string_flex_fec03(&c.rtp_codec_capability.mime_type) {
                    reordered_codecs.push(c);
                }
            }
        }
    } else {
        reordered_codecs.extend(left_codecs);
    }
    let _ = tr.set_codec_preferences(reordered_codecs);
}

fn non_simulcast_rtx_repairs_from_sdp(
    s: &sdp::SessionDescription,
    logger: &Logger,
) -> HashMap<u32, u32> {
    let mut rtx_repair_flows = HashMap::new();
    for media in &s.media_descriptions {
        // extract rtx repair flows from the media section for non-simulcast stream,
        // pion will handle simulcast streams by rid probe, don't need handle it here.
        let mut rid_found = false;
        let mut rtx_pairs = HashMap::new();
        for attr in &media.attributes {
            match attr.key.as_str() {
                "rid" => {
                    rid_found = true;
                    break;
                }
                sdp::ATTR_KEY_SSRC_GROUP => {
                    let split: Vec<&str> = attr.value.as_deref().unwrap_or("").split(' ').collect();
                    if split[0] == sdp::SEMANTIC_TOKEN_FLOW_IDENTIFICATION {
                        // Essentially lines like `a=ssrc-group:FID 2231627014 632943048` are processed by this section
                        // as this declares that the second SSRC (632943048) is a rtx repair flow (RFC4588) for the first
                        // (2231627014) as specified in RFC5576
                        if split.len() == 3 {
                            let base_ssrc = match parse_uint(split[1], u32::MAX as u64) {
                                Ok(ssrc) => ssrc,
                                Err(err) => {
                                    warnw!(logger, "Failed to parse SSRC", err, "ssrc" => split[1]);
                                    continue;
                                }
                            };
                            let rtx_repair_flow = match parse_uint(split[2], u32::MAX as u64) {
                                Ok(ssrc) => ssrc,
                                Err(err) => {
                                    warnw!(logger, "Failed to parse SSRC", err, "ssrc" => split[2]);
                                    continue;
                                }
                            };
                            rtx_pairs.insert(rtx_repair_flow as u32, base_ssrc as u32);
                        }
                    }
                }
                _ => {}
            }
        }
        if !rid_found {
            rtx_repair_flows.extend(rtx_pairs);
        }
    }

    rtx_repair_flows
}

// fecPairsFromSDP extracts FlexFEC repair flows declared via
// `a=ssrc-group:FEC-FR <media ssrc> <fec ssrc>` (RFC 5956) from the remote
// description, returning a map of fec ssrc -> protected media ssrc.
fn fec_pairs_from_sdp(s: &sdp::SessionDescription, logger: &Logger) -> HashMap<u32, u32> {
    let mut fec_flows = HashMap::new();
    for media in &s.media_descriptions {
        for attr in &media.attributes {
            if attr.key != sdp::ATTR_KEY_SSRC_GROUP {
                continue;
            }
            let fields: Vec<&str> = attr
                .value
                .as_deref()
                .unwrap_or("")
                .split_whitespace()
                .collect();
            if fields.len() != 3
                || fields[0] != sdp::SEMANTIC_TOKEN_FORWARD_ERROR_CORRECTION_FRAMEWORK
            {
                continue;
            }
            let base_ssrc = match parse_uint(fields[1], u32::MAX as u64) {
                Ok(ssrc) => ssrc,
                Err(err) => {
                    warnw!(logger, "failed to parse SSRC", err, "ssrc" => fields[1]);
                    continue;
                }
            };
            let fec_ssrc = match parse_uint(fields[2], u32::MAX as u64) {
                Ok(ssrc) => ssrc,
                Err(err) => {
                    warnw!(logger, "failed to parse SSRC", err, "ssrc" => fields[2]);
                    continue;
                }
            };
            fec_flows.insert(fec_ssrc as u32, base_ssrc as u32);
        }
    }

    fec_flows
}

// ----------------------

struct IceCandidatePairStatsEncoder<'a> {
    stats: &'a [IceCandidatePairStats],
}

impl fmt::Debug for IceCandidatePairStatsEncoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.stats).finish()
    }
}

// What the original logs of `webrtc.ICECandidatePairStats` with its two candidates, as far as
// the layer's statistics have it (see the header).
#[derive(Clone, Default)]
struct IceCandidatePairStats {
    state: String,
    nominated: bool,
    local: String,
    remote: String,
    requests_sent: u64,
    responses_received: u64,
    requests_received: u64,
    responses_sent: u64,
}

impl fmt::Debug for IceCandidatePairStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("")
            .field("state", &self.state)
            .field("nominated", &self.nominated)
            .field("local", &self.local)
            .field("remote", &self.remote)
            .field("requestsSent", &self.requests_sent)
            .field("responsesReceived", &self.responses_received)
            .field("requestsReceived", &self.requests_received)
            .field("responsesSent", &self.responses_sent)
            .finish()
    }
}

#[cfg(test)]
#[path = "transport_test.rs"]
mod transport_test;

#[cfg(test)]
#[path = "transport_fec_test.rs"]
mod transport_fec_test;

#[cfg(test)]
#[path = "transport_media_sections_test.rs"]
mod transport_media_sections_test;
