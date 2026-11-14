// Ported from LiveKit, pkg/rtc/transportmanager.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, against the transport of `transport_impl.rs` and
// musivum's connection layer. What else differs:
//
// - `NewTransportManager` is `TransportManager::new` and gives an `Arc`. Its params have the
//   node the transports are on (`node`, new: one per process, made by the owner), which it
//   passes on to them.
// - `TransportManagerTransportHandler` embeds the participant's handler: here it holds it and
//   passes every method of `transport::Handler` on to it, as embedding does, `on_failed`
//   apart. Go hands it the manager's pointer before the transports are made; here it holds
//   the manager weakly through a cell that `new` fills once the manager is made (making a
//   transport can fail, and then there is no manager).
// - The publisher is never nil once `new` has returned: it is an `Arc`, and the original's
//   checks for a nil publisher, always true, are comments at their places. The subscriber
//   is an `Option`. Where the original calls it without a check, `subscriber()` takes it
//   out and panics where Go would dereference nil: never in the branches of the
//   two-connection mode, which has one, but in `SubscriberClose` and `SetMigrateInfo`
//   called in the single-connection or one-shot mode, and in `onMediaLossUpdate` in the
//   one-shot mode, as upstream.
// - `iceConfig` is never nil (made empty, replaced only by a value): it is a value, and
//   `get_ice_config` gives `Some` of a copy where the original checks for nil. The preferences
//   of `livekit.ICEConfig` are compared as the protocol's `i32`, as Go compares its int32
//   enum, so that a value the enum does not know equals none of the known ones.
// - ICE over TCP and TURN are not in the connection layer (docs/webrtc-library.md). The
//   fallback decisions are ported as logic and made as upstream makes them; what they would
//   change in a transport (`set_prefer_tcp`) is called as upstream calls it and changes only
//   the transport's own TCP ICE timer, and a preference for TURN goes to the client (through
//   `on_ice_config_changed`) and finds no relay candidate here. Comments at the places.
// - `AddICECandidate` takes the target as the enum: the original's arm for an unknown target
//   cannot be reached and is a comment. The participant converts the request's `i32`.
// - `PingTimeoutSeconds` (participant.go) and `connectionDetailsFields` (room.go), which are
//   not ported yet, are written out here for this file's use.
// - `GetDataTrackTransport` hands out the transport as `types::DataTrackTransport`: Go's
//   `*PCTransport` is one by having the method, and the `impl` that says so is at the end of
//   this file, the one place that needs it.
// - `utils.ErrorIsOneOf` and `errors.Is` on the errors of a data channel write are matches
//   on the variants that carry Go's errors in this port: `io.ErrClosedPipe` and
//   `sctp.ErrStreamClosed` are `webrtc::Error::{ClosedPipe, StreamClosed}` as the writer
//   (`Error::DataChannelWriter`) or the connection (`Error::Webrtc`) returns them,
//   `context.DeadlineExceeded` the writer's or the package's `DeadlineExceeded`, and the
//   writer's two errors of dropped data whatever numbers they carry; `Error::Wrap` is looked
//   through, as `errors.Is` looks through a wrapped error.
// - The media loss proxy's callback is set once the manager is made, after the transports;
//   upstream sets it first. Nothing reports loss before `new` returns.
//
// Who holds whom: the participant holds the manager. The manager holds its transports, its
// media loss proxy and, in its params, the participant's two handlers. Each transport holds
// its handler, a `TransportManagerTransportHandler`, as does the stream allocator of a
// sending transport (transport_impl.rs); that handler holds the participant's handler
// strongly and the manager weakly, and the media loss proxy's callback holds the manager
// weakly. A participant's handler that holds the participant strongly makes the cycle
// participant, manager, transport, handler, participant: it holds the participant weakly.
// `close` closes the transports, which blocks until their events workers have stopped: it
// is never called from a callback that runs on such a worker (`on_offer`, `on_answer`,
// `on_ice_candidate`, `on_unmatched_media`, `on_set_remote_description_offer`, and
// `on_negotiation_failed` from a failed event).

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::RwLock;

use crate::config;
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::mediatransportutil::twcc;
use crate::protocol::livekit;
use crate::protocol::logger::{Level, Logger};
use crate::rtc::transport::{self, NegotiationState};
use crate::rtc::types;
use crate::rtc::{
    BytesTrackStats, ClientInfo, DATA_TRACK_DATA_CHANNEL, Error, LOSSY_DATA_CHANNEL,
    MediaLossProxy, MediaLossProxyParams, PcTransport, RELIABLE_DATA_CHANNEL, RtcpFeedbackConfig,
    TransportParams, WebRtcConfig, logger_with_pc_target, maybe_truncate_ip,
};
use crate::sfu;
use crate::sfu::datachannel;
use crate::sfu::interceptor::SimulcastTrackInfo;
use crate::sfu::pacer;
use crate::sfu::streamallocator::StreamStateUpdate;
use crate::webrtc;
use crate::webrtc::peerconnection::{
    DataChannelInit, IceCandidate, IceCandidateInit, Node, RtpReceiver, RtpSender, RtpTransceiver,
    SessionDescription,
};
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;
use crate::webrtc::sdp;
use crate::{debugw, errorw, infow, warnw};

const FAILURE_COUNT_THRESHOLD: i64 = 2;
const PREFER_NEXT_BY_FAILURE_WINDOW: Duration = time::MINUTE;

// when RR report loss percentage over this threshold, we consider it is a unstable event
const UDP_LOSS_FRAC_UNSTABLE: u32 = 25;
// if in last 32 times RR, the unstable report count over this threshold, the connection is unstable
const UDP_LOSS_UNSTABLE_COUNT_THRESHOLD: u32 = 20;

// `PingTimeoutSeconds` of participant.go, which is not ported yet.
const PING_TIMEOUT_SECONDS: i64 = 15;

// -------------------------------

pub struct TransportManagerTransportHandler {
    // the embedded `transport.Handler`
    handler: Arc<dyn transport::Handler>,
    // `t`: the manager, weakly, once it is made (see the header)
    t: Arc<OnceLock<Weak<TransportManager>>>,
    logger: Logger,
}

impl transport::Handler for TransportManagerTransportHandler {
    fn on_failed(&self, is_short_lived: bool, ice_connection_info: &types::IceConnectionInfo) {
        if is_short_lived {
            log_with_fields(
                &self.logger,
                Level::Info,
                "short ice connection",
                connection_details_fields(std::slice::from_ref(ice_connection_info)),
            );
        }
        if let Some(t) = self.t.get().and_then(Weak::upgrade) {
            t.handle_connection_failed(is_short_lived);
        }
        self.handler.on_failed(is_short_lived, ice_connection_info);
    }

    // The methods of the embedded handler.

    fn on_ice_candidate(
        &self,
        c: Option<&IceCandidate>,
        target: livekit::SignalTarget,
    ) -> Result<(), Error> {
        self.handler.on_ice_candidate(c, target)
    }

    fn on_initial_connected(&self) {
        self.handler.on_initial_connected();
    }

    fn on_fully_established(&self) {
        self.handler.on_fully_established();
    }

    fn on_track(
        &self,
        track: Arc<dyn webrtc::TrackRemote>,
        rtp_receiver: Arc<dyn webrtc::RtpReceiver>,
    ) {
        self.handler.on_track(track, rtp_receiver);
    }

    fn on_data_message(&self, kind: livekit::data_packet::Kind, data: &[u8]) {
        self.handler.on_data_message(kind, data);
    }

    fn on_data_message_unlabeled(&self, data: &[u8]) {
        self.handler.on_data_message_unlabeled(data);
    }

    fn on_data_track_message(&self, data: &[u8], arrival_time: i64) {
        self.handler.on_data_track_message(data, arrival_time);
    }

    fn on_data_send_error(&self, err: Error) {
        self.handler.on_data_send_error(err);
    }

    fn on_offer(
        &self,
        sd: SessionDescription,
        offer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        self.handler.on_offer(sd, offer_id, mid_to_track_id)
    }

    fn on_set_remote_description_offer(&self) {
        self.handler.on_set_remote_description_offer();
    }

    fn on_answer(
        &self,
        sd: SessionDescription,
        answer_id: u32,
        mid_to_track_id: &HashMap<String, String>,
    ) -> Result<(), Error> {
        self.handler.on_answer(sd, answer_id, mid_to_track_id)
    }

    fn on_negotiation_state_changed(&self, state: NegotiationState) {
        self.handler.on_negotiation_state_changed(state);
    }

    fn on_negotiation_failed(&self) {
        self.handler.on_negotiation_failed();
    }

    fn on_stream_state_change(&self, update: &StreamStateUpdate) -> Result<(), Error> {
        self.handler.on_stream_state_change(update)
    }

    fn on_unmatched_media(&self, num_audios: u32, num_videos: u32) -> Result<(), Error> {
        self.handler.on_unmatched_media(num_audios, num_videos)
    }
}

// -------------------------------

#[derive(Clone)]
pub struct TransportManagerParams {
    pub subscriber_as_primary: bool,
    pub use_single_peer_connection: bool,
    pub config: Arc<WebRtcConfig>,
    pub twcc: Option<Arc<twcc::Responder>>,
    pub protocol_version: types::ProtocolVersion,
    pub congestion_control_config: config::CongestionControlConfig,
    pub enabled_subscribe_codecs: Vec<livekit::Codec>,
    pub enabled_publish_codecs: Vec<livekit::Codec>,
    pub sim_tracks: HashMap<u32, SimulcastTrackInfo>,
    pub client_info: ClientInfo,
    pub migration: bool,
    pub allow_tcp_fallback: bool,
    pub tcp_fallback_rtt_threshold: i64,
    pub allow_udp_unstable_fallback: bool,
    pub turns_enabled: bool,
    pub allow_playout_delay: bool,
    pub data_channel_max_buffered_amount: u64,
    pub datachannel_slow_threshold: i64,
    pub datachannel_lossy_target_latency: Duration,
    pub datachannel_data_track_target_latency: Duration,
    pub logger: Logger,
    pub publisher_handler: Arc<dyn transport::Handler>,
    pub subscriber_handler: Arc<dyn transport::Handler>,
    pub data_channel_stats: Arc<BytesTrackStats>,
    pub use_one_shot_signalling_mode: bool,
    pub fire_on_track_by_sdp: bool,
    pub enable_data_tracks: bool,
    pub exclude_ipv6_local_candidates: bool,
    pub enable_warp: bool,

    // New: the node the transports are on (see the header).
    pub node: Node,
}

pub struct TransportManager {
    params: TransportManagerParams,

    // what `lock sync.RWMutex` guards upstream
    lock: RwLock<TransportManagerInner>,

    publisher: Arc<PcTransport>,
    subscriber: Option<Arc<PcTransport>>,
    signal_source_valid: atomic::Bool,

    media_loss_proxy: Arc<MediaLossProxy>,

    data_channel_send_error_dropped_by_slow_reader_count: atomic::Uint32,
    data_channel_send_error_count: atomic::Uint32,
}

// What `lock` guards.
struct TransportManagerInner {
    failure_count: i64,
    is_transport_reconfigured: bool,
    last_failure: Time,
    last_signal_at: Time,

    pending_offer_publisher: Option<SessionDescription>,
    pending_offer_id_publisher: u32,
    pending_data_channels_publisher: Vec<livekit::DataChannelInfo>,
    ice_config: livekit::IceConfig,

    udp_loss_unstable_count: u32,
    signaling_rtt: u32,
    udp_rtt: u32,

    #[allow(clippy::type_complexity)]
    on_ice_config_changed: Option<Arc<dyn Fn(&livekit::IceConfig) + Send + Sync>>,
}

impl TransportManager {
    pub fn new(params: TransportManagerParams) -> Result<Arc<TransportManager>, Error> {
        // `if params.Logger == nil`: a `Logger` is never nil here
        let media_loss_proxy = MediaLossProxy::new(MediaLossProxyParams {
            logger: params.logger.clone(),
        });
        // `t`, as the transports' handlers hold it (see the header)
        let this = Arc::new(OnceLock::new());

        let lgr = logger_with_pc_target(&params.logger, livekit::SignalTarget::Publisher);
        // In single-PC mode the one PC carries both directions, so it needs both
        // codec lists registered on its MediaEngine. In dual-PC mode the publisher
        // PC is recvonly, so subscribe codecs are left nil.
        let mut publisher_subscribe_codecs = Vec::new();
        if params.use_single_peer_connection || params.use_one_shot_signalling_mode {
            publisher_subscribe_codecs = params.enabled_subscribe_codecs.clone();
        }
        let publisher = PcTransport::new(TransportParams {
            protocol_version: params.protocol_version,
            config: params.config.clone(),
            twcc: params.twcc.clone(),
            direction_config: params.config.publisher.clone(),
            congestion_control_config: params.congestion_control_config.clone(),
            enabled_publish_codecs: params.enabled_publish_codecs.clone(),
            enabled_subscribe_codecs: publisher_subscribe_codecs,
            logger: lgr.clone(),
            sim_tracks: params.sim_tracks.clone(),
            client_info: params.client_info.clone(),
            is_offerer: false,
            is_send_side: params.use_one_shot_signalling_mode || params.use_single_peer_connection,
            allow_playout_delay: params.allow_playout_delay,
            transport: livekit::SignalTarget::Publisher,
            handler: Arc::new(TransportManagerTransportHandler {
                handler: params.publisher_handler.clone(),
                t: this.clone(),
                logger: lgr,
            }),
            use_one_shot_signalling_mode: params.use_one_shot_signalling_mode,
            data_channel_max_buffered_amount: params.data_channel_max_buffered_amount,
            datachannel_slow_threshold: params.datachannel_slow_threshold,
            datachannel_lossy_target_latency: params.datachannel_lossy_target_latency,
            datachannel_data_track_target_latency: params.datachannel_data_track_target_latency,
            datachannel_max_receiver_buffer_size: 0,
            fire_on_track_by_sdp: params.fire_on_track_by_sdp,
            enable_data_tracks: params.enable_data_tracks,
            exclude_ipv6_local_candidates: params.exclude_ipv6_local_candidates,
            enable_warp: params.enable_warp,
            node: params.node.clone(),
        })?;

        let mut subscriber = None;
        if !params.use_one_shot_signalling_mode && !params.use_single_peer_connection {
            let lgr = logger_with_pc_target(&params.logger, livekit::SignalTarget::Subscriber);
            subscriber = Some(PcTransport::new(TransportParams {
                protocol_version: params.protocol_version,
                config: params.config.clone(),
                twcc: None,
                direction_config: params.config.subscriber.clone(),
                congestion_control_config: params.congestion_control_config.clone(),
                enabled_publish_codecs: Vec::new(),
                enabled_subscribe_codecs: params.enabled_subscribe_codecs.clone(),
                logger: lgr.clone(),
                sim_tracks: HashMap::new(),
                client_info: params.client_info.clone(),
                is_offerer: true,
                is_send_side: true,
                allow_playout_delay: params.allow_playout_delay,
                data_channel_max_buffered_amount: params.data_channel_max_buffered_amount,
                datachannel_slow_threshold: params.datachannel_slow_threshold,
                datachannel_lossy_target_latency: params.datachannel_lossy_target_latency,
                datachannel_data_track_target_latency: params.datachannel_data_track_target_latency,
                datachannel_max_receiver_buffer_size: 0,
                transport: livekit::SignalTarget::Subscriber,
                handler: Arc::new(TransportManagerTransportHandler {
                    handler: params.subscriber_handler.clone(),
                    t: this.clone(),
                    logger: lgr,
                }),
                use_one_shot_signalling_mode: false,
                fire_on_track_by_sdp: params.fire_on_track_by_sdp,
                enable_data_tracks: params.enable_data_tracks,
                exclude_ipv6_local_candidates: params.exclude_ipv6_local_candidates,
                enable_warp: params.enable_warp,
                node: params.node.clone(),
            })?);
        }

        let t = Arc::new(TransportManager {
            params,
            lock: RwLock::new(TransportManagerInner {
                failure_count: 0,
                is_transport_reconfigured: false,
                last_failure: Time::ZERO,
                last_signal_at: Time::ZERO,
                pending_offer_publisher: None,
                pending_offer_id_publisher: 0,
                pending_data_channels_publisher: Vec::new(),
                ice_config: livekit::IceConfig::default(),
                udp_loss_unstable_count: 0,
                signaling_rtt: 0,
                udp_rtt: 0,
                on_ice_config_changed: None,
            }),
            publisher,
            subscriber,
            signal_source_valid: atomic::Bool::new(false),
            media_loss_proxy,
            data_channel_send_error_dropped_by_slow_reader_count: atomic::Uint32::new(0),
            data_channel_send_error_count: atomic::Uint32::new(0),
        });
        let _ = this.set(Arc::downgrade(&t));
        {
            let weak = Arc::downgrade(&t);
            t.media_loss_proxy
                .on_media_loss_update(Some(Box::new(move |loss| {
                    if let Some(t) = weak.upgrade() {
                        t.on_media_loss_update(loss);
                    }
                })));
        }

        if t.use_negotiated_data_channels() {
            t.create_negotiated_data_channels_for_publisher()?;
        }
        if !t.params.migration && t.params.subscriber_as_primary {
            t.create_data_channels_for_subscriber(&[])?;
        }

        t.signal_source_valid.store(true);
        Ok(t)
    }

    // `t.subscriber` where the original calls it without checking for nil (see the header).
    fn subscriber(&self) -> &Arc<PcTransport> {
        self.subscriber
            .as_ref()
            .expect("no subscriber peer connection")
    }

    pub fn close(&self) {
        // `if t.publisher != nil`: it always is
        self.publisher.close();
        if let Some(subscriber) = &self.subscriber {
            subscriber.close();
        }
    }

    pub fn subscriber_close(&self) {
        self.subscriber().close();
    }

    pub fn has_publisher_ice_ever_connected(&self) -> bool {
        self.publisher.ice_has_ever_connected()
    }

    pub fn has_publisher_ever_connected(&self) -> bool {
        self.publisher.peer_connection_has_ever_connected()
    }

    pub fn publisher_first_connected_at(&self) -> Time {
        self.publisher.peer_connection_first_connected_at()
    }

    pub fn is_publisher_established(&self) -> bool {
        self.publisher.is_established()
    }

    pub fn get_publisher_rtt(&self) -> (f64, bool) {
        self.publisher.get_rtt()
    }

    pub fn get_publisher_mid(&self, rtp_receiver: &RtpReceiver) -> String {
        self.publisher.get_mid(rtp_receiver)
    }

    pub fn get_publisher_rtp_transceiver(&self, mid: &str) -> Option<Arc<RtpTransceiver>> {
        self.publisher.get_rtp_transceiver(mid)
    }

    pub fn get_publisher_rtp_receiver(&self, mid: &str) -> Option<Arc<RtpReceiver>> {
        self.publisher.get_rtp_receiver(mid)
    }

    pub fn write_publisher_rtcp(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), Error> {
        self.publisher.write_rtcp(pkts)
    }

    pub fn get_subscriber_rtt(&self) -> (f64, bool) {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.get_rtt()
        } else {
            self.subscriber().get_rtt()
        }
    }

    pub fn has_subscriber_ice_ever_connected(&self) -> bool {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.ice_has_ever_connected()
        } else {
            self.subscriber().ice_has_ever_connected()
        }
    }

    pub fn has_subscriber_ever_connected(&self) -> bool {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.peer_connection_has_ever_connected()
        } else {
            self.subscriber().peer_connection_has_ever_connected()
        }
    }

    pub fn subscriber_first_connected_at(&self) -> Time {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.peer_connection_first_connected_at()
        } else {
            self.subscriber().peer_connection_first_connected_at()
        }
    }

    pub fn add_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
        enabled_codecs: &[livekit::Codec],
        rtcp_feedback_config: &RtcpFeedbackConfig,
    ) -> Result<(Arc<RtpSender>, Arc<RtpTransceiver>), Error> {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher
                .add_track(track_local, params, enabled_codecs, rtcp_feedback_config)
        } else {
            self.subscriber()
                .add_track(track_local, params, enabled_codecs, rtcp_feedback_config)
        }
    }

    pub fn add_transceiver_from_track_local(
        &self,
        track_local: Arc<dyn webrtc::TrackLocal>,
        params: types::AddTrackParams,
        enabled_codecs: &[livekit::Codec],
        rtcp_feedback_config: &RtcpFeedbackConfig,
    ) -> Result<(Arc<RtpSender>, Arc<RtpTransceiver>), Error> {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.add_transceiver_from_track(
                track_local,
                params,
                enabled_codecs,
                rtcp_feedback_config,
            )
        } else {
            self.subscriber().add_transceiver_from_track(
                track_local,
                params,
                enabled_codecs,
                rtcp_feedback_config,
            )
        }
    }

    pub fn remove_track_local(&self, sender: &Arc<RtpSender>) -> Result<(), Error> {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.remove_track(sender)
        } else {
            self.subscriber().remove_track(sender)
        }
    }

    pub fn write_subscriber_rtcp(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), Error> {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.write_rtcp(pkts)
        } else {
            self.subscriber().write_rtcp(pkts)
        }
    }

    pub fn get_subscriber_pacer(&self) -> Option<Arc<dyn pacer::Pacer>> {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.get_pacer()
        } else {
            self.subscriber().get_pacer()
        }
    }

    pub fn add_subscribed_track(&self, sub_track: &dyn types::SubscribedTrack) {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.add_track_to_stream_allocator(sub_track);
        } else {
            self.subscriber().add_track_to_stream_allocator(sub_track);
        }
    }

    pub fn remove_subscribed_track(&self, sub_track: &dyn types::SubscribedTrack) {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher.remove_track_from_stream_allocator(sub_track);
        } else {
            self.subscriber()
                .remove_track_from_stream_allocator(sub_track);
        }
    }

    pub fn send_data_message(
        &self,
        kind: livekit::data_packet::Kind,
        data: &[u8],
    ) -> Result<(), Error> {
        // downstream data is sent via primary peer connection
        self.handle_send_data_result(
            self.get_transport(true).send_data_message(kind, data),
            kind.as_str_name(),
            data.len(),
        )
    }

    pub fn send_data_message_unlabeled(
        &self,
        data: &[u8],
        use_raw: bool,
        sender: &livekit::ParticipantIdentity,
    ) -> Result<(), Error> {
        // downstream data is sent via primary peer connection
        self.handle_send_data_result(
            self.get_transport(true)
                .send_data_message_unlabeled(data, use_raw, sender),
            "unlabeled",
            data.len(),
        )
    }

    fn handle_send_data_result(
        &self,
        res: Result<(), Error>,
        kind: &str,
        size: usize,
    ) -> Result<(), Error> {
        match &res {
            Err(err) => {
                if !(is_closed_pipe(err)
                    || is_stream_closed(err)
                    || matches!(cause(err), Error::TransportFailure)
                    || matches!(cause(err), Error::DataChannelUnavailable)
                    || is_deadline_exceeded(err)
                    || matches!(
                        cause(err),
                        Error::DataChannelWriter(
                            datachannel::Error::DataDroppedByHighBufferedAmount { .. }
                        )
                    ))
                {
                    if matches!(
                        cause(err),
                        Error::DataChannelWriter(
                            datachannel::Error::DataDroppedBySlowReader { .. }
                        )
                    ) {
                        let dropped_by_slow_reader_count = self
                            .data_channel_send_error_dropped_by_slow_reader_count
                            .inc();
                        if dropped_by_slow_reader_count
                            .wrapping_sub(1)
                            .is_multiple_of(100)
                        {
                            infow!(
                                self.params.logger,
                                "drop data message by slow reader",
                                "error" => err,
                                "kind" => kind,
                                "count" => dropped_by_slow_reader_count,
                            );
                        }
                    } else {
                        let count = self.data_channel_send_error_count.inc();
                        if count.wrapping_sub(1).is_multiple_of(100) {
                            infow!(
                                self.params.logger,
                                "send data message error",
                                "error" => err,
                                "kind" => kind,
                                "count" => count,
                            );
                        }
                    }
                }
                if is_stream_closed(err) || is_closed_pipe(err) {
                    if self.params.subscriber_as_primary {
                        self.params
                            .subscriber_handler
                            .on_data_send_error(err.clone());
                    } else {
                        self.params
                            .publisher_handler
                            .on_data_send_error(err.clone());
                    }
                }
            }
            Ok(()) => {
                self.params.data_channel_stats.add_bytes(size as u64, true);
            }
        }

        res
    }

    fn use_negotiated_data_channels(&self) -> bool {
        self.params
            .protocol_version
            .supports_negotiated_data_channels()
            && self.params.use_single_peer_connection
            && !self.params.use_one_shot_signalling_mode
    }

    fn create_negotiated_data_channels_for_publisher(&self) -> Result<(), Error> {
        // Migration recreates the publisher data channels as negotiated with the IDs the client reports
        // in ProcessPendingPublisherDataChannels instead.
        if self.params.migration {
            return Ok(());
        }

        let negotiated = true;

        let reliable_id = livekit::NegotiatedDataChannelId::NdciReliable as i32 as u16;
        let mut ordered = true;
        self.publisher.create_data_channel(
            RELIABLE_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(ordered),
                id: Some(reliable_id),
                negotiated: Some(negotiated),
                ..Default::default()
            }),
        )?;

        ordered = false;
        let retransmits: u16 = 0;
        let lossy_id = livekit::NegotiatedDataChannelId::NdciLossy as i32 as u16;
        self.publisher.create_data_channel(
            LOSSY_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(ordered),
                max_retransmits: Some(retransmits),
                id: Some(lossy_id),
                negotiated: Some(negotiated),
                ..Default::default()
            }),
        )?;

        let data_track_id = livekit::NegotiatedDataChannelId::NdciDataTrack as i32 as u16;
        self.publisher.create_data_channel(
            DATA_TRACK_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(ordered),
                max_retransmits: Some(retransmits),
                id: Some(data_track_id),
                negotiated: Some(negotiated),
                ..Default::default()
            }),
        )
    }

    fn create_data_channels_for_subscriber(
        &self,
        pending_data_channels: &[livekit::DataChannelInfo],
    ) -> Result<(), Error> {
        let mut reliable_id_ptr: Option<u16> = None;
        let mut lossy_id_ptr: Option<u16> = None;
        let mut data_track_id_ptr: Option<u16> = None;

        //
        // For old version migration clients, they don't send subscriber data channel info
        // so we need to create data channels with default ID and don't negotiate as client already has
        // data channels with default ID.
        //
        // For new version migration clients, we create data channels with new ID and negotiate with client
        //
        for dc in pending_data_channels {
            match dc.label.as_str() {
                RELIABLE_DATA_CHANNEL => {
                    // pion use step 2 for auto generated ID, so we need to add 6 to avoid conflict
                    reliable_id_ptr = Some((dc.id as u16).wrapping_add(6));
                }
                LOSSY_DATA_CHANNEL => {
                    lossy_id_ptr = Some((dc.id as u16).wrapping_add(6));
                }
                DATA_TRACK_DATA_CHANNEL => {
                    data_track_id_ptr = Some((dc.id as u16).wrapping_add(6));
                }
                _ => {}
            }
        }

        let mut ordered = true;
        let mut negotiated = self.params.migration && reliable_id_ptr.is_none();
        self.subscriber().create_data_channel(
            RELIABLE_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(ordered),
                id: reliable_id_ptr,
                negotiated: Some(negotiated),
                ..Default::default()
            }),
        )?;

        ordered = false;
        let retransmits: u16 = 0;
        negotiated = self.params.migration && lossy_id_ptr.is_none();
        self.subscriber().create_data_channel(
            LOSSY_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(ordered),
                max_retransmits: Some(retransmits),
                id: lossy_id_ptr,
                negotiated: Some(negotiated),
                ..Default::default()
            }),
        )?;

        negotiated = self.params.migration && data_track_id_ptr.is_none();
        self.subscriber().create_data_channel(
            DATA_TRACK_DATA_CHANNEL,
            Some(DataChannelInit {
                ordered: Some(ordered),
                max_retransmits: Some(retransmits),
                id: data_track_id_ptr,
                negotiated: Some(negotiated),
                ..Default::default()
            }),
        )?;

        Ok(())
    }

    pub fn get_unmatch_media_for_offer<'a>(
        &self,
        parsed_offer: &'a sdp::SessionDescription,
        media_type: &str,
    ) -> Result<Vec<&'a sdp::MediaDescription>, Error> {
        let mut unmatched = Vec::new();
        let mut last_matched_mid = "";
        let last_answer = self.publisher.current_local_description();
        let parsed_answer;
        if let Some(last_answer) = &last_answer {
            parsed_answer = match last_answer.unmarshal() {
                Ok(parsed_answer) => parsed_answer,
                Err(err1) => {
                    // should not happen
                    errorw!(self.params.logger, "failed to parse last answer", err1);
                    return Err(err1.into());
                }
            };

            for media in parsed_answer.media_descriptions.iter().rev() {
                if media.media_name.media == media_type {
                    last_matched_mid =
                        sdp::attribute(&media.attributes, sdp::ATTR_KEY_MID).unwrap_or("");
                    break;
                }
            }
        }

        for media in parsed_offer.media_descriptions.iter().rev() {
            if media.media_name.media == media_type {
                let mid = sdp::attribute(&media.attributes, sdp::ATTR_KEY_MID).unwrap_or("");
                if mid == last_matched_mid {
                    break;
                }
                unmatched.push(media);
            }
        }

        Ok(unmatched)
    }

    // LastPublisherOfferParsed returns the parsed form of the last offer set on the publisher peer connection
    pub fn last_publisher_offer_parsed(&self) -> Option<Arc<sdp::SessionDescription>> {
        self.publisher.remote_offer_parsed()
    }

    pub fn handle_offer(
        &self,
        offer: SessionDescription,
        offer_id: u32,
        should_pend: bool,
    ) -> Result<(), Error> {
        {
            let mut inner = self.lock.write();
            if should_pend {
                inner.pending_offer_publisher = Some(offer);
                inner.pending_offer_id_publisher = offer_id;
                return Ok(());
            }
        }

        self.publisher.handle_remote_description(offer, offer_id)
    }

    pub fn get_answer(&self) -> Result<(SessionDescription, u32), Error> {
        self.publisher.get_answer()
    }

    pub fn get_publisher_ice_session_ufrag(&self) -> Result<String, Error> {
        self.publisher.get_ice_session_ufrag()
    }

    pub fn handle_ice_trickle_sdp_fragment(&self, sdp_fragment: &str) -> Result<(), Error> {
        self.publisher.handle_ice_trickle_sdp_fragment(sdp_fragment)
    }

    pub fn handle_ice_restart_sdp_fragment(&self, sdp_fragment: &str) -> Result<String, Error> {
        self.publisher.handle_ice_restart_sdp_fragment(sdp_fragment)
    }

    pub fn process_pending_publisher_offer(&self) {
        let (pending_offer, pending_offer_id) = {
            let mut inner = self.lock.write();
            let pending_offer = inner.pending_offer_publisher.take();

            let pending_offer_id = inner.pending_offer_id_publisher;
            inner.pending_offer_id_publisher = 0;
            (pending_offer, pending_offer_id)
        };

        if let Some(pending_offer) = pending_offer {
            let _ = self.handle_offer(pending_offer, pending_offer_id, false);
        }
    }

    pub fn handle_answer(&self, answer: SessionDescription, answer_id: u32) {
        let Some(subscriber) = &self.subscriber else {
            // no subscriber transport in single-PC / one-shot signalling mode
            warnw!(
                self.params.logger,
                "answer received, but no subscriber peer connection",
                nil
            );
            return;
        };
        let _ = subscriber.handle_remote_description(answer, answer_id);
    }

    // AddICECandidate adds candidates for remote peer
    pub fn add_ice_candidate(&self, candidate: IceCandidateInit, target: livekit::SignalTarget) {
        // (no `default` arm: "ice candidate for unknown signal target", with the error
        // "unknown signal target", cannot be reached with the enum, see the header)
        match target {
            livekit::SignalTarget::Publisher => {
                // `if t.publisher != nil`: it always is ("ice candidate for publisher, but no
                // peer connection" otherwise)
                self.publisher.add_ice_candidate(candidate);
            }
            livekit::SignalTarget::Subscriber => {
                if let Some(subscriber) = &self.subscriber {
                    subscriber.add_ice_candidate(candidate);
                } else {
                    warnw!(self.params.logger, "ice candidate for subscriber, but no peer connection", nil, "candidate" => candidate);
                }
            }
        }
    }

    pub fn negotiate_subscriber(&self, force: bool) {
        if let Some(subscriber) = &self.subscriber {
            subscriber.negotiate(force);
        } else {
            self.publisher.negotiate(force);
        }
    }

    pub fn handle_client_reconnect(&self, reason: livekit::ReconnectReason) {
        let mut is_short = false;
        let mut duration = Duration::ZERO;
        let mut reset_short_connection = false;
        match reason {
            livekit::ReconnectReason::RrPublisherFailed => {
                // `if t.publisher != nil`: it always is
                reset_short_connection = true;
                (is_short, duration) = self.publisher.is_short_connection(time::now());
            }

            livekit::ReconnectReason::RrSubscriberFailed => {
                if let Some(subscriber) = &self.subscriber {
                    reset_short_connection = true;
                    (is_short, duration) = subscriber.is_short_connection(time::now());
                }
            }

            _ => {}
        }

        if is_short {
            self.lock.write().reset_transport_configure_locked(false);
            infow!(self.params.logger, "short connection by client ice restart", "duration" => duration, "reason" => reason);
            self.handle_connection_failed(is_short);
        }

        if reset_short_connection {
            // `if t.publisher != nil`: it always is
            self.publisher.reset_short_conn_on_ice_restart();
            if let Some(subscriber) = &self.subscriber {
                subscriber.reset_short_conn_on_ice_restart();
            }
        }
    }

    pub fn ice_restart(&self, ice_config: Option<&livekit::IceConfig>) -> Result<(), Error> {
        self.set_ice_config(ice_config);

        if let Some(subscriber) = &self.subscriber {
            return subscriber.ice_restart();
        }

        Ok(())
    }

    pub fn on_ice_config_changed(&self, f: Box<dyn Fn(&livekit::IceConfig) + Send + Sync>) {
        self.lock.write().on_ice_config_changed = Some(Arc::from(f));
    }

    pub fn set_ice_config(&self, ice_config: Option<&livekit::IceConfig>) {
        if let Some(ice_config) = ice_config {
            self.configure_ice(ice_config, true);
        }
    }

    pub fn get_ice_config(&self) -> Option<livekit::IceConfig> {
        let inner = self.lock.read();
        // `if t.iceConfig == nil`: it never is (see the header)
        Some(inner.ice_config)
    }

    fn configure_ice(&self, ice_config: &livekit::IceConfig, reset: bool) {
        let on_ice_config_changed = {
            let mut inner = self.lock.write();
            let is_equal = inner.ice_config == *ice_config;
            if reset || !is_equal {
                inner.reset_transport_configure_locked(!reset);
            }

            if is_equal {
                return;
            }

            infow!(self.params.logger, "setting ICE config", "iceConfig" => ice_config);
            let on_ice_config_changed = inner.on_ice_config_changed.clone();
            inner.ice_config = *ice_config;
            on_ice_config_changed
        };

        if ice_config.preference_subscriber != livekit::IceCandidateType::IctNone as i32 {
            self.media_loss_proxy.on_media_loss_update(None);
        }

        // The transport keeps the preference for TCP and arms its TCP ICE connect timer with
        // it; the connection layer has no ICE over TCP, so the candidates and the connection
        // stay as they are (see the header).
        // `if t.publisher != nil`: it always is
        self.publisher.set_prefer_tcp(
            ice_config.preference_publisher == livekit::IceCandidateType::IctTcp as i32,
        );
        if let Some(subscriber) = &self.subscriber {
            subscriber.set_prefer_tcp(
                ice_config.preference_subscriber == livekit::IceCandidateType::IctTcp as i32,
            );
        }

        if let Some(on_ice_config_changed) = on_ice_config_changed {
            on_ice_config_changed(ice_config);
        }
    }

    pub fn subscriber_as_primary(&self) -> bool {
        self.params.subscriber_as_primary
    }

    pub fn get_ice_connection_info(&self) -> Vec<types::IceConnectionInfo> {
        let mut infos = Vec::with_capacity(2);
        for pc in std::iter::once(&self.publisher).chain(self.subscriber.as_ref()) {
            let info = pc.get_ice_connection_info();
            if info.has_candidates() {
                infos.push(info);
            }
        }
        infos
    }

    pub fn get_data_track_transport(&self) -> Arc<dyn types::DataTrackTransport> {
        self.get_transport(true)
    }

    fn get_transport(&self, is_primary: bool) -> Arc<PcTransport> {
        // `case t.publisher == nil: return t.subscriber`: it never is
        match &self.subscriber {
            None => self.publisher.clone(),

            Some(subscriber) => {
                let mut pc_transport = &self.publisher;
                if (is_primary && self.params.subscriber_as_primary)
                    || (!is_primary && !self.params.subscriber_as_primary)
                {
                    pc_transport = subscriber;
                }

                pc_transport.clone()
            }
        }
    }

    fn get_lowest_priority_connection_type(&self) -> types::IceConnectionType {
        // `case t.publisher == nil: return t.subscriber.GetICEConnectionType()`: it never is
        match &self.subscriber {
            None => self.publisher.get_ice_connection_type(),

            Some(subscriber) => {
                let mut ctype = self.publisher.get_ice_connection_type();
                let stype = subscriber.get_ice_connection_type();
                if stype > ctype {
                    ctype = stype;
                }
                ctype
            }
        }
    }

    fn handle_connection_failed(&self, is_short_lived: bool) {
        if !self.params.allow_tcp_fallback || self.params.use_one_shot_signalling_mode {
            return;
        }

        let prefer_next = {
            let mut inner = self.lock.write();
            if inner.is_transport_reconfigured {
                return;
            }

            let signal_valid = self.signal_source_valid.load();
            if !inner.has_recent_signal_locked() || !signal_valid {
                // the failed might cause by network interrupt because signal closed or we have not seen any signal in the time window,
                // so don't switch to next candidate type
                debugw!(
                    self.params.logger,
                    "ignoring prefer candidate check by ICE failure because signal connection interrupted",
                    "sinceLastSignal" => time::since(inner.last_signal_at),
                    "signalValid" => signal_valid,
                );
                inner.failure_count = 0;
                inner.last_failure = Time::ZERO;
                return;
            }

            let lowest_priority_connection_type = self.get_lowest_priority_connection_type();

            //
            // Checking only `PreferenceSubscriber` field although any connection failure (PUBLISHER OR SUBSCRIBER) will
            // flow through here.
            //
            // As both transports are switched to the same type on any failure, checking just subscriber should be fine.
            //
            // (`ICECandidateType` as Go's int32: see the header)
            let get_next = |ic: &livekit::IceConfig, inner: &TransportManagerInner| -> i32 {
                match lowest_priority_connection_type {
                    types::IceConnectionType::Udp => {
                        // try ICE/TCP if ICE/UDP failed
                        if ic.preference_subscriber == livekit::IceCandidateType::IctNone as i32 {
                            if self.params.client_info.supports_ice_tcp()
                                && self.can_use_ice_tcp(inner)
                            {
                                return livekit::IceCandidateType::IctTcp as i32;
                            } else if self.params.turns_enabled {
                                // fallback to TURN/TLS if TCP is not supported
                                return livekit::IceCandidateType::IctTls as i32;
                            }
                        }
                    }

                    types::IceConnectionType::Tcp => {
                        // try TURN/TLS if ICE/TCP failed,
                        // the configuration could have been ICT_NONE or ICT_TCP,
                        // in either case, fallback to TURN/TLS
                        if self.params.turns_enabled {
                            return livekit::IceCandidateType::IctTls as i32;
                        } else {
                            // keep the current config
                            return ic.preference_subscriber;
                        }
                    }

                    types::IceConnectionType::Turn => {
                        // TURN/TLS is the most permissive option, if that fails there is nowhere to go to
                        // the configuration could have been ICT_NONE or ICT_TLS,
                        // keep the current config
                        return ic.preference_subscriber;
                    }

                    types::IceConnectionType::Unknown => {}
                }
                livekit::IceCandidateType::IctNone as i32
            };

            let prefer_next = if is_short_lived {
                get_next(&inner.ice_config, &inner)
            } else {
                inner.failure_count += 1;
                let last_failure = inner.last_failure;
                inner.last_failure = time::now();
                if inner.failure_count < FAILURE_COUNT_THRESHOLD
                    || time::since(last_failure) > PREFER_NEXT_BY_FAILURE_WINDOW
                {
                    return;
                }

                get_next(&inner.ice_config, &inner)
            };

            if prefer_next == inner.ice_config.preference_subscriber {
                return;
            }

            inner.is_transport_reconfigured = true;
            prefer_next
        };

        // What the preference makes of the connections: see `configure_ice`. A client told to
        // prefer TCP or TURN (`on_ice_config_changed`) gets only UDP host candidates here.
        match livekit::IceCandidateType::try_from(prefer_next) {
            Ok(livekit::IceCandidateType::IctTcp) => {
                debugw!(
                    self.params.logger,
                    "prefer TCP transport on both peer connections"
                );
            }

            Ok(livekit::IceCandidateType::IctTls) => {
                debugw!(
                    self.params.logger,
                    "prefer TLS transport both peer connections"
                );
            }

            Ok(livekit::IceCandidateType::IctNone) => {
                debugw!(
                    self.params.logger,
                    "allowing all transports on both peer connections"
                );
            }

            Err(_) => {}
        }

        // irrespective of which one fails, force prefer candidate on both as the other one might
        // fail at a different time and cause another disruption
        self.configure_ice(
            &livekit::IceConfig {
                preference_subscriber: prefer_next,
                preference_publisher: prefer_next,
            },
            false,
        );
    }

    pub fn set_migrate_info(
        &self,
        previous_offer: Option<&SessionDescription>,
        previous_answer: Option<&SessionDescription>,
        data_channels: &[livekit::DataChannelInfo],
    ) {
        let mut pending_data_channels_subscriber = Vec::with_capacity(data_channels.len());
        {
            let mut inner = self.lock.write();
            inner.pending_data_channels_publisher = Vec::with_capacity(data_channels.len());
            for dci in data_channels {
                if dci.target == livekit::SignalTarget::Subscriber as i32 {
                    pending_data_channels_subscriber.push(dci.clone());
                } else {
                    inner.pending_data_channels_publisher.push(dci.clone());
                }
            }
        }

        if self.params.subscriber_as_primary
            && let Err(err) =
                self.create_data_channels_for_subscriber(&pending_data_channels_subscriber)
        {
            errorw!(
                self.params.logger,
                "create subscriber data channels during migration failed",
                err
            );
        }

        if self.params.use_single_peer_connection {
            self.publisher
                .set_previous_sdp(previous_answer, previous_offer);
        } else {
            self.subscriber()
                .set_previous_sdp(previous_offer, previous_answer);
        }
    }

    pub fn process_pending_publisher_data_channels(&self) {
        let pending_data_channels =
            std::mem::take(&mut self.lock.write().pending_data_channels_publisher);

        // one for all the channels, as upstream: a reliable channel after a lossy one is
        // made unordered (see the report)
        let mut ordered = true;
        let negotiated = true;

        for ci in &pending_data_channels {
            // `dcLabel, dcID, dcExisting, err`, Go's zero values for a label of none of the
            // three
            let res = match ci.label.as_str() {
                RELIABLE_DATA_CHANNEL => {
                    let id = ci.id as u16;
                    self.publisher.create_data_channel_if_empty(
                        RELIABLE_DATA_CHANNEL,
                        Some(DataChannelInit {
                            ordered: Some(ordered),
                            negotiated: Some(negotiated),
                            id: Some(id),
                            ..Default::default()
                        }),
                    )
                }
                LOSSY_DATA_CHANNEL => {
                    ordered = false;
                    let retransmits: u16 = 0;
                    let id = ci.id as u16;
                    self.publisher.create_data_channel_if_empty(
                        LOSSY_DATA_CHANNEL,
                        Some(DataChannelInit {
                            ordered: Some(ordered),
                            max_retransmits: Some(retransmits),
                            negotiated: Some(negotiated),
                            id: Some(id),
                            ..Default::default()
                        }),
                    )
                }
                DATA_TRACK_DATA_CHANNEL => {
                    ordered = false;
                    let retransmits: u16 = 0;
                    let id = ci.id as u16;
                    self.publisher.create_data_channel_if_empty(
                        DATA_TRACK_DATA_CHANNEL,
                        Some(DataChannelInit {
                            ordered: Some(ordered),
                            max_retransmits: Some(retransmits),
                            negotiated: Some(negotiated),
                            id: Some(id),
                            ..Default::default()
                        }),
                    )
                }
                _ => Ok((String::new(), 0, false)),
            };
            match res {
                Err(err) => {
                    errorw!(self.params.logger, "create migrated data channel failed", err, "label" => ci.label);
                }
                Ok((dc_label, dc_id, true)) => {
                    debugw!(self.params.logger, "existing data channel during migration", "label" => dc_label, "id" => dc_id);
                }
                Ok((dc_label, dc_id, false)) => {
                    debugw!(self.params.logger, "create migrated data channel", "label" => dc_label, "id" => dc_id);
                }
            }
        }
    }

    pub fn handle_receiver_report(&self, dt: &sfu::DownTrack, report: &ReceiverReport) {
        self.media_loss_proxy.handle_max_loss_feedback(dt, report);
    }

    fn on_media_loss_update(&self, loss: u8) {
        if self.params.tcp_fallback_rtt_threshold == 0 || !self.params.allow_udp_unstable_fallback {
            return;
        }
        let mut inner = self.lock.write();
        inner.udp_loss_unstable_count <<= 1;
        if loss >= (255 * UDP_LOSS_FRAC_UNSTABLE / 100) as u8 {
            inner.udp_loss_unstable_count |= 1;
            if inner.udp_loss_unstable_count.count_ones() >= UDP_LOSS_UNSTABLE_COUNT_THRESHOLD
                && inner.udp_rtt > 0
                && inner.signaling_rtt < (inner.udp_rtt as f32 * 1.3) as u32
                && (inner.signaling_rtt as i64) < self.params.tcp_fallback_rtt_threshold
                && inner.has_recent_signal_locked()
            {
                inner.udp_loss_unstable_count = 0;
                let signaling_rtt = inner.signaling_rtt;
                drop(inner);

                infow!(self.params.logger, "udp connection unstable, switch to tcp", "signalingRTT" => signaling_rtt);
                // switch ICE preference to TCP before asking the client to resume,
                // the raw params handler only sends a leave request and does not
                // reconfigure the transport, so the client would reconnect over UDP
                // again and this path would keep firing
                self.handle_connection_failed(true);
                if self.params.use_single_peer_connection {
                    self.params
                        .publisher_handler
                        .on_failed(true, &self.publisher.get_ice_connection_info());
                } else {
                    self.params
                        .subscriber_handler
                        .on_failed(true, &self.subscriber().get_ice_connection_info());
                }
            }
        }
    }

    pub fn update_signaling_rtt(&self, rtt: u32) {
        self.lock.write().signaling_rtt = rtt;
        // `if t.publisher != nil`: it always is
        self.publisher.set_signaling_rtt(rtt);
        if let Some(subscriber) = &self.subscriber {
            subscriber.set_signaling_rtt(rtt);
        }

        // TODO: considering using tcp rtt to calculate ice connection cost, if ice connection can't be established
        // within 5 * tcp rtt(at least 5s), means udp traffic might be block/dropped, switch to tcp.
        // Currently, most cases reported is that ice connected but subsequent connection, so left the thinking for now.
    }

    pub fn update_media_rtt(&self, rtt: u32) {
        let mut inner = self.lock.write();
        if inner.udp_rtt == 0 {
            inner.udp_rtt = rtt;
        } else {
            inner.udp_rtt = (inner.udp_rtt as i64 + (rtt as i64 - inner.udp_rtt as i64) / 2) as u32;
        }
    }

    pub fn update_last_seen_signal(&self) {
        self.lock.write().last_signal_at = time::now();
    }

    pub fn since_last_signal(&self) -> Duration {
        time::since(self.lock.read().last_signal_at)
    }

    pub fn last_seen_signal_at(&self) -> Time {
        self.lock.read().last_signal_at
    }

    // `canUseICETCP`, which upstream calls with the lock held
    fn can_use_ice_tcp(&self, inner: &TransportManagerInner) -> bool {
        self.params.tcp_fallback_rtt_threshold == 0
            || (inner.signaling_rtt as i64) < self.params.tcp_fallback_rtt_threshold
    }

    pub fn set_signal_source_valid(&self, valid: bool) {
        self.signal_source_valid.store(valid);
        debugw!(self.params.logger, "signal source valid", "valid" => valid);
    }

    pub fn set_subscriber_allow_pause(&self, allow_pause: bool) {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher
                .set_allow_pause_of_stream_allocator(allow_pause);
        } else {
            self.subscriber()
                .set_allow_pause_of_stream_allocator(allow_pause);
        }
    }

    pub fn set_subscriber_channel_capacity(&self, channel_capacity: i64) {
        if self.params.use_one_shot_signalling_mode || self.params.use_single_peer_connection {
            self.publisher
                .set_channel_capacity_of_stream_allocator(channel_capacity);
        } else {
            self.subscriber()
                .set_channel_capacity_of_stream_allocator(channel_capacity);
        }
    }

    pub fn rtp_stream_published(&self, ssrc: u32, mid: &str, rid: &str) {
        self.publisher.rtp_stream_published(ssrc, mid, rid);
    }
}

impl TransportManagerInner {
    fn reset_transport_configure_locked(&mut self, reconfigured: bool) {
        self.failure_count = 0;
        self.is_transport_reconfigured = reconfigured;
        self.udp_loss_unstable_count = 0;
        self.last_failure = Time::ZERO;
    }

    fn has_recent_signal_locked(&self) -> bool {
        time::since(self.last_signal_at) < Duration::from_secs(PING_TIMEOUT_SECONDS)
    }
}

// What `errors.Is` finds of an error: what `Error::Wrap` wraps, as Go looks through a wrapped
// error (see the header).
fn cause(err: &Error) -> &Error {
    match err {
        Error::Wrap { cause: inner, .. } => cause(inner),
        err => err,
    }
}

// `errors.Is(err, io.ErrClosedPipe)`
fn is_closed_pipe(err: &Error) -> bool {
    matches!(
        cause(err),
        Error::Webrtc(webrtc::Error::ClosedPipe)
            | Error::DataChannelWriter(datachannel::Error::Channel(webrtc::Error::ClosedPipe))
    )
}

// `errors.Is(err, sctp.ErrStreamClosed)`
fn is_stream_closed(err: &Error) -> bool {
    matches!(
        cause(err),
        Error::Webrtc(webrtc::Error::StreamClosed)
            | Error::DataChannelWriter(datachannel::Error::Channel(webrtc::Error::StreamClosed))
    )
}

// `errors.Is(err, context.DeadlineExceeded)`
fn is_deadline_exceeded(err: &Error) -> bool {
    matches!(
        cause(err),
        Error::DeadlineExceeded | Error::DataChannelWriter(datachannel::Error::DeadlineExceeded)
    )
}

// `connectionDetailsFields` of room.go, which is not ported yet: the fields this file logs
// a short connection with.
fn connection_details_fields(
    infos: &[types::IceConnectionInfo],
) -> Vec<(String, Box<dyn fmt::Debug>)> {
    let mut fields: Vec<(String, Box<dyn fmt::Debug>)> = Vec::new();
    let mut connection_type = types::IceConnectionType::Unknown;
    for info in infos {
        let mut candidates = Vec::with_capacity(info.remote.len() + info.local.len());
        for c in &info.local {
            let mut c_str = "[local]".to_string();
            if c.selected_order != 0 {
                c_str += &format!("[selected:{}]", c.selected_order);
            } else if c.filtered {
                c_str += "[filtered]";
            }
            if c.trickle {
                c_str += "[trickle]";
            }
            c_str += &format!(" {}", c.candidate);
            candidates.push(c_str);
        }
        for c in &info.remote {
            let mut c_str = "[remote]".to_string();
            if c.selected_order != 0 {
                c_str += &format!("[selected:{}]", c.selected_order);
            } else if c.filtered {
                c_str += "[filtered]";
            }
            if c.trickle {
                c_str += "[trickle]";
            }
            c_str += &format!(
                " {} {} {}:{}",
                c.candidate.protocol,
                c.candidate.typ,
                maybe_truncate_ip(&c.candidate.address),
                c.candidate.port
            );
            if !c.candidate.related_address.is_empty() {
                let related_addr = maybe_truncate_ip(&c.candidate.related_address);
                if !related_addr.is_empty() {
                    c_str += &format!("  related {}:{}", related_addr, c.candidate.related_port);
                }
            }
            candidates.push(c_str);
        }
        if !candidates.is_empty() {
            fields.push((
                format!("{}Candidates", info.transport.as_str_name().to_lowercase()),
                Box::new(candidates),
            ));
        }
        if info.r#type != types::IceConnectionType::Unknown {
            connection_type = info.r#type;
        }
    }
    fields.push(("connectionType".to_string(), Box::new(connection_type)));
    fields
}

// `logger.Infow(msg, fields...)` with fields made at run time.
fn log_with_fields(
    logger: &Logger,
    level: Level,
    msg: &str,
    fields: Vec<(String, Box<dyn fmt::Debug>)>,
) {
    if !logger.enabled(level) {
        return;
    }
    let kvs: Vec<(&str, &dyn fmt::Debug)> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_ref() as &dyn fmt::Debug))
        .collect();
    logger.log(level, msg, None, &kvs);
}

// `*PCTransport` is a `types.DataTrackTransport` upstream by having the method (see the
// header).
impl types::DataTrackTransport for PcTransport {
    fn send_data_track_message(&self, data: &[u8]) -> Result<(), Error> {
        PcTransport::send_data_track_message(self, data)
    }
}

#[cfg(test)]
#[path = "transportmanager_test.rs"]
mod transportmanager_test;
