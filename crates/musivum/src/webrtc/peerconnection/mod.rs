//! musivum's peer connection: what pion's `webrtc.PeerConnection` is to LiveKit, on top of the
//! sans-I/O library `rtc` (`docs/webrtc-library.md`).
//!
//! `rtc` has no sockets, no timers and no callbacks: one `RTCPeerConnection` that is fed
//! datagrams and polled for what it has to say. This module is the layer between that and
//! the ported code, which was written against pion: methods that can be called from any
//! thread, events as callbacks, a track that is bound and then written to, and a door through
//! which every received packet comes out ([`PacketSink`], pion's buffer factory).
//!
//! # How it runs
//!
//! - A [`Node`] has one UDP socket for all its connections. One task reads it and gives each
//!   datagram to the connection it belongs to, through a queue per connection. A node may
//!   also have a TCP listener ([`Node::new_with_tcp`]): a stream is given to the connection
//!   its first binding request names, and its packets go through the same queue.
//! - Every [`PeerConnection`] has a task of its own on `crate::go::time::runtime()`. It
//!   takes the datagrams from the queue, drives the timers of `rtc`, and makes all the calls
//!   out of the connection: the callbacks set with `on_...`, the sink, `bind` and `unbind` of
//!   local tracks. So what one connection reports comes one at a time and in the order it
//!   happened, and never from a thread that is inside one of the connection's methods.
//! - The `RTCPeerConnection` is behind one mutex. A method takes it, does its work, sends
//!   the datagrams that came of it, and lets go; what there is to report is put in a queue
//!   for the task. No callback runs while the mutex is held, so a callback may call anything,
//!   on this connection or on another.
//! - A write (`write_rtp`, `write_rtcp`, a data channel's `send`) has pushed its datagrams
//!   into the socket when it returns, as pion's has. One the socket does not take is lost.
//!
//! # What differs from pion
//!
//! - A connection has to be closed ([`PeerConnection::close`]); dropping the last handle
//!   closes it too, unless a callback holds a handle.
//! - Gathering is over at once: the candidates are the node's addresses, or the addresses
//!   the connection was given in their place ([`SettingEngine::host_candidate_ips`]), over
//!   UDP and, on a node with a TCP listener, as passive TCP candidates; active TCP
//!   candidates only where the settings ask for them ([`SettingEngine::active_tcp`]). ICE
//!   servers are taken ([`Configuration::ice_servers`]) and nothing is asked of them: a
//!   connection has no server reflexive and no relayed candidates of its own.
//! - `on_track` comes when the first packet of a stream has arrived (and after that packet
//!   has gone to the sink), never from the description alone.
//! - Received media is not read from a track: it comes out at the sink, as it does for
//!   LiveKit through pion's buffer factory.
//! - A local description has to be set as it was created: `rtc` compares the text.
//! - A track is bound and unbound from the connection's task, also when it takes the place
//!   of another ([`RtpSender::replace_track`]): the call that caused it does not wait for
//!   it, and an error of `bind` is logged and not returned.
//! - The sink is told of a stream the connection sends when its track is bound and when it
//!   is unbound ([`PacketSink::bind_local_stream`]); pion tells its interceptors when a
//!   sender starts and when it stops, and nothing when a track is replaced.
//!
//! # What `rtc` 0.21.1 does not do
//!
//! - A retransmission stream that is known only by the layer it names (the
//!   `repaired-rtp-stream-id` extension, as with simulcast) is not paired with its stream:
//!   its packets come out at the sink, but no `bind_remote_stream` announces it and
//!   `TrackRemote::rtx_ssrc` stays 0.
//! - Data channels have to be in the first negotiation. One that is added to a connection
//!   that started without any never opens: the SCTP association is only begun when the DTLS
//!   handshake completes.
//! - Of a data channel the other side opened, how often or how long it retransmits is not
//!   told.
//! - The states of the connection are told as events only, and the ICE transport is reached
//!   through a sender, a receiver or the SCTP transport only. This layer follows the states.
//! - That the selected candidate pair changed is an event inside `rtc` only. The layer asks
//!   for the pair where it can have changed and tells when it has
//!   ([`PeerConnection::on_selected_candidate_pair_change`], which says how). The numbers of
//!   the pair are in [`PeerConnection::get_stats`] (a round-trip time only where the ICE
//!   agent sends checks of its own, which an ICE-lite one does not).
//! - An ICE agent selects a pair once and keeps it until an ICE restart or a failure; pion's
//!   moves to a better pair that is nominated later.

mod convert;
mod datachannel;
mod media;
mod node;
mod settings;
mod tap;
mod tcp;

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Weak};
use std::time::Instant;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rtc::data_channel::RTCDataChannelId;
use rtc::interceptor::{Registry, Slot};
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent};
use rtc::peer_connection::message::{RTCMessage, TaggedRTCMessage};
use rtc::peer_connection::transport::{CandidateConfig, CandidateHostConfig};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtp_transceiver::rtp_sender::RTCRtpHeaderExtensionCapability;
use rtc::rtp_transceiver::{RTCRtpReceiverId, RTCRtpSenderId, RTCRtpTransceiverInit};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtc::statistics::StatsSelector;
use tokio::sync::{Notify, mpsc};

use crate::go::time::Time;
use crate::protocol::logger::Logger;
use crate::webrtc::rtcp;
use crate::webrtc::rtp;
use crate::webrtc::{
    Error, PacketSink, RtpCodecParameters, RtpCodecType, RtpParameters, StreamInfo, TrackLocal,
    TrackLocalContext,
};
use crate::{debugw, warnw};

use self::media::SenderContext;
use self::node::{ConnectionGuard, Datagram};
use self::tap::{Captured, Inbox, Tap};

pub use self::datachannel::{DataChannel, DataChannelInit, DataChannelMessage};
pub use self::media::{RtpReceiver, RtpSender, RtpTransceiver, RtpTransceiverInit, TrackRemote};
pub use self::node::{Node, NodeConfig, NodeStats};
pub use self::settings::SettingEngine;

/// pion's `DataChannelState`.
pub use rtc::data_channel::RTCDataChannelState as DataChannelState;
/// pion's `OfferOptions`: `ice_restart`.
pub use rtc::peer_connection::configuration::RTCOfferOptions as OfferOptions;
/// pion's `MediaEngine`: the codecs and header extensions a connection offers and accepts.
/// It is `rtc`'s, which has pion's shape; [`register_codec`] and
/// [`register_header_extension`] fill it from musivum's descriptions.
pub use rtc::peer_connection::configuration::media_engine::MediaEngine;
/// pion's `SDPType`.
pub use rtc::peer_connection::sdp::RTCSdpType as SdpType;
/// pion's `ICEConnectionState`.
pub use rtc::peer_connection::state::RTCIceConnectionState as IceConnectionState;
/// pion's `ICEGatheringState`.
pub use rtc::peer_connection::state::RTCIceGatheringState as IceGatheringState;
/// pion's `PeerConnectionState`.
pub use rtc::peer_connection::state::RTCPeerConnectionState as PeerConnectionState;
/// pion's `SignalingState`.
pub use rtc::peer_connection::state::RTCSignalingState as SignalingState;
/// pion's `DTLSRole`.
pub use rtc::peer_connection::transport::RTCDtlsRole as DtlsRole;
/// pion's `ICECandidate`; `to_json` gives the [`IceCandidateInit`] that is signalled.
pub use rtc::peer_connection::transport::RTCIceCandidate as IceCandidate;
/// pion's `ICECandidateInit`.
pub use rtc::peer_connection::transport::RTCIceCandidateInit as IceCandidateInit;
/// pion's `ICECandidatePair`: `local()` and `remote()`.
pub use rtc::peer_connection::transport::RTCIceCandidatePair as IceCandidatePair;
/// pion's `ICECandidateType`.
pub use rtc::peer_connection::transport::RTCIceCandidateType as IceCandidateType;
/// pion's `ICEProtocol`.
pub use rtc::peer_connection::transport::RTCIceProtocol as IceProtocol;
/// pion's `ICEServer`; the credential is a string, and there is no type of credential.
pub use rtc::peer_connection::transport::RTCIceServer as IceServer;
/// pion's `ICETCPCandidateType`.
pub use rtc::peer_connection::transport::RTCIceTcpCandidateType as IceTcpCandidateType;
/// pion's `RTPTransceiverDirection`.
pub use rtc::rtp_transceiver::RTCRtpTransceiverDirection as RtpTransceiverDirection;
/// pion's `StatsReport`, as `rtc` has it: `transport()` names the selected candidate pair
/// (`selected_candidate_pair_id`), `get(id)` gives its entry.
pub use rtc::statistics::report::RTCStatsReport as StatsReport;
/// One entry of a [`StatsReport`]: pion's `Stats`, an enum where pion has an interface.
pub use rtc::statistics::report::RTCStatsReportEntry as StatsReportEntry;
/// pion's `ICECandidateStats`.
pub use rtc::statistics::stats::ice_candidate::RTCIceCandidateStats as IceCandidateStats;
/// pion's `ICECandidatePairStats`.
pub use rtc::statistics::stats::ice_candidate_pair::RTCIceCandidatePairStats as IceCandidatePairStats;

/// pion's `ice.MulticastDNSMode`, which its `SettingEngine` takes.
pub use crate::webrtc::ice::MulticastDnsMode;
/// pion's `NetworkType`. It is the ICE library's, which pion's `webrtc` package has a copy
/// of.
pub use crate::webrtc::ice::NetworkType;

use crate::webrtc::ice::TcpType;

/// What a connection is made from: pion's `webrtc.NewAPI(WithMediaEngine, WithSettingEngine)`
/// and `Configuration` in one. A certificate is made for each connection.
#[derive(Default)]
pub struct Configuration {
    pub media_engine: MediaEngine,
    /// The settings of this connection. LiveKit copies its engine for every connection and
    /// changes the copy; this is that copy.
    pub setting_engine: SettingEngine,
    /// pion's `Configuration.ICEServers`. The URLs are checked when the connection is made
    /// and kept ([`PeerConnection::ice_servers`]). Nothing is gathered from them: the
    /// connection's candidates are host candidates on the node's socket.
    pub ice_servers: Vec<IceServer>,
    /// Where received packets go. It can also be set later with
    /// [`PeerConnection::set_packet_sink`], but what arrives before is lost.
    pub packet_sink: Option<Arc<dyn PacketSink>>,
    pub logger: Logger,
}

/// pion's `SessionDescription`: the type and the SDP as text.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionDescription {
    pub r#type: SdpType,
    pub sdp: String,
}

/// pion's `MediaEngine.RegisterCodec`, from musivum's description of a codec.
pub fn register_codec(
    media_engine: &mut MediaEngine,
    codec: &RtpCodecParameters,
    typ: RtpCodecType,
) -> Result<(), Error> {
    media_engine.register_codec(
        convert::codec_parameters_to_rtc(codec),
        convert::codec_type_to_rtc(typ),
    )?;
    Ok(())
}

/// pion's `MediaEngine.RegisterHeaderExtension`. `allowed_direction` limits the extension to
/// what is sent or to what is received; `None` allows both.
pub fn register_header_extension(
    media_engine: &mut MediaEngine,
    uri: &str,
    typ: RtpCodecType,
    allowed_direction: Option<RtpTransceiverDirection>,
) -> Result<(), Error> {
    media_engine.register_header_extension(
        RTCRtpHeaderExtensionCapability {
            uri: uri.to_string(),
        },
        convert::codec_type_to_rtc(typ),
        allowed_direction,
    )?;
    Ok(())
}

type Handler<T> = Option<Arc<dyn Fn(T) + Send + Sync>>;
type TrackHandler = Option<Arc<dyn Fn(Arc<TrackRemote>, Arc<RtpReceiver>) + Send + Sync>>;

/// The callbacks of a connection, pion's `On...` handlers.
#[derive(Default)]
struct Handlers {
    ice_candidate: Handler<Option<IceCandidate>>,
    ice_connection_state: Handler<IceConnectionState>,
    selected_candidate_pair: Handler<IceCandidatePair>,
    ice_gathering_state: Handler<IceGatheringState>,
    connection_state: Handler<PeerConnectionState>,
    signaling_state: Handler<SignalingState>,
    negotiation_needed: Option<Arc<dyn Fn() + Send + Sync>>,
    track: TrackHandler,
    data_channel: Handler<Arc<DataChannel>>,
}

/// Something the connection has to tell: made while the connection's mutex is held, told by
/// the connection's task after it was let go.
pub(crate) enum Delivery {
    Rtp {
        ssrc: u32,
        packet: Bytes,
        at: Time,
    },
    Rtcp {
        packets: Vec<Box<dyn rtcp::Packet>>,
        at: Time,
    },
    BindRemoteStream(StreamInfo),
    UnbindRemoteStream(StreamInfo),
    IceCandidate(Option<IceCandidate>),
    IceConnectionState(IceConnectionState),
    SelectedCandidatePair(IceCandidatePair),
    IceGatheringState(IceGatheringState),
    ConnectionState(PeerConnectionState),
    SignalingState(SignalingState),
    NegotiationNeeded,
    Track(Arc<TrackRemote>, Arc<RtpReceiver>),
    DataChannel(Arc<DataChannel>),
    DataChannelOpen(Arc<DataChannel>),
    DataChannelMessage(Arc<DataChannel>, DataChannelMessage),
    DataChannelBufferedAmountLow(Arc<DataChannel>),
    DataChannelClose(Arc<DataChannel>),
    Bind {
        track: Arc<dyn TrackLocal>,
        context: Arc<SenderContext>,
    },
    Unbind {
        track: Arc<dyn TrackLocal>,
        context: Arc<SenderContext>,
    },
    /// Nothing to tell: a track that was never bound is let go of.
    Release(Arc<dyn TrackLocal>),
}

/// What the connection's mutex guards: `rtc`'s connection and what this layer knows about
/// it.
pub(crate) struct Core {
    pub(crate) rtc: RTCPeerConnection,
    inbox: Inbox,
    pub(crate) transceivers: Vec<Arc<RtpTransceiver>>,
    pub(crate) data_channels: HashMap<RTCDataChannelId, Arc<DataChannel>>,

    // `rtc` tells its states as events only; these follow them.
    signaling_state: SignalingState,
    ice_connection_state: IceConnectionState,
    ice_gathering_state: IceGatheringState,
    connection_state: PeerConnectionState,

    // The candidate pair that was told last, none after a restart or a failure; and whether
    // something happened since that can have changed it (see `note_selected_pair`).
    selected_pair: Option<IceCandidatePair>,
    selected_pair_in_doubt: bool,

    // The connection's candidates, once they were given to `rtc`.
    local_candidates: Vec<IceCandidate>,
    candidates_added: bool,
    local_ufrag: String,

    // Media can be written: the DTLS handshake is done and the connection is not closed.
    media_open: bool,
    pub(crate) closed: bool,

    // What there is to tell, in order; moved to `Inner::pending` before the mutex is let go.
    pub(crate) out: Vec<Delivery>,
}

impl Core {
    pub(crate) fn check_open(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::ConnectionClosed)
        } else {
            Ok(())
        }
    }
}

pub(crate) struct Inner {
    id: u64,
    node: Node,
    pub(crate) logger: Logger,
    pub(crate) this: Weak<Inner>,
    // The addresses this connection advertises: the node's, or its own.
    host_candidates: Vec<SocketAddr>,
    // The same with the port of the node's TCP listener: its passive TCP candidates. Empty
    // when the node has no TCP listener or the settings leave TCP out.
    tcp_host_candidates: Vec<SocketAddr>,
    // Whether the connection has active TCP candidates, and dials.
    active_tcp: bool,
    core: Mutex<Core>,
    // Filled with the connection's mutex held, emptied by the connection's task.
    pending: Mutex<VecDeque<Delivery>>,
    wake: Notify,
    handlers: Mutex<Handlers>,
    sink: RwLock<Option<Arc<dyn PacketSink>>>,
}

/// Closes the connection when the last handle is dropped.
struct Shared {
    inner: Arc<Inner>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.inner.close();
    }
}

/// pion's `PeerConnection`, as far as LiveKit uses it. Cheap to clone; all clones are the same
/// connection. See the module's description for what runs where.
#[derive(Clone)]
pub struct PeerConnection {
    shared: Arc<Shared>,
}

impl PeerConnection {
    /// pion's `API.NewPeerConnection`. The connection uses the socket of `node` and counts as
    /// one of its connections until it is closed.
    pub fn new(node: &Node, config: Configuration) -> Result<PeerConnection, Error> {
        let inbox = Inbox::default();
        // None of the library's interceptors: retransmission, feedback and reports are the
        // ported code's. Only the tap, closest to the wire.
        let registry = Registry::new().with(Slot::Custom(100), Tap::new(inbox.clone()));
        // `rtc` reads the URLs of the servers here and refuses those it cannot read.
        let rtc = RTCPeerConnectionBuilder::new()
            .with_configuration(
                RTCConfigurationBuilder::new()
                    .with_ice_servers(config.ice_servers)
                    .build(),
            )
            .with_setting_engine(config.setting_engine.to_rtc())
            .with_media_engine(config.media_engine)
            .with_interceptor_registry(registry)
            .build(Instant::now())?;

        let host_candidates = match &config.setting_engine.host_candidate_ips {
            Some(ips) => {
                let candidates = node.candidates_for(ips);
                if candidates.len() < ips.len() {
                    warnw!(config.logger, "not every address of the connection can be advertised", nil,
                        "addresses" => ips,
                        "candidates" => candidates);
                }
                candidates
            }
            None => node.candidates(),
        };
        let tcp = tcp_allowed(&config.setting_engine.network_types);
        let tcp_host_candidates = match &config.setting_engine.host_candidate_ips {
            _ if !tcp => Vec::new(),
            Some(ips) => node.tcp_candidates_for(ips),
            None => node.tcp_candidates(),
        };
        let active_tcp = tcp && config.setting_engine.active_tcp;

        let (id, datagrams, guard) = node.register(active_tcp);
        let inner = Arc::new_cyclic(|this| Inner {
            id,
            node: node.clone(),
            logger: config.logger,
            this: this.clone(),
            host_candidates,
            tcp_host_candidates,
            active_tcp,
            core: Mutex::new(Core {
                rtc,
                inbox,
                transceivers: Vec::new(),
                data_channels: HashMap::new(),
                signaling_state: SignalingState::Stable,
                ice_connection_state: IceConnectionState::New,
                ice_gathering_state: IceGatheringState::New,
                connection_state: PeerConnectionState::New,
                selected_pair: None,
                selected_pair_in_doubt: false,
                local_candidates: Vec::new(),
                candidates_added: false,
                local_ufrag: String::new(),
                media_open: false,
                closed: false,
                out: Vec::new(),
            }),
            pending: Mutex::new(VecDeque::new()),
            wake: Notify::new(),
            handlers: Mutex::new(Handlers::default()),
            sink: RwLock::new(config.packet_sink),
        });
        crate::go::time::runtime().spawn(run(inner.clone(), datagrams, guard));
        Ok(PeerConnection {
            shared: Arc::new(Shared { inner }),
        })
    }

    fn inner(&self) -> &Arc<Inner> {
        &self.shared.inner
    }

    // Negotiation

    /// CreateOffer starts the PeerConnection and generates the localDescription.
    pub fn create_offer(&self, options: Option<OfferOptions>) -> Result<SessionDescription, Error> {
        self.inner().act(|inner, core, _| {
            core.check_open()?;
            let offer = core.rtc.create_offer(options)?;
            inner.sync_transceivers(core);
            Ok(convert::session_description_from_rtc(&offer))
        })
    }

    /// CreateAnswer starts the PeerConnection and generates the localDescription.
    pub fn create_answer(&self) -> Result<SessionDescription, Error> {
        self.inner().act(|_, core, _| {
            core.check_open()?;
            let answer = core.rtc.create_answer(None)?;
            Ok(convert::session_description_from_rtc(&answer))
        })
    }

    /// SetLocalDescription sets the SessionDescription of the local peer.
    ///
    /// The description has to be the one `create_offer` or `create_answer` gave last, as
    /// text too: `rtc` compares.
    pub fn set_local_description(&self, desc: SessionDescription) -> Result<(), Error> {
        let parsed = convert::session_description_to_rtc(&desc)?;
        self.inner().act(|inner, core, now| {
            core.check_open()?;
            core.rtc.set_local_description(now, parsed)?;
            inner.after_local_description(core)
        })
    }

    /// SetRemoteDescription sets the SessionDescription of the remote peer.
    pub fn set_remote_description(&self, desc: SessionDescription) -> Result<(), Error> {
        let parsed = convert::session_description_to_rtc(&desc)?;
        self.inner().act(|inner, core, now| {
            core.check_open()?;
            core.rtc.set_remote_description(now, parsed)?;
            inner.sync_transceivers(core);
            let kinds = media_kinds(&desc.sdp);
            for transceiver in &core.transceivers {
                if transceiver.kind_hint() != RtpCodecType::Unknown {
                    continue;
                }
                let mid = core
                    .rtc
                    .rtp_transceiver(transceiver.index())
                    .and_then(|t| t.mid().clone());
                if let Some((_, kind)) = kinds.iter().find(|(m, _)| Some(m) == mid.as_ref()) {
                    transceiver.set_kind(*kind);
                }
            }
            Ok(())
        })
    }

    /// LocalDescription returns PendingLocalDescription if it is not null and
    /// otherwise it returns CurrentLocalDescription. This property is used to
    /// determine if SetLocalDescription has already been called.
    ///
    /// The node's candidates are in it.
    pub fn local_description(&self) -> Option<SessionDescription> {
        self.inner().read(|core| {
            core.rtc
                .local_description()
                .map(|d| convert::session_description_from_rtc(&d))
        })
    }

    /// CurrentLocalDescription represents the local description that was
    /// successfully negotiated the last time the PeerConnection transitioned
    /// into the stable state plus any local candidates that have been generated
    /// by the ICEAgent since the offer or answer was created.
    pub fn current_local_description(&self) -> Option<SessionDescription> {
        self.inner().read(|core| {
            core.rtc
                .current_local_description()
                .map(|d| convert::session_description_from_rtc(&d))
        })
    }

    /// PendingLocalDescription represents a local description that is in the
    /// process of being negotiated plus any local candidates that have been
    /// generated by the ICEAgent since the offer or answer was created. If the
    /// PeerConnection is in the stable state, the value is null.
    pub fn pending_local_description(&self) -> Option<SessionDescription> {
        self.inner().read(|core| {
            core.rtc
                .pending_local_description()
                .map(|d| convert::session_description_from_rtc(&d))
        })
    }

    /// RemoteDescription returns pendingRemoteDescription if it is not null and
    /// otherwise it returns currentRemoteDescription. This property is used to
    /// determine if setRemoteDescription has already been called.
    pub fn remote_description(&self) -> Option<SessionDescription> {
        self.inner().read(|core| {
            core.rtc
                .remote_description()
                .map(convert::session_description_from_rtc)
        })
    }

    /// CurrentRemoteDescription represents the last remote description that was
    /// successfully negotiated the last time the PeerConnection transitioned
    /// into the stable state plus any remote candidates that have been supplied
    /// via AddICECandidate() since the offer or answer was created.
    pub fn current_remote_description(&self) -> Option<SessionDescription> {
        self.inner().read(|core| {
            core.rtc
                .current_remote_description()
                .map(convert::session_description_from_rtc)
        })
    }

    /// PendingRemoteDescription represents a remote description that is in the
    /// process of being negotiated, complete with any remote candidates that
    /// have been supplied via AddICECandidate() since the offer or answer was
    /// created. If the PeerConnection is in the stable state, the value is
    /// null.
    pub fn pending_remote_description(&self) -> Option<SessionDescription> {
        self.inner().read(|core| {
            core.rtc
                .pending_remote_description()
                .map(convert::session_description_from_rtc)
        })
    }

    /// AddICECandidate accepts an ICE candidate string and adds it
    /// to the existing set of candidates.
    pub fn add_ice_candidate(&self, candidate: IceCandidateInit) -> Result<(), Error> {
        self.inner().act(|_, core, _| {
            core.check_open()?;
            core.rtc.add_remote_candidate(candidate)?;
            Ok(())
        })
    }

    /// Asks for new ICE credentials in the next offer, as `create_offer` with
    /// `ice_restart` does (the W3C's `restartIce`).
    pub fn restart_ice(&self) {
        self.inner().act(|_, core, _| core.rtc.restart_ice());
    }

    /// SignalingState attribute returns the signaling state of the
    /// PeerConnection instance.
    pub fn signaling_state(&self) -> SignalingState {
        self.inner().read(|core| core.signaling_state)
    }

    /// ICEConnectionState returns the ICE connection state of the
    /// PeerConnection instance.
    pub fn ice_connection_state(&self) -> IceConnectionState {
        self.inner().read(|core| core.ice_connection_state)
    }

    /// ICEGatheringState attribute returns the ICE gathering state of the
    /// PeerConnection instance.
    pub fn ice_gathering_state(&self) -> IceGatheringState {
        self.inner().read(|core| core.ice_gathering_state)
    }

    /// ConnectionState attribute returns the connection state of the
    /// PeerConnection instance.
    pub fn connection_state(&self) -> PeerConnectionState {
        self.inner().read(|core| core.connection_state)
    }

    /// pion's `ICETransport.GetSelectedCandidatePair`: the pair of candidates the ICE agent
    /// sends on, `None` while it has none (before it connects, after a restart or a failure
    /// until it has connected again, and when the connection is closed).
    ///
    /// The local candidate of a connection on a node that advertises several addresses is the
    /// first of them, whichever the client sent to: the node has one socket for them all.
    pub fn get_selected_candidate_pair(&self) -> Option<IceCandidatePair> {
        self.inner()
            .read(|core| selected_candidate_pair(core).flatten())
    }

    /// pion's `GetConfiguration().ICEServers`: the servers the connection was made with.
    pub fn ice_servers(&self) -> Vec<IceServer> {
        self.inner()
            .read(|core| core.rtc.get_configuration().ice_servers().to_vec())
    }

    /// Close ends the PeerConnection.
    ///
    /// It does not wait: the last events (the closed states, `unbind` of the tracks) come
    /// from the connection's task, which then ends. Callbacks set on the connection are
    /// dropped after that.
    pub fn close(&self) -> Result<(), Error> {
        self.inner().close()
    }

    // Events

    /// OnICECandidate sets an event handler which is invoked when a new ICE
    /// candidate is found.
    /// ICE candidate gathering only begins when SetLocalDescription or
    /// SetRemoteDescription is called.
    /// Take note that the handler will be called with a nil pointer when
    /// gathering is finished.
    pub fn on_ice_candidate(&self, f: impl Fn(Option<IceCandidate>) + Send + Sync + 'static) {
        self.inner().handlers.lock().ice_candidate = Some(Arc::new(f));
    }

    /// OnICEConnectionStateChange sets an event handler which is called
    /// when an ICE connection state is changed.
    pub fn on_ice_connection_state_change(
        &self,
        f: impl Fn(IceConnectionState) + Send + Sync + 'static,
    ) {
        self.inner().handlers.lock().ice_connection_state = Some(Arc::new(f));
    }

    /// pion's `ICETransport.OnSelectedCandidatePairChange`: sets an event handler which is
    /// invoked when the ICE agent has selected a candidate pair, the first time and after
    /// every ICE restart or failure it has come back from. Like every event it comes from
    /// the connection's task, after the ICE connection state that came with it.
    ///
    /// `rtc` tells the change inside itself only, so the layer asks: the pair is read and
    /// compared with the one told last after every STUN message that was read (a pair is
    /// selected by a check or by the answer to one, never by anything else), and after every
    /// change of the ICE connection state (which is how a pair is dropped: by a restart, by a
    /// failure that a timer brings, by closing). Media and timers cost one test of a flag.
    pub fn on_selected_candidate_pair_change(
        &self,
        f: impl Fn(IceCandidatePair) + Send + Sync + 'static,
    ) {
        self.inner().handlers.lock().selected_candidate_pair = Some(Arc::new(f));
    }

    /// OnICEGatheringStateChange sets an event handler which is invoked when the
    /// ICE candidate gathering state has changed.
    pub fn on_ice_gathering_state_change(
        &self,
        f: impl Fn(IceGatheringState) + Send + Sync + 'static,
    ) {
        self.inner().handlers.lock().ice_gathering_state = Some(Arc::new(f));
    }

    /// OnConnectionStateChange sets an event handler which is called
    /// when the PeerConnectionState has changed.
    pub fn on_connection_state_change(
        &self,
        f: impl Fn(PeerConnectionState) + Send + Sync + 'static,
    ) {
        self.inner().handlers.lock().connection_state = Some(Arc::new(f));
    }

    /// OnSignalingStateChange sets an event handler which is invoked when the
    /// peer connection's signaling state changes.
    pub fn on_signaling_state_change(&self, f: impl Fn(SignalingState) + Send + Sync + 'static) {
        self.inner().handlers.lock().signaling_state = Some(Arc::new(f));
    }

    /// OnNegotiationNeeded sets an event handler which is invoked when
    /// a change has occurred which requires session negotiation.
    pub fn on_negotiation_needed(&self, f: impl Fn() + Send + Sync + 'static) {
        self.inner().handlers.lock().negotiation_needed = Some(Arc::new(f));
    }

    /// OnTrack sets an event handler which is called when remote track
    /// arrives from a remote peer.
    ///
    /// Once for every stream: a simulcast layer is a track of its own with its rid, on the
    /// receiver the layers share.
    pub fn on_track(&self, f: impl Fn(Arc<TrackRemote>, Arc<RtpReceiver>) + Send + Sync + 'static) {
        self.inner().handlers.lock().track = Some(Arc::new(f));
    }

    /// OnDataChannel sets an event handler which is invoked when a data
    /// channel message arrives from a remote peer.
    pub fn on_data_channel(&self, f: impl Fn(Arc<DataChannel>) + Send + Sync + 'static) {
        self.inner().handlers.lock().data_channel = Some(Arc::new(f));
    }

    /// Where received packets go from now on: pion's `SettingEngine.BufferFactory`.
    ///
    /// The sink the connection has when it ends is told so ([`PacketSink::closed`]); one that
    /// was replaced before is not, and one that is set later is told of the tracks that are
    /// unbound from then on, also of those it was not told were bound.
    pub fn set_packet_sink(&self, sink: Option<Arc<dyn PacketSink>>) {
        *self.inner().sink.write() = sink;
    }

    // Tracks and transceivers

    /// AddTrack adds a Track to the PeerConnection.
    ///
    /// The track is bound when the negotiation that carries it is done and the connection
    /// is up: `bind` gets a context with what was negotiated and the writer.
    pub fn add_track(&self, track: Arc<dyn TrackLocal>) -> Result<Arc<RtpSender>, Error> {
        // What the track says about itself is asked before the mutex is taken: it is the
        // application's code.
        let media_track = media::media_stream_track(track.as_ref());
        let kind = track.kind();
        self.inner().act(|inner, core, _| {
            core.check_open()?;
            let sender_id = core.rtc.add_track(media_track)?;
            Ok(inner.adopt_sender(core, sender_id.into(), kind, track.clone()))
        })
    }

    /// AddTransceiverFromTrack Create a new RtpTransceiver(SendRecv or SendOnly) and add it to the set of transceivers.
    pub fn add_transceiver_from_track(
        &self,
        track: Arc<dyn TrackLocal>,
        init: Option<RtpTransceiverInit>,
    ) -> Result<Arc<RtpTransceiver>, Error> {
        let media_track = media::media_stream_track(track.as_ref());
        let kind = track.kind();
        self.inner().act(|inner, core, _| {
            core.check_open()?;
            let init = init.map(|init| RTCRtpTransceiverInit {
                direction: init.direction,
                streams: Vec::new(),
                send_encodings: Vec::new(),
            });
            let index = core.rtc.add_transceiver_from_track(media_track, init)?;
            inner.adopt_sender(core, index, kind, track.clone());
            Ok(core.transceivers[index].clone())
        })
    }

    /// AddTransceiverFromKind Create a new RtpTransceiver and adds it to the set of transceivers.
    ///
    /// Without `init` the transceiver receives only. One that is to send has a sender
    /// without a track (pion's has a track of its own that writes nothing): nothing is bound
    /// and nothing can be written on it until the sender is given a track
    /// ([`RtpSender::replace_track`]), which then takes the stream `rtc` made for it.
    pub fn add_transceiver_from_kind(
        &self,
        kind: RtpCodecType,
        init: Option<RtpTransceiverInit>,
    ) -> Result<Arc<RtpTransceiver>, Error> {
        self.inner().act(|inner, core, _| {
            core.check_open()?;
            let sends = init.is_some_and(|init| init.direction.has_send());
            let init = init.map(|init| RTCRtpTransceiverInit {
                direction: init.direction,
                streams: Vec::new(),
                send_encodings: if init.direction.has_send() {
                    vec![media::send_encoding("")]
                } else {
                    Vec::new()
                },
            });
            let index = core
                .rtc
                .add_transceiver_from_kind(convert::codec_type_to_rtc(kind), init)?;
            inner.sync_transceivers(core);
            let transceiver = core.transceivers[index].clone();
            transceiver.set_kind(kind);
            if sends {
                // `rtc` has made a sender, with a stream for the track it is given later.
                transceiver.set_sender(Some(Arc::new(RtpSender::new(
                    inner.this.clone(),
                    index,
                    None,
                ))));
            }
            Ok(transceiver)
        })
    }

    /// RemoveTrack removes a Track from the PeerConnection.
    pub fn remove_track(&self, sender: &Arc<RtpSender>) -> Result<(), Error> {
        self.inner().act(|_, core, _| {
            core.check_open()?;
            let index = sender.index();
            let transceiver = core
                .transceivers
                .get(index)
                .filter(|t| t.sender().is_some_and(|s| Arc::ptr_eq(&s, sender)))
                .cloned()
                .ok_or_else(|| {
                    Error::Other("RtpSender not created by this PeerConnection".to_string())
                })?;
            core.rtc.remove_track(RTCRtpSenderId::from(index))?;
            core.out.extend(sender.stop());
            transceiver.set_sender(None);
            Ok(())
        })
    }

    /// GetTransceivers returns the RtpTransceiver that are currently attached to this PeerConnection.
    pub fn get_transceivers(&self) -> Vec<Arc<RtpTransceiver>> {
        self.inner().read(|core| core.transceivers.clone())
    }

    /// GetSenders returns the RTPSender that are currently attached to this PeerConnection.
    pub fn get_senders(&self) -> Vec<Arc<RtpSender>> {
        self.inner().read(|core| {
            core.transceivers
                .iter()
                .filter_map(|t| t.sender())
                .collect()
        })
    }

    /// GetReceivers returns the RTPReceivers that are currently attached to this PeerConnection.
    pub fn get_receivers(&self) -> Vec<Arc<RtpReceiver>> {
        self.inner().read(|core| {
            core.transceivers
                .iter()
                .filter_map(|t| t.receiver())
                .collect()
        })
    }

    /// pion's `MediaEngine.getRTPParametersByPayloadType`, which its connection asks for a
    /// stream nobody announced: the codec that was negotiated for a payload type, alone in
    /// `codecs`, with the header extensions that were negotiated for media of its kind.
    ///
    /// `rtc` keeps its media engine to itself. What it shows of it is what a receiver or a
    /// sender has: the codecs of its media section (the transceiver's: what was negotiated,
    /// unless codec preferences are set), and the header extensions of its kind. So the
    /// media sections are asked, video before audio as the media engine looks, each where it
    /// receives if it does and where it sends otherwise. That differs from pion's in four ways:
    /// [`Error::CodecNotFound`] until a remote description was set (pion's falls back on
    /// what was registered) and for a payload type that no media section of the connection
    /// has; the header extensions are those of the direction that was asked (pion's gives
    /// them for both; they differ only where an extension was registered for one direction);
    /// a media section whose transceiver was stopped does not answer; and one whose
    /// transceiver has codec preferences answers with the preferred codec (pion's with the
    /// negotiated one, which may differ in its fmtp line and feedback).
    pub fn get_rtp_parameters_by_payload_type(
        &self,
        payload_type: u8,
    ) -> Result<RtpParameters, Error> {
        self.inner().read(|core| {
            if core.rtc.remote_description().is_none() {
                return Err(Error::CodecNotFound);
            }
            let indexes = |video: bool| {
                core.transceivers
                    .iter()
                    .filter(|t| (t.kind_hint() == RtpCodecType::Video) == video)
                    .map(|t| t.index())
                    .collect::<Vec<usize>>()
            };
            for index in indexes(true).into_iter().chain(indexes(false)) {
                let negotiated = core
                    .rtc
                    .rtp_transceiver(index)
                    .is_some_and(|t| t.mid().is_some());
                if !negotiated {
                    continue;
                }
                let parameters = if let Some(mut receiver) =
                    core.rtc.rtp_receiver(RTCRtpReceiverId::from(index))
                {
                    receiver.get_parameters().rtp_parameters.clone()
                } else if let Some(mut sender) = core.rtc.rtp_sender(RTCRtpSenderId::from(index)) {
                    sender.get_parameters().rtp_parameters.clone()
                } else {
                    continue;
                };
                if let Some(codec) = parameters
                    .codecs
                    .iter()
                    .find(|codec| codec.payload_type == payload_type)
                {
                    return Ok(RtpParameters {
                        header_extensions: parameters
                            .header_extensions
                            .iter()
                            .map(convert::header_extension_from_rtc)
                            .collect(),
                        codecs: vec![convert::codec_parameters_from_rtc(codec)],
                    });
                }
            }
            Err(Error::CodecNotFound)
        })
    }

    /// WriteRTCP sends a user provided RTCP packet to the connected peer. If no peer is connected the
    /// packet is discarded.
    ///
    /// The packets go out as one compound packet. [`Error::ClosedPipe`] before the connection
    /// is up and after it was closed.
    pub fn write_rtcp(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), Error> {
        self.inner().write_rtcp(pkts)
    }

    // Data channels

    /// CreateDataChannel creates a new DataChannel object with the given label
    /// and optional DataChannelInit used to configure properties of the
    /// underlying channel such as data reliability.
    pub fn create_data_channel(
        &self,
        label: &str,
        options: Option<DataChannelInit>,
    ) -> Result<Arc<DataChannel>, Error> {
        let options = options.unwrap_or_default();
        let init = convert::data_channel_init_to_rtc(&options)?;
        self.inner().act(|inner, core, _| {
            core.check_open()?;
            let id = core.rtc.create_data_channel(label, Some(init))?.id();
            let channel = Arc::new(DataChannel::new(
                inner.this.clone(),
                id,
                label.to_string(),
                &options,
            ));
            core.data_channels.insert(id, channel.clone());
            Ok(channel)
        })
    }

    /// GetStats return data providing statistics about the overall connection.
    ///
    /// These are `rtc`'s numbers. It counts the RTP it is given to send and what it receives;
    /// reports and feedback are the ported code's business and it knows nothing of them.
    pub fn get_stats(&self) -> StatsReport {
        self.inner()
            .read(|core| core.rtc.get_stats(Instant::now(), StatsSelector::None))
    }
}

impl Inner {
    /// Looks at the connection, without changing it.
    pub(crate) fn read<R>(&self, f: impl FnOnce(&mut Core) -> R) -> R {
        f(&mut self.core.lock())
    }

    /// Does something to the connection, takes out of it what came of that (datagrams to the
    /// socket, events to the queue) and wakes the connection's task: there may be something
    /// to tell, and the timers may have changed.
    pub(crate) fn act<R>(&self, f: impl FnOnce(&Inner, &mut Core, Instant) -> R) -> R {
        let mut core = self.core.lock();
        let res = f(self, &mut core, Instant::now());
        self.drain(&mut core, Time::now());
        drop(core);
        self.wake.notify_one();
        res
    }

    /// Takes out of `rtc` everything it has to say after it was given something. `at` is the
    /// arrival time of what was read, for the packets that came of it.
    fn drain(&self, core: &mut Core, at: Time) {
        loop {
            let mut progressed = false;
            // What the tap took first: a stream is announced after its first packet has gone
            // to the sink, as pion announces a track when the packet is in its buffer.
            self.take_inbox(core, at);
            while let Some(event) = core.rtc.poll_event() {
                progressed = true;
                self.on_event(core, event);
            }
            while let Some(message) = core.rtc.poll_read() {
                // RTP comes out here a second time, with RTX unwrapped: the sink has had it
                // as it arrived.
                if let RTCMessage::DataChannelMessage(id, message) = message.message
                    && let Some(channel) = core.data_channels.get(&id)
                {
                    core.out.push(Delivery::DataChannelMessage(
                        channel.clone(),
                        DataChannelMessage {
                            is_string: message.is_string,
                            data: message.data.freeze(),
                        },
                    ));
                }
            }
            self.flush(core);
            if !progressed {
                break;
            }
        }
        if core.selected_pair_in_doubt {
            self.note_selected_pair(core);
        }
        if !core.out.is_empty() {
            self.pending.lock().extend(core.out.drain(..));
        }
    }

    /// Looks whether the ICE agent has another candidate pair than the one that was told
    /// last, and has the new one told. It is called when something happened that can have
    /// changed the pair; see [`PeerConnection::on_selected_candidate_pair_change`].
    fn note_selected_pair(&self, core: &mut Core) {
        // Without a way to the ICE transport (nothing negotiated yet) the question stays
        // open until the next turn.
        let Some(pair) = selected_candidate_pair(core) else {
            return;
        };
        core.selected_pair_in_doubt = false;
        if pair == core.selected_pair {
            return;
        }
        core.selected_pair = pair.clone();
        // A pair that was dropped is not told, as with pion: the state says it.
        if let Some(pair) = pair {
            debugw!(self.logger, "selected candidate pair", "pair" => pair.to_string());
            core.out.push(Delivery::SelectedCandidatePair(pair));
        }
    }

    /// Pushes the datagrams `rtc` wants sent into the socket.
    fn flush(&self, core: &mut Core) {
        while let Some(datagram) = core.rtc.poll_write() {
            self.node
                .send(self.id, &datagram.transport, &datagram.message);
        }
    }

    fn take_inbox(&self, core: &mut Core, at: Time) {
        let captured = std::mem::take(&mut *core.inbox.lock());
        for item in captured {
            core.out.push(match item {
                Captured::Rtp { ssrc, packet } => Delivery::Rtp { ssrc, packet, at },
                Captured::Rtcp(packets) => Delivery::Rtcp { packets, at },
                Captured::BindRemoteStream(info) => {
                    Delivery::BindRemoteStream(convert::stream_info_from_rtc(&info))
                }
                Captured::UnbindRemoteStream(info) => {
                    Delivery::UnbindRemoteStream(convert::stream_info_from_rtc(&info))
                }
            });
        }
    }

    fn on_event(&self, core: &mut Core, event: RTCPeerConnectionEvent) {
        match event {
            RTCPeerConnectionEvent::OnNegotiationNeededEvent => {
                core.out.push(Delivery::NegotiationNeeded);
            }
            RTCPeerConnectionEvent::OnIceCandidateEvent(event) => {
                core.local_candidates.push(event.candidate.clone());
                core.out.push(Delivery::IceCandidate(Some(event.candidate)));
            }
            RTCPeerConnectionEvent::OnSignalingStateChangeEvent(state) => {
                core.signaling_state = state;
                core.out.push(Delivery::SignalingState(state));
                self.bind_senders(core);
            }
            RTCPeerConnectionEvent::OnIceConnectionStateChangeEvent(state) => {
                core.ice_connection_state = state;
                core.out.push(Delivery::IceConnectionState(state));
                // A pair was selected (connected), or dropped (a restart goes back to
                // checking, a failure and closing say so).
                core.selected_pair_in_doubt = true;
            }
            RTCPeerConnectionEvent::OnIceGatheringStateChangeEvent(state) => {
                // `rtc` goes back to gathering with every local description; that is only
                // told when there was something to gather (the first time, and a restart).
                if core.ice_gathering_state != state {
                    core.ice_gathering_state = state;
                    core.out.push(Delivery::IceGatheringState(state));
                    if state == IceGatheringState::Complete {
                        core.out.push(Delivery::IceCandidate(None));
                    }
                }
            }
            RTCPeerConnectionEvent::OnConnectionStateChangeEvent(state) => {
                core.connection_state = state;
                if state == PeerConnectionState::Connected && !core.closed {
                    core.media_open = true;
                }
                core.out.push(Delivery::ConnectionState(state));
                self.bind_senders(core);
            }
            RTCPeerConnectionEvent::OnDataChannel(event) => self.on_data_channel_event(core, event),
            RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => {
                self.sync_transceivers(core);
                let index: usize = init.receiver_id.into();
                let Some(transceiver) = core.transceivers.get(index).cloned() else {
                    return;
                };
                let Some(receiver) = transceiver.receiver() else {
                    return;
                };
                let Some(mut rtc_receiver) = core.rtc.rtp_receiver(init.receiver_id) else {
                    return;
                };
                let track = Arc::new(TrackRemote::new(&mut rtc_receiver, &init));
                transceiver.set_kind(track.kind());
                receiver.add_track(track.clone());
                debugw!(
                    self.logger,
                    "remote track",
                    "ssrc" => track.ssrc(),
                    "rid" => track.rid(),
                    "codec" => track.codec().mime_type
                );
                core.out.push(Delivery::Track(track, receiver));
            }
            _ => {}
        }
    }

    fn on_data_channel_event(&self, core: &mut Core, event: RTCDataChannelEvent) {
        match event {
            RTCDataChannelEvent::OnOpen(id) => {
                let channel = match core.data_channels.get(&id) {
                    Some(channel) => channel.clone(),
                    None => {
                        // The other side opened it.
                        let Some(rtc_channel) = core.rtc.data_channel(id) else {
                            return;
                        };
                        let channel =
                            Arc::new(DataChannel::from_rtc(self.this.clone(), &rtc_channel));
                        core.data_channels.insert(id, channel.clone());
                        core.out.push(Delivery::DataChannel(channel.clone()));
                        channel
                    }
                };
                if let Some(rtc_channel) = core.rtc.data_channel(id) {
                    channel.opened(rtc_channel.stream_id());
                }
                core.out.push(Delivery::DataChannelOpen(channel));
            }
            RTCDataChannelEvent::OnBufferedAmountLow(id) => {
                if let Some(channel) = core.data_channels.get(&id) {
                    core.out
                        .push(Delivery::DataChannelBufferedAmountLow(channel.clone()));
                }
            }
            RTCDataChannelEvent::OnClose(id) => {
                if let Some(channel) = core.data_channels.remove(&id) {
                    channel.closed();
                    core.out.push(Delivery::DataChannelClose(channel));
                }
            }
            _ => {}
        }
    }

    /// Makes this layer's list of transceivers as long as `rtc`'s, which grows when a track
    /// is added and when a remote description brings new media.
    pub(crate) fn sync_transceivers(&self, core: &mut Core) {
        let count = core.rtc.get_transceivers().count();
        while core.transceivers.len() < count {
            let index = core.transceivers.len();
            core.transceivers
                .push(Arc::new(RtpTransceiver::new(self.this.clone(), index)));
        }
    }

    /// The sender object for a track `rtc` has just taken.
    fn adopt_sender(
        &self,
        core: &mut Core,
        index: usize,
        kind: RtpCodecType,
        track: Arc<dyn TrackLocal>,
    ) -> Arc<RtpSender> {
        self.sync_transceivers(core);
        let transceiver = core.transceivers[index].clone();
        transceiver.set_kind(kind);
        let sender = Arc::new(RtpSender::new(self.this.clone(), index, Some(track)));
        transceiver.set_sender(Some(sender.clone()));
        sender
    }

    /// Binds the local tracks that can be bound now: pion's `startRTPSenders`. A track is
    /// bound once, when its media section has been negotiated and DTLS is up, so that what
    /// it writes right away can go out.
    fn bind_senders(&self, core: &mut Core) {
        for transceiver in core.transceivers.clone() {
            if let Some(sender) = transceiver.sender() {
                self.bind_sender(core, &sender);
            }
        }
    }

    /// Binds the track of one sender if it can be bound now and is not: when the connection
    /// is up and no negotiation is under way, and the sender's media section has been
    /// negotiated. A sender that is given a track when that holds already
    /// (`RtpSender::replace_track`) has it bound here; any other waits for `bind_senders`.
    pub(crate) fn bind_sender(&self, core: &mut Core, sender: &RtpSender) {
        if !core.media_open || core.closed || core.signaling_state != SignalingState::Stable {
            return;
        }
        if !sender.wants_binding() {
            return;
        }
        let index = sender.index();
        let negotiated = core
            .rtc
            .rtp_transceiver(index)
            .is_some_and(|t| t.mid().is_some());
        if !negotiated {
            return;
        }
        let Some(mut rtc_sender) = core.rtc.rtp_sender(RTCRtpSenderId::from(index)) else {
            return;
        };
        let parameters = convert::send_parameters_from_rtc(rtc_sender.get_parameters());
        let context = Arc::new(SenderContext::new(self.this.clone(), parameters));
        if let Some(track) = sender.bound(context.clone()) {
            core.out.push(Delivery::Bind { track, context });
        }
    }

    /// After a local description was set: the connection is found by its ICE user name from
    /// now on, and the node's addresses are its candidates.
    fn after_local_description(&self, core: &mut Core) -> Result<(), Error> {
        self.sync_transceivers(core);
        let ufrag = core
            .rtc
            .local_description()
            .and_then(|d| ice_ufrag(&d.sdp).map(str::to_string))
            .unwrap_or_default();
        let restarted = !core.local_ufrag.is_empty() && core.local_ufrag != ufrag;
        if !ufrag.is_empty() && core.local_ufrag != ufrag {
            self.node.add_ufrag(self.id, &ufrag);
            core.local_ufrag = ufrag;
        }

        if !core.candidates_added {
            core.candidates_added = true;
            core.ice_gathering_state = IceGatheringState::Gathering;
            core.out
                .push(Delivery::IceGatheringState(IceGatheringState::Gathering));
            for addr in &self.host_candidates {
                core.rtc
                    .add_local_candidate(host_candidate(*addr, TcpType::Unspecified)?)?;
            }
            for addr in &self.tcp_host_candidates {
                core.rtc
                    .add_local_candidate(host_candidate(*addr, TcpType::Passive)?)?;
            }
            if self.active_tcp {
                // One for each address of the connection, with the port that says it has
                // none (RFC 6544): it dials from one of the system's choosing.
                let mut ips: Vec<IpAddr> = Vec::new();
                for addr in &self.host_candidates {
                    if !ips.contains(&addr.ip()) {
                        ips.push(addr.ip());
                    }
                }
                for ip in ips {
                    let addr = SocketAddr::new(ip, tcp::ACTIVE_TCP_PORT);
                    core.rtc
                        .add_local_candidate(host_candidate(addr, TcpType::Active)?)?;
                }
            }
        } else if restarted {
            // The candidates are the same after a restart, and the other side needs them
            // again.
            core.ice_gathering_state = IceGatheringState::Gathering;
            core.out
                .push(Delivery::IceGatheringState(IceGatheringState::Gathering));
            for candidate in core.local_candidates.clone() {
                core.out.push(Delivery::IceCandidate(Some(candidate)));
            }
        }
        // An empty candidate ends the gathering, which `rtc` took up again with the
        // description.
        core.rtc.add_local_candidate(IceCandidateInit::default())?;
        Ok(())
    }

    pub(crate) fn write_rtp(&self, header: &rtp::Header, payload: &[u8]) -> Result<usize, Error> {
        let packet = convert::rtp_packet_to_rtc(header, payload);
        let mut size = header.marshal_size() + payload.len();
        if header.padding {
            size += header.padding_size as usize;
        }
        let mut core = self.core.lock();
        if !core.media_open {
            return Err(Error::ClosedPipe);
        }
        // Straight into the connection, not through `rtc`'s sender, which takes the track's
        // own SSRC only: retransmissions and padding go out on the repair stream.
        core.rtc.handle_write(TaggedRTCMessage {
            now: Instant::now(),
            message: RTCMessage::RtpPacket(String::new(), packet),
        })?;
        self.flush(&mut core);
        Ok(size)
    }

    fn write_rtcp(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), Error> {
        if pkts.is_empty() {
            return Ok(());
        }
        let mut core = self.core.lock();
        if !core.media_open {
            return Err(Error::ClosedPipe);
        }
        core.rtc.handle_write(TaggedRTCMessage {
            now: Instant::now(),
            message: RTCMessage::RtcpPacket(String::new(), pkts.to_vec()),
        })?;
        self.flush(&mut core);
        Ok(())
    }

    fn close(&self) -> Result<(), Error> {
        let mut guard = self.core.lock();
        let core = &mut *guard;
        if core.closed {
            return Ok(());
        }
        core.closed = true;
        core.media_open = false;
        for transceiver in core.transceivers.clone() {
            if let Some(sender) = transceiver.sender() {
                core.out.extend(sender.stop());
            }
        }
        // `rtc` says goodbye (an SCTP abort, DTLS close_notify): the drain sends it.
        let res = core.rtc.close();
        core.signaling_state = SignalingState::Closed;
        self.drain(core, Time::now());
        for (_, channel) in core.data_channels.drain() {
            channel.closed();
            core.out.push(Delivery::DataChannelClose(channel));
        }
        if !core.out.is_empty() {
            self.pending.lock().extend(core.out.drain(..));
        }
        drop(guard);
        self.wake.notify_one();
        res.map_err(Error::from)
    }

    /// One turn of the connection's task with the mutex held: the datagrams that have come,
    /// the timers that are due. Gives the time of the next timer, or `None` when the
    /// connection is closed.
    fn step(&self, datagrams: &mut Vec<Datagram>) -> Option<Option<Instant>> {
        let mut core = self.core.lock();
        if core.closed {
            datagrams.clear();
            return None;
        }
        for datagram in datagrams.drain(..) {
            let at = datagram.at;
            // STUN, as against DTLS and RTP (RFC 7983): the only thing that makes the ICE
            // agent select a candidate pair.
            if datagram.data.first().is_some_and(|first| *first < 4) {
                core.selected_pair_in_doubt = true;
            }
            let res = core.rtc.handle_read(TaggedBytesMut {
                now: Instant::now(),
                transport: TransportContext {
                    local_addr: self.local_addr_of(&datagram),
                    peer_addr: datagram.from,
                    ecn: None,
                    transport_protocol: datagram.protocol,
                },
                message: datagram.data,
            });
            if let Err(err) = res {
                debugw!(self.logger, "could not read a datagram", "error" => err.to_string(), "from" => datagram.from);
            }
            self.drain(&mut core, at);
        }
        let now = Instant::now();
        if core.rtc.poll_timeout().is_some_and(|t| t <= now) {
            if let Err(err) = core.rtc.handle_timeout(now) {
                debugw!(self.logger, "timeout handling failed", "error" => err.to_string());
            }
            self.drain(&mut core, Time::now());
        }
        Some(core.rtc.poll_timeout())
    }

    /// The address `rtc` is told a datagram arrived at. Its ICE agent takes a datagram only
    /// at an address that is a candidate of the connection, so for a connection with
    /// addresses of its own it is the first of those on the socket the datagram came in on.
    ///
    /// A packet on a TCP stream the connection dialled arrived at its active candidate (the
    /// node keeps that address with the stream), which the agent knows by its IP address.
    fn local_addr_of(&self, datagram: &Datagram) -> SocketAddr {
        let candidates = match datagram.protocol {
            TransportProtocol::TCP if datagram.local.port() == tcp::ACTIVE_TCP_PORT => {
                return datagram.local;
            }
            TransportProtocol::TCP => &self.tcp_host_candidates,
            _ => &self.host_candidates,
        };
        if candidates.contains(&datagram.local) {
            return datagram.local;
        }
        candidates
            .iter()
            .find(|c| c.port() == datagram.local.port() && c.is_ipv4() == datagram.local.is_ipv4())
            .copied()
            .unwrap_or(datagram.local)
    }

    /// Tells what there is to tell. Only the connection's task calls this, so it is one at a
    /// time and in order; the connection's mutex is not held.
    fn deliver(&self) {
        loop {
            let batch = std::mem::take(&mut *self.pending.lock());
            if batch.is_empty() {
                return;
            }
            let sink = self.sink.read().clone();
            for delivery in batch {
                self.tell(delivery, sink.as_deref());
            }
        }
    }

    fn tell(&self, delivery: Delivery, sink: Option<&dyn PacketSink>) {
        match delivery {
            Delivery::Rtp { ssrc, packet, at } => {
                if let Some(sink) = sink {
                    sink.on_rtp(ssrc, packet, at);
                }
            }
            Delivery::Rtcp { packets, at } => {
                if let Some(sink) = sink {
                    sink.on_rtcp(&packets, at);
                }
            }
            Delivery::BindRemoteStream(info) => {
                if let Some(sink) = sink {
                    sink.bind_remote_stream(&info);
                }
            }
            Delivery::UnbindRemoteStream(info) => {
                if let Some(sink) = sink {
                    sink.unbind_remote_stream(&info);
                }
            }
            Delivery::IceCandidate(candidate) => {
                let handler = self.handlers.lock().ice_candidate.clone();
                if let Some(handler) = handler {
                    handler(candidate);
                }
            }
            Delivery::IceConnectionState(state) => {
                let handler = self.handlers.lock().ice_connection_state.clone();
                if let Some(handler) = handler {
                    handler(state);
                }
            }
            Delivery::SelectedCandidatePair(pair) => {
                let handler = self.handlers.lock().selected_candidate_pair.clone();
                if let Some(handler) = handler {
                    handler(pair);
                }
            }
            Delivery::IceGatheringState(state) => {
                let handler = self.handlers.lock().ice_gathering_state.clone();
                if let Some(handler) = handler {
                    handler(state);
                }
            }
            Delivery::ConnectionState(state) => {
                let handler = self.handlers.lock().connection_state.clone();
                if let Some(handler) = handler {
                    handler(state);
                }
            }
            Delivery::SignalingState(state) => {
                let handler = self.handlers.lock().signaling_state.clone();
                if let Some(handler) = handler {
                    handler(state);
                }
            }
            Delivery::NegotiationNeeded => {
                let handler = self.handlers.lock().negotiation_needed.clone();
                if let Some(handler) = handler {
                    handler();
                }
            }
            Delivery::Track(track, receiver) => {
                let handler = self.handlers.lock().track.clone();
                if let Some(handler) = handler {
                    handler(track, receiver);
                }
            }
            Delivery::DataChannel(channel) => {
                let handler = self.handlers.lock().data_channel.clone();
                if let Some(handler) = handler {
                    handler(channel);
                }
            }
            Delivery::DataChannelOpen(channel) => channel.tell_open(),
            Delivery::DataChannelMessage(channel, message) => channel.tell_message(message),
            Delivery::DataChannelBufferedAmountLow(channel) => channel.tell_buffered_amount_low(),
            Delivery::DataChannelClose(channel) => channel.tell_close(),
            Delivery::Bind { track, context } => {
                let t: Arc<dyn TrackLocalContext> = context.clone();
                match track.bind(t) {
                    Ok(codec) => {
                        // After the bind the context has the codec the track chose, and the
                        // interceptors are told of the stream, as pion's `RTPSender.Send`
                        // does both.
                        let info = context.bound_as(codec);
                        if let Some(sink) = sink {
                            sink.bind_local_stream(&info);
                        }
                    }
                    Err(err) => {
                        warnw!(self.logger, "could not bind track", err, "trackID" => track.id());
                    }
                }
            }
            Delivery::Unbind { track, context } => {
                // None for a track whose `bind` failed: the sink was not told of a stream.
                let info = context.take_stream_info();
                let t: Arc<dyn TrackLocalContext> = context;
                if let Err(err) = track.unbind(t) {
                    warnw!(self.logger, "could not unbind track", err, "trackID" => track.id());
                }
                if let (Some(sink), Some(info)) = (sink, info) {
                    sink.unbind_local_stream(&info);
                }
            }
            Delivery::Release(track) => drop(track),
        }
    }

    /// Puts something to tell in the queue from outside the connection's mutex (a handler
    /// set on a data channel that is open already).
    pub(crate) fn tell_later(&self, delivery: Delivery) {
        self.pending.lock().push_back(delivery);
        self.wake.notify_one();
    }

    /// Lets go of what the application gave the connection, when it is over: callbacks hold
    /// what they captured, often the connection itself. The sink is told that this is the
    /// end. Only the connection's task calls this, as the last thing it does, so the sink has
    /// had everything else by then.
    fn forget(&self) {
        *self.handlers.lock() = Handlers::default();
        // Taken out first: neither the sink's `closed` nor what dropping the sink sets off
        // runs with a lock of the connection held.
        let sink = self.sink.write().take();
        if let Some(sink) = sink {
            sink.closed();
        }
    }
}

/// How many datagrams the task takes in one turn, before it tells what came of them.
const BATCH: usize = 16;

/// The task of a connection: reads its datagrams, drives its timers, makes its calls out.
async fn run(inner: Arc<Inner>, mut queue: mpsc::Receiver<Datagram>, guard: ConnectionGuard) {
    let mut datagrams: Vec<Datagram> = Vec::with_capacity(BATCH);
    loop {
        let step = inner.step(&mut datagrams);
        inner.deliver();
        let Some(next) = step else {
            break;
        };
        let timer = async {
            match next {
                Some(at) => {
                    // Never a busy loop, whatever `rtc` asks for.
                    let floor = Instant::now() + std::time::Duration::from_millis(1);
                    tokio::time::sleep_until(at.max(floor).into()).await
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            datagram = queue.recv() => match datagram {
                Some(datagram) => {
                    datagrams.push(datagram);
                    while datagrams.len() < BATCH
                        && let Ok(datagram) = queue.try_recv()
                    {
                        datagrams.push(datagram);
                    }
                }
                // The node was closed.
                None => {
                    let _ = inner.close();
                }
            },
            _ = inner.wake.notified() => {}
            _ = timer => {}
        }
    }
    // What closing left to tell came out in the last turn. From here on the node does not
    // know the connection.
    drop(guard);
    inner.forget();
}

/// The candidate pair the ICE agent has selected. The outer `None`: there is no telling,
/// because `rtc` gives its ICE transport only through a sender or a receiver whose media
/// section was negotiated, or through the SCTP transport once the descriptions have one
/// (`pc.sctp()?.transport().ice_transport()`). A connection that runs ICE has one of them.
fn selected_candidate_pair(core: &mut Core) -> Option<Option<IceCandidatePair>> {
    if let Some(sctp) = core.rtc.sctp() {
        return Some(
            sctp.transport()
                .ice_transport()
                .get_selected_candidate_pair(),
        );
    }
    for index in 0..core.transceivers.len() {
        if let Some(sender) = core.rtc.rtp_sender(RTCRtpSenderId::from(index))
            && let Some(transport) = sender.transport()
        {
            return Some(transport.ice_transport().get_selected_candidate_pair());
        }
        if let Some(receiver) = core.rtc.rtp_receiver(RTCRtpReceiverId::from(index))
            && let Some(transport) = receiver.transport()
        {
            return Some(transport.ice_transport().get_selected_candidate_pair());
        }
    }
    None
}

/// A host candidate at this address, over UDP or, with a TCP type, over TCP.
fn host_candidate(addr: SocketAddr, tcp_type: TcpType) -> Result<IceCandidateInit, Error> {
    let network = if tcp_type == TcpType::Unspecified {
        "udp"
    } else {
        "tcp"
    };
    let candidate = CandidateHostConfig {
        base_config: CandidateConfig {
            network: network.to_owned(),
            address: addr.ip().to_string(),
            port: addr.port(),
            component: 1,
            ..Default::default()
        },
        tcp_type,
    }
    .new_candidate_host()?;
    Ok(IceCandidate::from(&candidate).to_json()?)
}

/// Whether the agent takes TCP candidates with these network types: all of them, or the TCP
/// ones among them.
fn tcp_allowed(network_types: &[NetworkType]) -> bool {
    network_types.is_empty() || network_types.iter().any(|t| t.is_tcp())
}

/// The ICE user name of a description (the first, which with BUNDLE is the only one).
fn ice_ufrag(sdp: &str) -> Option<&str> {
    sdp.lines()
        .find_map(|line| line.trim_end().strip_prefix("a=ice-ufrag:"))
}

/// The media sections of a description that have a mid, with their kind.
fn media_kinds(sdp: &str) -> Vec<(String, RtpCodecType)> {
    let mut kinds = Vec::new();
    let mut kind = None;
    for line in sdp.lines() {
        let line = line.trim_end();
        if let Some(media) = line.strip_prefix("m=") {
            kind = media.split(' ').next().map(RtpCodecType::new);
        } else if let Some(mid) = line.strip_prefix("a=mid:")
            && let Some(kind) = kind
        {
            kinds.push((mid.to_string(), kind));
        }
    }
    kinds
}

#[cfg(test)]
#[path = "peerconnection_test.rs"]
mod peerconnection_test;
