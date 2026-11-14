//! pion's `SettingEngine` for a connection of this layer.
//!
//! pion's engine is a struct that LiveKit copies for every connection and then changes (ICE
//! lite off for some clients, other addresses for others). `rtc`'s can be neither read nor
//! changed once it is built. So the layer's engine is data: every field is public, under the
//! name of pion's setter, a copy is a `clone()`, and `rtc`'s engine is built from it when the
//! connection is made.
//!
//! Only settings that do something are here: those `rtc` 0.21 has, and the two that are the
//! layer's own (the addresses a connection advertises, and whether it dials over TCP). What LiveKit sets on pion's engine
//! and has no counterpart is listed at the end of this file's description of
//! [`SettingEngine`], so that nothing is taken to be set that is not.

use std::net::IpAddr;

use rtc::peer_connection::configuration::setting_engine as rtc_engine;

use super::{DtlsRole, MulticastDnsMode, NetworkType};
use crate::go::time::Duration;
use crate::webrtc::Error;

/// pion's `SettingEngine`, as data. The default is pion's: a full ICE agent, the library's
/// timeouts, the node's addresses.
///
/// Set on pion's engine by LiveKit and not here, because `rtc` 0.21 has no such setting:
/// `DisableSRTPReplayProtection` and `DisableSRTCPReplayProtection` (a window can be sized,
/// not switched off), `SetDTLSEllipticCurves`, `SetDTLSInsecureSkipHelloVerify`,
/// `SetDTLSRetransmissionInterval`, `SetDTLSConnectContextMaker` (no limit on the time of a
/// handshake), `DisableCloseByDTLS`, `EnableSCTPZeroChecksum`, `SetSCTPMinCwnd`,
/// `SetSCTPFastRtxWnd`, `SetSCTPCwndCAStep`, `EnableSped`, `EnableSctpSnap`. Not here
/// because the layer does it
/// otherwise: `DetachDataChannels` and `EnableDataChannelBlockWrite` (a channel has `send`,
/// which never waits), `SetFireOnTrackBeforeFirstRTP` (a track is announced with its first
/// packet), `BufferFactory` (the connection's `PacketSink`), `DisableMediaEngineCopy` (a
/// connection has the media engine it was given), `LoggerFactory` (the connection's logger),
/// `SetInterfaceFilter`, `SetIPFilter`, `SetICEAddressRewriteRules`,
/// `SetIncludeLoopbackCandidate` and `SetEphemeralUDPPortRange` (nothing is gathered: the
/// outcome of these is `host_candidate_ips`), `SetICETCPMux` (the node's TCP listener,
/// [`Node::new_with_tcp`](super::Node::new_with_tcp)), `DisableActiveTCP` (`active_tcp`, the
/// other way round).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SettingEngine {
    /// `SetLite`: whether the ICE agent of the connection is a lite one, which answers
    /// checks and sends none.
    pub lite: bool,
    /// `SetAnsweringDTLSRole`: the DTLS role of the connection when it answers.
    /// `Unspecified` leaves it to the library (the client).
    pub answering_dtls_role: DtlsRole,
    /// `SetICETimeouts`, its first argument: how long nothing may arrive before the ICE
    /// transport counts as disconnected. `None`: the library's 5 seconds.
    pub ice_disconnected_timeout: Option<Duration>,
    /// `SetICETimeouts`, its second argument: how long after that before it counts as
    /// failed. `None`: the library's 25 seconds.
    pub ice_failed_timeout: Option<Duration>,
    /// `SetICETimeouts`, its third argument: how often a full agent sends a check on the
    /// selected pair. `None`: the library's 2 seconds.
    pub ice_keepalive_interval: Option<Duration>,
    /// `SetHostAcceptanceMinWait`. `None`: not set.
    pub host_acceptance_min_wait: Option<Duration>,
    /// `SetSrflxAcceptanceMinWait`. `None`: not set.
    pub srflx_acceptance_min_wait: Option<Duration>,
    /// `SetPrflxAcceptanceMinWait`. `None`: not set.
    pub prflx_acceptance_min_wait: Option<Duration>,
    /// `SetRelayAcceptanceMinWait`. `None`: not set.
    pub relay_acceptance_min_wait: Option<Duration>,
    /// `SetICEMulticastDNSMode`. `Disabled` drops remote candidates with an mDNS name; with
    /// the default (queries only) the agent asks, but the node gives the answer to no
    /// connection, so such a candidate is never resolved either.
    pub ice_multicast_dns_mode: MulticastDnsMode,
    /// `SetNetworkTypes`: the kinds of candidates the agent takes, its own and the other
    /// side's. Empty: all the library knows. Without a TCP type the connection has no TCP
    /// candidates, also on a node with a TCP listener; without a UDP type the agent drops
    /// the UDP candidates the connection is given.
    pub network_types: Vec<NetworkType>,
    /// `SetSCTPMaxMessageSize`. 0: the library's (64 KiB).
    pub sctp_max_message_size: u32,
    /// `SetSCTPMaxReceiveBufferSize`. 0: the library's (1 MiB).
    pub sctp_max_receive_buffer_size: u32,
    /// `SetIgnoreRidPauseForRecv`.
    pub ignore_rid_pause_for_recv: bool,
    /// pion's `DisableActiveTCP`, the other way round and off unless set: whether the
    /// connection has active TCP candidates (one for each address it advertises, with port
    /// 9) and dials the passive TCP candidates of the other side, once each, from a port of
    /// the system's choosing. pion's agent dials unless told not to; LiveKit tells it not to
    /// on a server ("clients should be dialing", `pkg/rtc/config.go`), and a server that
    /// dialled whatever address a description names would let a client point it anywhere.
    /// A client's connection, as in the layer's tests, sets it. Nothing happens without a TCP
    /// type in `network_types` (when that is not empty).
    pub active_tcp: bool,
    /// The addresses this connection gives as host candidates, each with the port of the
    /// node's socket, when they are not the node's own: what pion's agent makes of
    /// `SetICEAddressRewriteRules`, `SetIPFilter` and `SetInterfaceFilter` on the copy of the
    /// engine a connection is made from (LiveKit gives some clients the external addresses
    /// only). `None`: the node's ([`Node::candidates`](super::Node::candidates)).
    ///
    /// The node's socket is the same for every connection: an address here has to be one
    /// that reaches it. Addresses of a family the node has no socket for are left out, and
    /// with none left the connection advertises nothing.
    pub host_candidate_ips: Option<Vec<IpAddr>>,
}

impl SettingEngine {
    /// pion's `SetICETimeouts`.
    pub fn set_ice_timeouts(
        &mut self,
        disconnected_timeout: Duration,
        failed_timeout: Duration,
        keep_alive_interval: Duration,
    ) {
        self.ice_disconnected_timeout = Some(disconnected_timeout);
        self.ice_failed_timeout = Some(failed_timeout);
        self.ice_keepalive_interval = Some(keep_alive_interval);
    }

    /// pion's `SetAnsweringDTLSRole`, which takes the client's role or the server's and
    /// refuses anything else.
    pub fn set_answering_dtls_role(&mut self, role: DtlsRole) -> Result<(), Error> {
        if role != DtlsRole::Client && role != DtlsRole::Server {
            return Err(Error::Other(
                "SetAnsweringDTLSRole must DTLSRoleClient or DTLSRoleServer".to_string(),
            ));
        }

        self.answering_dtls_role = role;

        Ok(())
    }

    /// `rtc`'s engine with these settings.
    pub(crate) fn to_rtc(&self) -> rtc_engine::SettingEngine {
        let std = |d: Option<Duration>| d.map(Duration::to_std);
        let mut builder = rtc_engine::SettingEngineBuilder::new()
            .with_lite(self.lite)
            .with_answering_dtls_role(self.answering_dtls_role)
            .with_ice_timeouts(
                std(self.ice_disconnected_timeout),
                std(self.ice_failed_timeout),
                std(self.ice_keepalive_interval),
            )
            .with_host_acceptance_min_wait(std(self.host_acceptance_min_wait))
            .with_srflx_acceptance_min_wait(std(self.srflx_acceptance_min_wait))
            .with_prflx_acceptance_min_wait(std(self.prflx_acceptance_min_wait))
            .with_relay_acceptance_min_wait(std(self.relay_acceptance_min_wait))
            .with_multicast_dns_mode(self.ice_multicast_dns_mode)
            .with_network_types(self.network_types.clone())
            .with_ignore_rid_pause_for_recv(self.ignore_rid_pause_for_recv);
        if self.sctp_max_message_size > 0 {
            builder = builder.with_sctp_max_message_size(rtc_engine::SctpMaxMessageSize::Bounded(
                self.sctp_max_message_size,
            ));
        }
        if self.sctp_max_receive_buffer_size > 0 {
            builder = builder.with_sctp_max_receive_buffer_size(self.sctp_max_receive_buffer_size);
        }
        builder.build()
    }
}
