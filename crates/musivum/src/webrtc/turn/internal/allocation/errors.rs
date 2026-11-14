// Ported from pion/turn, internal/allocation/errors.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust: one `Error` enum, its variants the package's `ErrX` and
// `errX` values. Those of RFC 6062's TCP relays (`ErrTCPConnectionTimeoutOrFailure`,
// `ErrDupeTCPConnection`, `errAllocateListenerMustBeSet`, `errAllocateConnMustBeSet`,
// `errFailedToGenerateConnectionID`, `errInvalidPeerAddress`) are not here, as the TCP relays
// are not ported; `TcpRelayNotSupported` is new and says so to a request for one. What the
// relay address allocator fails with is carried as it is (`Allocate`), as are I/O errors.

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ErrSameChannelDifferentPeer is returned when a client attempts to bind a
    // channel number that is already bound to a different peer address.
    #[error("you cannot use the same channel number with different peer")]
    SameChannelDifferentPeer,

    // ErrSamePeerDifferentChannel is returned when a client attempts to bind a
    // peer address that is already bound to a different channel number.
    #[error("you cannot use the same peer with different channel number")]
    SamePeerDifferentChannel,

    #[error("AllocatePacketConn must be set")]
    AllocatePacketConnMustBeSet,
    #[error("LeveledLogger must be set")]
    LeveledLoggerMustBeSet,
    #[error("allocations must not be created with nil FivTuple")]
    NilFiveTuple,
    #[error("allocations must not be created with nil FiveTuple.SrcAddr")]
    NilFiveTupleSrcAddr,
    #[error("allocations must not be created with nil FiveTuple.DstAddr")]
    NilFiveTupleDstAddr,
    #[error("allocations must not be created with nil turnSocket")]
    NilTurnSocket,
    #[error("allocations must not be created with a lifetime of 0")]
    LifetimeZero,
    // fmt.Errorf("%w: %v", errDupeFiveTuple, fiveTuple)
    #[error("allocation attempt created with duplicate FiveTuple: {0}")]
    DupeFiveTuple(String),
    #[error("failed to cast net.Addr to *net.UDPAddr")]
    FailedToCastUdpAddr,
    #[error("failed to allocate an even port")]
    FailedToAllocateEvenPort,
    #[error("permission request administratively prohibited")]
    AdminProhibited,
    #[error("allocation has no relay socket")]
    NilRelaySocket,

    /// New: an allocation of a TCP relay (RFC 6062), which is not ported.
    #[error("TCP relay allocations (RFC 6062) are not supported")]
    TcpRelayNotSupported,

    /// What the relay address allocator failed with.
    #[error(transparent)]
    Allocate(BoxError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
