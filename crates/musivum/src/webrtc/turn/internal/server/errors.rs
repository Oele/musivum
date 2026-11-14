// Ported from pion/turn, internal/server/errors.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust: one `Error` enum, its variants the package's `errX`
// values; one that the original wraps with details (`fmt.Errorf("%w ...")`) carries them, as
// text where the original writes them with `%v`. Errors of other packages that the original
// returns as they are (STUN, the allocations, I/O) are transparent variants.
// `errUnexpectedMethod` and `errFailedToHandle` are what Connect and ConnectionBind (RFC 6062,
// not ported) get here.

use std::io;

use crate::webrtc::stun::Error as StunError;
use crate::webrtc::turn::internal::allocation;
use crate::webrtc::turn::internal::proto;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to generate nonce")]
    FailedToGenerateNonce,
    #[error("invalid nonce")]
    InvalidNonce,
    // fmt.Errorf("%w: %v", errInvalidNonce, err)
    #[error("invalid nonce: {0}")]
    InvalidNonceWith(String),
    // fmt.Errorf("%w %v %v", errFailedToSendError, sendErr, err)
    #[error("failed to send error message {0} {1}")]
    FailedToSendError(String, String),
    // fmt.Errorf("%w %s", errNoSuchUser, username)
    #[error("no such user exists {0}")]
    NoSuchUser(String),
    #[error("unexpected class: {0}")]
    UnexpectedClass(String),
    #[error("unexpected method: {0}")]
    UnexpectedMethod(String),
    // fmt.Errorf("%w %v-%v from %v: %v", errFailedToHandle, method, class, srcAddr, err)
    #[error("failed to handle {0}: {1}")]
    FailedToHandle(String, String),
    // fmt.Errorf("%w %v-%v from %v: %v", errUnhandledSTUNPacket, method, class, srcAddr, err)
    #[error("unhandled STUN packet {0}: {1}")]
    UnhandledStunPacket(String, String),
    // fmt.Errorf("%w from %v: %v", errUnableToHandleChannelData, srcAddr, err)
    #[error("unable to handle ChannelData from {0}: {1}")]
    UnableToHandleChannelData(String, String),
    #[error("failed to create stun message from packet: {0}")]
    FailedToCreateStunPacket(String),
    #[error("failed to create channel data from packet: {0}")]
    FailedToCreateChannelData(String),
    #[error("relay already allocated for 5-TUPLE")]
    RelayAlreadyAllocatedForFiveTuple,
    #[error("RequestedTransport must be UDP or TCP")]
    UnsupportedTransportProtocol,
    #[error("no support for DONT-FRAGMENT")]
    NoDontFragmentSupport,
    #[error("Request must not contain RESERVATION-TOKEN and EVEN-PORT")]
    RequestWithReservationTokenAndEvenPort,
    #[error("Request must not contain RESERVATION-TOKEN and REQUESTED-ADDRESS-FAMILY")]
    RequestWithReservationTokenAndRequestedFamily,
    #[error("unsupported address family")]
    UnsupportedAddressFamily,
    #[error("peer address family does not match allocation")]
    PeerAddressFamilyMismatch,
    #[error("no allocation found")]
    NoAllocationFound,
    // fmt.Errorf("%w %v:%v", errNoAllocationFound, srcAddr, localAddr)
    #[error("no allocation found {0}")]
    NoAllocationFoundFor(String),
    // fmt.Errorf("%w: %v", errNoPermission, msgDst)
    #[error("unable to handle send-indication, no permission added: {0}")]
    NoPermission(String),
    // fmt.Errorf("%w %d != %d (expected)", errShortWrite, l, len(data))
    #[error("packet write smaller than packet {0} != {1} (expected)")]
    ShortWrite(usize, usize),
    // fmt.Errorf("%w %x", errNoSuchChannelBind, uint16(number))
    #[error("no such channel bind {0:x}")]
    NoSuchChannelBind(u16),
    // fmt.Errorf("%w: %s", errFailedWriteSocket, err.Error())
    #[error("failed writing to socket: {0}")]
    FailedWriteSocket(String),

    // ipnet's errFailedToCastAddr
    #[error("failed to cast net.Addr to *net.UDPAddr or *net.TCPAddr")]
    FailedToCastAddr,

    #[error(transparent)]
    Stun(#[from] StunError),
    #[error(transparent)]
    Allocation(#[from] allocation::Error),
    #[error(transparent)]
    Proto(#[from] proto::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}
