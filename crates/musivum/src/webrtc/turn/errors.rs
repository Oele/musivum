// Ported from pion/turn, errors.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust: one `Error` enum, its variants the server's `errX`
// values (the client's are not here: the client is not ported). `errMinPortNotZero` says
// "MaxPort" as the original does. What `NewServer` and `Close` wrap with details carries them;
// errors of the internal packages and I/O errors are transparent variants.

use std::io;

use super::internal::{allocation, server};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("turn: RelayAddress must be valid IP to use RelayAddressGeneratorStatic")]
    RelayAddressInvalid,
    #[error("turn: PacketConnConfigs and ConnConfigs are empty, unable to proceed")]
    NoAvailableConns,
    #[error("turn: PacketConnConfig must have a non-nil Conn")]
    ConnUnset,
    #[error("turn: ListenerConfig must have a non-nil Listener")]
    ListenerUnset,
    #[error("turn: RelayAddressGenerator has invalid ListeningAddress")]
    ListeningAddressInvalid,
    #[error("turn: RelayAddressGenerator in RelayConfig is unset")]
    RelayAddressGeneratorUnset,
    #[error("turn: max retries exceeded")]
    MaxRetriesExceeded,
    #[error("turn: MaxPort must be not 0")]
    MaxPortNotZero,
    #[error("turn: MaxPort must be not 0")]
    MinPortNotZero,
    #[error("turn: conn cannot not be nil")]
    NilConn,
    // `errFailedToClose` with the errors of the closes: fmt.Errorf("%s; close error (%w) ", ...)
    #[error("turn: Server failed to close{0}")]
    FailedToClose(String),
    #[error("RelayAddressGenerator is nil")]
    RelayAddressGeneratorNil,
    // fmt.Errorf("failed to create AllocationManager: %w", err)
    #[error("failed to create AllocationManager: {0}")]
    FailedToCreateAllocationManager(#[source] allocation::Error),

    #[error(transparent)]
    Server(#[from] server::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}
