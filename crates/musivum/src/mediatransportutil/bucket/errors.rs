// Ported from LiveKit's mediatransportutil, pkg/bucket/errors.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum. Those that the bucket wraps
// with details (`fmt.Errorf("%w, ...")`) carry as text what follows the `%w` there, the comma
// included, so that an empty text gives the bare error of the original. `errors.Is(err, ErrX)`
// is `matches!(err, Error::X(_))`.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("no bucket")]
    NoBucket,
    #[error("buffer too small")]
    BufferTooSmall,
    #[error("received packet too old{0}")]
    PacketTooOld(String),
    #[error("received packet too new{0}")]
    PacketTooNew(String),
    #[error("packet already received")]
    RtxPacket,
    #[error("packet already received, size mismatch{0}")]
    RtxPacketSize(String),
    #[error("sequence number mismatch{0}")]
    PacketMismatch(String),
    #[error("invalid size{0}")]
    PacketSizeInvalid(String),
    #[error("packet too large")]
    PacketTooLarge,
}
