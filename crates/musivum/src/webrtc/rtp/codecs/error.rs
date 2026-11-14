// Ported from pion/rtp, codecs/error.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the errors are one enum; only the errors of what is
// ported (`VP9Packet`) are here, `errUnhandledNALUType` and the AV1 error are not.

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("packet is not large enough")]
    ShortPacket,
    #[error("invalid nil packet")]
    NilPacket,
    #[error("too many PDiff")]
    TooManyPDiff,
    #[error("too many spatial layers")]
    TooManySpatialLayers,
}
