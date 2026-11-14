// Ported from pion/rtp, error.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the errors are one enum. It also has the two errors
// that audiolevelextension.go and playoutdelayextension.go define, and Go's
// `io.ErrShortBuffer`, which the original returns as it is. The errors that the original wraps
// with numbers (`fmt.Errorf("... %w ...")`) carry those numbers, and their texts are the
// wrapped ones.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The original wraps this one in two ways, as "%w: %d < %d" and as "size %d < %d: %w":
    /// `before` and `after` are what is written around its text.
    #[error("{before}RTP header size insufficient{after}")]
    HeaderSizeInsufficient { before: String, after: String },
    #[error("size {0} < {1}: RTP header size insufficient for extension")]
    HeaderSizeInsufficientForExtension(usize, usize),
    #[error("buffer too small")]
    TooSmall,
    #[error("h.Extension not enabled")]
    HeaderExtensionsNotEnabled,
    #[error("extension not found")]
    HeaderExtensionNotFound,

    #[error(
        "header extension id must be between 1 and 14 for RFC 5285 one byte extensions actual({0})"
    )]
    Rfc8285OneByteHeaderIdRange(u8),
    #[error(
        "header extension payload must be 16bytes or less for RFC 5285 one byte extensions actual({0})"
    )]
    Rfc8285OneByteHeaderSize(usize),

    #[error(
        "header extension id must be between 1 and 255 for RFC 5285 two byte extensions actual({0})"
    )]
    Rfc8285TwoByteHeaderIdRange(u8),
    #[error(
        "header extension payload must be 255bytes or less for RFC 5285 two byte extensions actual({0})"
    )]
    Rfc8285TwoByteHeaderSize(usize),

    #[error("header extension id must be 0 for non-RFC 5285 extensions actual({0})")]
    Rfc3550HeaderIdRange(u8),

    #[error("invalid RTP padding")]
    InvalidRtpPadding,

    /// errAudioLevelOverflow of audiolevelextension.go.
    #[error("audio level overflow")]
    AudioLevelOverflow,

    /// errPlayoutDelayInvalidValue of playoutdelayextension.go.
    #[error("invalid playout delay value")]
    PlayoutDelayInvalidValue,

    /// Go's `io.ErrShortBuffer`.
    #[error("short buffer")]
    ShortBuffer,
}
