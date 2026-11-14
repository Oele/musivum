//! LiveKit's `pkg/sfu/buffer`: the receive side of a stream. Every RTP packet of a
//! publisher's stream goes into its [`Buffer`] (`write`), which extends sequence numbers,
//! keeps statistics and the history that NACKs are answered from, works out layer, key frame
//! and codec header, and hands the packet on as an [`ExtPacket`] to whoever reads from it
//! (`read_extended`); towards the publisher it asks for what is missing and reports what it
//! received. A [`Factory`] has the buffers of a connection by SSRC and pairs repair (RTX)
//! and FEC streams with the stream they belong to. Next to that: the video layer type and
//! the conversions between rids, qualities and spatial layers, the frame integrity checker,
//! the statistics of a data channel, the RTCP reader and the parser of the dependency
//! descriptor.

// The file has the name of its package, as upstream.
#[allow(clippy::module_inception)]
mod buffer;
mod buffer_base;
mod datastats;
mod dependencydescriptorparser;
mod factory;
mod fps;
mod frameintegrity;
mod rtcpreader;
mod streamstats;
mod videolayer;
mod videolayerutils;
mod vp9keyframe;

pub use buffer::*;
pub use buffer_base::*;
pub use datastats::*;
pub use dependencydescriptorparser::*;
pub use factory::*;
pub use fps::*;
pub use frameintegrity::*;
pub use rtcpreader::*;
pub use streamstats::*;
pub use videolayer::*;
pub use videolayerutils::*;
pub use vp9keyframe::*;

/// The errors of the package: its `ErrX` values, the errors that the original makes on the
/// spot (`errors.New`, `fmt.Errorf`), `io.EOF` where the original returns that, and the
/// errors of other packages that the buffer passes on.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    // io.EOF
    #[error("EOF")]
    Eof,

    // dependencydescriptorparser.go
    #[error("frame is earlier than current keyframe")]
    FrameEarlierThanKeyFrame,
    #[error("dependency descriptor structure is attached to non-first packet of a frame")]
    DdStructureAttachedToNonFirstPacket,
    #[error("dependency descriptor extension not found")]
    DdExtentionNotFound,
    // an error of the reader of the dependency descriptor, which the parser passes on
    #[error(transparent)]
    DependencyDescriptor(#[from] crate::sfu::rtpextension::dependencydescriptor::Error),

    // buffer.go
    #[error("invalid codec")]
    InvalidCodec,
    #[error("fec media packet not found")]
    FecMediaPacketNotFound,

    // buffer_base.go, which makes them on the spot
    #[error("cannot start on rtx packet")]
    CannotStartOnRtxPacket,
    #[error("unhandled reason: {0}")]
    UnhandledReason(crate::sfu::rtpstats::RtpFlowUnhandledReason),
    #[error("padding only packet")]
    PaddingOnlyPacket,
    #[error("could not get ext packet")]
    CouldNotGetExtPacket,
    #[error("invalid H265 packet")]
    InvalidH265Packet,

    // errors of what the buffer stands on, which it passes on: of the RTP packet, the
    // bucket, the map of sequence number ranges, and the VP8 and VP9 payload descriptors
    #[error(transparent)]
    Rtp(#[from] crate::webrtc::rtp::Error),
    #[error(transparent)]
    Bucket(#[from] crate::mediatransportutil::bucket::Error),
    #[error(transparent)]
    RangeMap(#[from] crate::sfu::utils::Error),
    #[error(transparent)]
    Codec(#[from] crate::mediatransportutil::codec::Error),
    #[error(transparent)]
    Vp9(#[from] crate::webrtc::rtp::codecs::Error),
}
