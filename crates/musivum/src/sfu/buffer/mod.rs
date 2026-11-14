//! LiveKit's `pkg/sfu/buffer`: the receive side of a stream. Ported so far: the files that
//! stand alone (the video layer type and the conversions between rids, qualities and spatial
//! layers, the frame integrity checker, the statistics of a data channel, the RTCP reader),
//! the packet as the rest of the server sees it (`ExtPacket`) and the types of the
//! dependency descriptor that travel with it.
//!
//! `streamstats.rs` is written but not declared here: it holds `rtpstats::RtpDeltaInfo`,
//! and `sfu/rtpstats` is not ported yet. When it is, `mod streamstats;` and
//! `pub use streamstats::*;` are what is missing.

mod buffer_base;
mod datastats;
mod dependencydescriptorparser;
mod frameintegrity;
mod rtcpreader;
mod videolayer;
mod videolayerutils;

pub use buffer_base::*;
pub use datastats::*;
pub use dependencydescriptorparser::*;
pub use frameintegrity::*;
pub use rtcpreader::*;
pub use videolayer::*;
pub use videolayerutils::*;

/// The errors of the package: its `ErrX` values, to be added as the files that return them
/// are ported, and `io.EOF` where the original returns that.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    // io.EOF
    #[error("EOF")]
    Eof,
}
