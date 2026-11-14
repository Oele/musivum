//! LiveKit's `pkg/sfu/interceptor`, and what feeds it: the part of a connection between the
//! peer connection and the buffers.
//!
//! Upstream the files of this package are pion interceptors: objects pion calls when a remote
//! stream is bound and whose readers it passes a stream's packets through. musivum's
//! connection has no interceptors of the application's; it has one [`crate::webrtc::PacketSink`].
//! [`BufferFactorySink`] is that sink. It writes what the connection receives into the
//! buffers of a [`crate::sfu::buffer::Factory`] and calls what the interceptors became:
//!
//! - [`RtxInfoExtractor`] (`rtx.go`) learns which stream repairs which;
//! - [`RttFromXr`] (`rttfromxr.go` of LiveKit's mediatransportutil) asks the other side for
//!   the round trip time with RTCP extended reports;
//! - [`UnhandleSimulcastInterceptor`] (`unhandlesimulcast.go`) is ported but has nothing to
//!   call it: see its header.

mod rttfromxr;
mod rtx;
mod sink;
mod unhandlesimulcast;

pub use rttfromxr::*;
pub use rtx::*;
pub use sink::*;
pub use unhandlesimulcast::*;
