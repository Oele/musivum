// Ported from pion/webrtc (as LiveKit builds it: livekit/webrtc-pion v4.2.18-warp.7),
// track_local.go, track_remote.go, rtpsender.go, rtpreceiver.go, rtptransceiver.go,
// rtpsendparameters.go, rtpencodingparameters.go and errors.go; `StreamInfo` is from
// pion/interceptor, streaminfo.go at v0.1.49 (its `RTPHeaderExtension` is in
// `webrtc::interceptor`).
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. Only the declarations are here, and of them only what
// LiveKit's `pkg/sfu` calls: pion's structs `TrackRemote`, `RTPSender`, `RTPReceiver` and
// `RTPTransceiver` are traits with those methods, so that a test can stand in for the peer
// connection (`webrtc::peerconnection` has the real ones). Left out because nothing calls
// them: `TrackLocalWriter.Write`, `TrackLocalContext.ID`, `.RTCPReader` and
// `.SSRCForwardErrorCorrection`, `StreamInfo.ID` and `.Attributes`. Of the errors only the
// four that LiveKit's code names are here, and `Other` for what the library underneath says.
// `PacketSink` is new: it stands where pion has its buffer factory.

//! The seam between the ported media code and the peer connection: what `pkg/sfu` takes from
//! pion's `webrtc` package as behaviour. A down track is a [`TrackLocal`] that the connection
//! binds; what it writes goes through a [`TrackLocalWriter`]; a published stream is a
//! [`TrackRemote`] with an [`RtpReceiver`]; what a connection receives comes out at a
//! [`PacketSink`].
//!
//! Nothing here knows the WebRTC library.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bytes::Bytes;

use super::interceptor::RtpHeaderExtension;
use super::{
    RtcpFeedback, RtpCodecParameters, RtpCodecType, RtpCodingParameters,
    RtpHeaderExtensionParameter, RtpParameters, Ssrc,
};
use super::{rtcp, rtp};
use crate::go::time::Time;

/// The errors of pion's `webrtc` package that LiveKit's code tells apart, and Go's
/// `io.ErrClosedPipe`, which a write to a connection that is gone (or not up yet) returns.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// ErrConnectionClosed indicates an operation executed after connection
    /// has already been closed.
    #[error("connection closed")]
    ConnectionClosed,
    /// ErrCodecNotFound is returned when a codec search to the Media Engine fails.
    #[error("codec not found")]
    CodecNotFound,
    /// ErrUnsupportedCodec indicates the remote peer doesn't support the requested codec.
    #[error("unable to start track, codec is not supported by remote")]
    UnsupportedCodec,
    /// Go's `io.ErrClosedPipe`.
    #[error("io: read/write on closed pipe")]
    ClosedPipe,
    /// What the WebRTC library said, as text.
    #[error("{0}")]
    Other(String),
}

/// TrackLocalWriter is the Writer for outbound RTP Packets.
pub trait TrackLocalWriter: Send + Sync {
    /// WriteRTP encrypts a RTP packet and writes to the connection
    ///
    /// The packet goes out with the SSRC, payload type and sequence number the header has
    /// (the track's own stream or its retransmission stream), followed by
    /// `header.padding_size` octets of padding when `header.padding` is set. What is returned
    /// is the size of the packet before encryption. [`Error::ClosedPipe`] says that the
    /// connection is not up, or that the track was unbound.
    fn write_rtp(&self, header: &rtp::Header, payload: &[u8]) -> Result<usize, Error>;
}

/// TrackLocalContext is the Context passed when a TrackLocal has been Binded/Unbinded from a PeerConnection, and used
/// in Interceptors.
pub trait TrackLocalContext: Send + Sync {
    /// CodecParameters returns the negotiated RTPCodecParameters. These are the codecs supported by both
    /// PeerConnections and the PayloadTypes
    fn codec_parameters(&self) -> Vec<RtpCodecParameters>;

    /// HeaderExtensions returns the negotiated RTPHeaderExtensionParameters. These are the header extensions supported by
    /// both PeerConnections and the URI/IDs
    fn header_extensions(&self) -> Vec<RtpHeaderExtensionParameter>;

    /// SSRC returns the negotiated SSRC of this track
    fn ssrc(&self) -> Ssrc;

    /// SSRCRetransmission returns the negotiated SSRC used to send retransmissions for this track
    ///
    /// 0 when no retransmission stream was negotiated.
    fn ssrc_retransmission(&self) -> Ssrc;

    /// WriteStream returns the WriteStream for this TrackLocal. The implementer writes the outbound
    /// media packets to it
    fn write_stream(&self) -> Arc<dyn TrackLocalWriter>;
}

/// TrackLocal is an interface that controls how the user can send media
/// The user can provide their own TrackLocal implementations, or use
/// the implementations in pkg/media.
pub trait TrackLocal: Send + Sync {
    /// Bind should implement the way how the media data flows from the Track to the PeerConnection
    /// This will be called internally after signaling is complete and the list of available
    /// codecs has been determined
    ///
    /// The context may be kept: LiveKit's down track finishes binding later, when its
    /// receiver is ready.
    fn bind(&self, t: Arc<dyn TrackLocalContext>) -> Result<RtpCodecParameters, Error>;

    /// Unbind should implement the teardown logic when the track is no longer needed. This happens
    /// because a track has been stopped.
    fn unbind(&self, t: Arc<dyn TrackLocalContext>) -> Result<(), Error>;

    /// ID is the unique identifier for this Track. This should be unique for the
    /// stream, but doesn't have to globally unique. A common example would be 'audio' or 'video'
    /// and StreamID would be 'desktop' or 'webcam'
    fn id(&self) -> String;

    /// RID is the RTP Stream ID for this track.
    fn rid(&self) -> String;

    /// StreamID is the group this track belongs too. This must be unique
    fn stream_id(&self) -> String;

    /// Kind controls if this TrackLocal is audio or video
    fn kind(&self) -> RtpCodecType;
}

/// TrackRemote represents a single inbound source of media.
///
/// These are the methods of pion's struct that LiveKit's `sfu.TrackRemote` interface names.
/// `Read`, which LiveKit uses once to wait for a first packet, has no place here: the
/// connection announces a track when its first packet has come.
pub trait TrackRemote: Send + Sync {
    /// ID is the unique identifier for this Track. This should be unique for the
    /// stream, but doesn't have to globally unique. A common example would be 'audio' or 'video'
    /// and StreamID would be 'desktop' or 'webcam'
    fn id(&self) -> String;

    /// RID gets the RTP Stream ID of this Track
    /// With Simulcast you will have multiple tracks with the same ID, but different RID values.
    /// In many cases a TrackRemote will not have an RID, so it is important to assert it is non-zero
    fn rid(&self) -> String;

    /// Msid gets the Msid of the track.
    fn msid(&self) -> String;

    /// SSRC gets the SSRC of the track.
    fn ssrc(&self) -> Ssrc;

    /// RtxSSRC returns the RTX SSRC for a track, or 0 if track does not have a separate RTX stream.
    fn rtx_ssrc(&self) -> Ssrc;

    /// StreamID is the group this track belongs too. This must be unique.
    fn stream_id(&self) -> String;

    /// Kind gets the Kind of the track.
    fn kind(&self) -> RtpCodecType;

    /// Codec gets the Codec of the track.
    fn codec(&self) -> RtpCodecParameters;
}

/// RTPEncodingParameters provides information relating to both encoding and decoding.
/// This is a subset of the RFC since Pion WebRTC doesn't implement encoding itself
/// http://draft.ortc.org/#dom-rtcrtpencodingparameters
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtpEncodingParameters {
    pub rtp_coding_parameters: RtpCodingParameters,
}

impl Deref for RtpEncodingParameters {
    type Target = RtpCodingParameters;
    fn deref(&self) -> &RtpCodingParameters {
        &self.rtp_coding_parameters
    }
}

impl DerefMut for RtpEncodingParameters {
    fn deref_mut(&mut self) -> &mut RtpCodingParameters {
        &mut self.rtp_coding_parameters
    }
}

/// RTPSendParameters contains the RTP stack settings used by receivers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtpSendParameters {
    pub rtp_parameters: RtpParameters,
    pub encodings: Vec<RtpEncodingParameters>,
}

impl Deref for RtpSendParameters {
    type Target = RtpParameters;
    fn deref(&self) -> &RtpParameters {
        &self.rtp_parameters
    }
}

impl DerefMut for RtpSendParameters {
    fn deref_mut(&mut self) -> &mut RtpParameters {
        &mut self.rtp_parameters
    }
}

/// RTPSender allows an application to control how a given Track is encoded and transmitted to a remote peer.
pub trait RtpSender: Send + Sync {
    /// GetParameters describes the current configuration for the encoding and
    /// transmission of media on the sender's track.
    fn get_parameters(&self) -> RtpSendParameters;
}

/// An RTPReceiver allows an application to inspect the receipt of a TrackRemote.
pub trait RtpReceiver: Send + Sync {
    /// GetParameters describes the current configuration for the encoding and
    /// transmission of media on the receiver's track.
    fn get_parameters(&self) -> RtpParameters;
}

/// RTPTransceiver represents a combination of an RTPSender and an RTPReceiver that share a common mid.
pub trait RtpTransceiver: Send + Sync {
    /// Sender returns the RTPTransceiver's RTPSender if it has one.
    fn sender(&self) -> Option<Arc<dyn RtpSender>>;

    /// Mid gets the Transceiver's Mid value. When not already set, this value will be set in CreateOffer or CreateAnswer.
    fn mid(&self) -> String;
}

/// StreamInfo is the Context passed when a StreamLocal or StreamRemote has been Binded or Unbinded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamInfo {
    pub ssrc: u32,
    pub ssrc_retransmission: u32,
    pub ssrc_forward_error_correction: u32,
    pub payload_type: u8,
    pub payload_type_retransmission: u8,
    pub payload_type_forward_error_correction: u8,
    pub rtp_header_extensions: Vec<RtpHeaderExtension>,
    pub mime_type: String,
    pub clock_rate: u32,
    pub channels: u16,
    pub sdp_fmtp_line: String,
    pub rtcp_feedback: Vec<RtcpFeedback>,
}

/// Where a connection hands over what it receives: the counterpart of pion's buffer factory
/// (`SettingEngine.BufferFactory`, through which LiveKit gets every packet per SSRC right
/// after decryption) and of `BindRemoteStream` of pion's interceptors, which is how LiveKit
/// learns what a stream is.
///
/// A connection calls its sink from one place at a time and in the order things happened; it
/// holds no lock of its own while it does, so a sink may call back into the connection.
pub trait PacketSink: Send + Sync {
    /// An RTP packet as it arrived, whole, on whatever SSRC: media, retransmissions, forward
    /// error correction and padding-only packets alike. `arrival` is when the datagram was
    /// read from the socket.
    fn on_rtp(&self, ssrc: Ssrc, packet: Bytes, arrival: Time);

    /// A compound RTCP packet as it arrived: reports and feedback about what the connection
    /// receives and about what it sends. `rtcp::packet::marshal` makes the bytes of it, for
    /// code that reads RTCP as pion hands it over.
    fn on_rtcp(&self, packets: &[Box<dyn rtcp::Packet>], arrival: Time);

    /// BindRemoteStream of pion's interceptors: a stream of the remote side was identified.
    /// It comes before the first packet of that stream.
    fn bind_remote_stream(&self, _info: &StreamInfo) {}

    /// UnbindRemoteStream of pion's interceptors: the stream was removed.
    fn unbind_remote_stream(&self, _info: &StreamInfo) {}
}
