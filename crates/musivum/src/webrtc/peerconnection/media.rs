//! The media objects of a connection: transceivers, senders, receivers, remote tracks, and
//! what a bound local track is given (its context and its writer).
//!
//! `rtc` has these as indexes into the connection. Here they are objects that can be kept,
//! as pion's are, and that ask the connection when they are asked something.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::event::RTCTrackEventInit;
use rtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters,
};
use rtc::rtp_transceiver::{RTCRtpReceiverId, RTCRtpSenderId};

use super::{Delivery, Inner, RtpTransceiverDirection, convert};
use crate::webrtc::rtp;
use crate::webrtc::track;
use crate::webrtc::{
    Error, RtpCodecParameters, RtpCodecType, RtpHeaderExtensionParameter, RtpParameters,
    RtpSendParameters, Ssrc, TrackLocal, TrackLocalContext, TrackLocalWriter,
};

/// pion's `RTPTransceiverInit`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RtpTransceiverInit {
    pub direction: RtpTransceiverDirection,
}

/// The one encoding of a track that is sent. `rtc` chooses the SSRC, and that of the
/// retransmission stream if the media engine has a codec for one.
pub(crate) fn send_encoding(rid: &str) -> RTCRtpEncodingParameters {
    RTCRtpEncodingParameters {
        rtp_coding_parameters: RTCRtpCodingParameters {
            rid: rid.to_string(),
            ssrc: None,
            rtx: None,
            fec: None,
        },
        // No codec: which one it is, the track says when it is bound, from what was
        // negotiated. Until then the media section offers what the media engine has, or
        // what the transceiver's codec preferences say.
        codec: RTCRtpCodec::default(),
        ..Default::default()
    }
}

/// A local track as `rtc` wants to be told about it.
pub(crate) fn media_stream_track(track: &dyn TrackLocal) -> MediaStreamTrack {
    let id = track.id();
    MediaStreamTrack::new(
        track.stream_id(),
        id.clone(),
        id,
        convert::codec_type_to_rtc(track.kind()),
        vec![send_encoding(&track.rid())],
    )
}

/// pion's `RTPTransceiver`: a media section of the session, with what is sent in it and
/// what is received.
pub struct RtpTransceiver {
    pc: Weak<Inner>,
    index: usize,
    kind: Mutex<RtpCodecType>,
    sender: Mutex<Option<Arc<RtpSender>>>,
    receiver: Arc<RtpReceiver>,
}

impl RtpTransceiver {
    pub(crate) fn new(pc: Weak<Inner>, index: usize) -> RtpTransceiver {
        RtpTransceiver {
            receiver: Arc::new(RtpReceiver {
                pc: pc.clone(),
                index,
                tracks: Mutex::new(Vec::new()),
            }),
            pc,
            index,
            kind: Mutex::new(RtpCodecType::Unknown),
            sender: Mutex::new(None),
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn kind_hint(&self) -> RtpCodecType {
        *self.kind.lock()
    }

    pub(crate) fn set_kind(&self, kind: RtpCodecType) {
        if kind != RtpCodecType::Unknown {
            *self.kind.lock() = kind;
        }
    }

    pub(crate) fn set_sender(&self, sender: Option<Arc<RtpSender>>) {
        *self.sender.lock() = sender;
    }

    /// Sender returns the RTPTransceiver's RTPSender if it has one.
    pub fn sender(&self) -> Option<Arc<RtpSender>> {
        self.sender.lock().clone()
    }

    /// Receiver returns the RTPTransceiver's RTPReceiver if it has one.
    pub fn receiver(&self) -> Option<Arc<RtpReceiver>> {
        Some(self.receiver.clone())
    }

    /// Mid gets the Transceiver's Mid value. When not already set, this value will be set in CreateOffer or CreateAnswer.
    pub fn mid(&self) -> String {
        let Some(pc) = self.pc.upgrade() else {
            return String::new();
        };
        pc.read(|core| {
            core.rtc
                .rtp_transceiver(self.index)
                .and_then(|t| t.mid().clone())
                .unwrap_or_default()
        })
    }

    /// Kind returns RTPTransceiver's kind.
    pub fn kind(&self) -> RtpCodecType {
        *self.kind.lock()
    }

    /// Direction returns the RTPTransceiver's current direction.
    pub fn direction(&self) -> RtpTransceiverDirection {
        let Some(pc) = self.pc.upgrade() else {
            return RtpTransceiverDirection::Unspecified;
        };
        pc.read(|core| {
            core.rtc
                .rtp_transceiver(self.index)
                .map_or(RtpTransceiverDirection::Unspecified, |t| t.direction())
        })
    }

    /// The direction the last negotiation gave it (the W3C's `currentDirection`).
    pub fn current_direction(&self) -> RtpTransceiverDirection {
        let Some(pc) = self.pc.upgrade() else {
            return RtpTransceiverDirection::Unspecified;
        };
        pc.read(|core| {
            core.rtc
                .rtp_transceiver(self.index)
                .map_or(RtpTransceiverDirection::Unspecified, |t| {
                    t.current_direction()
                })
        })
    }

    /// SetCodecPreferences sets preferred list of supported codecs
    /// if codecs is empty or nil we reset to default from MediaEngine.
    pub fn set_codec_preferences(&self, codecs: Vec<RtpCodecParameters>) -> Result<(), Error> {
        let pc = self.pc.upgrade().ok_or(Error::ConnectionClosed)?;
        let codecs = codecs
            .iter()
            .map(convert::codec_parameters_to_rtc)
            .collect();
        pc.act(|_, core, _| {
            core.check_open()?;
            let mut transceiver = core
                .rtc
                .rtp_transceiver(self.index)
                .ok_or(Error::ConnectionClosed)?;
            transceiver.set_codec_preferences(codecs)?;
            Ok(())
        })
    }

    /// Stop irreversibly stops the RTPTransceiver.
    pub fn stop(&self) -> Result<(), Error> {
        let pc = self.pc.upgrade().ok_or(Error::ConnectionClosed)?;
        pc.act(|_, core, _| {
            core.check_open()?;
            let mut transceiver = core
                .rtc
                .rtp_transceiver(self.index)
                .ok_or(Error::ConnectionClosed)?;
            transceiver.stop()?;
            if let Some(sender) = self.sender() {
                core.out.extend(sender.stop());
            }
            Ok(())
        })
    }
}

impl track::RtpTransceiver for RtpTransceiver {
    fn sender(&self) -> Option<Arc<dyn track::RtpSender>> {
        let sender = RtpTransceiver::sender(self)?;
        Some(sender)
    }

    fn mid(&self) -> String {
        RtpTransceiver::mid(self)
    }
}

/// What a sender is doing with its track.
struct SenderState {
    track: Option<Arc<dyn TrackLocal>>,
    // Set while the track is bound.
    context: Option<Arc<SenderContext>>,
    stopped: bool,
}

/// pion's `RTPSender`: a local track in a media section.
pub struct RtpSender {
    pc: Weak<Inner>,
    index: usize,
    state: Mutex<SenderState>,
}

impl RtpSender {
    pub(crate) fn new(pc: Weak<Inner>, index: usize, track: Arc<dyn TrackLocal>) -> RtpSender {
        RtpSender {
            pc,
            index,
            state: Mutex::new(SenderState {
                track: Some(track),
                context: None,
                stopped: false,
            }),
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn wants_binding(&self) -> bool {
        let state = self.state.lock();
        state.track.is_some() && state.context.is_none() && !state.stopped
    }

    /// Notes that the track is being bound with this context, and gives the track.
    pub(crate) fn bound(&self, context: Arc<SenderContext>) -> Option<Arc<dyn TrackLocal>> {
        let mut state = self.state.lock();
        if state.stopped || state.context.is_some() {
            return None;
        }
        let track = state.track.clone()?;
        state.context = Some(context);
        Some(track)
    }

    /// The end of the sender (its track was removed, or the connection closed). Nothing can
    /// be written from here on. The track is given back, with its context if it was bound:
    /// it is the application's, to be unbound and let go of outside of the connection's
    /// mutex.
    pub(crate) fn stop(&self) -> Option<Delivery> {
        let mut state = self.state.lock();
        state.stopped = true;
        let track = state.track.take()?;
        Some(match state.context.take() {
            Some(context) => {
                context.close();
                Delivery::Unbind { track, context }
            }
            None => Delivery::Release(track),
        })
    }

    /// Track returns the RTCRtpTransceiver track, or nil.
    pub fn track(&self) -> Option<Arc<dyn TrackLocal>> {
        self.state.lock().track.clone()
    }

    /// GetParameters describes the current configuration for the encoding and
    /// transmission of media on the sender's track.
    ///
    /// The codecs are those negotiated for the sender's media section, the header extensions
    /// those negotiated for its kind; the one encoding has the SSRC and that of the
    /// retransmission stream.
    pub fn get_parameters(&self) -> RtpSendParameters {
        let Some(pc) = self.pc.upgrade() else {
            return RtpSendParameters::default();
        };
        pc.read(|core| {
            core.rtc
                .rtp_sender(RTCRtpSenderId::from(self.index))
                .map(|mut sender| convert::send_parameters_from_rtc(sender.get_parameters()))
                .unwrap_or_default()
        })
    }
}

impl track::RtpSender for RtpSender {
    fn get_parameters(&self) -> RtpSendParameters {
        RtpSender::get_parameters(self)
    }
}

/// pion's `RTPReceiver`: what is received in a media section.
pub struct RtpReceiver {
    pc: Weak<Inner>,
    index: usize,
    tracks: Mutex<Vec<Arc<TrackRemote>>>,
}

impl RtpReceiver {
    pub(crate) fn add_track(&self, track: Arc<TrackRemote>) {
        self.tracks.lock().push(track);
    }

    /// GetParameters describes the current configuration for the encoding and
    /// transmission of media on the receiver's track.
    pub fn get_parameters(&self) -> RtpParameters {
        let Some(pc) = self.pc.upgrade() else {
            return RtpParameters::default();
        };
        pc.read(|core| {
            core.rtc
                .rtp_receiver(RTCRtpReceiverId::from(self.index))
                .map(|mut receiver| {
                    convert::rtp_parameters_from_rtc(&receiver.get_parameters().rtp_parameters)
                })
                .unwrap_or_default()
        })
    }

    /// Track returns the RtpTransceiver TrackRemote.
    ///
    /// The first, when simulcast layers have made several.
    pub fn track(&self) -> Option<Arc<TrackRemote>> {
        self.tracks.lock().first().cloned()
    }

    /// Tracks returns the RtpTransceiver tracks
    /// A RTPReceiver to support Simulcast may now have multiple tracks.
    pub fn tracks(&self) -> Vec<Arc<TrackRemote>> {
        self.tracks.lock().clone()
    }

    /// RTPTransceiver returns the RTPTransceiver this
    /// RTPReceiver belongs too, or nil if none.
    pub fn rtp_transceiver(&self) -> Option<Arc<RtpTransceiver>> {
        let pc = self.pc.upgrade()?;
        pc.read(|core| core.transceivers.get(self.index).cloned())
    }
}

impl track::RtpReceiver for RtpReceiver {
    fn get_parameters(&self) -> RtpParameters {
        RtpReceiver::get_parameters(self)
    }
}

/// pion's `TrackRemote`: a stream the other side sends, as it was when its first packet came.
/// The packets themselves come out at the connection's sink, by SSRC.
#[derive(Debug)]
pub struct TrackRemote {
    id: String,
    stream_id: String,
    rid: String,
    kind: RtpCodecType,
    ssrc: Ssrc,
    rtx_ssrc: AtomicU32,
    codec: RtpCodecParameters,
}

impl TrackRemote {
    pub(crate) fn new(receiver: &mut RTCRtpReceiver<'_>, init: &RTCTrackEventInit) -> TrackRemote {
        let track = receiver.track();
        let kind = convert::codec_type_from_rtc(track.kind());
        let capability = track.codec(init.ssrc).cloned().unwrap_or_default();
        let rtx_ssrc = track
            .codings()
            .iter()
            .find(|coding| coding.rtp_coding_parameters.ssrc == Some(init.ssrc))
            .and_then(|coding| coding.rtp_coding_parameters.rtx.as_ref())
            .map_or(0, |rtx| rtx.ssrc);
        // The track has the codec without its payload type; the receiver's parameters have
        // both.
        let codecs = &receiver.get_parameters().rtp_parameters.codecs;
        let payload_type = codecs
            .iter()
            .find(|c| c.rtp_codec == capability)
            .or_else(|| {
                codecs.iter().find(|c| {
                    c.rtp_codec
                        .mime_type
                        .eq_ignore_ascii_case(&capability.mime_type)
                })
            })
            .map_or(0, |c| c.payload_type);
        TrackRemote {
            id: init.track_id.clone(),
            stream_id: init.stream_ids.first().cloned().unwrap_or_default(),
            rid: init.rid.clone().unwrap_or_default(),
            kind,
            ssrc: init.ssrc,
            rtx_ssrc: AtomicU32::new(rtx_ssrc),
            codec: RtpCodecParameters {
                rtp_codec_capability: convert::codec_capability_from_rtc(&capability),
                payload_type,
            },
        }
    }

    /// ID is the unique identifier for this Track. This should be unique for the
    /// stream, but doesn't have to globally unique. A common example would be 'audio' or 'video'
    /// and StreamID would be 'desktop' or 'webcam'
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// RID gets the RTP Stream ID of this Track
    /// With Simulcast you will have multiple tracks with the same ID, but different RID values.
    /// In many cases a TrackRemote will not have an RID, so it is important to assert it is non-zero
    pub fn rid(&self) -> String {
        self.rid.clone()
    }

    /// Msid gets the Msid of the track.
    pub fn msid(&self) -> String {
        format!("{} {}", self.stream_id, self.id)
    }

    /// SSRC gets the SSRC of the track.
    pub fn ssrc(&self) -> Ssrc {
        self.ssrc
    }

    /// RtxSSRC returns the RTX SSRC for a track, or 0 if track does not have a separate RTX stream.
    ///
    /// Known when the description named it. For a simulcast layer, whose streams are told
    /// apart by their rid only, it stays 0: `rtc` does not pair those.
    pub fn rtx_ssrc(&self) -> Ssrc {
        self.rtx_ssrc.load(Ordering::Relaxed)
    }

    /// StreamID is the group this track belongs too. This must be unique.
    pub fn stream_id(&self) -> String {
        self.stream_id.clone()
    }

    /// Kind gets the Kind of the track.
    pub fn kind(&self) -> RtpCodecType {
        self.kind
    }

    /// PayloadType gets the PayloadType of the track.
    pub fn payload_type(&self) -> u8 {
        self.codec.payload_type
    }

    /// Codec gets the Codec of the track.
    pub fn codec(&self) -> RtpCodecParameters {
        self.codec.clone()
    }
}

impl track::TrackRemote for TrackRemote {
    fn id(&self) -> String {
        TrackRemote::id(self)
    }

    fn rid(&self) -> String {
        TrackRemote::rid(self)
    }

    fn msid(&self) -> String {
        TrackRemote::msid(self)
    }

    fn ssrc(&self) -> Ssrc {
        TrackRemote::ssrc(self)
    }

    fn rtx_ssrc(&self) -> Ssrc {
        TrackRemote::rtx_ssrc(self)
    }

    fn stream_id(&self) -> String {
        TrackRemote::stream_id(self)
    }

    fn kind(&self) -> RtpCodecType {
        TrackRemote::kind(self)
    }

    fn codec(&self) -> RtpCodecParameters {
        TrackRemote::codec(self)
    }
}

/// pion's `TrackLocalWriter` of a sender: writes RTP on the connection, on whatever SSRC the
/// header names.
struct Writer {
    pc: Weak<Inner>,
    closed: AtomicBool,
}

impl TrackLocalWriter for Writer {
    fn write_rtp(&self, header: &rtp::Header, payload: &[u8]) -> Result<usize, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::ClosedPipe);
        }
        let pc = self.pc.upgrade().ok_or(Error::ClosedPipe)?;
        pc.write_rtp(header, payload)
    }
}

/// pion's `TrackLocalContext`: what a track is bound with.
pub(crate) struct SenderContext {
    codecs: Mutex<Vec<RtpCodecParameters>>,
    header_extensions: Vec<RtpHeaderExtensionParameter>,
    ssrc: Ssrc,
    ssrc_retransmission: Ssrc,
    writer: Arc<Writer>,
}

impl SenderContext {
    pub(crate) fn new(pc: Weak<Inner>, parameters: RtpSendParameters) -> SenderContext {
        let encoding = parameters.encodings.first();
        SenderContext {
            ssrc: encoding.map_or(0, |e| e.ssrc),
            ssrc_retransmission: encoding.map_or(0, |e| e.rtx.ssrc),
            codecs: Mutex::new(parameters.rtp_parameters.codecs),
            header_extensions: parameters.rtp_parameters.header_extensions,
            writer: Arc::new(Writer {
                pc,
                closed: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn set_codec(&self, codec: RtpCodecParameters) {
        *self.codecs.lock() = vec![codec];
    }

    /// The writer writes nothing from here on.
    pub(crate) fn close(&self) {
        self.writer.closed.store(true, Ordering::Relaxed);
    }
}

impl TrackLocalContext for SenderContext {
    fn codec_parameters(&self) -> Vec<RtpCodecParameters> {
        self.codecs.lock().clone()
    }

    fn header_extensions(&self) -> Vec<RtpHeaderExtensionParameter> {
        self.header_extensions.clone()
    }

    fn ssrc(&self) -> Ssrc {
        self.ssrc
    }

    fn ssrc_retransmission(&self) -> Ssrc {
        self.ssrc_retransmission
    }

    fn write_stream(&self) -> Arc<dyn TrackLocalWriter> {
        self.writer.clone()
    }
}
