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
use crate::webrtc::interceptor::RtpHeaderExtension;
use crate::webrtc::rtp;
use crate::webrtc::track;
use crate::webrtc::{
    Error, MIME_TYPE_FLEX_FEC, RtpCodecParameters, RtpCodecType, RtpHeaderExtensionParameter,
    RtpParameters, RtpSendParameters, Ssrc, StreamInfo, TrackLocal, TrackLocalContext,
    TrackLocalWriter,
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
    ///
    /// The track of its sender is unbound and what it receives is unbound at the sink, both
    /// from the connection's task; its direction is inactive from here on, which the next
    /// negotiation tells the other side. Stopping it again is nothing.
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

    fn stop(&self) -> Result<(), Error> {
        RtpTransceiver::stop(self)
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
    /// A sender with this track, or with none (of a transceiver made from a kind).
    pub(crate) fn new(
        pc: Weak<Inner>,
        index: usize,
        track: Option<Arc<dyn TrackLocal>>,
    ) -> RtpSender {
        RtpSender {
            pc,
            index,
            state: Mutex::new(SenderState {
                track,
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

    /// Takes another track, or none. The track it had is given back as `stop` gives it: to
    /// be unbound, or let go of, outside of the connection's mutex.
    fn replace(&self, track: Option<Arc<dyn TrackLocal>>) -> Option<Delivery> {
        let mut state = self.state.lock();
        let replaced = std::mem::replace(&mut state.track, track)?;
        Some(match state.context.take() {
            Some(context) => {
                context.close();
                Delivery::Unbind {
                    track: replaced,
                    context,
                }
            }
            None => Delivery::Release(replaced),
        })
    }

    /// Track returns the RTCRtpTransceiver track, or nil.
    pub fn track(&self) -> Option<Arc<dyn TrackLocal>> {
        self.state.lock().track.clone()
    }

    /// ReplaceTrack replaces the track currently being used as the sender's source with a new TrackLocal.
    /// The new track must be of the same media kind (audio, video, etc) and switching the track should not
    /// require negotiation.
    ///
    /// The track the sender had is unbound as when it is removed (its writer writes no more),
    /// and the new one is bound when tracks are bound: at once if the sender's media section
    /// was negotiated, the connection is up and no negotiation is under way, otherwise when
    /// that has come about. It is bound with what the media section has: the codecs that
    /// were negotiated for it, its SSRC and that of its retransmission stream. `None` leaves
    /// the sender without a track, to be given one later.
    ///
    /// Both calls come from the connection's task, after this has returned or while it
    /// does. So, unlike pion's, this does not say whether the new track could be bound: an
    /// error of its `bind` is logged, and the track the sender had is not put back. What is
    /// refused here: a track of another kind, a sender that was stopped (its track removed,
    /// its transceiver stopped), a connection that was closed.
    pub fn replace_track(&self, track: Option<Arc<dyn TrackLocal>>) -> Result<(), Error> {
        let pc = self.pc.upgrade().ok_or(Error::ConnectionClosed)?;
        // What the track says about itself is asked before the mutex is taken: it is the
        // application's code.
        let described = track
            .as_ref()
            .map(|track| (track.stream_id(), track.id(), track.kind()));
        pc.act(|inner, core, _| {
            core.check_open()?;
            let stopped = || Error::Other("Sender has already been stopped".to_string());
            if self.state.lock().stopped {
                return Err(stopped());
            }
            if let Some((stream_id, id, kind)) = described {
                let mut rtc_sender = core
                    .rtc
                    .rtp_sender(RTCRtpSenderId::from(self.index))
                    .ok_or_else(stopped)?;
                // `rtc` has the SSRCs of the stream with the track it was given, and writes
                // the track's names into the next description. The new track takes the
                // place of the old one with the same encodings. A track of another kind is
                // refused here.
                let codings = rtc_sender.track().codings().to_vec();
                rtc_sender.replace_track(MediaStreamTrack::new(
                    stream_id,
                    id.clone(),
                    id,
                    convert::codec_type_to_rtc(kind),
                    codings,
                ))?;
            }
            core.out.extend(self.replace(track));
            inner.bind_sender(core, self);
            Ok(())
        })
    }

    /// GetParameters describes the current configuration for the encoding and
    /// transmission of media on the sender's track.
    ///
    /// The codecs are the transceiver's, as pion's: its codec preferences, or the media
    /// engine's codecs of its kind where none are set (after a negotiation, what was
    /// negotiated). The header extensions are those of its kind; the one encoding has the SSRC
    /// and that of the retransmission stream.
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

    fn replace_track(&self, track: Option<Arc<dyn TrackLocal>>) -> Result<(), Error> {
        RtpSender::replace_track(self, track)
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
    ssrc_forward_error_correction: Ssrc,
    writer: Arc<Writer>,
    // The stream as the sink was told of it when the track was bound, until it is unbound.
    stream_info: Mutex<Option<StreamInfo>>,
}

impl SenderContext {
    pub(crate) fn new(pc: Weak<Inner>, parameters: RtpSendParameters) -> SenderContext {
        let encoding = parameters.encodings.first();
        SenderContext {
            ssrc: encoding.map_or(0, |e| e.ssrc),
            ssrc_retransmission: encoding.map_or(0, |e| e.rtx.ssrc),
            ssrc_forward_error_correction: encoding.map_or(0, |e| e.fec.ssrc),
            codecs: Mutex::new(parameters.rtp_parameters.codecs),
            header_extensions: parameters.rtp_parameters.header_extensions,
            writer: Arc::new(Writer {
                pc,
                closed: AtomicBool::new(false),
            }),
            stream_info: Mutex::new(None),
        }
    }

    /// The track was bound and chose this codec. Gives the stream as pion's `RTPSender.Send`
    /// describes it to the interceptors (`createStreamInfo`): with the payload types of the
    /// retransmission and of the forward error correction that go with the codec, among the
    /// codecs the track was offered. After that the context has the codec the track chose,
    /// as pion's; a track that named none leaves the context as it was.
    pub(crate) fn bound_as(&self, codec: RtpCodecParameters) -> StreamInfo {
        let mut codecs = self.codecs.lock();
        // pion's `findRTXPayloadType` and `findFECPayloadType`
        let apt = format!("apt={}", codec.payload_type);
        let payload_type_retransmission = codecs
            .iter()
            .find(|c| c.sdp_fmtp_line == apt)
            .map_or(0, |c| c.payload_type);
        let payload_type_forward_error_correction = codecs
            .iter()
            .find(|c| c.mime_type.contains(MIME_TYPE_FLEX_FEC))
            .map_or(0, |c| c.payload_type);
        let info = StreamInfo {
            ssrc: self.ssrc,
            ssrc_retransmission: self.ssrc_retransmission,
            ssrc_forward_error_correction: self.ssrc_forward_error_correction,
            payload_type: codec.payload_type,
            payload_type_retransmission,
            payload_type_forward_error_correction,
            rtp_header_extensions: self
                .header_extensions
                .iter()
                .map(|e| RtpHeaderExtension {
                    uri: e.uri.clone(),
                    id: e.id,
                })
                .collect(),
            mime_type: codec.mime_type.clone(),
            clock_rate: codec.clock_rate,
            channels: codec.channels,
            sdp_fmtp_line: codec.sdp_fmtp_line.clone(),
            rtcp_feedback: codec.rtcp_feedback.clone(),
        };
        if !codec.mime_type.is_empty() {
            *codecs = vec![codec];
        }
        *self.stream_info.lock() = Some(info.clone());
        info
    }

    /// The stream the sink was told of when the track was bound, once: for telling it that
    /// the track was unbound.
    pub(crate) fn take_stream_info(&self) -> Option<StreamInfo> {
        self.stream_info.lock().take()
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
