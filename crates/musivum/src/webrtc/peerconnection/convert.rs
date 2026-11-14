//! The one place where musivum's types (`crate::webrtc`, ported from pion) and `rtc`'s meet.
//!
//! RTP goes one way only: what the ported code writes is turned into `rtc`'s packet. What
//! arrives is handed over as bytes, which the ported code unmarshals itself.

use bytes::Bytes;

use rtc::data_channel::RTCDataChannelInit;
use rtc::peer_connection::sdp::{RTCSdpType, RTCSessionDescription};
use rtc::rtp_transceiver::rtp_sender::{
    RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpHeaderExtensionParameters,
    RTCRtpParameters, RTCRtpSendParameters, RtpCodecKind,
};

use super::{DataChannelInit, SessionDescription};
use crate::webrtc::interceptor::RtpHeaderExtension;
use crate::webrtc::rtp;
use crate::webrtc::{
    Error, RtcpFeedback, RtpCodecCapability, RtpCodecParameters, RtpCodecType, RtpCodingParameters,
    RtpEncodingParameters, RtpFecParameters, RtpHeaderExtensionParameter, RtpParameters,
    RtpRtxParameters, RtpSendParameters, StreamInfo,
};

impl From<rtc::shared::error::Error> for Error {
    fn from(err: rtc::shared::error::Error) -> Error {
        match err {
            rtc::shared::error::Error::ErrConnectionClosed => Error::ConnectionClosed,
            rtc::shared::error::Error::ErrCodecNotFound => Error::CodecNotFound,
            rtc::shared::error::Error::ErrUnsupportedCodec => Error::UnsupportedCodec,
            other => Error::Other(other.to_string()),
        }
    }
}

/// A description as the signalling has it, parsed for `rtc`.
pub(crate) fn session_description_to_rtc(
    desc: &SessionDescription,
) -> Result<RTCSessionDescription, Error> {
    let sdp = desc.sdp.clone();
    Ok(match desc.r#type {
        RTCSdpType::Offer => RTCSessionDescription::offer(sdp)?,
        RTCSdpType::Answer => RTCSessionDescription::answer(sdp)?,
        RTCSdpType::Pranswer => RTCSessionDescription::pranswer(sdp)?,
        RTCSdpType::Rollback => {
            RTCSessionDescription::rollback(if sdp.is_empty() { None } else { Some(sdp) })?
        }
        _ => {
            return Err(Error::Other(
                "session description without a type".to_string(),
            ));
        }
    })
}

pub(crate) fn session_description_from_rtc(desc: &RTCSessionDescription) -> SessionDescription {
    SessionDescription {
        r#type: desc.sdp_type,
        sdp: desc.sdp.clone(),
    }
}

/// pion's `DataChannelInit` as `rtc`'s: there a negotiated channel is one that has an id.
pub(crate) fn data_channel_init_to_rtc(
    init: &DataChannelInit,
) -> Result<RTCDataChannelInit, Error> {
    let negotiated = match (init.negotiated.unwrap_or(false), init.id) {
        (true, Some(id)) => Some(id),
        (true, None) => {
            return Err(Error::Other(
                "a negotiated data channel needs an id".to_string(),
            ));
        }
        (false, _) => None,
    };
    Ok(RTCDataChannelInit {
        ordered: init.ordered.unwrap_or(true),
        max_packet_life_time: init.max_packet_life_time,
        max_retransmits: init.max_retransmits,
        protocol: init.protocol.clone().unwrap_or_default(),
        negotiated,
    })
}

/// musivum's RTP header and a payload as `rtc`'s packet. The padding size goes along, in the
/// field `third_party/rtc-rtp` adds; the payload is copied once.
pub(crate) fn rtp_packet_to_rtc(header: &rtp::Header, payload: &[u8]) -> rtc::rtp::Packet {
    rtc::rtp::Packet {
        header: rtp_header_to_rtc(header),
        payload: Bytes::copy_from_slice(payload),
    }
}

pub(crate) fn rtp_header_to_rtc(header: &rtp::Header) -> rtc::rtp::Header {
    // The fields of musivum's `Extension` are its own; the ids and then each value are what
    // the header gives out. There are a handful per packet at most.
    let extensions = if header.extension && !header.extensions.is_empty() {
        header
            .get_extension_ids()
            .into_iter()
            .filter_map(|id| {
                header
                    .get_extension(id)
                    .map(|payload| rtc::rtp::header::Extension { id, payload })
            })
            .collect()
    } else {
        Vec::new()
    };
    rtc::rtp::Header {
        version: header.version,
        padding: header.padding,
        extension: header.extension,
        marker: header.marker,
        payload_type: header.payload_type,
        sequence_number: header.sequence_number,
        timestamp: header.timestamp,
        ssrc: header.ssrc,
        csrc: header.csrc.clone(),
        extension_profile: header.extension_profile,
        extensions,
        extensions_padding: 0,
        padding_size: header.padding_size,
    }
}

pub(crate) fn codec_type_to_rtc(kind: RtpCodecType) -> RtpCodecKind {
    match kind {
        RtpCodecType::Audio => RtpCodecKind::Audio,
        RtpCodecType::Video => RtpCodecKind::Video,
        RtpCodecType::Unknown => RtpCodecKind::Unspecified,
    }
}

pub(crate) fn codec_type_from_rtc(kind: RtpCodecKind) -> RtpCodecType {
    match kind {
        RtpCodecKind::Audio => RtpCodecType::Audio,
        RtpCodecKind::Video => RtpCodecType::Video,
        _ => RtpCodecType::Unknown,
    }
}

pub(crate) fn codec_capability_from_rtc(codec: &RTCRtpCodec) -> RtpCodecCapability {
    RtpCodecCapability {
        mime_type: codec.mime_type.clone(),
        clock_rate: codec.clock_rate,
        channels: codec.channels,
        sdp_fmtp_line: codec.sdp_fmtp_line.clone(),
        rtcp_feedback: codec
            .rtcp_feedback
            .iter()
            .map(|fb| RtcpFeedback {
                r#type: fb.typ.clone(),
                parameter: fb.parameter.clone(),
            })
            .collect(),
    }
}

pub(crate) fn codec_capability_to_rtc(codec: &RtpCodecCapability) -> RTCRtpCodec {
    RTCRtpCodec {
        mime_type: codec.mime_type.clone(),
        clock_rate: codec.clock_rate,
        channels: codec.channels,
        sdp_fmtp_line: codec.sdp_fmtp_line.clone(),
        rtcp_feedback: codec
            .rtcp_feedback
            .iter()
            .map(|fb| RTCPFeedback {
                typ: fb.r#type.clone(),
                parameter: fb.parameter.clone(),
            })
            .collect(),
    }
}

pub(crate) fn codec_parameters_from_rtc(codec: &RTCRtpCodecParameters) -> RtpCodecParameters {
    RtpCodecParameters {
        rtp_codec_capability: codec_capability_from_rtc(&codec.rtp_codec),
        payload_type: codec.payload_type,
    }
}

pub(crate) fn codec_parameters_to_rtc(codec: &RtpCodecParameters) -> RTCRtpCodecParameters {
    RTCRtpCodecParameters {
        rtp_codec: codec_capability_to_rtc(&codec.rtp_codec_capability),
        payload_type: codec.payload_type,
    }
}

pub(crate) fn header_extension_from_rtc(
    extension: &RTCRtpHeaderExtensionParameters,
) -> RtpHeaderExtensionParameter {
    RtpHeaderExtensionParameter {
        uri: extension.uri.clone(),
        id: extension.id as i32,
    }
}

pub(crate) fn rtp_parameters_from_rtc(parameters: &RTCRtpParameters) -> RtpParameters {
    RtpParameters {
        header_extensions: parameters
            .header_extensions
            .iter()
            .map(header_extension_from_rtc)
            .collect(),
        codecs: parameters
            .codecs
            .iter()
            .map(codec_parameters_from_rtc)
            .collect(),
    }
}

pub(crate) fn stream_info_from_rtc(info: &rtc::interceptor::StreamInfo) -> StreamInfo {
    StreamInfo {
        ssrc: info.ssrc,
        ssrc_retransmission: info.ssrc_rtx.unwrap_or(0),
        ssrc_forward_error_correction: info.ssrc_fec.unwrap_or(0),
        payload_type: info.payload_type,
        payload_type_retransmission: info.payload_type_rtx.unwrap_or(0),
        payload_type_forward_error_correction: info.payload_type_fec.unwrap_or(0),
        rtp_header_extensions: info
            .rtp_header_extensions
            .iter()
            .map(|e| RtpHeaderExtension {
                uri: e.uri.clone(),
                id: e.id as i32,
            })
            .collect(),
        mime_type: info.mime_type.clone(),
        clock_rate: info.clock_rate,
        channels: info.channels,
        sdp_fmtp_line: info.sdp_fmtp_line.clone(),
        rtcp_feedback: info
            .rtcp_feedback
            .iter()
            .map(|fb| RtcpFeedback {
                r#type: fb.typ.clone(),
                parameter: fb.parameter.clone(),
            })
            .collect(),
    }
}

pub(crate) fn send_parameters_from_rtc(parameters: &RTCRtpSendParameters) -> RtpSendParameters {
    RtpSendParameters {
        rtp_parameters: rtp_parameters_from_rtc(&parameters.rtp_parameters),
        encodings: parameters
            .encodings
            .iter()
            .map(|encoding| {
                let coding = &encoding.rtp_coding_parameters;
                RtpEncodingParameters {
                    rtp_coding_parameters: RtpCodingParameters {
                        rid: coding.rid.clone(),
                        ssrc: coding.ssrc.unwrap_or(0),
                        payload_type: 0,
                        rtx: RtpRtxParameters {
                            ssrc: coding.rtx.as_ref().map_or(0, |rtx| rtx.ssrc),
                        },
                        fec: RtpFecParameters {
                            ssrc: coding.fec.as_ref().map_or(0, |fec| fec.ssrc),
                        },
                    },
                }
            })
            .collect(),
    }
}
