// Ported from LiveKit, pkg/rtc/config.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, against musivum's connection layer
// (`crate::webrtc::peerconnection`) where the original is written against pion.
// - `NewWebRTCConfig` is `WebRtcConfig::new`. It returns the error of `rtcconfig`, the only
//   one there is to return (`rtc::Error` cannot hold it: that one is `Clone` and `Eq`).
// - The embedded `rtcconfig.WebRTCConfig` is the field `web_rtc_config`. Its `SettingEngine`
//   is data here (`rtcconfig::SettingEngineConfig`), which has no field for the two settings
//   this file makes on pion's engine. `SetAnsweringDTLSRole` is the field
//   `answering_dtls_role` of `WebRtcConfig`: whoever makes the layer's engine for a
//   connection (`web_rtc_config.setting_engine.build()`) puts it into that engine's field
//   of the same name. `DisableActiveTCP` has no counterpart: the layer has no ICE over TCP
//   (a comment stands at its place).
// - `SetBufferFactory` keeps the factory and sets no `SettingEngine.BufferFactory`: in the
//   layer the received packets of a connection come out at its `PacketSink`
//   (`sfu::interceptor::BufferFactorySink`, which is made from the factory when the
//   connection is). The factory, a pointer that is nil until it is set, is an `Option`.
// - `UpdateSubscriberConfig` takes the congestion control configuration by reference.
// - The sizes of `ReceiverConfig`, `int`s that count packets, are `usize`.
// - A feedback of the tables is written with `rtcp_feedback(type, parameter)`, where the
//   original has a struct literal.
// - The tests at the end are new: the original has none.

use std::sync::Arc;

use crate::config;
use crate::mediatransportutil::rtcconfig;
use crate::sfu;
use crate::sfu::buffer;
use crate::sfu::rtpextension::abscapturetime as act;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::webrtc;
use crate::webrtc::peerconnection::DtlsRole;
use crate::webrtc::sdp;

const FRAME_MARKING_URI: &str = "urn:ietf:params:rtp-hdrext:framemarking";
const REPAIRED_RTP_STREAM_ID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id";

#[derive(Clone)]
pub struct WebRtcConfig {
    pub web_rtc_config: rtcconfig::WebRtcConfig,

    pub buffer_factory: Option<Arc<buffer::Factory>>,
    pub receiver: ReceiverConfig,
    pub publisher: DirectionConfig,
    pub subscriber: DirectionConfig,

    flex_fec: config::FlexFecConfig,

    /// `SettingEngine.SetAnsweringDTLSRole` of the original, which `new` sets to the
    /// server's role for a node that is ICE lite; `Unspecified` where it is not set. It
    /// goes into the engine a connection is made from: see the header.
    pub answering_dtls_role: DtlsRole,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReceiverConfig {
    pub packet_buffer_size_video: usize,
    pub packet_buffer_size_audio: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtpHeaderExtensionConfig {
    pub audio: Vec<String>,
    pub video: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RtcpFeedbackConfig {
    pub audio: Vec<webrtc::RtcpFeedback>,
    pub video: Vec<webrtc::RtcpFeedback>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DirectionConfig {
    pub rtp_header_extension: RtpHeaderExtensionConfig,
    pub rtcp_feedback: RtcpFeedbackConfig,
    pub flex_fec: FlexFecDirectionConfig,
    pub rtx_budget: sfu::RtxBudgetConfig,
}

// FlexFECDirectionConfig enables flexfec-03 for a transport direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlexFecDirectionConfig {
    pub enabled: bool,
}

impl WebRtcConfig {
    pub fn new(conf: &config::Config) -> Result<WebRtcConfig, rtcconfig::Error> {
        let rtc_conf = &conf.rtc;
        let flex_fec = rtc_conf.flex_fec;

        let web_rtc_config = rtcconfig::new_webrtc_config(&rtc_conf.rtc_config, conf.development)?;

        // we don't want to use active TCP on a server, clients should be dialing
        // (`webRTCConfig.SettingEngine.DisableActiveTCP(true)`: the layer has no ICE over TCP)

        let mut answering_dtls_role = DtlsRole::Unspecified;
        if rtc_conf.rtc_config.use_ice_lite {
            answering_dtls_role = DtlsRole::Server;
        }

        // the original changes its copy of the configuration
        let mut packet_buffer_size = rtc_conf.packet_buffer_size;
        let mut packet_buffer_size_video = rtc_conf.packet_buffer_size_video;
        let mut packet_buffer_size_audio = rtc_conf.packet_buffer_size_audio;
        if packet_buffer_size == 0 {
            packet_buffer_size = 500;
        }
        if packet_buffer_size_video == 0 {
            packet_buffer_size_video = packet_buffer_size;
        }
        if packet_buffer_size_audio == 0 {
            packet_buffer_size_audio = packet_buffer_size;
        }

        let mut subscriber_config = get_subscriber_config(
            rtc_conf.congestion_control.use_send_side_bwe_interceptor
                || rtc_conf.congestion_control.use_send_side_bwe,
        );
        subscriber_config.rtx_budget = rtc_conf.rtx_budget;

        Ok(WebRtcConfig {
            web_rtc_config,
            buffer_factory: None,
            receiver: ReceiverConfig {
                packet_buffer_size_video: packet_buffer_size_video as usize,
                packet_buffer_size_audio: packet_buffer_size_audio as usize,
            },
            publisher: get_publisher_config(false, flex_fec),
            subscriber: subscriber_config,
            flex_fec,
            answering_dtls_role,
        })
    }

    pub fn update_publisher_config(&mut self, consolidated: bool) {
        self.publisher = get_publisher_config(consolidated, self.flex_fec);
    }

    pub fn update_subscriber_config(&mut self, cc_conf: &config::CongestionControlConfig) {
        let rtx_budget = self.subscriber.rtx_budget;
        self.subscriber = get_subscriber_config(
            cc_conf.use_send_side_bwe_interceptor || cc_conf.use_send_side_bwe,
        );
        self.subscriber.rtx_budget = rtx_budget;
    }

    pub fn set_buffer_factory(&mut self, factory: Arc<buffer::Factory>) {
        self.buffer_factory = Some(factory);
        // `c.SettingEngine.BufferFactory = factory.GetOrNew`: see the header
    }
}

// `webrtc.RTCPFeedback{Type: typ, Parameter: parameter}`
fn rtcp_feedback(typ: &str, parameter: &str) -> webrtc::RtcpFeedback {
    webrtc::RtcpFeedback {
        r#type: typ.to_string(),
        parameter: parameter.to_string(),
    }
}

fn get_publisher_config(consolidated: bool, flex_fec: config::FlexFecConfig) -> DirectionConfig {
    let publisher_flex_fec = FlexFecDirectionConfig {
        enabled: flex_fec.upstream_enabled,
    };
    if consolidated {
        return DirectionConfig {
            flex_fec: publisher_flex_fec,
            rtp_header_extension: RtpHeaderExtensionConfig {
                audio: [
                    sdp::SDES_MID_URI,
                    sdp::SDES_RTP_STREAM_ID_URI,
                    sdp::AUDIO_LEVEL_URI,
                    act::ABS_CAPTURE_TIME_URI,
                ]
                .map(String::from)
                .to_vec(),
                video: [
                    sdp::SDES_MID_URI,
                    sdp::SDES_RTP_STREAM_ID_URI,
                    sdp::TRANSPORT_CC_URI,
                    sdp::ABS_SEND_TIME_URI,
                    FRAME_MARKING_URI,
                    dd::EXTENSION_URI,
                    REPAIRED_RTP_STREAM_ID_URI,
                    act::ABS_CAPTURE_TIME_URI,
                ]
                .map(String::from)
                .to_vec(),
            },
            rtcp_feedback: RtcpFeedbackConfig {
                audio: vec![rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, "")],
                video: vec![
                    rtcp_feedback(webrtc::TYPE_RTCP_FB_TRANSPORT_CC, ""),
                    rtcp_feedback(webrtc::TYPE_RTCP_FB_GOOG_REMB, ""),
                    rtcp_feedback(webrtc::TYPE_RTCP_FB_CCM, "fir"),
                    rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, ""),
                    rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, "pli"),
                ],
            },
            ..Default::default()
        };
    }

    DirectionConfig {
        flex_fec: publisher_flex_fec,
        rtp_header_extension: RtpHeaderExtensionConfig {
            audio: [
                sdp::SDES_MID_URI,
                sdp::SDES_RTP_STREAM_ID_URI,
                sdp::AUDIO_LEVEL_URI,
                act::ABS_CAPTURE_TIME_URI,
            ]
            .map(String::from)
            .to_vec(),
            video: [
                sdp::SDES_MID_URI,
                sdp::SDES_RTP_STREAM_ID_URI,
                sdp::TRANSPORT_CC_URI,
                FRAME_MARKING_URI,
                dd::EXTENSION_URI,
                REPAIRED_RTP_STREAM_ID_URI,
                act::ABS_CAPTURE_TIME_URI,
            ]
            .map(String::from)
            .to_vec(),
        },
        rtcp_feedback: RtcpFeedbackConfig {
            audio: vec![rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, "")],
            video: vec![
                rtcp_feedback(webrtc::TYPE_RTCP_FB_TRANSPORT_CC, ""),
                rtcp_feedback(webrtc::TYPE_RTCP_FB_CCM, "fir"),
                rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, ""),
                rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, "pli"),
            ],
        },
        ..Default::default()
    }
}

fn get_subscriber_config(enable_twcc: bool) -> DirectionConfig {
    let mut subscriber_config = DirectionConfig {
        rtp_header_extension: RtpHeaderExtensionConfig {
            video: [dd::EXTENSION_URI, act::ABS_CAPTURE_TIME_URI]
                .map(String::from)
                .to_vec(),
            audio: [act::ABS_CAPTURE_TIME_URI].map(String::from).to_vec(),
        },
        rtcp_feedback: RtcpFeedbackConfig {
            audio: vec![
                // always enable NACK for audio but disable it later for red enabled transceiver. https://github.com/pion/webrtc/pull/2972
                rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, ""),
            ],
            video: vec![
                rtcp_feedback(webrtc::TYPE_RTCP_FB_CCM, "fir"),
                rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, ""),
                rtcp_feedback(webrtc::TYPE_RTCP_FB_NACK, "pli"),
            ],
        },
        ..Default::default()
    };
    if enable_twcc {
        subscriber_config
            .rtp_header_extension
            .video
            .push(sdp::TRANSPORT_CC_URI.to_string());
        subscriber_config
            .rtcp_feedback
            .video
            .push(rtcp_feedback(webrtc::TYPE_RTCP_FB_TRANSPORT_CC, ""));
    } else {
        subscriber_config
            .rtp_header_extension
            .video
            .push(sdp::ABS_SEND_TIME_URI.to_string());
        subscriber_config
            .rtcp_feedback
            .video
            .push(rtcp_feedback(webrtc::TYPE_RTCP_FB_GOOG_REMB, ""));
    }

    subscriber_config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sfu::buffer::FactoryOfBufferFactory;

    const MID: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";
    const RID: &str = "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id";
    const REPAIRED_RID: &str = "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id";
    const AUDIO_LEVEL: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
    const ABS_CAPTURE_TIME: &str = "http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time";
    const ABS_SEND_TIME: &str = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time";
    const TRANSPORT_CC: &str =
        "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";
    const FRAME_MARKING: &str = "urn:ietf:params:rtp-hdrext:framemarking";
    const DEPENDENCY_DESCRIPTOR: &str =
        "https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension";

    // As the Go test that printed the expectations wrote a feedback: "type/parameter".
    fn feedback(fbs: &[webrtc::RtcpFeedback]) -> Vec<String> {
        fbs.iter()
            .map(|fb| format!("{}/{}", fb.r#type, fb.parameter))
            .collect()
    }

    #[track_caller]
    fn assert_direction(
        d: &DirectionConfig,
        ext_audio: &[&str],
        ext_video: &[&str],
        fb_audio: &[&str],
        fb_video: &[&str],
    ) {
        assert_eq!(ext_audio, d.rtp_header_extension.audio);
        assert_eq!(ext_video, d.rtp_header_extension.video);
        assert_eq!(fb_audio, feedback(&d.rtcp_feedback.audio));
        assert_eq!(fb_video, feedback(&d.rtcp_feedback.video));
    }

    // As upstream's tests make their configuration (`newParticipantForTestWithOpts`).
    fn new_config() -> config::Config {
        let mut conf = config::Config::new("", true).unwrap();
        conf.rtc.rtc_config.tcp_port = 0;
        conf
    }

    const PUBLISHER_EXT_AUDIO: [&str; 4] = [MID, RID, AUDIO_LEVEL, ABS_CAPTURE_TIME];
    const PUBLISHER_EXT_VIDEO: [&str; 7] = [
        MID,
        RID,
        TRANSPORT_CC,
        FRAME_MARKING,
        DEPENDENCY_DESCRIPTOR,
        REPAIRED_RID,
        ABS_CAPTURE_TIME,
    ];
    const PUBLISHER_FB_VIDEO: [&str; 4] = ["transport-cc/", "ccm/fir", "nack/", "nack/pli"];
    const SUBSCRIBER_EXT_VIDEO_REMB: [&str; 3] =
        [DEPENDENCY_DESCRIPTOR, ABS_CAPTURE_TIME, ABS_SEND_TIME];
    const SUBSCRIBER_FB_VIDEO_REMB: [&str; 4] = ["ccm/fir", "nack/", "nack/pli", "goog-remb/"];
    const SUBSCRIBER_EXT_VIDEO_TWCC: [&str; 3] =
        [DEPENDENCY_DESCRIPTOR, ABS_CAPTURE_TIME, TRANSPORT_CC];
    const SUBSCRIBER_FB_VIDEO_TWCC: [&str; 4] = ["ccm/fir", "nack/", "nack/pli", "transport-cc/"];

    // New for musivum: the original has no test of config.go. The header extensions and the
    // RTCP feedback of both directions, for the default configuration and for the send-side
    // estimator switched on, are what the original gives: a Go test laid over `pkg/rtc` at
    // v1.13.11 (`go test -overlay`) printed them from `NewWebRTCConfig`, and what
    // `UpdatePublisherConfig`, `UpdateSubscriberConfig` and the packet buffer sizes give
    // with them.
    #[test]
    fn the_tables_of_both_directions_are_those_of_the_original() {
        // the default configuration: the receiver-side estimator (REMB)
        let conf = new_config();
        let mut c = WebRtcConfig::new(&conf).unwrap();
        assert_eq!(
            ReceiverConfig {
                packet_buffer_size_video: 500,
                packet_buffer_size_audio: 200,
            },
            c.receiver
        );
        assert_direction(
            &c.publisher,
            &PUBLISHER_EXT_AUDIO,
            &PUBLISHER_EXT_VIDEO,
            &["nack/"],
            &PUBLISHER_FB_VIDEO,
        );
        assert!(!c.publisher.flex_fec.enabled);
        assert_eq!(sfu::RtxBudgetConfig::default(), c.publisher.rtx_budget);
        assert_direction(
            &c.subscriber,
            &[ABS_CAPTURE_TIME],
            &SUBSCRIBER_EXT_VIDEO_REMB,
            &["nack/"],
            &SUBSCRIBER_FB_VIDEO_REMB,
        );
        assert!(!c.subscriber.flex_fec.enabled);
        assert_eq!(sfu::DEFAULT_RTX_BUDGET_CONFIG, c.subscriber.rtx_budget);
        assert_eq!(4_000, c.subscriber.rtx_budget.audio.burst_bytes);
        assert_eq!(64_000, c.subscriber.rtx_budget.video.burst_bytes);

        // a publisher that also receives (one peer connection)
        c.update_publisher_config(true);
        assert_direction(
            &c.publisher,
            &PUBLISHER_EXT_AUDIO,
            &[
                MID,
                RID,
                TRANSPORT_CC,
                ABS_SEND_TIME,
                FRAME_MARKING,
                DEPENDENCY_DESCRIPTOR,
                REPAIRED_RID,
                ABS_CAPTURE_TIME,
            ],
            &["nack/"],
            &[
                "transport-cc/",
                "goog-remb/",
                "ccm/fir",
                "nack/",
                "nack/pli",
            ],
        );
        c.update_publisher_config(false);
        assert_direction(
            &c.publisher,
            &PUBLISHER_EXT_AUDIO,
            &PUBLISHER_EXT_VIDEO,
            &["nack/"],
            &PUBLISHER_FB_VIDEO,
        );

        // the subscriber's side follows the estimator and keeps its budget
        c.update_subscriber_config(&config::CongestionControlConfig {
            use_send_side_bwe: true,
            ..Default::default()
        });
        assert_direction(
            &c.subscriber,
            &[ABS_CAPTURE_TIME],
            &SUBSCRIBER_EXT_VIDEO_TWCC,
            &["nack/"],
            &SUBSCRIBER_FB_VIDEO_TWCC,
        );
        assert_eq!(sfu::DEFAULT_RTX_BUDGET_CONFIG, c.subscriber.rtx_budget);

        // the send-side estimator switched on, by either switch
        for interceptor in [false, true] {
            let mut conf = new_config();
            if interceptor {
                conf.rtc.congestion_control.use_send_side_bwe_interceptor = true;
            } else {
                conf.rtc.congestion_control.use_send_side_bwe = true;
            }
            let mut c = WebRtcConfig::new(&conf).unwrap();
            assert_direction(
                &c.publisher,
                &PUBLISHER_EXT_AUDIO,
                &PUBLISHER_EXT_VIDEO,
                &["nack/"],
                &PUBLISHER_FB_VIDEO,
            );
            assert_direction(
                &c.subscriber,
                &[ABS_CAPTURE_TIME],
                &SUBSCRIBER_EXT_VIDEO_TWCC,
                &["nack/"],
                &SUBSCRIBER_FB_VIDEO_TWCC,
            );
            assert_eq!(sfu::DEFAULT_RTX_BUDGET_CONFIG, c.subscriber.rtx_budget);

            c.update_subscriber_config(&config::CongestionControlConfig::default());
            assert_direction(
                &c.subscriber,
                &[ABS_CAPTURE_TIME],
                &SUBSCRIBER_EXT_VIDEO_REMB,
                &["nack/"],
                &SUBSCRIBER_FB_VIDEO_REMB,
            );
        }

        // FlexFEC is the publisher's only, and stays when its tables are changed
        let mut conf = new_config();
        conf.rtc.flex_fec.upstream_enabled = true;
        let mut c = WebRtcConfig::new(&conf).unwrap();
        assert!(c.publisher.flex_fec.enabled);
        assert!(!c.subscriber.flex_fec.enabled);
        c.update_publisher_config(true);
        assert!(c.publisher.flex_fec.enabled);
        c.update_publisher_config(false);
        assert!(c.publisher.flex_fec.enabled);

        // the sizes of the packet buffers
        let mut conf = new_config();
        conf.rtc.packet_buffer_size = 0;
        conf.rtc.packet_buffer_size_video = 0;
        conf.rtc.packet_buffer_size_audio = 0;
        let c = WebRtcConfig::new(&conf).unwrap();
        assert_eq!(500, c.receiver.packet_buffer_size_video);
        assert_eq!(500, c.receiver.packet_buffer_size_audio);
        conf.rtc.packet_buffer_size = 300;
        conf.rtc.packet_buffer_size_audio = 100;
        let c = WebRtcConfig::new(&conf).unwrap();
        assert_eq!(300, c.receiver.packet_buffer_size_video);
        assert_eq!(100, c.receiver.packet_buffer_size_audio);
    }

    // New for musivum: what this file sets on pion's engine upstream, and the factory.
    #[test]
    fn the_settings_of_the_engine_and_the_buffer_factory_are_kept() {
        let mut conf = new_config();
        let mut c = WebRtcConfig::new(&conf).unwrap();
        assert_eq!(DtlsRole::Unspecified, c.answering_dtls_role);
        assert!(!c.web_rtc_config.setting_engine.lite);
        assert!(c.buffer_factory.is_none());

        let factory = FactoryOfBufferFactory::new(500, 200).create_buffer_factory();
        c.set_buffer_factory(factory.clone());
        // a participant's configuration is a copy of the node's
        let copy = c.clone();
        assert!(Arc::ptr_eq(&factory, copy.buffer_factory.as_ref().unwrap()));

        conf.rtc.rtc_config.use_ice_lite = true;
        let c = WebRtcConfig::new(&conf).unwrap();
        assert_eq!(DtlsRole::Server, c.answering_dtls_role);
        assert!(c.web_rtc_config.setting_engine.lite);
        let mut engine = c.web_rtc_config.setting_engine.build();
        engine.answering_dtls_role = c.answering_dtls_role;
        assert!(engine.lite);
    }
}
