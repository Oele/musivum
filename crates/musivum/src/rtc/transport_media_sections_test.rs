// Ported from LiveKit, pkg/rtc/transport_media_sections_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The client is a connection of musivum's layer on a
// node of its own; `webrtc.NewTrackLocalStaticRTP` is `StaticTrack` here, a local track that
// binds to the first codec of its MIME type it is offered, as pion's does. `t.Cleanup` is the
// harness's `Drop`. The waits of the harness are on the runtime.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use parking_lot::Mutex;

use super::transport_test::{codec, new_node, register_codecs, transport_params};
use super::*;
use crate::rtc::transport::FakeHandler;
use crate::testutils::eventually_async;
use crate::webrtc::peerconnection::register_codec;
use crate::webrtc::{
    MIME_TYPE_AV1, MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_RTX, MIME_TYPE_VP8, MIME_TYPE_VP9,
    RtpCodecCapability, TrackLocalContext,
};

#[derive(Default)]
struct HarnessState {
    answer: Option<SessionDescription>,
    answers: i32,
    requirements: i32,
    requested_audios: u32,
    requested_videos: u32,
    pending_audios: u32,
    pending_videos: u32,
    offer_id: u32,
}

struct MediaSectionsHarness {
    server: Arc<PcTransport>,
    #[allow(dead_code)]
    handler: Arc<FakeHandler>,
    client: PeerConnection,

    mu: Arc<Mutex<HarnessState>>,
}

fn new_media_sections_harness() -> MediaSectionsHarness {
    let codecs = vec![codec(MIME_TYPE_OPUS), codec(MIME_TYPE_VP8)];
    let client_me = register_codecs(&codecs, RtcpFeedbackConfig::default());
    new_media_sections_harness_with_client(codecs, client_me)
}

fn new_media_sections_harness_with_client(
    codecs: Vec<livekit::Codec>,
    client_me: MediaEngine,
) -> MediaSectionsHarness {
    let handler = Arc::new(FakeHandler::default());
    let mu = Arc::new(Mutex::new(HarnessState::default()));

    let mut params = transport_params(&handler);
    params.enabled_publish_codecs = codecs.clone();
    params.enabled_subscribe_codecs = codecs;
    params.is_send_side = true;
    let server = PcTransport::new(params).unwrap();

    let client = PeerConnection::new(
        &new_node(),
        Configuration {
            media_engine: client_me,
            ..Default::default()
        },
    )
    .unwrap();

    handler.on_answer_calls({
        let mu = mu.clone();
        move |sd, _, _| {
            let mut h = mu.lock();
            h.answer = Some(sd);
            h.answers += 1;
            Ok(())
        }
    });
    handler.on_unmatched_media_calls({
        let mu = mu.clone();
        move |num_audios, num_videos| {
            let mut h = mu.lock();
            h.requirements += 1;
            h.requested_audios += num_audios;
            h.requested_videos += num_videos;
            h.pending_audios += num_audios;
            h.pending_videos += num_videos;
            Ok(())
        }
    });

    MediaSectionsHarness {
        server,
        handler,
        client,
        mu,
    }
}

// `t.Cleanup` of the original
impl Drop for MediaSectionsHarness {
    fn drop(&mut self) {
        self.server.close();
        let _ = self.client.close();
    }
}

impl MediaSectionsHarness {
    async fn negotiate(&self, options: Option<OfferOptions>) {
        let (answers_before, offer_id) = {
            let mut h = self.mu.lock();
            for _ in 0..h.pending_audios {
                self.client
                    .add_transceiver_from_kind(
                        RtpCodecType::Audio,
                        Some(RtpTransceiverInit {
                            direction: RtpTransceiverDirection::Recvonly,
                        }),
                    )
                    .unwrap();
            }
            for _ in 0..h.pending_videos {
                self.client
                    .add_transceiver_from_kind(
                        RtpCodecType::Video,
                        Some(RtpTransceiverInit {
                            direction: RtpTransceiverDirection::Recvonly,
                        }),
                    )
                    .unwrap();
            }
            (h.pending_audios, h.pending_videos) = (0, 0);
            let answers_before = h.answers;
            h.offer_id += 1;
            (answers_before, h.offer_id)
        };

        let offer = self.client.create_offer(options).unwrap();
        self.client.set_local_description(offer).unwrap();
        self.server
            .handle_remote_description(self.client.local_description().unwrap(), offer_id)
            .unwrap();

        // "server did not produce answer"
        eventually_async(
            || self.mu.lock().answers > answers_before,
            StdDuration::from_secs(5),
            StdDuration::from_millis(10),
        )
        .await;

        let answer = self.mu.lock().answer.clone().unwrap();
        self.client.set_remote_description(answer).unwrap();
    }

    fn add_video_track(&self, id: &str) -> (Arc<RtpSender>, Arc<RtpTransceiver>) {
        let track = StaticTrack::new(
            RtpCodecCapability {
                mime_type: MIME_TYPE_VP8.to_string(),
                clock_rate: 90000,
                ..Default::default()
            },
            id,
            id,
        );

        self.server
            .add_track(
                track,
                types::AddTrackParams::default(),
                &[codec(MIME_TYPE_VP8)],
                &RtcpFeedbackConfig::default(),
            )
            .unwrap()
    }

    fn requested(&self) -> (i32, u32, u32) {
        let h = self.mu.lock();
        (h.requirements, h.requested_audios, h.requested_videos)
    }

    fn client_video_transceivers(&self) -> usize {
        self.client
            .get_transceivers()
            .iter()
            .filter(|tr| tr.kind() == RtpCodecType::Video)
            .count()
    }
}

// `webrtc.NewTrackLocalStaticRTP(capability, id, streamID)`: binds to the first codec it is
// offered whose MIME type is its own.
struct StaticTrack {
    capability: RtpCodecCapability,
    id: String,
    stream_id: String,
}

impl StaticTrack {
    fn new(capability: RtpCodecCapability, id: &str, stream_id: &str) -> Arc<StaticTrack> {
        Arc::new(StaticTrack {
            capability,
            id: id.to_string(),
            stream_id: stream_id.to_string(),
        })
    }
}

impl webrtc::TrackLocal for StaticTrack {
    fn bind(&self, t: Arc<dyn TrackLocalContext>) -> Result<RtpCodecParameters, webrtc::Error> {
        t.codec_parameters()
            .into_iter()
            .find(|c| {
                c.rtp_codec_capability
                    .mime_type
                    .eq_ignore_ascii_case(&self.capability.mime_type)
            })
            .ok_or(webrtc::Error::UnsupportedCodec)
    }

    fn unbind(&self, _t: Arc<dyn TrackLocalContext>) -> Result<(), webrtc::Error> {
        Ok(())
    }

    fn id(&self) -> String {
        self.id.clone()
    }

    fn rid(&self) -> String {
        String::new()
    }

    fn stream_id(&self) -> String {
        self.stream_id.clone()
    }

    fn kind(&self) -> RtpCodecType {
        RtpCodecType::Video
    }
}

fn sendonly() -> Option<RtpTransceiverInit> {
    Some(RtpTransceiverInit {
        direction: RtpTransceiverDirection::Sendonly,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_media_sections_requirement_not_repeated_for_unmatchable_transceiver() {
    let h = new_media_sections_harness();
    h.client
        .add_transceiver_from_kind(RtpCodecType::Audio, sendonly())
        .unwrap();
    h.negotiate(None).await;
    let (sender, transceiver) = h.add_video_track("v1");
    assert!(transceiver.mid().is_empty());
    h.server.remove_track(&sender).unwrap();

    const ROUNDS: usize = 5;
    for _ in 0..ROUNDS {
        h.negotiate(None).await;
    }
    h.negotiate(None).await;

    let (_, _, requested_videos) = h.requested();
    assert!(
        requested_videos <= 1,
        "unmatchable transceiver re-requested on every answer: {requested_videos} video sections requested over {ROUNDS} rounds"
    );
    assert!(h.client_video_transceivers() <= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_media_sections_requirement_after_transceiver_reuse() {
    let h = new_media_sections_harness();

    h.client
        .add_transceiver_from_kind(RtpCodecType::Audio, sendonly())
        .unwrap();
    h.negotiate(None).await;

    let (sender, released) = h.add_video_track("v1");
    h.server.remove_track(&sender).unwrap();
    h.negotiate(None).await;
    h.negotiate(None).await;
    assert!(released.mid().is_empty());

    let (_, transceiver) = h.add_video_track("v2");
    assert!(
        Arc::ptr_eq(&released, &transceiver),
        "released transceiver should be reused"
    );
    assert!(transceiver.mid().is_empty());
    h.negotiate(None).await;
    h.server.negotiate(true);
    h.negotiate(None).await;
    h.negotiate(None).await;

    assert!(
        !transceiver.mid().is_empty(),
        "subscribed track never got a media section"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_media_sections_requirement_reissued_when_lost() {
    let h = new_media_sections_harness();

    h.client
        .add_transceiver_from_kind(RtpCodecType::Audio, sendonly())
        .unwrap();
    h.negotiate(None).await;

    let (_, transceiver) = h.add_video_track("v1");
    h.negotiate(None).await;
    h.mu.lock().pending_videos = 0;

    h.negotiate(Some(OfferOptions { ice_restart: true })).await;
    h.negotiate(None).await;
    h.negotiate(None).await;

    assert!(
        !transceiver.mid().is_empty(),
        "lost requirement was never re-issued"
    );
}

// firefoxMediaEngine registers the payload types Firefox offers.
fn firefox_media_engine() -> MediaEngine {
    let mut me = MediaEngine::default();
    register_codec(
        &mut me,
        &RtpCodecParameters {
            rtp_codec_capability: RtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_string(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "maxplaybackrate=48000;stereo=1;useinbandfec=1".to_string(),
                ..Default::default()
            },
            payload_type: 109,
        },
        RtpCodecType::Audio,
    )
    .unwrap();
    let mut video = |pt: u8, mime_type: &str, fmtp: &str| {
        register_codec(
            &mut me,
            &RtpCodecParameters {
                rtp_codec_capability: RtpCodecCapability {
                    mime_type: mime_type.to_string(),
                    clock_rate: 90000,
                    sdp_fmtp_line: fmtp.to_string(),
                    ..Default::default()
                },
                payload_type: pt,
            },
            RtpCodecType::Video,
        )
        .unwrap();
    };
    video(120, MIME_TYPE_VP8, "max-fs=12288;max-fr=60");
    video(124, MIME_TYPE_RTX, "apt=120");
    video(121, MIME_TYPE_VP9, "max-fs=12288;max-fr=60");
    video(125, MIME_TYPE_RTX, "apt=121");
    video(
        126,
        MIME_TYPE_H264,
        "profile-level-id=42e01f;level-asymmetry-allowed=1;packetization-mode=1",
    );
    video(127, MIME_TYPE_RTX, "apt=126");
    video(
        97,
        MIME_TYPE_H264,
        "profile-level-id=42e01f;level-asymmetry-allowed=1",
    );
    video(98, MIME_TYPE_RTX, "apt=97");
    video(99, MIME_TYPE_AV1, "profile=0;level-idx=9;tier=0");
    video(100, MIME_TYPE_RTX, "apt=99");
    me
}

// videoRtpmaps returns payload type -> "codec/clockrate" of the first video section.
fn video_rtpmaps(sdp_text: &str) -> HashMap<u8, String> {
    let sd = SessionDescription {
        r#type: SdpType::Offer,
        sdp: sdp_text.to_string(),
    };
    let parsed = sd.unmarshal().unwrap();
    let mut rtpmaps = HashMap::new();
    if let Some(md) = parsed
        .media_descriptions
        .iter()
        .find(|md| md.media_name.media == "video")
    {
        for attr in &md.attributes {
            if attr.key != "rtpmap" {
                continue;
            }
            // `fmt.Sscanf(attr.Value, "%d %s", &pt, &codec)`
            let value = attr.value.as_deref().unwrap_or("");
            let mut fields = value.split_whitespace();
            let pt: u8 = fields.next().unwrap().parse().unwrap();
            let codec = fields.next().unwrap().to_string();
            rtpmaps.insert(pt, codec);
        }
    }
    rtpmaps
}

// Single peer connection mode: the answer must echo the offered video payload
// types. Firefox receives on the payload types it offered, so a server-chosen
// payload type is never decoded (livekit/livekit#5030).
#[tokio::test(flavor = "multi_thread")]
async fn test_media_sections_answer_echoes_offered_video_payload_types() {
    let codecs = vec![
        codec(MIME_TYPE_OPUS),
        codec(MIME_TYPE_VP8),
        codec(MIME_TYPE_VP9),
        codec(MIME_TYPE_H264),
    ];
    let h = new_media_sections_harness_with_client(codecs, firefox_media_engine());

    h.client.create_data_channel("d", None).unwrap();
    h.client
        .add_transceiver_from_kind(RtpCodecType::Audio, sendonly())
        .unwrap();
    h.negotiate(None).await;

    h.add_video_track("v1");
    // an unrelated offer/answer round while the video sender has no media section
    h.negotiate(None).await;
    // "media section for video was never requested"
    eventually_async(
        || h.mu.lock().pending_videos > 0,
        StdDuration::from_secs(5),
        StdDuration::from_millis(10),
    )
    .await;
    h.negotiate(None).await;

    let offered = video_rtpmaps(&h.client.local_description().unwrap().sdp);
    assert!(!offered.is_empty());
    let answered = video_rtpmaps(&h.mu.lock().answer.as_ref().unwrap().sdp);
    assert!(!answered.is_empty());
    for (pt, codec) in &answered {
        assert_eq!(
            Some(codec),
            offered.get(pt),
            "answer payload type {pt} ({codec}) is not what the client offered"
        );
    }
}
