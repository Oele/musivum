// Ported from LiveKit, pkg/rtc/dynacast/dynacastmanager_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks of the one test. A test keeps
// its listener as an `Arc` and hands the manager a `Weak` of it, as a manager does not keep
// its listener alive. `c.String()` of a message is its `Debug`. `subscribed_quality` is new:
// it writes a `livekit.SubscribedQuality` of the expected values on one line. The managers
// are the types themselves, not the interface, so the test that sets a field needs no type
// assertion. The wait and the tick of the `require.Eventually` calls that all have the same
// are two constants.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::go::time;
use crate::protocol::codecs::mime::MimeType;
use crate::protocol::livekit::{self, VideoQuality};
use crate::rtc::types;
use crate::testutils::eventually_async;

#[derive(Default)]
struct TestDynacastManagerListener {
    #[allow(clippy::type_complexity)]
    on_subscribed_max_quality_change:
        Option<Box<dyn Fn(Vec<livekit::SubscribedCodec>) + Send + Sync>>,
    #[allow(clippy::type_complexity)]
    on_subscribed_audio_codec_change:
        Option<Box<dyn Fn(Vec<livekit::SubscribedAudioCodec>) + Send + Sync>>,
}

impl DynacastManagerListener for TestDynacastManagerListener {
    fn on_dynacast_subscribed_max_quality_change(
        &self,
        subscribed_qualities: Vec<livekit::SubscribedCodec>,
        _max_subscribed_qualities: Vec<types::SubscribedCodecQuality>,
    ) {
        (self
            .on_subscribed_max_quality_change
            .as_ref()
            .expect("onSubscribedMaxQualityChange is set"))(subscribed_qualities);
    }

    fn on_dynacast_subscribed_audio_codec_change(
        &self,
        codecs: Vec<livekit::SubscribedAudioCodec>,
    ) {
        (self
            .on_subscribed_audio_codec_change
            .as_ref()
            .expect("onSubscribedAudioCodecChange is set"))(codecs);
    }
}

fn subscribed_quality(quality: VideoQuality, enabled: bool) -> livekit::SubscribedQuality {
    livekit::SubscribedQuality {
        quality: quality as i32,
        enabled,
    }
}

const WAIT_FOR: std::time::Duration = std::time::Duration::from_secs(10);
const TICK: std::time::Duration = std::time::Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribed_max_quality() {
    // t.Run("subscribers muted")
    {
        let actual_subscribed_qualities: Arc<Mutex<Vec<livekit::SubscribedCodec>>> =
            Arc::new(Mutex::new(Vec::new()));

        let listener: Arc<dyn DynacastManagerListener> = Arc::new(TestDynacastManagerListener {
            on_subscribed_max_quality_change: Some(Box::new({
                let actual_subscribed_qualities = actual_subscribed_qualities.clone();
                move |subscribed_qualities| {
                    *actual_subscribed_qualities.lock() = subscribed_qualities;
                }
            })),
            ..Default::default()
        });
        let dm = DynacastManagerVideo::new(DynacastManagerVideoParams {
            listener: Some(Arc::downgrade(&listener)),
            ..Default::default()
        });

        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::High);
        dm.notify_subscriber_max_quality(&"s2".into(), MimeType::Av1, VideoQuality::High);

        // mute all subscribers of vp8
        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::Off);

        let mut expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, false),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, true),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;
    }

    // t.Run("subscribers max quality")
    {
        let actual_subscribed_qualities: Arc<Mutex<Vec<livekit::SubscribedCodec>>> =
            Arc::new(Mutex::new(Vec::new()));

        let listener: Arc<dyn DynacastManagerListener> = Arc::new(TestDynacastManagerListener {
            on_subscribed_max_quality_change: Some(Box::new({
                let actual_subscribed_qualities = actual_subscribed_qualities.clone();
                move |subscribed_qualities| {
                    *actual_subscribed_qualities.lock() = subscribed_qualities;
                }
            })),
            ..Default::default()
        });
        let dm = DynacastManagerVideo::new(DynacastManagerVideoParams {
            listener: Some(Arc::downgrade(&listener)),
            ..Default::default()
        });

        dm.dynacast_manager_base
            .lock
            .write()
            .manager
            .max_subscribed_quality = HashMap::from([
            (MimeType::Vp8, VideoQuality::Low),
            (MimeType::Av1, VideoQuality::Low),
        ]);
        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::High);
        dm.notify_subscriber_max_quality(&"s2".into(), MimeType::Vp8, VideoQuality::Medium);
        dm.notify_subscriber_max_quality(&"s3".into(), MimeType::Av1, VideoQuality::Medium);

        let mut expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, true),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // "s1" dropping to MEDIUM should disable HIGH layer
        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::Medium);

        expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // "s1" , "s2" , "s3" dropping to LOW should disable HIGH & MEDIUM
        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::Low);
        dm.notify_subscriber_max_quality(&"s2".into(), MimeType::Vp8, VideoQuality::Low);
        dm.notify_subscriber_max_quality(&"s3".into(), MimeType::Av1, VideoQuality::Low);

        expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // muting "s2" only should not disable all qualities of vp8, no change of expected qualities
        dm.notify_subscriber_max_quality(&"s2".into(), MimeType::Vp8, VideoQuality::Off);

        time::sleep(100 * time::MILLISECOND).await;
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // muting "s1" and s3 also should disable all qualities
        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::Off);
        dm.notify_subscriber_max_quality(&"s3".into(), MimeType::Av1, VideoQuality::Off);

        expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, false),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, false),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // unmuting "s1" should enable vp8 previously set max quality
        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::Low);

        expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, false),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // a higher quality from a different node should trigger that quality
        dm.notify_subscriber_node_max_quality(
            &"n1".into(),
            &[
                types::SubscribedCodecQuality {
                    codec_mime: MimeType::Vp8,
                    quality: VideoQuality::High,
                },
                types::SubscribedCodecQuality {
                    codec_mime: MimeType::Av1,
                    quality: VideoQuality::Medium,
                },
            ],
        );

        expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, true),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_codec_regression() {
    // t.Run("codec regression video")
    {
        let actual_subscribed_qualities: Arc<Mutex<Vec<livekit::SubscribedCodec>>> =
            Arc::new(Mutex::new(Vec::new()));

        let listener: Arc<dyn DynacastManagerListener> = Arc::new(TestDynacastManagerListener {
            on_subscribed_max_quality_change: Some(Box::new({
                let actual_subscribed_qualities = actual_subscribed_qualities.clone();
                move |subscribed_qualities| {
                    *actual_subscribed_qualities.lock() = subscribed_qualities;
                }
            })),
            ..Default::default()
        });
        let dm = DynacastManagerVideo::new(DynacastManagerVideoParams {
            listener: Some(Arc::downgrade(&listener)),
            ..Default::default()
        });

        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Av1, VideoQuality::High);

        let mut expected_subscribed_qualities = vec![livekit::SubscribedCodec {
            codec: MimeType::Av1.to_string(),
            qualities: vec![
                subscribed_quality(VideoQuality::Low, true),
                subscribed_quality(VideoQuality::Medium, true),
                subscribed_quality(VideoQuality::High, true),
            ],
        }];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        dm.handle_codec_regression(MimeType::Av1, MimeType::Vp8);

        expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, false),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, true),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // av1 quality change should be forwarded to vp8
        // av1 quality change of node should be ignored
        dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Av1, VideoQuality::Medium);
        dm.notify_subscriber_node_max_quality(
            &"n1".into(),
            &[types::SubscribedCodecQuality {
                codec_mime: MimeType::Av1,
                quality: VideoQuality::High,
            }],
        );
        expected_subscribed_qualities = vec![
            livekit::SubscribedCodec {
                codec: MimeType::Av1.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, false),
                    subscribed_quality(VideoQuality::Medium, false),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
            livekit::SubscribedCodec {
                codec: MimeType::Vp8.to_string(),
                qualities: vec![
                    subscribed_quality(VideoQuality::Low, true),
                    subscribed_quality(VideoQuality::Medium, true),
                    subscribed_quality(VideoQuality::High, false),
                ],
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_qualities = actual_subscribed_qualities.lock();

                subscribed_codecs_as_string(&mut expected_subscribed_qualities)
                    == subscribed_codecs_as_string(&mut actual_subscribed_qualities)
            },
            WAIT_FOR,
            TICK,
        )
        .await;
    }

    // t.Run("codec regression audio")
    {
        let actual_subscribed_codecs: Arc<Mutex<Vec<livekit::SubscribedAudioCodec>>> =
            Arc::new(Mutex::new(Vec::new()));

        let listener: Arc<dyn DynacastManagerListener> = Arc::new(TestDynacastManagerListener {
            on_subscribed_audio_codec_change: Some(Box::new({
                let actual_subscribed_codecs = actual_subscribed_codecs.clone();
                move |subscribed_codecs| {
                    *actual_subscribed_codecs.lock() = subscribed_codecs;
                }
            })),
            ..Default::default()
        });
        let dm = DynacastManagerAudio::new(DynacastManagerAudioParams {
            listener: Some(Arc::downgrade(&listener)),
            ..Default::default()
        });

        dm.notify_subscription(&"s1".into(), MimeType::Red, true);

        let mut expected_subscribed_codecs = vec![livekit::SubscribedAudioCodec {
            codec: MimeType::Red.to_string(),
            enabled: true,
        }];
        eventually_async(
            || {
                let mut actual_subscribed_codecs = actual_subscribed_codecs.lock();

                subscribed_audio_codecs_as_string(&mut expected_subscribed_codecs)
                    == subscribed_audio_codecs_as_string(&mut actual_subscribed_codecs)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        dm.handle_codec_regression(MimeType::Red, MimeType::Opus);

        expected_subscribed_codecs = vec![
            livekit::SubscribedAudioCodec {
                codec: MimeType::Red.to_string(),
                enabled: false,
            },
            livekit::SubscribedAudioCodec {
                codec: MimeType::Opus.to_string(),
                enabled: true,
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_codecs = actual_subscribed_codecs.lock();

                subscribed_audio_codecs_as_string(&mut expected_subscribed_codecs)
                    == subscribed_audio_codecs_as_string(&mut actual_subscribed_codecs)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // RED disable as subscriber or subscriber node should be ignored as it has been regressed
        dm.notify_subscription(&"s1".into(), MimeType::Red, false);
        dm.notify_subscription_node(
            &"n1".into(),
            &[livekit::SubscribedAudioCodec {
                codec: MimeType::Red.to_string(),
                enabled: false,
            }],
        );
        eventually_async(
            || {
                let mut actual_subscribed_codecs = actual_subscribed_codecs.lock();

                subscribed_audio_codecs_as_string(&mut expected_subscribed_codecs)
                    == subscribed_audio_codecs_as_string(&mut actual_subscribed_codecs)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // `s1` unsubscription should turn off `opus`
        dm.notify_subscription(&"s1".into(), MimeType::Opus, false);
        expected_subscribed_codecs = vec![
            livekit::SubscribedAudioCodec {
                codec: MimeType::Red.to_string(),
                enabled: false,
            },
            livekit::SubscribedAudioCodec {
                codec: MimeType::Opus.to_string(),
                enabled: false,
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_codecs = actual_subscribed_codecs.lock();

                subscribed_audio_codecs_as_string(&mut expected_subscribed_codecs)
                    == subscribed_audio_codecs_as_string(&mut actual_subscribed_codecs)
            },
            WAIT_FOR,
            TICK,
        )
        .await;

        // a node subscription should turn `opus` back on
        dm.notify_subscription_node(
            &"n1".into(),
            &[livekit::SubscribedAudioCodec {
                codec: MimeType::Opus.to_string(),
                enabled: true,
            }],
        );
        expected_subscribed_codecs = vec![
            livekit::SubscribedAudioCodec {
                codec: MimeType::Red.to_string(),
                enabled: false,
            },
            livekit::SubscribedAudioCodec {
                codec: MimeType::Opus.to_string(),
                enabled: true,
            },
        ];
        eventually_async(
            || {
                let mut actual_subscribed_codecs = actual_subscribed_codecs.lock();

                subscribed_audio_codecs_as_string(&mut expected_subscribed_codecs)
                    == subscribed_audio_codecs_as_string(&mut actual_subscribed_codecs)
            },
            WAIT_FOR,
            TICK,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resend_committed_quality() {
    let notified: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let listener: Arc<dyn DynacastManagerListener> = Arc::new(TestDynacastManagerListener {
        on_subscribed_max_quality_change: Some(Box::new({
            let notified = notified.clone();
            move |mut subscribed_qualities| {
                notified
                    .lock()
                    .push(subscribed_codecs_as_string(&mut subscribed_qualities));
            }
        })),
        ..Default::default()
    });
    let dm = DynacastManagerVideo::new(DynacastManagerVideoParams {
        // keeps the downgrade below pending for the whole test
        dynacast_pause_delay: time::MINUTE,
        listener: Some(Arc::downgrade(&listener)),
        ..Default::default()
    });
    let notifications = || -> Vec<String> { notified.lock().clone() };

    // nothing committed yet, nothing to resend
    dm.resend_committed_quality();
    time::sleep(100 * time::MILLISECOND).await;
    assert!(notifications().is_empty());

    dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::Low);
    eventually_async(
        || notifications().len() == 1,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(10),
    )
    .await;

    // the downgrade waits for the debounce, LOW is still the committed quality
    dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::Off);
    time::sleep(100 * time::MILLISECOND).await;
    assert_eq!(1, notifications().len());

    // the resend notifies LOW again, without committing the pending OFF
    dm.resend_committed_quality();
    eventually_async(
        || notifications().len() == 2,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(10),
    )
    .await;
    assert_eq!(notifications()[0], notifications()[1]);

    // no layer paused, nothing to resend
    dm.notify_subscriber_max_quality(&"s1".into(), MimeType::Vp8, VideoQuality::High);
    eventually_async(
        || notifications().len() == 3,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(10),
    )
    .await;
    dm.resend_committed_quality();
    time::sleep(100 * time::MILLISECOND).await;
    assert_eq!(3, notifications().len());

    // `defer dm.Close()` upstream
    dm.close();
}

fn subscribed_codecs_as_string(c1: &mut [livekit::SubscribedCodec]) -> String {
    c1.sort_by(|a, b| a.codec.cmp(&b.codec));

    let mut s1 = String::new();
    for c in c1.iter() {
        s1.push_str(&format!("{c:?}"));
    }
    s1
}

fn subscribed_audio_codecs_as_string(c1: &mut [livekit::SubscribedAudioCodec]) -> String {
    c1.sort_by(|a, b| a.codec.cmp(&b.codec));

    let mut s1 = String::new();
    for c in c1.iter() {
        s1.push_str(&format!("{c:?}"));
    }
    s1
}
