// Ported from LiveKit, pkg/sfu/buffer/videolayerutils_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are the rows of a table, each run in a
// loop and named in the messages of its assertions; the maps of the tables are lists of
// pairs, in the order they are written in; a `VideoLayersRid` that the original writes with
// fewer than three rids has the empty ones written out, through the helper `rids`.

use super::*;

#[test]
fn test_rid_conversion() {
    struct RidAndLayer {
        rid: &'static str,
        layer: i32,
    }
    struct Test {
        name: &'static str,
        track_info: Option<livekit::TrackInfo>,
        mime_type: mime::MimeType,
        rid_to_layer: Vec<(&'static str, RidAndLayer)>,
    }
    let tests = vec![
        Test {
            name: "no track info",
            track_info: None,
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: FULL_RESOLUTION_F,
                        layer: 2,
                    },
                ),
            ],
        },
        Test {
            name: "no layers",
            track_info: Some(livekit::TrackInfo::default()),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: FULL_RESOLUTION_F,
                        layer: 2,
                    },
                ),
            ],
        },
        Test {
            name: "single layer, low",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Low as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
            ],
        },
        Test {
            name: "single layer, medium",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Medium as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
            ],
        },
        Test {
            name: "single layer, high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Medium as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
            ],
        },
        Test {
            name: "two layers, low and medium",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
            ],
        },
        Test {
            name: "two layers, low and high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
            ],
        },
        Test {
            name: "two layers, medium and high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
            ],
        },
        Test {
            name: "three layers",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_layer: vec![
                (
                    "",
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    QUARTER_RESOLUTION_Q,
                    RidAndLayer {
                        rid: QUARTER_RESOLUTION_Q,
                        layer: 0,
                    },
                ),
                (
                    HALF_RESOLUTION_H,
                    RidAndLayer {
                        rid: HALF_RESOLUTION_H,
                        layer: 1,
                    },
                ),
                (
                    FULL_RESOLUTION_F,
                    RidAndLayer {
                        rid: FULL_RESOLUTION_F,
                        layer: 2,
                    },
                ),
            ],
        },
    ];

    for test in tests {
        for (test_rid, expected_result) in &test.rid_to_layer {
            let actual_layer = rid_to_spatial_layer(
                test.mime_type,
                test_rid,
                test.track_info.as_ref(),
                &DEFAULT_VIDEO_LAYERS_RID,
            );
            assert_eq!(
                expected_result.layer, actual_layer,
                "{}: rid {test_rid:?}",
                test.name
            );

            let actual_rid = spatial_layer_to_rid(
                test.mime_type,
                actual_layer,
                test.track_info.as_ref(),
                &DEFAULT_VIDEO_LAYERS_RID,
            );
            assert_eq!(
                expected_result.rid, actual_rid,
                "{}: rid {test_rid:?}",
                test.name
            );
        }
    }
}

#[test]
fn test_quality_conversion() {
    struct QualityAndLayer {
        quality: livekit::VideoQuality,
        layer: i32,
    }
    struct Test {
        name: &'static str,
        track_info: Option<livekit::TrackInfo>,
        mime_type: mime::MimeType,
        quality_to_layer: Vec<(livekit::VideoQuality, QualityAndLayer)>,
    }
    let tests = vec![
        Test {
            name: "no track info",
            track_info: None,
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 1,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 2,
                    },
                ),
            ],
        },
        Test {
            name: "no layers",
            track_info: Some(livekit::TrackInfo::default()),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 1,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 2,
                    },
                ),
            ],
        },
        Test {
            name: "single layer, low",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Low as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
            ],
        },
        Test {
            name: "single layer, medium",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Medium as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 0,
                    },
                ),
            ],
        },
        Test {
            name: "single layer, high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::High as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 0,
                    },
                ),
            ],
        },
        Test {
            name: "two layers, low and medium",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 1,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 1,
                    },
                ),
            ],
        },
        Test {
            name: "two layers, low and high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 1,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 1,
                    },
                ),
            ],
        },
        Test {
            name: "two layers, medium and high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 1,
                    },
                ),
            ],
        },
        Test {
            name: "three layers",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            quality_to_layer: vec![
                (
                    livekit::VideoQuality::Low,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Low,
                        layer: 0,
                    },
                ),
                (
                    livekit::VideoQuality::Medium,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::Medium,
                        layer: 1,
                    },
                ),
                (
                    livekit::VideoQuality::High,
                    QualityAndLayer {
                        quality: livekit::VideoQuality::High,
                        layer: 2,
                    },
                ),
            ],
        },
    ];

    for test in tests {
        for (test_quality, expected_result) in &test.quality_to_layer {
            let actual_layer = video_quality_to_spatial_layer(
                test.mime_type,
                *test_quality,
                test.track_info.as_ref(),
            );
            assert_eq!(
                expected_result.layer, actual_layer,
                "{}: quality {test_quality:?}",
                test.name
            );

            let actual_quality = spatial_layer_to_video_quality(
                test.mime_type,
                actual_layer,
                test.track_info.as_ref(),
            );
            assert_eq!(
                expected_result.quality, actual_quality,
                "{}: quality {test_quality:?}",
                test.name
            );
        }
    }
}

#[test]
fn test_video_quality_to_rid_conversion() {
    struct Test {
        name: &'static str,
        track_info: Option<livekit::TrackInfo>,
        mime_tye: mime::MimeType,
        quality_to_rid: Vec<(livekit::VideoQuality, &'static str)>,
    }
    let tests = vec![
        Test {
            name: "no track info",
            track_info: None,
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, HALF_RESOLUTION_H),
                (livekit::VideoQuality::High, FULL_RESOLUTION_F),
            ],
        },
        Test {
            name: "no layers",
            track_info: Some(livekit::TrackInfo::default()),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, HALF_RESOLUTION_H),
                (livekit::VideoQuality::High, FULL_RESOLUTION_F),
            ],
        },
        Test {
            name: "single layer, low",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Low as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::High, QUARTER_RESOLUTION_Q),
            ],
        },
        Test {
            name: "single layer, medium",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Medium as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::High, QUARTER_RESOLUTION_Q),
            ],
        },
        Test {
            name: "single layer, high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::High as i32,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::High, QUARTER_RESOLUTION_Q),
            ],
        },
        Test {
            name: "two layers, low and medium",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, HALF_RESOLUTION_H),
                (livekit::VideoQuality::High, HALF_RESOLUTION_H),
            ],
        },
        Test {
            name: "two layers, low and high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, HALF_RESOLUTION_H),
                (livekit::VideoQuality::High, HALF_RESOLUTION_H),
            ],
        },
        Test {
            name: "two layers, medium and high",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::High, HALF_RESOLUTION_H),
            ],
        },
        Test {
            name: "three layers",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_tye: mime::MimeType::Vp8,
            quality_to_rid: vec![
                (livekit::VideoQuality::Low, QUARTER_RESOLUTION_Q),
                (livekit::VideoQuality::Medium, HALF_RESOLUTION_H),
                (livekit::VideoQuality::High, FULL_RESOLUTION_F),
            ],
        },
    ];

    for test in tests {
        for (test_quality, expected_rid) in &test.quality_to_rid {
            let actual_rid = video_quality_to_rid(
                test.mime_tye,
                *test_quality,
                test.track_info.as_ref(),
                &DEFAULT_VIDEO_LAYERS_RID,
            );
            assert_eq!(
                *expected_rid, actual_rid,
                "{}: quality {test_quality:?}",
                test.name
            );
        }
    }
}

#[test]
fn test_get_spatial_layer_for_rid() {
    struct Test {
        name: &'static str,
        track_info: Option<livekit::TrackInfo>,
        mime_type: mime::MimeType,
        rid_to_spatial_layer: Vec<(&'static str, i32)>,
    }
    let tests = vec![
        Test {
            name: "no track info",
            track_info: None,
            mime_type: mime::MimeType::Vp8,
            rid_to_spatial_layer: vec![
                (QUARTER_RESOLUTION_Q, INVALID_LAYER_SPATIAL),
                (HALF_RESOLUTION_H, INVALID_LAYER_SPATIAL),
                (FULL_RESOLUTION_F, INVALID_LAYER_SPATIAL),
            ],
        },
        Test {
            name: "no layers",
            track_info: Some(livekit::TrackInfo::default()),
            mime_type: mime::MimeType::Vp8,
            rid_to_spatial_layer: vec![
                // SIMULCAST-CODEC-TODO
                // (QUARTER_RESOLUTION_Q, INVALID_LAYER_SPATIAL),
                // (HALF_RESOLUTION_H, INVALID_LAYER_SPATIAL),
                // (FULL_RESOLUTION_F, INVALID_LAYER_SPATIAL),
                (QUARTER_RESOLUTION_Q, 0),
                (HALF_RESOLUTION_H, 0),
                (FULL_RESOLUTION_F, 0),
            ],
        },
        Test {
            name: "no rid",
            track_info: Some(livekit::TrackInfo::default()),
            mime_type: mime::MimeType::Vp8,
            rid_to_spatial_layer: vec![("", 0)],
        },
        Test {
            name: "single layer",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![livekit::VideoLayer {
                        quality: livekit::VideoQuality::Low as i32,
                        spatial_layer: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_spatial_layer: vec![
                (QUARTER_RESOLUTION_Q, 0),
                (HALF_RESOLUTION_H, 0),
                (FULL_RESOLUTION_F, 0),
            ],
        },
        Test {
            name: "layers",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            spatial_layer: 0,
                            rid: QUARTER_RESOLUTION_Q.to_string(),
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            spatial_layer: 1,
                            rid: HALF_RESOLUTION_H.to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_spatial_layer: vec![
                (QUARTER_RESOLUTION_Q, 0),
                (HALF_RESOLUTION_H, 1),
                // SIMULCAST-CODEC-TODO
                // (FULL_RESOLUTION_F, INVALID_LAYER_SPATIAL),
                (FULL_RESOLUTION_F, 0),
            ],
        },
        Test {
            name: "layers - no rid",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            spatial_layer: 0,
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            spatial_layer: 1,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            rid_to_spatial_layer: vec![
                (QUARTER_RESOLUTION_Q, 0),
                (HALF_RESOLUTION_H, 0),
                (FULL_RESOLUTION_F, 0),
            ],
        },
    ];

    for test in tests {
        for (test_rid, expected_spatial_layer) in &test.rid_to_spatial_layer {
            let actual_spatial_layer =
                get_spatial_layer_for_rid(test.mime_type, test_rid, test.track_info.as_ref());
            assert_eq!(
                *expected_spatial_layer, actual_spatial_layer,
                "{}: rid {test_rid:?}",
                test.name
            );
        }
    }
}

#[test]
fn test_get_spatial_layer_for_video_quality() {
    struct Test {
        name: &'static str,
        track_info: Option<livekit::TrackInfo>,
        mime_type: mime::MimeType,
        video_quality_to_spatial_layer: Vec<(livekit::VideoQuality, i32)>,
    }
    let tests = vec![
        Test {
            name: "no track info",
            track_info: None,
            mime_type: mime::MimeType::Vp8,
            video_quality_to_spatial_layer: vec![
                (livekit::VideoQuality::Low, INVALID_LAYER_SPATIAL),
                (livekit::VideoQuality::Medium, INVALID_LAYER_SPATIAL),
                (livekit::VideoQuality::High, INVALID_LAYER_SPATIAL),
                (livekit::VideoQuality::Off, INVALID_LAYER_SPATIAL),
            ],
        },
        Test {
            name: "no layers",
            track_info: Some(livekit::TrackInfo::default()),
            mime_type: mime::MimeType::Vp8,
            video_quality_to_spatial_layer: vec![
                (livekit::VideoQuality::Low, 0),
                (livekit::VideoQuality::Medium, 0),
                (livekit::VideoQuality::High, 0),
                (livekit::VideoQuality::Off, INVALID_LAYER_SPATIAL),
            ],
        },
        Test {
            name: "not all layers",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            spatial_layer: 0,
                            rid: QUARTER_RESOLUTION_Q.to_string(),
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            spatial_layer: 1,
                            rid: HALF_RESOLUTION_H.to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            video_quality_to_spatial_layer: vec![
                (livekit::VideoQuality::Low, 0),
                (livekit::VideoQuality::Medium, 1),
                (livekit::VideoQuality::High, 1),
                (livekit::VideoQuality::Off, INVALID_LAYER_SPATIAL),
            ],
        },
        Test {
            name: "all layers",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            spatial_layer: 0,
                            rid: QUARTER_RESOLUTION_Q.to_string(),
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            spatial_layer: 1,
                            rid: HALF_RESOLUTION_H.to_string(),
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::High as i32,
                            spatial_layer: 2,
                            rid: FULL_RESOLUTION_F.to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            video_quality_to_spatial_layer: vec![
                (livekit::VideoQuality::Low, 0),
                (livekit::VideoQuality::Medium, 1),
                (livekit::VideoQuality::High, 2),
                (livekit::VideoQuality::Off, INVALID_LAYER_SPATIAL),
            ],
        },
    ];

    for test in tests {
        for (test_video_quality, expected_spatial_layer) in &test.video_quality_to_spatial_layer {
            let actual_spatial_layer = get_spatial_layer_for_video_quality(
                test.mime_type,
                *test_video_quality,
                test.track_info.as_ref(),
            );
            assert_eq!(
                *expected_spatial_layer, actual_spatial_layer,
                "{}: quality {test_video_quality:?}",
                test.name
            );
        }
    }
}

#[test]
fn test_get_video_qualityor_spatial_layer() {
    struct Test {
        name: &'static str,
        track_info: Option<livekit::TrackInfo>,
        mime_type: mime::MimeType,
        spatial_layer_to_video_quality: Vec<(i32, livekit::VideoQuality)>,
    }
    let tests = vec![
        Test {
            name: "no track info",
            track_info: None,
            mime_type: mime::MimeType::Vp8,
            spatial_layer_to_video_quality: vec![
                (INVALID_LAYER_SPATIAL, livekit::VideoQuality::Off),
                (0, livekit::VideoQuality::Off),
                (1, livekit::VideoQuality::Off),
                (2, livekit::VideoQuality::Off),
            ],
        },
        Test {
            name: "no layers",
            track_info: Some(livekit::TrackInfo::default()),
            mime_type: mime::MimeType::Vp8,
            spatial_layer_to_video_quality: vec![
                (INVALID_LAYER_SPATIAL, livekit::VideoQuality::Off),
                (0, livekit::VideoQuality::Off),
                (1, livekit::VideoQuality::Off),
                (2, livekit::VideoQuality::Off),
            ],
        },
        Test {
            name: "layers",
            track_info: Some(livekit::TrackInfo {
                codecs: vec![livekit::SimulcastCodecInfo {
                    mime_type: mime::MimeType::Vp8.to_string(),
                    layers: vec![
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Low as i32,
                            spatial_layer: 0,
                            rid: QUARTER_RESOLUTION_Q.to_string(),
                            ..Default::default()
                        },
                        livekit::VideoLayer {
                            quality: livekit::VideoQuality::Medium as i32,
                            spatial_layer: 1,
                            rid: HALF_RESOLUTION_H.to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            mime_type: mime::MimeType::Vp8,
            spatial_layer_to_video_quality: vec![
                (INVALID_LAYER_SPATIAL, livekit::VideoQuality::Off),
                (0, livekit::VideoQuality::Low),
                (1, livekit::VideoQuality::Medium),
                (2, livekit::VideoQuality::Off),
            ],
        },
    ];

    for test in tests {
        for (test_spatial_layer, expected_video_quality) in &test.spatial_layer_to_video_quality {
            let actual_video_quality = get_video_quality_for_spatial_layer(
                test.mime_type,
                *test_spatial_layer,
                test.track_info.as_ref(),
            );
            assert_eq!(
                *expected_video_quality, actual_video_quality,
                "{}: layer {test_spatial_layer}",
                test.name
            );
        }
    }
}

#[test]
fn test_normalize_video_layers_rid() {
    fn rids(rids: [&str; 3]) -> VideoLayersRid {
        rids.map(str::to_string)
    }
    struct Test {
        name: &'static str,
        rids: VideoLayersRid,
        normalized: VideoLayersRid,
    }
    let tests = vec![
        Test {
            name: "empty",
            rids: VideoLayersRid::default(),
            normalized: VideoLayersRid::default(),
        },
        Test {
            name: "unknown pattern",
            rids: rids(["3", "2", "1"]),
            normalized: rids(["3", "2", "1"]),
        },
        Test {
            name: "qhf",
            rids: VIDEO_LAYERS_RID_QHF.clone(),
            normalized: VIDEO_LAYERS_RID_QHF.clone(),
        },
        Test {
            name: "scrambled qhf",
            rids: rids(["f", "h", "q"]),
            normalized: VIDEO_LAYERS_RID_QHF.clone(),
        },
        Test {
            name: "partial qhf",
            rids: rids(["h", "q", ""]),
            normalized: rids(["q", "h", ""]),
        },
        Test {
            name: "210",
            rids: VIDEO_LAYERS_RID_210.clone(),
            normalized: VIDEO_LAYERS_RID_210.clone(),
        },
        Test {
            name: "scrambled 210",
            rids: rids(["2", "0", "1"]),
            normalized: VIDEO_LAYERS_RID_210.clone(),
        },
        Test {
            name: "partial 210",
            rids: rids(["1", "2", ""]),
            normalized: rids(["2", "1", ""]),
        },
    ];

    for test in tests {
        let normalized_rids = normalize_video_layers_rid(&test.rids);
        assert_eq!(test.normalized, normalized_rids, "{}", test.name);
    }
}
