// Ported from LiveKit, pkg/sfu/buffer/videolayer.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `String()` is `Display`.

use std::fmt;

pub const INVALID_LAYER_SPATIAL: i32 = -1;
pub const INVALID_LAYER_TEMPORAL: i32 = -1;

pub const DEFAULT_MAX_LAYER_SPATIAL: i32 = 2;
pub const DEFAULT_MAX_LAYER_TEMPORAL: i32 = 3;

pub const INVALID_LAYER: VideoLayer = VideoLayer {
    spatial: INVALID_LAYER_SPATIAL,
    temporal: INVALID_LAYER_TEMPORAL,
};

pub const DEFAULT_MAX_LAYER: VideoLayer = VideoLayer {
    spatial: DEFAULT_MAX_LAYER_SPATIAL,
    temporal: DEFAULT_MAX_LAYER_TEMPORAL,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct VideoLayer {
    pub spatial: i32,
    pub temporal: i32,
}

impl fmt::Display for VideoLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VideoLayer{{s: {}, t: {}}}", self.spatial, self.temporal)
    }
}

impl VideoLayer {
    pub fn greater_than(self, v2: VideoLayer) -> bool {
        self.spatial > v2.spatial || (self.spatial == v2.spatial && self.temporal > v2.temporal)
    }

    pub fn spatial_greater_than_or_equal(self, v2: VideoLayer) -> bool {
        self.spatial >= v2.spatial
    }

    pub fn is_valid(self) -> bool {
        self.spatial != INVALID_LAYER_SPATIAL && self.temporal != INVALID_LAYER_TEMPORAL
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no test of this file.
    #[test]
    fn layers_compare_by_spatial_then_temporal() {
        let a = VideoLayer {
            spatial: 1,
            temporal: 2,
        };
        assert!(a.greater_than(VideoLayer {
            spatial: 1,
            temporal: 1
        }));
        assert!(a.greater_than(VideoLayer {
            spatial: 0,
            temporal: 3
        }));
        assert!(!a.greater_than(a));
        assert!(!a.greater_than(VideoLayer {
            spatial: 2,
            temporal: 0
        }));
        assert!(a.spatial_greater_than_or_equal(VideoLayer {
            spatial: 1,
            temporal: 3
        }));
        assert!(!a.spatial_greater_than_or_equal(VideoLayer {
            spatial: 2,
            temporal: 0
        }));
        assert!(a.is_valid() && DEFAULT_MAX_LAYER.is_valid());
        assert!(!INVALID_LAYER.is_valid());
        assert!(
            !VideoLayer {
                spatial: 0,
                temporal: INVALID_LAYER_TEMPORAL
            }
            .is_valid()
        );
        assert_eq!(a.to_string(), "VideoLayer{s: 1, t: 2}");
    }
}
