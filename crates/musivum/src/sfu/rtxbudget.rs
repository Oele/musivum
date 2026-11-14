// Ported from LiveKit, pkg/sfu/rtxbudget.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the `yaml` tags of the configuration are left out, as
// nothing reads a configuration file yet; `DefaultRTXBudgetConfig` is a constant; what `lock`
// guards is in an inner struct. A nil budget is "no cap" in the original, and its methods
// may be called on one: here `new` gives `None` for it, and `on_media_sent`, `allow` and
// `spend` take the budget as an `Option`, their first argument, where the original has the
// receiver that may be nil. The sizes are unsigned.

use parking_lot::Mutex;

use crate::go::atomic;
use crate::webrtc;

// RTXBudgetConfig caps the bytes a down track retransmits in response to NACKs.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RtxBudgetConfig {
    pub audio: RtxBudgetLimit,
    pub video: RtxBudgetLimit,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RtxBudgetLimit {
    // retransmitted bytes allowed, as a fraction of the media bytes sent; 0 turns the cap off
    pub max_ratio: f64,
    // retransmitted bytes allowed at once, above the steady ratio; at least one full packet
    pub burst_bytes: i64,
}

// the budget never holds less than one full packet
const MIN_RTX_BURST_BYTES: i64 = 1500;

// The bursts cover about 250 ms of total loss: a 2 Mbps video stream and a 128 kbps audio stream.
pub const DEFAULT_RTX_BUDGET_CONFIG: RtxBudgetConfig = RtxBudgetConfig {
    audio: RtxBudgetLimit {
        max_ratio: 0.0,
        burst_bytes: 4_000,
    },
    video: RtxBudgetLimit {
        max_ratio: 0.0,
        burst_bytes: 64_000,
    },
};

impl RtxBudgetConfig {
    pub(super) fn for_kind(&self, kind: webrtc::RtpCodecType) -> RtxBudgetLimit {
        if kind == webrtc::RtpCodecType::Audio {
            return self.audio;
        }
        self.video
    }
}

// rtxBudget is a token bucket filled by media bytes sent and drained by retransmitted bytes.
// A retransmission is allowed while the bucket is above zero and may overdraw it, so the
// check needs no packet size and runs before the packet is read from the publisher's buffer.
// A nil budget allows every retransmission.
pub(super) struct RtxBudget {
    ratio: f64,
    burst: f64,

    media_bytes: atomic::Uint64,

    lock: Mutex<RtxBudgetInner>,

    // NACKed packets not retransmitted because the budget was empty
    pub(super) num_capped: atomic::Uint64,
}

// What `lock` guards.
struct RtxBudgetInner {
    tokens: f64,
    media_bytes_seen: u64,
}

impl RtxBudget {
    pub(super) fn new(config: RtxBudgetLimit) -> Option<RtxBudget> {
        if config.max_ratio <= 0.0 {
            return None;
        }
        let burst = config.burst_bytes.max(MIN_RTX_BURST_BYTES) as f64;
        Some(RtxBudget {
            ratio: config.max_ratio,
            burst,
            media_bytes: atomic::Uint64::new(0),
            lock: Mutex::new(RtxBudgetInner {
                tokens: burst,
                media_bytes_seen: 0,
            }),
            num_capped: atomic::Uint64::new(0),
        })
    }

    pub(super) fn on_media_sent(b: Option<&RtxBudget>, size: usize) {
        let Some(b) = b else {
            return;
        };
        b.media_bytes.add(size as u64);
    }

    // allow reports whether the budget has room for another retransmission.
    pub(super) fn allow(b: Option<&RtxBudget>) -> bool {
        let Some(b) = b else {
            return true;
        };

        let mut inner = b.lock.lock();

        let media_bytes = b.media_bytes.load();
        inner.tokens = min(
            b.burst,
            inner.tokens + b.ratio * media_bytes.wrapping_sub(inner.media_bytes_seen) as f64,
        );
        inner.media_bytes_seen = media_bytes;
        inner.tokens > 0.0
    }

    pub(super) fn spend(b: Option<&RtxBudget>, size: usize) {
        let Some(b) = b else {
            return;
        };

        let mut inner = b.lock.lock();
        inner.tokens -= size as f64;
    }
}

// Go's builtin `min` of two floats, which is a NaN if either is one (`f64::min` is then the
// other).
fn min(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        f64::NAN
    } else {
        x.min(y)
    }
}

#[cfg(test)]
#[path = "rtxbudget_test.rs"]
mod rtxbudget_test;
