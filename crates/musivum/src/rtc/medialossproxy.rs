// Ported from LiveKit, pkg/rtc/medialossproxy.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewMediaLossProxy` is `MediaLossProxy::new` and gives
// an `Arc`, as upstream hands the proxy's `HandleMaxLossFeedback` out as a callback; what
// `lock` guards is in an inner struct, the callback with it; the callback of
// `on_media_loss_update` is an `Option`, for the nil that the transport manager sets when it
// closes. The zero time that the first loss is compared with is the Unix epoch here and the
// year 1 upstream: both are more than the interval ago. The tests are new: the original has
// none.
// Who holds whom: the proxy keeps the callback of `on_media_loss_update`; an owner of the
// proxy hands in one that holds it weakly.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time::{self, Duration, Time};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu;
use crate::webrtc::rtcp::receiver_report::ReceiverReport;

const DOWN_LOST_UPDATE_DELTA: Duration = time::SECOND;

pub struct MediaLossProxyParams {
    pub logger: Logger,
}

pub struct MediaLossProxy {
    #[allow(dead_code)] // not read upstream either
    params: MediaLossProxyParams,

    lock: Mutex<MediaLossProxyInner>,
}

// What `lock sync.Mutex` guards upstream.
#[derive(Default)]
struct MediaLossProxyInner {
    max_down_frac_lost: u8,
    max_down_frac_lost_ts: Time,
    max_down_frac_lost_valid: bool,

    on_media_loss_update: Option<Arc<dyn Fn(u8) + Send + Sync>>,
}

impl MediaLossProxy {
    pub fn new(params: MediaLossProxyParams) -> Arc<MediaLossProxy> {
        Arc::new(MediaLossProxy {
            params,
            lock: Mutex::new(MediaLossProxyInner::default()),
        })
    }

    pub fn on_media_loss_update(&self, f: Option<Box<dyn Fn(u8) + Send + Sync>>) {
        let mut m = self.lock.lock();
        m.on_media_loss_update = f.map(Arc::from);
    }

    pub fn handle_max_loss_feedback(&self, _dt: &sfu::DownTrack, report: &ReceiverReport) {
        {
            let mut m = self.lock.lock();
            for rr in &report.reports {
                m.max_down_frac_lost_valid = true;
                if m.max_down_frac_lost < rr.fraction_lost {
                    m.max_down_frac_lost = rr.fraction_lost;
                }
            }
        }

        self.maybe_update_loss();
    }

    pub fn notify_subscriber_node_media_loss(
        &self,
        _node_id: &livekit::NodeId,
        fractional_loss: u8,
    ) {
        {
            let mut m = self.lock.lock();
            m.max_down_frac_lost_valid = true;
            if m.max_down_frac_lost < fractional_loss {
                m.max_down_frac_lost = fractional_loss;
            }
        }

        self.maybe_update_loss();
    }

    fn maybe_update_loss(&self) {
        let mut should_update = false;
        let mut max_lost = 0u8;

        let mut m = self.lock.lock();
        let now = time::now();
        if now.sub(m.max_down_frac_lost_ts) > DOWN_LOST_UPDATE_DELTA && m.max_down_frac_lost_valid {
            should_update = true;
            max_lost = m.max_down_frac_lost;
            m.max_down_frac_lost = 0;
            m.max_down_frac_lost_ts = now;
            m.max_down_frac_lost_valid = false;
        }
        let on_media_loss_update = m.on_media_loss_update.clone();
        drop(m);

        if should_update && let Some(on_media_loss_update) = on_media_loss_update {
            on_media_loss_update(max_lost);
        }
    }
}

#[cfg(test)]
#[path = "medialossproxy_test.rs"]
mod medialossproxy_test;
