// Ported from LiveKit's mediatransportutil, pkg/twcc/twcc.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the responder is made as an `Arc`, as it is shared by
// the buffers of a publisher; the callback, a field that the original writes without the
// lock, has a lock of its own, and a responder without a callback drops the feedback where
// the original would panic on the nil function; the sender SSRC is random as upstream, from
// the standard library's hasher seed instead of `math/rand`.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::webrtc::interceptor::twcc as piontwcc;
use crate::webrtc::rtcp;

const TCC_REPORT_DELTA: i64 = 100_000_000; // 1e8
const TCC_REPORT_DELTA_AFTER_MARK: i64 = 50_000_000; // 50e6

type OnFeedback = Arc<dyn Fn(Vec<Box<dyn rtcp::Packet>>) + Send + Sync>;

/// Responder is a lightweight wrapper around pion/interceptor's implementation of TWCC
pub struct Responder {
    inner: Mutex<ResponderInner>,

    on_feedback: Mutex<Option<OnFeedback>>,
}

// What the `sync.Mutex` of the original guards.
struct ResponderInner {
    #[allow(dead_code)] // not read upstream either
    s_ssrc: u32,
    last_report: i64,
    recorder: piontwcc::Recorder,
}

pub fn new_transport_wide_cc_responder() -> Arc<Responder> {
    let s_ssrc = rand_uint32();
    let recorder = piontwcc::Recorder::new(s_ssrc);
    Arc::new(Responder {
        inner: Mutex::new(ResponderInner {
            s_ssrc,
            last_report: 0,
            recorder,
        }),
        on_feedback: Mutex::new(None),
    })
}

impl Responder {
    /// Push a sequence number read from rtp packet ext packet
    pub fn push(&self, ssrc: u32, sn: u16, time_ns: i64, marker: bool) {
        let mut t = self.inner.lock();

        t.recorder.record(ssrc, sn, time_ns / 1000);

        let delta = time_ns.wrapping_sub(t.last_report);
        if t.recorder.packets_held() > 20
            && (delta >= TCC_REPORT_DELTA
                || t.recorder.packets_held() > 100
                || (marker && delta >= TCC_REPORT_DELTA_AFTER_MARK))
        {
            let pkts = t.recorder.build_feedback_packet();
            if !pkts.is_empty() {
                let on_feedback = self.on_feedback.lock().clone();
                if let Some(on_feedback) = on_feedback {
                    on_feedback(pkts);
                }
            }
            t.last_report = time_ns;
        }
    }

    /// OnFeedback sets the callback for the formed twcc feedback rtcp packet
    pub fn on_feedback(&self, f: impl Fn(Vec<Box<dyn rtcp::Packet>>) + Send + Sync + 'static) {
        *self.on_feedback.lock() = Some(Arc::new(f));
    }
}

// Go's `rand.Uint32()`. Every `RandomState` of the standard library has keys of its own, which
// start from the operating system's randomness; the hash of nothing under them is the number.
fn rand_uint32() -> u32 {
    RandomState::new().build_hasher().finish() as u32
}

#[cfg(test)]
#[path = "twcc_test.rs"]
mod twcc_test;
