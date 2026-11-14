// Ported from LiveKit's mediatransportutil, pkg/interceptor/rttfromxr.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust.
// - Upstream this is a pion interceptor and its factory. Here nothing registers it: the
//   connection's sink (`sink.rs`) calls it. `NewInterceptor(id)` cannot fail and returns the
//   object, as an `Arc` (its worker holds it weakly, and so ends when the object is let go of,
//   closed or not). `BindRemoteStream(info, reader)` and `BindLocalStream(info, writer)`,
//   which returned what they were given, take the info only. `BindRTCPReader(reader)`, which
//   wrapped the reader in a function that sees every compound packet that is read, is that
//   function: `read_rtcp(pkts)`, for a caller that has the packets parsed already.
//   `BindRTCPWriter(writer)` takes an `RtcpWriter`, the one method of pion's
//   `interceptor.RTCPWriter` without the attributes and the count of bytes, which nothing
//   here reads; it is declared here because `webrtc::interceptor` has no such trait.
// - What the lock guards is `RttFromXrInner`. The channel `close` is a cancellation token, so
//   `close` may be called more than once (Go panics on closing a closed channel), and it also
//   lets go of the writer, which may hold the connection.
// - The report blocks of an extended report are `Box<dyn rtcp::Packet>` in `rtc-rtcp`; Go's
//   type switch is a downcast.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::go::time;
use crate::mediatransportutil as util;
use crate::webrtc;
use crate::webrtc::rtcp;
use crate::webrtc::rtcp::extended_report::{
    DLRRReport, DLRRReportBlock, ExtendedReport, ReceiverReferenceTimeReportBlock,
};

/// pion's `interceptor.RTCPWriter`: where an interceptor writes RTCP of its own.
pub trait RtcpWriter: Send + Sync {
    /// Write a batch of rtcp packets
    fn write(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), webrtc::Error>;
}

/// pion's `interceptor.RTCPWriterFunc`: a function is a writer.
impl<F> RtcpWriter for F
where
    F: Fn(&[Box<dyn rtcp::Packet>]) -> Result<(), webrtc::Error> + Send + Sync,
{
    fn write(&self, pkts: &[Box<dyn rtcp::Packet>]) -> Result<(), webrtc::Error> {
        self(pkts)
    }
}

pub struct RttFromXrFactory {
    on_rtt: Arc<dyn Fn(u32) + Send + Sync>,
}

impl RttFromXrFactory {
    pub fn new(on_rtt: impl Fn(u32) + Send + Sync + 'static) -> Self {
        Self {
            on_rtt: Arc::new(on_rtt),
        }
    }

    pub fn new_interceptor(&self, _id: &str) -> Arc<RttFromXr> {
        Arc::new_cyclic(|this| RttFromXr {
            this: this.clone(),
            lock: Mutex::new(RttFromXrInner {
                started: false,
                remote_track_ssrcs: Vec::new(),
                local_track_ssrcs: Vec::new(),
                writer: None,
            }),
            on_rtt: self.on_rtt.clone(),
            close: CancellationToken::new(),
        })
    }
}

pub struct RttFromXr {
    this: Weak<RttFromXr>,
    lock: Mutex<RttFromXrInner>,
    on_rtt: Arc<dyn Fn(u32) + Send + Sync>,

    close: CancellationToken,
}

// What the lock guards.
struct RttFromXrInner {
    started: bool,
    remote_track_ssrcs: Vec<u32>,
    local_track_ssrcs: Vec<u32>,
    writer: Option<Arc<dyn RtcpWriter>>,
}

impl RttFromXr {
    // (the goroutine: it holds the object only while it works, so that it ends with it)
    async fn start(this: Weak<RttFromXr>, close: CancellationToken) {
        loop {
            tokio::select! {
                _ = time::sleep(time::SECOND) => {
                    let Some(r) = this.upgrade() else {
                        return;
                    };
                    let mut sender_ssrc = 0u32;
                    let writer = {
                        let inner = r.lock.lock();
                        if !inner.remote_track_ssrcs.is_empty() {
                            sender_ssrc = inner.remote_track_ssrcs[0];
                        }
                        inner.writer.clone()
                    };
                    let Some(writer) = writer else {
                        continue;
                    };
                    if sender_ssrc == 0 {
                        continue;
                    }

                    let xr = ExtendedReport {
                        sender_ssrc,
                        reports: vec![Box::new(ReceiverReferenceTimeReportBlock {
                            ntp_timestamp: util::to_ntp_time(time::now()).0,
                        })],
                    };
                    let _ = writer.write(&[Box::new(xr)]);
                }
                _ = close.cancelled() => {
                    return;
                }
            }
        }
    }

    pub fn bind_remote_stream(&self, info: &webrtc::StreamInfo) {
        let mut r = self.lock.lock();
        if !r.started {
            r.started = true;
            time::spawn(Self::start(self.this.clone(), self.close.clone()));
        }

        r.remote_track_ssrcs.push(info.ssrc);
    }

    pub fn unbind_remote_stream(&self, info: &webrtc::StreamInfo) {
        let mut r = self.lock.lock();
        for i in 0..r.remote_track_ssrcs.len() {
            if r.remote_track_ssrcs[i] == info.ssrc {
                r.remote_track_ssrcs.swap_remove(i);
                break;
            }
        }
    }

    pub fn bind_local_stream(&self, info: &webrtc::StreamInfo) {
        let mut r = self.lock.lock();
        r.local_track_ssrcs.push(info.ssrc);
    }

    pub fn unbind_local_stream(&self, info: &webrtc::StreamInfo) {
        let mut r = self.lock.lock();
        for i in 0..r.local_track_ssrcs.len() {
            if r.local_track_ssrcs[i] == info.ssrc {
                r.local_track_ssrcs.swap_remove(i);
                break;
            }
        }
    }

    // BindRTCPReader: what the function it returns does with a compound packet that was read
    pub fn read_rtcp(&self, pkts: &[Box<dyn rtcp::Packet>]) {
        for pkt in pkts {
            if let Some(xr) = pkt.as_any().downcast_ref::<ExtendedReport>() {
                self.handle_xr(xr);
                break;
            }
        }
    }

    pub fn bind_rtcp_writer(&self, writer: Arc<dyn RtcpWriter>) {
        let mut r = self.lock.lock();
        r.writer = Some(writer);
    }

    pub fn close(&self) {
        self.close.cancel();
        // not in the original: see the header
        let writer = self.lock.lock().writer.take();
        drop(writer);
    }

    fn handle_xr(&self, xr: &ExtendedReport) {
        for report in &xr.reports {
            if let Some(rr) = report
                .as_any()
                .downcast_ref::<ReceiverReferenceTimeReportBlock>()
            {
                let mut responses: Vec<Box<dyn rtcp::Packet>> = Vec::new();
                let writer = {
                    let r = self.lock.lock();
                    for &ssrc in &r.local_track_ssrcs {
                        responses.push(Box::new(ExtendedReport {
                            sender_ssrc: ssrc,
                            reports: vec![Box::new(DLRRReportBlock {
                                reports: vec![DLRRReport {
                                    ssrc: xr.sender_ssrc,
                                    last_rr: (rr.ntp_timestamp >> 16) as u32,
                                    dlrr: 0, //respond immediately
                                }],
                            })],
                        }));
                    }
                    r.writer.clone()
                };
                if !responses.is_empty()
                    && let Some(writer) = writer
                {
                    let _ = writer.write(&responses);
                }
            } else if let Some(rr) = report.as_any().downcast_ref::<DLRRReportBlock>() {
                // (only the first report: the original leaves the loop after it)
                if let Some(dlrr_report) = rr.reports.first() {
                    let now_ntp = util::to_ntp_time(time::now());
                    let now_ntp32 = (now_ntp.0 >> 16) as u32;
                    let ntp_diff = now_ntp32
                        .wrapping_sub(dlrr_report.last_rr)
                        .wrapping_sub(dlrr_report.dlrr);
                    let rtt = (ntp_diff as f64 * 1000.0 / 65536.0).ceil() as u32;
                    (self.on_rtt)(rtt);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "rttfromxr_test.rs"]
mod rttfromxr_test;
