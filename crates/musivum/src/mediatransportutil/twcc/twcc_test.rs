// Ported from LiveKit's mediatransportutil, pkg/twcc/twcc_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are a loop over the table. One case of
// the table fails upstream: it is kept with its expected value, and what the original does is
// asserted (see there). The original has one test; the second one here is new and says so.

use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::*;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;

const INVALIDM_SSRC: u32 = 0;
const VALIDM_SSRC: u32 = 1;

#[derive(Clone)]
struct TestPacket {
    sn: u16,
    time_ns: i64,
    marker: bool,
}

fn make_test_packets(start_tm: i64, start_sn: u16, length: u16) -> Vec<TestPacket> {
    let mut pkts = Vec::new();
    for i in 0..length {
        pkts.push(TestPacket {
            sn: start_sn.wrapping_add(i),
            time_ns: start_tm + i as i64,
            marker: false,
        });
    }
    pkts
}

#[test]
fn test_build_feedback_on_push() {
    struct TestCase {
        name: &'static str,
        m_ssrc: u32,
        pkts: Vec<TestPacket>,
        expected_feedback_packets: usize,
    }
    let test_cases = [
        TestCase {
            name: "should not build when fewer than 21 packets",
            m_ssrc: VALIDM_SSRC,
            expected_feedback_packets: 0,
            pkts: make_test_packets(TCC_REPORT_DELTA, 1, 20),
        },
        TestCase {
            name: "should not build when mSSRC is invalid",
            m_ssrc: INVALIDM_SSRC,
            expected_feedback_packets: 0,
            pkts: make_test_packets(TCC_REPORT_DELTA, 1, 21),
        },
        TestCase {
            name: "should not build when delta is too small",
            m_ssrc: VALIDM_SSRC,
            expected_feedback_packets: 0,
            pkts: make_test_packets(0, 1, 21),
        },
        TestCase {
            name: "should build when more than 20 packets and delta is sufficient",
            m_ssrc: VALIDM_SSRC,
            expected_feedback_packets: 1,
            pkts: make_test_packets(TCC_REPORT_DELTA, 1, 21),
        },
        TestCase {
            name: "should build when more than 100 packets regardless of delta",
            m_ssrc: VALIDM_SSRC,
            expected_feedback_packets: 1,
            pkts: make_test_packets(0, 1, 101),
        },
        TestCase {
            name: "should build when more than 20 packets and marker with sufficient delta",
            m_ssrc: VALIDM_SSRC,
            expected_feedback_packets: 1,
            pkts: {
                let mut pkts = make_test_packets(TCC_REPORT_DELTA_AFTER_MARK, 1, 21);
                let last = pkts.len() - 1;
                pkts[last].marker = true; // update final packet to be marker
                pkts
            },
        },
    ];
    for tc in test_cases {
        let fbrecv = Arc::new(AtomicUsize::new(0));
        let responder = new_transport_wide_cc_responder();
        responder.on_feedback({
            let fbrecv = fbrecv.clone();
            move |pkts| {
                fbrecv.fetch_add(pkts.len(), Ordering::SeqCst);
            }
        });
        for pkt in &tc.pkts {
            responder.push(tc.m_ssrc, pkt.sn, pkt.time_ns, pkt.marker);
        }
        let fbrecv = fbrecv.load(Ordering::SeqCst);
        if tc.name == "should not build when mSSRC is invalid" {
            // This case fails in the original (run at the versions LiveKit is built with:
            // "expected: 0, actual: 1"). Neither the responder nor pion's recorder looks at
            // the media SSRC, so a feedback packet is built for SSRC 0 as for any other. The
            // table keeps upstream's expectation; what is asserted is what the original does.
            assert_ne!(tc.expected_feedback_packets, fbrecv, "{}", tc.name);
            assert_eq!(1, fbrecv, "{}", tc.name);
            continue;
        }
        assert_eq!(tc.expected_feedback_packets, fbrecv, "{}", tc.name);
    }
}

// New for musivum: several reports one after the other, which the original does not test (its
// cases end with the first report, so nothing there depends on the time of the last one). The
// expected numbers of packets per feedback are what the original gives for the same pushes.
#[test]
fn feedback_is_paced_by_the_time_of_the_last_report() {
    // The number of packets each feedback reports on.
    let sizes: Arc<Mutex<Vec<u16>>> = Arc::new(Mutex::new(Vec::new()));
    let responder = new_transport_wide_cc_responder();
    responder.on_feedback({
        let sizes = sizes.clone();
        move |pkts| {
            for p in &pkts {
                let twcc = p
                    .as_any()
                    .downcast_ref::<TransportLayerCc>()
                    .expect("a transport layer cc packet");
                sizes.lock().push(twcc.packet_status_count);
            }
        }
    });
    let mut sn: u16 = 1;
    let mut push = |n: i64, tm: i64, marker: bool| {
        for i in 0..n {
            responder.push(VALIDM_SSRC, sn, tm + i, marker);
            sn = sn.wrapping_add(1);
        }
    };

    // 21 packets at 100 ms: a first feedback, with the last of them.
    push(21, TCC_REPORT_DELTA, false);
    assert_eq!(vec![21], *sizes.lock());
    let last_report = TCC_REPORT_DELTA + 20;

    // 30 more 10 ms later: held, the last report was 10 ms ago.
    push(30, TCC_REPORT_DELTA + 10_000_000, false);
    assert_eq!(vec![21], *sizes.lock());

    // One with the marker 49 ms after the last report: still held.
    push(
        1,
        last_report + TCC_REPORT_DELTA_AFTER_MARK - 1_000_000,
        true,
    );
    assert_eq!(vec![21], *sizes.lock());

    // One with the marker 50 ms after the last report: a feedback.
    push(1, last_report + TCC_REPORT_DELTA_AFTER_MARK, true);
    assert_eq!(vec![21, 32], *sizes.lock());
    let last_report = last_report + TCC_REPORT_DELTA_AFTER_MARK;

    // 21 more right after: held. Then one 100 ms after the last report: a feedback.
    push(21, last_report + 1000, false);
    assert_eq!(vec![21, 32], *sizes.lock());
    push(1, last_report + TCC_REPORT_DELTA, false);
    assert_eq!(vec![21, 32, 22], *sizes.lock());
}
