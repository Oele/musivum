// Ported from LiveKit, pkg/sfu/pacer/base_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; there are no pools, so the test of the header
// extensions makes a header and a packet per send; its assertion on allocations per send is
// left out, and so is the assertion of `test_hold_extension` that the held extension lies
// in the packet's scratch, which does not exist here. What follows `test_hold_extension` is
// new: helpers that the tests of the other files of the package use too (an estimator and a
// writer that record, the packets of the reference run), and tests of what the original's
// two tests leave out, with expectations taken from a run of the original.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use parking_lot::Mutex;

use super::super::{
    LeakyBucket, MAX_HELD_EXTENSION_SIZE, NoQueue, Pacer, PacerProbeObserverListener, PassThrough,
};
use super::*;
use crate::go::time::{self, Time};
use crate::protocol::logger;
use crate::sfu::bwe::{self, BweListener, BweType, CongestionState};
use crate::sfu::ccutils::{
    self, PROBE_CLUSTER_ID_INVALID, PROBE_CLUSTER_INFO_INVALID, ProbeClusterGoal, ProbeClusterId,
    ProbeClusterInfo, ProbeClusterResult,
};
use crate::testutils::{capture_log, eventually_async};
use crate::webrtc::TrackLocalWriter;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;

// bwe.NullBWE is meant to be embedded and lacks Type
#[derive(Default)]
struct NullBwe {
    null_bwe: bwe::NullBwe,
}

impl Bwe for NullBwe {
    fn r#type(&self) -> BweType {
        BweType::None
    }

    fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn BweListener>>) {
        self.null_bwe.set_bwe_listener(bwe_listner);
    }

    fn reset(&self) {
        self.null_bwe.reset();
    }

    fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    ) {
        self.null_bwe.handle_remb(
            received_estimate,
            expected_bandwidth_usage,
            sent_packets,
            repeated_nacks,
        );
    }

    fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ProbeClusterId,
        is_probe: bool,
    ) -> u16 {
        self.null_bwe.record_packet_send_and_get_sequence_number(
            at_micro,
            size,
            is_rtx,
            probe_cluster_id,
            is_probe,
        )
    }

    fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        self.null_bwe.handle_twcc_feedback(report);
    }

    fn update_rtt(&self, rtt: f64) {
        self.null_bwe.update_rtt(rtt);
    }

    fn congestion_state(&self) -> CongestionState {
        self.null_bwe.congestion_state()
    }

    fn can_probe(&self) -> bool {
        self.null_bwe.can_probe()
    }

    fn probe_duration(&self) -> Duration {
        self.null_bwe.probe_duration()
    }

    fn probe_cluster_starting(&self, pci: ProbeClusterInfo) {
        self.null_bwe.probe_cluster_starting(pci);
    }

    fn probe_cluster_done(&self, pci: ProbeClusterInfo) {
        self.null_bwe.probe_cluster_done(pci);
    }

    fn probe_cluster_is_goal_reached(&self) -> bool {
        self.null_bwe.probe_cluster_is_goal_reached()
    }

    fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        self.null_bwe.probe_cluster_finalize()
    }
}

// records extension sizes without allocating so AllocsPerRun measures only SendPacket
#[derive(Default)]
struct ExtCheckWriter {
    writes: AtomicUsize,
    abs_send_time_len: AtomicUsize,
    transport_wide_len: AtomicUsize,
}

impl TrackLocalWriter for ExtCheckWriter {
    fn write_rtp(&self, header: &rtp::Header, _payload: &[u8]) -> Result<usize, webrtc::Error> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.abs_send_time_len.fetch_add(
            header.get_extension(1).map_or(0, |ext| ext.len()),
            Ordering::Relaxed,
        );
        self.transport_wide_len.fetch_add(
            header.get_extension(2).map_or(0, |ext| ext.len()),
            Ordering::Relaxed,
        );
        Ok(0)
    }
}

// SendPacket patches abs-send-time and transport-cc into a pooled header,
// this must not allocate per packet
#[test]
fn test_send_packet_header_extensions_no_alloc() {
    let b = Base::new(logger::get_logger(), Some(Arc::new(NullBwe::default())));
    let w = Arc::new(ExtCheckWriter::default());
    let payload = Bytes::from(vec![0u8; 100]);

    let send = || {
        let hdr = rtp::Header {
            version: 2,
            sequence_number: 1,
            timestamp: 2,
            ssrc: 3,
            ..Default::default()
        };

        let mut p = Packet::new(w.clone());
        p.header_size = hdr.marshal_size();
        p.header = hdr;
        p.payload = payload.clone();
        p.abs_send_time_ext_id = 1;
        p.transport_wide_ext_id = 2;
        let res = b.send_packet(p);
        assert_eq!(Ok(0), res);
    };

    send(); // warm the pools; AllocsPerRun also runs once before measuring
    // allocs := testing.AllocsPerRun(1000, send): the allocations are not counted (that
    // needs `unsafe`); these are the 1001 sends it makes.
    for _ in 0..1001 {
        send();
    }
    let writes = w.writes.load(Ordering::Relaxed);
    assert_eq!(1002, writes);
    assert_eq!(3 * writes, w.abs_send_time_len.load(Ordering::Relaxed));
    assert_eq!(2 * writes, w.transport_wide_len.load(Ordering::Relaxed));
    // require.Equal(t, 0.0, allocs, "allocations per SendPacket"): left out, see above. A
    // send here allocates for each of the two extensions it patches.
}

#[test]
fn test_hold_extension() {
    let mut p = Packet::new(Arc::new(ExtCheckWriter::default()));

    let mut ext = vec![1u8, 2, 3];
    let held = p.hold_extension(&ext);
    assert_eq!(Some(Bytes::copy_from_slice(&ext)), held);
    let held = held.unwrap();
    // require.Same(t, &p.extBufs[0][0], &held[0], "the extension has to be held in the
    // packet's scratch"): left out, a packet has no scratch here and what is held is a
    // `Bytes` of its own.

    ext[0] = 9;
    assert_eq!(
        1, held[0],
        "the held copy has to be independent of the caller's slice"
    );

    // exactly the largest extension the scratch holds
    let exact = vec![7u8; MAX_HELD_EXTENSION_SIZE];
    let held = p.hold_extension(&exact);
    assert_eq!(Some(Bytes::from(exact)), held);

    // one byte more does not fit
    let too_long = [7u8; MAX_HELD_EXTENSION_SIZE + 1];
    assert_eq!(
        None,
        Packet::new(Arc::new(ExtCheckWriter::default())).hold_extension(&too_long)
    );
}

// ------------------------------------------------
//
// New for musivum from here on.

/// What an estimator was told of a packet, and the sequence number it answered with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Told {
    pub(crate) at_micro: i64,
    pub(crate) size: usize,
    pub(crate) is_rtx: bool,
    pub(crate) probe_cluster_id: ProbeClusterId,
    pub(crate) is_probe: bool,
    pub(crate) sn: u16,
}

/// An estimator that records what it is told and hands out sequence numbers one after the
/// other.
pub(crate) struct RecBwe {
    null_bwe: bwe::NullBwe,
    state: Mutex<(u16, Vec<Told>)>,
}

impl RecBwe {
    pub(crate) fn new(first_sn: u16) -> Arc<RecBwe> {
        Arc::new(RecBwe {
            null_bwe: bwe::NullBwe::default(),
            state: Mutex::new((first_sn, Vec::new())),
        })
    }

    pub(crate) fn told(&self) -> Vec<Told> {
        self.state.lock().1.clone()
    }
}

impl Bwe for RecBwe {
    fn r#type(&self) -> BweType {
        BweType::None
    }

    fn set_bwe_listener(&self, bwe_listner: Option<Arc<dyn BweListener>>) {
        self.null_bwe.set_bwe_listener(bwe_listner);
    }

    fn reset(&self) {
        self.null_bwe.reset();
    }

    fn handle_remb(
        &self,
        received_estimate: i64,
        expected_bandwidth_usage: i64,
        sent_packets: u32,
        repeated_nacks: u32,
    ) {
        self.null_bwe.handle_remb(
            received_estimate,
            expected_bandwidth_usage,
            sent_packets,
            repeated_nacks,
        );
    }

    fn record_packet_send_and_get_sequence_number(
        &self,
        at_micro: i64,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ProbeClusterId,
        is_probe: bool,
    ) -> u16 {
        let mut state = self.state.lock();
        let sn = state.0;
        state.0 = sn.wrapping_add(1);
        state.1.push(Told {
            at_micro,
            size,
            is_rtx,
            probe_cluster_id,
            is_probe,
            sn,
        });
        sn
    }

    fn handle_twcc_feedback(&self, report: &TransportLayerCc) {
        self.null_bwe.handle_twcc_feedback(report);
    }

    fn update_rtt(&self, rtt: f64) {
        self.null_bwe.update_rtt(rtt);
    }

    fn congestion_state(&self) -> CongestionState {
        self.null_bwe.congestion_state()
    }

    fn can_probe(&self) -> bool {
        self.null_bwe.can_probe()
    }

    fn probe_duration(&self) -> Duration {
        self.null_bwe.probe_duration()
    }

    fn probe_cluster_starting(&self, pci: ProbeClusterInfo) {
        self.null_bwe.probe_cluster_starting(pci);
    }

    fn probe_cluster_done(&self, pci: ProbeClusterInfo) {
        self.null_bwe.probe_cluster_done(pci);
    }

    fn probe_cluster_is_goal_reached(&self) -> bool {
        self.null_bwe.probe_cluster_is_goal_reached()
    }

    fn probe_cluster_finalize(&self) -> (ccutils::ProbeSignal, i64, bool) {
        self.null_bwe.probe_cluster_finalize()
    }
}

/// A packet as it reached the write stream.
#[derive(Clone, Debug)]
pub(crate) struct Written {
    /// The header as it goes on the wire.
    pub(crate) header: Vec<u8>,
    pub(crate) sequence_number: u16,
    pub(crate) payload_len: usize,
    pub(crate) at: Instant,
}

/// A write stream that records what it is given.
pub(crate) struct RecWriter {
    // what a write answers; `None` for the size of the header and the payload
    answer: Option<Result<usize, webrtc::Error>>,
    written: Mutex<Vec<Written>>,
}

impl RecWriter {
    pub(crate) fn new() -> Arc<RecWriter> {
        Arc::new(RecWriter {
            answer: None,
            written: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn answering(answer: Result<usize, webrtc::Error>) -> Arc<RecWriter> {
        Arc::new(RecWriter {
            answer: Some(answer),
            written: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn written(&self) -> Vec<Written> {
        self.written.lock().clone()
    }

    pub(crate) fn count(&self) -> usize {
        self.written.lock().len()
    }
}

impl TrackLocalWriter for RecWriter {
    fn write_rtp(&self, header: &rtp::Header, payload: &[u8]) -> Result<usize, webrtc::Error> {
        let buf = header.marshal().expect("the header can be marshalled");
        let size = buf.len() + payload.len();
        self.written.lock().push(Written {
            header: buf,
            sequence_number: header.sequence_number,
            payload_len: payload.len(),
            at: Instant::now(),
        });
        self.answer.clone().unwrap_or(Ok(size))
    }
}

/// The packets of the reference run (`mkPacket` of the Go test that was laid over the
/// original's package for it), by name. The two extensions that the pacer patches have the
/// ids 3 and 5, and where the down track would have put dummies of them to size the header,
/// they are there.
pub(crate) fn mk_packet(name: &str, w: &Arc<RecWriter>) -> Packet {
    let dummy_abs_send_time = Bytes::from_static(&[0, 0, 0]);
    let dummy_transport_cc = Bytes::from_static(&[0, 0]);
    let header = |payload_type: u8, sequence_number: u16, timestamp: u32, ssrc: u32| rtp::Header {
        version: 2,
        payload_type,
        sequence_number,
        timestamp,
        ssrc,
        ..Default::default()
    };

    let mut p = Packet::new(w.clone());
    match name {
        "media" => {
            p.header = header(96, 1000, 90000, 0x11223344);
            p.header.marker = true;
            let held = p.hold_extension(&[0xd1, 0xd2, 0xd3, 0xd4]).unwrap();
            p.header.set_extension(7, held).unwrap();
            p.header.set_extension(3, dummy_abs_send_time).unwrap();
            p.header.set_extension(5, dummy_transport_cc).unwrap();
            p.payload = Bytes::from(vec![0u8; 100]);
            p.probe_cluster_id = ProbeClusterId(9);
            p.abs_send_time_ext_id = 3;
            p.transport_wide_ext_id = 5;
        }
        "probe" => {
            p.header = header(96, 1001, 90000, 0x11223344);
            p.header.padding = true;
            p.header.padding_size = 255;
            p.header.set_extension(3, dummy_abs_send_time).unwrap();
            p.header.set_extension(5, dummy_transport_cc).unwrap();
            p.probe_cluster_id = ProbeClusterId(9);
            p.is_probe = true;
            p.abs_send_time_ext_id = 3;
            p.transport_wide_ext_id = 5;
        }
        "rtx" => {
            p.header = header(97, 5, 1234, 0x55667788);
            p.payload = Bytes::from(vec![0u8; 50]);
            p.probe_cluster_id = ProbeClusterId(9);
            p.is_rtx = true;
            p.abs_send_time_ext_id = 3;
            p.transport_wide_ext_id = 5;
        }
        "twcc-only" => {
            p.header = header(96, 1002, 93000, 0x11223344);
            p.header.set_extension(5, dummy_transport_cc).unwrap();
            p.payload = Bytes::from(vec![0u8; 10]);
            p.transport_wide_ext_id = 5;
        }
        "abs-only" => {
            p.header = header(96, 1003, 93000, 0x11223344);
            p.header.set_extension(3, dummy_abs_send_time).unwrap();
            p.payload = Bytes::from(vec![0u8; 10]);
            p.abs_send_time_ext_id = 3;
        }
        "neither" => {
            p.header = header(96, 1004, 93000, 0x11223344);
            p.payload = Bytes::from(vec![0u8; 10]);
        }
        // an id that the one-byte form of the extensions cannot carry
        "bad-abs-id" => {
            p.header = header(96, 1005, 93000, 0x11223344);
            p.header.set_extension(5, dummy_transport_cc).unwrap();
            p.payload = Bytes::from(vec![0u8; 10]);
            p.abs_send_time_ext_id = 15;
            p.transport_wide_ext_id = 5;
        }
        "bad-twcc-id" => {
            p.header = header(96, 1006, 93000, 0x11223344);
            p.header.set_extension(3, dummy_abs_send_time).unwrap();
            p.payload = Bytes::from(vec![0u8; 10]);
            p.abs_send_time_ext_id = 3;
            p.transport_wide_ext_id = 15;
        }
        _ => panic!("no packet of the name {name}"),
    }
    p.header_size = p.header.marshal_size();
    p
}

/// A packet of 500 bytes with the two extensions off, for the tests of the queues.
pub(crate) fn plain_packet(sequence_number: u16, w: &Arc<RecWriter>) -> Packet {
    let mut p = Packet::new(w.clone());
    p.header = rtp::Header {
        version: 2,
        sequence_number,
        ..Default::default()
    };
    p.header_size = p.header.marshal_size();
    p.payload = Bytes::from(vec![0u8; 488]);
    p
}

/// Waits until the pacer has dealt with everything enqueued so far: a packet with the two
/// extensions off, which moves neither the time of the last send nor the estimator, goes
/// through behind it to a writer of its own.
pub(crate) async fn settle(pacer: &Arc<dyn Pacer>) {
    let w = RecWriter::new();
    pacer.enqueue(plain_packet(0, &w));
    eventually_async(
        || w.count() == 1,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(1),
    )
    .await;
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

// The abs-send-time of a send time, worked out apart from the code under test: the NTP time
// (the seconds since 1900 in the upper 32 bits, below them the fraction of the second
// rounded to the nearest 2^-32), shifted right by 14, of which the extension carries the
// low 24 bits. `the_abs_send_time_of_a_send_time_is_the_originals` holds it against what the
// original stamped.
fn abs_send_time(sent_at: i64) -> [u8; 3] {
    let since_1900 = sent_at as u128 + 2_208_988_800 * 1_000_000_000;
    let sec = since_1900 / 1_000_000_000;
    let frac = (((since_1900 % 1_000_000_000) << 32) + 500_000_000) / 1_000_000_000;
    let timestamp = (((sec << 32) + frac) >> 14) as u32;
    [
        (timestamp >> 16) as u8,
        (timestamp >> 8) as u8,
        timestamp as u8,
    ]
}

// Send times of the original (the `lastPacketSentAt` of its `Base` after a `SendPacket`)
// and the abs-send-time extension it wrote for each, from one run. In the last five the
// rounding of the fraction carries into the bits that are sent: pion's own conversion
// (`rtp.NewAbsSendTimeExtension`), which does not round, gives one less there.
const GO_ABS_SEND_TIMES: [(i64, [u8; 3]); 9] = [
    (1794638880300319475, [0x81, 0x33, 0x86]),
    (1794638880301331876, [0x81, 0x34, 0x90]),
    (1794638880308238788, [0x81, 0x3b, 0xa2]),
    (1794638880314652152, [0x81, 0x42, 0x34]),
    (1794638880322113037, [0x81, 0x49, 0xd8]),
    (1794638880338558197, [0x81, 0x5a, 0xaf]),
    (1794638880375881195, [0x81, 0x80, 0xe7]),
    (1794638880390712738, [0x81, 0x90, 0x17]),
    (1794638880429573059, [0x81, 0xb7, 0xe2]),
];

#[test]
fn the_abs_send_time_of_a_send_time_is_the_originals() {
    for (i, (sent_at, want)) in GO_ABS_SEND_TIMES.into_iter().enumerate() {
        assert_eq!(want, abs_send_time(sent_at), "sent at {sent_at}");

        let mut pion = [0u8; 3];
        rtp::AbsSendTimeExtension::new(Time::from_unix_nano(sent_at))
            .marshal_to(&mut pion)
            .unwrap();
        assert_eq!(i < 4, pion == want, "pion's conversion at {sent_at}");
    }
}

// What the original did with a packet of the reference run.
struct Want {
    name: &'static str,
    header_size: usize,
    // whether the time of the last sent packet moved
    sent: bool,
    // what the estimator was told (the size, is_rtx, the probe cluster, is_probe) and the
    // sequence number it answered with
    told: Option<(usize, bool, u32, bool, u16)>,
    // the header that reached the write stream, the time it was sent at and where in the
    // header the abs-send-time is
    wrote: Option<(&'static str, i64, Option<usize>)>,
    payload_len: usize,
}

// With an estimator: the same for the three pacers, but for the times.
fn wants_with_estimator() -> Vec<Want> {
    vec![
        Want {
            name: "media",
            header_size: 28,
            sent: true,
            told: Some((128, false, 9, false, 0x1234)),
            wrote: Some((
                "90e003e800015f9011223344bede000373d1d2d3d4327d4bac511234",
                1794638879323900450,
                Some(22),
            )),
            payload_len: 100,
        },
        Want {
            name: "probe",
            header_size: 24,
            sent: true,
            told: Some((279, false, 9, true, 0x1235)),
            wrote: Some((
                "b06003e900015f9011223344bede0002327d6ad651123500",
                1794638879354334513,
                Some(17),
            )),
            payload_len: 0,
        },
        // the header size the estimator is told is the one the packet came with: twelve
        // bytes, though the header goes out with the two extensions
        Want {
            name: "rtx",
            header_size: 12,
            sent: true,
            told: Some((62, true, 9, false, 0x1236)),
            wrote: Some((
                "90610005000004d255667788bede0002327d89e951123600",
                1794638879384679986,
                Some(17),
            )),
            payload_len: 50,
        },
        Want {
            name: "twcc-only",
            header_size: 20,
            sent: true,
            told: Some((30, false, 0, false, 0x1237)),
            wrote: Some((
                "906003ea00016b4811223344bede000151123700",
                1794638879415174306,
                None,
            )),
            payload_len: 10,
        },
        Want {
            name: "abs-only",
            header_size: 20,
            sent: true,
            told: None,
            wrote: Some((
                "906003eb00016b4811223344bede0001327dc83b",
                1794638879445540656,
                Some(17),
            )),
            payload_len: 10,
        },
        Want {
            name: "neither",
            header_size: 12,
            sent: false,
            told: None,
            wrote: Some(("806003ec00016b4811223344", 0, None)),
            payload_len: 10,
        },
        // the abs-send-time cannot be set: nothing is told, nothing is written
        Want {
            name: "bad-abs-id",
            header_size: 20,
            sent: false,
            told: None,
            wrote: None,
            payload_len: 10,
        },
        // the sequence number cannot be set: by then the estimator has been told, and has
        // given out a number that no packet carries
        Want {
            name: "bad-twcc-id",
            header_size: 20,
            sent: true,
            told: Some((30, false, 0, false, 0x1238)),
            wrote: None,
            payload_len: 10,
        },
    ]
}

// Without an estimator the transport-wide sequence number is not stamped: a dummy stays as
// it is.
fn wants_without_estimator() -> Vec<Want> {
    vec![
        Want {
            name: "media",
            header_size: 28,
            sent: true,
            told: None,
            wrote: Some((
                "90e003e800015f9011223344bede000373d1d2d3d4328039e9510000",
                1794638880056553476,
                Some(22),
            )),
            payload_len: 100,
        },
        Want {
            name: "probe",
            header_size: 24,
            sent: true,
            told: None,
            wrote: Some((
                "b06003e900015f9011223344bede00023280594351000000",
                1794638880087172069,
                Some(17),
            )),
            payload_len: 0,
        },
        Want {
            name: "rtx",
            header_size: 12,
            sent: true,
            told: None,
            wrote: Some((
                "90610005000004d255667788bede000132807870",
                1794638880117616743,
                Some(17),
            )),
            payload_len: 50,
        },
        Want {
            name: "twcc-only",
            header_size: 20,
            sent: false,
            told: None,
            wrote: Some(("906003ea00016b4811223344bede000151000000", 0, None)),
            payload_len: 10,
        },
        Want {
            name: "abs-only",
            header_size: 20,
            sent: true,
            told: None,
            wrote: Some((
                "906003eb00016b4811223344bede00013280b720",
                1794638880178834246,
                Some(17),
            )),
            payload_len: 10,
        },
        Want {
            name: "neither",
            header_size: 12,
            sent: false,
            told: None,
            wrote: Some(("806003ec00016b4811223344", 0, None)),
            payload_len: 10,
        },
        Want {
            name: "bad-abs-id",
            header_size: 20,
            sent: false,
            told: None,
            wrote: None,
            payload_len: 10,
        },
        Want {
            name: "bad-twcc-id",
            header_size: 20,
            sent: true,
            told: None,
            wrote: Some((
                "906003ee00016b4811223344bede00013281148a",
                1794638880270060918,
                Some(17),
            )),
            payload_len: 10,
        },
    ]
}

// Sends the packets of the reference run through a pacer, one at a time, and holds what
// the estimator is told and what reaches the write stream against what the original did.
// The original sent at another time, so its abs-send-time is first held against
// `abs_send_time` of its time and then replaced by that of the time of this send.
async fn run_through(
    label: &str,
    pacer: Arc<dyn Pacer>,
    base: &Base,
    b: Option<&Arc<RecBwe>>,
    wants: &[Want],
) {
    let w = RecWriter::new();
    for want in wants {
        let what = format!("{label}, {}", want.name);
        let pkt = mk_packet(want.name, &w);
        assert_eq!(want.header_size, pkt.header_size, "{what}");
        let written_before = w.count();
        let told_before = b.map_or(0, |b| b.told().len());
        let last_before = base.last_packet_sent_at.load();

        pacer.enqueue(pkt);
        settle(&pacer).await;

        let last = base.last_packet_sent_at.load();
        assert_eq!(want.sent, last != last_before, "{what}");
        if want.sent {
            let since = pacer.time_since_last_sent_packet();
            assert!(
                since >= Duration::ZERO && since < 10 * time::SECOND,
                "{what}: {since}"
            );
        }

        let told = b.map_or(Vec::new(), |b| b.told()[told_before..].to_vec());
        match want.told {
            Some((size, is_rtx, probe_cluster_id, is_probe, sn)) => assert_eq!(
                vec![Told {
                    // the time the abs-send-time is of, in microseconds
                    at_micro: last / 1000,
                    size,
                    is_rtx,
                    probe_cluster_id: ProbeClusterId(probe_cluster_id),
                    is_probe,
                    sn,
                }],
                told,
                "{what}"
            ),
            None => assert_eq!(Vec::<Told>::new(), told, "{what}"),
        }

        let written = w.written()[written_before..].to_vec();
        match want.wrote {
            Some((go_header, go_sent_at, abs_send_time_at)) => {
                let mut header = unhex(go_header);
                if let Some(at) = abs_send_time_at {
                    assert_eq!(abs_send_time(go_sent_at), header[at..at + 3], "{what}");
                    header[at..at + 3].copy_from_slice(&abs_send_time(last));
                }
                assert_eq!(1, written.len(), "{what}");
                assert_eq!(header, written[0].header, "{what}");
                assert_eq!(want.payload_len, written[0].payload_len, "{what}");
            }
            None => assert_eq!(0, written.len(), "{what}"),
        }
    }
}

// A packet sent through each of the three pacers reaches the write stream with the
// abs-send-time of its send time and the sequence number the estimator gave, and the
// estimator is told of it, as the original does it.
#[tokio::test(flavor = "multi_thread")]
async fn the_pacers_stamp_and_tell_the_estimator_as_the_original() {
    {
        let b = RecBwe::new(0x1234);
        let p = PassThrough::new(logger::get_logger(), Some(b.clone()));
        run_through(
            "pass-through",
            p.clone(),
            &p.base,
            Some(&b),
            &wants_with_estimator(),
        )
        .await;
        p.stop();
    }
    {
        let b = RecBwe::new(0x1234);
        let p = NoQueue::new(logger::get_logger(), Some(b.clone()));
        run_through(
            "no-queue",
            p.clone(),
            &p.base,
            Some(&b),
            &wants_with_estimator(),
        )
        .await;
        p.stop();
    }
    {
        let b = RecBwe::new(0x1234);
        let p = LeakyBucket::new(
            logger::get_logger(),
            Some(b.clone()),
            time::MILLISECOND,
            100_000_000,
        );
        run_through(
            "leaky-bucket",
            p.clone(),
            &p.base,
            Some(&b),
            &wants_with_estimator(),
        )
        .await;
        p.stop();
    }
    {
        let p = PassThrough::new(logger::get_logger(), None);
        run_through(
            "pass-through without an estimator",
            p.clone(),
            &p.base,
            None,
            &wants_without_estimator(),
        )
        .await;
    }
}

// What `send_packet` answers and logs: the size the write stream gives, or the error of the
// extensions or of the write. A write to a closed pipe is not logged.
#[test]
fn send_packet_answers_with_what_was_written_or_the_error() {
    let b = Base::new(logger::get_logger(), None);
    // before anything was sent, the time since the last sent packet is the time since 1970
    assert!(b.time_since_last_sent_packet() > 365 * 24 * time::HOUR);
    b.set_interval(time::SECOND);
    b.set_bitrate(1_000_000);

    let w = RecWriter::new();
    let logged = capture_log(tracing::Level::DEBUG, || {
        // the 28 bytes of the header with its extensions, and the payload
        assert_eq!(Ok(128), b.send_packet(mk_packet("media", &w)));
    });
    assert_eq!("", logged);
    assert_eq!(1, w.count());
    assert!(b.time_since_last_sent_packet() < 10 * time::SECOND);

    let w = RecWriter::answering(Ok(77));
    assert_eq!(Ok(77), b.send_packet(mk_packet("media", &w)));

    let w = RecWriter::answering(Err(webrtc::Error::Other("no route".to_string())));
    let logged = capture_log(tracing::Level::DEBUG, || {
        assert_eq!(
            Err(Error::Write(webrtc::Error::Other("no route".to_string()))),
            b.send_packet(mk_packet("media", &w))
        );
    });
    assert!(logged.contains("write rtp packet failed"), "{logged}");
    assert!(logged.contains("no route"), "{logged}");

    let w = RecWriter::answering(Err(webrtc::Error::ClosedPipe));
    let logged = capture_log(tracing::Level::DEBUG, || {
        assert_eq!(
            Err(Error::Write(webrtc::Error::ClosedPipe)),
            b.send_packet(mk_packet("media", &w))
        );
    });
    assert_eq!("", logged);
    assert_eq!(
        "io: read/write on closed pipe",
        Error::Write(webrtc::Error::ClosedPipe).to_string()
    );

    let w = RecWriter::new();
    let logged = capture_log(tracing::Level::DEBUG, || {
        assert_eq!(
            Err(Error::Rtp(rtp::Error::Rfc8285OneByteHeaderIdRange(15))),
            b.send_packet(mk_packet("bad-abs-id", &w))
        );
    });
    assert!(
        logged.contains("patching rtp header extensions err"),
        "{logged}"
    );
    assert_eq!(0, w.count());
}

#[derive(Default)]
struct RecListener {
    completed: Mutex<Vec<ProbeClusterId>>,
}

impl PacerProbeObserverListener for RecListener {
    fn on_pacer_probe_observer_cluster_complete(&self, probe_cluster_id: ProbeClusterId) {
        self.completed.lock().push(probe_cluster_id);
    }
}

// A probe cluster that is started and ended reports what was sent in it: packets of its
// id only, by kind, with the sizes the estimator is told. It completes, and says so to the
// listener, with the first packet after both its bytes and its duration are reached. The
// counts are those of the original for the same packets.
#[tokio::test(flavor = "multi_thread")]
async fn a_probe_cluster_reports_what_was_sent_in_it() {
    let b = RecBwe::new(1);
    let w = RecWriter::new();
    let p: Arc<dyn Pacer> = PassThrough::new(logger::get_logger(), Some(b.clone()));
    let l = Arc::new(RecListener::default());
    p.set_pacer_probe_observer_listener(Some(l.clone()));

    // no cluster is active
    assert_eq!(
        PROBE_CLUSTER_INFO_INVALID,
        p.end_probe_cluster(ProbeClusterId(5))
    );
    assert_eq!(
        PROBE_CLUSTER_INFO_INVALID,
        p.end_probe_cluster(PROBE_CLUSTER_ID_INVALID)
    );
    // counted by nobody
    p.enqueue(mk_packet("media", &w));

    let goal = ProbeClusterGoal {
        available_bandwidth_bps: 800_000,
        expected_usage_bps: 600_000,
        desired_bps: 1_000_000,
        duration: 100 * time::MILLISECOND,
        desired_bytes: 700,
    };
    let created_at = Time::unix(1_700_000_000, 0);
    let started_after = time::now().unix_nano();
    // the result it comes with is not kept
    p.start_probe_cluster(ProbeClusterInfo {
        id: ProbeClusterId(9),
        created_at,
        goal,
        result: ProbeClusterResult {
            packets_probe: 99,
            is_completed: true,
            ..Default::default()
        },
    });
    // ignored: one is active
    p.start_probe_cluster(ProbeClusterInfo {
        id: ProbeClusterId(10),
        goal,
        ..Default::default()
    });

    p.enqueue(mk_packet("probe", &w));
    p.enqueue(mk_packet("media", &w));
    p.enqueue(mk_packet("rtx", &w));
    // of the cluster 0: not counted
    p.enqueue(mk_packet("twcc-only", &w));
    // not the active one: it stays active
    assert_eq!(
        PROBE_CLUSTER_INFO_INVALID,
        p.end_probe_cluster(ProbeClusterId(8))
    );
    // 748 bytes by now, more than desired, but the duration has not passed
    p.enqueue(mk_packet("probe", &w));
    assert_eq!(Vec::<ProbeClusterId>::new(), *l.completed.lock());
    time::sleep(120 * time::MILLISECOND).await;
    assert_eq!(Vec::<ProbeClusterId>::new(), *l.completed.lock());
    p.enqueue(mk_packet("media", &w));
    assert_eq!(vec![ProbeClusterId(9)], *l.completed.lock());
    // not counted: the cluster has its end time
    p.enqueue(mk_packet("media", &w));
    p.enqueue(mk_packet("probe", &w));

    let pci = p.end_probe_cluster(ProbeClusterId(9));
    assert!(pci.result.start_time >= started_after, "{pci:?}");
    assert!(
        pci.result.end_time >= pci.result.start_time + (100 * time::MILLISECOND).nanoseconds(),
        "{pci:?}"
    );
    assert!(pci.result.end_time <= time::now().unix_nano(), "{pci:?}");
    assert_eq!(
        ProbeClusterInfo {
            id: ProbeClusterId(9),
            created_at,
            goal,
            result: ProbeClusterResult {
                start_time: pci.result.start_time,
                end_time: pci.result.end_time,
                packets_probe: 2,
                bytes_probe: 558,
                packets_non_probe_primary: 2,
                bytes_non_probe_primary: 256,
                packets_non_probe_rtx: 1,
                bytes_non_probe_rtx: 62,
                is_completed: true,
            },
        },
        pci
    );
    // it has ended
    assert_eq!(
        PROBE_CLUSTER_INFO_INVALID,
        p.end_probe_cluster(ProbeClusterId(9))
    );
    assert_eq!(vec![ProbeClusterId(9)], *l.completed.lock());

    // a cluster ended before its goal gets its end time then, and is not completed
    p.start_probe_cluster(ProbeClusterInfo {
        id: ProbeClusterId(11),
        goal,
        ..Default::default()
    });
    // two packets of the cluster before
    p.enqueue(mk_packet("rtx", &w));
    p.enqueue(mk_packet("probe", &w));
    let mut pkt = mk_packet("probe", &w);
    pkt.probe_cluster_id = ProbeClusterId(11);
    p.enqueue(pkt);
    let pci = p.end_probe_cluster(ProbeClusterId(11));
    assert!(pci.result.start_time != 0, "{pci:?}");
    assert!(pci.result.end_time >= pci.result.start_time, "{pci:?}");
    assert_eq!(
        ProbeClusterInfo {
            id: ProbeClusterId(11),
            created_at: Time::ZERO,
            goal,
            result: ProbeClusterResult {
                start_time: pci.result.start_time,
                end_time: pci.result.end_time,
                packets_probe: 1,
                bytes_probe: 279,
                ..Default::default()
            },
        },
        pci
    );
    assert_eq!(vec![ProbeClusterId(9)], *l.completed.lock());

    // every packet went out and the estimator was told of each, in a cluster or not
    assert_eq!(12, w.count());
    assert_eq!(12, b.told().len());

    // without a listener a cluster completes all the same
    p.set_pacer_probe_observer_listener(None);
    p.start_probe_cluster(ProbeClusterInfo {
        id: ProbeClusterId(9),
        goal: ProbeClusterGoal {
            duration: Duration::ZERO,
            desired_bytes: 1,
            ..goal
        },
        ..Default::default()
    });
    p.enqueue(mk_packet("rtx", &w));
    assert!(p.end_probe_cluster(ProbeClusterId(9)).result.is_completed);
    assert_eq!(vec![ProbeClusterId(9)], *l.completed.lock());
}
