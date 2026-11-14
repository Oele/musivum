// New for musivum: the original has no test of these statistics. What is expected is the
// original's: it is what a Go test laid over `pkg/rtc` at v1.13.11 printed for the same
// calls.

use super::*;
use crate::protocol::logger;
use crate::testutils;

fn new_stats() -> DataTrackStats {
    DataTrackStats::new(DataTrackStatsParams {
        logger: logger::get_logger(),
    })
}

fn packet(sequence_number: u16, is_final_of_frame: bool) -> datatrack::Packet {
    datatrack::Packet {
        header: datatrack::Header {
            sequence_number,
            is_final_of_frame,
            ..Default::default()
        },
        ..Default::default()
    }
}

// The start time, whether the end time is set, the highest sequence number, and the numbers
// of packets, packets lost, packets out of order, frames and bytes.
type Seen = (i64, bool, u16, i64, i64, i64, i64, i64);

fn seen(s: &DataTrackStats) -> Seen {
    let d = s.inner.lock();
    (
        d.start_time,
        d.end_time != 0,
        d.highest_sequence_number,
        d.num_packets,
        d.num_packets_lost,
        d.num_packets_out_of_order,
        d.num_frames,
        d.num_bytes,
    )
}

#[test]
fn the_statistics_count_packets_frames_and_bytes_as_the_original() {
    let s = new_stats();
    // the sequence number, whether the packet ends a frame, the length of what arrived, and
    // what the statistics are after it
    let steps: [(u16, bool, usize, Seen); 11] = [
        // the first packet
        (65534, false, 10, (1000, false, 65534, 1, 0, 0, 0, 10)),
        // in order, also across the wrap of the sequence number
        (65535, true, 20, (1000, false, 65535, 2, 0, 0, 1, 30)),
        (0, false, 30, (1000, false, 0, 3, 0, 0, 1, 60)),
        // a duplicate counts with its bytes only, not as a frame either
        (0, true, 5, (1000, false, 0, 3, 0, 0, 1, 65)),
        // a gap of two
        (3, true, 7, (1000, false, 3, 4, 2, 0, 2, 72)),
        // which arrive late, out of order
        (1, true, 8, (1000, false, 3, 5, 1, 1, 3, 80)),
        (2, false, 9, (1000, false, 3, 6, 0, 2, 3, 89)),
        // an older one that was not counted as lost takes nothing off
        (65530, true, 1, (1000, false, 3, 7, 0, 3, 4, 90)),
        // half the range ahead is still in order
        (32771, false, 2, (1000, false, 32771, 8, 32767, 3, 4, 92)),
        (32771, true, 3, (1000, false, 32771, 8, 32767, 3, 4, 95)),
        (32772, true, 4, (1000, false, 32772, 9, 32767, 3, 5, 99)),
    ];
    for (i, (sequence_number, is_final_of_frame, size, expected)) in steps.into_iter().enumerate() {
        s.update(
            &packet(sequence_number, is_final_of_frame),
            1000 + i as i64,
            size,
        );
        assert_eq!(expected, seen(&s), "step {i}");
    }

    // closed, they say what they counted, and count bytes only
    let log = testutils::capture_log(tracing::Level::INFO, || s.close());
    assert!(log.contains("data track stats"), "{log}");
    for counted in [
        "numPackets=9",
        "numPacketsLost=32767",
        "numPacketsOutOfOrder=3",
        "numFrames=5",
        "numBytes=99",
    ] {
        assert!(log.contains(counted), "{counted} is not in: {log}");
    }
    s.update(&packet(32773, true), 2000, 100);
    assert_eq!((1000, true, 32772, 9, 32767, 3, 5, 199), seen(&s));

    // an arrival time of zero does not start them: the next packet is the first again
    let z = new_stats();
    z.update(&packet(10, true), 0, 1);
    assert_eq!((0, false, 10, 1, 0, 0, 1, 1), seen(&z));
    z.update(&packet(20, true), 5, 1);
    assert_eq!((5, false, 20, 1, 0, 0, 2, 2), seen(&z));
    z.update(&packet(22, false), 0, 1);
    assert_eq!((5, false, 22, 2, 1, 0, 2, 3), seen(&z));

    // closed before anything arrived, they have nothing to say
    let e = new_stats();
    let log = testutils::capture_log(tracing::Level::INFO, || e.close());
    assert!(!log.contains("data track stats"), "{log}");
    e.update(&packet(10, true), 7, 11);
    assert_eq!((0, true, 0, 0, 0, 0, 0, 11), seen(&e));
}
