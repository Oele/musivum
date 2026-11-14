// Ported from LiveKit, pkg/rtc/datatrack_stats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the fields `lock` guards are in the lock; the
// counters, `int` upstream, are `i64`, and the length of a payload is a `usize`. The tests
// are new: the original has none.

use parking_lot::Mutex;

use crate::go::time::Duration;
use crate::infow;
use crate::protocol::datatrack;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;

pub(super) struct DataTrackStatsParams {
    pub logger: Logger,
}

pub(super) struct DataTrackStats {
    params: DataTrackStatsParams,

    // what `lock sync.Mutex` guards upstream
    pub(super) inner: Mutex<DataTrackStatsInner>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct DataTrackStatsInner {
    pub(super) start_time: i64,
    pub(super) end_time: i64,
    pub(super) highest_sequence_number: u16,
    pub(super) num_packets: i64,
    pub(super) num_packets_lost: i64,
    pub(super) num_packets_out_of_order: i64,
    pub(super) num_frames: i64, // count of `F` tagged packets, i. e. packets with final packet of frame marker
    pub(super) num_bytes: i64,
}

impl DataTrackStats {
    pub(super) fn new(params: DataTrackStatsParams) -> Self {
        Self {
            params,
            inner: Mutex::new(DataTrackStatsInner::default()),
        }
    }

    pub(super) fn update(
        &self,
        packet: &datatrack::Packet,
        arrival_time: i64,
        payload_length: usize,
    ) {
        let mut d = self.inner.lock();

        d.num_bytes += payload_length as i64;

        if d.end_time != 0 {
            return;
        }

        if d.start_time == 0 {
            d.start_time = arrival_time;
            d.highest_sequence_number = packet.sequence_number;
            d.num_packets = 1;
        } else {
            let diff = packet
                .sequence_number
                .wrapping_sub(d.highest_sequence_number);
            if diff == 0 {
                // duplicate
                return;
            } else if diff > (1 << 15) {
                // out of order
                d.num_packets += 1;
                d.num_packets_out_of_order += 1;
                if d.num_packets_lost > 0 {
                    d.num_packets_lost -= 1;
                }
            } else {
                // in order
                d.num_packets += 1;
                d.num_packets_lost += diff as i64 - 1;
                d.highest_sequence_number = packet.sequence_number;
            }
        }

        if packet.is_final_of_frame {
            d.num_frames += 1;
        }
    }

    pub(super) fn close(&self) {
        let mut d = self.inner.lock();

        d.end_time = mono::unix_nano();

        if d.start_time != 0 {
            let duration = Duration::from_nanos(d.end_time.wrapping_sub(d.start_time)).seconds();
            let fps = d.num_frames as f64 / duration;

            infow!(
                self.params.logger,
                "data track stats",
                "duration" => duration,
                "numPackets" => d.num_packets,
                "numPacketsLost" => d.num_packets_lost,
                "numPacketsOutOfOrder" => d.num_packets_out_of_order,
                "numFrames" => d.num_frames,
                "fps" => fps,
                "numBytes" => d.num_bytes,
            );
        }
    }
}

#[cfg(test)]
#[path = "datatrack_stats_test.rs"]
mod datatrack_stats_test;
