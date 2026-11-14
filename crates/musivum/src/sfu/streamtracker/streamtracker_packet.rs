// Ported from LiveKit, pkg/sfu/streamtracker/streamtracker_packet.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewStreamTrackerPacket` is `StreamTrackerPacket::new`
// and gives the type itself (boxed by whoever wants a `dyn StreamTrackerImpl`); the two maps
// of defaults are built on first use; the YAML tags of `StreamTrackerPacketConfig` are not
// carried over (the fields have the names of the keys).

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::go::time::{self, Duration};
use crate::protocol::logger::Logger;

use super::{StreamStatusChange, StreamTrackerImpl};

// --------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamTrackerPacketConfig {
    pub samples_required: u32, // number of samples needed per cycle
    pub cycles_required: u32,  // number of cycles needed to be active
    pub cycle_duration: Duration,
}

pub static DEFAULT_STREAM_TRACKER_PACKET_CONFIG_VIDEO: LazyLock<
    HashMap<i32, StreamTrackerPacketConfig>,
> = LazyLock::new(|| {
    HashMap::from([
        (
            0,
            StreamTrackerPacketConfig {
                samples_required: 1,
                cycles_required: 4,
                cycle_duration: 500 * time::MILLISECOND,
            },
        ),
        (
            1,
            StreamTrackerPacketConfig {
                samples_required: 5,
                cycles_required: 20,
                cycle_duration: 500 * time::MILLISECOND,
            },
        ),
        (
            2,
            StreamTrackerPacketConfig {
                samples_required: 5,
                cycles_required: 20,
                cycle_duration: 500 * time::MILLISECOND,
            },
        ),
    ])
});

pub static DEFAULT_STREAM_TRACKER_PACKET_CONFIG_SCREENSHARE: LazyLock<
    HashMap<i32, StreamTrackerPacketConfig>,
> = LazyLock::new(|| {
    HashMap::from([
        (
            0,
            StreamTrackerPacketConfig {
                samples_required: 1,
                cycles_required: 1,
                cycle_duration: 2 * time::SECOND,
            },
        ),
        (
            1,
            StreamTrackerPacketConfig {
                samples_required: 1,
                cycles_required: 1,
                cycle_duration: 2 * time::SECOND,
            },
        ),
        (
            2,
            StreamTrackerPacketConfig {
                samples_required: 1,
                cycles_required: 1,
                cycle_duration: 2 * time::SECOND,
            },
        ),
    ])
});

// --------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct StreamTrackerPacketParams {
    pub config: StreamTrackerPacketConfig,
    pub logger: Logger,
}

pub struct StreamTrackerPacket {
    params: StreamTrackerPacketParams,

    count_since_last: u32, // number of packets received since last check

    initialized: bool,

    cycle_count: u32,
}

impl StreamTrackerPacket {
    pub fn new(params: StreamTrackerPacketParams) -> Self {
        StreamTrackerPacket {
            params,
            count_since_last: 0,
            initialized: false,
            cycle_count: 0,
        }
    }
}

impl StreamTrackerImpl for StreamTrackerPacket {
    fn start(&mut self) {}

    fn stop(&mut self) {}

    fn reset(&mut self) {
        self.count_since_last = 0;
        self.cycle_count = 0;

        self.initialized = false;
    }

    fn get_check_interval(&self) -> Duration {
        self.params.config.cycle_duration
    }

    fn observe(&mut self, _has_marker: bool, _ts: u32) -> StreamStatusChange {
        if !self.initialized {
            // first packet
            self.initialized = true;
            self.count_since_last = 1;
            return StreamStatusChange::Active;
        }

        self.count_since_last = self.count_since_last.wrapping_add(1);
        StreamStatusChange::None
    }

    fn check_status(&mut self) -> StreamStatusChange {
        if !self.initialized {
            // should not be getting called when not initialized, but be safe
            return StreamStatusChange::None;
        }

        if self.count_since_last >= self.params.config.samples_required {
            self.cycle_count = self.cycle_count.wrapping_add(1);
        } else {
            self.cycle_count = 0;
        }

        let mut status_change = StreamStatusChange::None;
        if self.cycle_count == 0 {
            // no packets seen for a period, flip to stopped
            status_change = StreamStatusChange::Stopped;
        } else if self.cycle_count >= self.params.config.cycles_required {
            // packets seen for some time after resume, flip to active
            status_change = StreamStatusChange::Active;
        }

        self.count_since_last = 0;
        status_change
    }
}

#[cfg(test)]
#[path = "streamtracker_packet_test.rs"]
mod streamtracker_packet_test;
