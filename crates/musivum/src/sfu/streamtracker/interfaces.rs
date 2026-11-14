// Ported from LiveKit, pkg/sfu/streamtracker/interfaces.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `StreamStatusChange` is an enum and its `String()` is
// `Display`, without the text for a value that is none of the three; the methods of
// `StreamTrackerImpl` that change the tracker take `&mut self`, as a tracker without a lock
// needs (its owner guards it); the callbacks that `StreamTrackerWorker` takes are boxed.

use std::fmt;

use crate::go::time::Duration;
use crate::sfu::buffer;

use super::StreamStatus;

// ------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StreamStatusChange {
    #[default]
    None,
    Stopped,
    Active,
}

impl fmt::Display for StreamStatusChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StreamStatusChange::None => "none",
            StreamStatusChange::Stopped => "stopped",
            StreamStatusChange::Active => "active",
        })
    }
}

// ------------------------------------------------------------

pub trait StreamTrackerImpl: Send + Sync {
    fn start(&mut self);
    fn stop(&mut self);
    fn reset(&mut self);

    fn get_check_interval(&self) -> Duration;

    fn observe(&mut self, has_marker: bool, ts: u32) -> StreamStatusChange;
    fn check_status(&mut self) -> StreamStatusChange;
}

pub trait StreamTrackerWorker: Send + Sync {
    fn start(&self);
    fn stop(&self);
    fn reset(&self);
    fn on_status_changed(&self, f: Box<dyn Fn(StreamStatus) + Send + Sync>);
    fn on_bitrate_available(&self, f: Box<dyn Fn() + Send + Sync>);
    fn status(&self) -> StreamStatus;
    fn bitrate_temporal_cumulative(&self) -> Vec<i64>;
    fn set_paused(&self, paused: bool);
    fn observe(
        &self,
        temporal_layer: i32,
        pkt_size: usize,
        payload_size: usize,
        has_marker: bool,
        ts: u32,
        dd: Option<&buffer::ExtDependencyDescriptor>,
    );
}
