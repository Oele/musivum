// Ported from LiveKit, pkg/telemetry/analyticsservice.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the interface and its null implementation are
// here: `analyticsService`, which sends to LiveKit's analytics recorder, is not ported
// (musivum has no analytics service). The `context.Context` that is the first parameter of
// every method is dropped: musivum carries no tracing context. `RoomProjectReporter` is left
// out, with a comment at its place: it returns a `roomobs.ProjectReporter`, of the
// protocol's `observability` package, which musivum does not have. A message is borrowed;
// the original's implementation writes an id and the node into the message it is handed,
// which an implementation here does in a copy.

use crate::protocol::livekit;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait AnalyticsService: Send + Sync {
    fn send_stats(&self, stats: &[livekit::AnalyticsStat]);
    fn send_event(&self, events: &livekit::AnalyticsEvent);
    fn send_node_room_states(&self, node_rooms: &livekit::AnalyticsNodeRooms);
    // Not here: RoomProjectReporter(ctx context.Context) roomobs.ProjectReporter
}

// ----------------------------

#[derive(Clone, Copy, Debug, Default)]
pub struct NullAnalyticService {}

impl AnalyticsService for NullAnalyticService {
    fn send_stats(&self, _stats: &[livekit::AnalyticsStat]) {}
    fn send_event(&self, _events: &livekit::AnalyticsEvent) {}
    fn send_node_room_states(&self, _node_rooms: &livekit::AnalyticsNodeRooms) {}
}
