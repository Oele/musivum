// Ported from LiveKit, pkg/telemetry/prometheus/node.go (`GetNodeStats`, `getNodeStatsRate`,
// `perSec`), node_nonwindows.go (`getLoadAvg`), node_nonlinux.go (`getTCStats`) and the
// counters of rooms.go and packets.go that `GetNodeStats` reads, at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. This belongs to `crate::telemetry::prometheus`
// (`node.rs`, `rooms.rs`, `packets.rs`) and is here only because the port of `pkg/routing`
// could not touch anything outside `routing`; it is meant to move there.
// - The counters are statics of `crate::go::atomic`, as the module of telemetry has them.
//   Of the functions that write them only those of rooms and participants are here
//   (`RoomStarted`, `RoomEnded`, `AddParticipant`, `SubParticipant`), without the Prometheus
//   gauges and the histogram of room durations they also set upstream; the other counters
//   are read as they are, 0 until their writers are ported. `forwardLatency` and
//   `trackSubscribeCancels`, which telemetry keeps to itself, are read from the public
//   collectors that the same functions set to the same values (`PROM_FORWARD_LATENCY`, the
//   "cancel" count of `PROM_TRACK_SUBSCRIBE_COUNTER`).
// - `cpuStats` and `memoryStats`, which `Init` makes upstream, are made by the first call of
//   `get_node_stats`, which returns their error, as `Init` does.
// - `getTCStats` gives 0 on every system: on Linux the original asks the kernel's traffic
//   control over netlink, which needs a dependency (go-tc upstream); so the system's packets
//   and drops, and the gauge `promSysPacketGauge` of them, are not here.

use std::sync::OnceLock;

use super::hwstats::{self, CpuStats, MemoryStats};
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::livekit;
use crate::telemetry::prometheus as telemetry;

// rooms.go
pub(crate) static ROOM_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static PARTICIPANT_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static TRACK_PUBLISHED_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static TRACK_SUBSCRIBED_CURRENT: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static TRACK_PUBLISH_ATTEMPTS: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static TRACK_PUBLISH_SUCCESS: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static TRACK_PUBLISH_CANCELS: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static TRACK_SUBSCRIBE_ATTEMPTS: atomic::Int32 = atomic::Int32::new(0);
pub(crate) static TRACK_SUBSCRIBE_SUCCESS: atomic::Int32 = atomic::Int32::new(0);

// packets.go
pub(crate) static BYTES_IN: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static BYTES_OUT: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static PACKETS_IN: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static PACKETS_OUT: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static NACK_TOTAL: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static RETRANSMIT_BYTES: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static RETRANSMIT_PACKETS: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static PARTICIPANT_SIGNAL_CONNECTED: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static PARTICIPANT_RTC_CONNECTED: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static PARTICIPANT_RTC_INIT: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static PARTICIPANT_RTC_CANCELED: atomic::Uint64 = atomic::Uint64::new(0);
pub(crate) static PARTICIPANT_RTC_ACTIVE: atomic::Uint64 = atomic::Uint64::new(0);

// node.go: made by `Init` upstream
static CPU_STATS: OnceLock<Result<std::sync::Arc<CpuStats>, hwstats::Error>> = OnceLock::new();
static MEMORY_STATS: OnceLock<Result<MemoryStats, hwstats::Error>> = OnceLock::new();

// rooms.go

pub fn room_started() {
    ROOM_CURRENT.inc();
}

pub fn room_ended(started_at: Time) {
    // the duration of the room, which upstream observes in `promRoomDuration`, is not kept
    let _ = started_at;
    ROOM_CURRENT.dec();
}

pub fn add_participant() {
    PARTICIPANT_CURRENT.inc();
}

pub fn sub_participant() {
    PARTICIPANT_CURRENT.dec();
}

// node_nonwindows.go
fn get_load_avg() -> Result<hwstats::osstat::LoadavgStats, hwstats::Error> {
    hwstats::osstat::loadavg_get()
}

// node_nonlinux.go, for every system (see the header)
fn get_tc_stats() -> (u32, u32) {
    // linux only
    (0, 0)
}

// node.go

pub fn get_node_stats(
    node_started_at: i64,
    prev_stats: &[Option<livekit::NodeStats>],
    rate_intervals: &[Duration],
) -> Result<livekit::NodeStats, hwstats::Error> {
    let cpu_stats = CPU_STATS
        .get_or_init(|| CpuStats::new(None))
        .as_ref()
        .map_err(Clone::clone)?;
    let memory_stats = MEMORY_STATS
        .get_or_init(MemoryStats::new)
        .as_ref()
        .map_err(Clone::clone)?;

    let load_avg = get_load_avg()?;

    // On MacOS, get "\"vm_stat\": executable file not found in $PATH" although it is in /usr/bin
    // So, do not error out. Use the information if it is available.
    let (mem_used, mem_total) = memory_stats.get_memory().unwrap_or_default();

    let (sys_packets, sys_dropped_packets) = get_tc_stats();

    let mut stats = livekit::NodeStats {
        started_at: node_started_at,
        updated_at: time::now().unix_secs(),
        num_rooms: ROOM_CURRENT.load(),
        num_clients: PARTICIPANT_CURRENT.load(),
        num_tracks_in: TRACK_PUBLISHED_CURRENT.load(),
        num_tracks_out: TRACK_SUBSCRIBED_CURRENT.load(),
        num_track_publish_attempts: TRACK_PUBLISH_ATTEMPTS.load(),
        num_track_publish_success: TRACK_PUBLISH_SUCCESS.load(),
        num_track_publish_cancels: TRACK_PUBLISH_CANCELS.load(),
        num_track_subscribe_attempts: TRACK_SUBSCRIBE_ATTEMPTS.load(),
        num_track_subscribe_success: TRACK_SUBSCRIBE_SUCCESS.load(),
        num_track_subscribe_cancels: telemetry::PROM_TRACK_SUBSCRIBE_COUNTER
            .with_label_values(["cancel", ""])
            .get() as i32,
        bytes_in: BYTES_IN.load(),
        bytes_out: BYTES_OUT.load(),
        packets_in: PACKETS_IN.load(),
        packets_out: PACKETS_OUT.load(),
        retransmit_bytes_out: RETRANSMIT_BYTES.load(),
        retransmit_packets_out: RETRANSMIT_PACKETS.load(),
        nack_total: NACK_TOTAL.load(),
        participant_signal_connected: PARTICIPANT_SIGNAL_CONNECTED.load(),
        participant_rtc_init: PARTICIPANT_RTC_INIT.load(),
        participant_rtc_connected: PARTICIPANT_RTC_CONNECTED.load(),
        participant_rtc_canceled: PARTICIPANT_RTC_CANCELED.load(),
        participant_rtc_active: PARTICIPANT_RTC_ACTIVE.load(),
        forward_latency: telemetry::PROM_FORWARD_LATENCY.get() as u32,
        num_cpus: cpu_stats.num_cpu() as u32, // this will round down to the nearest integer
        cpu_load: cpu_stats.get_cpu_load() as f32,
        memory_total: mem_total,
        memory_used: mem_used,
        load_avg_last1min: load_avg.loadavg1 as f32,
        load_avg_last5min: load_avg.loadavg5 as f32,
        load_avg_last15min: load_avg.loadavg15 as f32,
        sys_packets_out: sys_packets,
        sys_packets_dropped: sys_dropped_packets,
        ..Default::default()
    };

    for rate_interval in rate_intervals {
        for idx in (0..prev_stats.len()).rev() {
            let Some(prev) = &prev_stats[idx] else {
                continue;
            };

            if stats.updated_at.wrapping_sub(prev.updated_at) >= rate_interval.seconds() as i64 {
                // append(prevStats[idx:], stats)
                let mut history: Vec<Option<&livekit::NodeStats>> =
                    prev_stats[idx..].iter().map(Option::as_ref).collect();
                history.push(Some(&stats));
                if let Some(rate) = get_node_stats_rate(&history) {
                    stats.rates.push(rate);
                }
                break;
            }
        }
    }

    Ok(stats)
}

fn get_node_stats_rate(
    stats_history: &[Option<&livekit::NodeStats>],
) -> Option<livekit::NodeStatsRate> {
    if stats_history.is_empty() {
        return None;
    }

    // the first is a node's statistics and the last the new ones: neither is nil
    let earlier = stats_history[0].expect("the earlier statistics");
    let later = stats_history[stats_history.len() - 1].expect("the later statistics");
    let elapsed = later.updated_at.wrapping_sub(earlier.updated_at);
    if elapsed <= 0 {
        return None;
    }

    // time weighted averages
    let (mut cpu_load, mut memory_used, mut memory_total, mut memory_load) =
        (0f32, 0f32, 0f32, 0f32);
    for idx in (1..stats_history.len()).rev() {
        let (Some(stats), Some(prev_stats)) = (stats_history[idx], stats_history[idx - 1]) else {
            continue;
        };

        let span_elapsed = stats.updated_at.wrapping_sub(prev_stats.updated_at);
        if span_elapsed <= 0 {
            continue;
        }

        cpu_load += stats.cpu_load * span_elapsed as f32;
        memory_used += stats.memory_used as f32 * span_elapsed as f32;
        memory_total += stats.memory_total as f32 * span_elapsed as f32;
        if stats.memory_total > 0 {
            memory_load +=
                stats.memory_used as f32 / stats.memory_total as f32 * span_elapsed as f32;
        }
    }

    let rate = livekit::NodeStatsRate {
        started_at: earlier.updated_at,
        ended_at: later.updated_at,
        duration: elapsed,
        bytes_in: per_sec(earlier.bytes_in, later.bytes_in, elapsed),
        bytes_out: per_sec(earlier.bytes_out, later.bytes_out, elapsed),
        packets_in: per_sec(earlier.packets_in, later.packets_in, elapsed),
        packets_out: per_sec(earlier.packets_out, later.packets_out, elapsed),
        retransmit_bytes_out: per_sec(
            earlier.retransmit_bytes_out,
            later.retransmit_bytes_out,
            elapsed,
        ),
        retransmit_packets_out: per_sec(
            earlier.retransmit_packets_out,
            later.retransmit_packets_out,
            elapsed,
        ),
        nack_total: per_sec(earlier.nack_total, later.nack_total, elapsed),
        participant_signal_connected: per_sec(
            earlier.participant_signal_connected,
            later.participant_signal_connected,
            elapsed,
        ),
        participant_signal_failed: per_sec(
            earlier.participant_signal_failed,
            later.participant_signal_failed,
            elapsed,
        ),
        participant_signal_validation_failed: per_sec(
            earlier.participant_signal_validation_failed,
            later.participant_signal_validation_failed,
            elapsed,
        ),
        participant_rtc_init: per_sec(
            earlier.participant_rtc_init,
            later.participant_rtc_init,
            elapsed,
        ),
        participant_rtc_connected: per_sec(
            earlier.participant_rtc_connected,
            later.participant_rtc_connected,
            elapsed,
        ),
        participant_rtc_canceled: per_sec(
            earlier.participant_rtc_canceled,
            later.participant_rtc_canceled,
            elapsed,
        ),
        participant_rtc_active: per_sec(
            earlier.participant_rtc_active,
            later.participant_rtc_active,
            elapsed,
        ),
        sys_packets_out: per_sec(
            u64::from(earlier.sys_packets_out),
            u64::from(later.sys_packets_out),
            elapsed,
        ),
        sys_packets_dropped: per_sec(
            u64::from(earlier.sys_packets_dropped),
            u64::from(later.sys_packets_dropped),
            elapsed,
        ),
        track_publish_attempts: per_sec(
            earlier.num_track_publish_attempts as u64,
            later.num_track_publish_attempts as u64,
            elapsed,
        ),
        track_publish_success: per_sec(
            earlier.num_track_publish_success as u64,
            later.num_track_publish_success as u64,
            elapsed,
        ),
        track_publish_cancels: per_sec(
            earlier.num_track_publish_cancels as u64,
            later.num_track_publish_cancels as u64,
            elapsed,
        ),
        track_subscribe_attempts: per_sec(
            earlier.num_track_subscribe_attempts as u64,
            later.num_track_subscribe_attempts as u64,
            elapsed,
        ),
        track_subscribe_success: per_sec(
            earlier.num_track_subscribe_success as u64,
            later.num_track_subscribe_success as u64,
            elapsed,
        ),
        track_subscribe_cancels: per_sec(
            earlier.num_track_subscribe_cancels as u64,
            later.num_track_subscribe_cancels as u64,
            elapsed,
        ),
        cpu_load: cpu_load / elapsed as f32,
        memory_load: memory_load / elapsed as f32,
        memory_used: memory_used / elapsed as f32,
        memory_total: memory_total / elapsed as f32,
    };
    Some(rate)
}

fn per_sec(prev: u64, curr: u64, secs: i64) -> f32 {
    curr.wrapping_sub(prev) as f32 / secs as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no test of the rates. Over ten seconds, the counters'
    // differences per second, and the loads weighted by the time each sample stood for.
    #[test]
    fn a_rate_is_the_difference_per_second_over_the_history() {
        let s =
            |updated_at: i64, bytes_in: u64, cpu_load: f32, memory_used: u64| livekit::NodeStats {
                updated_at,
                bytes_in,
                cpu_load,
                memory_used,
                memory_total: 1000,
                num_track_publish_attempts: (updated_at - 100) as i32,
                ..Default::default()
            };
        let (a, b, c) = (
            s(100, 1000, 0.0, 0),
            s(104, 3000, 0.5, 500),
            s(110, 6000, 0.2, 250),
        );
        let rate = get_node_stats_rate(&[Some(&a), None, Some(&b), Some(&c)]).unwrap();
        assert_eq!(100, rate.started_at);
        assert_eq!(110, rate.ended_at);
        assert_eq!(10, rate.duration);
        assert_eq!(500.0, rate.bytes_in);
        assert_eq!(1.0, rate.track_publish_attempts);
        // the span before `b` is next to a missing sample and is left out
        assert!((rate.cpu_load - 0.2 * 6.0 / 10.0).abs() < 1e-6);
        assert!((rate.memory_used - 250.0 * 6.0 / 10.0).abs() < 1e-3);
        assert!((rate.memory_load - 0.25 * 6.0 / 10.0).abs() < 1e-6);
        assert!((rate.memory_total - 1000.0 * 6.0 / 10.0).abs() < 1e-3);

        assert_eq!(None, get_node_stats_rate(&[]));
        assert_eq!(None, get_node_stats_rate(&[Some(&c), Some(&c)]));
    }
}
