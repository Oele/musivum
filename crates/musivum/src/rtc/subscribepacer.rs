// Ported from LiveKit, pkg/rtc/subscribepacer.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the pacer is made as an `Arc`, as the room manager
// hands the one of the node to every participant. A pacer that is off, a nil pointer
// upstream, is `None`: `new` returns an `Option`, and `Delays` on a nil pointer, which gives
// a nil map there, has no counterpart (its caller reads the delays of `None` as none).
// `lock` guards the buckets, so they are in the lock. `strconv.FormatFloat(rate, 'f', -1,
// 64)` is the function `format_float` and the order of `slices.Sort` the function
// `compare_float`, both at the end.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time::{Duration, SECOND, Time};
use crate::infow;
use crate::protocol::logger;

/// SubscribePacer measures, in shadow, how long video subscribes on this node would wait
/// if they were paced at each of a set of rates. It never delays a subscribe.
///
/// Each rate is a token bucket that refills at that many subscribes per second and holds
/// one second of tokens, so a burst of up to one second's worth goes at once and the rest
/// waits in line.
pub struct SubscribePacer {
    // what `lock sync.Mutex` guards upstream
    buckets: Mutex<Vec<SubscribePaceBucket>>,
}

struct SubscribePaceBucket {
    rate_label: String,
    interval: Duration, // time to refill one token
    burst: Duration,    // how far ahead of now tokens may be taken without waiting
    next_at: Time, // when the next token becomes free, if every earlier subscribe had waited for its token
}

impl SubscribePacer {
    /// NewSubscribePacer returns nil when no rates are given, which turns the shadow off.
    pub fn new(rates: &[f64]) -> Option<Arc<SubscribePacer>> {
        let mut rates = rates.to_vec();
        rates.sort_by(compare_float);

        let mut buckets: Vec<SubscribePaceBucket> = Vec::new();
        let mut labels: Vec<String> = Vec::new();
        for (i, &rate) in rates.iter().enumerate() {
            if rate <= 0.0 || (i > 0 && rate == rates[i - 1]) {
                continue;
            }

            let interval = Duration((SECOND.0 as f64 / rate) as i64);
            let label = format_float(rate);
            buckets.push(SubscribePaceBucket {
                rate_label: label.clone(),
                interval,
                burst: (SECOND - interval).max(Duration::ZERO),
                next_at: Time::default(),
            });
            labels.push(label);
        }
        if buckets.is_empty() {
            return None;
        }

        infow!(logger::get_logger(), "subscribe pacing shadow enabled", "rates" => labels);
        Some(Arc::new(SubscribePacer {
            buckets: Mutex::new(buckets),
        }))
    }

    /// Delays takes a token from every bucket for a subscribe that starts at now and returns,
    /// keyed by rate, how long that subscribe would have waited for it.
    pub fn delays(&self, now: Time) -> HashMap<String, Duration> {
        let mut buckets = self.buckets.lock();

        let mut delays = HashMap::with_capacity(buckets.len());
        for b in buckets.iter_mut() {
            let mut take_at = b.next_at;
            if take_at.before(now) {
                take_at = now;
            }
            delays.insert(
                b.rate_label.clone(),
                (take_at.sub(now) - b.burst).max(Duration::ZERO),
            );
            b.next_at = take_at.add(b.interval);
        }
        delays
    }
}

// The order of Go's slices.Sort for floats: a NaN is before every other value.
fn compare_float(x: &f64, y: &f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
    }
}

// Go's strconv.FormatFloat(f, 'f', -1, 64): the shortest digits that give the number back,
// without an exponent. That is what Rust's `Display` writes too, except for the infinities.
fn format_float(f: f64) -> String {
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    f.to_string()
}

#[cfg(test)]
#[path = "subscribepacer_test.rs"]
mod subscribepacer_test;
