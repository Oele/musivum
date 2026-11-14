// Ported from LiveKit, pkg/sfu/utils/debounce.go at v1.13.11.
// Copyright LiveKit, Inc. (the original file has no notice of its own). Licensed under the
// Apache License, Version 2.0.
// Changes: translated from Go to Rust. At this version the original is a debouncer of its
// own on `time.AfterFunc`, not a wrapper of another package, and is ported as it is.

use parking_lot::Mutex;

use crate::go::time::{self, Duration, Timer};

pub struct Debouncer {
    // what `mu sync.Mutex` guards upstream
    inner: Mutex<DebouncerInner>,
}

struct DebouncerInner {
    after: Duration,
    timer: Option<Timer>,
}

impl Debouncer {
    pub fn new(after: Duration) -> Self {
        Self {
            inner: Mutex::new(DebouncerInner { after, timer: None }),
        }
    }

    pub fn add(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut inner = self.inner.lock();

        if let Some(timer) = &inner.timer {
            timer.stop();
        }
        inner.timer = Some(time::after_func(inner.after, f));
    }

    pub fn set_duration(&self, after: Duration) {
        let mut inner = self.inner.lock();
        inner.after = after;
    }
}

// New for musivum: the original has no test.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::go::atomic;
    use crate::testutils::eventually_async;

    #[tokio::test(flavor = "multi_thread")]
    async fn only_the_last_function_added_within_the_time_runs() {
        let d = Debouncer::new(100 * time::MILLISECOND);
        let ran = Arc::new(atomic::Int32::new(0));
        for i in 0..3 {
            let ran = ran.clone();
            d.add(move || {
                // Each function leaves its own mark: 1, 2 or 4.
                ran.add(1 << i);
            });
            time::sleep(5 * time::MILLISECOND).await;
        }
        assert_eq!(0, ran.load(), "nothing runs before the time is over");
        eventually_async(
            || ran.load() == 4,
            std::time::Duration::from_secs(3),
            std::time::Duration::from_millis(5),
        )
        .await;

        // A new duration counts for what is added after it.
        d.set_duration(500 * time::MILLISECOND);
        let ran_late = ran.clone();
        d.add(move || {
            ran_late.add(8);
        });
        time::sleep(150 * time::MILLISECOND).await;
        assert_eq!(4, ran.load(), "the longer time is not over yet");
        eventually_async(
            || ran.load() == 12,
            std::time::Duration::from_secs(3),
            std::time::Duration::from_millis(10),
        )
        .await;
    }
}
