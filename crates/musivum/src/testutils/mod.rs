//! Helpers for the ported tests: what the originals take from testify (`require.InDelta`,
//! `require.Eventually`) and from `pkg/testutils`. Written new, compiled for tests only.

use std::time::{Duration, Instant};

/// testify's `require.InDelta(t, expected, actual, delta)`.
#[track_caller]
pub fn assert_in_delta(expected: f64, actual: f64, delta: f64) {
    let diff = (expected - actual).abs();
    assert!(
        diff <= delta && !diff.is_nan(),
        "expected {expected}, got {actual}: further apart than {delta}"
    );
}

/// testify's `require.Eventually(t, condition, wait_for, tick)`, for a test on a plain
/// thread: tries the condition every `tick` until it holds or `wait_for` has passed.
#[track_caller]
pub fn eventually(mut condition: impl FnMut() -> bool, wait_for: Duration, tick: Duration) {
    let deadline = Instant::now() + wait_for;
    loop {
        if condition() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("the condition did not hold within {wait_for:?}");
        }
        std::thread::sleep(tick);
    }
}

/// [`eventually`] for a test on the runtime: waits without blocking a worker thread.
#[track_caller]
pub fn eventually_async(
    mut condition: impl FnMut() -> bool,
    wait_for: Duration,
    tick: Duration,
) -> impl Future<Output = ()> {
    let location = std::panic::Location::caller();
    async move {
        let deadline = Instant::now() + wait_for;
        loop {
            if condition() {
                return;
            }
            if Instant::now() >= deadline {
                panic!("the condition did not hold within {wait_for:?} (at {location})");
            }
            tokio::time::sleep(tick).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_delta_and_eventually_pass_when_they_should() {
        assert_in_delta(1.0, 1.05, 0.1);
        let mut n = 0;
        eventually(
            || {
                n += 1;
                n == 3
            },
            Duration::from_secs(1),
            Duration::from_millis(1),
        );
    }

    #[test]
    #[should_panic(expected = "further apart")]
    fn in_delta_fails_outside_the_delta() {
        assert_in_delta(1.0, 1.2, 0.1);
    }

    #[test]
    #[should_panic(expected = "did not hold")]
    fn eventually_gives_up() {
        eventually(
            || false,
            Duration::from_millis(20),
            Duration::from_millis(5),
        );
    }
}
