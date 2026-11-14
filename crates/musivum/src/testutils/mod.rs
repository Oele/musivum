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

/// `ConnectTimeout` of LiveKit's `pkg/testutils`: how long [`with_timeout`] waits when it is
/// given no timeout.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

// How often `with_timeout` asks.
const WITH_TIMEOUT_TICK: Duration = Duration::from_millis(10);

/// `WithTimeout(t, f, timeouts...)` of LiveKit's `pkg/testutils`, for a test on a plain
/// thread: calls `f` every 10 ms, for the first time after 10 ms, until it returns an empty
/// string, and fails with the last string it returned once the timeout has passed. `timeout`
/// is the original's optional last argument; `None` is [`CONNECT_TIMEOUT`].
///
/// The original fails only if `f` was called at least once, and waits forever if the timeout
/// is shorter than the first 10 ms; this fails then too.
#[track_caller]
pub fn with_timeout(mut f: impl FnMut() -> String, timeout: Option<Duration>) {
    let timeout = timeout.unwrap_or(CONNECT_TIMEOUT);
    let deadline = Instant::now() + timeout;
    let mut last_err = String::new();
    loop {
        let now = Instant::now();
        if now + WITH_TIMEOUT_TICK > deadline {
            std::thread::sleep(deadline.saturating_duration_since(now));
            panic!("did not reach expected state after {timeout:?}: {last_err}");
        }
        std::thread::sleep(WITH_TIMEOUT_TICK);
        last_err = f();
        if last_err.is_empty() {
            return;
        }
    }
}

/// [`with_timeout`] for a test on the runtime: waits without blocking a worker thread.
#[track_caller]
pub fn with_timeout_async(
    mut f: impl FnMut() -> String,
    timeout: Option<Duration>,
) -> impl Future<Output = ()> {
    let location = std::panic::Location::caller();
    async move {
        let timeout = timeout.unwrap_or(CONNECT_TIMEOUT);
        let deadline = Instant::now() + timeout;
        let mut last_err = String::new();
        loop {
            let now = Instant::now();
            if now + WITH_TIMEOUT_TICK > deadline {
                tokio::time::sleep(deadline.saturating_duration_since(now)).await;
                panic!(
                    "did not reach expected state after {timeout:?}: {last_err} (at {location})"
                );
            }
            tokio::time::sleep(WITH_TIMEOUT_TICK).await;
            last_err = f();
            if last_err.is_empty() {
                return;
            }
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

    #[test]
    fn with_timeout_asks_until_the_answer_is_empty() {
        let mut n = 0;
        with_timeout(
            || {
                n += 1;
                if n == 3 {
                    String::new()
                } else {
                    format!("asked {n} times")
                }
            },
            None,
        );
        assert_eq!(n, 3);
    }

    #[test]
    #[should_panic(expected = "did not reach expected state after 50ms: still waiting")]
    fn with_timeout_gives_up_with_the_last_answer() {
        with_timeout(
            || "still waiting".to_string(),
            Some(Duration::from_millis(50)),
        );
    }

    #[tokio::test]
    async fn with_timeout_async_asks_until_the_answer_is_empty() {
        let mut n = 0;
        with_timeout_async(
            || {
                n += 1;
                if n == 3 {
                    String::new()
                } else {
                    format!("asked {n} times")
                }
            },
            None,
        )
        .await;
        assert_eq!(n, 3);
    }

    #[tokio::test]
    #[should_panic(expected = "did not reach expected state after 50ms: still waiting")]
    async fn with_timeout_async_gives_up_with_the_last_answer() {
        with_timeout_async(
            || "still waiting".to_string(),
            Some(Duration::from_millis(50)),
        )
        .await;
    }
}
