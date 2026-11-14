//! Helpers for the ported tests: what the originals take from testify (`require.InDelta`,
//! `require.Eventually`) and from `pkg/testutils`. Written new, compiled for tests only.

use std::time::{Duration, Instant};

/// What the fakes that `#[musivum_fakes::fake]` makes of traits are built from. How a fake
/// is used is written down at the macro.
pub mod fakes;
mod fakes_test;

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

thread_local! {
    // What a test on this thread captures of the log: the most verbose level it wants and
    // the lines so far. `None` while no capture is going on, and on every other thread.
    static LOG_CAPTURE: std::cell::RefCell<Option<(tracing::Level, Vec<u8>)>> =
        const { std::cell::RefCell::new(None) };
}

struct LogCaptureLines;

impl std::io::Write for LogCaptureLines {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOG_CAPTURE.with(|capture| {
            if let Some((_, lines)) = capture.borrow_mut().as_mut() {
                lines.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// One subscriber for the whole test binary, which asks the calling thread what it wants to
// see. A subscriber per test, installed for its thread only (`tracing::subscriber::
// with_default`), loses lines now and then: while it is the only one, tracing decides whether
// a call site is of interest once, on the thread that reaches it first, and a test on another
// thread that logs through the same call site at that moment has it decided as "never".
fn install_log_capture() {
    use tracing_subscriber::layer::SubscriberExt;

    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let wanted = tracing_subscriber::filter::dynamic_filter_fn(|metadata, _| {
            LOG_CAPTURE.with(|capture| {
                capture
                    .borrow()
                    .as_ref()
                    .is_some_and(|(max, _)| metadata.level() <= max)
            })
        });
        let lines = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .without_time()
            .with_writer(|| LogCaptureLines);
        let subscriber = tracing_subscriber::registry().with(wanted).with(lines);
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other subscriber in the test binary");
    });
}

/// What the calling thread logs from now on, at `max` and less verbose levels, until this is
/// dropped: one line per message, level first, without time or colours. For tests of what
/// the ported code logs. Only what is logged on the thread that started the capture is seen,
/// and a thread has one capture at a time. Do not install a subscriber of a test's own
/// instead (see `install_log_capture` for why).
pub struct LogCapture {
    // of the thread it was started on
    _not_send: std::marker::PhantomData<*const ()>,
}

impl LogCapture {
    pub fn start(max: tracing::Level) -> LogCapture {
        install_log_capture();
        LOG_CAPTURE.with(|capture| *capture.borrow_mut() = Some((max, Vec::new())));
        LogCapture {
            _not_send: std::marker::PhantomData,
        }
    }

    /// The lines so far.
    pub fn text(&self) -> String {
        LOG_CAPTURE.with(|capture| {
            let capture = capture.borrow();
            let (_, lines) = capture.as_ref().expect("the capture was started");
            String::from_utf8(lines.clone()).unwrap()
        })
    }
}

impl Drop for LogCapture {
    fn drop(&mut self) {
        LOG_CAPTURE.with(|capture| *capture.borrow_mut() = None);
    }
}

/// What `f` logs on the calling thread, see [`LogCapture`].
pub fn capture_log(max: tracing::Level, f: impl FnOnce()) -> String {
    let capture = LogCapture::start(max);
    f();
    capture.text()
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
