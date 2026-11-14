//! Go's `time` values for ported code: a signed [`Duration`] and a [`Time`] with a zero value.
//!
//! The ported logic subtracts times in either order, keeps "no time yet" as the zero time and
//! multiplies durations by plain numbers. `std::time` has none of that (its duration is
//! unsigned, its instant has no zero), so ported code uses these instead and reads like its
//! original: `time.Since(t)` is [`since`], `d.Seconds()` is [`Duration::seconds`].
//!
//! [`Time`] is nanoseconds since the Unix epoch, read from a monotonic clock that is anchored
//! to the wall clock once, when the process starts. That is what LiveKit's `mono` package
//! does on top of Go's `time`, and both Go's `time.Now()` and `mono.Now()` map to [`now`].

use std::fmt;
use std::ops::{Add, AddAssign, Div, Mul, Neg, Rem, Sub, SubAssign};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

/// A length of time in nanoseconds, which may be negative. Go's `time.Duration`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration(pub i64);

pub const NANOSECOND: Duration = Duration(1);
pub const MICROSECOND: Duration = Duration(1_000);
pub const MILLISECOND: Duration = Duration(1_000_000);
pub const SECOND: Duration = Duration(1_000_000_000);
pub const MINUTE: Duration = Duration(60 * 1_000_000_000);
pub const HOUR: Duration = Duration(60 * 60 * 1_000_000_000);

impl Duration {
    pub const ZERO: Duration = Duration(0);
    pub const MAX: Duration = Duration(i64::MAX);

    pub const fn from_nanos(n: i64) -> Duration {
        Duration(n)
    }

    pub const fn from_micros(n: i64) -> Duration {
        Duration(n * 1_000)
    }

    pub const fn from_millis(n: i64) -> Duration {
        Duration(n * 1_000_000)
    }

    pub const fn from_secs(n: i64) -> Duration {
        Duration(n * 1_000_000_000)
    }

    /// A number of seconds with a fraction, truncated to the nanosecond as Go's
    /// `time.Duration(f * float64(time.Second))` is.
    pub fn from_secs_f64(f: f64) -> Duration {
        Duration((f * 1e9) as i64)
    }

    pub const fn nanoseconds(self) -> i64 {
        self.0
    }

    pub const fn microseconds(self) -> i64 {
        self.0 / 1_000
    }

    pub const fn milliseconds(self) -> i64 {
        self.0 / 1_000_000
    }

    pub fn seconds(self) -> f64 {
        let sec = self.0 / 1_000_000_000;
        let nsec = self.0 % 1_000_000_000;
        sec as f64 + nsec as f64 / 1e9
    }

    pub fn minutes(self) -> f64 {
        let min = self.0 / MINUTE.0;
        let nsec = self.0 % MINUTE.0;
        min as f64 + nsec as f64 / (60.0 * 1e9)
    }

    pub const fn abs(self) -> Duration {
        if self.0 >= 0 {
            self
        } else if self.0 == i64::MIN {
            Duration(i64::MAX)
        } else {
            Duration(-self.0)
        }
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// For the runtime's timers, which cannot wait a negative time: that becomes zero.
    pub const fn to_std(self) -> std::time::Duration {
        if self.0 <= 0 {
            std::time::Duration::ZERO
        } else {
            std::time::Duration::from_nanos(self.0 as u64)
        }
    }

    pub const fn from_std(d: std::time::Duration) -> Duration {
        let n = d.as_nanos();
        if n > i64::MAX as u128 {
            Duration(i64::MAX)
        } else {
            Duration(n as i64)
        }
    }
}

impl Add for Duration {
    type Output = Duration;
    fn add(self, rhs: Duration) -> Duration {
        Duration(self.0.wrapping_add(rhs.0))
    }
}

impl AddAssign for Duration {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl Sub for Duration {
    type Output = Duration;
    fn sub(self, rhs: Duration) -> Duration {
        Duration(self.0.wrapping_sub(rhs.0))
    }
}

impl SubAssign for Duration {
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl Neg for Duration {
    type Output = Duration;
    fn neg(self) -> Duration {
        Duration(self.0.wrapping_neg())
    }
}

impl Mul<i64> for Duration {
    type Output = Duration;
    fn mul(self, rhs: i64) -> Duration {
        Duration(self.0.wrapping_mul(rhs))
    }
}

impl Mul<Duration> for i64 {
    type Output = Duration;
    fn mul(self, rhs: Duration) -> Duration {
        Duration(self.wrapping_mul(rhs.0))
    }
}

impl Div<i64> for Duration {
    type Output = Duration;
    fn div(self, rhs: i64) -> Duration {
        Duration(self.0 / rhs)
    }
}

/// How many times `rhs` goes into `self`, as Go's `d1 / d2` of two durations.
impl Div<Duration> for Duration {
    type Output = i64;
    fn div(self, rhs: Duration) -> i64 {
        self.0 / rhs.0
    }
}

impl Rem<Duration> for Duration {
    type Output = Duration;
    fn rem(self, rhs: Duration) -> Duration {
        Duration(self.0 % rhs.0)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.0;
        let a = n.unsigned_abs();
        let sign = if n < 0 { "-" } else { "" };
        if a == 0 {
            write!(f, "0s")
        } else if a < 1_000 {
            write!(f, "{sign}{a}ns")
        } else if a < 1_000_000 {
            write!(f, "{sign}{}µs", a as f64 / 1e3)
        } else if a < 1_000_000_000 {
            write!(f, "{sign}{}ms", a as f64 / 1e6)
        } else if a < 60 * 1_000_000_000 {
            write!(f, "{sign}{}s", a as f64 / 1e9)
        } else {
            let secs = a as f64 / 1e9;
            let m = (secs / 60.0).floor();
            write!(f, "{sign}{m}m{}s", secs - m * 60.0)
        }
    }
}

impl fmt::Debug for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// A moment, in nanoseconds since the Unix epoch. The default value is the zero time, which
/// stands for "not set" as Go's zero `time.Time` does.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Time(i64);

struct Epoch {
    instant: Instant,
    unix_nano: i64,
}

static EPOCH: LazyLock<Epoch> = LazyLock::new(|| Epoch {
    instant: Instant::now(),
    unix_nano: SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0),
});

impl Time {
    pub const ZERO: Time = Time(0);

    pub fn now() -> Time {
        let e = &*EPOCH;
        Time(e.unix_nano + e.instant.elapsed().as_nanos() as i64)
    }

    /// Go's `time.Unix(sec, nsec)`.
    pub const fn unix(sec: i64, nsec: i64) -> Time {
        Time(sec.wrapping_mul(1_000_000_000).wrapping_add(nsec))
    }

    pub const fn from_unix_nano(nsec: i64) -> Time {
        Time(nsec)
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub const fn unix_nano(self) -> i64 {
        self.0
    }

    pub const fn unix_micro(self) -> i64 {
        self.0 / 1_000
    }

    pub const fn unix_milli(self) -> i64 {
        self.0 / 1_000_000
    }

    pub const fn unix_secs(self) -> i64 {
        self.0.div_euclid(1_000_000_000)
    }

    /// The nanoseconds within the second, Go's `t.Nanosecond()`.
    pub const fn nanosecond(self) -> i64 {
        self.0.rem_euclid(1_000_000_000)
    }

    /// `self - u`, Go's `t.Sub(u)`.
    pub const fn sub(self, u: Time) -> Duration {
        Duration(self.0.wrapping_sub(u.0))
    }

    /// Go's `t.Add(d)`.
    pub const fn add(self, d: Duration) -> Time {
        Time(self.0.wrapping_add(d.0))
    }

    pub const fn before(self, u: Time) -> bool {
        self.0 < u.0
    }

    pub const fn after(self, u: Time) -> bool {
        self.0 > u.0
    }

    pub const fn equal(self, u: Time) -> bool {
        self.0 == u.0
    }

    /// For the runtime's timers.
    pub fn to_instant(self) -> Instant {
        let e = &*EPOCH;
        let delta = self.0 - e.unix_nano;
        if delta >= 0 {
            e.instant + std::time::Duration::from_nanos(delta as u64)
        } else {
            e.instant
                .checked_sub(std::time::Duration::from_nanos(delta.unsigned_abs()))
                .unwrap_or(e.instant)
        }
    }
}

impl Add<Duration> for Time {
    type Output = Time;
    fn add(self, d: Duration) -> Time {
        Time::add(self, d)
    }
}

impl Sub<Time> for Time {
    type Output = Duration;
    fn sub(self, u: Time) -> Duration {
        Time::sub(self, u)
    }
}

impl fmt::Debug for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_zero() {
            write!(f, "0")
        } else {
            write!(f, "{}.{:09}", self.unix_secs(), self.nanosecond())
        }
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Go's `time.Now()` and LiveKit's `mono.Now()`.
pub fn now() -> Time {
    Time::now()
}

/// Go's `time.Since(t)`.
pub fn since(t: Time) -> Duration {
    Time::now().sub(t)
}

/// Go's `time.Until(t)`.
pub fn until(t: Time) -> Duration {
    t.sub(Time::now())
}

/// Go's `time.Sleep`, for code that runs on the runtime.
pub async fn sleep(d: Duration) {
    tokio::time::sleep(d.to_std()).await;
}

/// The runtime that timers and workers started from ported code run on: the one the caller is
/// on, or, for a caller on a plain thread, a small one of the library's own.
pub fn runtime() -> tokio::runtime::Handle {
    static OWN: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("musivum-timer")
            .enable_time()
            .build()
            .expect("the timer runtime starts")
    });
    tokio::runtime::Handle::try_current().unwrap_or_else(|_| OWN.handle().clone())
}

/// Go's `go f()` for a function that waits: runs the future on [`runtime`].
pub fn spawn<F>(f: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    runtime().spawn(f);
}

struct TimerShared {
    f: Box<dyn Fn() + Send + Sync>,
    // Which arming of the timer is current; a sleeper of an earlier one does nothing.
    generation: AtomicU64,
    // Whether the current arming has neither fired nor been stopped.
    pending: AtomicBool,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Go's `*time.Timer` as `time.AfterFunc` returns it.
#[derive(Clone)]
pub struct Timer {
    shared: Arc<TimerShared>,
}

/// Go's `time.AfterFunc(d, f)`: calls `f` once after `d`, on the runtime, unless stopped.
pub fn after_func<F>(d: Duration, f: F) -> Timer
where
    F: Fn() + Send + Sync + 'static,
{
    let timer = Timer {
        shared: Arc::new(TimerShared {
            f: Box::new(f),
            generation: AtomicU64::new(0),
            pending: AtomicBool::new(false),
            handle: Mutex::new(None),
        }),
    };
    timer.arm(d);
    timer
}

impl Timer {
    fn arm(&self, d: Duration) {
        let generation = self.shared.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.shared.pending.store(true, Ordering::SeqCst);
        let shared = self.shared.clone();
        let handle = runtime().spawn(async move {
            tokio::time::sleep(d.to_std()).await;
            if shared.generation.load(Ordering::SeqCst) == generation
                && shared.pending.swap(false, Ordering::SeqCst)
            {
                (shared.f)();
            }
        });
        if let Some(old) = self.shared.handle.lock().replace(handle) {
            old.abort();
        }
    }

    /// Prevents the timer from firing. True if it stopped it, false if it had already fired
    /// or been stopped.
    pub fn stop(&self) -> bool {
        let was_pending = self.shared.pending.swap(false, Ordering::SeqCst);
        self.shared.generation.fetch_add(1, Ordering::SeqCst);
        if let Some(handle) = self.shared.handle.lock().take() {
            handle.abort();
        }
        was_pending
    }

    /// Makes the timer fire after `d` from now. True if it was still pending.
    pub fn reset(&self, d: Duration) -> bool {
        let was_pending = self.shared.pending.load(Ordering::SeqCst);
        self.arm(d);
        was_pending
    }
}

impl fmt::Debug for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timer")
            .field("pending", &self.shared.pending.load(Ordering::SeqCst))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn durations_are_signed_and_read_like_go() {
        let d = 1500 * MILLISECOND;
        assert_eq!(d.seconds(), 1.5);
        assert_eq!(d.milliseconds(), 1500);
        assert_eq!((SECOND - d).milliseconds(), -500);
        assert_eq!((SECOND - d).abs(), 500 * MILLISECOND);
        assert_eq!(d / SECOND, 1);
        assert_eq!((d / 3).microseconds(), 500_000);
        assert_eq!(format!("{d}"), "1.5s");
        assert_eq!(format!("{}", -250 * MICROSECOND), "-250µs");
        assert_eq!((-SECOND).to_std(), std::time::Duration::ZERO);
    }

    #[test]
    fn times_subtract_both_ways_and_have_a_zero() {
        let t = Time::unix(10, 500);
        let u = t.add(2 * SECOND);
        assert_eq!(u.sub(t), 2 * SECOND);
        assert_eq!(t.sub(u), -2 * SECOND);
        assert!(t.before(u) && u.after(t));
        assert!(Time::default().is_zero());
        assert!(!now().is_zero());
        assert_eq!(t.unix_nano(), 10_000_000_500);
        assert_eq!(t.unix_secs(), 10);
    }

    #[test]
    fn now_does_not_go_back_and_is_near_the_wall_clock() {
        let a = now();
        let b = now();
        assert!(!b.before(a));
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        assert!((a.unix_nano() - wall).abs() < 5 * SECOND.0);
        assert!(since(a) >= Duration::ZERO);
    }

    #[tokio::test]
    async fn after_func_fires_once_and_can_be_stopped_and_reset() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let timer = after_func(20 * MILLISECOND, move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        sleep(60 * MILLISECOND).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(!timer.stop(), "it had fired");

        assert!(!timer.reset(20 * MILLISECOND));
        assert!(timer.stop(), "it was pending");
        sleep(60 * MILLISECOND).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);

        timer.reset(40 * MILLISECOND);
        assert!(timer.reset(10 * MILLISECOND), "still pending when reset");
        sleep(80 * MILLISECOND).await;
        assert_eq!(count.load(Ordering::SeqCst), 2, "a reset timer fires once");
    }

    #[test]
    fn after_func_works_from_a_plain_thread() {
        let (tx, rx) = std::sync::mpsc::channel();
        let tx = Mutex::new(tx);
        let _timer = after_func(10 * MILLISECOND, move || {
            let _ = tx.lock().send(());
        });
        rx.recv_timeout(std::time::Duration::from_secs(2))
            .expect("the timer fired");
    }
}
