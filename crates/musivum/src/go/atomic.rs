//! The atomic values of `go.uber.org/atomic`, which the ported code uses throughout: `load`
//! and `store` without an ordering to choose (always sequentially consistent, as in Go).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};

use parking_lot::RwLock;

use super::time;

const ORDER: Ordering = Ordering::SeqCst;

#[derive(Default)]
pub struct Bool(AtomicBool);

impl Bool {
    pub const fn new(v: bool) -> Self {
        Self(AtomicBool::new(v))
    }

    pub fn load(&self) -> bool {
        self.0.load(ORDER)
    }

    pub fn store(&self, v: bool) {
        self.0.store(v, ORDER);
    }

    /// Stores `v` and returns what was there.
    pub fn swap(&self, v: bool) -> bool {
        self.0.swap(v, ORDER)
    }

    /// Go's `CompareAndSwap`: stores `new` if the value is `old`; true if it did.
    pub fn cas(&self, old: bool, new: bool) -> bool {
        self.0.compare_exchange(old, new, ORDER, ORDER).is_ok()
    }

    /// Flips the value and returns what was there.
    pub fn toggle(&self) -> bool {
        self.0.fetch_xor(true, ORDER)
    }
}

impl fmt::Debug for Bool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.load(), f)
    }
}

macro_rules! integer {
    ($name:ident, $atomic:ident, $int:ty) => {
        #[derive(Default)]
        pub struct $name($atomic);

        impl $name {
            pub const fn new(v: $int) -> Self {
                Self($atomic::new(v))
            }

            pub fn load(&self) -> $int {
                self.0.load(ORDER)
            }

            pub fn store(&self, v: $int) {
                self.0.store(v, ORDER);
            }

            /// Stores `v` and returns what was there.
            pub fn swap(&self, v: $int) -> $int {
                self.0.swap(v, ORDER)
            }

            /// Go's `CompareAndSwap`: stores `new` if the value is `old`; true if it did.
            pub fn cas(&self, old: $int, new: $int) -> bool {
                self.0.compare_exchange(old, new, ORDER, ORDER).is_ok()
            }

            /// Adds and returns the new value.
            pub fn add(&self, delta: $int) -> $int {
                self.0.fetch_add(delta, ORDER).wrapping_add(delta)
            }

            /// Subtracts and returns the new value.
            pub fn sub(&self, delta: $int) -> $int {
                self.0.fetch_sub(delta, ORDER).wrapping_sub(delta)
            }

            /// Adds one and returns the new value.
            pub fn inc(&self) -> $int {
                self.add(1)
            }

            /// Subtracts one and returns the new value.
            pub fn dec(&self) -> $int {
                self.sub(1)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&self.load(), f)
            }
        }
    };
}

integer!(Int32, AtomicI32, i32);
integer!(Int64, AtomicI64, i64);
integer!(Uint32, AtomicU32, u32);
integer!(Uint64, AtomicU64, u64);

#[derive(Default)]
pub struct Float64(AtomicU64);

impl Float64 {
    pub fn new(v: f64) -> Self {
        Self(AtomicU64::new(v.to_bits()))
    }

    pub fn load(&self) -> f64 {
        f64::from_bits(self.0.load(ORDER))
    }

    pub fn store(&self, v: f64) {
        self.0.store(v.to_bits(), ORDER);
    }

    pub fn swap(&self, v: f64) -> f64 {
        f64::from_bits(self.0.swap(v.to_bits(), ORDER))
    }
}

impl fmt::Debug for Float64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.load(), f)
    }
}

#[derive(Default)]
pub struct Duration(AtomicI64);

impl Duration {
    pub const fn new(v: time::Duration) -> Self {
        Self(AtomicI64::new(v.0))
    }

    pub fn load(&self) -> time::Duration {
        time::Duration(self.0.load(ORDER))
    }

    pub fn store(&self, v: time::Duration) {
        self.0.store(v.0, ORDER);
    }

    pub fn swap(&self, v: time::Duration) -> time::Duration {
        time::Duration(self.0.swap(v.0, ORDER))
    }

    /// Adds and returns the new value.
    pub fn add(&self, delta: time::Duration) -> time::Duration {
        time::Duration(self.0.fetch_add(delta.0, ORDER).wrapping_add(delta.0))
    }
}

impl fmt::Debug for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.load(), f)
    }
}

/// An atomic [`time::Time`]; the default is the zero time.
#[derive(Default)]
pub struct Time(AtomicI64);

impl Time {
    pub const fn new(v: time::Time) -> Self {
        Self(AtomicI64::new(v.unix_nano()))
    }

    pub fn load(&self) -> time::Time {
        time::Time::from_unix_nano(self.0.load(ORDER))
    }

    pub fn store(&self, v: time::Time) {
        self.0.store(v.unix_nano(), ORDER);
    }

    pub fn swap(&self, v: time::Time) -> time::Time {
        time::Time::from_unix_nano(self.0.swap(v.unix_nano(), ORDER))
    }
}

impl fmt::Debug for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.load(), f)
    }
}

/// Go's `atomic.String`.
#[derive(Default)]
pub struct String(RwLock<Arc<str>>);

impl String {
    pub fn new(v: &str) -> Self {
        Self(RwLock::new(Arc::from(v)))
    }

    pub fn load(&self) -> Arc<str> {
        self.0.read().clone()
    }

    pub fn store(&self, v: &str) {
        *self.0.write() = Arc::from(v);
    }
}

impl fmt::Debug for String {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.load(), f)
    }
}

/// Go's `atomic.Pointer[T]`: a shared value that can be replaced, or absent (Go's nil).
pub struct Pointer<T>(RwLock<Option<Arc<T>>>);

impl<T> Pointer<T> {
    pub const fn new(v: Option<Arc<T>>) -> Self {
        Self(RwLock::new(v))
    }

    pub fn load(&self) -> Option<Arc<T>> {
        self.0.read().clone()
    }

    pub fn store(&self, v: Option<Arc<T>>) {
        *self.0.write() = v;
    }

    pub fn swap(&self, v: Option<Arc<T>>) -> Option<Arc<T>> {
        std::mem::replace(&mut *self.0.write(), v)
    }
}

impl<T> Default for Pointer<T> {
    fn default() -> Self {
        Self::new(None)
    }
}

impl<T: fmt::Debug> fmt::Debug for Pointer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.load(), f)
    }
}

/// Go's `atomic.Value` holding a `T`, absent until first stored.
pub struct Value<T>(RwLock<Option<T>>);

impl<T: Clone> Value<T> {
    pub const fn new() -> Self {
        Self(RwLock::new(None))
    }

    pub fn load(&self) -> Option<T> {
        self.0.read().clone()
    }

    pub fn store(&self, v: T) {
        *self.0.write() = Some(v);
    }
}

impl<T: Clone> Default for Value<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for Value<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.load(), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_return_the_new_value_like_uber_atomic() {
        let a = Int32::new(5);
        assert_eq!(a.inc(), 6);
        assert_eq!(a.add(4), 10);
        assert_eq!(a.dec(), 9);
        assert_eq!(a.swap(1), 9);
        assert!(a.cas(1, 2));
        assert!(!a.cas(1, 3));
        assert_eq!(a.load(), 2);
        let u = Uint32::new(0);
        assert_eq!(u.dec(), u32::MAX);
    }

    #[test]
    fn the_others_hold_what_was_stored() {
        let b = Bool::default();
        assert!(!b.swap(true));
        assert!(b.cas(true, false));
        assert!(!b.toggle());
        assert!(b.load());

        let f = Float64::new(1.5);
        f.store(-0.25);
        assert_eq!(f.load(), -0.25);

        let d = Duration::default();
        d.store(3 * time::SECOND);
        assert_eq!(d.load(), 3 * time::SECOND);

        let t = Time::default();
        assert!(t.load().is_zero());
        t.store(time::Time::unix(7, 0));
        assert_eq!(t.load().unix_secs(), 7);

        let s = String::new("a");
        s.store("b");
        assert_eq!(&*s.load(), "b");

        let p: Pointer<u8> = Pointer::default();
        assert!(p.load().is_none());
        p.store(Some(Arc::new(3)));
        assert_eq!(*p.load().unwrap(), 3);

        let v: Value<u8> = Value::new();
        assert_eq!(v.load(), None);
        v.store(9);
        assert_eq!(v.load(), Some(9));
    }
}
