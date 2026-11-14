// Ported from frostbyte73/core, throttle.go at v0.1.1.
// Copyright (c) 2023 David Colburn. Licensed under the MIT License.
// Changes: translated from Go to Rust. The timer, which the original keeps and never reads, is
// not kept. The functions are run on `go::time`'s timer task, as the original runs them on a
// timer's goroutine.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time::{self, Duration};

// Throttle is a function throttler that takes a function as its argument.
// If ready, it will execute immediately, and it will always wait the specified duration
// between executions. If multiple functions are added within the same execution window,
// only the last function added will be executed.
pub struct Throttle {
    t: Arc<ThrottleInner>,
}

struct ThrottleInner {
    period: Duration,
    m: Mutex<ThrottleState>,
}

struct ThrottleState {
    ready: bool,
    // `timer`, which the original keeps and never reads, is not kept
    next: Option<Box<dyn FnOnce() + Send>>,
}

impl Throttle {
    pub fn new(period: Duration) -> Throttle {
        Throttle {
            t: Arc::new(ThrottleInner {
                period,
                m: Mutex::new(ThrottleState {
                    ready: true,
                    next: None,
                }),
            }),
        }
    }

    pub fn call(&self, f: impl FnOnce() + Send + 'static) {
        ThrottleInner::add(&self.t, Box::new(f));
    }
}

impl ThrottleInner {
    fn add(t: &Arc<ThrottleInner>, f: Box<dyn FnOnce() + Send>) {
        let mut m = t.m.lock();
        let ready = m.ready;
        let mut f = Some(f);
        if ready {
            m.ready = false;
            ThrottleInner::after_period(t);
        } else {
            m.next = f.take();
        }
        drop(m);

        if let Some(f) = f {
            f();
        }
    }

    fn after_period(t: &Arc<ThrottleInner>) {
        let weak = Arc::downgrade(t);
        time::after_func(t.period, move || {
            if let Some(t) = weak.upgrade() {
                ThrottleInner::execute(&t);
            }
        });
    }

    fn execute(t: &Arc<ThrottleInner>) {
        let mut m = t.m.lock();
        let f = m.next.take();
        if f.is_some() {
            ThrottleInner::after_period(t);
        } else {
            m.ready = true;
        }
        drop(m);

        if let Some(f) = f {
            f();
        }
    }
}
