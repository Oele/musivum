// Ported from pion/turn, internal/allocation/permission.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. A permission is held as an `Arc`; it holds its
// allocation weakly (set when it is added) and its timer, once started.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::Allocation;
use crate::go::time::{self, Duration, Timer};
use crate::protocol::logger::Logger;
use crate::webrtc::turn::net::Addr;

pub const DEFAULT_PERMISSION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

// Permission represents a TURN permission. TURN permissions mimic the address-restricted
// filtering mechanism of NATs that comply with [RFC4787].
// See: https://tools.ietf.org/html/rfc5766#section-2.3
pub struct Permission {
    pub addr: Addr,
    pub(crate) allocation: Mutex<Weak<Allocation>>,
    pub(crate) timeout: Duration,
    pub(crate) lifetime_timer: Mutex<Option<Timer>>,
    log: Logger,
}

impl Permission {
    // NewPermission create a new Permission.
    pub fn new(addr: Addr, log: Logger, timeout: Duration) -> Arc<Permission> {
        Arc::new(Permission {
            addr,
            allocation: Mutex::new(Weak::new()),
            timeout,
            lifetime_timer: Mutex::new(None),
            log,
        })
    }

    pub(crate) fn start(&self, lifetime: Duration) {
        let allocation = self.allocation.lock().clone();
        let addr = self.addr;
        *self.lifetime_timer.lock() = Some(time::after_func(lifetime, move || {
            if let Some(allocation) = allocation.upgrade() {
                allocation.remove_permission(addr);
            }
        }));
    }

    pub(crate) fn refresh(&self, lifetime: Duration) {
        let reset = self
            .lifetime_timer
            .lock()
            .as_ref()
            .is_some_and(|t| t.reset(lifetime));
        if !reset {
            let five_tuple = self.allocation.lock().upgrade().and_then(|a| a.five_tuple);
            errorf!(
                self.log,
                "Failed to reset permission timer for {} {:?}",
                self.addr,
                five_tuple
            );
        }
    }
}
