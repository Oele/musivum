// Ported from pion/turn, internal/allocation/channel_bind.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. A channel binding is held as an `Arc`; it holds its
// allocation weakly (set when it is added) and its timer, once started.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::Allocation;
use crate::go::time::{self, Duration, Timer};
use crate::protocol::logger::Logger;
use crate::webrtc::turn::internal::proto::ChannelNumber;
use crate::webrtc::turn::net::Addr;

// ChannelBind represents a TURN Channel
// See: https://tools.ietf.org/html/rfc5766#section-2.5
pub struct ChannelBind {
    pub peer: Addr,
    pub number: ChannelNumber,

    pub(crate) allocation: Mutex<Weak<Allocation>>,
    pub(crate) lifetime_timer: Mutex<Option<Timer>>,
    log: Logger,
}

impl ChannelBind {
    // NewChannelBind creates a new ChannelBind.
    pub fn new(number: ChannelNumber, peer: Addr, log: Logger) -> Arc<ChannelBind> {
        Arc::new(ChannelBind {
            number,
            peer,
            allocation: Mutex::new(Weak::new()),
            lifetime_timer: Mutex::new(None),
            log,
        })
    }

    pub(crate) fn start(&self, lifetime: Duration) {
        let allocation = self.allocation.lock().clone();
        let (number, peer, log) = (self.number, self.peer, self.log.clone());
        *self.lifetime_timer.lock() = Some(time::after_func(lifetime, move || {
            let Some(allocation) = allocation.upgrade() else {
                return;
            };
            if !allocation.remove_channel_bind(number) {
                errorf!(
                    log,
                    "Failed to remove ChannelBind for {:?} {:?} {:?}",
                    number,
                    peer,
                    allocation.five_tuple
                );
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
                "Failed to reset ChannelBind timer for {:?} {:?} {:?}",
                self.number,
                self.peer,
                five_tuple
            );
        }
    }
}

#[cfg(test)]
#[path = "channel_bind_test.rs"]
mod channel_bind_test;
