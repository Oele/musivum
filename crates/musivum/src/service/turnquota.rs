// Ported from LiveKit, pkg/service/turnquota.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The quota is made as an `Arc` (`new_turn_allocation_quota`)
// and keeps a `Weak` of itself for its reclaim timers; a slot is an `Arc` (its identity is what
// `reclaimPending` compares). `reservationTTL` can be set before the quota is used (the tests
// do), behind a lock. `OnCreated` and `OnDeleted` take the arguments pion's event handler
// hands them (the addresses as `Option`s, as they may be nil there).

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::go::time::{self, Duration, Timer};
use crate::infow;
use crate::protocol::logger;
use crate::webrtc::turn::net::Addr;
use crate::webrtc::turn::{EventHandler, QuotaHandler};

// defaultTURNReservationTTL bounds how long a reserved-but-unconfirmed
// allocation slot may live. Pion calls the QuotaHandler and then synchronously
// creates the allocation, so a healthy reservation is confirmed within
// microseconds; this generous window only matters if allocation creation fails
// after the quota check (see turnAllocationQuota).
const DEFAULT_TURN_RESERVATION_TTL: Duration = Duration::from_secs(30);

// turnAllocationQuota enforces a per-user cap on concurrent relay allocations on
// the embedded TURN server. Without it, a single authenticated participant can
// reuse its credential across many client 5-tuples and open one relay socket/port
// per request, exhausting the shared relay-port range for every other participant.
//
// The quota is keyed by the user ID returned from TURNAuthHandler.HandleAuth,
// which is the stable participant ID embedded in the signed TURN username. A
// participant cannot forge a different ID without the API secret, so this bounds
// the total relay footprint of any one participant.
//
// Slots are reserved in Allow (which Pion calls before creating an allocation)
// and released in OnDeleted, all under a single lock, so a burst of concurrent
// Allocate requests cannot race past the limit. Reservations are keyed by the
// client source address so that Allocate retransmissions from the same 5-tuple
// are idempotent rather than double-counted.
//
// A reservation starts out pending and is confirmed in OnCreated. Pion emits no
// event when an Allocate passes the quota check but then fails to create a relay
// (e.g. the relay-port range is exhausted), so each pending reservation carries a
// reclaim timer that frees the slot after reservationTTL. This keeps a failed
// attempt from occupying a slot forever, which would otherwise let a participant
// lock itself out and grow the tracking map without bound.
pub(crate) struct TurnAllocationQuota {
    this: Weak<TurnAllocationQuota>,
    limit: usize,
    pub(crate) reservation_ttl: Mutex<Duration>,

    // what `mu` guards: userID -> source-address key -> slot
    pub(crate) mu: Mutex<HashMap<String, HashMap<String, Arc<TurnAllocationSlot>>>>,
}

pub(crate) struct TurnAllocationSlot {
    confirmed: Mutex<bool>,
    timer: Mutex<Option<Timer>>,
}

pub(crate) fn new_turn_allocation_quota(limit: usize) -> Arc<TurnAllocationQuota> {
    Arc::new_cyclic(|this| TurnAllocationQuota {
        this: this.clone(),
        limit,
        reservation_ttl: Mutex::new(DEFAULT_TURN_RESERVATION_TTL),
        mu: Mutex::new(HashMap::new()),
    })
}

pub(crate) fn src_addr_key(src_addr: Option<Addr>) -> String {
    let Some(src_addr) = src_addr else {
        return String::new();
    };
    // include the network so an ip:port reused across UDP and TCP relays is not
    // collapsed into a single slot
    format!("{}|{}", src_addr.network(), src_addr)
}

impl TurnAllocationQuota {
    // Allow implements turn.QuotaHandler. It returns true if the allocation may
    // proceed, reserving a slot for the user, and false (Pion replies 486 Allocation
    // Quota Reached) once the user is at its limit.
    pub(crate) fn allow(&self, user_id: &str, _: &str, src_addr: Option<Addr>) -> bool {
        let key = src_addr_key(src_addr);

        let mut users = self.mu.lock();

        if users
            .get(user_id)
            .is_some_and(|slots| slots.contains_key(&key))
        {
            // already reserved for this 5-tuple (retransmit / create race) - idempotent
            return true;
        }
        if users.get(user_id).map_or(0, |slots| slots.len()) >= self.limit {
            infow!(
                logger::get_logger(),
                "TURN allocation quota reached",
                "participantID" => user_id,
                "limit" => self.limit,
            );
            return false;
        }
        let slots = users.entry(user_id.to_string()).or_default();
        // reserve a pending slot; reclaim it if the allocation is never created so a
        // failed attempt (Pion emits no created/deleted event) cannot hold it forever
        let slot = Arc::new(TurnAllocationSlot {
            confirmed: Mutex::new(false),
            timer: Mutex::new(None),
        });
        {
            let (this, user_id, key, weak_slot) = (
                self.this.clone(),
                user_id.to_string(),
                key.clone(),
                Arc::downgrade(&slot),
            );
            *slot.timer.lock() = Some(time::after_func(*self.reservation_ttl.lock(), move || {
                if let (Some(q), Some(slot)) = (this.upgrade(), weak_slot.upgrade()) {
                    q.reclaim_pending(&user_id, &key, &slot);
                }
            }));
        }
        slots.insert(key, slot);
        true
    }

    // OnCreated confirms a reservation once the relay allocation actually exists,
    // cancelling its reclaim timer so a live allocation is never evicted early.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_created(
        &self,
        src_addr: Option<Addr>,
        _: Option<Addr>,
        _: &str,
        user_id: &str,
        _: &str,
        _: Option<Addr>,
        _: i64,
    ) {
        let key = src_addr_key(src_addr);

        let users = self.mu.lock();

        let Some(slot) = users.get(user_id).and_then(|slots| slots.get(&key)) else {
            return;
        };
        *slot.confirmed.lock() = true;
        if let Some(timer) = slot.timer.lock().take() {
            timer.stop();
        }
    }

    // OnDeleted releases the slot reserved in Allow so the user regains capacity once
    // an allocation ends.
    pub(crate) fn on_deleted(
        &self,
        src_addr: Option<Addr>,
        _: Option<Addr>,
        _: &str,
        user_id: &str,
        _: &str,
    ) {
        let key = src_addr_key(src_addr);

        let mut users = self.mu.lock();

        remove_locked(&mut users, user_id, &key);
    }

    // reclaimPending drops a reservation that was never confirmed, freeing a slot
    // left behind by an Allocate that passed the quota but failed to create a relay.
    // It is identity-aware: a timer that fires after its slot was already released
    // must not evict a replacement reservation created for the same userID+key.
    pub(crate) fn reclaim_pending(&self, user_id: &str, key: &str, slot: &Arc<TurnAllocationSlot>) {
        let mut users = self.mu.lock();

        let current = users.get(user_id).and_then(|slots| slots.get(key));
        if !current.is_some_and(|s| Arc::ptr_eq(s, slot)) || *slot.confirmed.lock() {
            return;
        }
        remove_locked(&mut users, user_id, key);
    }

    // eventHandler returns the Pion EventHandler wired to confirm and release quota
    // slots as allocations are created and torn down.
    pub(crate) fn event_handler(self: &Arc<Self>) -> EventHandler {
        let (created, deleted) = (self.clone(), self.clone());
        EventHandler {
            on_allocation_created: Some(Arc::new(
                move |src, dst, protocol, user_id, realm, relay, requested_port| {
                    created.on_created(src, dst, protocol, user_id, realm, relay, requested_port)
                },
            )),
            on_allocation_deleted: Some(Arc::new(move |src, dst, protocol, user_id, realm| {
                deleted.on_deleted(src, dst, protocol, user_id, realm)
            })),
            ..Default::default()
        }
    }

    /// `quota.Allow` as pion's `QuotaHandler`.
    pub(crate) fn quota_handler(self: &Arc<Self>) -> QuotaHandler {
        let q = self.clone();
        Arc::new(move |user_id: &str, realm: &str, src_addr: Addr| {
            q.allow(user_id, realm, Some(src_addr))
        })
    }
}

fn remove_locked(
    users: &mut HashMap<String, HashMap<String, Arc<TurnAllocationSlot>>>,
    user_id: &str,
    key: &str,
) {
    let Some(slots) = users.get_mut(user_id) else {
        return;
    };
    let Some(slot) = slots.remove(key) else {
        return;
    };
    if let Some(timer) = slot.timer.lock().take() {
        timer.stop();
    }
    if slots.is_empty() {
        users.remove(user_id);
    }
}

#[cfg(test)]
#[path = "turnquota_test.rs"]
mod turnquota_test;
