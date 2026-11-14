// Ported from LiveKit's mediatransportutil, pkg/nack/nack.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the methods of `NackQueueInterface` that change the
// queue take `&mut self`, as the queue without a lock needs; `NewNACKQueue` and
// `NewNACKQueueSafe` are `NackQueue::new` and `NackQueueSafe::new` and give the type itself
// (boxed by whoever wants a `dyn NackQueueInterface`), so the two types are public; so is
// `Nack`, which `Nacks()` hands out, and `nacks()` gives copies of the queue's nacks, not the
// queue's own slice; `NackQueueSafe` has its methods on `&self` under its lock and the trait's
// on top of those; the queue is full at `params.max_nacks`, which is what the capacity of the
// original's slice is throughout; a nack shares the queue's `NackParams` through an `Arc`.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time::{self, Duration, Time};
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_nack::NackPair;

const DEFAULT_RTT: u32 = 70; // default RTT in ms
const MAX_NACKS: usize = 100; // Max NACK sn the sfu will keep reference
const MAX_TRIES: u8 = 5; // Max number of times a packet will be NACKed
const MIN_INTERVAL: Duration = Duration(20 * time::MILLISECOND.0); // minimum interval between NACK tries for the same sequence number
const MAX_INTERVAL: Duration = Duration(400 * time::MILLISECOND.0); // maximum interval between NACK tries for the same sequence number
const BACKOFF_FACTOR: f64 = 1.25;
const MAX_LIFETIME: Duration = Duration(2 * time::MINUTE.0);

const RTT_SMOOTHING_FACTOR: f64 = 0.75; // use 75% of new value

pub trait NackQueueInterface: Send + Sync {
    fn set_rtt(&mut self, rtt: u32);
    fn remove(&mut self, sn: u16);
    fn push(&mut self, sn: u16);
    fn pairs(&mut self) -> (Vec<NackPair>, usize);
    fn nacks(&self) -> Vec<Nack>; // for testing only
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NackQueueParams {
    pub default_rtt: u32,
    pub max_nacks: usize,
    pub max_tries: u8,
    pub min_interval: Duration,
    pub max_interval: Duration,
    pub backoff_factor: f64,
    pub max_lifetime: Duration,
}

pub const NACK_QUEUE_PARAMS_DEFAULT: NackQueueParams = NackQueueParams {
    default_rtt: DEFAULT_RTT,
    max_nacks: MAX_NACKS,
    max_tries: MAX_TRIES,
    min_interval: MIN_INTERVAL,
    max_interval: MAX_INTERVAL,
    backoff_factor: BACKOFF_FACTOR,
    max_lifetime: MAX_LIFETIME,
};

pub struct NackQueue {
    params: NackQueueParams,
    nack_params: Arc<NackParams>,

    nacks: Vec<Nack>,
    rtt: u32,
}

impl NackQueue {
    pub fn new(params: NackQueueParams) -> Self {
        Self {
            params,
            nack_params: Arc::new(NackParams {
                max_tries: params.max_tries,
                min_interval: params.min_interval,
                max_interval: params.max_interval,
                backoff_factor: params.backoff_factor,
                max_life_time: params.max_lifetime,
            }),
            nacks: Vec::with_capacity(params.max_nacks),
            rtt: params.default_rtt,
        }
    }
}

impl NackQueueInterface for NackQueue {
    fn set_rtt(&mut self, rtt: u32) {
        if rtt == 0 {
            self.rtt = self.params.default_rtt;
        } else if self.rtt == 0 {
            self.rtt = rtt;
        } else {
            self.rtt = (RTT_SMOOTHING_FACTOR * rtt as f64
                + (1.0 - RTT_SMOOTHING_FACTOR) * self.rtt as f64) as u32;
        }
    }

    fn remove(&mut self, sn: u16) {
        if let Some(idx) = self.nacks.iter().position(|nack| nack.seq_num == sn) {
            self.nacks.remove(idx);
        }
    }

    fn push(&mut self, sn: u16) {
        // if at capacity, pop the first one
        if self.nacks.len() == self.params.max_nacks {
            self.nacks.remove(0);
        }

        self.nacks.push(Nack::new(self.nack_params.clone(), sn));
    }

    fn pairs(&mut self) -> (Vec<NackPair>, usize) {
        if self.nacks.is_empty() {
            return (Vec::new(), 0);
        }

        let now = time::now();

        // set it far back to get the first pair
        let mut base_sn = self.nacks[0].seq_num.wrapping_sub(17);

        let mut sns_to_purge: Vec<u16> = Vec::new();

        let mut num_seq_nums_nacked = 0;
        let mut is_pair_active = false;
        let mut np = NackPair::default();
        let mut nps: Vec<NackPair> = Vec::new();
        for nack in self.nacks.iter_mut() {
            let (should_send, should_remove, sn) = nack.get_nack(now, self.rtt);
            if should_remove {
                sns_to_purge.push(sn);
                continue;
            }
            if !should_send {
                continue;
            }

            num_seq_nums_nacked += 1;
            if sn.wrapping_sub(base_sn) > 16 {
                // need a new nack pair
                if is_pair_active {
                    nps.push(np);
                    // (the original clears isPairActive here and sets it again below)
                }

                base_sn = sn;

                np.packet_id = sn;
                np.lost_packets = 0;

                is_pair_active = true;
            } else {
                // 1 << (sn - baseSN - 1): the count is 65535 when the sequence number is the
                // base itself (pushed twice), and the shift is zero in Go then.
                np.lost_packets |= 1u16
                    .checked_shl(sn.wrapping_sub(base_sn).wrapping_sub(1) as u32)
                    .unwrap_or(0);
            }
        }

        // add any left over
        if is_pair_active {
            nps.push(np);
        }

        for sn in sns_to_purge {
            self.remove(sn);
        }

        (nps, num_seq_nums_nacked)
    }

    fn nacks(&self) -> Vec<Nack> {
        self.nacks.clone()
    }
}

// -----------------------------------------------------------------

pub struct NackQueueSafe {
    // `lock sync.Mutex` and the `NackQueueInterface` that it guards
    lock: Mutex<Box<dyn NackQueueInterface>>,
}

impl NackQueueSafe {
    pub fn new(params: NackQueueParams) -> Self {
        Self {
            lock: Mutex::new(Box::new(NackQueue::new(params))),
        }
    }

    pub fn set_rtt(&self, rtt: u32) {
        let mut n = self.lock.lock();
        n.set_rtt(rtt);
    }

    pub fn remove(&self, sn: u16) {
        let mut n = self.lock.lock();
        n.remove(sn);
    }

    pub fn push(&self, sn: u16) {
        let mut n = self.lock.lock();
        n.push(sn);
    }

    pub fn pairs(&self) -> (Vec<NackPair>, usize) {
        let mut n = self.lock.lock();
        n.pairs()
    }

    pub fn nacks(&self) -> Vec<Nack> {
        let n = self.lock.lock();
        n.nacks()
    }
}

// The interface, whose methods take `&mut self`, on top of the methods above.
impl NackQueueInterface for NackQueueSafe {
    fn set_rtt(&mut self, rtt: u32) {
        NackQueueSafe::set_rtt(self, rtt)
    }

    fn remove(&mut self, sn: u16) {
        NackQueueSafe::remove(self, sn)
    }

    fn push(&mut self, sn: u16) {
        NackQueueSafe::push(self, sn)
    }

    fn pairs(&mut self) -> (Vec<NackPair>, usize) {
        NackQueueSafe::pairs(self)
    }

    fn nacks(&self) -> Vec<Nack> {
        NackQueueSafe::nacks(self)
    }
}

// -----------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct NackParams {
    max_tries: u8,
    min_interval: Duration,
    max_interval: Duration,
    backoff_factor: f64,
    max_life_time: Duration,
}

#[derive(Clone, Debug)]
pub struct Nack {
    params: Arc<NackParams>,

    seq_num: u16,
    born_at: Time,
    tries: u8,
    last_nacked_at: Time,
}

impl Nack {
    fn new(params: Arc<NackParams>, sn: u16) -> Self {
        let now = time::now();
        Self {
            params,
            seq_num: sn,
            born_at: now,
            tries: 0,
            last_nacked_at: now,
        }
    }

    // Gives (shouldSend, shouldRemove, sn).
    fn get_nack(&mut self, now: Time, rtt: u32) -> (bool, bool, u16) {
        let sn = self.seq_num;
        if self.tries >= self.params.max_tries
            || time::since(self.born_at) > self.params.max_life_time
        {
            // shouldRemove = true
            return (false, true, sn);
        }

        let mut required_interval = Duration::ZERO;
        if self.tries > 0 {
            // exponentially backoff retries, but cap maximum spacing between retries
            required_interval = self.params.max_interval;
            let backoff_interval =
                (rtt as f64 * self.params.backoff_factor.powf((self.tries - 1) as f64)) as i64
                    * time::MILLISECOND;
            if backoff_interval < required_interval {
                required_interval = backoff_interval;
            }
        }
        if required_interval < self.params.min_interval {
            //
            // Wait for some time for out-of-order packets before NACKing even if before NACKing first time.
            // For subsequent tries, maintain minimum spacing.
            //
            required_interval = self.params.min_interval;
        }

        if now.sub(self.last_nacked_at) < required_interval {
            return (false, false, sn);
        }

        self.tries = self.tries.wrapping_add(1);
        self.last_nacked_at = now;
        // shouldSend = true
        (true, false, sn)
    }
}

#[cfg(test)]
#[path = "nack_test.rs"]
mod nack_test;
