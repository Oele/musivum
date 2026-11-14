// Ported from LiveKit, pkg/sfu/videolayerselector/selectordecisioncache.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `selectorDecision` is an enum of its four values and is public, since public functions
//   give and take it; `String()` is `Display`, without the branch for other values.
// - `get_decision` gives the decision or the error, not both: where the original gives an
//   error it gives `selectorDecisionMissing` with it, which is what a caller that ignores the
//   error has to take.
// - The cache has no lock, as in the original. The selector and its frame chains share it as
//   an `Arc<Mutex<SelectorDecisionCache>>` where the original shares a pointer. A function
//   given to `expect_decision` is called with that lock held, so it must not call the cache.

use std::collections::HashMap;
use std::fmt;

use super::Error;

// ----------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SelectorDecision {
    #[default]
    Missing = 0,
    Dropped = 1,
    Forwarded = 2,
    Unknown = 3,
}

impl SelectorDecision {
    // The original's conversion `selectorDecision(bits)` of the two bits of an entry.
    fn from_bits(bits: u64) -> Self {
        match bits & 0x3 {
            0 => SelectorDecision::Missing,
            1 => SelectorDecision::Dropped,
            2 => SelectorDecision::Forwarded,
            _ => SelectorDecision::Unknown,
        }
    }
}

impl fmt::Display for SelectorDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SelectorDecision::Missing => "MISSING",
            SelectorDecision::Dropped => "DROPPED",
            SelectorDecision::Forwarded => "FORWARDED",
            SelectorDecision::Unknown => "UNKNOWN",
        })
    }
}

// ----------------------------------------------------------------------

type OnExpectEntityChanged = Box<dyn Fn(u64, SelectorDecision) + Send + Sync>;

pub struct SelectorDecisionCache {
    initialized: bool,
    base: u64,
    last: u64,
    masks: Vec<u64>,
    num_entries: u64,
    num_nack_entries: u64,

    on_expect_entity_changed: HashMap<u64, Vec<OnExpectEntityChanged>>,
}

impl SelectorDecisionCache {
    #[allow(clippy::manual_div_ceil)] // the rounding as the original has it
    pub fn new(max_num_elements: u64, num_nack_entries: u64) -> Self {
        let num_elements = (max_num_elements.wrapping_mul(2).wrapping_add(63)) / 64;
        Self {
            initialized: false,
            base: 0,
            last: 0,
            masks: vec![0; num_elements as usize],
            num_entries: num_elements.wrapping_mul(32), // 2 bits per entry
            num_nack_entries,
            on_expect_entity_changed: HashMap::new(),
        }
    }

    pub fn add_forwarded(&mut self, entity: u64) {
        self.add_entity(entity, SelectorDecision::Forwarded);
    }

    pub fn add_dropped(&mut self, entity: u64) {
        self.add_entity(entity, SelectorDecision::Dropped);
    }

    pub fn get_decision(&self, entity: u64) -> Result<SelectorDecision, Error> {
        if !self.initialized || entity < self.base {
            return Ok(SelectorDecision::Missing);
        }

        if entity > self.last {
            return Ok(SelectorDecision::Unknown);
        }

        let offset = self.last.wrapping_sub(entity);
        if offset >= self.num_entries {
            // asking for something too old
            return Err(Error::TooOld {
                oldest: self.last.wrapping_sub(self.num_entries).wrapping_add(1),
                asking: entity,
            });
        }

        Ok(self.get_entity(entity))
    }

    pub fn expect_decision(
        &mut self,
        entity: u64,
        f: impl Fn(u64, SelectorDecision) + Send + Sync + 'static,
    ) -> bool {
        if !self.initialized || entity < self.base {
            return false;
        }

        if entity < self.last {
            let offset = self.last.wrapping_sub(entity);
            if offset >= self.num_entries {
                return false; // too old
            }
        }

        self.on_expect_entity_changed
            .entry(entity)
            .or_default()
            .push(Box::new(f));
        true
    }

    fn add_entity(&mut self, entity: u64, sd: SelectorDecision) {
        if !self.initialized {
            self.initialized = true;
            self.base = entity;
            self.last = entity;
            self.set_entity(entity, sd);
            return;
        }

        if entity <= self.base {
            // before base, too old
            return;
        }

        if entity <= self.last {
            self.set_entity(entity, sd);
            return;
        }

        // only the last numEntries entities fit in the ring
        let mut floor = 0u64;
        if entity > self.num_entries {
            floor = entity - self.num_entries;
        }

        let mut e = self.last.wrapping_add(1).max(floor);
        while e != entity {
            self.set_entity(e, SelectorDecision::Unknown);
            e = e.wrapping_add(1);
        }

        // update [last+1-nack, entity-nack) to missing
        let mut missing_start = self.last;
        if missing_start > self.num_nack_entries.wrapping_add(self.base) {
            missing_start = missing_start.wrapping_sub(self.num_nack_entries);
        } else {
            missing_start = self.base;
        }
        let mut missing_end = entity;
        if missing_end > self.num_nack_entries.wrapping_add(self.base) {
            missing_end = missing_end.wrapping_sub(self.num_nack_entries);
        } else {
            missing_end = self.base;
        }
        if missing_end > missing_start.wrapping_add(self.num_entries) {
            missing_start = missing_end.wrapping_sub(self.num_entries);
        }
        if missing_end > missing_start {
            let mut e = missing_start;
            while e != missing_end {
                self.set_entity_if_unknown(e, SelectorDecision::Missing);
                e = e.wrapping_add(1);
            }
        }

        self.set_entity(entity, sd);
        self.last = entity;

        // The original deletes from the map while it ranges over it; the order in which the
        // entities come is not defined there either.
        let too_old: Vec<u64> = self
            .on_expect_entity_changed
            .keys()
            .copied()
            .filter(|e| e.wrapping_add(self.num_entries) < self.last)
            .collect();
        for e in too_old {
            if let Some(fns) = self.on_expect_entity_changed.remove(&e) {
                for f in fns {
                    f(e, SelectorDecision::Missing);
                }
            }
        }
    }

    fn set_entity_if_unknown(&mut self, entity: u64, sd: SelectorDecision) {
        if self.get_entity(entity) == SelectorDecision::Unknown {
            self.set_entity(entity, sd);
        }
    }

    fn set_entity(&mut self, entity: u64, sd: SelectorDecision) {
        let (index, bitpos) = self.get_pos(entity);
        self.masks[index] &= !(0x3 << bitpos); // clear before bitwise OR
        self.masks[index] |= (sd as u64 & 0x3) << bitpos;

        if sd != SelectorDecision::Unknown
            && let Some(fns) = self.on_expect_entity_changed.remove(&entity)
        {
            for f in fns {
                f(entity, sd);
            }
        }
    }

    fn get_entity(&self, entity: u64) -> SelectorDecision {
        let (index, bitpos) = self.get_pos(entity);
        SelectorDecision::from_bits((self.masks[index] >> bitpos) & 0x3)
    }

    fn get_pos(&self, entity: u64) -> (usize, u32) {
        // 2 bits per entity, a uint64 mask can hold 32 entities
        let offset = entity.wrapping_sub(self.base) % self.num_entries;
        ((offset >> 5) as usize, (offset & 0x1F) as u32 * 2)
    }
}

#[cfg(test)]
#[path = "selectordecisioncache_test.rs"]
mod selectordecisioncache_test;
