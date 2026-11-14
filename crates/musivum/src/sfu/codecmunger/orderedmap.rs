//! A map that remembers the order in which its keys were first set, so that the oldest can be
//! taken out when it has grown too large.
//!
//! It stands in for the Go package `github.com/elliotchance/orderedmap/v3`, which `vp8.go`
//! keeps its picture id caches in, and has the calls that file makes: `get`, `set`, `len`,
//! and `delete_front` for the original's `Front()` followed by `Delete(el.Key)`. As there,
//! setting a key that is already in the map replaces its value and leaves its place in the
//! order alone, and taking out the oldest costs the same however many keys there are.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

#[derive(Clone, Debug)]
pub(super) struct OrderedMap<K, V> {
    kv: HashMap<K, V>,
    // the keys, oldest first
    ll: VecDeque<K>,
}

impl<K: Copy + Eq + Hash, V: Copy + Default> OrderedMap<K, V> {
    pub(super) fn new() -> Self {
        Self {
            kv: HashMap::new(),
            ll: VecDeque::new(),
        }
    }

    /// The value of a key and whether the key is there. The value is the zero value when it
    /// is not, as Go's `value, ok := m.Get(key)` has it.
    pub(super) fn get(&self, key: K) -> (V, bool) {
        match self.kv.get(&key) {
            Some(value) => (*value, true),
            None => (V::default(), false),
        }
    }

    /// Sets the value of a key. A new key becomes the newest; a key that is already there
    /// keeps its place.
    pub(super) fn set(&mut self, key: K, value: V) {
        if let Some(existing) = self.kv.get_mut(&key) {
            *existing = value;
            return;
        }

        self.ll.push_back(key);
        self.kv.insert(key, value);
    }

    pub(super) fn len(&self) -> usize {
        self.kv.len()
    }

    /// Takes out the oldest key.
    pub(super) fn delete_front(&mut self) {
        if let Some(key) = self.ll.pop_front() {
            self.kv.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_stay_in_the_order_they_were_first_set() {
        let mut m = OrderedMap::<i32, i32>::new();
        assert_eq!(0, m.len());
        assert_eq!((0, false), m.get(7));

        m.set(7, 70);
        m.set(3, 30);
        m.set(9, 90);
        assert_eq!(3, m.len());
        assert_eq!((30, true), m.get(3));

        // a key that is there keeps its place and gets the new value
        m.set(7, 71);
        assert_eq!(3, m.len());
        assert_eq!((71, true), m.get(7));
        assert_eq!(vec![7, 3, 9], m.ll.iter().copied().collect::<Vec<_>>());

        // the oldest goes first
        m.delete_front();
        assert_eq!((0, false), m.get(7));
        assert_eq!(2, m.len());
        assert_eq!(vec![3, 9], m.ll.iter().copied().collect::<Vec<_>>());

        // a key that was taken out is the newest when it is set again
        m.set(7, 72);
        assert_eq!(vec![3, 9, 7], m.ll.iter().copied().collect::<Vec<_>>());

        m.delete_front();
        m.delete_front();
        m.delete_front();
        assert_eq!(0, m.len());
        assert_eq!((0, false), m.get(7));

        // nothing to take out of an empty map
        m.delete_front();
        assert_eq!(0, m.len());
    }
}
