// Ported from LiveKit, pkg/utils/changenotifier.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the notifiers are shared, so they are made and handed
// out as `Arc`s; the goroutine that calls the observers is a task on the runtime.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::go::time;

type Observer = Arc<dyn Fn() + Send + Sync>;

pub struct ChangeNotifier {
    lock: Mutex<ChangeNotifierInner>,
}

struct ChangeNotifierInner {
    observers: HashMap<String, Observer>,
}

impl ChangeNotifier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            lock: Mutex::new(ChangeNotifierInner {
                observers: HashMap::new(),
            }),
        })
    }

    pub fn add_observer(&self, key: &str, on_changed: impl Fn() + Send + Sync + 'static) {
        let mut n = self.lock.lock();

        n.observers.insert(key.to_string(), Arc::new(on_changed));
    }

    pub fn remove_observer(&self, key: &str) {
        let mut n = self.lock.lock();

        n.observers.remove(key);
    }

    pub fn has_observers(&self) -> bool {
        let n = self.lock.lock();

        !n.observers.is_empty()
    }

    pub fn notify_changed(&self) {
        let n = self.lock.lock();
        if n.observers.is_empty() {
            return;
        }
        let mut observers = Vec::with_capacity(n.observers.len());
        for f in n.observers.values() {
            observers.push(f.clone());
        }
        drop(n);

        time::spawn(async move {
            for f in observers {
                f();
            }
        });
    }
}

pub struct ChangeNotifierManager {
    lock: Mutex<ChangeNotifierManagerInner>,
}

struct ChangeNotifierManagerInner {
    notifiers: HashMap<String, Arc<ChangeNotifier>>,
}

impl ChangeNotifierManager {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(ChangeNotifierManagerInner {
                notifiers: HashMap::new(),
            }),
        }
    }

    pub fn get_notifier(&self, key: &str) -> Option<Arc<ChangeNotifier>> {
        let m = self.lock.lock();

        m.notifiers.get(key).cloned()
    }

    pub fn get_or_create_notifier(&self, key: &str) -> Arc<ChangeNotifier> {
        let mut m = self.lock.lock();

        if let Some(notifier) = m.notifiers.get(key) {
            return notifier.clone();
        }

        let notifier = ChangeNotifier::new();
        m.notifiers.insert(key.to_string(), notifier.clone());
        notifier
    }

    pub fn remove_notifier(&self, key: &str, force: bool) {
        let mut m = self.lock.lock();

        if let Some(notifier) = m.notifiers.get(key)
            && (force || !notifier.has_observers())
        {
            m.notifiers.remove(key);
        }
    }
}

impl Default for ChangeNotifierManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "changenotifier_test.rs"]
mod changenotifier_test;
