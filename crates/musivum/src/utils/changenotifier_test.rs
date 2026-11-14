// Ported from LiveKit, pkg/utils/changenotifier_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the wait group and the channel the test waits on are
// one channel that each observer writes to; two notifiers are the same one when they are the
// same allocation.

use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_change_notifier() {
    // Observer management
    {
        let notifier = ChangeNotifier::new();
        assert!(!notifier.has_observers());

        let called = Arc::new(AtomicBool::new(false));
        let c = called.clone();
        notifier.add_observer("test-key", move || {
            c.store(true, Ordering::SeqCst);
        });
        assert!(notifier.has_observers());

        notifier.remove_observer("test-key");
        assert!(!notifier.has_observers());
        assert!(!called.load(Ordering::SeqCst));
    }

    // Notification triggers callbacks asynchronously
    {
        let notifier = ChangeNotifier::new();
        let (wg, mut done) = tokio::sync::mpsc::unbounded_channel();

        let call_counts = Arc::new(Mutex::new(HashMap::<&'static str, i32>::new()));

        let (mu, wg1) = (call_counts.clone(), wg.clone());
        notifier.add_observer("obs1", move || {
            *mu.lock().entry("obs1").or_default() += 1;
            let _ = wg1.send(());
        });

        let (mu, wg2) = (call_counts.clone(), wg.clone());
        notifier.add_observer("obs2", move || {
            *mu.lock().entry("obs2").or_default() += 1;
            let _ = wg2.send(());
        });

        notifier.notify_changed();

        // Wait for async execution of observers
        for _ in 0..2 {
            tokio::time::timeout(std::time::Duration::from_secs(1), done.recv())
                .await
                .expect("Timeout waiting for change notification callbacks");
        }

        let counts = call_counts.lock();
        assert_eq!(Some(&1), counts.get("obs1"));
        assert_eq!(Some(&1), counts.get("obs2"));
    }
}

#[test]
fn test_change_notifier_manager() {
    // Get and Create Notifiers
    {
        let manager = ChangeNotifierManager::new();
        assert!(manager.get_notifier("non-existent").is_none());

        let notifier = manager.get_or_create_notifier("room1");

        let retrieved = manager.get_notifier("room1").unwrap();
        assert!(Arc::ptr_eq(&notifier, &retrieved));

        // GetOrCreate should return the existing one
        let again = manager.get_or_create_notifier("room1");
        assert!(Arc::ptr_eq(&notifier, &again));
    }

    // Remove Notifiers with HasObservers check
    {
        let manager = ChangeNotifierManager::new();
        let _ = manager.get_or_create_notifier("room1");

        // Case 1: notifier has no observers, should be removed
        manager.remove_notifier("room1", false);
        assert!(manager.get_notifier("room1").is_none());

        // Re-create and add an observer
        let notifier = manager.get_or_create_notifier("room1");
        notifier.add_observer("observer", || {});

        // Case 2: notifier has observer, RemoveNotifier(..., false) should not remove it
        manager.remove_notifier("room1", false);
        assert!(manager.get_notifier("room1").is_some());

        // Case 3: notifier has observer, RemoveNotifier(..., true) (force) should remove it
        manager.remove_notifier("room1", true);
        assert!(manager.get_notifier("room1").is_none());
    }
}
