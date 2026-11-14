// Ported from LiveKit, pkg/sfu/videolayerselector/selectordecisioncache_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the function given to `expect_decision` writes to a
// list behind a lock, since it has to be `Send + Sync`; `require.NoError` followed by
// `require.Equal` on the decision is one comparison with `Ok(..)`; the time bound of
// `test_selector_decision_cache_jump_is_bounded` is ten seconds in a build without
// optimisation, see there.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::*;

#[test]
fn test_selector_decision_cache_large_jump() {
    let mut s = SelectorDecisionCache::new(256, 80);
    s.add_forwarded(10);
    s.add_forwarded(11);

    let expected: Arc<Mutex<Vec<SelectorDecision>>> = Arc::new(Mutex::new(Vec::new()));
    let expected_in_fn = expected.clone();
    assert!(s.expect_decision(12, move |_, sd| {
        expected_in_fn.lock().push(sd);
    }));

    let jump_to = 11u64 + 65535;
    s.add_dropped(jump_to);

    // skipped entity that was expected is reported missing
    assert_eq!(vec![SelectorDecision::Missing], *expected.lock());

    let sd = s.get_decision(jump_to);
    assert_eq!(Ok(SelectorDecision::Dropped), sd);

    // the missing range wraps the whole ring, so every other slot reads missing
    for e in jump_to - s.num_entries + 1..jump_to {
        let sd = s.get_decision(e);
        assert_eq!(Ok(SelectorDecision::Missing), sd, "entity {e}");
    }

    let err = s.get_decision(jump_to - s.num_entries);
    assert!(err.is_err());
}

#[test]
fn test_selector_decision_cache_small_jump() {
    let mut s = SelectorDecisionCache::new(256, 80);
    s.add_forwarded(1000);
    s.add_dropped(1100);

    // within nack window -> unknown, older skipped -> missing
    for e in 1001u64..1100 {
        let sd = s.get_decision(e);
        if e >= 1100 - 80 {
            assert_eq!(Ok(SelectorDecision::Unknown), sd, "entity {e}");
        } else {
            assert_eq!(Ok(SelectorDecision::Missing), sd, "entity {e}");
        }
    }
}

#[test]
fn test_selector_decision_cache_jump_is_bounded() {
    let mut s = SelectorDecisionCache::new(256, 80);
    let mut entity = 1u64;
    s.add_forwarded(entity);

    // uncapped, each jump walks ~2x the gap (seconds in total), capped it walks ~2x the ring
    let start = Instant::now();
    for _ in 0..10000 {
        entity += 65535;
        s.add_dropped(entity);
    }
    // The original's bound is one second, for optimised code. A build without optimisation
    // takes about 0.4 s here on an idle machine and went over the second on a loaded one, so
    // it gets ten: uncapped, the loop walks 256 times as far and takes about 100 s.
    let bound = if cfg!(debug_assertions) { 10 } else { 1 };
    assert!(start.elapsed() < Duration::from_secs(bound));
}
