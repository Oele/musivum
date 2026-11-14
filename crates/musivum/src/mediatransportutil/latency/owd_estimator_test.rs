// New for musivum: the original has no test for owd_estimator.go. The expected values are the
// original's: they were printed by a Go test that feeds these same series to the estimator of
// mediatransportutil at v0.0.0-20261014192331-dd8e31a0adc0 and reads its fields after every
// update.

use super::*;

const MS: i64 = 1_000_000;
const SEC: i64 = 1_000_000_000;
// The sender's clock at the first report.
const BASE: i64 = 1_700_000_000 * SEC;

// A steady series, then one that drifts, then one that falls back, then a jump: one report a
// second through all of it, as the original was fed.
#[test]
fn a_steady_and_a_drifting_series() {
    let mut o = OwdEstimator::new(OWD_ESTIMATOR_PARAMS_DEFAULT);
    let mut sender = BASE;

    // Steady: every report takes 50 ms. The first one sets the estimate and counts as a step
    // change, the others leave it where it is.
    for i in 0..5 {
        let (est, step) = o.update(sender, sender + 50 * MS);
        assert_eq!((50 * MS, i == 0), (est, step), "steady {i}");
        assert_eq!(50 * MS, o.estimated_propagation_delay(), "steady {i}");
        assert_eq!(0, o.last_delta_propagation_delay_ns, "steady {i}");
        assert_eq!(0, o.long_term_delta_propagation_delay_ns, "steady {i}");
        sender += SEC;
    }

    // Drifting: every report takes half a millisecond longer than the one before. The estimate
    // follows with a tenth of what it is behind, and the long term delta grows with it.
    let expected: [(i64, i64); 20] = [
        (50050000, 500000),
        (50145000, 545000),
        (50280500, 626000),
        (50452450, 735350),
        (50657205, 866570),
        (50891484, 1014192),
        (51152335, 1173624),
        (51437101, 1341028),
        (51743390, 1513215),
        (52069051, 1687554),
        (52412145, 1861893),
        (52770930, 2034489),
        (53143837, 2203947),
        (53529453, 2369168),
        (53926507, 2529305),
        (54333856, 2683723),
        (54750470, 2831965),
        (55175423, 2973721),
        (55607880, 3108806),
        (56047092, 3237137),
    ];
    for (i, (expected_est, expected_long_term)) in expected.iter().enumerate() {
        let k = i as i64 + 1;
        let (est, step) = o.update(sender, sender + 50 * MS + k * MS / 2);
        assert_eq!((*expected_est, false), (est, step), "drift {k}");
        assert_eq!(
            *expected_long_term, o.long_term_delta_propagation_delay_ns,
            "drift {k}"
        );
        assert_eq!(0, o.propagation_delay_delta_high_count, "drift {k}");
        sender += SEC;
    }

    // Falling back to 45 ms: the estimate goes nine tenths of the way with every report.
    let expected: [(i64, i64, i64); 3] = [
        (46104710, -11047092, 1808715),
        (45110471, -1104710, 1517373),
        (45011048, -110471, 1354589),
    ];
    for (i, (expected_est, expected_delta, expected_long_term)) in expected.iter().enumerate() {
        let (est, step) = o.update(sender, sender + 45 * MS);
        assert_eq!((*expected_est, false), (est, step), "fall {i}");
        assert_eq!(
            *expected_delta, o.last_delta_propagation_delay_ns,
            "fall {i}"
        );
        assert_eq!(
            *expected_long_term, o.long_term_delta_propagation_delay_ns,
            "fall {i}"
        );
        sender += SEC;
    }

    // A jump of 100 ms: more than the threshold and more than twice the long term delta, so it
    // is counted as a possible path change and the estimate is not moved. It would be taken
    // over after the ten seconds of the default parameters, which do not pass here.
    for i in 0..3i64 {
        let (est, step) = o.update(sender, sender + 145 * MS);
        assert_eq!((45011048, false), (est, step), "jump {i}");
        assert_eq!(99988952, o.last_delta_propagation_delay_ns, "jump {i}");
        assert_eq!(1354589, o.long_term_delta_propagation_delay_ns, "jump {i}");
        assert_eq!(i + 1, o.propagation_delay_delta_high_count, "jump {i}");
        assert!(
            !o.propagation_delay_delta_high_start_time.is_zero(),
            "jump {i}"
        );
        assert_eq!(145 * MS, o.propagation_delay_spike_ns, "jump {i}");
        sender += SEC;
    }

    // What `MarshalLogObject` writes upstream.
    let logged = format!("{o:?}");
    for field in [
        "lastSenderClockTimeNs: 1700000030.000000000",
        "lastPropagationDelayNs: 145ms",
        "lastDeltaPropagationDelayNs: 99.988952ms",
        "estimatedPropagationDelayNs: 45.011048ms",
        "longTermDeltaPropagationDelayNs: 1.354589ms",
        "propagationDelayDeltaHighCount: 3",
        "propagationDelayDeltaHighStartTime: ",
        "propagationDelaySpikeNs: 145ms",
    ] {
        assert!(logged.contains(field), "{field} in {logged}");
    }
}

// A fall from a steady delay: the delta is negative, and the long term delta, which would
// take it over, stays at zero.
#[test]
fn a_fall_leaves_no_negative_long_term_delta() {
    let mut o = OwdEstimator::new(OWD_ESTIMATOR_PARAMS_DEFAULT);

    assert_eq!((50 * MS, true), o.update(BASE, BASE + 50 * MS));

    assert_eq!(
        (45500000, false),
        o.update(BASE + SEC, BASE + SEC + 45 * MS)
    );
    assert_eq!(-5 * MS, o.last_delta_propagation_delay_ns);
    assert_eq!(0, o.long_term_delta_propagation_delay_ns);

    assert_eq!(
        (45050000, false),
        o.update(BASE + 2 * SEC, BASE + 2 * SEC + 45 * MS)
    );
    assert_eq!(-500000, o.last_delta_propagation_delay_ns);
    assert_eq!(0, o.long_term_delta_propagation_delay_ns);
}

// A change of path: the delay jumps and stays. With a reset wait of a millisecond instead of
// ten seconds the second high report, some milliseconds later, takes the new delay over.
#[test]
fn a_jump_that_lasts_becomes_the_estimate() {
    let mut params = OWD_ESTIMATOR_PARAMS_DEFAULT;
    params.propagation_delay_delta_high_reset_wait = time::MILLISECOND;
    let mut o = OwdEstimator::new(params);
    let mut sender = BASE;

    assert_eq!((50 * MS, true), o.update(sender, sender + 50 * MS));
    sender += SEC;

    // Without a long term delta nothing counts as a jump: a small rise gives it one.
    assert_eq!((50200000, false), o.update(sender, sender + 52 * MS));
    assert_eq!(2 * MS, o.long_term_delta_propagation_delay_ns);
    sender += SEC;

    assert_eq!((50200000, false), o.update(sender, sender + 150 * MS));
    assert_eq!(1, o.propagation_delay_delta_high_count);
    assert!(!o.propagation_delay_delta_high_start_time.is_zero());
    assert_eq!(150 * MS, o.propagation_delay_spike_ns);
    assert_eq!(2 * MS, o.long_term_delta_propagation_delay_ns);
    sender += SEC;

    std::thread::sleep(std::time::Duration::from_millis(5));
    // The spike moves half of the way to the new report, and is the new estimate.
    assert_eq!((151 * MS, true), o.update(sender, sender + 152 * MS));
    assert_eq!(0, o.propagation_delay_delta_high_count);
    assert!(o.propagation_delay_delta_high_start_time.is_zero());
    assert_eq!(0, o.propagation_delay_spike_ns);
    assert_eq!(0, o.long_term_delta_propagation_delay_ns);
    sender += SEC;

    assert_eq!((151100000, false), o.update(sender, sender + 152 * MS));
    assert_eq!(MS, o.long_term_delta_propagation_delay_ns);
}

#[test]
fn the_initial_adjustment_is_made_once_and_only_downwards() {
    let new = || {
        let mut o = OwdEstimator::new(OWD_ESTIMATOR_PARAMS_DEFAULT);
        o.update(BASE, BASE + 50 * MS);
        o
    };

    let mut o = new();
    assert_eq!(40 * MS, o.initial_adjustment(-10 * MS));
    assert_eq!(40 * MS, o.initial_adjustment(-10 * MS));
    assert_eq!(40 * MS, o.estimated_propagation_delay());

    let mut o = new();
    assert_eq!(50 * MS, o.initial_adjustment(5 * MS));
    assert_eq!(50 * MS, o.estimated_propagation_delay());

    // Not by as much as the estimate or more.
    let mut o = new();
    assert_eq!(50 * MS, o.initial_adjustment(-60 * MS));
    assert_eq!(50 * MS, o.estimated_propagation_delay());

    let mut o = new();
    assert_eq!(50 * MS, o.initial_adjustment(-50 * MS));
    assert_eq!(50 * MS, o.estimated_propagation_delay());
}
