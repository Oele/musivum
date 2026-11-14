// Ported from LiveKit's protocol, utils/timed_aggregator_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Two assertions of the original cannot fail: it calls
// `require.Error(t, ErrAnachronousSample, <result>)`, which checks that `ErrAnachronousSample`
// is an error and takes the result as the message. Here they check the result. The tests after
// `test_timed_aggregator` are new and say so.

use super::*;
use crate::go::time::{MILLISECOND, SECOND};

#[test]
fn test_timed_aggregator() {
    // functions
    {
        let ta = TimedAggregator::<f64>::new(TimedAggregatorParams::default());

        let (aggregate, aggregate_duration) = ta.get_aggregate();
        assert_eq!(0.0, aggregate);
        assert_eq!(Duration(0), aggregate_duration);
        assert_eq!(0.0, ta.get_average());

        let now = time::now();
        assert_eq!(Ok(()), ta.add_sample_at(1.0, now));
        // nothing to aggregate with just one sample
        let (aggregate, aggregate_duration) = ta.get_aggregate();
        assert_eq!(0.0, aggregate);
        assert_eq!(Duration(0), aggregate_duration);
        assert_eq!(0.0, ta.get_average());

        assert_eq!(Ok(()), ta.add_sample_at(2.0, now.add(500 * MILLISECOND)));
        // second sample added should make stats available
        let (aggregate, aggregate_duration) = ta.get_aggregate();
        assert_eq!(0.5, aggregate);
        assert_eq!(500 * MILLISECOND, aggregate_duration);
        assert_eq!(1.0, ta.get_average());

        // cannot add anachronous sample
        assert_eq!(
            Err(Error::AnachronousSample),
            ta.add_sample_at(10.0, now.add(200 * MILLISECOND))
        );

        // cannot get aggregate or average at an older time
        let res = ta.get_aggregate_at(now.add(200 * MILLISECOND));
        assert_eq!(Err(Error::AnachronousSample), res);

        // check a bit later, last value should continue
        // 1.0 (0.5s) +  2.0 (1.0s)
        let res = ta.get_aggregate_at(now.add(1500 * MILLISECOND));
        assert_eq!(Ok((2.5, 1500 * MILLISECOND)), res);

        assert_eq!(2.5 / 1.5, ta.get_average());

        // another second out and restart
        // 1.0 (0.5s) +  2.0 (2.0s)
        let res = ta.get_aggregate_and_restart_at(now.add(2500 * MILLISECOND));
        assert_eq!(Ok((4.5, 2500 * MILLISECOND)), res);

        // restart should have reset the aggregates
        let (aggregate, aggregate_duration) = ta.get_aggregate();
        assert_eq!(0.0, aggregate);
        assert_eq!(Duration(0), aggregate_duration);

        assert_eq!(0.0, ta.get_average());

        // add a sample a bit later and check aggregates
        assert_eq!(Ok(()), ta.add_sample_at(20.0, now.add(2800 * MILLISECOND)));

        // 2.0 (0.3s) + 20.0 (0.2s)
        let res = ta.get_aggregate_at(now.add(3000 * MILLISECOND));
        assert_eq!(Ok((4.6, 500 * MILLISECOND)), res);

        assert_eq!(4.6 / 0.5, ta.get_average());

        // get average a bit later
        // 2.0 (0.3s) +  20.0 (0.5s)
        let average = ta.get_average_at(now.add(3300 * MILLISECOND));
        assert_eq!(Ok(10.6f64 / 0.8f64), average);

        let (aggregate, aggregate_duration) = ta.get_aggregate();
        assert_eq!(10.6, aggregate);
        assert_eq!(800 * MILLISECOND, aggregate_duration);

        // get average and restart a bit later
        // 2.0 (0.3s) +  20.0 (1.0s)
        let average = ta.get_average_and_restart_at(now.add(3800 * MILLISECOND));
        assert_eq!(Ok(20.6f64 / 1.3f64), average);

        // restart should have reset the aggregates
        let (aggregate, aggregate_duration) = ta.get_aggregate();
        assert_eq!(0.0, aggregate);
        assert_eq!(Duration(0), aggregate_duration);

        // add a negative value sample
        assert_eq!(Ok(()), ta.add_sample_at(-2.0, now.add(4000 * MILLISECOND)));

        // get average a bit later
        // 20.0 (0.2s) +  -2.0 (0.5s)
        let average = ta.get_average_at(now.add(4500 * MILLISECOND));
        assert_eq!(Ok(3.0f64 / 0.7f64), average);

        let (aggregate, aggregate_duration) = ta.get_aggregate();
        assert_eq!(3.0, aggregate);
        assert_eq!(700 * MILLISECOND, aggregate_duration);
    }

    // negative_values
    {
        let ta = TimedAggregator::<i64>::new(TimedAggregatorParams {
            cap_negative_values: true,
        });

        let now = time::now();
        assert_eq!(Ok(()), ta.add_sample_at(1, now));
        assert_eq!(Ok(()), ta.add_sample_at(-1, now.add(SECOND)));
        assert_eq!(Ok(()), ta.add_sample_at(1, now.add(2 * SECOND)));

        // 1 (1.0s) + 0 (capped value) (1.0s) + 1 (1.0s)
        let res = ta.get_aggregate_at(now.add(3 * SECOND));
        assert_eq!(Ok((2i64, 3 * SECOND)), res);

        assert_eq!(2.0f64 / 3.0f64, ta.get_average());
    }
}

// New for musivum: what the original's test leaves open (values run at the pin): a restart
// that is not part of reading the aggregate, a restart at an older time after a read that
// failed, and a reset.
#[test]
fn a_restart_starts_from_the_time_it_is_given() {
    let now = Time::unix(1_700_000_000, 0);
    let ta = TimedAggregator::<f64>::new(TimedAggregatorParams::default());

    // no samples yet, nothing to restart
    ta.restart_at(now);
    assert_eq!(Ok((0.0, Duration(0))), ta.get_aggregate_at(now.add(SECOND)));

    assert_eq!(Ok(()), ta.add_sample_at(1.0, now));
    ta.restart_at(now.add(SECOND));
    // 1.0 (0.5s), the second before the restart does not count
    assert_eq!(
        Ok((0.5, 500 * MILLISECOND)),
        ta.get_aggregate_at(now.add(1500 * MILLISECOND))
    );
    assert_eq!(1.0, ta.get_average());

    // an older time: nothing is read, and a restart goes back to it all the same
    assert_eq!(
        Err(Error::AnachronousSample),
        ta.get_average_at(now.add(SECOND))
    );
    assert_eq!(
        Err(Error::AnachronousSample),
        ta.get_average_and_restart_at(now.add(SECOND))
    );
    assert_eq!((0.0, Duration(0)), ta.get_aggregate());
    assert_eq!(
        Err(Error::AnachronousSample),
        ta.get_aggregate_and_restart_at(now.add(500 * MILLISECOND))
    );
    // 1.0 (1.5s), from the time of the last restart
    assert_eq!(
        Ok((1.5, 1500 * MILLISECOND)),
        ta.get_aggregate_at(now.add(2 * SECOND))
    );

    ta.reset();
    assert_eq!(
        Ok((0.0, Duration(0))),
        ta.get_aggregate_at(now.add(5 * SECOND))
    );
    assert_eq!(0.0, ta.get_average());
}

// New for musivum: with whole numbers, what a sample adds is cut off towards zero (values
// run at the pin).
#[test]
fn whole_numbers_are_cut_off_towards_zero() {
    let now = Time::unix(1_700_000_000, 0);
    let ta = TimedAggregator::<i64>::new(TimedAggregatorParams::default());

    assert_eq!(Ok(()), ta.add_sample_at(3, now));
    // 3 (1.5s) is 4.5
    assert_eq!(
        Ok((4, 1500 * MILLISECOND)),
        ta.get_aggregate_at(now.add(1500 * MILLISECOND))
    );
    assert_eq!(4.0 / 1.5, ta.get_average());

    assert_eq!(Ok(()), ta.add_sample_at(-3, now.add(1500 * MILLISECOND)));
    // -3 (0.5s) is -1.5
    assert_eq!(
        Ok((3, 2 * SECOND)),
        ta.get_aggregate_at(now.add(2 * SECOND))
    );
    assert_eq!(1.5, ta.get_average());
}
