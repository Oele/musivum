// Ported from LiveKit's protocol, utils/timed_aggregator.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the type constraint is a trait; the error of the
// package is an enum.

use parking_lot::RwLock;

use crate::go::time::{self, Duration, Time};

// ------------------------------------------------

/// The errors of the package `utils`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("anachronous sample")]
    AnachronousSample,
}

// ------------------------------------------------

/// `int64 | float64` upstream.
pub trait TimedAggregatorNumber: Copy + PartialOrd + Default + Send + Sync {
    fn to_f64(self) -> f64;
    /// Go's conversion `T(v)`.
    fn from_f64(v: f64) -> Self;
    /// Go's `+` of the type: wrapping for the integer.
    fn add(self, rhs: Self) -> Self;
}

impl TimedAggregatorNumber for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(v: f64) -> Self {
        v as i64
    }
    fn add(self, rhs: Self) -> Self {
        self.wrapping_add(rhs)
    }
}

impl TimedAggregatorNumber for f64 {
    fn to_f64(self) -> f64 {
        self
    }
    fn from_f64(v: f64) -> Self {
        v
    }
    fn add(self, rhs: Self) -> Self {
        self + rhs
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct TimedAggregatorParams {
    pub cap_negative_values: bool,
}

pub struct TimedAggregator<T: TimedAggregatorNumber> {
    params: TimedAggregatorParams,

    lock: RwLock<TimedAggregatorInner<T>>,
}

#[derive(Default)]
struct TimedAggregatorInner<T: TimedAggregatorNumber> {
    last_sample: T,
    last_sample_at: Time,
    aggregate: T,
    aggregate_duration: Duration,
}

impl<T: TimedAggregatorNumber> TimedAggregator<T> {
    pub fn new(params: TimedAggregatorParams) -> Self {
        Self {
            params,
            lock: RwLock::new(TimedAggregatorInner::default()),
        }
    }

    pub fn add_sample_at(&self, mut val: T, at: Time) -> Result<(), Error> {
        let mut t = self.lock.write();

        if val < T::default() && self.params.cap_negative_values {
            val = T::default();
        }

        t.add_sample_at_locked(val, at)
    }

    pub fn add_sample(&self, val: T) -> Result<(), Error> {
        self.add_sample_at(val, time::now())
    }

    pub fn get_aggregate(&self) -> (T, Duration) {
        let t = self.lock.read();

        (t.aggregate, t.aggregate_duration)
    }

    pub fn get_aggregate_at(&self, at: Time) -> Result<(T, Duration), Error> {
        let mut t = self.lock.write();

        t.get_aggregate_at_locked(at)
    }

    pub fn get_aggregate_and_restart_at(&self, at: Time) -> Result<(T, Duration), Error> {
        let mut t = self.lock.write();

        let res = t.get_aggregate_at_locked(at);
        t.restart_at_locked(at);
        res
    }

    pub fn get_average(&self) -> f64 {
        let t = self.lock.read();

        t.get_average_locked()
    }

    pub fn get_average_at(&self, at: Time) -> Result<f64, Error> {
        let mut t = self.lock.write();

        t.get_average_at_locked(at)
    }

    pub fn get_average_and_restart_at(&self, at: Time) -> Result<f64, Error> {
        let mut t = self.lock.write();

        let res = t.get_average_at_locked(at);
        t.restart_at_locked(at);
        res
    }

    pub fn reset(&self) {
        let mut t = self.lock.write();

        t.last_sample = T::default();
        t.last_sample_at = Time::ZERO;
        t.aggregate = T::default();
        t.aggregate_duration = Duration::ZERO;
    }

    pub fn restart_at(&self, at: Time) {
        let mut t = self.lock.write();

        t.restart_at_locked(at);
    }

    pub fn restart(&self) {
        self.restart_at(time::now());
    }
}

impl<T: TimedAggregatorNumber> TimedAggregatorInner<T> {
    fn add_sample_at_locked(&mut self, val: T, at: Time) -> Result<(), Error> {
        let mut since_last = Duration::ZERO;
        if !self.last_sample_at.is_zero() {
            if self.last_sample_at.after(at) {
                return Err(Error::AnachronousSample);
            }

            since_last = at.sub(self.last_sample_at);
        }
        let last_val = self.last_sample;

        self.last_sample = val;
        self.last_sample_at = at;

        self.aggregate = self
            .aggregate
            .add(T::from_f64(since_last.seconds() * last_val.to_f64()));
        self.aggregate_duration += since_last;
        Ok(())
    }

    fn get_aggregate_at_locked(&mut self, at: Time) -> Result<(T, Duration), Error> {
        if !self.last_sample_at.is_zero() {
            // re-add last sample at given time
            if self.add_sample_at_locked(self.last_sample, at).is_err() {
                return Err(Error::AnachronousSample);
            }
        }

        Ok((self.aggregate, self.aggregate_duration))
    }

    fn get_average_locked(&self) -> f64 {
        let seconds = self.aggregate_duration.seconds();
        if seconds == 0.0 {
            return 0.0;
        }

        self.aggregate.to_f64() / seconds
    }

    fn get_average_at_locked(&mut self, at: Time) -> Result<f64, Error> {
        if !self.last_sample_at.is_zero() {
            // re-add last sample at given time
            self.add_sample_at_locked(self.last_sample, at)?;
        }

        Ok(self.get_average_locked())
    }

    fn restart_at_locked(&mut self, at: Time) {
        if self.last_sample_at.is_zero() {
            // no samples yet, nothing to restart
            return;
        }

        self.last_sample_at = at;
        self.aggregate = T::default();
        self.aggregate_duration = Duration::ZERO;
    }
}

#[cfg(test)]
#[path = "timed_aggregator_test.rs"]
mod timed_aggregator_test;
