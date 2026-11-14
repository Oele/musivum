// Ported from LiveKit's mediatransportutil, pkg/latency/owd_estimator.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the default parameters, a variable upstream, are a
// constant; `MarshalLogObject` is `Debug`, with the keys and the fields of the original; the
// two closures of `Update` are functions inside `update` that take the estimator.

use std::fmt;

use crate::go::time::{self, Duration, Time};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct OwdEstimatorParams {
    pub propagation_delay_fall_factor: f64,
    pub propagation_delay_rise_factor: f64,

    pub propagation_delay_spike_adaptation_factor: f64,

    pub propagation_delay_delta_threshold_min: Duration,
    pub propagation_delay_delta_threshold_max_factor: i64,
    pub propagation_delay_delta_high_reset_num_reports: i64,
    pub propagation_delay_delta_high_reset_wait: Duration,
    pub propagation_delay_delta_long_term_adaptation_threshold: Duration,
}

pub const OWD_ESTIMATOR_PARAMS_DEFAULT: OwdEstimatorParams = OwdEstimatorParams {
    // OWD (One-Way-Delay) Estimator is used to estimate propagation delay between sender and receiver.
    // As they operate on different clock domains, it is not possible to get exact propagation delay easily.
    // So, this module is an estimator using a simple approach explained below. It should not be used for
    // things that require high accuracy.
    //
    // One example is RTCP Sender Reports getting re-based to SFU time base so that all subscriber side
    // can have the same time base (i. e. SFU time base). To convert publisher side
    // RTCP Sender Reports to SFU timebase, a propagation delay is maintained.
    //    propagation_delay = time_of_report_reception - ntp_timestamp_in_report
    //
    // Propagation delay is adapted continuously. If it falls, adapt quickly to the
    // lower value as that could be the real propagation delay. If it rises, adapt slowly
    // as it might be a temporary change or slow drift. See below for handling of high deltas
    // which could be a result of a path change.
    propagation_delay_fall_factor: 0.9,
    propagation_delay_rise_factor: 0.1,

    propagation_delay_spike_adaptation_factor: 0.5,

    // To account for path changes mid-stream, if the delta of the propagation delay is consistently higher, reset.
    // Reset at whichever of the below happens later.
    //   1. 10 seconds of persistent high delta.
    //   2. at least 2 consecutive reports with high delta.
    //
    // A long term estimate of delta of propagation delay is maintained and delta propagation delay exceeding
    // a factor of the long term estimate is considered a sharp increase. That will trigger the start of the
    // path change condition and if it persists, propagation delay will be reset.
    propagation_delay_delta_threshold_min: Duration::from_millis(10),
    propagation_delay_delta_threshold_max_factor: 2,
    propagation_delay_delta_high_reset_num_reports: 2,
    propagation_delay_delta_high_reset_wait: Duration::from_secs(10),
    propagation_delay_delta_long_term_adaptation_threshold: Duration::from_millis(50),
};

#[derive(Default)]
pub struct OwdEstimator {
    params: OwdEstimatorParams,

    initialized: bool,
    initial_adjustment_done: bool,
    last_sender_clock_time_ns: i64,
    last_propagation_delay_ns: i64,
    last_delta_propagation_delay_ns: i64,
    estimated_propagation_delay_ns: i64,
    long_term_delta_propagation_delay_ns: i64,
    propagation_delay_delta_high_count: i64,
    propagation_delay_delta_high_start_time: Time,
    propagation_delay_spike_ns: i64,
}

impl OwdEstimator {
    pub fn new(params: OwdEstimatorParams) -> Self {
        Self {
            params,
            ..Default::default()
        }
    }
}

// MarshalLogObject of the original.
impl fmt::Debug for OwdEstimator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwdEstimator")
            .field(
                "lastSenderClockTimeNs",
                &Time::unix(0, self.last_sender_clock_time_ns),
            )
            .field(
                "lastPropagationDelayNs",
                &Duration(self.last_propagation_delay_ns),
            )
            .field(
                "lastDeltaPropagationDelayNs",
                &Duration(self.last_delta_propagation_delay_ns),
            )
            .field(
                "estimatedPropagationDelayNs",
                &Duration(self.estimated_propagation_delay_ns),
            )
            .field(
                "longTermDeltaPropagationDelayNs",
                &Duration(self.long_term_delta_propagation_delay_ns),
            )
            .field(
                "propagationDelayDeltaHighCount",
                &self.propagation_delay_delta_high_count,
            )
            .field(
                "propagationDelayDeltaHighStartTime",
                &self.propagation_delay_delta_high_start_time,
            )
            .field(
                "propagationDelaySpikeNs",
                &Duration(self.propagation_delay_spike_ns),
            )
            .finish()
    }
}

impl OwdEstimator {
    pub fn update(
        &mut self,
        sender_clock_time_ns: i64,
        receiver_clock_time_ns: i64,
    ) -> (i64, bool) {
        fn reset_delta(o: &mut OwdEstimator) {
            o.propagation_delay_delta_high_count = 0;
            o.propagation_delay_delta_high_start_time = Time::ZERO;
            o.propagation_delay_spike_ns = 0;
        }

        fn init_propagation_delay(o: &mut OwdEstimator, pd: i64) {
            o.estimated_propagation_delay_ns = pd;
            o.long_term_delta_propagation_delay_ns = 0;
            reset_delta(o);
        }

        self.last_propagation_delay_ns = receiver_clock_time_ns.wrapping_sub(sender_clock_time_ns);
        if !self.initialized {
            self.initialized = true;
            self.last_sender_clock_time_ns = sender_clock_time_ns;
            init_propagation_delay(self, self.last_propagation_delay_ns);
            return (self.estimated_propagation_delay_ns, true);
        }

        let mut step_change = false;
        self.last_delta_propagation_delay_ns = self
            .last_propagation_delay_ns
            .wrapping_sub(self.estimated_propagation_delay_ns);
        // check for path changes, i. e. a step jump increase in propagation delay observed over time
        if self.last_delta_propagation_delay_ns
            > self
                .params
                .propagation_delay_delta_threshold_min
                .nanoseconds()
        {
            // ignore small changes for path change consideration
            if self.long_term_delta_propagation_delay_ns != 0
                && self.last_delta_propagation_delay_ns
                    > self
                        .long_term_delta_propagation_delay_ns
                        .wrapping_mul(self.params.propagation_delay_delta_threshold_max_factor)
            {
                self.propagation_delay_delta_high_count += 1;
                if self.propagation_delay_delta_high_start_time.is_zero() {
                    self.propagation_delay_delta_high_start_time = time::now();
                }
                if self.propagation_delay_spike_ns == 0 {
                    self.propagation_delay_spike_ns = self.last_propagation_delay_ns;
                } else {
                    self.propagation_delay_spike_ns = self.propagation_delay_spike_ns.wrapping_add(
                        (self.params.propagation_delay_spike_adaptation_factor
                            * self
                                .last_propagation_delay_ns
                                .wrapping_sub(self.propagation_delay_spike_ns)
                                as f64) as i64,
                    );
                }

                if self.propagation_delay_delta_high_count
                    >= self.params.propagation_delay_delta_high_reset_num_reports
                    && time::since(self.propagation_delay_delta_high_start_time)
                        >= self.params.propagation_delay_delta_high_reset_wait
                {
                    step_change = true;
                    init_propagation_delay(self, self.propagation_delay_spike_ns);
                }
            } else {
                reset_delta(self);
            }
        } else {
            reset_delta(self);

            let mut factor = self.params.propagation_delay_fall_factor;
            if self.last_propagation_delay_ns > self.estimated_propagation_delay_ns {
                factor = self.params.propagation_delay_rise_factor;
            }
            self.estimated_propagation_delay_ns = self.estimated_propagation_delay_ns.wrapping_add(
                (factor
                    * self
                        .last_propagation_delay_ns
                        .wrapping_sub(self.estimated_propagation_delay_ns)
                        as f64) as i64,
            );
        }

        if self.last_delta_propagation_delay_ns
            < self
                .params
                .propagation_delay_delta_long_term_adaptation_threshold
                .nanoseconds()
        {
            if self.long_term_delta_propagation_delay_ns == 0 {
                self.long_term_delta_propagation_delay_ns = self.last_delta_propagation_delay_ns;
            } else {
                // do not adapt to large +ve spikes, can happen when channel is congested and reports are delivered very late
                // if the spike is in fact a path change, it will persist and handled by path change detection above
                let since_last = sender_clock_time_ns.wrapping_sub(self.last_sender_clock_time_ns);
                let quotient = since_last as f64
                    / self
                        .params
                        .propagation_delay_delta_high_reset_wait
                        .nanoseconds() as f64;
                // Go's min, which is NaN when the quotient is.
                let adaptation_factor = if quotient.is_nan() {
                    quotient
                } else {
                    1.0_f64.min(quotient)
                };
                self.long_term_delta_propagation_delay_ns =
                    self.long_term_delta_propagation_delay_ns.wrapping_add(
                        (adaptation_factor
                            * self
                                .last_delta_propagation_delay_ns
                                .wrapping_sub(self.long_term_delta_propagation_delay_ns)
                                as f64) as i64,
                    );
            }
        }
        if self.long_term_delta_propagation_delay_ns < 0 {
            self.long_term_delta_propagation_delay_ns = 0;
        }
        self.last_sender_clock_time_ns = sender_clock_time_ns;
        (self.estimated_propagation_delay_ns, step_change)
    }

    pub fn initial_adjustment(&mut self, adjustment_ns: i64) -> i64 {
        if self.initial_adjustment_done {
            return self.estimated_propagation_delay_ns;
        }

        self.initial_adjustment_done = true;
        // one time adjustment at init
        // example: when this is used to measure one-way-delay of RTCP sender reports,
        // it is possible that the first sender report is delayed and experiences more
        // than existing propagation delay. This allows adjustment of initial estimate.
        if adjustment_ns < 0 && adjustment_ns.wrapping_neg() < self.estimated_propagation_delay_ns {
            self.estimated_propagation_delay_ns = self
                .estimated_propagation_delay_ns
                .wrapping_add(adjustment_ns);
        }
        self.estimated_propagation_delay_ns
    }

    pub fn estimated_propagation_delay(&self) -> i64 {
        self.estimated_propagation_delay_ns
    }
}

#[cfg(test)]
#[path = "owd_estimator_test.rs"]
mod owd_estimator_test;
