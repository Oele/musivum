// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/twcc_feedback.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `MarshalLogObject` is `Debug`, written to read like a
// derived one, with the value the original gives for the cycles.

use std::fmt;

use crate::go::time::{Duration, Time};
use crate::infow;
use crate::protocol::logger::Logger;
use crate::webrtc::rtcp::transport_feedbacks::transport_layer_cc::TransportLayerCc;

// ------------------------------------------------------

const C_OUTLIER_REPORT_FACTOR: i64 = 3;
const C_ESTIMATED_FEEDBACK_INTERVAL_ALPHA: f64 = 0.9;

const C_REFERENCE_TIME_MASK: u32 = (1 << 24) - 1;
const C_REFERENCE_TIME_RESOLUTION: i64 = 64; // 64 ms

// ------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct TwccFeedbackParams {
    pub(super) logger: Logger,
}

pub(super) struct TwccFeedback {
    params: TwccFeedbackParams,

    last_feedback_time: Time,
    pub(super) estimated_feedback_interval: Duration,
    pub(super) num_reports: i64,
    pub(super) num_reports_out_of_order: i64,

    highest_feedback_count: u8,

    pub(super) cycles: i64,
    highest_reference_time: u32,
}

impl TwccFeedback {
    pub(super) fn new(params: TwccFeedbackParams) -> Self {
        Self {
            params,
            last_feedback_time: Time::ZERO,
            estimated_feedback_interval: Duration::ZERO,
            num_reports: 0,
            num_reports_out_of_order: 0,
            highest_feedback_count: 0,
            cycles: 0,
            highest_reference_time: 0,
        }
    }

    pub(super) fn process_report(&mut self, report: &TransportLayerCc, at: Time) -> (i64, bool) {
        self.num_reports += 1;
        if self.last_feedback_time.is_zero() {
            self.last_feedback_time = at;
            self.highest_reference_time = report.reference_time;
            self.highest_feedback_count = report.fb_pkt_count;
            return (
                self.cycles
                    .wrapping_add(report.reference_time as i64)
                    .wrapping_mul(C_REFERENCE_TIME_RESOLUTION)
                    .wrapping_mul(1000),
                false,
            );
        }

        let mut is_out_of_order = false;
        if report
            .fb_pkt_count
            .wrapping_sub(self.highest_feedback_count)
            > (1 << 7)
        {
            self.num_reports_out_of_order += 1;
            is_out_of_order = true;
            infow!(
                self.params.logger,
                "send side bwe: received out-of-order feedback report",
                "highestFeedbackCount" => self.highest_feedback_count,
                "fbPktCount" => report.fb_pkt_count,
                "numReportsOutOfOrder" => self.num_reports_out_of_order,
            );
        }

        // reference time wrap around handling
        let reference_time = if (report
            .reference_time
            .wrapping_sub(self.highest_reference_time)
            & C_REFERENCE_TIME_MASK)
            < (1 << 23)
        {
            if report.reference_time < self.highest_reference_time {
                self.cycles = self.cycles.wrapping_add(1 << 24);
            }
            self.highest_reference_time = report.reference_time;
            self.cycles.wrapping_add(report.reference_time as i64)
        } else {
            let mut cycles = self.cycles;
            if report.reference_time > self.highest_reference_time && cycles >= (1 << 24) {
                cycles -= 1 << 24;
            }
            cycles.wrapping_add(report.reference_time as i64)
        };

        if !is_out_of_order {
            let since_last = at.sub(self.last_feedback_time);
            if self.estimated_feedback_interval == Duration::ZERO {
                self.estimated_feedback_interval = since_last;
            } else {
                // filter out outliers from estimate
                if since_last.0 > self.estimated_feedback_interval.0 / C_OUTLIER_REPORT_FACTOR
                    && since_last.0
                        < C_OUTLIER_REPORT_FACTOR.wrapping_mul(self.estimated_feedback_interval.0)
                {
                    // smoothed version of inter feedback interval
                    self.estimated_feedback_interval = Duration(
                        (C_ESTIMATED_FEEDBACK_INTERVAL_ALPHA
                            * self.estimated_feedback_interval.0 as f64
                            + (1.0 - C_ESTIMATED_FEEDBACK_INTERVAL_ALPHA) * since_last.0 as f64)
                            as i64,
                    );
                }
            }
            self.last_feedback_time = at;
            self.highest_feedback_count = report.fb_pkt_count;
        }

        (
            reference_time
                .wrapping_mul(C_REFERENCE_TIME_RESOLUTION)
                .wrapping_mul(1000),
            is_out_of_order,
        )
    }
}

impl fmt::Debug for TwccFeedback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TwccFeedback")
            .field("last_feedback_time", &self.last_feedback_time)
            .field(
                "estimated_feedback_interval",
                &self.estimated_feedback_interval,
            )
            .field("num_reports", &self.num_reports)
            .field("num_reports_out_of_order", &self.num_reports_out_of_order)
            .field("cycles", &(self.cycles / (1 << 24)))
            .finish()
    }
}
