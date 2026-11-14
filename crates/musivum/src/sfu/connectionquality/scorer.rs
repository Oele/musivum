// Ported from LiveKit, pkg/sfu/connectionquality/scorer.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `qualityTransitionScore`, a map in the original, is a
// function, which gives Go's zero value for the quality the map has no entry for; what
// `lock` guards is in an inner struct, and the functions the original calls with the lock
// held take it as an argument; the window a scorer is updated with is a reference, which
// cannot be nil; `MarshalLogObject` of the window is `Debug`, and the times in its `String()`
// are written as this crate writes times; the logger with unlikely values is a closure, since
// the logging macros do not evaluate values when the level is off; `math.Max` and `math.Min`
// are two functions here, since Rust's own do not pass a NaN on.

use std::fmt;

use parking_lot::RwLock;

use crate::go::time::{self, Duration, Time};
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::protocol::utils;
use crate::{debugw, warnw};

pub const MAX_MOS: f32 = 4.5;
pub const MIN_MOS: f32 = 1.0;

const C_MAX_SCORE: f64 = 100.0;
const C_MIN_SCORE: f64 = 30.0;

const C_INCREASE_FACTOR: f64 = 0.4; // slower increase, i. e. when score is recovering move up slower -> conservative
const C_DECREASE_FACTOR: f64 = 0.8; // faster decrease, i. e. when score is dropping move down faster -> aggressive to be responsive to quality drops

const C_DISTANCE_WEIGHT: f64 = 35.0; // each spatial layer missed drops a quality level

const C_UNMUTE_TIME_THRESHOLD: f64 = 0.5;

pub(super) const C_PPS_QUANTIZATION: f64 = 2.0;
const C_PPS_MIN_READINGS: i64 = 10;
const C_PPS_MODE_MIN_BIN: usize = 5; // 10 pps
const C_MODE_CALCULATION_INTERVAL: Duration = Duration(2 * time::MINUTE.0);

fn quality_transition_score(quality: livekit::ConnectionQuality) -> f64 {
    match quality {
        livekit::ConnectionQuality::Good => 80.0,
        livekit::ConnectionQuality::Poor => 40.0,
        livekit::ConnectionQuality::Lost => 20.0,
        // not in the original's map
        livekit::ConnectionQuality::Excellent => 0.0,
    }
}

// Go's `math.Max`: +Inf if either is that, else a NaN if either is one, and +0 over -0.
fn math_max(x: f64, y: f64) -> f64 {
    if x == f64::INFINITY || y == f64::INFINITY {
        return f64::INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        if x.is_sign_negative() {
            return y;
        }
        return x;
    }
    if x > y { x } else { y }
}

// Go's `math.Min`: -Inf if either is that, else a NaN if either is one, and -0 under +0.
fn math_min(x: f64, y: f64) -> f64 {
    if x == f64::NEG_INFINITY || y == f64::NEG_INFINITY {
        return f64::NEG_INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        if x.is_sign_negative() {
            return x;
        }
        return y;
    }
    if x < y { x } else { y }
}

// ------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(super) struct WindowStat {
    pub(super) started_at: Time,
    pub(super) duration: Duration,
    pub(super) packets: u32,
    pub(super) packets_padding: u32,
    pub(super) packets_lost: u32,
    pub(super) packets_missing: u32,
    pub(super) packets_out_of_order: u32,
    pub(super) bytes: u64,
    pub(super) rtt_max: u32,
    pub(super) jitter_max: f64,
    pub(super) last_rtcp_at: Time,
}

impl WindowStat {
    fn calculate_packet_score(&self, aplw: f64, include_rtt: bool, include_jitter: bool) -> f64 {
        // this is based on simplified E-model based on packet loss, rtt, jitter as
        // outlined at https://www.pingman.com/kb/article/how-is-mos-calculated-in-pingplotter-pro-50.html.
        let mut effective_delay = 0.0;
        // discount the dependent factors if dependency indicated.
        // for example,
        // 1. in the up stream, RTT cannot be measured without RTCP-XR, it is using down stream RTT.
        // 2. in the down stream, up stream jitter affects it. although jitter can be adjusted to account for up stream
        //    jitter, this lever can be used to discount jitter in scoring.
        if include_rtt {
            effective_delay += self.rtt_max as f64 / 2.0;
        }
        if include_jitter {
            effective_delay += (self.jitter_max * 2.0) / 1000.0;
        }
        let mut delay_effect = effective_delay / 40.0;
        if effective_delay > 160.0 {
            delay_effect = (effective_delay - 120.0) / 10.0;
        }

        // discount out-of-order packets from loss to deal with a scenario like
        // 1. up stream has loss
        // 2. down stream forwards with loss/hole in sequence number
        // 3. down stream client reports a certain number of loss via RTCP RR
        // 4. while processing that RTCP RR, up stream could have retransmitted missing packets
        // 5. those retransmitted packets are forwarded,
        //    - server's view: it has forwarded those packets
        //    - client's view: it had not seen those packets when sending RTCP RR
        //    so those retransmitted packets appear like down stream loss to server.
        //
        // retransmitted packets would have arrived out-of-order. So, discounting them
        // will account for it.
        //
        // Note that packets can arrive out-of-order in the upstream during regular
        // streaming as well, i. e. without loss + NACK + retransmit. Those will be
        // discounted too. And that will skew the real loss. For example, let
        // us say that 40 out of 100 packets were reported lost by down stream.
        // These could be real losses. In the same window, 40 packets could have been
        // delivered out-of-order by the up stream, thus cancelling out the real loss.
        // But, those situations should be rare and is a compromise for not letting
        // up stream loss penalise down stream.
        let mut actual_lost = self
            .packets_lost
            .wrapping_sub(self.packets_missing)
            .wrapping_sub(self.packets_out_of_order);
        if (actual_lost as i32) < 0 {
            actual_lost = 0;
        }

        let mut loss_effect = 0.0;
        if self.packets.wrapping_add(self.packets_padding) > 0 {
            loss_effect =
                actual_lost as f64 * 100.0 / self.packets.wrapping_add(self.packets_padding) as f64;
        }
        loss_effect *= aplw;

        let mut score = C_MAX_SCORE - delay_effect - loss_effect;
        if score < 0.0 {
            score = 0.0;
        }

        score
    }

    // the two comparisons of the original are kept as they are
    #[allow(clippy::manual_clamp)]
    fn calculate_bitrate_score(&self, expected_bits: i64, is_enabled: bool) -> f64 {
        if expected_bits == 0 || !is_enabled {
            // unsupported mode OR all layers stopped
            return C_MAX_SCORE;
        }

        let mut score = 0.0;
        if self.bytes != 0 {
            // using the ratio of expectedBits / actualBits
            // the quality inflection points are approximately
            // GOOD at ~2.7x, POOR at ~20.1x
            score = C_MAX_SCORE
                - 20.0 * (expected_bits as f64 / self.bytes.wrapping_mul(8) as f64).ln();
            if score > C_MAX_SCORE {
                score = C_MAX_SCORE;
            }
            if score < 0.0 {
                score = 0.0;
            }
        }

        score
    }
}

impl fmt::Display for WindowStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "start: {}, dur: {}, p: {}, pp: {}, pl: {}, pm: {}, pooo: {}, b: {}, rtt: {}, jitter: {:.2}, lastRTCP: {}",
            self.started_at,
            self.duration,
            self.packets,
            self.packets_padding,
            self.packets_lost,
            self.packets_missing,
            self.packets_out_of_order,
            self.bytes,
            self.rtt_max,
            self.jitter_max,
            self.last_rtcp_at,
        )
    }
}

// ------------------------------------------

#[derive(Clone, Debug, Default)]
pub(super) struct QualityScorerParams {
    pub(super) include_rtt: bool,
    pub(super) include_jitter: bool,
    pub(super) enable_bitrate_score: bool,
    pub(super) logger: Logger,
}

pub(super) struct QualityScorer {
    params: QualityScorerParams,

    pub(super) lock: RwLock<QualityScorerInner>,
}

// What `lock` guards.
pub(super) struct QualityScorerInner {
    last_update_at: Time,

    packet_loss_weight: f64,

    pub(super) score: f64,
    stat: WindowStat,

    muted_at: Time,
    unmuted_at: Time,

    layer_muted_at: Time,
    layer_unmuted_at: Time,

    paused_at: Time,
    resumed_at: Time,

    pub(super) pps_histogram: [i64; 250],
    pub(super) num_pps_readings: i64,
    pub(super) pps_mode: usize,
    mode_calculated_at: Time,

    aggregate_bitrate: utils::TimedAggregator<i64>,
    layer_distance: utils::TimedAggregator<f64>,
}

impl QualityScorer {
    pub(super) fn new(params: QualityScorerParams) -> Self {
        Self {
            params,
            lock: RwLock::new(QualityScorerInner {
                last_update_at: Time::ZERO,
                packet_loss_weight: 0.0,
                score: C_MAX_SCORE,
                stat: WindowStat::default(),
                muted_at: Time::ZERO,
                unmuted_at: Time::ZERO,
                layer_muted_at: Time::ZERO,
                layer_unmuted_at: Time::ZERO,
                paused_at: Time::ZERO,
                resumed_at: Time::ZERO,
                pps_histogram: [0; 250],
                num_pps_readings: 0,
                pps_mode: 0,
                mode_calculated_at: Time::ZERO,
                aggregate_bitrate: utils::TimedAggregator::new(utils::TimedAggregatorParams {
                    cap_negative_values: true,
                }),
                layer_distance: utils::TimedAggregator::new(utils::TimedAggregatorParams {
                    cap_negative_values: true,
                }),
            }),
        }
    }

    fn start_at_locked(&self, q: &mut QualityScorerInner, packet_loss_weight: f64, at: Time) {
        q.packet_loss_weight = packet_loss_weight;
        q.last_update_at = at;
        q.mode_calculated_at = at.add(-C_MODE_CALCULATION_INTERVAL);
    }

    pub(super) fn start_at(&self, packet_loss_weight: f64, at: Time) {
        let mut q = self.lock.write();

        self.start_at_locked(&mut q, packet_loss_weight, at);
    }

    // not called in the original either
    #[allow(dead_code)]
    pub(super) fn start(&self, packet_loss_weight: f64) {
        let mut q = self.lock.write();

        self.start_at_locked(&mut q, packet_loss_weight, time::now());
    }

    pub(super) fn update_packet_loss_weight(&self, packet_loss_weight: f64) {
        let mut q = self.lock.write();

        q.packet_loss_weight = packet_loss_weight;
    }

    fn update_mute_at_locked(&self, q: &mut QualityScorerInner, is_muted: bool, at: Time) {
        if is_muted {
            q.muted_at = at;
            // muting when LOST should not push quality to EXCELLENT
            if q.score != quality_transition_score(livekit::ConnectionQuality::Lost) {
                q.score = C_MAX_SCORE;
            }
        } else {
            q.unmuted_at = at;
        }
    }

    pub(super) fn update_mute_at(&self, is_muted: bool, at: Time) {
        let mut q = self.lock.write();

        self.update_mute_at_locked(&mut q, is_muted, at);
    }

    pub(super) fn update_mute(&self, is_muted: bool) {
        let mut q = self.lock.write();

        self.update_mute_at_locked(&mut q, is_muted, time::now());
    }

    fn add_bitrate_transition_at_locked(&self, q: &mut QualityScorerInner, bitrate: i64, at: Time) {
        let _ = q.aggregate_bitrate.add_sample_at(bitrate, at);
    }

    pub(super) fn add_bitrate_transition_at(&self, bitrate: i64, at: Time) {
        let mut q = self.lock.write();

        self.add_bitrate_transition_at_locked(&mut q, bitrate, at);
    }

    pub(super) fn add_bitrate_transition(&self, bitrate: i64) {
        let mut q = self.lock.write();

        self.add_bitrate_transition_at_locked(&mut q, bitrate, time::now());
    }

    fn update_layer_mute_at_locked(&self, q: &mut QualityScorerInner, is_muted: bool, at: Time) {
        if is_muted {
            if !self.is_layer_muted(q) {
                q.aggregate_bitrate.reset();
                q.layer_distance.reset();
                q.layer_muted_at = at;
                q.score = C_MAX_SCORE;
            }
        } else if self.is_layer_muted(q) {
            q.layer_unmuted_at = at;
        }
    }

    pub(super) fn update_layer_mute_at(&self, is_muted: bool, at: Time) {
        let mut q = self.lock.write();

        self.update_layer_mute_at_locked(&mut q, is_muted, at);
    }

    pub(super) fn update_layer_mute(&self, is_muted: bool) {
        let mut q = self.lock.write();

        self.update_layer_mute_at_locked(&mut q, is_muted, time::now());
    }

    fn update_pause_at_locked(&self, q: &mut QualityScorerInner, is_paused: bool, at: Time) {
        if is_paused {
            if !self.is_paused(q) {
                q.aggregate_bitrate.reset();
                q.layer_distance.reset();
                q.paused_at = at;
                q.score = C_MIN_SCORE;
            }
        } else if self.is_paused(q) {
            q.resumed_at = at;
        }
    }

    pub(super) fn update_pause_at(&self, is_paused: bool, at: Time) {
        let mut q = self.lock.write();

        self.update_pause_at_locked(&mut q, is_paused, at);
    }

    pub(super) fn update_pause(&self, is_paused: bool) {
        let mut q = self.lock.write();

        self.update_pause_at_locked(&mut q, is_paused, time::now());
    }

    fn add_layer_transition_at_locked(&self, q: &mut QualityScorerInner, distance: f64, at: Time) {
        let _ = q.layer_distance.add_sample_at(distance, at);
    }

    pub(super) fn add_layer_transition_at(&self, distance: f64, at: Time) {
        let mut q = self.lock.write();

        self.add_layer_transition_at_locked(&mut q, distance, at);
    }

    pub(super) fn add_layer_transition(&self, distance: f64) {
        let mut q = self.lock.write();

        self.add_layer_transition_at_locked(&mut q, distance, time::now());
    }

    fn update_at_locked(&self, q: &mut QualityScorerInner, stat: &WindowStat, at: Time) {
        // always update transitions
        let expected_bits = match q.aggregate_bitrate.get_aggregate_and_restart_at(at) {
            Ok((expected_bits, _)) => expected_bits,
            Err(err) => {
                warnw!(self.params.logger, "error getting expected bitrate", err);
                0
            }
        };
        let expected_distance = match q.layer_distance.get_average_and_restart_at(at) {
            Ok(expected_distance) => expected_distance,
            Err(err) => {
                warnw!(self.params.logger, "error getting expected distance", err);
                0.0
            }
        };

        // nothing to do when muted or not unmuted for long enough
        // NOTE: it is possible that unmute -> mute -> unmute transition happens in the
        //       same analysis window. On a transition to mute, quality is immediately moved
        //       EXCELLENT for responsiveness. On an unmute, the entire window data is
        //       considered (as long as enough time has passed since unmute).
        //
        //       Similarly, when paused (possibly due to congestion), score is immediately
        //       set to cMinScore for responsiveness. The layer transition is reset.
        //       On a resume, quality climbs back up using normal operation.
        if self.is_muted(q)
            || !self.is_unmuted_enough(q, at)
            || self.is_layer_muted(q)
            || self.is_paused(q)
        {
            q.last_update_at = at;
            return;
        }

        let aplw = self.get_adjusted_packet_loss_weight(q, stat, at);
        let mut reason = "none";
        let (mut score, mut packet_score, mut bitrate_score, mut layer_score) =
            (0.0f64, 0.0f64, 0.0f64, 0.0f64);
        if stat.packets.wrapping_add(stat.packets_padding) == 0 {
            if !stat.last_rtcp_at.is_zero() && at.sub(stat.last_rtcp_at) > stat.duration {
                reason = "rtcp";
                score = quality_transition_score(livekit::ConnectionQuality::Lost);
            } else {
                reason = "dry";
                score = quality_transition_score(livekit::ConnectionQuality::Poor);
            }
        } else {
            packet_score = stat.calculate_packet_score(
                aplw,
                self.params.include_rtt,
                self.params.include_jitter,
            );
            bitrate_score =
                stat.calculate_bitrate_score(expected_bits, self.params.enable_bitrate_score);
            layer_score = math_max(
                math_min(
                    C_MAX_SCORE,
                    C_MAX_SCORE - (expected_distance * C_DISTANCE_WEIGHT),
                ),
                0.0,
            );

            let mut min_score = math_min(packet_score, bitrate_score);
            min_score = math_min(min_score, layer_score);

            if packet_score == min_score {
                reason = "packet";
                score = packet_score;
            } else if bitrate_score == min_score {
                reason = "bitrate";
                score = bitrate_score;
            } else if layer_score == min_score {
                reason = "layer";
                score = layer_score;
            }

            let mut factor = C_INCREASE_FACTOR;
            if score < q.score {
                factor = C_DECREASE_FACTOR;
            }
            score = factor * score + (1.0 - factor) * q.score;
            if score < C_MIN_SCORE {
                // lower bound to prevent score from becoming very small values due to extreme conditions.
                // Without a lower bound, it can get so low that it takes a long time to climb back to
                // better quality even under excellent conditions.
                score = C_MIN_SCORE;
            }
        }

        let prev_cq = score_to_connection_quality(q.score);
        let curr_cq = score_to_connection_quality(score);
        let ulgr_debugw = |msg: &str| {
            debugw!(
                self.params.logger,
                msg,
                "reason" => reason,
                "prevScore" => q.score,
                "prevQuality" => prev_cq,
                "prevStat" => q.stat,
                "score" => score,
                "packetScore" => packet_score,
                "layerScore" => layer_score,
                "bitrateScore" => bitrate_score,
                "quality" => curr_cq,
                "stat" => stat,
                "packetLossWeight" => q.packet_loss_weight,
                "adjustedPacketLossWeight" => aplw,
                "modePPS" => q.pps_mode * C_PPS_QUANTIZATION as usize,
                "expectedBits" => expected_bits,
                "expectedDistance" => expected_distance,
            );
        };
        if utils::is_connection_quality_lower(prev_cq, curr_cq) {
            ulgr_debugw("quality drop");
        } else if utils::is_connection_quality_higher(prev_cq, curr_cq) {
            ulgr_debugw("quality rise");
        } else {
            let packets = stat.packets.wrapping_add(stat.packets_padding);
            let mut lost = stat
                .packets_lost
                .wrapping_sub(stat.packets_missing)
                .wrapping_sub(stat.packets_out_of_order);
            if (lost as i32) < 0 {
                lost = 0;
            }
            if packets != 0 && lost.wrapping_mul(100) / packets > 10 {
                ulgr_debugw("quality hold - high loss");
            }
        }

        q.score = score;
        q.stat = *stat;
        q.last_update_at = at;
    }

    pub(super) fn update_at(&self, stat: &WindowStat, at: Time) {
        let mut q = self.lock.write();

        self.update_at_locked(&mut q, stat, at);
    }

    pub(super) fn update(&self, stat: &WindowStat) {
        let mut q = self.lock.write();

        self.update_at_locked(&mut q, stat, time::now());
    }

    fn is_muted(&self, q: &QualityScorerInner) -> bool {
        !q.muted_at.is_zero() && (q.unmuted_at.is_zero() || q.muted_at.after(q.unmuted_at))
    }

    fn is_unmuted_enough(&self, q: &QualityScorerInner, at: Time) -> bool {
        let since_unmute = if q.unmuted_at.is_zero() {
            at.sub(q.last_update_at)
        } else {
            at.sub(q.unmuted_at)
        };

        let since_layer_unmute = if q.layer_unmuted_at.is_zero() {
            at.sub(q.last_update_at)
        } else {
            at.sub(q.layer_unmuted_at)
        };

        let valid_duration = since_layer_unmute.min(since_unmute);

        let since_last_update = at.sub(q.last_update_at);

        valid_duration.seconds() / since_last_update.seconds() > C_UNMUTE_TIME_THRESHOLD
    }

    fn is_layer_muted(&self, q: &QualityScorerInner) -> bool {
        !q.layer_muted_at.is_zero()
            && (q.layer_unmuted_at.is_zero() || q.layer_muted_at.after(q.layer_unmuted_at))
    }

    fn is_paused(&self, q: &QualityScorerInner) -> bool {
        !q.paused_at.is_zero() && (q.resumed_at.is_zero() || q.paused_at.after(q.resumed_at))
    }

    fn get_adjusted_packet_loss_weight(
        &self,
        q: &mut QualityScorerInner,
        stat: &WindowStat,
        at: Time,
    ) -> f64 {
        if stat.duration <= Duration::ZERO {
            return q.packet_loss_weight;
        }

        // packet loss is weighted by comparing against mode of packet rate seen.
        // this is to handle situations like DTX in audio and variable bit rate tracks like screen share.
        // and the effect of loss is not pronounced in those scenarios (audio silence, static screen share).
        // for example, DTX typically uses only 5% of packets of full packet rate. at that rate,
        // packet loss weight is reduced to ~22% of configured weight (i. e. sqrt(0.05) * configured weight)
        let pps = stat.packets as f64 / stat.duration.seconds();
        let pps_quantized = (pps / C_PPS_QUANTIZATION + 0.5) as i64;
        let last_bin = q.pps_histogram.len() - 1;
        if pps_quantized < last_bin as i64 {
            q.pps_histogram[pps_quantized as usize] += 1;
        } else {
            q.pps_histogram[last_bin] += 1;
        }
        q.num_pps_readings += 1;

        // calculate mode sparingly, do it under the following conditions
        //  1. minimum number of readings available (AND)
        //  2. enough time has elapsed since last calculation
        if q.num_pps_readings > C_PPS_MIN_READINGS
            && at.sub(q.mode_calculated_at) > C_MODE_CALCULATION_INTERVAL
        {
            // the most common rate, ignoring rates as low as DTX or static content so they do not become the reference,
            // keep the previous mode through a long stretch of only low rates, for example a long silence
            let (mut mode, mut max_count) = (0, 0);
            for (i, &count) in q.pps_histogram.iter().enumerate().skip(C_PPS_MODE_MIN_BIN) {
                if count > max_count {
                    (mode, max_count) = (i, count);
                }
            }
            if max_count > 0 {
                q.pps_mode = mode;
            }

            // halve the counts so that the mode follows a lasting change of rate, for example a layer switch
            for count in q.pps_histogram.iter_mut() {
                *count >>= 1;
            }
            q.mode_calculated_at = at;
            debugw!(
                self.params.logger,
                "updating pps mode",
                "expected" => stat.packets,
                "duration" => stat.duration.seconds(),
                "pps" => pps,
                "ppsMode" => q.pps_mode,
            );
        }

        if q.pps_mode == 0 || q.pps_mode == last_bin {
            return q.packet_loss_weight;
        }

        let mut packet_ratio = pps / (q.pps_mode as f64 * C_PPS_QUANTIZATION);
        if packet_ratio > 1.0 {
            packet_ratio = 1.0;
        }
        packet_ratio.sqrt() * q.packet_loss_weight
    }

    // called by the tests only, in the original too
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn get_score_and_quality(&self) -> (f32, livekit::ConnectionQuality) {
        let q = self.lock.read();

        (q.score as f32, score_to_connection_quality(q.score))
    }

    pub(super) fn get_mos_and_quality(&self) -> (f32, livekit::ConnectionQuality) {
        let q = self.lock.read();

        (score_to_mos(q.score), score_to_connection_quality(q.score))
    }
}

// ------------------------------------------

fn score_to_connection_quality(score: f64) -> livekit::ConnectionQuality {
    // R-factor -> livekit.ConnectionQuality scale mapping roughly based on
    // https://www.itu.int/ITU-T/2005-2008/com12/emodelv1/tut.htm
    //
    // As there are only three levels in livekit.ConnectionQuality scale,
    // using a larger range for middling quality. Empirical evidence suggests
    // that a score of 60 does not correspond to `POOR` quality. Repair
    // mechanisms and use of algorithms like de-jittering makes the experience
    // better even under harsh conditions.
    if score > quality_transition_score(livekit::ConnectionQuality::Good) {
        return livekit::ConnectionQuality::Excellent;
    }

    if score > quality_transition_score(livekit::ConnectionQuality::Poor) {
        return livekit::ConnectionQuality::Good;
    }

    if score > quality_transition_score(livekit::ConnectionQuality::Lost) {
        return livekit::ConnectionQuality::Poor;
    }

    livekit::ConnectionQuality::Lost
}

// ------------------------------------------

fn score_to_mos(score: f64) -> f32 {
    if score <= 0.0 {
        return 1.0;
    }

    if score >= 100.0 {
        return 4.5;
    }

    (1.0 + 0.035 * score + (0.000007 * score * (score - 60.0) * (100.0 - score))) as f32
}

// ------------------------------------------
