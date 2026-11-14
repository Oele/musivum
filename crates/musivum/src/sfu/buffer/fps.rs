// Ported from LiveKit, pkg/sfu/buffer/fps.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - `FrameRateCalculator` is a trait whose `recv_packet` takes `&mut self`: the calculators
//   have no lock, their owner guards them. Every type has its methods as the original has
//   them, in its order; the types that are a `FrameRateCalculator` in Go implement the trait
//   after that, by passing its calls on (to the methods of the embedded calculator where Go
//   promotes those).
// - The views of one spatial layer (`FrameRateCalculatorForVp9Layer`,
//   `FrameRateCalculatorForDdLayer`) embed a pointer to their calculator in the original, so
//   the views, and the callback that sets the maximum layer, all work on the one calculator.
//   Here `FrameRateCalculatorVp9` and `FrameRateCalculatorDd` are handles: the fields of the
//   original are behind an `Arc<Mutex<..>>`, a clone of the handle is the same calculator,
//   and a view holds such a clone where the original embeds the pointer. The calls stay the
//   same: `get_frame_rate_calculator_for_spatial(spatial)` gives a `FrameRateCalculator`.
//   The methods of the two handles take `&self`. The lock is there for Rust only: whoever
//   guards the calculator guards its views with it, so it is never waited for.
// - `get_frame_rate` and `get_frame_rate_for_spatial` return a copy of the rates, where the
//   original returns a slice of the calculator's own array (in which a caller that keeps it
//   sees the rates that come later); where the original returns nil the `Vec` is empty.
// - A `frameInfo` is a value: where the original has one `*frameInfo` in several places
//   (`baseFrame`, `firstFrames`, `fnReceived`), there are copies. Only the H.26x calculator
//   changes a frame once it is made, in its list; of its base frame it reads `startSeq`
//   again, which does not change (a packet at or before it is dropped first).
// - A `container/list` is a `VecDeque` (the linked list of Rust's standard library cannot
//   insert in the middle), and the H.26x calculator's list is empty where the original's is
//   nil.
// - The calculators of the VP9 calculator's spatial layers are values, so the checks for a
//   nil one are gone.
// - `fn` is a keyword: the field and the parameter of that name are `r#fn`.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;

use super::{DEFAULT_MAX_LAYER_SPATIAL, DEFAULT_MAX_LAYER_TEMPORAL, ExtPacket};
use crate::protocol::logger::Logger;
use crate::{debugw, warnw};

const MIN_FRAMES_FOR_CALCULATION: [i64; 4] = [8, 15, 40, 60];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct FrameInfo {
    start_seq: u16,
    end_seq: u16,
    ts: u32,
    r#fn: u16,
    spatial: i32,
    temporal: i32,
}

pub trait FrameRateCalculator: Send + Sync {
    fn recv_packet(&mut self, ep: &ExtPacket) -> bool;
    fn get_frame_rate(&self) -> Vec<f32>;
    fn completed(&self) -> bool;
}

// -----------------------------

// FrameRateCalculator based on PictureID in VPx
struct FrameRateCalculatorVpx {
    frame_rates: [f32; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1],
    clock_rate: u32,
    logger: Logger,
    first_frames: [Option<FrameInfo>; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1],
    second_frames: [Option<FrameInfo>; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1],
    fn_received: [Option<FrameInfo>; 64],
    base_frame: Option<FrameInfo>,
    completed: bool,
}

impl FrameRateCalculatorVpx {
    fn new(clock_rate: u32, logger: Logger) -> Self {
        Self {
            frame_rates: Default::default(),
            clock_rate,
            logger,
            first_frames: Default::default(),
            second_frames: Default::default(),
            fn_received: [None; 64],
            base_frame: None,
            completed: false,
        }
    }

    fn completed(&self) -> bool {
        self.completed
    }

    fn recv_packet(&mut self, ep: &ExtPacket, r#fn: u16) -> bool {
        if self.completed {
            return true;
        }

        if ep.video_layer.temporal >= self.frame_rates.len() as i32 {
            warnw!(self.logger, "invalid temporal layer", nil, "temporal" => ep.video_layer.temporal);
            return false;
        }

        let mut temporal = ep.video_layer.temporal;
        if temporal < 0 {
            temporal = 0;
        }

        let Some(base_frame) = self.base_frame else {
            self.base_frame = Some(FrameInfo {
                ts: ep.packet.timestamp,
                r#fn,
                ..Default::default()
            });
            self.fn_received[0] = self.base_frame;
            self.first_frames[temporal as usize] = self.base_frame;
            return false;
        };

        let base_diff = r#fn.wrapping_sub(base_frame.r#fn);
        if base_diff == 0 || base_diff > 0x4000 {
            return false;
        }

        if base_diff >= self.fn_received.len() as u16 {
            // frame number is not continuous, reset
            self.reset();

            return false;
        }

        if self.fn_received[base_diff as usize].is_some() {
            return false;
        }

        let fi = FrameInfo {
            ts: ep.packet.timestamp,
            r#fn,
            temporal,
            ..Default::default()
        };
        self.fn_received[base_diff as usize] = Some(fi);

        let first_frame = self.first_frames[temporal as usize];
        let second_frame = self.second_frames[temporal as usize];
        match first_frame {
            None => {
                self.first_frames[temporal as usize] = Some(fi);
            }
            Some(first_frame) => {
                if second_frame.is_none_or(|second_frame| second_frame.r#fn < r#fn)
                    && r#fn != first_frame.r#fn
                    && r#fn.wrapping_sub(first_frame.r#fn) < 0x4000
                {
                    self.second_frames[temporal as usize] = Some(fi);
                }
            }
        }

        self.calc()
    }

    fn calc(&mut self) -> bool {
        // `calc` runs only when there is a base frame.
        let base_frame = self.base_frame.expect("baseFrame is nil");

        let mut rate_counter = 0i64;
        for current_temporal in 0..=DEFAULT_MAX_LAYER_TEMPORAL {
            if self.frame_rates[current_temporal as usize] > 0.0 {
                rate_counter += 1;
                continue;
            }

            let ff = self.first_frames[current_temporal as usize];
            let sf = self.second_frames[current_temporal as usize];
            // lower temporal layer has been calculated, but higher layer has not received any frames, it should not exist
            if rate_counter > 0 && ff.is_none() {
                rate_counter += 1;
                continue;
            }
            let (Some(ff), Some(sf)) = (ff, sf) else {
                continue;
            };

            let mut frame_count = 0i64;
            let mut last_ts = ff.ts;
            for j in ff.r#fn.wrapping_sub(base_frame.r#fn).wrapping_add(1)
                ..sf.r#fn.wrapping_sub(base_frame.r#fn).wrapping_add(1)
            {
                match self.fn_received[j as usize] {
                    None => break,
                    Some(f) => {
                        if f.temporal <= current_temporal {
                            frame_count += 1;
                            last_ts = f.ts;
                        }
                    }
                }
            }
            if frame_count >= MIN_FRAMES_FOR_CALCULATION[current_temporal as usize] {
                self.frame_rates[current_temporal as usize] = self.clock_rate as f32
                    / last_ts.wrapping_sub(ff.ts) as f32
                    * frame_count as f32;
                rate_counter += 1;
            }
        }

        if rate_counter == self.frame_rates.len() as i64 {
            self.completed = true;

            // normalize frame rates, Microsoft Edge use 3 temporal layers for vp8 but the middle layer has chance to
            // get a very low frame rate, so we need to normalize the frame rate(use fixed ration 1:2 of highest layer for that layer)
            if self.frame_rates[2] > 0.0 && self.frame_rates[2] > self.frame_rates[1] * 3.0 {
                self.frame_rates[1] = self.frame_rates[2] / 2.0;
            }
            self.reset();
            return true;
        }
        false
    }

    fn reset(&mut self) {
        self.first_frames.fill(None);
        self.second_frames.fill(None);

        self.fn_received.fill(None);
        self.base_frame = None;
    }

    fn get_frame_rate(&self) -> Vec<f32> {
        self.frame_rates.to_vec()
    }
}

// -----------------------------

// FrameRateCalculator based on PictureID in VP8
pub struct FrameRateCalculatorVp8 {
    frame_rate_calculator_vpx: FrameRateCalculatorVpx,
    logger: Logger,
}

impl FrameRateCalculatorVp8 {
    pub fn new(clock_rate: u32, logger: Logger) -> Self {
        Self {
            frame_rate_calculator_vpx: FrameRateCalculatorVpx::new(clock_rate, logger.clone()),
            logger,
        }
    }

    pub fn recv_packet(&mut self, ep: &ExtPacket) -> bool {
        if self.frame_rate_calculator_vpx.completed() {
            return true;
        }

        let Some(vp8) = &ep.vp8 else {
            debugw!(self.logger, "no vp8 payload", "sn" => ep.packet.sequence_number);
            return false;
        };
        if !vp8.i {
            debugw!(self.logger, "no vp8 pictureID", "sn" => ep.packet.sequence_number);
            return false;
        }
        let success = self
            .frame_rate_calculator_vpx
            .recv_packet(ep, vp8.picture_id);

        if self.frame_rate_calculator_vpx.completed() {
            debugw!(self.logger, "frame rate calculated", "rate" => self.frame_rate_calculator_vpx.get_frame_rate());
        }

        success
    }

    // The two methods of the embedded calculator that Go promotes.

    pub fn completed(&self) -> bool {
        self.frame_rate_calculator_vpx.completed()
    }

    pub fn get_frame_rate(&self) -> Vec<f32> {
        self.frame_rate_calculator_vpx.get_frame_rate()
    }
}

impl FrameRateCalculator for FrameRateCalculatorVp8 {
    fn recv_packet(&mut self, ep: &ExtPacket) -> bool {
        FrameRateCalculatorVp8::recv_packet(self, ep)
    }

    fn get_frame_rate(&self) -> Vec<f32> {
        FrameRateCalculatorVp8::get_frame_rate(self)
    }

    fn completed(&self) -> bool {
        FrameRateCalculatorVp8::completed(self)
    }
}

// -----------------------------

// FrameRateCalculator based on PictureID in VP9
#[derive(Clone)]
pub struct FrameRateCalculatorVp9 {
    logger: Logger,

    // the rest of the original's fields, which the views of the spatial layers share
    inner: Arc<Mutex<FrameRateCalculatorVp9Inner>>,
}

struct FrameRateCalculatorVp9Inner {
    completed: bool,

    // VP9-TODO - this is assuming three spatial layers. As `completed` marker relies on all layers being finished, have to assume this. FIX.
    //            Maybe look at number of layers in livekit.TrackInfo and declare completed once advertised layers are measured
    frame_rate_calculators_vpx: [FrameRateCalculatorVpx; DEFAULT_MAX_LAYER_SPATIAL as usize + 1],
}

impl FrameRateCalculatorVp9 {
    pub fn new(clock_rate: u32, logger: Logger) -> Self {
        let frame_rate_calculators_vpx =
            std::array::from_fn(|_| FrameRateCalculatorVpx::new(clock_rate, logger.clone()));

        Self {
            logger,
            inner: Arc::new(Mutex::new(FrameRateCalculatorVp9Inner {
                completed: false,
                frame_rate_calculators_vpx,
            })),
        }
    }

    pub fn completed(&self) -> bool {
        self.inner.lock().completed
    }

    pub fn recv_packet(&self, ep: &ExtPacket) -> bool {
        let mut f = self.inner.lock();

        if f.completed {
            return true;
        }

        let Some(vp9) = &ep.vp9 else {
            debugw!(self.logger, "no vp9 payload", "sn" => ep.packet.sequence_number);
            return false;
        };
        if !vp9.i {
            debugw!(self.logger, "no vp9 pictureID", "sn" => ep.packet.sequence_number);
            return false;
        }
        if ep.video_layer.spatial < 0
            || ep.video_layer.spatial >= f.frame_rate_calculators_vpx.len() as i32
        {
            debugw!(self.logger, "invalid spatial layer", "sn" => ep.packet.sequence_number, "spatial" => ep.video_layer.spatial);
            return false;
        }

        let success = f.frame_rate_calculators_vpx[ep.video_layer.spatial as usize]
            .recv_packet(ep, vp9.picture_id);

        let mut completed = true;
        for frc in &f.frame_rate_calculators_vpx {
            if !frc.completed() {
                completed = false;
                break;
            }
        }

        if completed {
            f.completed = true;

            let frame_rates = f
                .frame_rate_calculators_vpx
                .each_ref()
                .map(|frc| frc.get_frame_rate());
            debugw!(self.logger, "frame rate calculated", "rate" => frame_rates);
        }

        success
    }

    pub fn get_frame_rate_for_spatial(&self, spatial: i32) -> Vec<f32> {
        let f = self.inner.lock();

        if spatial < 0 || spatial >= f.frame_rate_calculators_vpx.len() as i32 {
            return Vec::new();
        }
        f.frame_rate_calculators_vpx[spatial as usize].get_frame_rate()
    }

    pub fn get_frame_rate_calculator_for_spatial(
        &self,
        spatial: i32,
    ) -> FrameRateCalculatorForVp9Layer {
        FrameRateCalculatorForVp9Layer {
            frame_rate_calculator_vp9: self.clone(),
            spatial,
        }
    }
}

// -----------------------------

pub struct FrameRateCalculatorForVp9Layer {
    frame_rate_calculator_vp9: FrameRateCalculatorVp9,
    spatial: i32,
}

impl FrameRateCalculatorForVp9Layer {
    pub fn get_frame_rate(&self) -> Vec<f32> {
        self.frame_rate_calculator_vp9
            .get_frame_rate_for_spatial(self.spatial)
    }
}

// `RecvPacket` and `Completed` are those of the embedded calculator.
impl FrameRateCalculator for FrameRateCalculatorForVp9Layer {
    fn recv_packet(&mut self, ep: &ExtPacket) -> bool {
        self.frame_rate_calculator_vp9.recv_packet(ep)
    }

    fn get_frame_rate(&self) -> Vec<f32> {
        FrameRateCalculatorForVp9Layer::get_frame_rate(self)
    }

    fn completed(&self) -> bool {
        self.frame_rate_calculator_vp9.completed()
    }
}

// -----------------------------------------------

// FrameRateCalculator based on Dependency descriptor
#[derive(Clone)]
pub struct FrameRateCalculatorDd {
    logger: Logger,

    // the rest of the original's fields, which the views of the spatial layers share
    inner: Arc<Mutex<FrameRateCalculatorDdInner>>,
}

struct FrameRateCalculatorDdInner {
    frame_rates:
        [[f32; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1]; DEFAULT_MAX_LAYER_SPATIAL as usize + 1],
    clock_rate: u32,
    first_frames: [[Option<FrameInfo>; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1];
        DEFAULT_MAX_LAYER_SPATIAL as usize + 1],
    second_frames: [[Option<FrameInfo>; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1];
        DEFAULT_MAX_LAYER_SPATIAL as usize + 1],
    fn_received: [Option<FrameInfo>; 256],
    base_frame: Option<FrameInfo>,
    completed: bool,

    // frames for each decode target
    target_frames: [[VecDeque<u16>; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1];
        DEFAULT_MAX_LAYER_SPATIAL as usize + 1],

    max_spatial: i32,
    max_temporal: i32,
}

impl FrameRateCalculatorDd {
    pub fn new(clock_rate: u32, logger: Logger) -> Self {
        Self {
            logger,
            inner: Arc::new(Mutex::new(FrameRateCalculatorDdInner {
                frame_rates: Default::default(),
                clock_rate,
                first_frames: Default::default(),
                second_frames: Default::default(),
                fn_received: [None; 256],
                base_frame: None,
                completed: false,
                target_frames: Default::default(),
                max_spatial: DEFAULT_MAX_LAYER_SPATIAL,
                max_temporal: DEFAULT_MAX_LAYER_TEMPORAL,
            })),
        }
    }

    pub fn completed(&self) -> bool {
        self.inner.lock().completed
    }

    pub fn set_max_layer(&self, spatial: i32, temporal: i32) {
        let mut f = self.inner.lock();

        if spatial <= DEFAULT_MAX_LAYER_SPATIAL {
            f.max_spatial = spatial;
        } else {
            warnw!(self.logger, "max spatial layer out of range", nil, "spatial" => spatial);
        }

        if temporal <= DEFAULT_MAX_LAYER_TEMPORAL {
            f.max_temporal = temporal;
        } else {
            warnw!(self.logger, "max temporal layer out of range", nil, "temporal" => temporal);
        }
    }

    #[allow(clippy::manual_range_contains)] // the comparisons as the original has them
    pub fn recv_packet(&self, ep: &ExtPacket) -> bool {
        let mut f = self.inner.lock();
        let f = &mut *f;

        if f.completed {
            return true;
        }

        let Some(dependency_descriptor) = &ep.dependency_descriptor else {
            debugw!(self.logger, "dependency descriptor is nil");
            return false;
        };

        let mut spatial = ep.video_layer.spatial;
        // non-SVC codec will set spatial to -1
        if spatial < 0 {
            spatial = 0;
        }
        let temporal = ep.video_layer.temporal;
        if temporal < 0
            || temporal > DEFAULT_MAX_LAYER_TEMPORAL
            || spatial > DEFAULT_MAX_LAYER_SPATIAL
        {
            warnw!(self.logger, "invalid spatial or temporal", nil, "spatial" => spatial, "temporal" => temporal, "sn" => ep.packet.sequence_number);
            return false;
        }

        // As in Go, a descriptor that is nil is a panic here, and so are frame dependencies
        // that are nil further down.
        let descriptor = dependency_descriptor
            .descriptor
            .as_ref()
            .expect("ExtDependencyDescriptor: Descriptor is nil");
        let r#fn = descriptor.frame_number;
        let Some(base_frame) = f.base_frame else {
            f.base_frame = Some(FrameInfo {
                ts: ep.packet.timestamp,
                r#fn,
                ..Default::default()
            });
            f.fn_received[0] = f.base_frame;
            f.first_frames[spatial as usize][temporal as usize] = f.base_frame;
            f.second_frames[spatial as usize][temporal as usize] = f.base_frame;
            return false;
        };

        let base_diff = r#fn.wrapping_sub(base_frame.r#fn);
        if base_diff == 0 || base_diff > 0x8000 {
            return false;
        }

        if base_diff >= f.fn_received.len() as u16 {
            // frame number is not continuous, reset
            f.base_frame = None;
            for i in 0..f.first_frames.len() {
                for j in 0..f.first_frames[i].len() {
                    f.first_frames[i][j] = None;
                    f.second_frames[i][j] = None;
                    f.target_frames[i][j].clear();
                }
            }
            f.fn_received.fill(None);
            return false;
        }

        if f.fn_received[base_diff as usize].is_some() {
            return false;
        }

        let fi = FrameInfo {
            ts: ep.packet.timestamp,
            r#fn,
            temporal,
            spatial,
            ..Default::default()
        };
        f.fn_received[base_diff as usize] = Some(fi);

        if f.first_frames[spatial as usize][temporal as usize].is_none() {
            f.first_frames[spatial as usize][temporal as usize] = Some(fi);
            f.second_frames[spatial as usize][temporal as usize] = Some(fi);
            return false;
        }

        let chain = &mut f.target_frames[spatial as usize][temporal as usize];
        if chain.is_empty() {
            chain.push_back(r#fn);
        }
        let frame_dependencies = descriptor
            .frame_dependencies
            .as_ref()
            .expect("DependencyDescriptor: FrameDependencies is nil");
        for &fdiff in &frame_dependencies.frame_diffs {
            let depend_frame = r#fn.wrapping_sub(fdiff as u16);
            // frame too old, ignore
            //
            // The second frame of a layer is set with its first one, which is there.
            let second_frame =
                f.second_frames[spatial as usize][temporal as usize].expect("secondFrames is nil");
            if depend_frame.wrapping_sub(second_frame.r#fn) > 0x8000 {
                continue;
            }

            // insertFrame:
            for e in (0..chain.len()).rev() {
                let val = chain[e];
                if val == depend_frame {
                    break;
                } else if sn16_lt(val, depend_frame) {
                    chain.insert(e + 1, depend_frame);
                    break;
                } else if e == 0 {
                    chain.push_front(depend_frame);
                    break;
                }
            }
        }
        self.calc(f)
    }

    fn calc(&self, f: &mut FrameRateCalculatorDdInner) -> bool {
        // `calc` runs only when there is a base frame.
        let base_frame = f.base_frame.expect("baseFrame is nil");

        let mut rate_counter = 0i64;
        for current_spatial in 0..=f.max_spatial {
            let mut current_spatial_rate_counter = 0i64;
            for current_temporal in 0..=f.max_temporal {
                let (s, t) = (current_spatial as usize, current_temporal as usize);
                if f.frame_rates[s][t] > 0.0 {
                    rate_counter += 1;
                    current_spatial_rate_counter += 1;
                    continue;
                }

                let mut first_frame = f.first_frames[s][t];
                // lower temporal layer has been calculated, but higher layer has not received any frames, it should not exist
                if current_spatial_rate_counter > 0 && first_frame.is_none() {
                    current_spatial_rate_counter += 1;
                    rate_counter += 1;
                    continue;
                }

                let chain = &f.target_frames[s][t];

                // find last decodable frame (no dependency frame is lost)
                let mut last_frame: Option<FrameInfo> = None;
                for &e in chain {
                    let diff = e.wrapping_sub(base_frame.r#fn);
                    if diff >= f.fn_received.len() as u16 {
                        continue;
                    }

                    let Some(fi) = f.fn_received[diff as usize] else {
                        break;
                    };
                    last_frame = Some(fi);
                    if first_frame.is_none()
                        && fi.spatial == current_spatial
                        && fi.temporal == current_temporal
                    {
                        first_frame = Some(fi);
                    }
                }

                // A frame of the chain has been received, so the layer has had its first
                // frame, and with that its second one: neither is nil below.
                let Some(last_frame) = last_frame else {
                    continue;
                };
                if last_frame.r#fn > f.second_frames[s][t].expect("secondFrames is nil").r#fn {
                    f.second_frames[s][t] = Some(last_frame);
                } else {
                    continue;
                }
                let first_frame = first_frame.expect("firstFrame is nil");

                let mut frame_count = 0i64;
                for i in first_frame.r#fn.wrapping_sub(base_frame.r#fn)
                    ..=last_frame.r#fn.wrapping_sub(base_frame.r#fn)
                {
                    let Some(fi) = f.fn_received[i as usize] else {
                        continue;
                    };
                    if fi.spatial == current_spatial && fi.temporal <= current_temporal {
                        frame_count += 1;
                    }
                }

                if frame_count >= MIN_FRAMES_FOR_CALCULATION[t] && last_frame.ts > first_frame.ts {
                    f.frame_rates[s][t] = f.clock_rate as f32
                        / last_frame.ts.wrapping_sub(first_frame.ts) as f32
                        * frame_count as f32;
                    rate_counter += 1;
                }
            }
        }

        if rate_counter == (f.max_spatial + 1) as i64 * (f.max_temporal + 1) as i64 {
            f.completed = true;
            Self::close(f);

            debugw!(self.logger, "frame rate calculated", "rate" => f.frame_rates);
            return true;
        }
        false
    }

    pub fn get_frame_rate_for_spatial(&self, spatial: i32) -> Vec<f32> {
        let f = self.inner.lock();

        if spatial < 0 || spatial >= f.frame_rates.len() as i32 {
            return Vec::new();
        }
        f.frame_rates[spatial as usize].to_vec()
    }

    fn close(f: &mut FrameRateCalculatorDdInner) {
        f.base_frame = None;
        for i in 0..f.first_frames.len() {
            for j in 0..f.first_frames[i].len() {
                f.first_frames[i][j] = None;
                f.second_frames[i][j] = None;
            }
        }

        f.fn_received.fill(None);
        for i in 0..f.target_frames.len() {
            for j in 0..f.target_frames[i].len() {
                f.target_frames[i][j].clear();
            }
        }
    }

    pub fn get_frame_rate_calculator_for_spatial(
        &self,
        spatial: i32,
    ) -> FrameRateCalculatorForDdLayer {
        FrameRateCalculatorForDdLayer {
            frame_rate_calculator_dd: self.clone(),
            spatial,
        }
    }
}

// -----------------------------------------------

pub struct FrameRateCalculatorForDdLayer {
    frame_rate_calculator_dd: FrameRateCalculatorDd,
    spatial: i32,
}

impl FrameRateCalculatorForDdLayer {
    pub fn get_frame_rate(&self) -> Vec<f32> {
        self.frame_rate_calculator_dd
            .get_frame_rate_for_spatial(self.spatial)
    }
}

// `RecvPacket` and `Completed` are those of the embedded calculator.
impl FrameRateCalculator for FrameRateCalculatorForDdLayer {
    fn recv_packet(&mut self, ep: &ExtPacket) -> bool {
        self.frame_rate_calculator_dd.recv_packet(ep)
    }

    fn get_frame_rate(&self) -> Vec<f32> {
        FrameRateCalculatorForDdLayer::get_frame_rate(self)
    }

    fn completed(&self) -> bool {
        self.frame_rate_calculator_dd.completed()
    }
}

// -----------------------------------------------

pub struct FrameRateCalculatorH26x {
    frame_rates: [f32; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1],
    clock_rate: u32,
    logger: Logger,
    fn_received: VecDeque<FrameInfo>,
    base_frame: Option<FrameInfo>,
    completed: bool,
}

impl FrameRateCalculatorH26x {
    pub fn new(clock_rate: u32, logger: Logger) -> Self {
        Self {
            frame_rates: Default::default(),
            clock_rate,
            logger,
            fn_received: VecDeque::new(),
            base_frame: None,
            completed: false,
        }
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    pub fn recv_packet(&mut self, ep: &ExtPacket) -> bool {
        if self.completed {
            return true;
        }

        if ep.video_layer.temporal >= self.frame_rates.len() as i32 {
            warnw!(self.logger, "invalid temporal layer", nil, "temporal" => ep.video_layer.temporal);
            return false;
        }

        let mut temporal = ep.video_layer.temporal;
        if temporal < 0 {
            temporal = 0;
        }

        let Some(base_frame) = self.base_frame else {
            let base_frame = FrameInfo {
                start_seq: ep.packet.sequence_number,
                end_seq: ep.packet.sequence_number,
                ts: ep.packet.timestamp,
                temporal,
                ..Default::default()
            };
            self.base_frame = Some(base_frame);
            self.fn_received = VecDeque::new();
            self.fn_received.push_back(base_frame);
            return false;
        };

        if sn16_lt_or_equal(ep.packet.sequence_number, base_frame.start_seq) {
            return false;
        }

        // insertFrame:
        for e in (0..self.fn_received.len()).rev() {
            let frame = &mut self.fn_received[e];
            if frame.ts == ep.packet.timestamp {
                if sn16_lt(frame.end_seq, ep.packet.sequence_number) {
                    frame.end_seq = ep.packet.sequence_number;
                }
                if sn16_lt(ep.packet.sequence_number, frame.start_seq) {
                    frame.start_seq = ep.packet.sequence_number;
                }
                break;
            } else if sn32_lt(frame.ts, ep.packet.timestamp) {
                self.fn_received.insert(
                    e + 1,
                    FrameInfo {
                        start_seq: ep.packet.sequence_number,
                        end_seq: ep.packet.sequence_number,
                        ts: ep.packet.timestamp,
                        temporal,
                        ..Default::default()
                    },
                );
                break;
            } else if e == 0 {
                self.fn_received.push_front(FrameInfo {
                    start_seq: ep.packet.sequence_number,
                    end_seq: ep.packet.sequence_number,
                    ts: ep.packet.timestamp,
                    temporal,
                    ..Default::default()
                });
                break;
            }
        }

        self.calc()
    }

    fn calc(&mut self) -> bool {
        let mut frame_counts = [0i64; DEFAULT_MAX_LAYER_TEMPORAL as usize + 1];
        let mut total_frame_count = 0i64;
        let mut ts_duration = 0i64;
        // `calc` runs only when the list has its base frame, so there is a front (which, were
        // it nil, the original would panic on).
        let mut cur = 0;
        loop {
            let next = cur + 1;
            if next >= self.fn_received.len() {
                break;
            }
            let ff = self.fn_received[cur];
            let nf = self.fn_received[next];
            if nf.start_seq.wrapping_sub(ff.end_seq) == 1 {
                total_frame_count += 1;
                ts_duration += nf.ts.wrapping_sub(ff.ts) as i64;
                for count in frame_counts.iter_mut().skip(nf.temporal as usize) {
                    *count += 1;
                }
            } else {
                // reset to find continuous frames
                total_frame_count = 0;
                frame_counts.fill(0);
                ts_duration = 0;
            }

            // received enough continuous frames, calculate fps
            if total_frame_count >= MIN_FRAMES_FOR_CALCULATION[DEFAULT_MAX_LAYER_TEMPORAL as usize]
            {
                for current_temporal in 0..=DEFAULT_MAX_LAYER_TEMPORAL as usize {
                    let count = frame_counts[current_temporal];
                    if current_temporal > 0 && count == frame_counts[current_temporal - 1] {
                        // no frames for this temporal layer
                        self.frame_rates[current_temporal] = 0.0;
                    } else {
                        self.frame_rates[current_temporal] =
                            self.clock_rate as f32 / ts_duration as f32 * count as f32;
                    }
                }
                debugw!(self.logger, "fps changed", "fps" => self.get_frame_rate());
                self.completed = true;
                self.reset();
                return true;
            }

            cur = next;
        }

        false
    }

    fn reset(&mut self) {
        self.fn_received.clear();
        self.base_frame = None;
    }

    pub fn get_frame_rate(&self) -> Vec<f32> {
        self.frame_rates.to_vec()
    }
}

impl FrameRateCalculator for FrameRateCalculatorH26x {
    fn recv_packet(&mut self, ep: &ExtPacket) -> bool {
        FrameRateCalculatorH26x::recv_packet(self, ep)
    }

    fn get_frame_rate(&self) -> Vec<f32> {
        FrameRateCalculatorH26x::get_frame_rate(self)
    }

    fn completed(&self) -> bool {
        FrameRateCalculatorH26x::completed(self)
    }
}

// -----------------------------------------------
fn sn16_lt(a: u16, b: u16) -> bool {
    a.wrapping_sub(b) > 0x8000
}

fn sn16_lt_or_equal(a: u16, b: u16) -> bool {
    a == b || a.wrapping_sub(b) > 0x8000
}

fn sn32_lt(a: u32, b: u32) -> bool {
    a.wrapping_sub(b) > 0x80000000
}

#[cfg(test)]
#[path = "fps_test.rs"]
mod fps_test;
