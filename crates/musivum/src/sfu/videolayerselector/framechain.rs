// Ported from LiveKit, pkg/sfu/videolayerselector/framechain.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - A chain is pointed at by the selector, by the decode targets it protects and by the
//   decision cache, which calls `on_expect_frame_changed`: it is made as an `Arc`, and the
//   fields that change are behind a lock the original does not have (`FrameChainInner`). The
//   methods take `&self`.
// - The decision cache, a pointer in the original, is an `Arc<Mutex<..>>`. The chain's lock
//   is held while the cache is asked, and the cache's while it calls the chain: one selector
//   is only ever used by one thread at a time (`select` takes `&mut self`).
// - What the chain gives the cache to call holds the chain weakly, where the original's
//   method value keeps it alive. A chain that nothing else holds any more would only change
//   itself when called.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::{SelectorDecision, SelectorDecisionCache};
use crate::protocol::logger::Logger;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::{debugw, warnw};

pub struct FrameChain {
    logger: Logger,
    decisions: Arc<Mutex<SelectorDecisionCache>>,
    chain_idx: usize,

    inner: Mutex<FrameChainInner>,
}

struct FrameChainInner {
    broken: bool,
    active: bool,
    updating_active: bool,

    expect_frames: Vec<u64>,
}

impl FrameChain {
    pub fn new(
        decisions: Arc<Mutex<SelectorDecisionCache>>,
        chain_idx: usize,
        logger: Logger,
    ) -> Arc<Self> {
        Arc::new(Self {
            logger,
            decisions,
            chain_idx,
            inner: Mutex::new(FrameChainInner {
                broken: true,
                active: false,
                updating_active: false,
                expect_frames: Vec::new(),
            }),
        })
    }

    pub fn on_frame(
        self: &Arc<Self>,
        ext_frame_num: u64,
        fd: &dd::FrameDependencyTemplate,
    ) -> bool {
        let mut fc = self.inner.lock();
        if !fc.active {
            return false;
        }

        if fd.chain_diffs.len() <= self.chain_idx {
            warnw!(self.logger, "invalid frame chain diff", nil, "chanIdx" => self.chain_idx, "frame" => ext_frame_num, "fd" => fd);
            return fc.broken;
        }

        // A decodable frame with frame_chain_fdiff equal to 0 indicates that the Chain is intact.
        if fd.chain_diffs[self.chain_idx] == 0 {
            if fc.broken {
                fc.broken = false;
                // debugw!(self.logger, "frame chain intact", "chanIdx" => self.chain_idx, "frame" => ext_frame_num);
            }
            fc.expect_frames.clear();
            return true;
        }

        if fc.broken {
            return false;
        }

        let prev_frame_in_chain = ext_frame_num.wrapping_sub(fd.chain_diffs[self.chain_idx] as u64);
        let sd = self.decisions.lock().get_decision(prev_frame_in_chain);
        let sd = match sd {
            Ok(sd) => sd,
            Err(err) => {
                debugw!(self.logger, "could not get decision", "err" => err, "chanIdx" => self.chain_idx, "frame" => ext_frame_num, "prevFrame" => prev_frame_in_chain);
                SelectorDecision::Missing
            }
        };

        let mut intact = false;
        match sd {
            SelectorDecision::Forwarded => {
                intact = true;
            }

            SelectorDecision::Unknown => {
                // If the previous frame is unknown, means it has not arrived but could be recovered by NACK / out-of-order arrival,
                // set up a expected callback here to determine if the chain is broken or intact
                let weak: Weak<FrameChain> = Arc::downgrade(self);
                let expected = self.decisions.lock().expect_decision(
                    prev_frame_in_chain,
                    move |frame_num, decision| {
                        if let Some(fc) = weak.upgrade() {
                            fc.on_expect_frame_changed(frame_num, decision);
                        }
                    },
                );
                if expected {
                    intact = true;
                    fc.expect_frames.push(prev_frame_in_chain);
                }
            }

            _ => {}
        }

        if !intact {
            fc.broken = true;
            // debugw!(self.logger, "frame chain broken", "chanIdx" => self.chain_idx, "sd" => sd, "frame" => ext_frame_num, "prevFrame" => prev_frame_in_chain);
        }
        intact
    }

    pub fn on_expect_frame_changed(&self, frame_num: u64, decision: SelectorDecision) {
        let mut fc = self.inner.lock();
        if fc.broken {
            return;
        }

        if let Some(i) = fc.expect_frames.iter().position(|&f| f == frame_num) {
            if decision != SelectorDecision::Forwarded {
                fc.broken = true;
                // debugw!(self.logger, "frame chain broken", "chanIdx" => self.chain_idx, "sd" => decision, "frame" => frame_num);
            }
            fc.expect_frames.swap_remove(i);
        }
    }

    pub fn broken(&self) -> bool {
        self.inner.lock().broken
    }

    pub fn begin_update_active(&self) {
        self.inner.lock().updating_active = false;
    }

    pub fn update_active(&self, active: bool) {
        let mut fc = self.inner.lock();
        fc.updating_active = fc.updating_active || active;
    }

    pub fn end_update_active(&self) {
        let mut fc = self.inner.lock();
        let active = fc.updating_active;
        fc.updating_active = false;

        if active == fc.active {
            return;
        }

        // if the chain transit from inactive to active, reset broken to wait a decodable SWITCH frame
        if !fc.active {
            fc.broken = true;
            debugw!(self.logger, "frame chain broken by inactive", "chanIdx" => self.chain_idx);
        }

        fc.active = active;
    }
}
