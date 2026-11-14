// Ported from LiveKit, pkg/sfu/playoutdelay.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `PlayoutDelayState` is an enum, whose three values
// are named without the type's name in front; what `lock` guards is in an inner struct;
// `NewPlayoutDelayController` is `PlayoutDelayController::new`, which gives the error of the
// extension's package, and no controller with it; the statistics of the sender are held by
// `Arc`; the bytes of the extension are `Bytes`, and no extension is `None`;
// `MarshalLogObject` of the controller's state is `Debug`.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::go::atomic;
use crate::go::time::{self, Time};
use crate::protocol::logger::Logger;
use crate::sfu::rtpextension::playoutdelay as pd;
use crate::sfu::rtpstats;
use crate::{errorw, infow};

const JITTER_MULTI_TO_DELAY: u32 = 10;
const TARGET_DELAY_LOG_THRESHOLD: u32 = 500;

// limit max delay change to make it smoother for a/v sync
const MAX_DELAY_CHANGE_PER_SEC: u32 = 80;

// ----------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum PlayoutDelayState {
    StateChanged = 0,
    Sending,
    Acked,
}

impl PlayoutDelayState {
    // Go's conversion `PlayoutDelayState(v)`, of the values the controller stores.
    fn from_i32(v: i32) -> Option<PlayoutDelayState> {
        match v {
            0 => Some(PlayoutDelayState::StateChanged),
            1 => Some(PlayoutDelayState::Sending),
            2 => Some(PlayoutDelayState::Acked),
            _ => None,
        }
    }
}

impl fmt::Display for PlayoutDelayState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PlayoutDelayState::StateChanged => "StateChanged",
            PlayoutDelayState::Sending => "Sending",
            PlayoutDelayState::Acked => "Acked",
        })
    }
}

// ----------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlayoutDelayControllerState {
    pub sender_snapshot_id: u32,
}

// ----------------------------------------------------

pub struct PlayoutDelayController {
    lock: Mutex<PlayoutDelayControllerInner>,
    state: atomic::Int32,
    min_delay: u32,
    max_delay: u32,
    ext_bytes: atomic::Value<Bytes>,
    logger: Logger,
    rtp_stats: Arc<rtpstats::RtpStatsSender>,

    high_delay_count: atomic::Uint32,
}

// What `lock` guards.
struct PlayoutDelayControllerInner {
    current_delay: u32,
    sending_at_seq: u16,
    sending_at_time: Time,
    sender_snapshot_id: u32,
}

impl PlayoutDelayController {
    pub fn new(
        min_delay: u32,
        mut max_delay: u32,
        logger: Logger,
        rtp_stats: Arc<rtpstats::RtpStatsSender>,
    ) -> Result<PlayoutDelayController, pd::Error> {
        if max_delay == 0 && min_delay > 0 {
            max_delay = pd::MAX_PLAYOUT_DELAY_DEFAULT as u32;
        }
        if max_delay > pd::PLAYOUT_DELAY_MAX_VALUE as u32 {
            max_delay = pd::PLAYOUT_DELAY_MAX_VALUE as u32;
        }
        let sender_snapshot_id = rtp_stats.new_sender_snapshot_id();
        let c = PlayoutDelayController {
            lock: Mutex::new(PlayoutDelayControllerInner {
                current_delay: min_delay,
                sending_at_seq: 0,
                sending_at_time: Time::ZERO,
                sender_snapshot_id,
            }),
            state: atomic::Int32::new(0),
            min_delay,
            max_delay,
            ext_bytes: atomic::Value::default(),
            logger,
            rtp_stats,
            high_delay_count: atomic::Uint32::new(0),
        };

        let res = {
            let inner = c.lock.lock();
            c.create_ext_data_locked(&inner)
        };
        res.map(|_| c)
    }

    pub fn get_state(&self) -> PlayoutDelayControllerState {
        let inner = self.lock.lock();

        PlayoutDelayControllerState {
            sender_snapshot_id: inner.sender_snapshot_id,
        }
    }

    pub fn seed_state(&self, pdcs: PlayoutDelayControllerState) {
        let mut inner = self.lock.lock();

        inner.sender_snapshot_id = pdcs.sender_snapshot_id;
    }

    pub fn set_jitter(&self, jitter: u32) {
        let mut inner = self.lock.lock();
        let (delta_info_sender, _) = self.rtp_stats.delta_info_sender(inner.sender_snapshot_id);
        let mut nack_percent = 0u32;
        if let Some(delta_info_sender) = delta_info_sender
            && delta_info_sender.packets > 0
        {
            nack_percent = delta_info_sender.nacks.wrapping_mul(100) / delta_info_sender.packets;
        }

        let mut target_delay = jitter.wrapping_mul(JITTER_MULTI_TO_DELAY);
        if nack_percent > 60 {
            target_delay = target_delay.wrapping_add((nack_percent - 60).wrapping_mul(2));
        }

        let elapsed = time::since(inner.sending_at_time);
        let mut delay_change_limit = (MAX_DELAY_CHANGE_PER_SEC as f64 * elapsed.seconds()) as u32;
        if delay_change_limit > MAX_DELAY_CHANGE_PER_SEC {
            delay_change_limit = MAX_DELAY_CHANGE_PER_SEC;
        }

        if target_delay > inner.current_delay.wrapping_add(delay_change_limit) {
            target_delay = inner.current_delay.wrapping_add(delay_change_limit);
        } else if inner.current_delay > target_delay.wrapping_add(delay_change_limit) {
            target_delay = inner.current_delay.wrapping_sub(delay_change_limit);
        }
        if target_delay < self.min_delay {
            target_delay = self.min_delay;
        }
        if target_delay > self.max_delay {
            target_delay = self.max_delay;
        }
        if inner.current_delay == target_delay {
            return;
        }
        if target_delay > TARGET_DELAY_LOG_THRESHOLD && self.high_delay_count.add(1) % 100 == 1 {
            infow!(
                self.logger,
                "high playout delay",
                "target" => target_delay,
                "jitter" => jitter,
                "nackPercent" => nack_percent,
                "current" => inner.current_delay,
            );
        }
        inner.current_delay = target_delay;
        let _ = self.create_ext_data_locked(&inner);
    }

    pub fn on_seq_acked(&self, seq: u16) {
        let inner = self.lock.lock();
        if PlayoutDelayState::from_i32(self.state.load()) == Some(PlayoutDelayState::Sending)
            && seq.wrapping_sub(inner.sending_at_seq) < 0x8000
        {
            self.state.store(PlayoutDelayState::Acked as i32);
        }
    }

    pub fn get_delay_extension(&self, seq: u16) -> Option<Bytes> {
        match PlayoutDelayState::from_i32(self.state.load()) {
            Some(PlayoutDelayState::StateChanged) => {
                {
                    let mut inner = self.lock.lock();
                    self.state.store(PlayoutDelayState::Sending as i32);
                    inner.sending_at_seq = seq;
                    inner.sending_at_time = time::now();
                }
                self.ext_bytes.load()
            }
            Some(PlayoutDelayState::Sending) => self.ext_bytes.load(),
            Some(PlayoutDelayState::Acked) => None,
            None => None,
        }
    }

    fn create_ext_data_locked(&self, inner: &PlayoutDelayControllerInner) -> Result<(), pd::Error> {
        let delay = pd::playout_delay_from_value(inner.current_delay as u16, self.max_delay as u16);
        match delay.marshal() {
            Ok(b) => {
                self.ext_bytes.store(Bytes::from(b));
                self.state.store(PlayoutDelayState::StateChanged as i32);
                Ok(())
            }
            Err(err) => {
                errorw!(self.logger, "failed to marshal playout delay", err, "playoutDelay" => delay);
                Err(err)
            }
        }
    }
}

#[cfg(test)]
#[path = "playoutdelay_test.rs"]
mod playoutdelay_test;
