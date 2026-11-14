// Ported from LiveKit, pkg/rtc/signalling/signallerasyncbase.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. What `resSinkMu` guards is in an inner struct, and the
// functions the original calls with the lock held take that. The base is made as an `Arc`
// and its timer reaches it through a `Weak`: a timer that fires after the base is gone does
// nothing, where in Go the timer keeps the base alive until it has fired. The embedded
// `signallerUnimplemented` is not here: it gives the original's base a `WriteMessage`, which
// `SignallerAsync` defines, and the base itself is not a `ParticipantSignaller`.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::go::time::{self, Duration, Timer};
use crate::protocol::logger::Logger;
use crate::routing::MessageSink;
use crate::rtc::types::SignallingCloseReason;
use crate::{debugw, warnw};

pub(super) struct SignallerAsyncBaseParams {
    pub logger: Logger,
    // called when the connection opens, i. e. when messages held back for the
    // handshake may be sent
    pub on_handshake_opened: Option<Box<dyn Fn() + Send + Sync>>,
}

// how long a resumed connection holds messages back waiting for its ReconnectResponse.
// A path that resumes without sending one lets messages flow after this instead of
// holding them back for the rest of the session.
const HANDSHAKE_WINDOW: Duration = Duration::from_secs(5);

pub(super) struct SignallerAsyncBase {
    params: SignallerAsyncBaseParams,

    // New, see the header.
    this: Weak<SignallerAsyncBase>,

    res_sink_mu: Mutex<SignallerAsyncBaseInner>,
}

struct SignallerAsyncBaseInner {
    res_sink: Option<Arc<dyn MessageSink>>,
    // set while a resumed connection holds messages back waiting for its
    // ReconnectResponse, the timer opens the connection if none is written.
    // handshakeGeneration tells a timeout whether it belongs to the current wait, a
    // Stop cannot cancel a callback that has already started running.
    handshake_pending: bool,
    handshake_generation: u32,
    handshake_timer: Option<Timer>,
}

impl SignallerAsyncBase {
    pub(super) fn new(params: SignallerAsyncBaseParams) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            params,
            this: this.clone(),
            res_sink_mu: Mutex::new(SignallerAsyncBaseInner {
                res_sink: None,
                handshake_pending: false,
                handshake_generation: 0,
                handshake_timer: None,
            }),
        })
    }

    pub(super) fn swap_response_sink(
        &self,
        sink: Option<Arc<dyn MessageSink>>,
        reason: SignallingCloseReason,
    ) {
        let mut inner = self.res_sink_mu.lock();
        let old_sink = std::mem::replace(&mut inner.res_sink, sink.clone());
        // a resumed connection has to open with the ReconnectResponse, the client takes it
        // only as the first message it reads
        let mut opened = false;
        match &sink {
            None => {
                // the connection is gone, keep anything queued for the next one
                self.disarm_handshake_locked(&mut inner);
            }
            Some(_) if reason == SignallingCloseReason::Resume => {
                self.arm_handshake_locked(&mut inner);
            }
            Some(_) => {
                opened = self.disarm_handshake_locked(&mut inner);
            }
        }
        drop(inner);

        if opened {
            self.notify_handshake_opened();
        }

        if let Some(old_sink) = old_sink {
            if let Some(sink) = &sink {
                debugw!(
                    self.params.logger,
                    "swapping signal connection",
                    "reason" => format_args!("{reason}"),
                    "connID" => old_sink.connection_id(),
                    "newConnID" => sink.connection_id(),
                );
            } else {
                debugw!(
                    self.params.logger,
                    "closing signal connection",
                    "reason" => format_args!("{reason}"),
                    "connID" => old_sink.connection_id(),
                );
            }
            old_sink.close();
        }
    }

    // HandshakePending is a plain read, so a caller can decide to hold a message back
    // while holding its own lock
    pub(super) fn handshake_pending(&self) -> bool {
        let inner = self.res_sink_mu.lock();

        inner.handshake_pending
    }

    pub(super) fn open_handshake(&self) {
        let mut inner = self.res_sink_mu.lock();
        let opened = self.disarm_handshake_locked(&mut inner);
        drop(inner);

        if opened {
            self.notify_handshake_opened();
        }
    }

    fn arm_handshake_locked(&self, inner: &mut SignallerAsyncBaseInner) {
        self.stop_handshake_timer_locked(inner);

        inner.handshake_pending = true;
        inner.handshake_generation = inner.handshake_generation.wrapping_add(1);
        let generation = inner.handshake_generation;
        // the connection opens on its own if nothing writes a ReconnectResponse, a path
        // that resumes without one should not hold messages back for the whole session
        let this = self.this.clone();
        inner.handshake_timer = Some(time::after_func(HANDSHAKE_WINDOW, move || {
            if let Some(s) = this.upgrade() {
                s.on_handshake_timeout(generation);
            }
        }));
    }

    // disarmHandshakeLocked reports whether it opened a connection that was holding
    // messages back
    fn disarm_handshake_locked(&self, inner: &mut SignallerAsyncBaseInner) -> bool {
        self.stop_handshake_timer_locked(inner);

        let was_pending = inner.handshake_pending;
        inner.handshake_pending = false;
        // a timeout of the wait that just ended is stale
        inner.handshake_generation = inner.handshake_generation.wrapping_add(1);
        was_pending
    }

    fn stop_handshake_timer_locked(&self, inner: &mut SignallerAsyncBaseInner) {
        if let Some(handshake_timer) = inner.handshake_timer.take() {
            handshake_timer.stop();
        }
    }

    fn on_handshake_timeout(&self, generation: u32) {
        let mut inner = self.res_sink_mu.lock();
        if generation != inner.handshake_generation {
            // the wait this timeout was armed for has ended, a later one may be in progress
            return;
        }
        let opened = self.disarm_handshake_locked(&mut inner);
        drop(inner);

        if !opened {
            return;
        }

        warnw!(
            self.params.logger,
            "resumed connection did not open with a ReconnectResponse",
            nil
        );
        self.notify_handshake_opened();
    }

    // notifyHandshakeOpened runs without resSinkMu held, the callback sends messages
    fn notify_handshake_opened(&self) {
        if let Some(on_handshake_opened) = &self.params.on_handshake_opened {
            on_handshake_opened();
        }
    }

    pub(super) fn get_response_sink(&self) -> Option<Arc<dyn MessageSink>> {
        let inner = self.res_sink_mu.lock();
        inner.res_sink.clone()
    }

    // closes signal connection to notify client to resume/reconnect
    pub(super) fn close_signal_connection(&self, reason: SignallingCloseReason) {
        self.swap_response_sink(None, reason);
    }
}

#[cfg(test)]
#[path = "signallerasyncbase_test.rs"]
mod signallerasyncbase_test;
