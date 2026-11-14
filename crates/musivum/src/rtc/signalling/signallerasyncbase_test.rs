// Ported from LiveKit, pkg/rtc/signalling/signallerasyncbase_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust; the subtests are blocks, and their names are in the
// messages of their assertions.

use std::sync::Arc;

use super::*;
use crate::go::atomic;
use crate::protocol::logger;
use crate::routing::FakeMessageSink;
use crate::testutils::eventually_async;

type OnHandshakeOpened = Option<Box<dyn Fn() + Send + Sync>>;

fn new_test_signaller_base(on_handshake_opened: OnHandshakeOpened) -> Arc<SignallerAsyncBase> {
    SignallerAsyncBase::new(SignallerAsyncBaseParams {
        logger: logger::get_logger(),
        on_handshake_opened,
    })
}

// currentHandshakeGeneration is what a timeout armed right now would be tagged with
fn current_handshake_generation(s: &SignallerAsyncBase) -> u32 {
    let inner = s.res_sink_mu.lock();

    inner.handshake_generation
}

// `&routingfakes.FakeMessageSink{}`
fn fake_message_sink() -> Option<Arc<dyn MessageSink>> {
    Some(Arc::new(FakeMessageSink::default()))
}

// `var opened atomic.Int32` and `func() { opened.Inc() }`
fn opened_counter() -> (Arc<atomic::Int32>, OnHandshakeOpened) {
    let opened = Arc::new(atomic::Int32::new(0));
    let counter = opened.clone();
    (
        opened,
        Some(Box::new(move || {
            counter.inc();
        })),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handshake_gate() {
    {
        let name = "a resumed connection holds messages back";
        let s = new_test_signaller_base(None);
        assert!(!s.handshake_pending(), "{name}");

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        assert!(s.handshake_pending(), "{name}");

        s.open_handshake();
        assert!(!s.handshake_pending(), "{name}");
    }

    {
        let name = "other sink swaps do not hold messages back";
        let s = new_test_signaller_base(None);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Unknown);
        assert!(!s.handshake_pending(), "{name}");
    }

    {
        let name = "closing the connection clears the gate";
        let s = new_test_signaller_base(None);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        assert!(s.handshake_pending(), "{name}");

        s.close_signal_connection(SignallingCloseReason::ParticipantClose);
        assert!(!s.handshake_pending(), "{name}");
    }

    {
        let name = "the handshake window opens the gate";
        let s = new_test_signaller_base(None);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        assert!(s.handshake_pending(), "{name}");

        s.on_handshake_timeout(current_handshake_generation(&s));
        assert!(!s.handshake_pending(), "{name}");
    }

    {
        let name = "a swap to a fresh connection opens the gate";
        let s = new_test_signaller_base(None);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        assert!(s.handshake_pending(), "{name}");

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Unknown);
        assert!(!s.handshake_pending(), "{name}");
    }

    {
        let name = "a timeout of a wait that has ended does nothing";
        let (opened, on_handshake_opened) = opened_counter();
        let s = new_test_signaller_base(on_handshake_opened);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        let stale = current_handshake_generation(&s);

        // the client resumes again while the first timeout is running, Stop cannot
        // cancel a callback that has already started
        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        s.on_handshake_timeout(stale);

        assert!(s.handshake_pending(), "{name}");
        assert_eq!(0, opened.load(), "{name}");

        // and the wait in progress still opens on its own timeout
        s.on_handshake_timeout(current_handshake_generation(&s));
        assert!(!s.handshake_pending(), "{name}");
        assert_eq!(1, opened.load(), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handshake_gate_notifies_on_open() {
    {
        let name = "on an explicit open";
        let (opened, on_handshake_opened) = opened_counter();
        let s = new_test_signaller_base(on_handshake_opened);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        assert_eq!(0, opened.load(), "{name}");

        s.open_handshake();
        assert_eq!(1, opened.load(), "{name}");

        // only the transition notifies
        s.open_handshake();
        assert_eq!(1, opened.load(), "{name}");
    }

    {
        let name = "on the handshake window expiring";
        let (opened, on_handshake_opened) = opened_counter();
        let s = new_test_signaller_base(on_handshake_opened);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        s.on_handshake_timeout(current_handshake_generation(&s));

        assert!(!s.handshake_pending(), "{name}");
        assert_eq!(1, opened.load(), "{name}");
    }

    {
        // the window fires without anything else touching the gate
        let (opened, on_handshake_opened) = opened_counter();
        let s = new_test_signaller_base(on_handshake_opened);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        eventually_async(
            || !s.handshake_pending() && opened.load() == 1,
            (2 * HANDSHAKE_WINDOW).to_std(),
            std::time::Duration::from_millis(100),
        )
        .await;
    }

    {
        let name = "not on a connection close";
        let (opened, on_handshake_opened) = opened_counter();
        let s = new_test_signaller_base(on_handshake_opened);

        s.swap_response_sink(fake_message_sink(), SignallingCloseReason::Resume);
        s.close_signal_connection(SignallingCloseReason::ParticipantClose);

        assert!(!s.handshake_pending(), "{name}");
        assert_eq!(0, opened.load(), "{name}");
    }

    {
        let name = "not when the gate was never armed";
        let (opened, on_handshake_opened) = opened_counter();
        let s = new_test_signaller_base(on_handshake_opened);

        s.open_handshake();
        assert_eq!(0, opened.load(), "{name}");
    }
}
