// Ported from LiveKit, pkg/routing/messagechannel_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the goroutines are threads. A write to a closed
// channel that panics in the writing thread fails the test when that thread is joined, where
// in Go it ends the program.

use super::*;

#[test]
fn test_message_channel_write_message_closed() {
    // ensure it doesn't panic when written to after closing
    let m = MessageChannel::new(
        livekit::ConnectionId::from("test"),
        DEFAULT_MESSAGE_CHANNEL_SIZE,
    );
    {
        let m = m.clone();
        std::thread::spawn(move || {
            let read_chan = m.read_chan();
            while let Some(_msg) = read_chan.blocking_recv() {}
        });
    }

    let wg = {
        let m = m.clone();
        std::thread::spawn(move || {
            for _ in 0..100 {
                let _ = m.write_message(livekit::SignalRequest::default().into());
            }
        })
    };
    let _ = m.write_message(livekit::SignalRequest::default().into());
    m.close();
    let _ = m.write_message(livekit::SignalRequest::default().into());

    wg.join().unwrap();
}
