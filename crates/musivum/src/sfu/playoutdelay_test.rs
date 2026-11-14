// Ported from LiveKit, pkg/sfu/playoutdelay_test.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use std::sync::Arc;

use super::*;
use crate::go::time::MILLISECOND;
use crate::protocol::logger;

#[test]
fn test_playout_delay() {
    let stats = Arc::new(rtpstats::RtpStatsSender::new(
        rtpstats::RtpStatsParams::default(),
        128,
    ));
    stats.set_clock_rate(90000);

    let c = PlayoutDelayController::new(100, 120, logger::get_logger(), stats)
        .expect("a playout delay controller");

    let mut ext = c.get_delay_extension(100);
    playout_delay_equal(ext.as_deref(), 100, 120);

    ext = c.get_delay_extension(105);
    playout_delay_equal(ext.as_deref(), 100, 120);

    // seq acked before delay changed
    c.on_seq_acked(65534);
    ext = c.get_delay_extension(105);
    playout_delay_equal(ext.as_deref(), 100, 120);

    c.on_seq_acked(90);
    ext = c.get_delay_extension(105);
    playout_delay_equal(ext.as_deref(), 100, 120);

    // seq acked, no extension sent for new packet
    c.on_seq_acked(103);
    ext = c.get_delay_extension(106);
    assert!(ext.is_none());

    // delay on change(can't go below min), no extension sent
    c.set_jitter(0);
    ext = c.get_delay_extension(107);
    assert!(ext.is_none());

    // delay changed, generate new extension to send
    std::thread::sleep((200 * MILLISECOND).to_std());
    c.set_jitter(50);
    println!("{} {}", c.lock.lock().current_delay, c.state.load());
    ext = c.get_delay_extension(108);
    let mut delay = pd::PlayOutDelay::default();
    // no extension is Go's nil slice
    delay
        .unmarshal(ext.as_deref().unwrap_or_default())
        .expect("a playout delay");
    assert!(delay.min > 100u16);

    // can't go above max
    std::thread::sleep((200 * MILLISECOND).to_std());
    c.set_jitter(10000);
    ext = c.get_delay_extension(109);
    playout_delay_equal(ext.as_deref(), 120, 120);
}

#[track_caller]
fn playout_delay_equal(data: Option<&[u8]>, min: u16, max: u16) {
    let mut delay = pd::PlayOutDelay::default();
    // no extension is Go's nil slice
    delay
        .unmarshal(data.unwrap_or_default())
        .expect("a playout delay");
    assert_eq!(min, delay.min);
    assert_eq!(max, delay.max);
}
