// Ported from LiveKit, pkg/sfu/videolayerselector/framenumberwrapper_test.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the random frame numbers come from a small generator
// of this file instead of `math/rand` (what is asserted holds for every sequence); the
// closure `testFrameOrder` is a function that is given the two wrappers; the logger is not
// set to the debug level, which the logger here takes from `tracing`.

use super::*;
use crate::mediatransportutil::utils;
use crate::protocol::logger;

// Stands in for `math/rand`.
struct Rng(u64);

impl Rng {
    // splitmix64
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    // `rand.Intn(n)`
    fn intn(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[track_caller]
fn test_frame_order(
    fn_wrap: &mut FrameNumberWrapper,
    fn_wrap_around: &mut utils::WrapAround<u16, u64>,
    frame: u16,
    is_key_frame: bool,
    frame2: u16,
    is_key_frame2: bool,
    expect_inorder: bool,
) {
    let frame_unwrap = fn_wrap_around.update(frame).extended_val;
    let wrapped_frame = fn_wrap.update_and_get(frame_unwrap, is_key_frame) as u16;

    // make sure wrap around always get in order frame number
    fn_wrap_around.update(frame.wrapping_add(frame2.wrapping_sub(frame) / 2));

    let frame2_unwrap = fn_wrap_around.update(frame2).extended_val;
    let wrapped_frame2 = fn_wrap.update_and_get(frame2_unwrap, is_key_frame2) as u16;
    // keeps order
    assert_eq!(
        expect_inorder,
        in_order(wrapped_frame2, wrapped_frame),
        "frame {frame}, frame2 {frame2}, wrappedFrame {wrapped_frame}, wrapped Frame2 {wrapped_frame2}, frameUnwrap {frame_unwrap}, frame2Unwrap {frame2_unwrap}"
    );
    // frame number diff should be the same if frame2 is not a key frame
    if !is_key_frame2 {
        assert_eq!(
            frame2.wrapping_sub(frame),
            wrapped_frame2.wrapping_sub(wrapped_frame)
        );
    }
}

#[test]
fn test_frame_number_wrapper() {
    let mut rng = Rng(0x006d_7573_6976_756d);

    let mut fn_wrap = FrameNumberWrapper {
        logger: logger::get_logger(),
        ..Default::default()
    };

    let mut fn_wrap_around = utils::WrapAround::<u16, u64>::new(utils::WrapAroundParams {
        is_restart_allowed: false,
    });

    let mut first_f = 1000u16;

    let mut second_f = get_frame(&mut rng, first_f, true);
    test_frame_order(
        &mut fn_wrap,
        &mut fn_wrap_around,
        first_f,
        true,
        second_f,
        false,
        true,
    );

    // non key frame keeps diff and order
    for _ in 0..100 {
        // frame in order
        first_f = second_f;
        second_f = get_frame(&mut rng, first_f, true);
        test_frame_order(
            &mut fn_wrap,
            &mut fn_wrap_around,
            first_f,
            false,
            second_f,
            false,
            true,
        );

        // frame out of order
        first_f = second_f;
        second_f = get_frame(&mut rng, first_f, false);
        // it is possible that an out of order non-keyframe has been converted to in order frame number if the diff is 32768
        // that is ok because the client can't decode in such case and always need to wait for the key frame.
        // so it is just a failure of test case and increase the frame number here.
        if second_f.wrapping_sub(first_f) == 0x8000 {
            second_f = second_f.wrapping_add(1);
        }
        test_frame_order(
            &mut fn_wrap,
            &mut fn_wrap_around,
            first_f,
            false,
            second_f,
            false,
            false,
        );

        // key frame in order
        first_f = second_f;
        second_f = get_frame(&mut rng, first_f, true);
        test_frame_order(
            &mut fn_wrap,
            &mut fn_wrap_around,
            first_f,
            false,
            second_f,
            true,
            true,
        );

        // frame in order
        first_f = second_f;
        second_f = get_frame(&mut rng, first_f, true);
        test_frame_order(
            &mut fn_wrap,
            &mut fn_wrap_around,
            first_f,
            false,
            second_f,
            false,
            true,
        );

        // key frame out of order but should be in order after wrap around
        first_f = second_f;
        second_f = get_frame(&mut rng, first_f, false);
        test_frame_order(
            &mut fn_wrap,
            &mut fn_wrap_around,
            first_f,
            false,
            second_f,
            true,
            true,
        );
    }
}

fn in_order(a: u16, b: u16) -> bool {
    a.wrapping_sub(b) < 0x8000 || (a.wrapping_sub(b) == 0x8000 && a > b)
}

fn get_frame(rng: &mut Rng, base: u16, inorder: bool) -> u16 {
    if inorder {
        return base.wrapping_add(rng.intn(0x8000) as u16);
    }

    loop {
        let ret = base
            .wrapping_add(rng.intn(0x8000) as u16)
            .wrapping_add(0x8000);
        if !in_order(ret, base) {
            return ret;
        }
    }
}
