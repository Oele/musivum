// Ported from LiveKit's mediatransportutil, pkg/rtcconfig/rtc_unix.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the option is set and read on the socket itself (the
// crate `nix`), where the original reads it from a duplicate of the descriptor and, when it
// cannot make one, reports a buffer of 0 without an error.

use std::io;
use std::net::{Ipv4Addr, UdpSocket};

use nix::sys::socket::{getsockopt, setsockopt, sockopt};

use crate::protocol::logger;
use crate::{debugw, warnw};

use super::config::{DEFAULT_UDP_BUFFER_SIZE, MIN_UDP_BUFFER_SIZE};

pub(super) fn check_udp_read_buffer() {
    if let Ok(val) = get_udp_read_buffer() {
        if val < MIN_UDP_BUFFER_SIZE {
            warnw!(logger::get_logger(), "UDP receive buffer is too small for a production set-up", nil,
                "current" => val,
                "suggested" => MIN_UDP_BUFFER_SIZE);
        } else {
            debugw!(logger::get_logger(), "UDP receive buffer size", "current" => val);
        }
    }
}

pub(super) fn get_udp_read_buffer() -> io::Result<usize> {
    let conn = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    let _ = setsockopt(&conn, sockopt::RcvBuf, &DEFAULT_UDP_BUFFER_SIZE);

    Ok(getsockopt(&conn, sockopt::RcvBuf)?)
}
