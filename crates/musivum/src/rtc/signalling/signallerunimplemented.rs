// Ported from LiveKit, pkg/rtc/signalling/signallerunimplemented.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. What the methods of the original do is the default
// bodies of the methods of `ParticipantSignaller` (`interfaces.rs`), and this is the
// signaller that has nothing but those. Nothing embeds it, as `signallerAsyncBase` does
// upstream: an implementation that wants the default of a method leaves the method out.

use super::interfaces::ParticipantSignaller;

pub struct SignallerUnimplemented;

impl ParticipantSignaller for SignallerUnimplemented {}
