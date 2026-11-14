// Ported from LiveKit, pkg/rtc/signalling/signalhandlerunimplemented.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. What the method of the original does is the default
// body of the method of `ParticipantSignalHandler` (`interfaces.rs`), and this is the handler
// that has nothing but that. Nothing embeds it: an implementation that wants the default
// leaves the method out.

use super::interfaces::ParticipantSignalHandler;

pub struct SignalHandlerUnimplemented;

impl ParticipantSignalHandler for SignalHandlerUnimplemented {}
