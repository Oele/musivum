// Ported from LiveKit, pkg/rtc/signalling/signallingunimplemented.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. What the methods of the original do, which is to
// return nil, is the default bodies of the methods of `ParticipantSignalling`
// (`interfaces.rs`), and this is the signalling that has nothing but those. Nothing embeds
// it: an implementation that wants the default of a method leaves the method out.

use super::interfaces::ParticipantSignalling;

pub struct SignallingUnimplemented;

impl ParticipantSignalling for SignallingUnimplemented {}
