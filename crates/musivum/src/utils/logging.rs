// Ported from LiveKit, pkg/utils/logging.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

pub const COMPONENT_PUB: &str = "pub";
pub const COMPONENT_SUB: &str = "sub";
pub const COMPONENT_ROOM: &str = "room";
pub const COMPONENT_API: &str = "api";
pub const COMPONENT_TRANSPORT: &str = "transport";
pub const COMPONENT_SFU: &str = "sfu";
// transport subcomponents
pub const COMPONENT_CONGESTION_CONTROL: &str = "cc";
