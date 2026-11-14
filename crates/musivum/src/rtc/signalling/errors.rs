// Ported from LiveKit, pkg/rtc/signalling/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, the one of the package.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid message type")]
    InvalidMessageType,
    #[error("name length exceeds limits")]
    NameExceedsLimits,
    #[error("metadata size exceeds limits")]
    MetadataExceedsLimits,
    #[error("attributes size exceeds limits")]
    AttributesExceedsLimits,
    #[error("update own metadata not allowed")]
    UpdateOwnMetadataNotAllowed,
}
