// Ported from LiveKit, pkg/sfu/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The original file is empty at this version: the
// errors of the package are variables in the files that return them (`downtrack.go`,
// `forwarder.go`, `receiver_base.go`). Here they are the variants of the package's one
// `Error`, each under the name of the file that defines it upstream, and added when the first
// file that returns it is ported.

/// The errors of the package: its `ErrX` and `errX` values, with the original's messages.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    // downtrack.go
    #[error("out-of-order sequence number not found in cache")]
    OutOfOrderSequenceNumberCacheMiss,
    #[error("padding only packet that need not be forwarded")]
    PaddingOnlyPacket,
    #[error("duplicate packet")]
    DuplicatePacket,
    #[error("padding cannot send on non-frame boundary")]
    PaddingNotOnFrameBoundary,
}
