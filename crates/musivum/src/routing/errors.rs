// Ported from LiveKit, pkg/routing/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, the one of the package. Only
// the errors that the ported files of the package return are here, the two of the message
// channel. Not here: `ErrNotFound`, `ErrIPNotSet`, `ErrHandlerNotDefined`,
// `ErrIncorrectRTCNode`, `ErrNodeNotFound`, `ErrNodeLimitReached` and
// `ErrInvalidRouterMessage` (of the routers and the nodes, which are not ported),
// `ErrRequestChannelClosed`, `ErrCouldNotMigrateParticipant` and `ErrClientInfoNotSet` (which
// nothing in the original returns).

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("channel closed")]
    ChannelClosed,
    #[error("channel is full")]
    ChannelFull,
}
