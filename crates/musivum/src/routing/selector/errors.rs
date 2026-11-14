// Ported from LiveKit, pkg/routing/selector/errors.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, the one of the package, with
// `ErrUnsupportedSelector` of `interfaces.go` in it. Not here: `ErrCurrentRegionNotSet` and
// `ErrCurrentRegionUnknownLatLon`, which only the region aware selector returns (not ported:
// one node has no other region to prefer).

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("could not find any available nodes")]
    NoAvailableNodes,
    #[error("sort by option cannot be blank")]
    SortByNotSet,
    #[error("node selector algorithm option cannot be blank")]
    AlgorithmNotSet,
    #[error("unknown sort by option")]
    SortByUnknown,
    #[error("unknown node selector algorithm option")]
    AlgorithmUnknown,

    // interfaces.go
    #[error("unsupported node selector")]
    UnsupportedSelector,
}
