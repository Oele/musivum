//! Stand-ins for what the ported code uses of Go's standard library and of
//! `go.uber.org/atomic`, so that a ported file reads like its original. Written new; and
//! [`core`], ported from `github.com/frostbyte73/core`.

pub mod atomic;
pub mod core;
pub mod sync;
pub mod time;
