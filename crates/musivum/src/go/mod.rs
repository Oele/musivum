//! Stand-ins for what the ported code uses of Go's standard library and of
//! `go.uber.org/atomic`, so that a ported file reads like its original. Written new.

pub mod atomic;
pub mod time;
