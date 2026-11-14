// Ported from LiveKit's protocol, utils/pointer/to.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the pointer is what a pointer that may be nil is in
// the port, an `Option<Arc<T>>`, which is what `crate::go::atomic::Pointer` stores: the
// original is used to store a value in one.

use std::sync::Arc;

pub fn to<T>(v: T) -> Option<Arc<T>> {
    Some(Arc::new(v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::atomic;

    // New for musivum: upstream has no test for this package.
    #[test]
    fn a_value_becomes_a_pointer_that_can_be_stored() {
        let p: atomic::Pointer<u32> = atomic::Pointer::default();
        assert!(p.load().is_none());
        p.store(to(7));
        assert_eq!(7, *p.load().unwrap());
    }
}
