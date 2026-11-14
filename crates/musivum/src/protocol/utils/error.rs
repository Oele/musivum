// Ported from LiveKit's protocol, utils/error.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust; an error is one of the targets when it is equal to it
// (`==`), where Go's `errors.Is` also follows what an error wraps: an error that carries
// details of its own is for the caller to match. An error that may be nil is an `Option`.

pub fn error_is_one_of<E: PartialEq>(err: &E, targets: &[E]) -> bool {
    for t in targets {
        if err == t {
            return true;
        }
    }
    false
}

pub fn screen_error<E: PartialEq>(err: Option<E>, ignored: &[E]) -> Option<E> {
    let err = err?;
    if error_is_one_of(&err, ignored) {
        return None;
    }
    Some(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: upstream has no test for this file.
    #[test]
    fn an_error_is_found_among_the_targets_and_screened() {
        #[derive(Debug, Clone, Copy, PartialEq)]
        enum Error {
            Closed,
            NoReceiver,
            Other,
        }

        assert!(error_is_one_of(
            &Error::Closed,
            &[Error::NoReceiver, Error::Closed]
        ));
        assert!(!error_is_one_of(
            &Error::Other,
            &[Error::NoReceiver, Error::Closed]
        ));
        assert!(!error_is_one_of(&Error::Other, &[]));

        assert_eq!(None, screen_error(Some(Error::Closed), &[Error::Closed]));
        assert_eq!(
            Some(Error::Other),
            screen_error(Some(Error::Other), &[Error::Closed])
        );
        assert_eq!(None, screen_error(None, &[Error::Closed]));
    }
}
