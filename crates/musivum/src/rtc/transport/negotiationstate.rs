// Ported from LiveKit, pkg/rtc/transport/negotiationstate.go at v1.13.11.
// Copyright 2024 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NegotiationState` is an enum and its `String()` is
// `Display`, without the text for a number that is none of the states, which an enum cannot
// hold. The test at the end is new: the original has none.

use std::fmt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum NegotiationState {
    #[default]
    None,
    // waiting for remote description
    Remote,
    // need to Negotiate again
    Retry,
}

impl fmt::Display for NegotiationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NegotiationState::None => "NONE",
            NegotiationState::Remote => "WAITING_FOR_REMOTE",
            NegotiationState::Retry => "RETRY",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The texts are the original's.
    #[test]
    fn negotiation_states_have_the_texts_of_the_original() {
        assert_eq!(NegotiationState::None.to_string(), "NONE");
        assert_eq!(NegotiationState::Remote.to_string(), "WAITING_FOR_REMOTE");
        assert_eq!(NegotiationState::Retry.to_string(), "RETRY");
        assert_eq!(NegotiationState::default(), NegotiationState::None);
    }
}
