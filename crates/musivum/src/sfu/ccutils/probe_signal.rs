// Ported from LiveKit, pkg/sfu/ccutils/probe_signal.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `ProbeSignal` is an enum and its `String()` is
// `Display`. The test at the end is new: the original has none.

use std::fmt;

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ProbeSignal {
    #[default]
    Inconclusive,
    Congesting,
    NotCongesting,
}

impl fmt::Display for ProbeSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ProbeSignal::Inconclusive => "INCONCLUSIVE",
            ProbeSignal::Congesting => "CONGESTING",
            ProbeSignal::NotCongesting => "NOT_CONGESTING",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The texts are the original's.
    #[test]
    fn probe_signals_have_the_texts_of_the_original() {
        assert_eq!(ProbeSignal::Inconclusive.to_string(), "INCONCLUSIVE");
        assert_eq!(ProbeSignal::Congesting.to_string(), "CONGESTING");
        assert_eq!(ProbeSignal::NotCongesting.to_string(), "NOT_CONGESTING");
        assert_eq!(ProbeSignal::default(), ProbeSignal::Inconclusive);
    }
}
