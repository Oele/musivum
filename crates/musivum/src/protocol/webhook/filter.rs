// Ported from LiveKit's protocol, webhook/filter.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `SetFilter` takes `&mut self`, its owner guards it.

use super::FilterParams;

#[derive(Clone, Debug, Default)]
pub(super) struct Filter {
    params: FilterParams,
}

pub(super) fn new_filter(params: FilterParams) -> Filter {
    Filter { params }
}

impl Filter {
    pub(super) fn set_filter(&mut self, params: FilterParams) {
        self.params = params;
    }
}

impl FilterParams {
    // IsAllowed reports whether event passes the include/exclude filter. IncludeEvents
    // takes precedence over ExcludeEvents; if neither is set, the event is allowed.
    pub fn is_allowed(&self, event: &str) -> bool {
        new_filter(self.clone()).is_allowed(event)
    }
}

impl Filter {
    // IncludeEvents and ExcludeEvents are mutually exclusive; only one will be checked
    // if neither are set, the event will be allowed
    pub(super) fn is_allowed(&self, event: &str) -> bool {
        // includes get higher precendence than excludes
        if !self.params.include_events.is_empty() {
            return self.params.include_events.iter().any(|e| e == event);
        }

        if !self.params.exclude_events.is_empty() {
            return !self.params.exclude_events.iter().any(|e| e == event);
        }

        true
    }
}
