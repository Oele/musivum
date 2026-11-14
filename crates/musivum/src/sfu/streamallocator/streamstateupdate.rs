// Ported from LiveKit, pkg/sfu/streamallocator/streamstateupdate.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `StreamState` is an enum and its `String()` is
// `Display`; an update holds its infos by value, where the original has pointers to them.
// The test at the end is new: the original has none.

use std::fmt;

use crate::protocol::livekit;

use super::Track;

// ------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StreamState {
    #[default]
    Inactive,
    Active,
    Paused,
}

impl fmt::Display for StreamState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StreamState::Inactive => "INACTIVE",
            StreamState::Active => "ACTIVE",
            StreamState::Paused => "PAUSED",
        })
    }
}

// ------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamStateInfo {
    pub participant_id: livekit::ParticipantId,
    pub track_id: livekit::TrackId,
    pub state: StreamState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamStateUpdate {
    pub stream_states: Vec<StreamStateInfo>,
}

impl StreamStateUpdate {
    pub fn new() -> StreamStateUpdate {
        StreamStateUpdate::default()
    }

    pub fn handle_streaming_change(&mut self, track: &Track, stream_state: StreamState) {
        match stream_state {
            StreamState::Inactive => {
                // inactive is not a notification, could get into this state because of mute
            }
            StreamState::Active => {
                self.stream_states.push(StreamStateInfo {
                    participant_id: track.publisher_id(),
                    track_id: track.id(),
                    state: StreamState::Active,
                });
            }
            StreamState::Paused => {
                self.stream_states.push(StreamStateInfo {
                    participant_id: track.publisher_id(),
                    track_id: track.id(),
                    state: StreamState::Paused,
                });
            }
        }
    }

    pub fn empty(&self) -> bool {
        self.stream_states.is_empty()
    }
}

// ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the original has no tests. The texts are the original's.
    #[test]
    fn the_stream_states_have_the_texts_of_the_original() {
        assert_eq!(StreamState::Inactive.to_string(), "INACTIVE");
        assert_eq!(StreamState::Active.to_string(), "ACTIVE");
        assert_eq!(StreamState::Paused.to_string(), "PAUSED");
        assert_eq!(StreamState::default(), StreamState::Inactive);
        assert!(StreamStateUpdate::new().empty());
    }
}
