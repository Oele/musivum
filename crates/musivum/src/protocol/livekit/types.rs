// Ported from LiveKit's protocol, livekit/types.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; only the identifier types are here (the YAML
// conversions and the tracing attributes of the original are not used by musivum). The
// identifiers hold their text behind an `Arc`, so that copying one, which Go does freely with
// its strings, does not allocate.

use std::fmt;
use std::sync::Arc;

macro_rules! string_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Arc<str>);

        impl $name {
            pub fn new(s: &str) -> Self {
                Self(Arc::from(s))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self(Arc::from(""))
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(Arc::from(s))
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(Arc::from(s))
            }
        }

        impl From<&String> for $name {
            fn from(s: &String) -> Self {
                Self(Arc::from(s.as_str()))
            }
        }

        impl From<$name> for String {
            fn from(s: $name) -> String {
                s.0.to_string()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                &*self.0 == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                &*self.0 == *other
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&*self.0, f)
            }
        }
    };
}

string_type!(TrackId);
string_type!(ParticipantId);
string_type!(ParticipantIdentity);
string_type!(ParticipantName);
string_type!(RoomId);
string_type!(RoomName);
string_type!(ConnectionId);
string_type!(NodeId);

#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub struct RoomKey {
    pub project_id: String,
    pub room_name: RoomName,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub struct ParticipantKey {
    pub room_key: RoomKey,
    pub identity: ParticipantIdentity,
}

impl fmt::Display for ParticipantKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.room_key.project_id, self.room_key.room_name, self.identity
        )
    }
}

pub fn ids_as_strings<T: AsRef<str>>(ids: &[T]) -> Vec<String> {
    ids.iter().map(|id| id.as_ref().to_string()).collect()
}

pub fn strings_as_ids<T: for<'a> From<&'a str>>(ids: &[String]) -> Vec<T> {
    ids.iter().map(|id| T::from(id.as_str())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream's types_test.go tests the YAML conversions, which are not ported.
    #[test]
    fn identifiers_are_strings_with_a_type() {
        let id = TrackId::from("TR_abc");
        assert_eq!(id.as_str(), "TR_abc");
        assert_eq!(id, "TR_abc");
        assert_eq!(id.to_string(), "TR_abc");
        assert_eq!(format!("{id:?}"), "\"TR_abc\"");
        assert!(TrackId::default().is_empty());
        assert_eq!(id.clone(), id);

        let ids: Vec<ParticipantId> = strings_as_ids(&["PA_1".to_string(), "PA_2".to_string()]);
        assert_eq!(ids_as_strings(&ids), vec!["PA_1", "PA_2"]);
    }
}
