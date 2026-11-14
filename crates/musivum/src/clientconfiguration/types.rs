// Ported from LiveKit, pkg/clientconfiguration/types.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the client's information is a reference, which cannot
// be nil (see staticconfiguration.rs), and the configuration is given by value, `None` where
// the original gives nil.

use crate::protocol::livekit;

pub trait ClientConfigurationManager: Send + Sync {
    fn get_configuration(
        &self,
        client_info: &livekit::ClientInfo,
    ) -> Option<livekit::ClientConfiguration>;
}
