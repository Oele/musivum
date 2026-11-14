// Ported from LiveKit's protocol, auth/interfaces.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors of the package are one enum, which also
// carries what the original passes on from its JWT and YAML libraries; a key is bytes where
// the original takes `interface{}`. The generated fakes (`authfakes`) are not ported.

use super::grants::ClaimGrants;
use super::jwt;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("missing API key or secret key")]
    KeysMissing,
    // ErrSensitiveCredentials of grants.go
    #[error("room configuration should not contain sensitive credentials")]
    SensitiveCredentials,
    /// What the JWT library says of a token it does not take: `jwt.ErrTokenMalformed` and
    /// the others, which the original returns as they are.
    #[error(transparent)]
    Jwt(#[from] jwt::Error),
    /// The claims could not be written as JSON.
    #[error("{0}")]
    Json(String),
    /// What the YAML decoder says of a key file.
    #[error("yaml: {0}")]
    Yaml(String),
    /// A key file without a document in it, for which the YAML decoder of the original
    /// returns `io.EOF`.
    #[error("EOF")]
    Eof,
}

// As in the original, nothing in this package is a TokenVerifier: `Verify` of
// `APIKeyTokenVerifier` gives the registered claims too, which this one does not. The
// original has a generated fake of it.
pub trait TokenVerifier: Send + Sync {
    fn identity(&self) -> String;
    fn verify(&self, key: &[u8]) -> Result<ClaimGrants, Error>;
}

pub trait KeyProvider: Send + Sync {
    fn get_secret(&self, key: &str) -> String;
    fn num_keys(&self) -> usize;
}
