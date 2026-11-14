// Ported from LiveKit's protocol, auth/verifier.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The JWT library of the original is `jwt.rs`. The key
// is bytes or text where the original takes `interface{}`: an empty key of either kind is
// `Error::KeysMissing` (the original says that of nil and of the empty string only, and
// finds the signature invalid for empty bytes).

use super::accesstoken::TokenClaims;
use super::grants::ClaimGrants;
use super::interfaces::Error;
use super::jwt::{self, RegisteredClaims};
use crate::go::time::{self, Duration};

const TOKEN_LEEWAY: Duration = time::MINUTE;

const ALLOWED_SIGNING_METHODS: &[&str] = &[jwt::SIGNING_METHOD_HS256];
// unverifiedParser of the original is jwt::parse_unverified: a parser looks at the methods
// it was told are valid only when it also verifies.

pub struct ApiKeyTokenVerifier {
    raw: String,
    identity: String,
    api_key: String,
}

pub fn parse_api_token(raw: &str) -> Result<ApiKeyTokenVerifier, Error> {
    let claims = jwt::parse_unverified::<RegisteredClaims>(raw)?.claims;

    let mut v = ApiKeyTokenVerifier {
        raw: raw.to_string(),
        api_key: claims.issuer,
        identity: claims.subject,
    };
    if v.identity.is_empty() {
        v.identity = claims.id;
    }
    Ok(v)
}

impl ApiKeyTokenVerifier {
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn verify(&self, key: impl AsRef<[u8]>) -> Result<(RegisteredClaims, ClaimGrants), Error> {
        let key = key.as_ref();
        if key.is_empty() {
            return Err(Error::KeysMissing);
        }

        let parser = jwt::Parser {
            valid_methods: ALLOWED_SIGNING_METHODS,
            issuer: &self.api_key,
            leeway: TOKEN_LEEWAY,
            // Without this, a token carrying no exp claim passes validation and
            // never expires. First-party SDKs always set exp, but any hand-rolled
            // or third-party minter that forgets it silently mints a permanent
            // credential.
            expiration_required: true,
        };
        let mut claims = parser.parse_with_claims::<TokenClaims>(&self.raw, key)?;

        claims.claim_grants.identity = self.identity.clone();
        Ok((claims.registered_claims, claims.claim_grants))
    }
}

#[cfg(test)]
#[path = "verifier_test.rs"]
mod verifier_test;
