// Ported from LiveKit's protocol, auth/accesstoken.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The setters take the token and give it back, so that
// they chain as the original's do. A grant is handed over to the token (the original keeps
// the caller's pointer, so a change the caller makes later is in the token too); `None`
// takes a grant away, as nil does. `get_grants` gives the grants to read, where the original
// gives a pointer through which they can be changed too. The JWT library of the original is
// `jwt.rs`, and the time is the system's clock (`jwt::time_now`), not `crate::go::time::now`.

use std::collections::HashMap;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use super::grants::{
    AgentEndpointGrant, AgentGrant, AgentThreadGrant, ClaimGrants, InferenceGrant,
    ObservabilityGrant, RoomConfiguration, SipGrant, VideoGrant,
};
use super::interfaces::Error;
use super::json;
use super::jwt::{self, Claims, NumericDate, RegisteredClaims};
use crate::go::time::{self, Duration};
use crate::protocol::livekit;

// tokenClaims combines the JWT registered claims with LiveKit grants for
// signing and verification.
#[derive(Serialize)]
pub(super) struct TokenClaims {
    #[serde(flatten)]
    pub registered_claims: RegisteredClaims,
    #[serde(flatten)]
    pub claim_grants: ClaimGrants,
}

// Go fills the two embedded structs from the one JSON object.
impl<'de> Deserialize<'de> for TokenClaims {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<TokenClaims, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(TokenClaims {
            registered_claims: json::from_value(value.clone()).map_err(D::Error::custom)?,
            claim_grants: json::from_value(value).map_err(D::Error::custom)?,
        })
    }
}

impl Claims for TokenClaims {
    fn get_expiration_time(&self) -> Option<NumericDate> {
        self.registered_claims.get_expiration_time()
    }

    fn get_not_before(&self) -> Option<NumericDate> {
        self.registered_claims.get_not_before()
    }

    fn get_issuer(&self) -> &str {
        self.registered_claims.get_issuer()
    }
}

const DEFAULT_VALID_DURATION: Duration = Duration(6 * time::HOUR.0);

// AccessToken produces token signed with API key and secret
pub struct AccessToken {
    api_key: String,
    secret: String,
    grant: ClaimGrants,
    valid_for: Duration,
    allow_sensitive_credentials: bool,
}

impl AccessToken {
    pub fn new(key: &str, secret: &str) -> AccessToken {
        AccessToken {
            api_key: key.to_string(),
            secret: secret.to_string(),
            grant: ClaimGrants::default(),
            valid_for: Duration::ZERO,
            allow_sensitive_credentials: false,
        }
    }

    pub fn set_identity(mut self, identity: &str) -> AccessToken {
        self.grant.identity = identity.to_string();
        self
    }

    pub fn set_valid_for(mut self, duration: Duration) -> AccessToken {
        self.valid_for = duration;
        self
    }

    pub fn set_name(mut self, name: &str) -> AccessToken {
        self.grant.name = name.to_string();
        self
    }

    pub fn set_kind(mut self, kind: impl Into<i32>) -> AccessToken {
        self.grant.set_participant_kind(kind);
        self
    }

    pub fn set_kind_detail<D: Into<i32> + Copy>(mut self, details: &[D]) -> AccessToken {
        self.grant.set_kind_detail(details);
        self
    }

    // Deprecated: use SetVideoGrant instead
    #[deprecated(note = "use set_video_grant instead")]
    pub fn add_grant(self, grant: impl Into<Option<VideoGrant>>) -> AccessToken {
        self.set_video_grant(grant)
    }

    pub fn set_video_grant(mut self, grant: impl Into<Option<VideoGrant>>) -> AccessToken {
        self.grant.video = grant.into();
        self
    }

    // Deprecated: use SetSIPGrant instead
    #[deprecated(note = "use set_sip_grant instead")]
    pub fn add_sip_grant(self, grant: impl Into<Option<SipGrant>>) -> AccessToken {
        self.set_sip_grant(grant)
    }

    pub fn set_sip_grant(mut self, grant: impl Into<Option<SipGrant>>) -> AccessToken {
        self.grant.sip = grant.into();
        self
    }

    pub fn set_agent_grant(mut self, grant: impl Into<Option<AgentGrant>>) -> AccessToken {
        self.grant.agent = grant.into();
        self
    }

    pub fn set_inference_grant(mut self, grant: impl Into<Option<InferenceGrant>>) -> AccessToken {
        self.grant.inference = grant.into();
        self
    }

    pub fn set_observability_grant(
        mut self,
        grant: impl Into<Option<ObservabilityGrant>>,
    ) -> AccessToken {
        self.grant.observability = grant.into();
        self
    }

    pub fn set_agent_endpoint_grant(
        mut self,
        grant: impl Into<Option<AgentEndpointGrant>>,
    ) -> AccessToken {
        self.grant.agent_endpoint = grant.into();
        self
    }

    pub fn set_agent_thread_grant(
        mut self,
        grant: impl Into<Option<AgentThreadGrant>>,
    ) -> AccessToken {
        self.grant.agent_thread = grant.into();
        self
    }

    pub fn set_metadata(mut self, md: &str) -> AccessToken {
        self.grant.metadata = md.to_string();
        self
    }

    pub fn set_attributes(mut self, attrs: &HashMap<String, String>) -> AccessToken {
        if attrs.is_empty() {
            return self;
        }
        for (k, v) in attrs {
            self.grant.attributes.insert(k.clone(), v.clone());
        }
        self
    }

    pub fn set_sha256(mut self, sha: &str) -> AccessToken {
        self.grant.sha256 = sha.to_string();
        self
    }

    pub fn set_room_preset(mut self, preset: &str) -> AccessToken {
        self.grant.room_preset = preset.to_string();
        self
    }

    pub fn set_room_config(
        mut self,
        config: impl Into<Option<livekit::RoomConfiguration>>,
    ) -> AccessToken {
        self.grant.room_config = config.into().map(RoomConfiguration);
        self
    }

    // SetAgents is a shortcut for setting agents in room configuration
    pub fn set_agents(mut self, agents: Vec<livekit::RoomAgentDispatch>) -> AccessToken {
        self.grant.room_config.get_or_insert_default().agents = agents;
        self
    }

    // SetAllowSensitiveCredentials enables the token to contain sensitive credentials, by default it is disabled.
    // When tokens are issued to end-users, it's not a good idea to issue sensitive data such as API keys/secrets in them
    // JWT tokens are not encrypted, so anything that is issued in them can be read by anyone.
    // When the tokens are used in a server environment (i.e. connecting from SIP or Agents), you can bypass the
    // credentials check by enabling this option.
    pub fn set_allow_sensitive_credentials(mut self, allow: bool) -> AccessToken {
        self.allow_sensitive_credentials = allow;
        self
    }

    pub fn get_grants(&self) -> &ClaimGrants {
        &self.grant
    }

    pub fn to_jwt(&self) -> Result<String, Error> {
        if self.api_key.is_empty() || self.secret.is_empty() {
            return Err(Error::KeysMissing);
        }

        if let Some(room_config) = &self.grant.room_config
            && !self.allow_sensitive_credentials
        {
            room_config.check_credentials()?;
        }

        let mut valid_for = DEFAULT_VALID_DURATION;
        if self.valid_for > Duration::ZERO {
            valid_for = self.valid_for;
        }

        let now = jwt::time_now();
        let claims = TokenClaims {
            registered_claims: RegisteredClaims {
                issuer: self.api_key.clone(),
                subject: self.grant.identity.clone(),
                issued_at: Some(NumericDate::new(now)),
                not_before: Some(NumericDate::new(now)),
                expires_at: Some(NumericDate::new_after(now, valid_for)),
                ..Default::default()
            },
            claim_grants: self.grant.clone(),
        };
        jwt::signed_string(&claims, self.secret.as_bytes())
            .map_err(|err| Error::Json(err.to_string()))
    }
}

#[cfg(test)]
#[path = "accesstoken_test.rs"]
mod accesstoken_test;
