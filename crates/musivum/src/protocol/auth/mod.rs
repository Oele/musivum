//! The Go package `github.com/livekit/protocol/auth`: access tokens. Building and signing
//! them ([`AccessToken`]), what they grant ([`ClaimGrants`], [`VideoGrant`]), reading and
//! verifying them ([`parse_api_token`]), and where the secret of an API key is found
//! ([`KeyProvider`]).
//!
//! A token is a JWT signed with HMAC-SHA256: the API key is its issuer, the API secret its
//! key. [`jwt`] has what the original takes from its JWT library.
//!
//! ```
//! use musivum::protocol::auth::{self, AccessToken, VideoGrant};
//!
//! let token = AccessToken::new("the-api-key", "the-api-secret")
//!     .set_identity("someone")
//!     .set_video_grant(VideoGrant {
//!         room_join: true,
//!         room: "a-room".into(),
//!         ..Default::default()
//!     })
//!     .to_jwt()
//!     .unwrap();
//!
//! let verifier = auth::parse_api_token(&token).unwrap();
//! assert_eq!(verifier.api_key(), "the-api-key");
//! let (claims, grants) = verifier.verify("the-api-secret").unwrap();
//! assert_eq!(grants.identity, "someone");
//! assert!(grants.video.unwrap().room_join);
//! assert!(claims.expires_at.is_some());
//! ```

mod accesstoken;
mod grants;
mod interfaces;
mod json;
mod provider;
mod verifier;

pub mod jwt;

pub use accesstoken::*;
pub use grants::*;
pub use interfaces::*;
pub use provider::*;
pub use verifier::*;
