// Ported from LiveKit's protocol, auth/verifier_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Where the original signs claims it wrote by hand with
// its JWT library (`jwt.MapClaims`), this signs a JSON value with `jwt::signed_string`. The
// original has one test; the others here are new and say so.

use std::collections::HashMap;

use serde_json::json;

use super::*;
use crate::protocol::auth::jwt::NumericDate;
use crate::protocol::auth::{AccessToken, VideoGrant};
use crate::protocol::livekit::participant_info::{Kind, KindDetail};
use crate::protocol::livekit::{TrackSource, encoded_file_output};

#[test]
fn test_verifier() {
    let api_key = "APID3B67uxk4Nj2GKiRPibAZ9";
    let secret = "YHC-CUhbQhGeVCaYgn1BNA++";
    let access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2MDg5MzAzMDgsImlzcyI6IkFQSUQzQjY3dXhrNE5qMkdLaVJQaWJBWjkiLCJuYmYiOjE2MDg5MjY3MDgsInJvb21fam9pbiI6dHJ1ZSwicm9vbV9zaWQiOiJteWlkIiwic3ViIjoiQVBJRDNCNjd1eGs0TmoyR0tpUlBpYkFaOSJ9.cmHEBq0MLyRqphmVLM2cLXg5ao5Sro7am8yXhcYKcwE";

    // cannot decode with incorrect key
    {
        let v = parse_api_token(access_token).unwrap();

        assert_eq!(api_key, v.api_key());
        assert!(v.verify("").is_err());

        assert!(v.verify("anothersecret").is_err());
    }

    // key has expired
    {
        let v = parse_api_token(access_token).unwrap();

        assert!(v.verify(secret).is_err());
    }

    // token without exp is rejected
    {
        // hand-rolled JWT with no exp claim (the Go SDK always sets one, so
        // build it directly to model a third-party minter forgetting exp)
        let claims = json!({
            "iss": api_key,
            "nbf": NumericDate::new(jwt::time_now().add(-time::MINUTE)),
            "video": {
                "roomCreate": true,
            },
        });
        let auth_token = jwt::signed_string(&claims, secret.as_bytes()).unwrap();

        let v = parse_api_token(&auth_token).unwrap();

        assert!(v.verify(secret).is_err());
    }

    // unexpired token is verified
    {
        let claim = VideoGrant {
            room_create: true,
            ..Default::default()
        };
        let at = AccessToken::new(api_key, secret)
            .set_video_grant(claim.clone())
            .set_valid_for(time::MINUTE)
            .set_identity("me");
        let auth_token = at.to_jwt().unwrap();

        let v = parse_api_token(&auth_token).unwrap();
        assert_eq!(api_key, v.api_key());
        assert_eq!("me", v.identity());

        let (_, decoded) = v.verify(secret).unwrap();
        assert_eq!(Some(claim), decoded.video);
    }

    // ensure metadata can be passed through
    {
        // The original writes the number as a float64 of 3, which Go writes as 3.
        let metadata = json!({
            "user": "value",
            "number": 3,
        });
        let md = serde_json::to_string(&metadata).unwrap();
        let attrs = HashMap::from([
            ("mykey".to_string(), "myval".to_string()),
            ("secondkey".to_string(), "secondval".to_string()),
        ]);
        let at = AccessToken::new(api_key, secret)
            .set_video_grant(VideoGrant {
                room_admin: true,
                room: "myroom".into(),
                ..Default::default()
            })
            .set_metadata(&md)
            .set_attributes(&attrs);

        let auth_token = at.to_jwt().unwrap();

        let v = parse_api_token(&auth_token).unwrap();

        let (_, decoded) = v.verify(secret).unwrap();

        assert_eq!(md, decoded.metadata);
        assert_eq!(attrs, decoded.attributes);
    }

    // unknown fields are ignored for forward compatibility
    {
        // Simulate a token issued by a newer client whose claims include fields
        // this server does not yet know about. The server should still accept the
        // token rather than failing with `unknown field`. This guards against
        // requiring server upgrades before client upgrades can roll out.
        let claims = json!({
            "iss": api_key,
            "sub": "me",
            "nbf": NumericDate::new(jwt::time_now()),
            "exp": NumericDate::new(jwt::time_now().add(time::MINUTE)),
            // unknown top-level claim grants field
            "someFutureGrant": {"enabled": true},
            "video": {
                "roomJoin": true,
                "room": "myroom",
                // unknown field inside a known grant
                "someFutureVideoField": "future-value",
            },
            "roomConfig": {
                "name": "myroom",
                // unknown field inside a protojson-decoded message
                "someFutureRoomConfigField": "future-value",
            },
        });
        let token = jwt::signed_string(&claims, secret.as_bytes()).unwrap();

        let v = parse_api_token(&token).unwrap();

        let (_, decoded) = v.verify(secret).unwrap();
        let video = decoded.video.as_ref().unwrap();
        assert_eq!("myroom", video.room);
        assert!(video.room_join);
        let room_config = decoded.room_config.as_ref().unwrap();
        assert_eq!("myroom", room_config.name);
    }

    // nil permissions are handled
    {
        let mut grant = VideoGrant {
            room: "myroom".into(),
            room_join: true,
            ..Default::default()
        };
        grant.set_can_publish_data(false);
        let at = AccessToken::new(api_key, secret).set_video_grant(grant);
        let token = at.to_jwt().unwrap();

        let v = parse_api_token(&token).unwrap();
        let (_, decoded) = v.verify(secret).unwrap();

        let video = decoded.video.unwrap();
        assert_eq!(None, video.can_subscribe);
        assert_eq!(None, video.can_publish);
        assert_eq!(Some(false), video.can_publish_data);
    }
}

// The key and the secret of the tokens that go between this code and the Go original (the
// same as in accesstoken_test.rs). They are made up for these tests.
const INTEROP_KEY: &str = "APImusivumTestKey";
const INTEROP_SECRET: &str = "musivum-test-secret-0123456789abcdef";

// A token with every grant in it, made by the Go original: `NewAccessToken` with the setters
// and `ToJWT`, with the key and the secret above, good for a hundred years (until 2126).
const GO_TOKEN_FULL: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJBUEltdXNpdnVtVGVzdEtleSIsInN1YiI6InVzZXItMSIsImV4cCI6NDk0ODIzMzU1NiwibmJmIjoxNzk0NjMzNTU2LCJpYXQiOjE3OTQ2MzM1NTYsImlkZW50aXR5IjoidXNlci0xIiwibmFtZSI6IlVzZXIgT25lIiwia2luZCI6ImFnZW50Iiwia2luZERldGFpbHMiOlsiY2xvdWRfYWdlbnQiLCJmb3J3YXJkZWQiXSwidmlkZW8iOnsicm9vbUpvaW4iOnRydWUsInJvb20iOiJteXJvb20iLCJjYW5QdWJsaXNoIjp0cnVlLCJjYW5TdWJzY3JpYmUiOmZhbHNlLCJjYW5QdWJsaXNoU291cmNlcyI6WyJjYW1lcmEiLCJzY3JlZW5fc2hhcmVfYXVkaW8iXSwiaGlkZGVuIjp0cnVlfSwic2lwIjp7ImFkbWluIjp0cnVlLCJjYWxsIjp0cnVlfSwiYWdlbnQiOnsiYWRtaW4iOnRydWUsImRpc3BhdGNoQWRtaW4iOnRydWV9LCJpbmZlcmVuY2UiOnsicGVyZm9ybSI6dHJ1ZX0sIm9ic2VydmFiaWxpdHkiOnsid3JpdGUiOnRydWV9LCJhZ2VudEVuZHBvaW50Ijp7ImNhbGwiOnRydWUsImFnZW50TmFtZSI6ImEiLCJkZXBsb3ltZW50IjoicHJvZCJ9LCJhZ2VudFRocmVhZCI6eyJzY29wZSI6InUxIiwidGhyZWFkSWRzIjpbIkFUXzEiXSwicmVhZCI6dHJ1ZX0sInJvb21Db25maWciOnsibmFtZSI6Im15cm9vbSIsImVtcHR5VGltZW91dCI6MzAwLCJtYXhQYXJ0aWNpcGFudHMiOjQsImVncmVzcyI6eyJyb29tIjp7ImZpbGVPdXRwdXRzIjpbeyJkaXNhYmxlTWFuaWZlc3QiOnRydWUsInMzIjp7ImJ1Y2tldCI6ImJ1Y2tldCJ9fV19fSwic3luY1N0cmVhbXMiOnRydWUsImFnZW50cyI6W3siYWdlbnROYW1lIjoiYWdlbnQxIiwibWV0YWRhdGEiOiJtZXRhZGF0YTEifV19LCJyb29tUHJlc2V0IjoicHJlc2V0LTEiLCJzaGEyNTYiOiJhYmMxMjMiLCJtZXRhZGF0YSI6IntcImFcIjpcIlx1MDAzY2JcdTAwM2VcdTAwMjZjXCJ9IiwiYXR0cmlidXRlcyI6eyJrMSI6InYxIiwiazIiOiJ2MiJ9fQ.gdXFZ6BcbRjnR-Qr4eocM5ioWSx5QnRBtFwYMQTO1A8";

// The least token the Go original makes: no identity, and a video grant with nothing in it.
const GO_TOKEN_MIN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJBUEltdXNpdnVtVGVzdEtleSIsImV4cCI6NDk0ODIzMzU1NiwibmJmIjoxNzk0NjMzNTU2LCJpYXQiOjE3OTQ2MzM1NTYsInZpZGVvIjp7fX0.JqaFiRvVHVLNisXyxqSYpd8NerQimPuYGz4HyOsNunQ";

// New for musivum: tokens made by the Go original are verified here, and everything they
// grant is read as the original wrote it.
#[test]
fn verifies_the_tokens_of_the_go_implementation() {
    let v = parse_api_token(GO_TOKEN_FULL).unwrap();
    assert_eq!(INTEROP_KEY, v.api_key());
    assert_eq!("user-1", v.identity());
    let (claims, grants) = v.verify(INTEROP_SECRET).unwrap();

    assert_eq!(INTEROP_KEY, claims.issuer);
    assert_eq!("user-1", claims.subject);
    assert_eq!(Some(NumericDate(1794633556)), claims.issued_at);
    assert_eq!(Some(NumericDate(1794633556)), claims.not_before);
    assert_eq!(
        Some(NumericDate(1794633556 + 100 * 365 * 24 * 3600)),
        claims.expires_at
    );
    assert!(claims.audience.is_empty());
    assert_eq!("", claims.id);

    assert_eq!("user-1", grants.identity);
    assert_eq!("User One", grants.name);
    assert_eq!("agent", grants.kind);
    assert_eq!(Kind::Agent, grants.get_participant_kind());
    assert_eq!(
        vec![KindDetail::CloudAgent, KindDetail::Forwarded],
        grants.get_kind_details()
    );
    let video = grants.video.as_ref().unwrap();
    assert_eq!(
        &VideoGrant {
            room_join: true,
            room: "myroom".into(),
            can_publish: Some(true),
            can_subscribe: Some(false),
            can_publish_sources: vec!["camera".into(), "screen_share_audio".into()],
            hidden: true,
            ..Default::default()
        },
        video
    );
    assert_eq!(
        vec![TrackSource::Camera, TrackSource::ScreenShareAudio],
        video.get_can_publish_sources()
    );
    assert!(video.get_can_publish());
    assert!(!video.get_can_subscribe());
    assert!(video.get_can_publish_data());
    let sip = grants.sip.as_ref().unwrap();
    assert!(sip.admin && sip.call);
    let agent = grants.agent.as_ref().unwrap();
    assert!(agent.admin && agent.dispatch_admin);
    assert!(!agent.simulation_admin && !agent.database_admin);
    assert!(grants.inference.as_ref().unwrap().perform);
    assert!(grants.observability.as_ref().unwrap().write);
    assert!(grants.agent_endpoint.as_ref().unwrap().allows("a", "prod"));
    assert!(!grants.agent_endpoint.as_ref().unwrap().allows("b", "prod"));
    let thread = grants.agent_thread.as_ref().unwrap();
    assert_eq!("u1", thread.scope);
    assert!(thread.can_read("AT_1"));
    assert!(!thread.can_write("AT_1"));
    let config = grants.get_room_configuration().unwrap();
    assert_eq!("myroom", config.name);
    assert_eq!(300, config.empty_timeout);
    assert_eq!(4, config.max_participants);
    assert!(config.sync_streams);
    assert_eq!(1, config.agents.len());
    assert_eq!("agent1", config.agents[0].agent_name);
    assert_eq!("metadata1", config.agents[0].metadata);
    let outputs = &config
        .egress
        .as_ref()
        .unwrap()
        .room
        .as_ref()
        .unwrap()
        .file_outputs;
    assert_eq!(1, outputs.len());
    assert!(outputs[0].disable_manifest);
    let Some(encoded_file_output::Output::S3(s3)) = &outputs[0].output else {
        panic!("the output is not S3");
    };
    assert_eq!("bucket", s3.bucket);
    assert_eq!("preset-1", grants.room_preset);
    assert_eq!("abc123", grants.sha256);
    assert_eq!(r#"{"a":"<b>&c"}"#, grants.metadata);
    assert_eq!(
        HashMap::from([
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
        ]),
        grants.attributes
    );

    // Written again, the claims are the JSON the original wrote.
    let payload = jwt::decode_segment(GO_TOKEN_FULL.split('.').nth(1).unwrap()).unwrap();
    let written_by_go: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    let written_here = serde_json::to_value(TokenClaims {
        registered_claims: claims,
        claim_grants: grants,
    })
    .unwrap();
    assert_eq!(written_by_go, written_here);

    // Another secret does not verify it, and neither does no secret.
    assert_eq!(
        Err(Error::Jwt(jwt::Error::TokenSignatureInvalid(
            "signature is invalid".into()
        ))),
        v.verify("another-secret").map(|_| ())
    );
    assert_eq!(Err(Error::KeysMissing), v.verify("").map(|_| ()));
    // Nor does the right secret a token that was changed after it was signed.
    let changed = GO_TOKEN_FULL.replacen("InVzZXItMSIs", "InVzZXItMiIs", 1);
    assert_ne!(GO_TOKEN_FULL, changed);
    let v = parse_api_token(&changed).unwrap();
    assert_eq!("user-2", v.identity());
    assert_eq!(
        Err(Error::Jwt(jwt::Error::TokenSignatureInvalid(
            "signature is invalid".into()
        ))),
        v.verify(INTEROP_SECRET).map(|_| ())
    );

    let v = parse_api_token(GO_TOKEN_MIN).unwrap();
    assert_eq!(INTEROP_KEY, v.api_key());
    assert_eq!("", v.identity());
    let (claims, grants) = v.verify(INTEROP_SECRET).unwrap();
    assert_eq!("", claims.subject);
    assert_eq!(Some(NumericDate(1794633556)), claims.not_before);
    assert_eq!(
        ClaimGrants {
            video: Some(VideoGrant::default()),
            ..Default::default()
        },
        grants
    );
    assert_eq!(Kind::Standard, grants.get_participant_kind());
}

// What became of a token.
#[derive(Debug, PartialEq)]
enum Outcome {
    // Verified: the API key and the identity it was parsed with, its `exp` and `nbf`, and
    // the grants written as JSON.
    Verified(String, String, i64, Option<i64>, String),
    // `parse_api_token` refused it.
    ParseError(String),
    // `verify` refused it: the API key and the identity it was parsed with, and the error.
    VerifyError(String, String, String),
}

fn check(token: &str) -> Outcome {
    let v = match parse_api_token(token) {
        Ok(v) => v,
        Err(err) => return Outcome::ParseError(err.to_string()),
    };
    let (key, id) = (v.api_key().to_string(), v.identity().to_string());
    match v.verify(INTEROP_SECRET) {
        Ok((claims, grants)) => Outcome::Verified(
            key,
            id,
            claims.expires_at.unwrap().unix(),
            claims.not_before.map(NumericDate::unix),
            serde_json::to_string(&grants).unwrap(),
        ),
        Err(err) => Outcome::VerifyError(key, id, err.to_string()),
    }
}

// An error text that ends in ": " is what the error starts with: what follows are the words
// of a JSON decoder, which are Go's there and serde's here.
#[track_caller]
fn assert_outcome(name: &str, want: Outcome, token: &str) {
    assert_same(name, want, check(token));
}

// The same, with the grants compared as JSON and not as text. A room configuration is
// written here with its fields in another order than protojson has them (what is one of
// several comes last).
#[track_caller]
fn assert_outcome_as_json(name: &str, want: Outcome, token: &str) {
    fn sorted(outcome: Outcome) -> Outcome {
        match outcome {
            Outcome::Verified(key, id, exp, nbf, grants) => {
                let grants: serde_json::Value = serde_json::from_str(&grants).unwrap();
                Outcome::Verified(key, id, exp, nbf, grants.to_string())
            }
            other => other,
        }
    }
    assert_same(name, sorted(want), sorted(check(token)));
}

#[track_caller]
fn assert_same(name: &str, want: Outcome, got: Outcome) {
    let same_text = |want: &str, got: &str| {
        if want.ends_with(": ") {
            got.starts_with(want)
        } else {
            want == got
        }
    };
    let same = match (&want, &got) {
        (Outcome::ParseError(want), Outcome::ParseError(got)) => same_text(want, got),
        (Outcome::VerifyError(wk, wi, want), Outcome::VerifyError(gk, gi, got)) => {
            wk == gk && wi == gi && same_text(want, got)
        }
        _ => want == got,
    };
    assert!(same, "{name}:\n want {want:?}\n  got {got:?}");
}

fn sign(header: &str, payload: &str) -> String {
    sign_bytes(header, payload.as_bytes())
}

fn sign_bytes(header: &str, payload: &[u8]) -> String {
    let signing_string = format!(
        "{}.{}",
        jwt::encode_segment(header.as_bytes()),
        jwt::encode_segment(payload)
    );
    let signature = jwt::sign(&signing_string, INTEROP_SECRET.as_bytes());
    format!("{signing_string}.{}", jwt::encode_segment(&signature))
}

const HDR: &str = r#"{"alg":"HS256","typ":"JWT"}"#;
const MALFORMED_CLAIM: &str = "token is malformed: could not JSON decode claim: ";
const EXPIRED: &str = "token has invalid claims: token is expired";
const NOT_VALID_YET: &str = "token has invalid claims: token is not valid yet";
const EXP_REQUIRED: &str =
    "token has invalid claims: token is missing required claim: exp claim is required";
const SIGNATURE_INVALID: &str = "token signature is invalid: signature is invalid";

// New for musivum: tokens that are unusual, written by hand and signed with the right
// secret. Each was given to the Go original (`ParseAPIToken`, then `Verify`; the same header
// and the same claims, with the times of that moment), and what is expected here is what
// the original did with it: whether it parsed and verified, the API key and the identity it
// read, `exp` and `nbf`, and the grants it read, written as JSON. Where this code does not
// do as the original does, the test says so.
#[test]
fn unusual_tokens_are_taken_and_refused_as_the_go_implementation_does() {
    let now = jwt::time_now().unix_secs();
    let hour = now + 3600;
    let exp = hour.to_string();
    let base =
        format!(r#""iss":"{INTEROP_KEY}","sub":"me","video":{{"roomJoin":true,"room":"r"}}"#);
    let grants = r#"{"identity":"me","video":{"roomJoin":true,"room":"r"}}"#;
    // The claims of `base` with an `exp` an hour from now, and more.
    let with = |more: &str| sign(HDR, &format!("{{{base},\"exp\":{exp}{more}}}"));
    // Claims without the video grant: with nothing else, with the API key, and with the
    // API key and the subject "me" (and `exp` before what else there is).
    let bare = |claims: &str| sign(HDR, &format!("{{{claims},\"exp\":{exp}}}"));
    let key_and = |claims: &str| bare(&format!(r#""iss":"{INTEROP_KEY}",{claims}"#));
    let me_and = |claims: &str| {
        sign(
            HDR,
            &format!(r#"{{"iss":"{INTEROP_KEY}","sub":"me","exp":{exp},{claims}}}"#),
        )
    };

    let verified = |grants: &str| {
        Outcome::Verified(INTEROP_KEY.into(), "me".into(), hour, None, grants.into())
    };
    let verified_as = |key: &str, id: &str, grants: &str| {
        Outcome::Verified(key.into(), id.into(), hour, None, grants.into())
    };
    let verified_until =
        |exp: i64| Outcome::Verified(INTEROP_KEY.into(), "me".into(), exp, None, grants.into());
    let verified_from = |nbf: i64| {
        Outcome::Verified(
            INTEROP_KEY.into(),
            "me".into(),
            hour,
            Some(nbf),
            grants.into(),
        )
    };
    let parse_error = |err: &str| Outcome::ParseError(err.into());
    let verify_error =
        |err: &str| Outcome::VerifyError(INTEROP_KEY.into(), "me".into(), err.into());
    let verify_error_as =
        |key: &str, id: &str, err: &str| Outcome::VerifyError(key.into(), id.into(), err.into());

    // exp
    assert_outcome("plain", verified(grants), &with(""));
    assert_outcome(
        "no exp",
        verify_error(EXP_REQUIRED),
        &sign(HDR, &format!("{{{base}}}")),
    );
    let exp_is = |exp: &str| sign(HDR, &format!("{{{base},\"exp\":{exp}}}"));
    assert_outcome("exp null", verify_error(EXP_REQUIRED), &exp_is("null"));
    assert_outcome(
        "exp as string",
        verified(grants),
        &exp_is(&format!("\"{exp}\"")),
    );
    for (name, exp) in [
        ("exp as bad string", r#""soon""#.to_string()),
        ("exp as empty string", r#""""#.to_string()),
        ("exp string padded", format!("\" {exp}\"")),
        ("exp string leading zero", format!("\"0{exp}\"")),
        ("exp as bool", "true".to_string()),
        ("exp list", format!("[{exp}]")),
        ("exp object", "{}".to_string()),
        ("exp 1e400", "1e400".to_string()),
    ] {
        assert_outcome(name, parse_error(MALFORMED_CLAIM), &exp_is(&exp));
    }
    assert_outcome("exp float", verified(grants), &exp_is(&format!("{exp}.75")));
    assert_outcome("exp exponent", verified_until(4100000000), &exp_is("4.1e9"));
    assert_outcome(
        "exp year 9999",
        verified_until(253402300799),
        &exp_is("253402300799"),
    );
    assert_outcome(
        "exp 253402300800",
        verified_until(253402300800),
        &exp_is("253402300800"),
    );
    assert_outcome("exp 1e12", verified_until(1000000000000), &exp_is("1e12"));
    // The last second Go's time can hold is 2^63 - 1 seconds after the year 1. A date up to
    // there is a date; one after it is, to Go, a date long ago.
    assert_outcome(
        "exp 9.2e18",
        verified_until(9200000000000000000),
        &exp_is("9.2e18"),
    );
    assert_outcome(
        "exp 9223371974719178752",
        verified_until(9223371974719178752),
        &exp_is("9223371974719178752"),
    );
    for exp in [
        "9223371974719179776",
        "9.2233720e18",
        "9223372036854775807",
        "1e19",
        "1e30",
        "\"1e30\"",
        "-1",
        "-1e30",
        "0",
        "-0",
        "1e-5",
    ] {
        assert_outcome(&format!("exp {exp}"), verify_error(EXPIRED), &exp_is(exp));
    }
    // The leeway: a token is good until a minute after it expired.
    let exp_at = |exp: i64| exp_is(&exp.to_string());
    assert_outcome("exp 30s ago", verified_until(now - 30), &exp_at(now - 30));
    assert_outcome("exp 58s ago", verified_until(now - 58), &exp_at(now - 58));
    assert_outcome("exp 61s ago", verify_error(EXPIRED), &exp_at(now - 61));
    assert_outcome("exp 90s ago", verify_error(EXPIRED), &exp_at(now - 90));

    // nbf
    let nbf_at = |nbf: i64| with(&format!(",\"nbf\":{nbf}"));
    assert_outcome("nbf in 30s", verified_from(now + 30), &nbf_at(now + 30));
    assert_outcome("nbf in 58s", verified_from(now + 58), &nbf_at(now + 58));
    assert_outcome("nbf in 62s", verify_error(NOT_VALID_YET), &nbf_at(now + 62));
    assert_outcome("nbf in 90s", verify_error(NOT_VALID_YET), &nbf_at(now + 90));
    assert_outcome(
        "nbf in 90s and expired",
        verify_error("token has invalid claims: token is expired, token is not valid yet"),
        &sign(
            HDR,
            &format!("{{{base},\"exp\":{},\"nbf\":{}}}", now - 90, now + 90),
        ),
    );
    assert_outcome("nbf null", verified(grants), &with(r#","nbf":null"#));
    assert_outcome(
        "nbf as string",
        parse_error(MALFORMED_CLAIM),
        &with(r#","nbf":"x""#),
    );
    for (nbf, want) in [
        ("-1", Some(-1)),
        ("0", Some(0)),
        ("1e-5", Some(0)),
        // After the last second Go's time can hold: long ago.
        ("9223371974719179776", Some(9223371974719179776)),
        ("9.2233720e18", Some(9223372000000000000)),
        // A number of seconds that is more than Go's int64 holds is, on the machines
        // LiveKit runs on, the least one.
        ("9223372036854775807", Some(i64::MIN)),
        ("1e19", Some(i64::MIN)),
        ("1e30", Some(i64::MIN)),
        ("-1e30", Some(i64::MIN)),
        ("9.2e18", None),
        ("9223371974719178752", None),
        ("253402300800", None),
        ("1e12", None),
    ] {
        let want = match want {
            Some(nbf) => verified_from(nbf),
            None => verify_error(NOT_VALID_YET),
        };
        assert_outcome(
            &format!("nbf {nbf}"),
            want,
            &with(&format!(",\"nbf\":{nbf}")),
        );
    }

    // iat is read, and not checked.
    assert_outcome(
        "iat in the future",
        verified(grants),
        &with(&format!(",\"iat\":{}", now + 900)),
    );
    assert_outcome("iat null", verified(grants), &with(r#","iat":null"#));
    assert_outcome(
        "iat string",
        parse_error(MALFORMED_CLAIM),
        &with(r#","iat":"x""#),
    );
    assert_outcome(
        "iat bool",
        parse_error(MALFORMED_CLAIM),
        &with(r#","iat":true"#),
    );

    // aud is read, and not checked.
    assert_outcome("aud string", verified(grants), &with(r#","aud":"x""#));
    assert_outcome("aud list", verified(grants), &with(r#","aud":["x","y"]"#));
    assert_outcome("aud empty list", verified(grants), &with(r#","aud":[]"#));
    assert_outcome("aud null", verified(grants), &with(r#","aud":null"#));
    for (name, aud) in [
        ("aud number", "5"),
        ("aud list of numbers", "[5]"),
        ("aud list with null", r#"["x",null]"#),
    ] {
        assert_outcome(
            name,
            parse_error("token is malformed: could not JSON decode claim: invalid type for claim"),
            &with(&format!(",\"aud\":{aud}")),
        );
    }

    // iss, sub and jti
    let only_me = r#"{"identity":"me"}"#;
    assert_outcome(
        "no iss",
        verified_as("", "me", only_me),
        &bare(r#""sub":"me""#),
    );
    assert_outcome(
        "iss null",
        verified_as("", "me", only_me),
        &bare(r#""iss":null,"sub":"me""#),
    );
    assert_outcome(
        "iss empty",
        verified_as("", "me", only_me),
        &bare(r#""iss":"","sub":"me""#),
    );
    assert_outcome(
        "iss number",
        parse_error(MALFORMED_CLAIM),
        &bare(r#""iss":5,"sub":"me""#),
    );
    assert_outcome(
        "sub number",
        parse_error(MALFORMED_CLAIM),
        &key_and(r#""sub":5"#),
    );
    assert_outcome(
        "jti number",
        parse_error(MALFORMED_CLAIM),
        &key_and(r#""sub":"me","jti":5"#),
    );
    // The identity is the subject, or the id of the token when there is no subject. What
    // the token claims as its identity does not count.
    let the_id = r#"{"identity":"the-id"}"#;
    assert_outcome(
        "no sub, jti",
        verified_as(INTEROP_KEY, "the-id", the_id),
        &key_and(r#""jti":"the-id""#),
    );
    assert_outcome(
        "sub null, jti",
        verified_as(INTEROP_KEY, "the-id", the_id),
        &key_and(r#""sub":null,"jti":"the-id""#),
    );
    assert_outcome(
        "sub empty, jti",
        verified_as(INTEROP_KEY, "the-id", the_id),
        &key_and(r#""sub":"","jti":"the-id""#),
    );
    assert_outcome(
        "sub and jti",
        verified(only_me),
        &key_and(r#""sub":"me","jti":"the-id""#),
    );
    assert_outcome(
        "jti only, identity claim",
        verified_as(INTEROP_KEY, "the-id", the_id),
        &key_and(r#""jti":"the-id","identity":"claimed""#),
    );
    assert_outcome(
        "identity claim only",
        verified_as(INTEROP_KEY, "", "{}"),
        &key_and(r#""identity":"claimed""#),
    );
    assert_outcome(
        "identity claim and sub",
        verified(only_me),
        &key_and(r#""identity":"claimed","sub":"me""#),
    );

    // null is a claim that is not there.
    assert_outcome(
        "nulls everywhere",
        verified(grants),
        &with(
            r#","name":null,"kind":null,"kindDetails":null,"sip":null,"metadata":null,"attributes":null,"roomConfig":null,"sha256":null"#,
        ),
    );
    assert_outcome(
        "nulls in video",
        verified(r#"{"identity":"me","video":{}}"#),
        &me_and(
            r#""video":{"roomJoin":null,"room":null,"canPublish":null,"canPublishSources":null}"#,
        ),
    );
    assert_outcome("video null", verified(only_me), &me_and(r#""video":null"#));
    assert_outcome(
        "null in attributes",
        verified(
            r#"{"identity":"me","video":{"roomJoin":true,"room":"r"},"attributes":{"a":"","b":"c"}}"#,
        ),
        &with(r#","attributes":{"a":null,"b":"c"}"#),
    );
    assert_outcome(
        "null in kindDetails",
        verified(
            r#"{"identity":"me","kindDetails":["","forwarded"],"video":{"roomJoin":true,"room":"r"}}"#,
        ),
        &with(r#","kindDetails":[null,"forwarded"]"#),
    );
    // And so is one that is empty.
    for (name, more) in [
        ("empty kind", r#","kind":"""#),
        ("metadata empty", r#","metadata":"""#),
        ("attributes empty", r#","attributes":{}"#),
    ] {
        assert_outcome(name, verified(grants), &with(more));
    }

    // A claim of another type than it should have. What is wrong with a grant is found when
    // the token is verified: parsing reads the registered claims only.
    for (name, token) in [
        ("video string", me_and(r#""video":"yes""#)),
        ("video list", me_and(r#""video":[true]"#)),
        ("roomJoin string", me_and(r#""video":{"roomJoin":"true"}"#)),
        ("roomJoin number", me_and(r#""video":{"roomJoin":1}"#)),
        (
            "canPublish string",
            me_and(r#""video":{"canPublish":"false"}"#),
        ),
        (
            "sources with number",
            me_and(r#""video":{"canPublishSources":["camera",5]}"#),
        ),
        ("name number", with(r#","name":5"#)),
        ("kind number", with(r#","kind":5"#)),
        ("kindDetails string", with(r#","kindDetails":"forwarded""#)),
        ("attributes number value", with(r#","attributes":{"a":5}"#)),
        ("attributes list", with(r#","attributes":[]"#)),
        ("attributes nested", with(r#","attributes":{"a":{"b":1}}"#)),
        ("metadata object", with(r#","metadata":{"a":1}"#)),
        ("sha256 number", with(r#","sha256":5"#)),
        ("sip string", with(r#","sip":"yes""#)),
        (
            "agentThread threadIds string",
            with(r#","agentThread":{"threadIds":"t"}"#),
        ),
    ] {
        assert_outcome(name, verify_error(MALFORMED_CLAIM), &token);
    }

    // What is not known is left out, and what is known is read whatever its value.
    assert_outcome(
        "unknown claim",
        verified(grants),
        &with(r#","other":{"a":[1,2,{"b":null}]}"#),
    );
    assert_outcome("empty key", verified(grants), &with(r#","":1"#));
    assert_outcome(
        "unknown claim with a long integer",
        verified(grants),
        &with(r#","other":123456789012345678901234567890"#),
    );
    assert_outcome(
        "unknown claim named as a field of video",
        verified(grants),
        &with(r#","roomJoin":false,"room":"x""#),
    );
    assert_outcome(
        "old snake case claims",
        verified(only_me),
        &me_and(r#""room_join":true,"room_sid":"myid""#),
    );
    assert_outcome(
        "snake case keys",
        verified(r#"{"identity":"me","video":{"room":"r"}}"#),
        &me_and(r#""video":{"room_join":true,"room":"r"}"#),
    );
    assert_outcome(
        "kind unknown",
        verified(r#"{"identity":"me","kind":"future_kind","video":{"roomJoin":true,"room":"r"}}"#),
        &with(r#","kind":"future_kind""#),
    );
    assert_outcome(
        "kindDetails unknown",
        verified(
            r#"{"identity":"me","kindDetails":["future","forwarded"],"video":{"roomJoin":true,"room":"r"}}"#,
        ),
        &with(r#","kindDetails":["future","forwarded"]"#),
    );
    assert_outcome(
        "sources empty",
        verified(r#"{"identity":"me","video":{}}"#),
        &me_and(r#""video":{"canPublishSources":[]}"#),
    );
    assert_outcome(
        "sources unknown",
        verified(r#"{"identity":"me","video":{"canPublishSources":["camera","future_source"]}}"#),
        &me_and(r#""video":{"canPublishSources":["camera","future_source"]}"#),
    );
    assert_outcome(
        "canPublish false",
        verified(r#"{"identity":"me","video":{"canPublish":false,"canSubscribe":true}}"#),
        &me_and(r#""video":{"canPublish":false,"canSubscribe":true}"#),
    );
    assert_outcome(
        "all grants",
        verified(concat!(
            r#"{"identity":"me","name":"n","video":{"roomJoin":true,"room":"r"},"sip":{"admin":true},"#,
            r#""agent":{"admin":true},"inference":{"perform":true},"observability":{"write":true},"#,
            r#""agentEndpoint":{"call":true,"agentName":"a"},"agentThread":{"all":true,"threadIds":["t",""]},"#,
            r#""roomPreset":"p","sha256":"s"}"#,
        )),
        &with(concat!(
            r#","sip":{"admin":true},"agent":{"admin":true,"future":1},"inference":{"perform":true},"#,
            r#""observability":{"write":true},"agentEndpoint":{"call":true,"agentName":"a"},"#,
            r#""agentThread":{"all":true,"threadIds":["t",null]},"roomPreset":"p","sha256":"s","name":"n""#,
        )),
    );

    // The name of a claim is read in any case, and so are the two characters outside
    // ASCII that Unicode holds to be an s and a k in another case.
    assert_outcome(
        "keys in another case",
        verified(grants),
        &sign(
            HDR,
            &format!(
                r#"{{"ISS":"{INTEROP_KEY}","Sub":"me","EXP":{exp},"Video":{{"ROOMJOIN":true,"Room":"r"}}}}"#
            ),
        ),
    );
    assert_outcome(
        "long s in sub",
        verified(only_me),
        &key_and("\"\u{17f}ub\":\"me\""),
    );
    assert_outcome(
        "kelvin in kind",
        verified(r#"{"identity":"me","kind":"agent","video":{"roomJoin":true,"room":"r"}}"#),
        &with(",\"\u{212a}ind\":\"agent\""),
    );

    // Text that is not Unicode, in a string, is read as the character that stands for
    // such, U+FFFD: a byte that is not UTF-8 (each such byte), and an escape that is half
    // of a surrogate pair. What is given here is what stands between the quotes of the
    // name in the token.
    let named = |name: &str| {
        verified(&format!(
            "{{\"identity\":\"me\",\"name\":{},\"video\":{{\"roomJoin\":true,\"room\":\"r\"}}}}",
            serde_json::to_string(name).unwrap()
        ))
    };
    let claims_with = |before: &str, bytes: &[u8], after: &str| {
        sign_bytes(
            HDR,
            &[
                format!("{{{base},\"exp\":{exp}{before}").as_bytes(),
                bytes,
                after.as_bytes(),
            ]
            .concat(),
        )
    };
    for (what, name, want) in [
        ("invalid utf-8 in a string", &b"a\xffb"[..], "a\u{fffd}b"),
        (
            "invalid utf-8, two bytes of three",
            b"a\xe2\x82b",
            "a\u{fffd}\u{fffd}b",
        ),
        ("half a surrogate pair", b"a\\ud800b", "a\u{fffd}b"),
        ("surrogate pair", b"a\\ud83d\\ude00b", "a\u{1f600}b"),
        (
            "pair the wrong way round",
            b"a\\ude00\\ud83db",
            "a\u{fffd}\u{fffd}b",
        ),
        (
            "high half and another escape",
            b"a\\ud83d\\u0041b",
            "a\u{fffd}Ab",
        ),
        ("high half at the end", b"a\\ud83d", "a\u{fffd}"),
        ("escaped backslash before u", b"a\\\\ud83db", "a\\ud83db"),
        (
            "upper case escape",
            b"a\\uD83D\\uDE00\\uD800b",
            "a\u{1f600}\u{fffd}b",
        ),
        (
            "escapes",
            b"\\u003c\\/\\u0041\\t\\u00e9\\u2028",
            "</A\t\u{e9}\u{2028}",
        ),
    ] {
        assert_outcome(what, named(want), &claims_with(",\"name\":\"", name, "\"}"));
    }
    // In the name of a claim it makes a name that is not known.
    assert_outcome(
        "half a pair in a key",
        verified(grants),
        &claims_with(",\"na", b"\\ud83d", "me\":\"x\"}"),
    );
    assert_outcome(
        "invalid utf-8 in a key",
        verified(grants),
        &claims_with(",\"na", b"\xff", "me\":\"x\"}"),
    );
    assert_outcome(
        "invalid utf-8 outside a string",
        parse_error(MALFORMED_CLAIM),
        &claims_with("", b"\xff", "}"),
    );

    for (name, more) in [
        ("bad escape", r#","name":"\x""#),
        ("control character in a string", ",\"name\":\"a\u{1}b\""),
    ] {
        assert_outcome(name, parse_error(MALFORMED_CLAIM), &with(more));
    }

    // The claims as a whole.
    assert_outcome(
        "payload null",
        verify_error_as("", "", EXP_REQUIRED),
        &sign(HDR, "null"),
    );
    assert_outcome(
        "empty object",
        verify_error_as("", "", EXP_REQUIRED),
        &sign(HDR, "{}"),
    );
    for (name, payload) in [
        ("payload array", "[]".to_string()),
        ("payload string", r#""x""#.to_string()),
        ("payload not json", "{".to_string()),
        ("empty payload", String::new()),
        (
            "payload with trailing text",
            format!("{{{base},\"exp\":{exp}}} x"),
        ),
        (
            "byte order mark",
            format!("\u{feff}{{{base},\"exp\":{exp}}}"),
        ),
    ] {
        assert_outcome(name, parse_error(MALFORMED_CLAIM), &sign(HDR, &payload));
    }
    assert_outcome(
        "payload with spaces",
        verified(only_me),
        &sign(
            HDR,
            &format!(r#" {{ "iss" : "{INTEROP_KEY}" , "sub":"me", "exp" : {exp} }} "#),
        ),
    );

    // The header.
    let claims = format!("{{{base},\"exp\":{exp}}}");
    for alg in ["HS512", "none", "RS256"] {
        assert_outcome(
            &format!("header alg {alg}"),
            verify_error(&format!(
                "token signature is invalid: signing method {alg} is invalid"
            )),
            &sign(&format!(r#"{{"alg":"{alg}","typ":"JWT"}}"#), &claims),
        );
    }
    for (name, header, err) in [
        (
            "header alg unknown",
            r#"{"alg":"XX999","typ":"JWT"}"#,
            "token is unverifiable: signing method (alg) is unavailable",
        ),
        (
            "header alg lower case",
            r#"{"alg":"hs256","typ":"JWT"}"#,
            "token is unverifiable: signing method (alg) is unavailable",
        ),
        (
            "header alg number",
            r#"{"alg":5}"#,
            "token is unverifiable: signing method (alg) is unspecified",
        ),
        (
            "header no alg",
            r#"{"typ":"JWT"}"#,
            "token is unverifiable: signing method (alg) is unspecified",
        ),
        (
            "header null",
            "null",
            "token is unverifiable: signing method (alg) is unspecified",
        ),
        (
            "header array",
            "[]",
            "token is malformed: could not JSON decode header: ",
        ),
    ] {
        assert_outcome(name, parse_error(err), &sign(header, &claims));
    }
    assert_outcome(
        "header no typ",
        verified(grants),
        &sign(r#"{"alg":"HS256"}"#, &claims),
    );
    assert_outcome(
        "header other typ, kid",
        verified(grants),
        &sign(r#"{"alg":"HS256","typ":"at+jwt","kid":"k"}"#, &claims),
    );

    // The token as text.
    let good = with("");
    let parts: Vec<&str> = good.split('.').collect();
    assert_outcome("trailing newline", verified(grants), &format!("{good}\n"));
    assert_outcome("trailing CRLF", verified(grants), &format!("{good}\r\n"));
    assert_outcome(
        "leading newline",
        verify_error(SIGNATURE_INVALID),
        &format!("\n{good}"),
    );
    for (name, token) in [
        ("trailing space", format!("{good} ")),
        ("padded signature", format!("{good}=")),
    ] {
        assert_outcome(
            name,
            parse_error("token is malformed: could not base64 decode signature: "),
            &token,
        );
    }
    for (name, token) in [
        ("two segments", format!("{}.{}", parts[0], parts[1])),
        ("four segments", format!("{good}.x")),
        ("empty token", String::new()),
    ] {
        assert_outcome(
            name,
            parse_error("token is malformed: token contains an invalid number of segments"),
            &token,
        );
    }
    assert_outcome(
        "empty signature",
        verify_error(SIGNATURE_INVALID),
        &format!("{}.{}.", parts[0], parts[1]),
    );
    assert_outcome(
        "signature cut short",
        verify_error(SIGNATURE_INVALID),
        &format!("{}.{}.{}", parts[0], parts[1], &parts[2][..20]),
    );
    // The last character of a signature of 32 bytes carries two bits that are not used.
    let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let last = alphabet.find(&parts[2][42..]).unwrap();
    let with_last = |index: usize| {
        format!(
            "{}.{}.{}{}",
            parts[0],
            parts[1],
            &parts[2][..42],
            &alphabet[index..index + 1]
        )
    };
    assert_outcome(
        "signature with other trailing bits",
        verified(grants),
        &with_last(last ^ 1),
    );
    assert_outcome(
        "signature differing in last bits",
        verify_error(SIGNATURE_INVALID),
        &with_last(last ^ 4),
    );

    // The key.
    let v = parse_api_token(&good).unwrap();
    assert_eq!(Err(Error::KeysMissing), v.verify("").map(|_| ()));
    assert!(v.verify(INTEROP_SECRET.as_bytes()).is_ok());
    assert_eq!(
        SIGNATURE_INVALID,
        v.verify("other").unwrap_err().to_string()
    );
    // Not as the original: a key of no bytes is no key here; to the original it is a key
    // that does not verify the signature ("signature is invalid").
    assert_eq!(Err(Error::KeysMissing), v.verify(b"").map(|_| ()));
    assert_eq!(
        "missing API key or secret key",
        Error::KeysMissing.to_string()
    );
}

// New for musivum: the room configuration in a token, which the original reads with
// protojson. What is expected is what the original did with the same token, and where this
// code does not do as the original does, the test says so.
#[test]
fn the_room_configuration_is_read_as_the_go_implementation_reads_it() {
    let now = jwt::time_now().unix_secs();
    let hour = now + 3600;
    let rc = |config: &str| {
        sign(
            HDR,
            &format!(
                r#"{{"iss":"{INTEROP_KEY}","sub":"me","video":{{"roomJoin":true,"room":"r"}},"exp":{hour},"roomConfig":{config}}}"#
            ),
        )
    };
    let verified = |config: &str| {
        Outcome::Verified(
            INTEROP_KEY.into(),
            "me".into(),
            hour,
            None,
            format!(
                r#"{{"identity":"me","video":{{"roomJoin":true,"room":"r"}},"roomConfig":{config}}}"#
            ),
        )
    };
    let refused = || Outcome::VerifyError(INTEROP_KEY.into(), "me".into(), MALFORMED_CLAIM.into());

    assert_outcome_as_json("empty", verified("{}"), &rc("{}"));
    assert_outcome_as_json(
        "unknown field",
        verified(r#"{"name":"r","egress":{}}"#),
        &rc(r#"{"name":"r","future":1,"egress":{"future":{"a":1}}}"#),
    );
    // A field has two names, and a number may be in quotes or written as a float.
    assert_outcome_as_json(
        "snake case",
        verified(r#"{"name":"r","emptyTimeout":5,"maxParticipants":7}"#),
        &rc(r#"{"name":"r","empty_timeout":5,"max_participants":"7"}"#),
    );
    assert_outcome_as_json(
        "float for an integer",
        verified(r#"{"emptyTimeout":5,"maxParticipants":10}"#),
        &rc(r#"{"emptyTimeout":5.0,"maxParticipants":1e1}"#),
    );
    assert_outcome_as_json(
        "large exponent",
        verified(r#"{"emptyTimeout":4000000000}"#),
        &rc(r#"{"emptyTimeout":4e9}"#),
    );
    assert_outcome_as_json(
        "negative zero",
        verified("{}"),
        &rc(r#"{"emptyTimeout":-0}"#),
    );
    assert_outcome_as_json(
        "negative zero float",
        verified("{}"),
        &rc(r#"{"emptyTimeout":-0.0}"#),
    );
    assert_outcome_as_json(
        "egress preset number",
        verified(r#"{"egress":{"room":{"preset":"H264_1080P_30"}}}"#),
        &rc(r#"{"egress":{"room":{"preset":2.0}}}"#),
    );
    assert_outcome_as_json(
        "empty tags",
        verified("{}"),
        &rc(r#"{"tags":{},"agents":[]}"#),
    );
    // But its name is not read in any case.
    assert_outcome_as_json(
        "case of a name",
        verified("{}"),
        &rc(r#"{"Name":"r","EMPTYTIMEOUT":5}"#),
    );
    assert_outcome_as_json(
        "null field",
        verified("{}"),
        &rc(r#"{"name":null,"emptyTimeout":null,"egress":null,"agents":null}"#),
    );
    assert_outcome_as_json(
        "enum by name",
        verified(r#"{"agents":[{"agentName":"a","restartPolicy":"JRP_NEVER"}]}"#),
        &rc(r#"{"agents":[{"agentName":"a","restartPolicy":"JRP_NEVER"}]}"#),
    );
    assert_outcome_as_json(
        "enum number",
        verified(r#"{"agents":[{"agentName":"a","restartPolicy":"JRP_NEVER"}]}"#),
        &rc(r#"{"agents":[{"agentName":"a","restartPolicy":1}]}"#),
    );
    assert_outcome_as_json(
        "enum null",
        verified(r#"{"agents":[{"agentName":"a"}]}"#),
        &rc(r#"{"agents":[{"agentName":"a","restartPolicy":null}]}"#),
    );
    assert_outcome_as_json(
        "egress enums",
        verified(
            r#"{"egress":{"room":{"preset":"H264_720P_30","fileOutputs":[{"fileType":"MP4"}],"segmentOutputs":[{"protocol":"HLS_PROTOCOL"}]}}}"#,
        ),
        &rc(
            r#"{"egress":{"room":{"preset":"H264_720P_30","fileOutputs":[{"fileType":"MP4"}],"segmentOutputs":[{"protocol":"HLS_PROTOCOL","suffix":"TIMESTAMP"}]}}}"#,
        ),
    );
    // Secrets are refused when a token is made, not when it is read.
    assert_outcome_as_json(
        "secrets",
        verified(
            r#"{"egress":{"room":{"fileOutputs":[{"s3":{"secret":"s"}}],"streamOutputs":[{"urls":["rtmp://x"]}]}}}"#,
        ),
        &rc(
            r#"{"egress":{"room":{"fileOutputs":[{"s3":{"secret":"s"}}],"streamOutputs":[{"urls":["rtmp://x"]}]}}}"#,
        ),
    );

    for (name, config) in [
        ("wrong type", r#"{"name":5}"#),
        ("string", r#""x""#),
        ("list", "[]"),
        ("number", "5"),
        ("fraction for an integer", r#"{"emptyTimeout":5.5}"#),
        ("negative for unsigned", r#"{"emptyTimeout":-1}"#),
        ("too large", r#"{"emptyTimeout":4294967296}"#),
        ("bool as string", r#"{"syncStreams":"true"}"#),
        ("null in agents", r#"{"agents":[null]}"#),
        ("null in tags", r#"{"tags":{"a":null}}"#),
        (
            "two outputs",
            r#"{"egress":{"room":{"fileOutputs":[{"s3":{"bucket":"b"},"gcp":{"bucket":"c"}}]}}}"#,
        ),
        ("both names", r#"{"emptyTimeout":1,"empty_timeout":2}"#),
        (
            "enum bool",
            r#"{"agents":[{"agentName":"a","restartPolicy":true}]}"#,
        ),
        ("padded number in quotes", r#"{"emptyTimeout":" 5"}"#),
        ("empty string for a number", r#"{"emptyTimeout":""}"#),
        ("hex in quotes", r#"{"emptyTimeout":"0x10"}"#),
        ("too large exponent", r#"{"emptyTimeout":5e9}"#),
        ("number for a bool", r#"{"syncStreams":1}"#),
        ("bool for a string", r#"{"name":true}"#),
        ("list for a message", r#"{"egress":[]}"#),
        ("object for a list", r#"{"agents":{}}"#),
        ("list for a map", r#"{"tags":[]}"#),
        ("number in a map of strings", r#"{"tags":{"a":1}}"#),
        (
            "egress two options",
            r#"{"egress":{"room":{"preset":1,"advanced":{"width":1}}}}"#,
        ),
    ] {
        assert_outcome_as_json(name, refused(), &rc(config));
    }

    // Not as the original, which takes a number in quotes for an integer also when it is
    // written with a fraction or an exponent (5 and 10): refused here.
    for (name, config) in [
        ("float in quotes", r#"{"emptyTimeout":"5.0"}"#),
        ("exponent in quotes", r#"{"emptyTimeout":"1e1"}"#),
    ] {
        assert_outcome_as_json(name, refused(), &rc(config));
    }

    // Not as the original, which leaves out a value of an enumeration that it does not
    // know by name, and keeps one that it does not know by number: both are refused here.
    for (name, config) in [
        (
            "unknown enum",
            r#"{"agents":[{"agentName":"a","restartPolicy":"JRP_FUTURE"}]}"#,
        ),
        (
            "enum lower case",
            r#"{"agents":[{"agentName":"a","restartPolicy":"jrp_never"}]}"#,
        ),
        (
            "enum unknown number",
            r#"{"agents":[{"agentName":"a","restartPolicy":99}]}"#,
        ),
    ] {
        assert_outcome_as_json(name, refused(), &rc(config));
    }
    // Not as the original, which refuses a field that is there twice: the last one counts
    // here.
    assert_outcome_as_json(
        "duplicate field",
        verified(r#"{"name":"s"}"#),
        &rc(r#"{"name":"r","name":"s"}"#),
    );
}

// New for musivum: where this code reads the claims of a token differently from the
// original. None of these is what a token of LiveKit's SDKs looks like.
#[test]
fn what_is_not_read_as_the_go_implementation_reads_it() {
    let now = jwt::time_now().unix_secs();
    let hour = now + 3600;
    let with = |more: &str| {
        sign(
            HDR,
            &format!(
                r#"{{"iss":"{INTEROP_KEY}","sub":"me","video":{{"roomJoin":true,"room":"r"}},"exp":{hour}{more}}}"#
            ),
        )
    };
    let verified = |grants: &str| {
        Outcome::Verified(INTEROP_KEY.into(), "me".into(), hour, None, grants.into())
    };

    // A claim that is there twice: the original reads one after the other, so that of two
    // strings the last counts and two grants are added up
    // ({"roomJoin":true,"room":"r","hidden":true}). Here the last counts.
    assert_outcome(
        "duplicate keys",
        verified(r#"{"identity":"me","name":"second","video":{"hidden":true}}"#),
        &with(r#","name":"first","name":"second","video":{"hidden":true}"#),
    );
    // A claim that is not known and holds what this code's JSON reader does not take: a
    // number that is more than a float holds, or lists in lists more than 127 deep. The
    // original does not look at what it does not know.
    assert_outcome(
        "unknown claim with a huge number",
        Outcome::ParseError(MALFORMED_CLAIM.into()),
        &with(r#","other":1e999"#),
    );
    assert_outcome(
        "roomConfig unknown field with any value",
        Outcome::ParseError(MALFORMED_CLAIM.into()),
        &with(r#","roomConfig":{"future":{"a":[1,"x",null,{"b":1e999}]}}"#),
    );
    assert_outcome(
        "unknown claim nested 200 deep",
        Outcome::ParseError(MALFORMED_CLAIM.into()),
        &with(&format!(
            ",\"other\":{}{}",
            "[".repeat(200),
            "]".repeat(200)
        )),
    );
    assert_outcome(
        "unknown claim nested 100 deep",
        verified(r#"{"identity":"me","video":{"roomJoin":true,"room":"r"}}"#),
        &with(&format!(
            ",\"other\":{}{}",
            "[".repeat(100),
            "]".repeat(100)
        )),
    );
    // Text that is not Unicode in the room configuration: protojson refuses it, where Go's
    // own JSON reader makes U+FFFD of it. Here it is U+FFFD everywhere.
    assert_outcome(
        "half a pair in the room configuration",
        verified(
            "{\"identity\":\"me\",\"video\":{\"roomJoin\":true,\"room\":\"r\"},\"roomConfig\":{\"name\":\"a\u{fffd}b\"}}",
        ),
        &with(",\"roomConfig\":{\"name\":\"a\\ud800b\"}"),
    );
    assert_outcome(
        "invalid utf-8 in the room configuration",
        verified(
            "{\"identity\":\"me\",\"video\":{\"roomJoin\":true,\"room\":\"r\"},\"roomConfig\":{\"name\":\"a\u{fffd}b\"}}",
        ),
        &sign_bytes(
            HDR,
            &[
                format!(
                    r#"{{"iss":"{INTEROP_KEY}","sub":"me","video":{{"roomJoin":true,"room":"r"}},"exp":{hour},"roomConfig":{{"name":"a"#
                )
                .as_bytes(),
                b"\xff",
                b"b\"}}",
            ]
            .concat(),
        ),
    );
}
