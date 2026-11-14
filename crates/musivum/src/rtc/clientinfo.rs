// Ported from LiveKit, pkg/rtc/clientinfo.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the message of the protocol that the original embeds
// as a pointer is the field `client_info`, an `Option<Arc<..>>`: its fields are reached
// through that field, not through the wrapper. The SDK and the capabilities are compared as
// the numbers they are in the message, as the original does, so that a value this version of
// the protocol does not name is not taken for `UNKNOWN`. `strings.EqualFold` is the function
// `equal_fold` at the end, `strconv.Atoi` with its error dropped the function `atoi`.

use std::num::IntErrorKind;
use std::sync::Arc;

use crate::protocol::livekit;
use crate::protocol::livekit::client_info::{Capability, Sdk};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientInfo {
    pub client_info: Option<Arc<livekit::ClientInfo>>,
}

impl ClientInfo {
    fn is_firefox(&self) -> bool {
        self.client_info.as_ref().is_some_and(|c| {
            equal_fold(&c.browser, "firefox") || equal_fold(&c.browser, "firefox mobile")
        })
    }

    fn is_safari(&self) -> bool {
        self.client_info
            .as_ref()
            .is_some_and(|c| equal_fold(&c.browser, "safari"))
    }

    fn is_go(&self) -> bool {
        self.client_info
            .as_ref()
            .is_some_and(|c| c.sdk == Sdk::Go as i32)
    }

    fn is_linux(&self) -> bool {
        self.client_info
            .as_ref()
            .is_some_and(|c| equal_fold(&c.os, "linux"))
    }

    fn is_android(&self) -> bool {
        self.client_info
            .as_ref()
            .is_some_and(|c| equal_fold(&c.os, "android"))
    }

    #[allow(dead_code)] // not called upstream either
    fn is_obs(&self) -> bool {
        self.client_info
            .as_ref()
            .is_some_and(|c| c.browser.contains("OBS"))
    }

    pub fn supports_audio_red(&self) -> bool {
        !self.is_firefox() && !self.is_safari()
    }

    pub fn supports_prflx_over_relay(&self) -> bool {
        !self.is_firefox()
    }

    /// GoSDK(pion) relies on rtp packets to fire ontrack event, browsers and native (libwebrtc) rely on sdp
    pub fn fire_track_by_rtp_packet(&self) -> bool {
        self.is_go()
    }

    pub fn supports_codec_change(&self) -> bool {
        self.client_info
            .as_ref()
            .is_some_and(|c| c.sdk != Sdk::Go as i32 && c.sdk != Sdk::Unknown as i32)
    }

    pub fn can_handle_reconnect_response(&self) -> bool {
        // The original reads `c.Sdk` here through the embedded pointer without checking it
        // for nil, and panics when there is no message. So does this.
        let c = self
            .client_info
            .as_ref()
            .expect("nil pointer dereference: ClientInfo without a message");
        if c.sdk == Sdk::Js as i32 {
            // JS handles Reconnect explicitly in 1.6.3, prior to 1.6.4 it could not handle unknown responses
            if self.compare_version("1.6.3") < 0 {
                return false;
            }
        }
        true
    }

    pub fn supports_ice_tcp(&self) -> bool {
        let Some(c) = self.client_info.as_ref() else {
            return false;
        };
        if c.sdk == Sdk::Go as i32 {
            // Go does not support active TCP
            return false;
        }
        if c.sdk == Sdk::Swift as i32 {
            // ICE/TCP added in 1.0.5
            return self.compare_version("1.0.5") >= 0;
        }
        // most SDKs support ICE/TCP
        true
    }

    pub fn supports_change_rtp_sender_encoding_active(&self) -> bool {
        !self.is_firefox()
    }

    pub fn comply_with_codec_order_in_sdp_answer(&self) -> bool {
        (!self.is_linux() && !self.is_android()) || !self.is_firefox()
    }

    /// Rust SDK can't decode unknown signal message (TrackSubscribed and ErrorResponse)
    pub fn supports_track_subscribed_event(&self) -> bool {
        self.get_sdk() != Sdk::Rust as i32 || self.get_protocol() >= 10
    }

    pub fn supports_request_response(&self) -> bool {
        self.supports_track_subscribed_event()
    }

    pub fn supports_sctp_zero_checksum(&self) -> bool {
        self.get_sdk() != Sdk::Unknown as i32
            && (!self.is_go() || self.compare_version("2.4.0") >= 0)
    }

    pub fn supports_transceiver_reuse(&self) -> bool {
        !self.is_safari()
    }

    pub fn has_capability(&self, cap: Capability) -> bool {
        let Some(c) = self.client_info.as_ref() else {
            return false;
        };
        c.capabilities.contains(&(cap as i32))
    }

    pub fn supports_packet_trailer(&self) -> bool {
        self.has_capability(Capability::CapPacketTrailer)
    }

    // compareVersion compares a semver against the current client SDK version
    // returning 1 if current version is greater than version
    // 0 if they are the same, and -1 if it's an earlier version
    fn compare_version(&self, version: &str) -> i64 {
        let Some(c) = self.client_info.as_ref() else {
            return -1;
        };
        let parts0: Vec<&str> = c.version.split('.').collect();
        let parts1: Vec<&str> = version.split('.').collect();
        let mut ints0 = [0i64; 3];
        let mut ints1 = [0i64; 3];
        for i in 0..3 {
            if parts0.len() > i {
                ints0[i] = atoi(parts0[i]);
            }
            if parts1.len() > i {
                ints1[i] = atoi(parts1[i]);
            }
            if ints0[i] > ints1[i] {
                return 1;
            } else if ints0[i] < ints1[i] {
                return -1;
            }
        }
        0
    }

    // `c.ClientInfo.GetSdk()` of the original: the getter of the generated code, which gives
    // the zero value when there is no message.
    fn get_sdk(&self) -> i32 {
        self.client_info.as_ref().map_or(0, |c| c.sdk)
    }

    // `c.ClientInfo.GetProtocol()` of the original, likewise.
    fn get_protocol(&self) -> i32 {
        self.client_info.as_ref().map_or(0, |c| c.protocol)
    }
}

// Go's strconv.Atoi(s) with its error dropped, as the original calls it: zero for what is
// not a number, and the largest or smallest value for a number that is out of range.
fn atoi(s: &str) -> i64 {
    match s.parse::<i64>() {
        Ok(n) => n,
        Err(err) => match err.kind() {
            IntErrorKind::PosOverflow => i64::MAX,
            IntErrorKind::NegOverflow => i64::MIN,
            _ => 0,
        },
    }
}

// Go's strings.EqualFold(s, t) for a `t` that is ASCII, as every name is that this file
// compares with: whether the two are equal under Unicode's simple case folding. That folding
// takes, next to the ASCII letters in their other case, two more characters to an ASCII
// letter: 'ſ' (U+017F) to 's' and the Kelvin sign (U+212A) to 'k'. (The same function is in
// `protocol/codecs/mime/mimetype.rs`, private to that file.)
fn equal_fold(s: &str, t: &str) -> bool {
    let mut t = t.chars();
    for sr in s.chars() {
        let Some(tr) = t.next() else {
            return false;
        };
        if sr.eq_ignore_ascii_case(&tr) {
            continue;
        }
        match (sr, tr) {
            ('\u{17f}', 's' | 'S') | ('\u{212a}', 'k' | 'K') => continue,
            _ => return false,
        }
    }
    t.next().is_none()
}

#[cfg(test)]
#[path = "clientinfo_test.rs"]
mod clientinfo_test;
