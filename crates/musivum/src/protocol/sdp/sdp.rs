// Ported from LiveKit's protocol, sdp/sdp.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the parsed description is the one of the WebRTC
// library's SDP crate (`crate::webrtc::sdp`), whose attributes hold their value as an
// `Option`: they are read with `sdp::attribute`, which is pion's `Attribute`, and written
// with `new_attribute` here, so that a value that is empty is no value, as pion marshals it;
// the errors are an enum, with those of pion's `webrtc` and `sdp` packages and of
// `strconv.ParseUint` that the original returns among them; a string and a flag that say
// whether it was found are an `Option`, two strings and an error a `Result` of a pair; the
// helpers return strings of their own, the accessors of `SdpFragment` borrow; the codecs of
// a media description are asked from it directly, where the original builds a session
// description around it; the pointers of the fragments are `Option`s, and one that the
// original dereferences without looking is made when it is not there; the fragments are
// built as strings, not as bytes; a condition inside a case of a switch is the guard of
// the match's arm where clippy asks for it.

use crate::webrtc::peerconnection::DtlsRole;
use crate::webrtc::sdp;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    // The errors of pion's `webrtc` package that the original returns.
    /// ErrSessionDescriptionNoFingerprint indicates SetRemoteDescription was called with a SessionDescription that has no
    /// fingerprint.
    #[error("SetRemoteDescription called with no fingerprint")]
    SessionDescriptionNoFingerprint,
    /// ErrSessionDescriptionInvalidFingerprint indicates SetRemoteDescription was called with a SessionDescription that
    /// has an invalid fingerprint.
    #[error("SetRemoteDescription called with an invalid fingerprint")]
    SessionDescriptionInvalidFingerprint,
    /// ErrSessionDescriptionConflictingFingerprints indicates SetRemoteDescription was called with a SessionDescription
    /// that has an conflicting fingerprints.
    #[error("SetRemoteDescription called with multiple conflicting fingerprint")]
    SessionDescriptionConflictingFingerprints,
    /// ErrSessionDescriptionMissingIceUfrag indicates SetRemoteDescription was called with a SessionDescription that
    /// is missing an ice-ufrag value.
    #[error("SetRemoteDescription called with no ice-ufrag")]
    SessionDescriptionMissingIceUfrag,
    /// ErrSessionDescriptionMissingIcePwd indicates SetRemoteDescription was called with a SessionDescription that
    /// is missing an ice-pwd value.
    #[error("SetRemoteDescription called with no ice-pwd")]
    SessionDescriptionMissingIcePwd,
    /// ErrSessionDescriptionConflictingIceUfrag  indicates SetRemoteDescription was called with a SessionDescription
    /// that contains multiple conflicting ice-ufrag values.
    #[error("SetRemoteDescription called with multiple conflicting ice-ufrag values")]
    SessionDescriptionConflictingIceUfrag,
    /// ErrSessionDescriptionConflictingIcePwd indicates SetRemoteDescription was called with a SessionDescription
    /// that contains multiple conflicting ice-pwd values.
    #[error("SetRemoteDescription called with multiple conflicting ice-pwd values")]
    SessionDescriptionConflictingIcePwd,

    // What `strconv.ParseUint` says of a format of a media description that is not a
    // payload type.
    #[error("strconv.ParseUint: parsing {0:?}: invalid syntax")]
    PayloadTypeSyntax(String),
    #[error("strconv.ParseUint: parsing {0:?}: value out of range")]
    PayloadTypeRange(String),
    /// The errors of pion's `sdp` package: a payload type that no codec is known for.
    #[error(transparent)]
    Sdp(#[from] sdp::Error),

    // The errors the original makes where it returns them.
    #[error("invalid media section")]
    InvalidMediaSection,
    #[error("too many media sections")]
    TooManyMediaSections,
    #[error("invalid attribute")]
    InvalidAttribute,
    #[error("missing media section")]
    MissingMediaSection,
    #[error("bundle media mismatch, expected: {expected}, got: {got}")]
    BundleMediaMismatch { expected: String, got: String },
    #[error("ice lite mismatch")]
    IceLiteMismatch,
    #[error("ice options mismatch")]
    IceOptionsMismatch,
    #[error("could not find media mid")]
    CouldNotFindMediaMid,
    #[error("could not get bundle mid")]
    CouldNotGetBundleMid,
    #[error("could not find bundle media: {0}")]
    CouldNotFindBundleMedia(String),
}

// pion's `sdp.Attribute{Key: key, Value: value}`.
fn new_attribute(key: &str, value: &str) -> sdp::Attribute {
    sdp::Attribute {
        key: key.to_string(),
        value: if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        },
    }
}

pub fn get_mid_value(media: &sdp::MediaDescription) -> String {
    for attr in &media.attributes {
        if attr.key == sdp::ATTR_KEY_MID {
            return attr.value.clone().unwrap_or_default();
        }
    }
    String::new()
}

pub fn extract_fingerprint(desc: &sdp::SessionDescription) -> Result<(String, String), Error> {
    let mut fingerprints: Vec<&str> = Vec::new();

    if let Some(fingerprint) = sdp::attribute(&desc.attributes, "fingerprint") {
        fingerprints.push(fingerprint);
    }

    for m in &desc.media_descriptions {
        if let Some(fingerprint) = sdp::attribute(&m.attributes, "fingerprint") {
            fingerprints.push(fingerprint);
        }
    }

    if fingerprints.is_empty() {
        return Err(Error::SessionDescriptionNoFingerprint);
    }

    for m in &fingerprints {
        if *m != fingerprints[0] {
            return Err(Error::SessionDescriptionConflictingFingerprints);
        }
    }

    let parts: Vec<&str> = fingerprints[0].split(' ').collect();
    if parts.len() != 2 {
        return Err(Error::SessionDescriptionInvalidFingerprint);
    }
    Ok((parts[1].to_string(), parts[0].to_string()))
}

pub fn extract_dtls_role(desc: &sdp::SessionDescription) -> DtlsRole {
    for md in &desc.media_descriptions {
        let Some(setup) = sdp::attribute(&md.attributes, sdp::ATTR_KEY_CONNECTION_SETUP) else {
            continue;
        };

        if setup == sdp::ConnectionRole::Active.to_string() {
            return DtlsRole::Client;
        }

        if setup == sdp::ConnectionRole::Passive.to_string() {
            return DtlsRole::Server;
        }
    }

    //
    // If 'setup' attribute is not available, use client role
    // as that is the default behaviour of answerers
    //
    // There seems to be some differences in how role is decided.
    // libwebrtc (Chrome) code - (https://source.chromium.org/chromium/chromium/src/+/main:third_party/webrtc/pc/jsep_transport.cc;l=592;drc=369fb686729e7eb20d2bd09717cec14269a399d7)
    // does not mention anything about ICE role when determining
    // DTLS Role.
    //
    // But, ORTC has this - https://github.com/w3c/ortc/issues/167#issuecomment-69409953
    // and pion/webrtc follows that (https://github.com/pion/webrtc/blob/e071a4eded1efd5d9b401bcfc4efacb3a2a5a53c/dtlstransport.go#L269)
    //
    // So if remote is ice-lite, pion will use DTLSRoleServer when answering
    // while browsers pick DTLSRoleClient.
    //
    DtlsRole::Client
}

pub fn extract_ice_credential(desc: &sdp::SessionDescription) -> Result<(String, String), Error> {
    let mut pwds: Vec<&str> = Vec::new();
    let mut ufrags: Vec<&str> = Vec::new();

    if let Some(ufrag) = sdp::attribute(&desc.attributes, "ice-ufrag") {
        ufrags.push(ufrag);
    }
    if let Some(pwd) = sdp::attribute(&desc.attributes, "ice-pwd") {
        pwds.push(pwd);
    }

    for m in &desc.media_descriptions {
        if let Some(ufrag) = sdp::attribute(&m.attributes, "ice-ufrag") {
            ufrags.push(ufrag);
        }
        if let Some(pwd) = sdp::attribute(&m.attributes, "ice-pwd") {
            pwds.push(pwd);
        }
    }

    if ufrags.is_empty() {
        return Err(Error::SessionDescriptionMissingIceUfrag);
    } else if pwds.is_empty() {
        return Err(Error::SessionDescriptionMissingIcePwd);
    }

    for m in &ufrags {
        if *m != ufrags[0] {
            return Err(Error::SessionDescriptionConflictingIceUfrag);
        }
    }

    for m in &pwds {
        if *m != pwds[0] {
            return Err(Error::SessionDescriptionConflictingIcePwd);
        }
    }

    Ok((ufrags[0].to_string(), pwds[0].to_string()))
}

pub fn extract_stream_id(media: &sdp::MediaDescription) -> Option<String> {
    let msid = sdp::attribute(&media.attributes, sdp::ATTR_KEY_MSID)?;
    let ids: Vec<&str> = msid.split(' ').collect();
    let stream_id = if ids.len() < 2 { msid } else { ids[1] };
    Some(stream_id.to_string())
}

pub fn get_ip(sdp: &sdp::SessionDescription) -> String {
    if let Some(ci) = &sdp.connection_information
        && ci.network_type == "IN"
        && let Some(address) = &ci.address
    {
        return address.address.clone();
    }

    for media in &sdp.media_descriptions {
        if let Some(ci) = &media.connection_information
            && ci.network_type == "IN"
            && let Some(address) = &ci.address
        {
            return address.address.clone();
        }
    }
    String::new()
}

pub fn get_media_stream_track(m: &sdp::MediaDescription) -> String {
    let mut mst = "";
    if let Some(msid) = sdp::attribute(&m.attributes, sdp::ATTR_KEY_MSID) {
        let parts: Vec<&str> = msid.split(' ').collect();
        if parts.len() == 2 {
            mst = parts[1];
        }
    }

    if mst.is_empty()
        && let Some(attr) = sdp::attribute(&m.attributes, sdp::ATTR_KEY_SSRC)
    {
        let parts: Vec<&str> = attr.split(' ').collect();
        if parts.len() == 3 && parts[1].to_lowercase().starts_with("msid:") {
            mst = parts[2];
        }
    }
    mst.to_string()
}

pub fn get_simulcast_rids(m: &sdp::MediaDescription) -> Option<Vec<String>> {
    let val = sdp::attribute(&m.attributes, "simulcast")?;

    let parts: Vec<&str> = val.split(' ').collect();
    if parts.len() != 2 || parts[0] != "send" {
        return None;
    }

    Some(parts[1].split(';').map(str::to_string).collect())
}

// Go's strconv.ParseUint(s, 10, 8): digits only, no sign.
fn parse_payload_type(payload_str: &str) -> Result<u8, Error> {
    if payload_str.is_empty() || !payload_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::PayloadTypeSyntax(payload_str.to_string()));
    }
    payload_str
        .parse()
        .map_err(|_| Error::PayloadTypeRange(payload_str.to_string()))
}

pub fn codecs_from_media_description(m: &sdp::MediaDescription) -> Result<Vec<sdp::Codec>, Error> {
    let mut out = Vec::new();
    let s = std::slice::from_ref(m);

    for payload_str in &m.media_name.formats {
        let payload_type = parse_payload_type(payload_str)?;

        let codec = match sdp::get_codec_for_payload_type(s, payload_type) {
            Ok(codec) => codec,
            Err(err) => {
                if payload_type == 0 {
                    continue;
                }
                return Err(err.into());
            }
        };

        out.push(codec);
    }

    Ok(out)
}

pub fn get_bundle_mid(parsed: &sdp::SessionDescription) -> Option<String> {
    if let Some(group_attribute) = sdp::attribute(&parsed.attributes, sdp::ATTR_KEY_GROUP) {
        let bundle_ids: Vec<&str> = group_attribute.split(' ').collect();
        // strings.EqualFold: no letter of "BUNDLE" has a fold outside ASCII
        if bundle_ids.len() > 1 && bundle_ids[0].eq_ignore_ascii_case("BUNDLE") {
            return Some(bundle_ids[1].to_string());
        }
    }

    None
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SdpFragmentIce {
    ufrag: String,
    pwd: String,
    lite: Option<bool>,
    options: String,
}

impl SdpFragmentIce {
    fn unmarshal(&mut self, attributes: &[sdp::Attribute]) -> Result<(), Error> {
        let get_attr = |key: &str| sdp::attribute(attributes, key);

        if let Some(ice_ufrag) = get_attr("ice-ufrag") {
            self.ufrag = ice_ufrag.to_string();
        }

        if let Some(ice_pwd) = get_attr("ice-pwd") {
            self.pwd = ice_pwd.to_string();
        }

        if get_attr(sdp::ATTR_KEY_ICE_LITE).is_some() {
            self.lite = Some(true);
        }

        if let Some(ice_options) = get_attr("ice-options") {
            self.options = ice_options.to_string();
        }

        Ok(())
    }

    fn marshal(&self) -> Result<String, Error> {
        let mut ice_fragment = String::new();
        let add_key_value = |ice_fragment: &mut String, key: &str, value: &str| {
            ice_fragment.push_str(key);
            if !value.is_empty() {
                ice_fragment.push_str(value);
            }
            ice_fragment.push_str("\r\n");
        };

        if !self.ufrag.is_empty() {
            add_key_value(&mut ice_fragment, "a=ice-ufrag:", &self.ufrag);
        }
        if !self.pwd.is_empty() {
            add_key_value(&mut ice_fragment, "a=ice-pwd:", &self.pwd);
        }
        if self.lite == Some(true) {
            add_key_value(&mut ice_fragment, "a=ice-lite", "");
        }
        if !self.options.is_empty() {
            add_key_value(&mut ice_fragment, "a=ice-options:", &self.options);
        }

        Ok(ice_fragment)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SdpFragmentMedia {
    info: String,
    mid: String,
    ice: Option<SdpFragmentIce>,
    candidates: Vec<String>,
    end_of_candidates: Option<bool>,
}

impl SdpFragmentMedia {
    fn unmarshal(&mut self, md: &sdp::MediaDescription) -> Result<(), Error> {
        // MediaName conversion to string taken from github.com/pion/sdp
        let mut info = String::new();
        let append_list = |info: &mut String, list: &[String], sep: char| {
            for (i, p) in list.iter().enumerate() {
                if i != 0 && i != list.len() {
                    info.push(sep);
                }
                info.push_str(p);
            }
        };

        info.push_str(&md.media_name.media);
        info.push(' ');

        info.push_str(&md.media_name.port.value.to_string());
        if let Some(range) = md.media_name.port.range {
            info.push('/');
            info.push_str(&range.to_string());
        }
        info.push(' ');

        append_list(&mut info, &md.media_name.protos, '/');
        info.push(' ');
        append_list(&mut info, &md.media_name.formats, ' ');
        self.info = info;

        if let Some(mid) = sdp::attribute(&md.attributes, sdp::ATTR_KEY_MID) {
            self.mid = mid.to_string();
        }

        let ice = self.ice.insert(SdpFragmentIce::default());
        ice.unmarshal(&md.attributes)?;

        for a in &md.attributes {
            if a.is_ice_candidate() {
                self.candidates.push(a.value.clone().unwrap_or_default());
            }
        }

        if sdp::attribute(&md.attributes, sdp::ATTR_KEY_END_OF_CANDIDATES).is_some() {
            self.end_of_candidates = Some(true);
        }
        Ok(())
    }

    fn marshal(&self) -> Result<String, Error> {
        let mut media_fragment = String::new();
        let add_key_value = |media_fragment: &mut String, key: &str, value: &str| {
            media_fragment.push_str(key);
            if !value.is_empty() {
                media_fragment.push_str(value);
            }
            media_fragment.push_str("\r\n");
        };

        if !self.info.is_empty() {
            add_key_value(&mut media_fragment, "m=", &self.info);
        }

        if !self.mid.is_empty() {
            add_key_value(&mut media_fragment, "a=mid:", &self.mid);
        }

        if let Some(ice) = &self.ice {
            let ice_fragment = ice.marshal()?;
            media_fragment.push_str(&ice_fragment);
        }

        for c in &self.candidates {
            add_key_value(&mut media_fragment, "a=candidate:", c);
        }
        if self.end_of_candidates == Some(true) {
            add_key_value(&mut media_fragment, "a=end-of-candidates", "");
        }

        Ok(media_fragment)
    }

    // `m.ice` where the original dereferences it: whatever makes a media section makes this
    // with it.
    fn ice_mut(&mut self) -> &mut SdpFragmentIce {
        self.ice.get_or_insert_with(SdpFragmentIce::default)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SdpFragment {
    group: String,
    ice: Option<SdpFragmentIce>,
    media: Option<SdpFragmentMedia>,
}

impl SdpFragment {
    // primarily for use with WHIP Trickle ICE - https://www.rfc-editor.org/rfc/rfc9725.html#name-trickle-ice
    pub fn unmarshal(&mut self, frag: &str) -> Result<(), Error> {
        let ice = self.ice.insert(SdpFragmentIce::default());

        let lines = frag.split('\n');
        for line in lines {
            let line = line.trim_end_matches([' ', '\r']);
            if line.is_empty() {
                continue;
            }

            // The original looks at bytes. Where a line is cut after its second byte, the
            // first two are known to be ASCII.
            let bytes = line.as_bytes();
            if bytes[0] == b'm' {
                if line.len() < 3 || bytes[1] != b'=' {
                    return Err(Error::InvalidMediaSection);
                }

                if self.media.is_some() {
                    return Err(Error::TooManyMediaSections);
                }

                self.media = Some(SdpFragmentMedia {
                    ice: Some(SdpFragmentIce::default()),
                    info: line[2..].to_string(),
                    ..Default::default()
                });
            }

            if bytes[0] != b'a' {
                // not an attribute, skip
                continue;
            }

            if line.len() < 2 || bytes[1] != b'=' {
                return Err(Error::InvalidAttribute);
            }

            let line = &line[2..];
            let Some(delim_index) = line.find(':') else {
                if line == sdp::ATTR_KEY_ICE_LITE {
                    match &mut self.media {
                        Some(media) => media.ice_mut().lite = Some(true),
                        None => ice.lite = Some(true),
                    }
                }
                continue;
            };

            let value = &line[delim_index + 1..];
            match &line[..delim_index] {
                sdp::ATTR_KEY_GROUP => self.group = value.to_string(),

                "ice-ufrag" => match &mut self.media {
                    Some(media) => media.ice_mut().ufrag = value.to_string(),
                    None => ice.ufrag = value.to_string(),
                },

                "ice-pwd" => match &mut self.media {
                    Some(media) => media.ice_mut().pwd = value.to_string(),
                    None => ice.pwd = value.to_string(),
                },

                "ice-options" => match &mut self.media {
                    Some(media) => media.ice_mut().options = value.to_string(),
                    None => ice.options = value.to_string(),
                },

                sdp::ATTR_KEY_MID => {
                    if let Some(media) = &mut self.media {
                        media.mid = value.to_string();
                    }
                }

                sdp::ATTR_KEY_CANDIDATE => {
                    if let Some(media) = &mut self.media {
                        media.candidates.push(value.to_string());
                    }
                }

                sdp::ATTR_KEY_END_OF_CANDIDATES => {
                    if let Some(media) = &mut self.media {
                        media.end_of_candidates = Some(true);
                    }
                }

                _ => {}
            }
        }

        let Some(media) = &self.media else {
            return Err(Error::MissingMediaSection);
        };

        if !self.group.is_empty() {
            let bundle_ids: Vec<&str> = self.group.split(' ').collect();
            if bundle_ids.len() > 1
                && bundle_ids[0].eq_ignore_ascii_case("BUNDLE")
                && media.mid != bundle_ids[1]
            {
                return Err(Error::BundleMediaMismatch {
                    expected: bundle_ids[1].to_string(),
                    got: media.mid.clone(),
                });
            }
        }

        Ok(())
    }

    // primarily for use with WHIP ICE Restart - https://www.rfc-editor.org/rfc/rfc9725.html#name-ice-restarts
    pub fn marshal(&self) -> Result<String, Error> {
        let mut sdp_fragment = String::new();
        let add_key_value = |sdp_fragment: &mut String, key: &str, value: &str| {
            sdp_fragment.push_str(key);
            if !value.is_empty() {
                sdp_fragment.push_str(value);
            }
            sdp_fragment.push_str("\r\n");
        };

        if !self.group.is_empty() {
            add_key_value(&mut sdp_fragment, "a=group:", &self.group);
        }

        if let Some(ice) = &self.ice {
            let ice_fragment = ice.marshal()?;
            sdp_fragment.push_str(&ice_fragment);
        }

        if let Some(media) = &self.media {
            let media_fragment = media.marshal()?;
            sdp_fragment.push_str(&media_fragment);
        }

        Ok(sdp_fragment)
    }

    pub fn mid(&self) -> &str {
        if let Some(media) = &self.media {
            return &media.mid;
        }

        ""
    }

    pub fn candidates(&self) -> &[String] {
        if let Some(media) = &self.media {
            return &media.candidates;
        }

        &[]
    }

    pub fn extract_ice_credential(&self) -> Result<(String, String), Error> {
        let mut pwds: Vec<&str> = Vec::new();
        let mut ufrags: Vec<&str> = Vec::new();

        if let Some(ice) = &self.ice {
            if !ice.ufrag.is_empty() {
                ufrags.push(&ice.ufrag);
            }
            if !ice.pwd.is_empty() {
                pwds.push(&ice.pwd);
            }
        }

        if let Some(media) = &self.media
            && let Some(ice) = &media.ice
        {
            if !ice.ufrag.is_empty() {
                ufrags.push(&ice.ufrag);
            }
            if !ice.pwd.is_empty() {
                pwds.push(&ice.pwd);
            }
        }

        if ufrags.is_empty() {
            return Err(Error::SessionDescriptionMissingIceUfrag);
        } else if pwds.is_empty() {
            return Err(Error::SessionDescriptionMissingIcePwd);
        }

        for m in &ufrags {
            if *m != ufrags[0] {
                return Err(Error::SessionDescriptionConflictingIceUfrag);
            }
        }

        for m in &pwds {
            if *m != pwds[0] {
                return Err(Error::SessionDescriptionConflictingIcePwd);
            }
        }

        Ok((ufrags[0].to_string(), pwds[0].to_string()))
    }

    // primarily for use with WHIP ICE Restart - https://www.rfc-editor.org/rfc/rfc9725.html#name-ice-restarts
    pub fn patch_ice_credential_and_candidates_into_sdp(
        &self,
        parsed: &mut sdp::SessionDescription,
    ) -> Result<(), Error> {
        // ice-options and ice-lite should match
        if let Some(ice) = &self.ice
            && (ice.lite.is_some() || !ice.options.is_empty())
        {
            for a in &parsed.attributes {
                match a.key.as_str() {
                    "ice-lite" if ice.lite != Some(true) => {
                        return Err(Error::IceLiteMismatch);
                    }
                    "ice-options"
                        if !ice.options.is_empty()
                            && ice.options != a.value.as_deref().unwrap_or("") =>
                    {
                        return Err(Error::IceOptionsMismatch);
                    }
                    _ => {}
                }
            }
        }

        let mut found_media_mid = false;
        if let Some(media) = &self.media
            && !media.mid.is_empty()
        {
            for md in &parsed.media_descriptions {
                if sdp::attribute(&md.attributes, sdp::ATTR_KEY_MID) == Some(media.mid.as_str()) {
                    found_media_mid = true;
                    break;
                }
            }
        }
        if !found_media_mid {
            return Err(Error::CouldNotFindMediaMid);
        }

        if let Some(media) = &self.media
            && let Some(ice) = &media.ice
            && (ice.lite.is_some() || !ice.options.is_empty())
        {
            for md in &parsed.media_descriptions {
                for a in &md.attributes {
                    match a.key.as_str() {
                        "ice-lite" if ice.lite != Some(true) => {
                            return Err(Error::IceLiteMismatch);
                        }
                        "ice-options"
                            if !ice.options.is_empty()
                                && ice.options != a.value.as_deref().unwrap_or("") =>
                        {
                            return Err(Error::IceOptionsMismatch);
                        }
                        _ => {}
                    }
                }
            }
        }

        if let Some(ice) = &self.ice
            && !ice.ufrag.is_empty()
            && !ice.pwd.is_empty()
        {
            for a in parsed.attributes.iter_mut() {
                match a.key.as_str() {
                    "ice-ufrag" => *a = new_attribute("ice-ufrag", &ice.ufrag),
                    "ice-pwd" => *a = new_attribute("ice-pwd", &ice.pwd),
                    _ => {}
                }
            }
        }

        if let Some(media) = &self.media {
            for md in parsed.media_descriptions.iter_mut() {
                if let Some(ice) = &media.ice {
                    for a in md.attributes.iter_mut() {
                        match a.key.as_str() {
                            "ice-ufrag" if !ice.ufrag.is_empty() => {
                                *a = new_attribute("ice-ufrag", &ice.ufrag);
                            }
                            "ice-pwd" if !ice.pwd.is_empty() => {
                                *a = new_attribute("ice-pwd", &ice.pwd);
                            }
                            _ => {}
                        }
                    }
                }

                // clean out existing candidates and patch in new ones
                md.attributes.retain(|a| {
                    !(a.is_ice_candidate() || a.key == sdp::ATTR_KEY_END_OF_CANDIDATES)
                });

                for ic in &media.candidates {
                    md.attributes
                        .push(new_attribute(sdp::ATTR_KEY_CANDIDATE, ic));
                }

                if media.end_of_candidates == Some(true) {
                    md.attributes
                        .push(new_attribute(sdp::ATTR_KEY_END_OF_CANDIDATES, ""));
                }
            }
        }
        Ok(())
    }
}

// primarily for use with WHIP ICE Restart - https://www.rfc-editor.org/rfc/rfc9725.html#name-ice-restarts
pub fn extract_sdp_fragment(parsed: &sdp::SessionDescription) -> Result<SdpFragment, Error> {
    let Some(bundle_mid) = get_bundle_mid(parsed) else {
        return Err(Error::CouldNotGetBundleMid);
    };

    let mut s = SdpFragment::default();
    if let Some(group) = sdp::attribute(&parsed.attributes, sdp::ATTR_KEY_GROUP) {
        s.group = group.to_string();
    }

    let ice = s.ice.insert(SdpFragmentIce::default());
    ice.unmarshal(&parsed.attributes)?;

    let mut found_bundle_media = false;
    for md in &parsed.media_descriptions {
        if sdp::attribute(&md.attributes, sdp::ATTR_KEY_MID) != Some(bundle_mid.as_str()) {
            continue;
        }

        found_bundle_media = true;

        let media = s.media.insert(SdpFragmentMedia::default());
        media.unmarshal(md)?;
        break;
    }

    if !found_bundle_media {
        return Err(Error::CouldNotFindBundleMedia(bundle_mid));
    }

    Ok(s)
}

#[cfg(test)]
#[path = "sdp_test.rs"]
mod sdp_test;

#[cfg(test)]
#[path = "reference_test.rs"]
mod reference_test;
