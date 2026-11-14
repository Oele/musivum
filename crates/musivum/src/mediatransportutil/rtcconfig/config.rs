// Ported from LiveKit's mediatransportutil, pkg/rtcconfig/config.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the `yaml` tags are doc comments on the fields and
// their names for serde, with `Deserialize` derived where the original lets yaml.v3 decode
// (a field that is left out keeps Go's zero value); `UnmarshalYAML` of `PortRange` and
// `NodeIP` is `Deserialize` (a number or a string where the original takes the text of a
// scalar; a mapping or a sequence is passed over without a word, as there), `MarshalYAML` is
// `Serialize`; a `time.Duration` is decoded from a string such as "100ms", which is what
// yaml.v3 takes for one (`parse_duration`, written new from the documented format of Go's
// `time.ParseDuration`); the errors the original makes with `fmt.Errorf` are variants of the
// package's `Error` (`mod.rs`) with the same texts, `strconv.Atoi`'s among them (`atoi`).

use std::fmt;
use std::net::IpAddr;

use serde::de::{self, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::ser::{self, Serializer};
use serde::{Deserialize, Serialize};

use crate::go::time::Duration;
use crate::protocol::logger;
use crate::warnw;

use super::Error;

// number of packets to buffer up
#[allow(dead_code)] // for the TCP mux, which is not there (see `new_webrtc_config`)
pub(super) const READ_BUFFER_SIZE: usize = 50;

#[cfg_attr(not(unix), allow(dead_code))] // read in rtc_unix.rs
pub(super) const MIN_UDP_BUFFER_SIZE: usize = 5_000_000;
#[allow(dead_code)] // for the TCP mux, which is not there (see `new_webrtc_config`)
pub(super) const WRITE_BUFFER_SIZE_IN_BYTES: usize = 4 * 1024 * 1024;
pub(super) const DEFAULT_UDP_BUFFER_SIZE: usize = 16_777_216;

pub const DEFAULT_STUN_SERVERS: [&str; 3] = [
    "global.stun.twilio.com:3478",
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
];

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct RtcConfig {
    /// `udp_port` in YAML.
    pub udp_port: PortRange,
    /// `tcp_port` in YAML.
    pub tcp_port: u32,
    /// `port_range_start` in YAML.
    #[serde(rename = "port_range_start")]
    pub ice_port_range_start: u32,
    /// `port_range_end` in YAML.
    #[serde(rename = "port_range_end")]
    pub ice_port_range_end: u32,
    /// `use_stun_port_as_ice` in YAML.
    pub use_stun_port_as_ice: bool,
    /// `node_ip` in YAML.
    pub node_ip: NodeIp,
    /// Not in YAML.
    #[serde(skip)]
    pub node_ip_auto_generated: bool,
    /// `stun_servers` in YAML.
    pub stun_servers: Vec<String>,
    /// `use_external_ip` in YAML.
    pub use_external_ip: bool,
    /// `require_ipv4` in YAML.
    // require a routable IPv4: dial STUN over udp4 and skip local IPv6 fallback
    pub require_ipv4: bool,
    /// `skip_external_ip_validation` in YAML.
    pub skip_external_ip_validation: bool,
    /// `advertise_internal_ip` in YAML.
    pub advertise_internal_ip: bool,
    /// `use_ice_lite` in YAML.
    pub use_ice_lite: bool,
    /// `interfaces` in YAML.
    pub interfaces: InterfacesConfig,
    /// `ips` in YAML.
    pub ips: IpsConfig,
    /// `enable_loopback_candidate` in YAML.
    pub enable_loopback_candidate: bool,
    /// `use_mdns` in YAML.
    pub use_mdns: bool,
    /// `external_ip_only` in YAML.
    // when UseExternalIP is true, only advertise the external IP to client
    pub external_ip_only: bool,
    /// `batch_io` in YAML.
    pub batch_io: BatchIoConfig,

    // SCTP congestion control
    /// `sctp_min_cwnd` in YAML.
    pub sctp_min_cwnd: i64,
    /// `sctp_fast_rtx_wnd` in YAML.
    pub sctp_fast_rtx_wnd: i64,
    /// `sctp_cwnd_ca_step` in YAML.
    pub sctp_cwnd_ca_step: i64,

    // ICE candidate-pair acceptance min-waits; 0 keeps pion's default
    /// `host_acceptance_min_wait` in YAML.
    #[serde(deserialize_with = "deserialize_duration")]
    pub host_acceptance_min_wait: Duration,
    /// `srflx_acceptance_min_wait` in YAML.
    #[serde(deserialize_with = "deserialize_duration")]
    pub srflx_acceptance_min_wait: Duration,
    /// `prflx_acceptance_min_wait` in YAML.
    #[serde(deserialize_with = "deserialize_duration")]
    pub prflx_acceptance_min_wait: Duration,
    /// `relay_acceptance_min_wait` in YAML.
    #[serde(deserialize_with = "deserialize_duration")]
    pub relay_acceptance_min_wait: Duration,

    // for testing, disable UDP
    /// `force_tcp` in YAML.
    pub force_tcp: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct InterfacesConfig {
    /// `includes` in YAML.
    pub includes: Vec<String>,
    /// `excludes` in YAML.
    pub excludes: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct IpsConfig {
    /// `includes` in YAML.
    pub includes: Vec<String>,
    /// `excludes` in YAML.
    pub excludes: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct BatchIoConfig {
    /// `batch_size` in YAML.
    pub batch_size: i64,
    /// `max_flush_interval` in YAML.
    #[serde(deserialize_with = "deserialize_duration")]
    pub max_flush_interval: Duration,
}

impl RtcConfig {
    pub fn validate(&mut self, development: bool) -> Result<(), Error> {
        // set defaults for ports if none are set
        if !self.udp_port.valid() && self.ice_port_range_start == 0 {
            // to make it easier to run in dev mode/docker, default to single port
            if development {
                self.udp_port = PortRange {
                    start: 7882,
                    ..Default::default()
                };
            } else {
                self.ice_port_range_start = 50000;
                self.ice_port_range_end = 60000;
            }
        }

        if self.node_ip.is_empty() || self.use_external_ip {
            self.resolve_node_ip()?;
        }

        Ok(())
    }

    // resolveNodeIP runs external-IP discovery (STUN when UseExternalIP, otherwise
    // local interface enumeration) and stores the result in NodeIP. When RequireIPv4
    // is set, discovery that cannot produce an IPv4 returns an error rather than
    // storing an IPv6-only NodeIP.
    fn resolve_node_ip(&mut self) -> Result<(), Error> {
        let ip_addr = match self.determine_ip() {
            Ok(ip_addr) => ip_addr,
            Err(err) => {
                warnw!(logger::get_logger(), "could not determine node ip", err);
                return Err(err);
            }
        };
        if parse_ip(&ip_addr).is_some_and(|ip| ip.is_ipv4()) {
            self.node_ip.v4 = ip_addr;
        } else {
            self.node_ip.v6 = ip_addr;
        }
        self.node_ip_auto_generated = true;
        Ok(())
    }

    // ReloadExternalIP discards the currently resolved NodeIP and re-runs discovery.
    // It lets callers retry a boot-time resolution miss (e.g. the node's IPv4 not
    // being attached yet) without mutating config fields and re-invoking Validate.
    // Set RequireIPv4 first if the caller needs an IPv4.
    pub fn reload_external_ip(&mut self) -> Result<(), Error> {
        self.node_ip = NodeIp::default();
        self.node_ip_auto_generated = false;
        self.resolve_node_ip()
    }
}

/// In YAML a port (`7882`) or a range of ports (`50000-60000`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PortRange {
    pub start: i64,
    pub end: i64,
}

// MarshalYAML
impl Serialize for PortRange {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.end == 0 {
            return serializer.serialize_i64(self.start);
        }

        if self.end <= self.start {
            return Err(ser::Error::custom(Error::EndPortNotGreater {
                end: self.end,
                start: self.start,
            }));
        }
        serializer.serialize_str(&format!("{}-{}", self.start, self.end))
    }
}

// UnmarshalYAML
impl<'de> Deserialize<'de> for PortRange {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut r = PortRange::default();
        if let Some(value) =
            deserializer.deserialize_any(ScalarText("a port or a range of ports"))?
        {
            r.unmarshal_string(&value).map_err(de::Error::custom)?;
        }
        Ok(r)
    }
}

impl PortRange {
    pub fn unmarshal_string(&mut self, str: &str) -> Result<(), Error> {
        if str.is_empty() {
            return Ok(());
        }
        if str.contains('-') {
            let parts: Vec<&str> = str.split('-').collect();
            if parts.len() != 2 {
                return Err(Error::InvalidPortRange(str.to_string()));
            }
            let start = atoi(parts[0].trim_matches(' '))
                .map_err(|err| Error::InvalidStartPort(parts[0].to_string(), err))?;
            let end = atoi(parts[1].trim_matches(' '))
                .map_err(|err| Error::InvalidEndPort(parts[1].to_string(), err))?;
            if end <= start {
                return Err(Error::EndPortNotGreater { end, start });
            }

            self.start = start;
            self.end = end;
            return Ok(());
        }

        let port = atoi(str).map_err(|err| Error::InvalidPort(str.to_string(), err))?;
        self.start = port;
        Ok(())
    }

    pub fn to_slice(&self) -> Vec<i64> {
        if self.end == 0 || self.end <= self.start {
            return vec![self.start];
        }

        (self.start..=self.end).collect()
    }

    pub fn valid(&self) -> bool {
        self.start != 0
    }
}

/// In YAML one address, or an IPv4 and an IPv6 address with a comma between them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeIp {
    pub v4: String,
    pub v6: String,
}

// MarshalYAML
impl Serialize for NodeIp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if !self.v4.is_empty() && !self.v6.is_empty() {
            return serializer.serialize_str(&format!("{},{}", self.v4, self.v6));
        }
        if !self.v4.is_empty() {
            return serializer.serialize_str(&self.v4);
        }
        serializer.serialize_str(&self.v6)
    }
}

// UnmarshalYAML
impl<'de> Deserialize<'de> for NodeIp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut n = NodeIp::default();
        if let Some(value) =
            deserializer.deserialize_any(ScalarText("the addresses of the node"))?
        {
            n.unmarshal_string(&value).map_err(de::Error::custom)?;
        }
        Ok(n)
    }
}

impl NodeIp {
    pub fn unmarshal_string(&mut self, str: &str) -> Result<(), Error> {
        if str.is_empty() {
            return Ok(());
        }
        let parts: Vec<&str> = str.split(',').collect();
        if parts.len() > 2 {
            return Err(Error::InvalidNodeIp(str.to_string()));
        }

        for part in parts {
            let Some(ip) = parse_ip(part.trim_matches(' ')) else {
                return Err(Error::InvalidIp(part.to_string()));
            };
            if ip.is_ipv4() {
                if !self.v4.is_empty() {
                    return Err(Error::MultipleIpv4(self.v4.clone(), ip.to_string()));
                }
                self.v4 = ip.to_string();
            } else {
                if !self.v6.is_empty() {
                    return Err(Error::MultipleIpv6(self.v6.clone(), ip.to_string()));
                }
                self.v6 = ip.to_string();
            }
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.v4.is_empty() && self.v6.is_empty()
    }

    pub fn to_string_slice(&self) -> Vec<String> {
        let mut ips = Vec::new();
        if !self.v4.is_empty() {
            ips.push(self.v4.clone());
        }
        if !self.v6.is_empty() {
            ips.push(self.v6.clone());
        }
        ips
    }

    pub fn primary_ip(&self) -> String {
        if !self.v4.is_empty() {
            return self.v4.clone();
        }
        self.v6.clone()
    }
}

/// Go's `net.ParseIP`: an IPv4 address in an IPv6 address (`::ffff:10.0.0.1`) is the IPv4
/// address, as `To4` and `String` have it there.
pub(super) fn parse_ip(s: &str) -> Option<IpAddr> {
    s.parse::<IpAddr>().ok().map(|ip| ip.to_canonical())
}

/// Go's `strconv.Atoi`, with the text of its error.
fn atoi(s: &str) -> Result<i64, String> {
    use std::num::IntErrorKind;

    s.parse::<i64>().map_err(|err| {
        let reason = match err.kind() {
            IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => "value out of range",
            _ => "invalid syntax",
        };
        format!("strconv.Atoi: parsing {s:?}: {reason}")
    })
}

/// What `UnmarshalYAML` of the original is given as `value.Value`: the text of a scalar.
/// Nothing for what has no such text (null, a mapping, a sequence), which the original takes
/// for an empty string and leaves at that.
struct ScalarText(&'static str);

impl<'de> Visitor<'de> for ScalarText {
    type Value = Option<String>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(Some(v.to_string()))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(Some(v.to_string()))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        Ok(Some(v.to_string()))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }
}

/// A `time.Duration` as yaml.v3 decodes one: from a string that Go's `time.ParseDuration`
/// takes. A number is refused, as there.
fn deserialize_duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    struct DurationText;

    impl Visitor<'_> for DurationText {
        type Value = Duration;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a duration as a string, such as \"100ms\"")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            parse_duration(v).ok_or_else(|| de::Error::invalid_value(de::Unexpected::Str(v), &self))
        }
    }

    deserializer.deserialize_any(DurationText)
}

/// Go's `time.ParseDuration`: a possibly signed sequence of decimal numbers, each with an
/// optional fraction and a unit ("ns", "us" or "µs", "ms", "s", "m", "h"), such as "300ms",
/// "-1.5h" or "2h45m"; "0" needs no unit.
pub(crate) fn parse_duration(s: &str) -> Option<Duration> {
    let (negative, mut rest) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if rest == "0" {
        return Some(Duration::ZERO);
    }
    if rest.is_empty() {
        return None;
    }

    let mut total: i128 = 0;
    while !rest.is_empty() {
        // The number: digits, and digits after a point; at least one digit in all.
        let whole_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        let (whole, after) = rest.split_at(whole_len);
        let (fraction, after) = match after.strip_prefix('.') {
            Some(after) => {
                let len = after.bytes().take_while(u8::is_ascii_digit).count();
                after.split_at(len)
            }
            None => ("", after),
        };
        if whole.is_empty() && fraction.is_empty() {
            return None;
        }

        // The unit: everything up to the next number.
        let unit_len = after
            .bytes()
            .take_while(|b| *b != b'.' && !b.is_ascii_digit())
            .count();
        let (unit, after) = after.split_at(unit_len);
        let unit: i128 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 3600 * 1_000_000_000,
            _ => return None,
        };

        let mut nanos = whole
            .bytes()
            .try_fold(0i128, |n, b| {
                n.checked_mul(10)?.checked_add((b - b'0') as i128)
            })?
            .checked_mul(unit)?;
        // The fraction, of which 18 digits are more than a nanosecond of any unit needs; what
        // is below a nanosecond is cut off.
        let fraction = &fraction[..fraction.len().min(18)];
        let digits = fraction
            .bytes()
            .fold(0i128, |n, b| n * 10 + (b - b'0') as i128);
        nanos += digits * unit / 10i128.pow(fraction.len() as u32);
        total = total.checked_add(nanos)?;
        if total > i64::MAX as i128 + negative as i128 {
            return None;
        }
        rest = after;
    }

    let total = if negative { -total } else { total };
    Some(Duration(i64::try_from(total).ok()?))
}

#[cfg(test)]
#[path = "config_test.rs"]
mod config_test;
