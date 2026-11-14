// Ported from LiveKit, cmd/server/commands.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. What the commands print goes to the writer they are
// given (standard output in `main`), where the original prints to standard output. Not
// here: `helpVerbose` (the flags generated from the configuration are not ported) and
// `listNodes` (the nodes of a cluster, in a table). In `createToken` the keys read from the
// key file are used; the original decodes them into a map that is nil there, which panics
// in its YAML library.

use std::io::Write;

use musivum::go::time;
use musivum::protocol::auth::{AccessToken, VideoGrant};
use musivum::protocol::utils::{API_KEY_PREFIX, new_guid, random_secret};

use super::{Error, Flags, get_config};

pub fn generate_keys(out: &mut impl Write) -> Result<(), Error> {
    let api_key = new_guid(API_KEY_PREFIX);
    let secret = random_secret();
    writeln!(out, "API Key:  {api_key}")?;
    writeln!(out, "API Secret:  {secret}")?;
    Ok(())
}

pub fn print_ports(c: &Flags, out: &mut impl Write) -> Result<(), Error> {
    let conf = get_config(c, musivum_server::logger::init_logger_from_config)?;
    let rtc = &conf.rtc.rtc_config;

    let mut udp_ports = Vec::new();
    let mut tcp_ports = Vec::new();

    tcp_ports.push(format!("{} - HTTP service", conf.port));
    if rtc.tcp_port != 0 {
        tcp_ports.push(format!("{} - ICE/TCP", rtc.tcp_port));
    }
    if rtc.udp_port.valid() {
        // `portStr, _ := conf.RTC.UDPPort.MarshalYAML()`, printed with `%s`: of a single port
        // MarshalYAML gives an int, which Go's `%s` prints as `%!s(int=7882)`.
        let port_str = if rtc.udp_port.end == 0 {
            format!("%!s(int={})", rtc.udp_port.start)
        } else if rtc.udp_port.end <= rtc.udp_port.start {
            "%!s(<nil>)".to_string()
        } else {
            format!("{}-{}", rtc.udp_port.start, rtc.udp_port.end)
        };
        udp_ports.push(format!("{port_str} - ICE/UDP"));
    } else {
        udp_ports.push(format!(
            "{}-{} - ICE/UDP range",
            rtc.ice_port_range_start, rtc.ice_port_range_end
        ));
    }

    if conf.turn.enabled {
        if conf.turn.tls_port > 0 {
            tcp_ports.push(format!("{} - TURN/TLS", conf.turn.tls_port));
        }
        if conf.turn.udp_port > 0 {
            udp_ports.push(format!("{} - TURN/UDP", conf.turn.udp_port));
        }
    }

    writeln!(out, "TCP Ports")?;
    for p in tcp_ports {
        writeln!(out, "{p}")?;
    }

    writeln!(out, "UDP Ports")?;
    for p in udp_ports {
        writeln!(out, "{p}")?;
    }
    Ok(())
}

// helpVerbose: not here (see the header).

#[allow(deprecated)]
pub fn create_token(
    c: &Flags,
    room: &str,
    identity: &str,
    recorder: bool,
    out: &mut impl Write,
) -> Result<(), Error> {
    let mut conf = get_config(c, musivum_server::logger::init_logger_from_config)?;

    // use the first API key from config
    if conf.keys.is_empty() {
        // try to load from file
        if let Err(err) = std::fs::metadata(&conf.key_file) {
            return Err(format!("stat {}: {err}", conf.key_file).into());
        }
        let f = std::fs::read_to_string(&conf.key_file)
            .map_err(|err| format!("open {}: {err}", conf.key_file))?;
        musivum::config::yaml::decode(&f, false, &mut conf.keys)?;

        if conf.keys.is_empty() {
            return Err("keys are not configured".into());
        }
    }

    let (api_key, api_secret) = conf
        .keys
        .iter()
        .next()
        .map(|(k, v)| (k.clone(), v.clone()))
        .unwrap_or_default();

    let mut grant = VideoGrant {
        room_join: true,
        room: room.to_string(),
        ..Default::default()
    };
    if recorder {
        grant.hidden = true;
        grant.recorder = true;
        grant.set_can_publish(false);
        grant.set_can_publish_data(false);
    }

    let at = AccessToken::new(&api_key, &api_secret)
        .add_grant(grant)
        .set_identity(identity)
        .set_valid_for(time::Duration(30 * 24 * time::HOUR.0));

    let token = at.to_jwt()?;

    writeln!(out, "Token: {token}")?;

    Ok(())
}

// listNodes: not here (see the header).

#[cfg(test)]
#[path = "commands_test.rs"]
mod commands_test;
