// Ported from LiveKit, pkg/service/turn.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.
// - The TURN server is pion/turn's as ported (`webrtc::turn`); `NewTurnServer` gives `Ok(None)`
//   where it gives nil, nil (TURN not enabled). Its errors are `Error`s of this package with the
//   original's texts.
// - The TLS listener is `webrtc::turn::net`'s (rustls with ring, as the HTTP client of
//   webhooks has it), the certificate and key read from PEM files as `tls.LoadX509KeyPair`
//   reads them; TLS 1.2 and 1.3, as `MinVersion: tls.VersionTLS12` allows. The PROXY protocol
//   listener is `proxyproto.rs`, written after the specification with go-proxyproto's
//   behaviour; `proxyProtocolPolicy` is its connection policy, an untrusted proxy being an
//   `InvalidUpstream` error (go-proxyproto's `ErrInvalidUpstream`).
// - pion's logger factory (`pionlogger`) is the server's logger.
// - `getTURNAuthHandlerFunc` gives the handler as pion's `AuthHandler` (an `Arc` of a function).
// - base62 is jxskiss/base62 as ported (`protocol/utils/base62.rs`).

use std::net::IpAddr;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::Error;
use super::proxyproto::{self, ConnPolicyFunc, ConnPolicyOptions, Policy, PolicyError};
use super::turnquota::new_turn_allocation_quota;
use super::turnrelay::{
    get_turn_local_ips, get_turn_media_ports, is_restricted_peer_ip,
    new_reject_tcp_relay_address_generator, new_turn_relay_policy,
};
use super::utils::is_valid_domain;
use crate::config::{self, Config, TurnConfig};
use crate::go::time::{self, Duration, Time};
use crate::protocol::auth::KeyProvider;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::protocol::utils::base62;
use crate::telemetry::{self, prometheus};
use crate::webrtc::stun::message::METHOD_ALLOCATE;
use crate::webrtc::turn::net::{self, Addr, IpNet, Listener, PacketConn, is_ipv4};
use crate::webrtc::turn::{
    self, AuthHandler, ListenerConfig, PacketConnConfig, RelayAddressGenerator,
    RelayAddressGeneratorPortRange, RequestAttributes, ServerConfig,
};
use crate::{infow, warnw};

pub const LIVEKIT_REALM: &str = "livekit";

const ALLOCATE_RETRIES: i64 = 50;

// parsePeerCIDRs compiles a list of CIDR strings, failing with a field-specific
// error on any invalid entry so a malformed peer policy is never silently ignored.
pub(crate) fn parse_peer_cidrs(field: &str, cidrs: &[String]) -> Result<Vec<IpNet>, Error> {
    let mut parsed = Vec::with_capacity(cidrs.len());
    for cidr in cidrs {
        let ipnet = IpNet::parse_cidr(cidr).map_err(|err| {
            Error::InvalidCidrIn(cidr.clone(), field.to_string(), err.to_string())
        })?;
        parsed.push(ipnet);
    }
    Ok(parsed)
}

/// Go's `net.JoinHostPort(host, strconv.Itoa(port))`.
fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

pub fn new_turn_server(
    conf: &Config,
    auth_handler: Option<AuthHandler>,
    standalone: bool,
) -> Result<Option<Arc<turn::Server>>, Error> {
    let turn_conf = &conf.turn;
    if !turn_conf.enabled {
        return Ok(None);
    }

    if turn_conf.tls_port <= 0 && turn_conf.udp_port <= 0 {
        return Err(Error::InvalidTurnPorts);
    } else if turn_conf.tls_port > 0 {
        if turn_conf.domain.is_empty() {
            return Err(Error::TurnDomainRequired);
        }

        if !is_valid_domain(&turn_conf.domain) {
            return Err(Error::TurnDomainIsNotCorrect);
        }
    }

    // parse peer CIDR policies once at startup so a malformed entry fails loudly
    // instead of being silently skipped on every permission decision (fail-open)
    let allow_restricted_peer_cidrs = Arc::new(parse_peer_cidrs(
        "turn.allow_restricted_peer_cidrs",
        &turn_conf.allow_restricted_peer_cidrs,
    )?);
    let deny_peer_cidrs = Arc::new(parse_peer_cidrs(
        "turn.deny_peer_cidrs",
        &turn_conf.deny_peer_cidrs,
    )?);

    let mut server_config = ServerConfig {
        realm: LIVEKIT_REALM.to_string(),
        auth_handler,
        logger_factory: Some(logger::get_logger()),
        ..Default::default()
    };

    // cap concurrent relay allocations per participant so one credential cannot
    // exhaust the shared relay-port range (a value <= 0 disables the quota)
    if turn_conf.per_user_relay_allocation_limit > 0 {
        let quota = new_turn_allocation_quota(turn_conf.per_user_relay_allocation_limit as usize);
        server_config.quota_handler = Some(quota.quota_handler());
        server_config.event_handler = quota.event_handler();
    }

    let mut log_values: Vec<(&str, String)> = vec![
        (
            "turn.relay_range_start",
            turn_conf.relay_port_range_start.to_string(),
        ),
        (
            "turn.relay_range_end",
            turn_conf.relay_port_range_end.to_string(),
        ),
        (
            "turn.per_user_relay_allocation_limit",
            turn_conf.per_user_relay_allocation_limit.to_string(),
        ),
    ];

    // embedded TURN shares the host with the SFU, limit relays to this host to its media ports
    let mut local_ips = Default::default();
    let mut media_ports = Vec::new();
    if !standalone {
        local_ips = get_turn_local_ips(&conf.rtc)?;
        media_ports = get_turn_media_ports(&conf.rtc);
    }

    for addr in &turn_conf.bind_addresses {
        let node_ip = if addr.parse::<IpAddr>().is_ok_and(is_ipv4) {
            &conf.rtc.rtc_config.node_ip.v4
        } else {
            &conf.rtc.rtc_config.node_ip.v6
        };
        if node_ip.is_empty() {
            return Err(Error::NoMatchingNodeIpForRelay);
        }

        let mut relay_addr_gen: Arc<dyn RelayAddressGenerator> =
            Arc::new(RelayAddressGeneratorPortRange {
                relay_address: node_ip.parse().ok(),
                address: addr.clone(),
                min_port: turn_conf.relay_port_range_start,
                max_port: turn_conf.relay_port_range_end,
                max_retries: ALLOCATE_RETRIES,
            });

        let permission_handler: turn::PermissionHandler = {
            let (allow_restricted_peer_cidrs, deny_peer_cidrs) =
                (allow_restricted_peer_cidrs.clone(), deny_peer_cidrs.clone());
            Arc::new(move |_client_addr: Addr, peer_ip: IpAddr| {
                // restricted peer IP is denied by default, unless allowed by the allow list,
                if is_restricted_peer_ip(peer_ip) {
                    let mut allowed = false;
                    for ipnet in allow_restricted_peer_cidrs.iter() {
                        if ipnet.contains(peer_ip) {
                            allowed = true;
                            break;
                        }
                    }
                    if !allowed {
                        return false;
                    }

                    // if allowed, check deny list for overrides
                }

                for ipnet in deny_peer_cidrs.iter() {
                    if ipnet.contains(peer_ip) {
                        return false;
                    }
                }

                true
            })
        };

        relay_addr_gen = Arc::new(new_reject_tcp_relay_address_generator(relay_addr_gen));
        if standalone {
            relay_addr_gen = Arc::new(telemetry::new_relay_address_generator(relay_addr_gen));
        } else {
            relay_addr_gen = Arc::new(new_turn_relay_policy(
                Some(relay_addr_gen),
                local_ips.clone(),
                media_ports.clone(),
            ));
        }

        if turn_conf.tls_port > 0 {
            let mut listener =
                new_turn_tcp_listener(turn_conf, &join_host_port(addr, turn_conf.tls_port))?;
            if standalone {
                listener = Box::new(telemetry::new_listener(listener));
            }

            let listener_config = ListenerConfig {
                listener: Some(Arc::from(listener)),
                relay_address_generator: Some(relay_addr_gen.clone()),
                permission_handler: Some(permission_handler.clone()),
            };
            server_config.listener_configs.push(listener_config);

            log_values.push(("turn.portTLS", turn_conf.tls_port.to_string()));
            log_values.push(("turn.externalTLS", turn_conf.external_tls.to_string()));
            log_values.push(("turn.proxyProtocol", turn_conf.proxy_protocol.to_string()));
        }

        if turn_conf.udp_port > 0 {
            let mut udp_listener: Arc<dyn PacketConn> =
                net::listen_packet("udp", &join_host_port(addr, turn_conf.udp_port))
                    .map_err(|err| Error::TurnUdpListen(err.to_string()))?;

            if standalone {
                udp_listener = Arc::new(telemetry::new_packet_conn(
                    udp_listener,
                    prometheus::Direction::Incoming,
                ));
            }

            let packet_config = PacketConnConfig {
                packet_conn: Some(udp_listener),
                relay_address_generator: Some(relay_addr_gen.clone()),
                permission_handler: Some(permission_handler.clone()),
            };
            server_config.packet_conn_configs.push(packet_config);
            log_values.push(("turn.portUDP", turn_conf.udp_port.to_string()));
        }
    }

    let kvs: Vec<(&str, &dyn std::fmt::Debug)> = log_values
        .iter()
        .map(|(k, v)| (*k, v as &dyn std::fmt::Debug))
        .collect();
    logger::get_logger().infow("Starting TURN server", &kvs);
    turn::Server::new(server_config)
        .map(Some)
        .map_err(|err| Error::Turn(err.to_string()))
}

// newTURNTCPListener returns the TCP listener for TURN/TLS. The PROXY protocol
// header, when enabled, is read before TLS so the client address is known to
// the TLS layer and to TURN regardless of who terminates TLS.
pub(crate) fn new_turn_tcp_listener(
    turn_conf: &TurnConfig,
    address: &str,
) -> Result<Box<dyn Listener>, Error> {
    let mut tls_config = None;
    if !turn_conf.external_tls {
        tls_config = Some(
            load_tls_config(&turn_conf.cert_file, &turn_conf.key_file)
                .map_err(|err| Error::TurnTlsCertRequired(err.to_string()))?,
        );
    }

    let mut proxy_policy = None;
    if turn_conf.proxy_protocol {
        let trusted = parse_peer_cidrs(
            "turn.proxy_protocol_trusted_cidrs",
            &turn_conf.proxy_protocol_trusted_cidrs,
        )?;
        if trusted.is_empty() {
            return Err(Error::TurnProxyProtocolRequiresTrustedCidrs);
        }
        proxy_policy = Some(proxy_protocol_policy(trusted));
    }

    let mut listener: Box<dyn Listener> =
        Box::new(net::listen("tcp", address).map_err(|err| Error::TurnTcpListen(err.to_string()))?);
    if let Some(proxy_policy) = proxy_policy {
        listener = Box::new(proxyproto::Listener {
            listener,
            conn_policy: Some(proxy_policy),
            read_header_timeout: std::time::Duration::ZERO,
        });
    }
    if let Some(tls_config) = tls_config {
        listener = Box::new(net::new_tls_listener(listener, Arc::new(tls_config)));
    }
    Ok(listener)
}

// tls.LoadX509KeyPair and the configuration made of it.
fn load_tls_config(
    cert_file: &str,
    key_file: &str,
) -> Result<rustls::ServerConfig, Box<dyn std::error::Error>> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certs = CertificateDer::pem_file_iter(cert_file)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(key_file)?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
    Ok(config)
}

// proxyProtocolPolicy requires the PROXY header from trusted proxies and closes
// every other connection, so the header cannot be forged by a direct client.
pub(crate) fn proxy_protocol_policy(trusted: Vec<IpNet>) -> ConnPolicyFunc {
    Arc::new(move |opts: &ConnPolicyOptions| {
        let Addr::Tcp(tcp_addr) = opts.upstream else {
            return Err(PolicyError::InvalidUpstream(format!(
                "unexpected address {}",
                opts.upstream
            )));
        };
        for ipnet in &trusted {
            if ipnet.contains(tcp_addr.ip()) {
                return Ok(Policy::Require);
            }
        }
        // wrapping ErrInvalidUpstream closes this connection and keeps the listener accepting
        Err(PolicyError::InvalidUpstream(format!(
            "{} is not a trusted proxy",
            tcp_addr.ip()
        )))
    })
}

pub fn get_turn_auth_handler_func(handler: &Arc<TurnAuthHandler>) -> AuthHandler {
    let handler = handler.clone();
    Arc::new(move |ra: &RequestAttributes| handler.handle_auth(ra))
}

pub struct TurnAuthHandler {
    key_provider: Arc<dyn KeyProvider>,
}

pub fn new_turn_auth_handler(key_provider: Arc<dyn KeyProvider>) -> Arc<TurnAuthHandler> {
    Arc::new(TurnAuthHandler { key_provider })
}

// strconv.ParseInt(s, 10, 64), as its error says
fn parse_int(s: &str) -> Result<i64, Error> {
    let valid = {
        let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
    };
    match s.parse::<i64>() {
        Ok(v) if valid => Ok(v),
        _ if valid => Err(Error::Turn(format!(
            "strconv.ParseInt: parsing {s:?}: value out of range"
        ))),
        _ => Err(Error::Turn(format!(
            "strconv.ParseInt: parsing {s:?}: invalid syntax"
        ))),
    }
}

impl TurnAuthHandler {
    pub fn create_username(
        &self,
        api_key: &str,
        p_id: &livekit::ParticipantId,
        ttl_seconds: i64,
    ) -> (String, i64) {
        // clamp defensively: non-positive TTLs fall back to the default and overflowing ones are capped
        let (ttl_seconds, _) = config::clamp_turn_ttl_seconds(ttl_seconds);
        let expiry = (time::now() + Duration::from_secs(ttl_seconds)).unix_secs();
        (
            base62::encode_to_string(format!("{api_key}|{}|{expiry}", p_id.as_str()).as_bytes()),
            expiry,
        )
    }

    pub fn parse_username(
        &self,
        username: &str,
    ) -> Result<(String, livekit::ParticipantId, i64), Error> {
        let decoded =
            base62::decode_string(username).map_err(|err| Error::Turn(err.to_string()))?;
        let decoded = String::from_utf8_lossy(&decoded);
        let parts: Vec<&str> = decoded.split('|').collect();
        if parts.len() != 3 {
            return Err(Error::InvalidUsername);
        }
        let expiry = parse_int(parts[2])?;
        if expiry == 0 {
            return Err(Error::Expired);
        }

        Ok((
            parts[0].to_string(),
            livekit::ParticipantId::new(parts[1]),
            expiry,
        ))
    }

    pub fn create_password(
        &self,
        api_key: &str,
        p_id: &livekit::ParticipantId,
        expiry: i64,
    ) -> Result<String, Error> {
        if expiry == 0 || time::now().after(Time::unix(expiry, 0)) {
            return Err(Error::Expired);
        }
        self.compute_password(api_key, p_id, expiry)
    }

    pub(crate) fn compute_password(
        &self,
        api_key: &str,
        p_id: &livekit::ParticipantId,
        expiry: i64,
    ) -> Result<String, Error> {
        let secret = self.key_provider.get_secret(api_key);
        if secret.is_empty() {
            return Err(Error::InvalidApiKey);
        }

        let key_input = format!("{secret}|{}|{expiry}", p_id.as_str());

        let sum = Sha256::digest(key_input.as_bytes());
        Ok(base62::encode_to_string(&sum))
    }

    pub fn handle_auth(&self, ra: &RequestAttributes) -> (String, Vec<u8>, bool) {
        let username = &ra.username;
        let Ok(decoded) = base62::decode_string(username) else {
            return (String::new(), Vec::new(), false);
        };
        let decoded = String::from_utf8_lossy(&decoded).into_owned();
        let parts: Vec<&str> = decoded.split('|').collect();
        if parts.len() != 3 {
            return (String::new(), Vec::new(), false);
        }
        let Ok(expiry) = parse_int(parts[2]) else {
            return (String::new(), Vec::new(), false);
        };
        if expiry == 0 {
            return (String::new(), Vec::new(), false);
        }
        let expiry_time = Time::unix(expiry, 0);
        if time::now().after(expiry_time) {
            // TTL only applies to initial allocation. Refresh / CreatePermission /
            // ChannelBind / Send / Data requests are still authenticated against the
            // username/password but skip the TTL check so long-running sessions can
            // keep refreshing past the credential expiry.
            if ra.method == METHOD_ALLOCATE {
                infow!(
                    logger::get_logger(),
                    "TURN credential expired",
                    "username" => decoded,
                    "participantID" => parts[1],
                    "expiry" => expiry_time,
                    "method" => ra.method.to_string(),
                );
                return (String::new(), Vec::new(), false);
            }
        }
        let password = match self.compute_password(
            parts[0],
            &livekit::ParticipantId::new(parts[1]),
            expiry,
        ) {
            Ok(p) => p,
            Err(err) => {
                warnw!(logger::get_logger(), "could not create TURN password", err, "username" => decoded);
                return (String::new(), Vec::new(), false);
            }
        };
        (
            parts[1].to_string(),
            turn::generate_auth_key(username, LIVEKIT_REALM, &password),
            true,
        )
    }
}

#[cfg(test)]
#[path = "turn_test.rs"]
pub(crate) mod turn_test;
