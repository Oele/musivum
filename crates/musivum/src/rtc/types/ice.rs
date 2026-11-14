// Ported from LiveKit, pkg/rtc/types/ice.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the candidates are those of the WebRTC library musivum
// stands on: pion's `webrtc.ICECandidate` is the connection layer's `IceCandidate`, and its
// `ice.Candidate`, `ice.UnmarshalCandidate`, `webrtc.ICECandidatePair` and the types of a
// candidate's fields are `rtc`'s, named from that crate here because the layer does not
// export them yet. A candidate line is parsed by `rtc`, which is stricter than pion in some
// places and laxer in others (the test file lists them). `ICEConnectionType` is an enum, its
// order the original's, without `ReporterType()`: musivum does not have the reporting of
// `observability/roomobs`. The candidates of the lists are values, not pointers:
// `isCandidateEqualTo` has no nil to look at, and `GetInfo` copies them. A candidate that
// may be nil where the original looks ("end-of-candidates") is an `Option`. The errors of
// the unexported functions, which reach the log only, are an enum of this file. The tests
// are new: the original has none.

use std::fmt;

use parking_lot::Mutex;
use rtc::ice::candidate as ice;
use rtc::peer_connection::transport::{
    RTCIceCandidatePair as IceCandidatePair, RTCIceCandidateType as IceCandidateType,
    RTCIceProtocol as IceProtocol,
};

use crate::errorw;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::webrtc::peerconnection::{IceCandidate, IceCandidateInit};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IceConnectionType {
    // this is in ICE priority highest -> lowest ordering
    // WARNING: Keep this ordering as it is used to find lowest priority connection type.
    #[default]
    Unknown,
    Udp,
    Tcp,
    Turn,
}

impl fmt::Display for IceConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            IceConnectionType::Unknown => "unknown",
            IceConnectionType::Udp => "udp",
            IceConnectionType::Tcp => "tcp",
            IceConnectionType::Turn => "turn",
        })
    }
}

// --------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IceCandidateExtended {
    pub candidate: IceCandidate,
    pub selected_order: i64,
    pub filtered: bool,
    pub trickle: bool,
}

// --------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IceConnectionInfo {
    pub local: Vec<IceCandidateExtended>,
    pub remote: Vec<IceCandidateExtended>,
    pub transport: livekit::SignalTarget,
    pub r#type: IceConnectionType,
}

impl IceConnectionInfo {
    pub fn has_candidates(&self) -> bool {
        !self.local.is_empty() || !self.remote.is_empty()
    }
}

pub fn ice_connection_infos_type(infos: &[IceConnectionInfo]) -> IceConnectionType {
    for info in infos {
        if info.r#type != IceConnectionType::Unknown {
            return info.r#type;
        }
    }
    IceConnectionType::Unknown
}

// --------------------------------------------

pub struct IceConnectionDetails {
    logger: Logger,

    // what `lock sync.Mutex` guards upstream
    inner: Mutex<IceConnectionDetailsInner>,
}

struct IceConnectionDetailsInner {
    ice_connection_info: IceConnectionInfo,
    selected_count: i64,
}

impl IceConnectionDetails {
    pub fn new(transport: livekit::SignalTarget, l: Logger) -> Self {
        Self {
            inner: Mutex::new(IceConnectionDetailsInner {
                ice_connection_info: IceConnectionInfo {
                    transport,
                    r#type: IceConnectionType::Unknown,
                    ..Default::default()
                },
                selected_count: 0,
            }),
            logger: l,
        }
    }

    pub fn get_info(&self) -> IceConnectionInfo {
        let d = self.inner.lock();
        let d = &d.ice_connection_info;
        let mut info = IceConnectionInfo {
            transport: d.transport,
            r#type: d.r#type,
            local: Vec::with_capacity(d.local.len()),
            remote: Vec::with_capacity(d.remote.len()),
        };
        for c in &d.local {
            info.local.push(IceCandidateExtended {
                candidate: c.candidate.clone(),
                filtered: c.filtered,
                selected_order: c.selected_order,
                trickle: c.trickle,
            });
        }
        for c in &d.remote {
            info.remote.push(IceCandidateExtended {
                candidate: c.candidate.clone(),
                filtered: c.filtered,
                selected_order: c.selected_order,
                trickle: c.trickle,
            });
        }
        info
    }

    pub fn get_connection_type(&self) -> IceConnectionType {
        let d = self.inner.lock();

        d.ice_connection_info.r#type
    }

    pub fn add_local_candidate(&self, c: &IceCandidate, filtered: bool, trickle: bool) {
        let mut d = self.inner.lock();
        let d = &mut d.ice_connection_info;
        let comp_fn = |e: &IceCandidateExtended| is_candidate_equal_to(&e.candidate, c);
        if d.local.iter().any(comp_fn) {
            return;
        }
        d.local.push(IceCandidateExtended {
            candidate: c.clone(),
            filtered,
            trickle,
            ..Default::default()
        });
    }

    pub fn add_local_ice_candidate(&self, c: &ice::Candidate, filtered: bool, trickle: bool) {
        let candidate = match unmarshal_candidate(c) {
            Ok(candidate) => candidate,
            Err(err) => {
                errorw!(self.logger, "could not unmarshal ice candidate", err, "candidate" => c);
                return;
            }
        };

        self.add_local_candidate(&candidate, filtered, trickle);
    }

    pub fn add_remote_candidate(
        &self,
        c: &IceCandidateInit,
        filtered: bool,
        trickle: bool,
        can_update: bool,
    ) {
        let ice_candidate = match unmarshal_ice_candidate(c) {
            Ok(ice_candidate) => ice_candidate,
            Err(err) => {
                errorw!(self.logger, "could not unmarshal candidate", err, "candidate" => c);
                return;
            }
        };
        self.add_remote_ice_candidate(ice_candidate.as_ref(), filtered, trickle, can_update);
    }

    pub fn add_remote_ice_candidate(
        &self,
        ice_candidate: Option<&ice::Candidate>,
        filtered: bool,
        trickle: bool,
        can_update: bool,
    ) {
        let Some(ice_candidate) = ice_candidate else {
            // end-of-candidates candidate
            return;
        };

        let candidate = match unmarshal_candidate(ice_candidate) {
            Ok(candidate) => candidate,
            Err(err) => {
                errorw!(self.logger, "could not unmarshal ice candidate", err, "candidate" => ice_candidate);
                return;
            }
        };

        let mut d = self.inner.lock();
        let index_fn = |e: &IceCandidateExtended| is_candidate_equal_to(&e.candidate, &candidate);
        if let Some(idx) = d.ice_connection_info.remote.iter().position(index_fn) {
            if can_update {
                d.ice_connection_info.remote[idx].filtered = filtered;
                d.ice_connection_info.remote[idx].trickle = trickle;
            }
            return;
        }
        d.ice_connection_info.remote.push(IceCandidateExtended {
            candidate,
            filtered,
            trickle,
            ..Default::default()
        });
        d.update_connection_type_locked();
    }

    pub fn clear(&self) {
        let mut d = self.inner.lock();
        d.ice_connection_info.local = Vec::new();
        d.ice_connection_info.remote = Vec::new();
        d.ice_connection_info.r#type = IceConnectionType::Unknown;
    }

    pub fn set_selected_pair(&self, pair: &IceCandidatePair) {
        let mut d = self.inner.lock();
        let d = &mut *d;

        d.selected_count += 1;

        let remote_idx = d
            .ice_connection_info
            .remote
            .iter()
            .position(|e| is_candidate_equal_to(&e.candidate, pair.remote()));
        let remote_idx = match remote_idx {
            Some(remote_idx) => remote_idx,
            None => {
                // it's possible for prflx candidates to be generated by Pion, we'll add them
                d.ice_connection_info.remote.push(IceCandidateExtended {
                    candidate: pair.remote().clone(),
                    filtered: false,
                    trickle: false,
                    ..Default::default()
                });
                d.ice_connection_info.remote.len() - 1
            }
        };
        d.ice_connection_info.remote[remote_idx].selected_order = d.selected_count;
        d.update_connection_type_locked();

        let local_idx = d
            .ice_connection_info
            .local
            .iter()
            .position(|e| is_candidate_equal_to(&e.candidate, pair.local()));
        let Some(local_idx) = local_idx else {
            errorw!(self.logger, "could not match local candidate", nil, "local" => pair.local());
            // should not happen
            return;
        };
        d.ice_connection_info.local[local_idx].selected_order = d.selected_count;
    }
}

impl IceConnectionDetailsInner {
    fn update_connection_type_locked(&mut self) {
        let d = &mut self.ice_connection_info;
        let mut highest_selected_order = -1;
        let mut selected_remote_candidate: Option<&IceCandidateExtended> = None;
        for remote in &d.remote {
            if remote.selected_order == 0 {
                continue;
            }

            if remote.selected_order > highest_selected_order {
                highest_selected_order = remote.selected_order;
                selected_remote_candidate = Some(remote);
            }
        }

        let Some(selected_remote_candidate) = selected_remote_candidate else {
            return;
        };

        let remote_candidate = &selected_remote_candidate.candidate;
        match remote_candidate.protocol {
            IceProtocol::Udp => d.r#type = IceConnectionType::Udp,

            IceProtocol::Tcp => d.r#type = IceConnectionType::Tcp,

            _ => {}
        }

        match remote_candidate.typ {
            IceCandidateType::Relay => d.r#type = IceConnectionType::Turn,

            IceCandidateType::Prflx => {
                // if the remote relay candidate pings us *before* we get a relay candidate,
                // Pion would have created a prflx candidate with the same address as the relay candidate.
                // to report an accurate connection type, we'll compare to see if existing relay candidates match
                for other in &d.remote {
                    let or = &other.candidate;
                    if or.typ == IceCandidateType::Relay &&
                        remote_candidate.address == or.address &&
                        // NOTE: port is not compared as relayed address  reported by TURN ALLOCATE from
                        // pion/turn server -> client and later sent from client -> server via ICE Trickle does not
                        // match port of `prflx` candidate learnt via TURN path. TODO-INVESTIGATE: how and why doesn't
                        // port match?
                        // remoteCandidate.Port == or.Port &&
                        remote_candidate.protocol == or.protocol
                    {
                        d.r#type = IceConnectionType::Turn;
                        break;
                    }
                }
            }

            _ => {}
        }
    }
}

// -------------------------------------------------------------

// What the unexported functions below return. None of it leaves this file other than as a
// line of the log.
#[derive(Debug, thiserror::Error)]
enum Error {
    // `ice.UnmarshalCandidate`
    #[error("{0}")]
    Ice(#[from] rtc::shared::error::Error),
    #[error("unknown ice candidate type: {0}")]
    UnknownIceCandidateType(ice::CandidateType),
    // `webrtc.NewICEProtocol`
    #[error("unknown protocol: {0}")]
    IceProtocolUnknown(String),
}

fn is_candidate_equal_to(c1: &IceCandidate, c2: &IceCandidate) -> bool {
    c1.typ == c2.typ
        && c1.protocol == c2.protocol
        && c1.address == c2.address
        && c1.port == c2.port
        && c1.foundation == c2.foundation
        && c1.priority == c2.priority
        && c1.related_address == c2.related_address
        && c1.related_port == c2.related_port
        && c1.tcp_type == c2.tcp_type
}

fn unmarshal_ice_candidate(c: &IceCandidateInit) -> Result<Option<ice::Candidate>, Error> {
    let candidate_value = c
        .candidate
        .strip_prefix("candidate:")
        .unwrap_or(&c.candidate);
    if candidate_value.is_empty() {
        return Ok(None);
    }

    let candidate = ice::unmarshal_candidate(candidate_value)?;

    Ok(Some(candidate))
}

fn unmarshal_candidate(i: &ice::Candidate) -> Result<IceCandidate, Error> {
    let typ = convert_type_from_ice(i.candidate_type())?;

    let network_short = i.network_type().network_short();
    let protocol = match IceProtocol::from(network_short.as_str()) {
        IceProtocol::Unspecified => return Err(Error::IceProtocolUnknown(network_short)),
        protocol => protocol,
    };

    let mut c = IceCandidate {
        foundation: i.foundation(),
        priority: i.priority(),
        address: i.address().to_string(),
        protocol,
        port: i.port(),
        component: i.component(),
        typ,
        tcp_type: i.tcp_type().into(),
        ..Default::default()
    };

    if let Some(related_address) = i.related_address() {
        c.related_address = related_address.address;
        c.related_port = related_address.port;
    }

    Ok(c)
}

fn convert_type_from_ice(t: ice::CandidateType) -> Result<IceCandidateType, Error> {
    match t {
        ice::CandidateType::Host => Ok(IceCandidateType::Host),
        ice::CandidateType::ServerReflexive => Ok(IceCandidateType::Srflx),
        ice::CandidateType::PeerReflexive => Ok(IceCandidateType::Prflx),
        ice::CandidateType::Relay => Ok(IceCandidateType::Relay),
        _ => Err(Error::UnknownIceCandidateType(t)),
    }
}

pub fn is_candidate_mdns(candidate: &IceCandidateInit) -> bool {
    let Ok(c) = unmarshal_ice_candidate(candidate) else {
        return false;
    };

    is_ice_candidate_mdns(c.as_ref())
}

pub fn is_ice_candidate_mdns(candidate: Option<&ice::Candidate>) -> bool {
    let Some(candidate) = candidate else {
        // end-of-candidates candidate
        return false;
    };

    candidate.address().ends_with(".local") || candidate.address().ends_with(".invalid")
}

#[cfg(test)]
#[path = "ice_test.rs"]
mod ice_test;
