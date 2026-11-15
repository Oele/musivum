// musivum: the controlled agent's handling of nominations (see MUSIVUM.md), against the
// rules of pion's controlled selector: a nomination is answered, one on a pair not yet
// checked takes effect when the check succeeds, and a later nomination moves the selected
// pair (a full agent to a pair of higher priority, a lite one always). The case that broke
// a call: the client's first check comes before its candidates do, so the first nominated
// pair has a peer-reflexive remote; the client then nominates a pair of a candidate it
// trickled, and media has to follow it there.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Instant;
use stun::message::*;
use stun::textattrs::Username;

use super::*;
use crate::attributes::{
    control::AttrControlling, priority::PriorityAttr, use_candidate::UseCandidateAttr,
};
use crate::candidate::candidate_host::*;
use crate::candidate::candidate_relay::CandidateRelayConfig;
use crate::candidate::*;

const REMOTE_UFRAG: &str = "remoteufrag";
const REMOTE_PWD: &str = "remotepasswordremotepassword";

fn provider() -> std::sync::Arc<dyn crypto::RTCCryptoProvider> {
    crypto::default_provider().expect("a built-in crypto provider must be enabled for tests")
}

fn addr(s: &str) -> SocketAddr {
    SocketAddr::from_str(s).unwrap()
}

fn host(ip: &str, port: u16) -> Candidate {
    CandidateHostConfig {
        base_config: CandidateConfig {
            network: "udp".to_owned(),
            address: ip.to_owned(),
            port,
            component: 1,
            ..Default::default()
        },
        ..Default::default()
    }
    .new_candidate_host()
    .unwrap()
}

fn relay(ip: &str, port: u16) -> Candidate {
    CandidateRelayConfig {
        base_config: CandidateConfig {
            network: "udp".to_owned(),
            address: ip.to_owned(),
            port,
            component: 1,
            ..Default::default()
        },
        rel_addr: "0.0.0.0".to_owned(),
        rel_port: 0,
        url: None,
    }
    .new_candidate_relay()
    .unwrap()
}

/// A controlled agent with one local host candidate, checking.
fn controlled(lite: bool) -> Agent {
    let config = AgentConfig {
        lite,
        candidate_types: if lite {
            vec![CandidateType::Host]
        } else {
            vec![]
        },
        ..Default::default()
    };
    let mut a = Agent::new(Instant::now(), Arc::new(config), provider()).unwrap();
    a.add_local_candidate(host("192.168.0.2", 777)).unwrap();
    a.start_connectivity_checks(
        Instant::now(),
        false,
        REMOTE_UFRAG.to_owned(),
        REMOTE_PWD.to_owned(),
    )
    .unwrap();
    a.write_outs.clear();
    a
}

/// The controlling side's check, nominating or not.
fn check(a: &Agent, nominate: bool) -> Message {
    let provider = provider();
    let username = a.ufrag_pwd.local_credentials.ufrag.clone() + ":" + REMOTE_UFRAG;
    let mut attrs: Vec<Box<dyn Setter>> = vec![
        Box::new(BINDING_REQUEST),
        Box::new(TransactionId::new()),
        Box::new(Username::new(ATTR_USERNAME, username)),
    ];
    if nominate {
        attrs.push(Box::new(UseCandidateAttr::new()));
    }
    attrs.push(Box::new(AttrControlling(1)));
    attrs.push(Box::new(PriorityAttr(1_000)));
    attrs.push(Box::new(
        MessageIntegrity::new_short_term_integrity_with_provider(
            a.ufrag_pwd.local_credentials.pwd.clone(),
            provider.crypto(),
        ),
    ));
    attrs.push(Box::new(FINGERPRINT));
    let mut m = Message::new();
    m.build(&attrs).unwrap();
    m
}

/// What the agent sent since the last call: (to, message).
fn sent(a: &mut Agent) -> Vec<(SocketAddr, Message)> {
    let mut out = vec![];
    while let Some(t) = a.write_outs.pop_front() {
        let mut m = Message::new();
        m.raw = t.message.to_vec();
        m.decode().unwrap();
        out.push((t.transport.peer_addr, m));
    }
    out
}

fn of_class(sent: &[(SocketAddr, Message)], class: MessageClass) -> Vec<SocketAddr> {
    sent.iter()
        .filter(|(_, m)| m.typ.class == class)
        .map(|(to, _)| *to)
        .collect()
}

/// The controlling side nominates the pair of `from` with a check; when the agent checks
/// back, the check is answered from `from`. Gives what the agent sent.
fn nominate(a: &mut Agent, from: SocketAddr) -> Vec<(SocketAddr, Message)> {
    let mut m = check(a, true);
    a.handle_inbound(Instant::now(), &mut m, 0, from).unwrap();
    let out = sent(a);
    let provider = provider();
    for (to, request) in out.iter().filter(|(_, m)| m.typ.class == CLASS_REQUEST) {
        let mut answer = Message::new();
        answer
            .build(&[
                Box::new(BINDING_SUCCESS),
                Box::new(request.transaction_id),
                Box::new(MessageIntegrity::new_short_term_integrity_with_provider(
                    REMOTE_PWD.to_owned(),
                    provider.crypto(),
                )),
                Box::new(FINGERPRINT),
            ])
            .unwrap();
        a.handle_inbound(Instant::now(), &mut answer, 0, *to).unwrap();
    }
    out
}

fn selected_remote(a: &Agent) -> Option<SocketAddr> {
    a.get_selected_candidate_pair().map(|(_, remote)| remote.addr())
}

#[test]
fn a_nomination_moves_from_the_first_peer_reflexive_pair_to_a_trickled_one() {
    let mut a = controlled(false);

    // The client's first check, before any of its candidates: a peer-reflexive remote.
    let prflx = addr("203.0.113.10:13210");
    let out = nominate(&mut a, prflx);
    assert_eq!(
        vec![prflx],
        of_class(&out, CLASS_SUCCESS_RESPONSE),
        "the nomination is answered"
    );
    assert_eq!(
        vec![prflx],
        of_class(&out, CLASS_REQUEST),
        "and checked back"
    );
    assert_eq!(
        Some(prflx),
        selected_remote(&a),
        "selected once our check succeeded"
    );
    assert_eq!(
        CandidateType::PeerReflexive,
        a.get_selected_candidate_pair().unwrap().1.candidate_type()
    );

    // Its candidates arrive, and it nominates a pair of one of them, of higher priority.
    let later = addr("10.0.0.5:5000");
    a.add_remote_candidate(host("10.0.0.5", 5000)).unwrap();
    a.write_outs.clear();
    nominate(&mut a, later);
    assert_eq!(
        Some(later),
        selected_remote(&a),
        "media follows the client to the pair it nominated last"
    );
}

#[test]
fn a_full_agent_does_not_move_to_a_nominated_pair_of_lower_priority() {
    let mut a = controlled(false);
    let first = addr("10.0.0.5:5000");
    a.add_remote_candidate(host("10.0.0.5", 5000)).unwrap();
    a.write_outs.clear();
    nominate(&mut a, first);
    assert_eq!(Some(first), selected_remote(&a));

    let lower = addr("198.51.100.20:3478");
    a.add_remote_candidate(relay("198.51.100.20", 3478)).unwrap();
    a.write_outs.clear();
    nominate(&mut a, lower);
    assert_eq!(
        Some(first),
        selected_remote(&a),
        "pion keeps a pair of higher priority"
    );
}

#[test]
fn a_lite_agent_follows_every_nomination_without_checks_of_its_own() {
    let mut a = controlled(true);

    let prflx = addr("203.0.113.10:13210");
    let out = nominate(&mut a, prflx);
    assert_eq!(vec![prflx], of_class(&out, CLASS_SUCCESS_RESPONSE));
    assert!(
        of_class(&out, CLASS_REQUEST).is_empty(),
        "a lite agent sends no checks"
    );
    assert_eq!(Some(prflx), selected_remote(&a), "selected at once");

    let lower = addr("198.51.100.20:3478");
    a.add_remote_candidate(relay("198.51.100.20", 3478)).unwrap();
    a.write_outs.clear();
    let out = nominate(&mut a, lower);
    assert!(of_class(&out, CLASS_REQUEST).is_empty());
    assert_eq!(
        Some(lower),
        selected_remote(&a),
        "a lite agent follows the last nomination"
    );
}

#[test]
fn a_check_without_nomination_leaves_the_selected_pair() {
    let mut a = controlled(false);
    let first = addr("10.0.0.5:5000");
    a.add_remote_candidate(host("10.0.0.5", 5000)).unwrap();
    a.write_outs.clear();
    nominate(&mut a, first);

    let other = addr("10.0.0.6:5000");
    a.add_remote_candidate(host("10.0.0.6", 5000)).unwrap();
    a.write_outs.clear();
    let mut m = check(&a, false);
    a.handle_inbound(Instant::now(), &mut m, 0, other).unwrap();
    assert_eq!(
        vec![other],
        of_class(&sent(&mut a), CLASS_SUCCESS_RESPONSE)
    );
    assert_eq!(Some(first), selected_remote(&a));
}

// musivum: RFC 8838 Section 11.4 (pion's replaceRedundantPeerReflexiveCandidates): a
// candidate signalled at the address of a peer-reflexive one takes its place, keeping the
// pair, its priority and the selection.
#[test]
fn a_signalled_candidate_takes_the_place_of_a_peer_reflexive_one() {
    let mut a = controlled(false);
    let at = addr("203.0.113.10:13210");
    nominate(&mut a, at);
    assert_eq!(Some(at), selected_remote(&a));
    let (pairs, priority) = (
        a.candidate_pairs.len(),
        a.candidate_pairs[a.get_selected_pair().unwrap()].priority(),
    );

    a.add_remote_candidate(relay("203.0.113.10", 13210)).unwrap();
    assert_eq!(1, a.remote_candidates.len(), "one candidate at the address");
    assert_eq!(pairs, a.candidate_pairs.len(), "no pair more");
    let (_, remote) = a.get_selected_candidate_pair().unwrap();
    assert_eq!(CandidateType::Relay, remote.candidate_type(), "the signalled type");
    assert_eq!(at, remote.addr());
    assert_eq!(
        priority,
        a.candidate_pairs[a.get_selected_pair().unwrap()].priority(),
        "the pair keeps its priority"
    );
    assert!(remote.last_received().is_some(), "and what was seen of it");

    // A check from there finds the same pair.
    a.write_outs.clear();
    let mut m = check(&a, true);
    a.handle_inbound(Instant::now(), &mut m, 0, at).unwrap();
    assert_eq!(Some(at), selected_remote(&a));
    assert_eq!(pairs, a.candidate_pairs.len());
}

