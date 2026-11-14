// Ported from LiveKit's protocol, utils/guid/id.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The random numbers an id is made of come from the
// crate `rand`, from the generator of the calling thread, where the original keeps a pool of
// generators of Go's `math/rand/v2.ChaCha8` seeded from `crypto/rand`: an id has to be
// random, it does not have to be the one Go would have made. `Hash`, which seeds that
// generator with a hash of its data, is left out for the same reason: its ids are the
// original's only with Go's generator, and nothing that is ported calls it
// (pkg/service/requestid.go does). `livekit.Guid` and `livekit.GuidBlock` are declared here
// (the trait `Guid` also says which prefix an identifier type has, which `guidPrefix` finds
// out from the type upstream). The pattern of `IsValidID` is matched by hand instead of with
// a regular expression. `LocalNodeID` is left out: it names a node of a cluster after the
// host.

use rand::Rng;
use sha1::{Digest, Sha1};

use crate::protocol::livekit;
use crate::protocol::utils::base62;

pub const SIZE: usize = 12;

pub const ROOM_PREFIX: &str = "RM_";
pub const NODE_PREFIX: &str = "ND_";
pub const PARTICIPANT_PREFIX: &str = "PA_";
pub const TRACK_PREFIX: &str = "TR_";
pub const DATA_TRACK_PREFIX: &str = "DTR_";
pub const API_KEY_PREFIX: &str = "API";
pub const EGRESS_PREFIX: &str = "EG_";
pub const INGRESS_PREFIX: &str = "IN_";
pub const SIP_TRUNK_PREFIX: &str = "ST_";
pub const SIP_DISPATCH_RULE_PREFIX: &str = "SDR_";
pub const SIP_CALL_PREFIX: &str = "SCL_";
pub const SIP_TRANSFER_PREFIX: &str = "STR_";
pub const RPC_PREFIX: &str = "RPC_";
pub const WHIP_RESOURCE_PREFIX: &str = "WH_";
pub const RTMP_RESOURCE_PREFIX: &str = "RT_";
pub const URL_RESOURCE_PREFIX: &str = "UR_";
pub const SIP_HOSTNAME_PREFIX: &str = "SH";
pub const AGENT_PREFIX: &str = "A_";
pub const AGENT_WORKER_PREFIX: &str = "AW_";
pub const AGENT_JOB_PREFIX: &str = "AJ_";
pub const AGENT_DISPATCH_PREFIX: &str = "AD_";
pub const AGENT_BUILDER_PREFIX: &str = "AB_";
pub const AGENT_BUILDER_VERSION_PREFIX: &str = "ABV_";
pub const CLOUD_AGENT_PREFIX: &str = "CA_";
pub const CLOUD_AGENT_REGION_PREFIX: &str = "CAR_";
pub const CLOUD_AGENT_VERSION_PREFIX: &str = "CAV_";
pub const CLOUD_AGENT_SECRET_PREFIX: &str = "CAS_";
pub const CLOUD_AGENT_WORKER_PREFIX: &str = "CAW_";
pub const CLOUD_AGENT_PRIVATE_LINK_PREFIX: &str = "CAPL_";
pub const CLOUD_AGENT_PRIVATE_LINK_CLUSTER_PREFIX: &str = "CAPLC_";
pub const CLOUD_AGENT_PRIVATE_LINK_GATEWAY_PREFIX: &str = "CAPLG_";
pub const CLOUD_AGENT_PRIVATE_LINK_SECRET_PREFIX: &str = "CAPLS_";
pub const AGENT_GATEWAY_PREFIX: &str = "GW_";
pub const CARRIER_PREFIX: &str = "CR_";
pub const PHONE_NUMBER_PREFIX: &str = "PN_";
pub const SCENARIO_PREFIX: &str = "SCN_";
pub const SCENARIO_GROUP_PREFIX: &str = "SCNG_";
pub const AGENT_THREAD_PREFIX: &str = "AT_";
pub const AGENT_STREAM_PREFIX: &str = "AST_"; // ST_ is SIPTrunkPrefix

// shortuuid.DefaultAlphabet
pub const ID_ALPHABET: &str = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
pub const HASHED_ID_ALPHABET: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

pub fn new(prefix: &str) -> String {
    let mut g = new_generator();
    g.new(prefix)
}

// HashedID creates a hashed ID from a unique string
pub fn hashed_id(id: &str) -> String {
    let mut h = Sha1::new();
    h.update(id.as_bytes());
    let val = h.finalize();

    base62::encode_to_string(val.as_slice())
}

static B57_INDEX: [u8; 256] = new_b57_index();
const B57_CHARS: &[u8] = ID_ALPHABET.as_bytes();

const fn new_b57_index() -> [u8; 256] {
    let mut index = [0u8; 256];
    let mut i = 0;
    while i < B57_CHARS.len() {
        index[B57_CHARS[i] as usize] = i as u8;
        i += 1;
    }
    index
}

struct GuidGenerator<R: Rng> {
    rng: R,
}

fn new_generator() -> GuidGenerator<rand::rngs::ThreadRng> {
    GuidGenerator { rng: rand::rng() }
}

impl<R: Rng> GuidGenerator<R> {
    fn read_id_chars(&mut self, b: &mut [u8]) {
        let mut n = 0;
        loop {
            let mut r = self.rng.next_u64();
            for _ in 0..10 {
                if ((r & 0x3f) as usize) < B57_CHARS.len() {
                    b[n] = B57_CHARS[(r & 0x3f) as usize];
                    n += 1;
                    if n == b.len() {
                        return;
                    }
                }
                r >>= 6;
            }
        }
    }

    #[allow(clippy::new_ret_no_self, clippy::wrong_self_convention)]
    fn new(&mut self, prefix: &str) -> String {
        let mut b = vec![0u8; prefix.len() + SIZE];
        b[..prefix.len()].copy_from_slice(prefix.as_bytes());
        self.read_id_chars(&mut b[prefix.len()..]);
        String::from_utf8(b).expect("a prefix and characters of the alphabet")
    }
}

/// `livekit.Guid` upstream, the identifiers that have a compact form:
/// `TrackID | ParticipantID | RoomID`.
pub trait Guid: AsRef<str> + for<'a> From<&'a str> {
    /// What `guidPrefix` gives for the type upstream.
    const PREFIX: &'static str;
}

impl Guid for livekit::TrackId {
    const PREFIX: &'static str = TRACK_PREFIX;
}

impl Guid for livekit::ParticipantId {
    const PREFIX: &'static str = PARTICIPANT_PREFIX;
}

impl Guid for livekit::RoomId {
    const PREFIX: &'static str = ROOM_PREFIX;
}

/// `livekit.GuidBlock` upstream.
pub type GuidBlock = [u8; SIZE * 3 / 4];

fn guid_prefix<T: Guid>() -> &'static str {
    T::PREFIX
}

pub fn marshal<T: Guid>(id: &T) -> GuidBlock {
    marshal_append(Vec::new(), id)
        .try_into()
        .expect("one block")
}

pub fn marshal_append<T: Guid>(mut b: Vec<u8>, id: &T) -> Vec<u8> {
    let off = b.len();
    b.resize(off + SIZE * 3 / 4, 0);
    let id = id.as_ref().as_bytes();
    let idb = &id[id.len() - SIZE..];
    for i in 0..3 {
        let j = i * 3 + off;
        let k = i * 4;
        b[j] = B57_INDEX[idb[k] as usize] << 2 | B57_INDEX[idb[k + 1] as usize] >> 4;
        b[j + 1] = B57_INDEX[idb[k + 1] as usize] << 4 | B57_INDEX[idb[k + 2] as usize] >> 2;
        b[j + 2] = B57_INDEX[idb[k + 2] as usize] << 6 | B57_INDEX[idb[k + 3] as usize];
    }
    b
}

pub fn unmarshal<T: Guid>(b: GuidBlock) -> T {
    let prefix = guid_prefix::<T>();
    let mut id = vec![0u8; prefix.len() + SIZE];
    id[..prefix.len()].copy_from_slice(prefix.as_bytes());
    let idb = &mut id[prefix.len()..];
    for i in 0..3 {
        let j = i * 3;
        let k = i * 4;
        idb[k] = B57_CHARS[(b[j] >> 2) as usize];
        idb[k + 1] = B57_CHARS[((b[j] & 3) << 4 | b[j + 1] >> 4) as usize];
        idb[k + 2] = B57_CHARS[((b[j + 1] & 15) << 2 | b[j + 2] >> 6) as usize];
        idb[k + 3] = B57_CHARS[(b[j + 2] & 63) as usize];
    }
    T::from(std::str::from_utf8(&id).expect("a prefix and characters of the alphabet"))
}

// The pattern upstream: `^([a-zA-Z0-9]{1,16}_){1,2}[a-zA-Z0-9]{0,12}$`
pub fn is_valid_id(id: &str) -> bool {
    fn is_alphanumeric(s: &str) -> bool {
        s.bytes().all(|c| c.is_ascii_alphanumeric())
    }

    let parts: Vec<&str> = id.split('_').collect();
    let Some((last, groups)) = parts.split_last() else {
        return false;
    };
    (1..=2).contains(&groups.len())
        && groups
            .iter()
            .all(|group| (1..=16).contains(&group.len()) && is_alphanumeric(group))
        && last.len() <= 12
        && is_alphanumeric(last)
}

#[cfg(test)]
#[path = "id_test.rs"]
mod id_test;
