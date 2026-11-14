// Ported from LiveKit, pkg/sfu/bwe/sendsidebwe/packet_info.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `MarshalLogObject` is the derived `Debug`.

use crate::sfu::ccutils;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PacketInfo {
    pub(super) sequence_number: u64,
    pub(super) send_time: i64,
    pub(super) recv_time: i64,
    pub(super) probe_cluster_id: ccutils::ProbeClusterId,
    pub(super) size: u16,
    pub(super) is_rtx: bool,
    pub(super) is_probe: bool,
}
