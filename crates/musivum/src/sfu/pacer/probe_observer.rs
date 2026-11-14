// Ported from LiveKit, pkg/sfu/pacer/probe_observer.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; what `lock` guards is in an inner struct; the
// listener, which the original sets and reads without a lock, has a lock of its own and is
// called with no lock held; a size is a `usize`. The original has no tests: what the
// observer counts and reports is tested through a pacer, in base_test.rs.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::go::atomic;
use crate::go::time::Duration;
use crate::protocol::logger::Logger;
use crate::protocol::utils::mono;
use crate::sfu::ccutils;
use crate::{debugw, warnw};

use super::PacerProbeObserverListener;

pub struct ProbeObserver {
    logger: Logger,

    listener: RwLock<Option<Arc<dyn PacerProbeObserverListener>>>,

    is_in_probe: atomic::Bool,

    lock: Mutex<ProbeObserverInner>,
}

// What `lock` guards.
struct ProbeObserverInner {
    pci: ccutils::ProbeClusterInfo,
}

impl ProbeObserver {
    pub fn new(logger: Logger) -> ProbeObserver {
        ProbeObserver {
            logger,
            listener: RwLock::new(None),
            is_in_probe: atomic::Bool::new(false),
            lock: Mutex::new(ProbeObserverInner {
                pci: ccutils::ProbeClusterInfo::default(),
            }),
        }
    }

    pub fn set_pacer_probe_observer_listener(
        &self,
        listener: Option<Arc<dyn PacerProbeObserverListener>>,
    ) {
        *self.listener.write() = listener;
    }

    pub fn start_probe_cluster(&self, pci: ccutils::ProbeClusterInfo) {
        if self.is_in_probe.load() {
            warnw!(
                self.logger,
                "ignoring start of a new probe cluster when already active",
                nil,
                "probeClusterInfo" => pci,
            );
            return;
        }

        let mut po = self.lock.lock();

        po.pci = pci;
        po.pci.result = ccutils::ProbeClusterResult {
            start_time: mono::unix_nano(),
            ..Default::default()
        };

        self.is_in_probe.store(true);
    }

    pub fn end_probe_cluster(
        &self,
        probe_cluster_id: ccutils::ProbeClusterId,
    ) -> ccutils::ProbeClusterInfo {
        if !self.is_in_probe.load() {
            // probe not active
            if probe_cluster_id != ccutils::PROBE_CLUSTER_ID_INVALID {
                debugw!(
                    self.logger,
                    "ignoring end of a probe cluster when not active",
                    "probeClusterId" => probe_cluster_id,
                );
            }
            return ccutils::PROBE_CLUSTER_INFO_INVALID;
        }

        let mut po = self.lock.lock();

        if po.pci.id != probe_cluster_id {
            // probe cluster id not active
            warnw!(
                self.logger,
                "ignoring end of a probe cluster of a non-active one",
                nil,
                "probeClusterId" => probe_cluster_id,
                "active" => po.pci.id,
            );
            return ccutils::PROBE_CLUSTER_INFO_INVALID;
        }

        if po.pci.result.end_time == 0 {
            po.pci.result.end_time = mono::unix_nano();
        }

        self.is_in_probe.store(false);

        po.pci
    }

    pub fn record_packet(
        &self,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ccutils::ProbeClusterId,
        is_probe: bool,
    ) {
        if !self.is_in_probe.load() {
            return;
        }

        let mut po = self.lock.lock();
        if probe_cluster_id != po.pci.id || po.pci.result.end_time != 0 {
            return;
        }

        if is_probe {
            po.pci.result.packets_probe += 1;
            po.pci.result.bytes_probe += size as i64;
        } else if is_rtx {
            po.pci.result.packets_non_probe_rtx += 1;
            po.pci.result.bytes_non_probe_rtx += size as i64;
        } else {
            po.pci.result.packets_non_probe_primary += 1;
            po.pci.result.bytes_non_probe_primary += size as i64;
        }

        let mut notify = false;
        let mut cluster_id = ccutils::ProbeClusterId::default();
        if po.pci.result.end_time == 0
            && ((po.pci.result.bytes() >= po.pci.goal.desired_bytes)
                && Duration(mono::unix_nano().wrapping_sub(po.pci.result.start_time))
                    >= po.pci.goal.duration)
        {
            po.pci.result.end_time = mono::unix_nano();
            po.pci.result.is_completed = true;

            notify = true;
            cluster_id = po.pci.id;
        }
        drop(po);

        if notify {
            let listener = self.listener.read().clone();
            if let Some(listener) = listener {
                listener.on_pacer_probe_observer_cluster_complete(cluster_id);
            }
        }
    }
}

// ------------------------------------------------
