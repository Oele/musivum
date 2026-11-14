// Ported from LiveKit, pkg/sfu/pacer/pass_through.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewPassThrough` is `PassThrough::new` and gives an
// `Arc`, as a pacer is shared; the embedded `Base` is a field, and the methods of `Pacer`
// that the original gets from it by embedding are passed on to it in the `impl Pacer` at
// the end. The original has no tests: the pass-through pacer is tested in base_test.rs.

use std::sync::Arc;

use crate::go::time::Duration;
use crate::protocol::logger::Logger;
use crate::sfu::bwe::Bwe;
use crate::sfu::ccutils;

use super::{Base, Pacer, PacerProbeObserverListener, Packet};

pub struct PassThrough {
    pub base: Base,
}

impl PassThrough {
    pub fn new(logger: Logger, bwe: Option<Arc<dyn Bwe>>) -> Arc<PassThrough> {
        Arc::new(PassThrough {
            base: Base::new(logger, bwe),
        })
    }

    pub fn stop(&self) {}

    pub fn enqueue(&self, pkt: Packet) {
        let _ = self.base.send_packet(pkt);
    }
}

// ------------------------------------------------

impl Pacer for PassThrough {
    fn enqueue(&self, p: Packet) {
        PassThrough::enqueue(self, p);
    }

    fn stop(&self) {
        PassThrough::stop(self);
    }

    fn set_interval(&self, interval: Duration) {
        self.base.set_interval(interval);
    }

    fn set_bitrate(&self, bitrate: i64) {
        self.base.set_bitrate(bitrate);
    }

    fn time_since_last_sent_packet(&self) -> Duration {
        self.base.time_since_last_sent_packet()
    }

    fn set_pacer_probe_observer_listener(
        &self,
        listener: Option<Arc<dyn PacerProbeObserverListener>>,
    ) {
        self.base
            .probe_observer
            .set_pacer_probe_observer_listener(listener);
    }

    fn start_probe_cluster(&self, pci: ccutils::ProbeClusterInfo) {
        self.base.probe_observer.start_probe_cluster(pci);
    }

    fn end_probe_cluster(
        &self,
        probe_cluster_id: ccutils::ProbeClusterId,
    ) -> ccutils::ProbeClusterInfo {
        self.base.probe_observer.end_probe_cluster(probe_cluster_id)
    }
}
