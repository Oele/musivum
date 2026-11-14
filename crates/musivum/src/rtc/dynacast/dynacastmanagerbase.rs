// Ported from LiveKit, pkg/rtc/dynacast/dynacastmanagerbase.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Upstream a manager embeds the base and guards its own
// fields with the base's lock. A lock owns what it guards here, so the base keeps those
// fields of the manager, of the type `M`, in what its lock guards (`manager`), and the two
// functions of the parameters that the base calls with the lock held (`on_restart`,
// `on_regress_codec`) get them as an argument. The back reference: the base does not hold
// the manager it is embedded in. The functions of the parameters that have to reach the
// manager (`on_dynacast_quality_create`, `on_update_needed`) hold a `Weak` of it, so the
// manager owns the base and the base keeps nothing alive. The two "null" types the original
// embeds are fields, to which a manager passes on the calls it does not define; the base
// itself does not implement `DynacastManager`. `regressed_codec`, a map to `struct{}`, is a
// set. Where the original hands `RegressTo` a nil interface and panics in it (the manager
// was closed in between, or `to_mime` is unknown), `regress_to` gets `None`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::protocol::codecs::mime::MimeType;
use crate::protocol::logger::Logger;
use crate::utils;
use crate::warnw;

use super::{DynacastManagerNull, DynacastQuality, DynacastQualityListenerNull};

pub(super) struct DynacastManagerBaseParams<M> {
    pub logger: Logger,
    pub ops_queue_depth: usize,
    pub on_restart: Box<dyn Fn(&mut M) + Send + Sync>,
    pub on_dynacast_quality_create: Box<dyn Fn(MimeType) -> Arc<dyn DynacastQuality> + Send + Sync>,
    #[allow(clippy::type_complexity)]
    pub on_regress_codec: Box<dyn Fn(&mut M, MimeType, MimeType) + Send + Sync>,
    pub on_update_needed: Box<dyn Fn(bool) + Send + Sync>,
}

pub(super) struct DynacastManagerBase<M> {
    params: DynacastManagerBaseParams<M>,

    pub(super) lock: RwLock<DynacastManagerBaseInner<M>>,

    pub(super) notify_ops_queue: utils::OpsQueue,

    pub(super) dynacast_manager_null: DynacastManagerNull,
    pub(super) dynacast_quality_listener_null: DynacastQualityListenerNull,
}

// What `lock` guards.
pub(super) struct DynacastManagerBaseInner<M> {
    pub(super) regressed_codec: HashSet<MimeType>,
    dynacast_quality: HashMap<MimeType, Arc<dyn DynacastQuality>>,

    is_closed: bool,

    // the fields of the manager that embeds this base
    pub(super) manager: M,
}

impl<M: Default> DynacastManagerBase<M> {
    pub(super) fn new(mut params: DynacastManagerBaseParams<M>) -> Self {
        if params.ops_queue_depth == 0 {
            params.ops_queue_depth = 4;
        }
        let d = Self {
            lock: RwLock::new(DynacastManagerBaseInner {
                regressed_codec: HashSet::new(),
                dynacast_quality: HashMap::new(),
                is_closed: false,
                manager: M::default(),
            }),
            notify_ops_queue: utils::OpsQueue::new(utils::OpsQueueParams {
                name: "dynacast-notify".to_string(),
                min_size: params.ops_queue_depth,
                flush_on_stop: true,
                logger: params.logger.clone(),
            }),
            params,
            dynacast_manager_null: DynacastManagerNull::default(),
            dynacast_quality_listener_null: DynacastQualityListenerNull::default(),
        };
        d.notify_ops_queue.start();
        d
    }

    pub(super) fn add_codec(&self, mime: MimeType) {
        self.get_or_create_dynacast_quality(mime);
    }

    pub(super) fn handle_codec_regression(&self, from_mime: MimeType, to_mime: MimeType) {
        let from_dq = self.get_or_create_dynacast_quality(from_mime);

        let mut inner = self.lock.write();
        if inner.is_closed {
            return;
        }

        let Some(from_dq) = from_dq else {
            // should not happen as we have added the codec on setup receiver
            warnw!(
                self.params.logger,
                "regression from codec not found",
                nil,
                "mime" => from_mime,
                "toMime" => to_mime,
            );
            return;
        };
        inner.regressed_codec.insert(from_mime);
        (self.params.on_regress_codec)(&mut inner.manager, from_mime, to_mime);
        drop(inner);

        (self.params.on_update_needed)(false);

        from_dq.stop();
        from_dq.regress_to(self.get_or_create_dynacast_quality(to_mime));
    }

    pub(super) fn restart(&self) {
        let mut inner = self.lock.write();
        (self.params.on_restart)(&mut inner.manager);

        let dqs = inner.get_dynacast_qualities_locked();
        drop(inner);

        for dq in dqs {
            dq.restart();
        }
    }

    pub(super) fn close(&self) {
        self.notify_ops_queue.stop();

        let mut inner = self.lock.write();
        let dqs = inner.get_dynacast_qualities_locked();
        inner.dynacast_quality = HashMap::new();

        inner.is_closed = true;
        drop(inner);

        for dq in dqs {
            dq.stop();
        }
    }

    // There are situations like track unmute or streaming from a different node
    // where subscription changes needs to sent to the provider immediately.
    // This bypasses any debouncing and forces a subscription change update
    // with immediate effect.
    pub(super) fn force_update(&self) {
        (self.params.on_update_needed)(true);
    }

    pub(super) fn clear_subscriber_nodes(&self) {
        let inner = self.lock.write();
        let dqs = inner.get_dynacast_qualities_locked();
        drop(inner);
        for dq in dqs {
            dq.clear_subscriber_nodes();
        }
    }

    pub(super) fn get_or_create_dynacast_quality(
        &self,
        mime_type: MimeType,
    ) -> Option<Arc<dyn DynacastQuality>> {
        let mut inner = self.lock.write();

        if inner.is_closed || mime_type == MimeType::Unknown {
            return None;
        }

        if let Some(dq) = inner.dynacast_quality.get(&mime_type) {
            return Some(dq.clone());
        }

        let dq = (self.params.on_dynacast_quality_create)(mime_type);
        dq.start();

        inner.dynacast_quality.insert(mime_type, dq.clone());
        Some(dq)
    }
}

impl<M> DynacastManagerBaseInner<M> {
    fn get_dynacast_qualities_locked(&self) -> Vec<Arc<dyn DynacastQuality>> {
        self.dynacast_quality.values().cloned().collect()
    }
}
