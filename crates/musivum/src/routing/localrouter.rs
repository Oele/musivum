// Ported from LiveKit, pkg/routing/localrouter.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The context parameters are dropped. `NewLocalRouter`
// returns an `Arc`, and the router keeps a weak reference to itself for its statistics
// worker, a task that ends when the router is dropped (upstream it runs for as long as the
// process; `Stop` does not end it in either). `requestChannels` and `responseChannels`, which
// the original makes and never uses, are not here. The error log of
// `StartParticipantSignalWithNodeID` has no connection ID: the signal client returns none
// with an error (see signal.rs). The commented-out `memStatsWorker` is not ported. The
// methods are in three blocks, the router's own, `MessageRouter`'s and `Router`'s, each in the
// original's order.

use std::sync::{Arc, Weak};

use super::{
    Error, LocalNode, MessageRouter, NodeStatsConfig, ParticipantInit, RoomManagerClient, Router,
    SignalClient, StartParticipantSignalResults,
};
use crate::errorw;
use crate::go::atomic;
use crate::go::time;
use crate::protocol::livekit;
use crate::protocol::logger;

// a router of messages on the same node, basic implementation for local testing
pub struct LocalRouter {
    current_node: Arc<dyn LocalNode>,
    signal_client: Arc<dyn SignalClient>,
    room_manager_client: Arc<dyn RoomManagerClient>,
    node_stats_config: NodeStatsConfig,

    is_started: atomic::Bool,
    weak_self: Weak<LocalRouter>,
}

impl LocalRouter {
    pub fn new(
        current_node: Arc<dyn LocalNode>,
        signal_client: Arc<dyn SignalClient>,
        room_manager_client: Arc<dyn RoomManagerClient>,
        node_stats_config: NodeStatsConfig,
    ) -> Arc<LocalRouter> {
        Arc::new_cyclic(|weak_self| LocalRouter {
            current_node,
            signal_client,
            room_manager_client,
            node_stats_config,
            is_started: atomic::Bool::new(false),
            weak_self: weak_self.clone(),
        })
    }

    pub fn get_node(&self, node_id: &livekit::NodeId) -> Result<livekit::Node, Error> {
        if *node_id == self.current_node.node_id() {
            return Ok(self.current_node.clone_node());
        }
        Err(Error::NotFound)
    }

    pub fn create_room_with_node_id(
        &self,
        req: &livekit::CreateRoomRequest,
        node_id: &livekit::NodeId,
    ) -> Result<livekit::Room, Error> {
        self.room_manager_client.create_room(node_id, req)
    }

    pub fn start_participant_signal_with_node_id(
        &self,
        room_name: &livekit::RoomName,
        pi: ParticipantInit,
        node_id: &livekit::NodeId,
    ) -> Result<StartParticipantSignalResults, Error> {
        let identity = pi.identity.clone();
        match self
            .signal_client
            .start_participant_signal(room_name, pi, node_id)
        {
            Err(err) => {
                errorw!(
                    logger::get_logger(),
                    "could not handle new participant",
                    &err,
                    "room" => room_name,
                    "participant" => identity
                );
                Err(err)
            }
            Ok((connection_id, req_sink, res_source)) => Ok(StartParticipantSignalResults {
                connection_id,
                request_sink: Some(req_sink),
                response_source: Some(res_source),
                node_id: node_id.clone(),
                ..Default::default()
            }),
        }
    }

    async fn stats_worker(weak_self: Weak<LocalRouter>) {
        loop {
            let Some(r) = weak_self.upgrade() else {
                return;
            };
            if !r.is_started.load() {
                return;
            }
            let interval = r.node_stats_config.stats_update_interval;
            drop(r);
            time::sleep(interval).await;
            let Some(r) = weak_self.upgrade() else {
                return;
            };
            r.current_node.update_node_stats();
        }
    }
}

impl MessageRouter for LocalRouter {
    fn create_room(&self, req: &livekit::CreateRoomRequest) -> Result<livekit::Room, Error> {
        self.create_room_with_node_id(req, &self.current_node.node_id())
    }

    fn start_participant_signal(
        &self,
        room_name: &livekit::RoomName,
        pi: ParticipantInit,
    ) -> Result<StartParticipantSignalResults, Error> {
        self.start_participant_signal_with_node_id(room_name, pi, &self.current_node.node_id())
    }
}

impl Router for LocalRouter {
    fn get_node_for_room(&self, _room_name: &livekit::RoomName) -> Result<livekit::Node, Error> {
        Ok(self.current_node.clone_node())
    }

    fn set_node_for_room(
        &self,
        _room_name: &livekit::RoomName,
        _node_id: &livekit::NodeId,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn clear_room_state(&self, _room_name: &livekit::RoomName) -> Result<(), Error> {
        Ok(())
    }

    fn register_node(&self) -> Result<(), Error> {
        Ok(())
    }

    fn unregister_node(&self) -> Result<(), Error> {
        Ok(())
    }

    fn remove_dead_nodes(&self) -> Result<(), Error> {
        Ok(())
    }

    fn list_nodes(&self) -> Result<Vec<livekit::Node>, Error> {
        Ok(vec![self.current_node.clone_node()])
    }

    fn start(&self) -> Result<(), Error> {
        if self.is_started.swap(true) {
            return Ok(());
        }
        time::spawn(LocalRouter::stats_worker(self.weak_self.clone()));
        // go r.memStatsWorker()
        Ok(())
    }

    fn drain(&self) {
        self.current_node
            .set_state(livekit::NodeState::ShuttingDown);
    }

    fn stop(&self) {}

    fn get_region(&self) -> String {
        self.current_node.region()
    }
}

#[cfg(test)]
#[path = "localrouter_test.rs"]
mod localrouter_test;
