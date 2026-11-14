// Ported from LiveKit, pkg/service/signal.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node and without psrpc (see `routing/bus.rs`
// and `routing/signal.rs`). The context parameters are dropped: `SessionHandler::Logger` is
// `logger()` (`utils.GetLogger(ctx)` is the default logger, there being no context to carry
// another), and the copy of the incoming headers into a fresh context has no counterpart.
// - `NewSignalServer` takes neither the bus's server options nor a `SignalRelayConfig`: the
//   relay over psrpc is not here, and the configuration was read only by it. `SignalServer`
//   keeps the bus and its service, which `Start` registers on the bus under the node's ID
//   (`RegisterAllNodeTopics`) and `Stop` takes off it (`Kill`).
// - `signalService.RelaySignal` is `relay_signal` of `routing::SignalServerImpl`, which is
//   handed what the original reads from its stream and makes of it: the first request's
//   `StartSession` (it cannot be absent, so the check for it and the wait for it, with its
//   timeout, have no counterpart), the source of the requests the client writes after it
//   (the original's `reqChan`, which its goroutine fills from the stream) and the sink of
//   the responses (the original's `NewSignalMessageSink` over the stream). The rest is the
//   original's: the participant read from the `StartSession`, the logger, the session
//   handler, the sink closed and the error logged and returned when the handler fails.
//   `stream.Hijack()` has no counterpart. The error is the service's, which the bus carries
//   as `routing::Error::Service`.
// - Not here, being the relay over psrpc: `signalResponseMessageWriter` and
//   `signalRequestMessageReader`, and with them the counters of relayed requests
//   (`prometheus.RecordSignalRequestSuccess`, `RecordSignalRequestFailure`).
// - `defaultSessionHandler` holds the room manager, the bus holds the signal service (which
//   holds the session handler) until `Stop`.

use std::sync::Arc;

use super::{Error, RoomManager};
use crate::protocol::livekit;
use crate::protocol::logger::{self, Logger};
use crate::routing::{self, LocalNode, MessageBus, MessageSink, MessageSource, Router};
use crate::telemetry::prometheus;
use crate::{debugw, errorw};

#[cfg_attr(test, musivum_fakes::fake)]
pub trait SessionHandler: Send + Sync {
    fn logger(&self) -> Logger;

    fn handle_session(
        &self,
        pi: routing::ParticipantInit,
        connection_id: &livekit::ConnectionId,
        request_source: Arc<dyn MessageSource>,
        response_sink: Arc<dyn MessageSink>,
    ) -> Result<(), Error>;
}

pub struct SignalServer {
    server: Arc<SignalService>,
    node_id: livekit::NodeId,
    // what psrpc's server registers on and is killed from
    bus: Arc<MessageBus>,
}

impl SignalServer {
    pub fn new(
        node_id: livekit::NodeId,
        region: &str,
        bus: Arc<MessageBus>,
        session_handler: Arc<dyn SessionHandler>,
    ) -> Result<SignalServer, Error> {
        let s = Arc::new(SignalService {
            region: region.to_string(),
            session_handler,
        });
        Ok(SignalServer {
            server: s,
            node_id,
            bus,
        })
    }

    pub fn new_default(
        current_node: Arc<dyn LocalNode>,
        bus: Arc<MessageBus>,
        router: Arc<dyn Router>,
        room_manager: Arc<RoomManager>,
    ) -> Result<SignalServer, Error> {
        SignalServer::new(
            current_node.node_id(),
            &current_node.region(),
            bus,
            Arc::new(DefaultSessionHandler {
                current_node,
                router,
                room_manager,
            }),
        )
    }
}

struct DefaultSessionHandler {
    current_node: Arc<dyn LocalNode>,
    router: Arc<dyn Router>,
    room_manager: Arc<RoomManager>,
}

impl SessionHandler for DefaultSessionHandler {
    fn logger(&self) -> Logger {
        logger::get_logger()
    }

    fn handle_session(
        &self,
        pi: routing::ParticipantInit,
        _connection_id: &livekit::ConnectionId,
        request_source: Arc<dyn MessageSource>,
        response_sink: Arc<dyn MessageSink>,
    ) -> Result<(), Error> {
        prometheus::increment_participant_rtc_init(1);

        let room_name = livekit::RoomName::from(
            pi.create_room
                .as_ref()
                .expect("invalid memory address or nil pointer dereference")
                .name
                .as_str(),
        );
        let rtc_node = self.router.get_node_for_room(&room_name)?;

        if livekit::NodeId::from(&rtc_node.id) != self.current_node.node_id() {
            let err = routing::Error::IncorrectRtcNode;
            errorw!(logger::get_logger(), "called participant on incorrect node", &err,
                "rtcNode" => rtc_node,
            );
            return Err(err.into());
        }

        self.room_manager
            .start_session(pi, request_source, response_sink, false)
    }
}

impl SignalServer {
    pub fn start(&self) -> Result<(), Error> {
        debugw!(logger::get_logger(), "starting relay signal server", "topic" => self.node_id);
        self.bus
            .register_signal_server(&self.node_id, self.server.clone());
        Ok(())
    }

    pub fn stop(&self) {
        self.bus.deregister_signal_server(&self.node_id);
    }
}

struct SignalService {
    region: String,
    session_handler: Arc<dyn SessionHandler>,
    // `config config.SignalRelayConfig`: read only by the relay over psrpc (see the header)
}

impl routing::SignalServerImpl for SignalService {
    fn relay_signal(
        &self,
        start_session: livekit::StartSession,
        request_source: Arc<dyn MessageSource>,
        response_sink: Arc<dyn MessageSink>,
    ) -> Result<(), routing::Error> {
        // the wait for the first request and the check that it starts a session: see the
        // header
        let ss = start_session;

        let pi = routing::participant_init_from_start_session(&ss, &self.region)
            .map_err(|err| Error::ReadParticipantFromSession(err.to_string()))?;

        let l = self.session_handler.logger().with_values(&[
            ("room", &ss.room_name),
            ("participant", &ss.identity),
            ("connID", &ss.connection_id),
        ]);

        // `stream.Hijack()`, the sink over the stream, `reqChan` and the goroutine that copies
        // the stream into it: `request_source` and `response_sink` (see the header)
        let sink = response_sink;

        let err = self.session_handler.handle_session(
            pi,
            &livekit::ConnectionId::from(&ss.connection_id),
            request_source,
            sink.clone(),
        );
        if let Err(err) = &err {
            sink.close();
            errorw!(l, "could not handle new participant", err);
        }
        err.map_err(routing::Error::from)
    }
}

#[cfg(test)]
#[path = "signal_test.rs"]
mod signal_test;
