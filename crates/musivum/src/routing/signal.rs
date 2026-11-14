// Ported from LiveKit, pkg/routing/signal.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node and without psrpc. Upstream the signal
// client opens a psrpc stream to the node's signal server, sends the `StartSession` as its
// first request, and relays the requests and responses of the session over it, in batches
// with sequence numbers and retries, between a message channel on each side. Here the
// stream is `SignalStream`, the two message channels it joins, and the server is called
// directly (`bus.rs`):
// - `StartParticipantSignal` takes the same steps in the same order: the connection ID, the
//   `StartSession` (`ToStartSession`, whose error it returns), the logger and its debug
//   line, then the stream: the node's server or `RequestTimedOut` where psrpc's `RelaySignal`
//   fails, and the `StartSession` handed to the server's `relay_signal` where it is sent.
//   The server is called on the caller's thread and has handed the session on when the call
//   returns (upstream it runs at the same time, on psrpc's goroutine); its error closes the
//   stream, as the original's server closes the stream with it. The result is that of the
//   original either way: the connection ID, the sink of the requests and the source of the
//   responses.
// - The channel of requests (which the original's server makes, `reqChan`) and the channel of
//   responses (`resChan`) are made by the client: requests written to the client's sink go
//   into the first, which is the server's source; responses written to the server's sink go
//   into the second, which is the client's source. A sink is `signalMessageSink` over the
//   stream: closing either closes the stream and so both channels, whose messages written
//   before are still read; a write to a closed stream gives `ChannelClosed`, where psrpc gives
//   `ErrStreamClosed` (which the signaller takes as the same). A write that finds the other
//   side's channel full closes that channel, as the original's copy of the stream into it does
//   when it fails, and succeeds, as a write into the original's queue does. Closing a source
//   closes only its channel. `ActiveCount` counts the open streams.
// - Not here, being the relay over psrpc: `signalRequestMessageWriter`,
//   `signalResponseMessageReader`, `RelaySignalMessage`, `SignalMessageWriter`,
//   `SignalMessageReader`, `CopySignalStreamToMessageChannel`, `signalMessageReader`,
//   `SignalSinkParams` and the queue, retries and sequence numbers of `signalMessageSink`;
//   `NewSignalClient` takes no `SignalRelayConfig`, which only they read; the counters of
//   relayed messages (`prometheus.RecordSignal...`) are not kept.
// - The connection ID is not returned with an error (Go's named results return it).

use std::sync::{Arc, Weak};

use super::bus::MessageBus;
use super::{Error, Message, MessageChannel, MessageSink, MessageSource, ParticipantInit};
use crate::debugw;
use crate::go::atomic;
use crate::protocol::livekit;
use crate::protocol::logger::{self, Logger};
use crate::protocol::utils::guid;

#[cfg_attr(test, musivum_fakes::fake)]
pub trait SignalClient: Send + Sync {
    fn active_count(&self) -> usize;
    #[allow(clippy::type_complexity)]
    fn start_participant_signal(
        &self,
        room_name: &livekit::RoomName,
        pi: ParticipantInit,
        node_id: &livekit::NodeId,
    ) -> Result<
        (
            livekit::ConnectionId,
            Arc<dyn MessageSink>,
            Arc<dyn MessageSource>,
        ),
        Error,
    >;
}

struct SignalClientImpl {
    // the node the client is on, which psrpc sends from; not read here
    #[allow(dead_code)]
    node_id: livekit::NodeId,
    client: Arc<MessageBus>,
    active: Arc<atomic::Int32>,
}

pub fn new_signal_client(
    node_id: livekit::NodeId,
    bus: Arc<MessageBus>,
) -> Result<Arc<dyn SignalClient>, Error> {
    Ok(Arc::new(SignalClientImpl {
        node_id,
        client: bus,
        active: Arc::new(atomic::Int32::new(0)),
    }))
}

impl SignalClient for SignalClientImpl {
    fn active_count(&self) -> usize {
        self.active.load() as usize
    }

    #[allow(clippy::type_complexity)]
    fn start_participant_signal(
        &self,
        room_name: &livekit::RoomName,
        pi: ParticipantInit,
        node_id: &livekit::NodeId,
    ) -> Result<
        (
            livekit::ConnectionId,
            Arc<dyn MessageSink>,
            Arc<dyn MessageSource>,
        ),
        Error,
    > {
        let connection_id = livekit::ConnectionId::from(guid::new("CO_"));
        let ss = pi.to_start_session(room_name, &connection_id)?;

        let l = logger::get_logger().with_values(&[
            ("room", room_name),
            ("reqNodeID", node_id),
            ("participant", &pi.identity),
            ("connID", &connection_id),
            ("participantInit", &pi),
            ("startSession", &ss),
        ]);

        debugw!(l, "starting signal connection");

        // `r.client.RelaySignal(ctx, nodeID)`
        let server = self.client.signal_server(node_id)?;

        // the stream, with the channel of requests the server reads and the channel of
        // responses the client reads
        let req_chan = MessageChannel::new_default(connection_id.clone());
        let res_chan = MessageChannel::new_default(connection_id.clone());
        let stream = SignalStream::new(req_chan.clone(), res_chan.clone(), l.clone());

        let sink = SignalMessageSink::new(stream.clone(), req_chan.clone(), connection_id.clone());

        // the goroutine that copies the stream into `resChan` counts while the stream is open
        self.active.inc();
        stream.on_close(Box::new({
            let active = self.active.clone();
            move || {
                active.dec();
            }
        }));

        // `stream.Send(&rpc.RelaySignalRequest{StartSession: ss})`: the server reads it and
        // hands the session on, with the source of the requests and the sink of the responses
        let response_sink =
            SignalMessageSink::new(stream.clone(), res_chan.clone(), connection_id.clone());
        if let Err(err) = server.relay_signal(ss, req_chan, response_sink) {
            stream.close_with_error(Some(err));
        }

        Ok((connection_id, sink, res_chan))
    }
}

// ----------------------------------------

/// The stream between the signal client and the signal server of one session. Written new:
/// psrpc's stream in the process.
struct SignalStream {
    req_chan: Weak<MessageChannel>,
    res_chan: Weak<MessageChannel>,
    logger: Logger,
    closed: atomic::Bool,
    on_close: parking_lot::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl SignalStream {
    fn new(
        req_chan: Arc<MessageChannel>,
        res_chan: Arc<MessageChannel>,
        logger: Logger,
    ) -> Arc<SignalStream> {
        Arc::new(SignalStream {
            req_chan: Arc::downgrade(&req_chan),
            res_chan: Arc::downgrade(&res_chan),
            logger,
            closed: atomic::Bool::new(false),
            on_close: parking_lot::Mutex::new(None),
        })
    }

    fn on_close(&self, f: Box<dyn FnOnce() + Send>) {
        *self.on_close.lock() = Some(f);
    }

    fn is_closed(&self) -> bool {
        self.closed.load()
    }

    fn close(&self) {
        self.close_with_error(None);
    }

    // Both ends see the stream end: each side's copy of it into its channel stops, and the
    // channel is closed.
    fn close_with_error(&self, err: Option<Error>) {
        if self.closed.swap(true) {
            return;
        }
        if let Some(req_chan) = self.req_chan.upgrade() {
            req_chan.close();
        }
        if let Some(res_chan) = self.res_chan.upgrade() {
            res_chan.close();
        }
        debugw!(self.logger, "signal stream closed", "error" => err);
        let on_close = self.on_close.lock().take();
        if let Some(on_close) = on_close {
            on_close();
        }
    }
}

/// `signalMessageSink`: what one side writes into the stream, read from the other side's
/// channel.
struct SignalMessageSink {
    stream: Arc<SignalStream>,
    ch: Arc<MessageChannel>,
    connection_id: livekit::ConnectionId,
}

impl SignalMessageSink {
    fn new(
        stream: Arc<SignalStream>,
        ch: Arc<MessageChannel>,
        connection_id: livekit::ConnectionId,
    ) -> Arc<SignalMessageSink> {
        Arc::new(SignalMessageSink {
            stream,
            ch,
            connection_id,
        })
    }
}

impl MessageSink for SignalMessageSink {
    fn write_message(&self, msg: Message) -> Result<(), Error> {
        if self.stream.is_closed() {
            // psrpc.ErrStreamClosed
            return Err(Error::ChannelClosed);
        }

        // a write into a closed channel is queued for the other side, which no longer reads
        // the stream; one into a full channel makes the other side's copy of the stream fail
        // and close its channel
        if let Err(Error::ChannelFull) = self.ch.write_message(msg) {
            self.ch.close();
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.stream.is_closed()
    }

    fn close(&self) {
        self.stream.close();
    }

    fn connection_id(&self) -> livekit::ConnectionId {
        self.connection_id.clone()
    }
}
