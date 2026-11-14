// Ported from LiveKit, pkg/service/signal_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The bus is the process's (`routing/bus.rs`), the client
// `routing`'s signal client, which takes no `SignalRelayConfig` (neither does the server):
// the configuration of the relay over psrpc has no counterpart. `prometheus.Init` in `init()`
// is not needed (the counters need no initializing). What the stub sees and does is handed
// back to the test through a channel where the original writes variables the goroutine
// shares; the stub's `go func()` is a task. `errors.New("start session failed")` is an error
// of the service (`OperationFailed`): only that it is one matters.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use super::*;
use crate::protocol::logger;
use crate::routing::{
    Message, MessageSink, MessageSource, ParticipantInit, new_local_message_bus, new_signal_client,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn test_signal() {
    {
        // messages are delivered
        let bus = new_local_message_bus();

        let req_message_in: Message = livekit::SignalRequest {
            message: Some(livekit::signal_request::Message::Ping(123)),
        }
        .into();
        let res_message_in: Message = livekit::SignalResponse {
            message: Some(livekit::signal_response::Message::Pong(321)),
        }
        .into();

        let (done_tx, done) = oneshot::channel::<(Option<Message>, Result<(), routing::Error>)>();
        let done_tx = parking_lot::Mutex::new(Some(done_tx));

        let client = new_signal_client("node0".into(), bus.clone()).unwrap();

        let handler = Arc::new(FakeSessionHandler::default());
        handler.logger_calls(logger::get_logger);
        handler.handle_session_calls({
            let res_message_in = res_message_in.clone();
            move |_pi: ParticipantInit,
                  _connection_id: &livekit::ConnectionId,
                  request_source: Arc<dyn MessageSource>,
                  response_sink: Arc<dyn MessageSink>| {
                let res_message_in = res_message_in.clone();
                let done_tx = done_tx.lock().take().unwrap();
                tokio::spawn(async move {
                    let req_message_out = request_source.read_chan().recv().await;
                    let res_err = response_sink.write_message(res_message_in);
                    response_sink.close();
                    let _ = done_tx.send((req_message_out, res_err));
                });
                Ok(())
            }
        });
        let server =
            SignalServer::new("node1".into(), "region", bus.clone(), handler.clone()).unwrap();

        server.start().unwrap();

        let (_, req_sink, res_source) = client
            .start_participant_signal(&"room1".into(), ParticipantInit::default(), &"node1".into())
            .unwrap();

        req_sink.write_message(req_message_in.clone()).unwrap();

        let (req_message_out, res_err) =
            tokio::time::timeout(TIMEOUT, done).await.unwrap().unwrap();
        assert_eq!(
            Some(req_message_in),
            req_message_out,
            "req message should match"
        );
        assert_eq!(Ok(()), res_err);

        let res_message_out = tokio::time::timeout(TIMEOUT, res_source.read_chan().recv())
            .await
            .unwrap();
        assert_eq!(
            Some(res_message_in),
            res_message_out,
            "res message should match"
        );
    }

    {
        // messages are delivered when session handler fails
        let bus = new_local_message_bus();

        let res_message_in: Message = livekit::SignalResponse {
            message: Some(livekit::signal_response::Message::Pong(321)),
        }
        .into();

        let (done_tx, done) = oneshot::channel::<Result<(), routing::Error>>();
        let done_tx = parking_lot::Mutex::new(Some(done_tx));

        let client = new_signal_client("node0".into(), bus.clone()).unwrap();

        let handler = Arc::new(FakeSessionHandler::default());
        handler.logger_calls(logger::get_logger);
        handler.handle_session_calls({
            let res_message_in = res_message_in.clone();
            move |_pi: ParticipantInit,
                  _connection_id: &livekit::ConnectionId,
                  _request_source: Arc<dyn MessageSource>,
                  response_sink: Arc<dyn MessageSink>| {
                // `defer close(done)`
                let res_err = response_sink.write_message(res_message_in.clone());
                let _ = done_tx.lock().take().unwrap().send(res_err);
                // `errors.New("start session failed")`
                Err(Error::OperationFailed)
            }
        });
        let server =
            SignalServer::new("node1".into(), "region", bus.clone(), handler.clone()).unwrap();

        server.start().unwrap();

        let (_, _, res_source) = client
            .start_participant_signal(&"room1".into(), ParticipantInit::default(), &"node1".into())
            .unwrap();

        let res_err = tokio::time::timeout(TIMEOUT, done).await.unwrap().unwrap();
        assert_eq!(Ok(()), res_err);

        let res_message_out = tokio::time::timeout(TIMEOUT, res_source.read_chan().recv())
            .await
            .unwrap();
        assert_eq!(
            Some(res_message_in),
            res_message_out,
            "res message should match"
        );
    }
}
