// Ported from LiveKit, pkg/routing/messagechannel.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. A message is a `Message` (`message.rs`), the channel
// tokio's, and `ReadChan()` gives its reading end as a `ReadChan`, which a task awaits and a
// thread blocks on. The writing end is dropped where the original closes the channel, which
// is what closes it here; a channel of size 0 has room for one message (see `make_chan`).
// `new` returns an `Arc`: the channel is held by who writes to it and by who reads from it.
// `onClose`, which the original sets and reads without a lock, is behind one of its own. The
// channel is a `MessageSink` and a `MessageSource` by the two `impl`s at the end, which hand
// on to the methods of the same names.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;

use super::errors::Error;
use super::interfaces::{MessageSink, MessageSource};
use super::message::{Message, ReadChan, make_chan};
use crate::protocol::livekit;

pub const DEFAULT_MESSAGE_CHANNEL_SIZE: usize = 200;

pub struct MessageChannel {
    connection_id: livekit::ConnectionId,
    read_chan: ReadChan,
    on_close: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    lock: RwLock<MessageChannelInner>,
}

struct MessageChannelInner {
    // The writing end of `msgChan`; `None` once the channel is closed.
    msg_chan: Option<mpsc::Sender<Message>>,
    is_closed: bool,
}

impl MessageChannel {
    pub fn new_default(connection_id: livekit::ConnectionId) -> Arc<Self> {
        Self::new(connection_id, DEFAULT_MESSAGE_CHANNEL_SIZE)
    }

    pub fn new(connection_id: livekit::ConnectionId, size: usize) -> Arc<Self> {
        // allow some buffer to avoid blocked writes
        let (msg_chan, read_chan) = make_chan(size);
        Arc::new(Self {
            connection_id,
            read_chan,
            on_close: Mutex::new(None),
            lock: RwLock::new(MessageChannelInner {
                msg_chan: Some(msg_chan),
                is_closed: false,
            }),
        })
    }

    pub fn on_close(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.on_close.lock() = Some(Arc::new(f));
    }

    pub fn is_closed(&self) -> bool {
        self.lock.read().is_closed
    }

    pub fn write_message(&self, msg: Message) -> Result<(), Error> {
        let inner = self.lock.read();
        if inner.is_closed {
            return Err(Error::ChannelClosed);
        }

        // Go panics when a closed channel is written to.
        let msg_chan = inner.msg_chan.as_ref().expect("send on closed channel");
        match msg_chan.try_send(msg) {
            // published
            Ok(()) => Ok(()),
            // channel is full
            Err(mpsc::error::TrySendError::Full(_)) => Err(Error::ChannelFull),
            // The reading end is gone. It is held by the channel itself, so this is not
            // reached.
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::ChannelClosed),
        }
    }

    pub fn read_chan(&self) -> ReadChan {
        self.read_chan.clone()
    }

    pub fn close(&self) {
        let mut inner = self.lock.write();
        if inner.is_closed {
            return;
        }
        inner.is_closed = true;
        inner.msg_chan = None;
        drop(inner);

        let on_close = self.on_close.lock().clone();
        if let Some(on_close) = on_close {
            on_close();
        }
    }

    pub fn connection_id(&self) -> livekit::ConnectionId {
        self.connection_id.clone()
    }
}

impl MessageSink for MessageChannel {
    fn write_message(&self, msg: Message) -> Result<(), Error> {
        MessageChannel::write_message(self, msg)
    }

    fn is_closed(&self) -> bool {
        MessageChannel::is_closed(self)
    }

    fn close(&self) {
        MessageChannel::close(self)
    }

    fn connection_id(&self) -> livekit::ConnectionId {
        MessageChannel::connection_id(self)
    }
}

impl MessageSource for MessageChannel {
    fn read_chan(&self) -> ReadChan {
        MessageChannel::read_chan(self)
    }

    fn is_closed(&self) -> bool {
        MessageChannel::is_closed(self)
    }

    fn close(&self) {
        MessageChannel::close(self)
    }

    fn connection_id(&self) -> livekit::ConnectionId {
        MessageChannel::connection_id(self)
    }
}

#[cfg(test)]
#[path = "messagechannel_test.rs"]
mod messagechannel_test;
