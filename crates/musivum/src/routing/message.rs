//! What travels through a participant's signalling, and the end of a channel it is read
//! from. Written new for musivum.
//!
//! Upstream a message is a `proto.Message`, any message of the protocol, and whoever is
//! handed one looks at what it is. Two kinds are ever written: what a client asks
//! (`*livekit.SignalRequest`, from the endpoint to the participant) and what it is told
//! (`*livekit.SignalResponse`, from the participant to the endpoint). [`Message`] is those
//! two. It is an enum and not a type parameter of the sinks and sources: there is one
//! `MessageSink` and one `MessageSource` as upstream, held as `Arc<dyn ..>` and faked for the
//! tests, and the code that takes a message still asks what it is and has its answer for the
//! wrong kind (`req, ok := msg.(*livekit.SignalRequest)` is a `match`).
//!
//! A message that may be absent, `nil` upstream, is an `Option<Message>`.

use std::fmt;
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::protocol::livekit;

/// Go's `proto.Message` where signalling is concerned: one of the two messages that are
/// written to a [`MessageSink`](super::MessageSink). The message is behind a pointer, as it
/// is in Go: `Message::from(request)` or `request.into()` makes one.
#[derive(Clone, PartialEq)]
pub enum Message {
    SignalRequest(Box<livekit::SignalRequest>),
    SignalResponse(Box<livekit::SignalResponse>),
}

impl Message {
    /// Go's `fmt.Sprintf("%T", msg)`: the Go type of the message, which the original logs
    /// for a message of the wrong kind.
    pub fn type_name(&self) -> &'static str {
        match self {
            Message::SignalRequest(_) => "*livekit.SignalRequest",
            Message::SignalResponse(_) => "*livekit.SignalResponse",
        }
    }
}

impl From<livekit::SignalRequest> for Message {
    fn from(msg: livekit::SignalRequest) -> Message {
        Message::SignalRequest(Box::new(msg))
    }
}

impl From<livekit::SignalResponse> for Message {
    fn from(msg: livekit::SignalResponse) -> Message {
        Message::SignalResponse(Box::new(msg))
    }
}

// What is logged is the message of the protocol.
impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::SignalRequest(msg) => fmt::Debug::fmt(msg, f),
            Message::SignalResponse(msg) => fmt::Debug::fmt(msg, f),
        }
    }
}

/// The reading end of a channel of messages: Go's `<-chan proto.Message`, which
/// [`MessageSource::read_chan`](super::MessageSource::read_chan) gives.
///
/// As upstream, every call of `read_chan` gives the same channel, and a message goes to one
/// of those who read it. A channel that was closed still gives the messages that were
/// written before; after the last of them a read gives `None`, where Go gives `nil`.
///
/// - A task awaits [`recv`](ReadChan::recv). It is cancel safe, so Go's `select` with other
///   channels is `tokio::select!` with other futures, and a `select` with a timer
///   `tokio::time::timeout`.
/// - A thread blocks on [`blocking_recv`](ReadChan::blocking_recv). A thread that has to
///   wait for something else at the same time runs a `select!` over `recv` with
///   `Handle::block_on`.
#[derive(Clone)]
pub struct ReadChan {
    // One reader at a time takes messages out; the others wait for the lock as, in Go, they
    // wait on the channel.
    rx: Arc<Mutex<mpsc::Receiver<Message>>>,
}

impl ReadChan {
    /// Go's `msg := <-ch`, for a task: the next message, or `None` when the channel is
    /// closed and empty.
    pub async fn recv(&self) -> Option<Message> {
        self.rx.lock().await.recv().await
    }

    /// Go's `msg := <-ch`, for a thread: blocks until there is a message, or returns `None`
    /// when the channel is closed and empty. Not for a thread of the runtime that is running
    /// a task: tokio panics there, and the task awaits [`recv`](ReadChan::recv).
    pub fn blocking_recv(&self) -> Option<Message> {
        self.rx.blocking_lock().blocking_recv()
    }
}

impl fmt::Debug for ReadChan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadChan")
    }
}

/// Go's `make(chan proto.Message, size)`: the writing end, which is written to with
/// `try_send` and closed by dropping it, and the reading end.
///
/// A size of 0 is taken as 1. In Go that is a channel without a buffer, on which a write
/// that does not wait succeeds only while a reader is waiting; tokio's channel always has
/// room for one message.
pub(super) fn make_chan(size: usize) -> (mpsc::Sender<Message>, ReadChan) {
    let (tx, rx) = mpsc::channel(size.max(1));
    (
        tx,
        ReadChan {
            rx: Arc::new(Mutex::new(rx)),
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::go::atomic;
    use crate::routing::{Error, MessageChannel, MessageSource, NullMessageSource};

    fn ping(n: i64) -> Message {
        livekit::SignalRequest {
            message: Some(livekit::signal_request::Message::Ping(n)),
        }
        .into()
    }

    // What a Go channel does and the callers count on: a full channel refuses a message, a
    // closed one too, and what was written before it was closed is still read, in order,
    // before the reader is told that the channel is closed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_task_reads_what_was_written_and_then_that_the_channel_is_closed() {
        let m = MessageChannel::new("test".into(), 2);
        let closed = Arc::new(atomic::Int32::new(0));
        let counter = closed.clone();
        m.on_close(move || {
            counter.inc();
        });

        assert_eq!(Ok(()), m.write_message(ping(1)));
        assert_eq!(Ok(()), m.write_message(ping(2)));
        assert_eq!(Err(Error::ChannelFull), m.write_message(ping(3)));

        let read_chan = m.read_chan();
        assert_eq!(Some(ping(1)), read_chan.recv().await);
        assert_eq!(Ok(()), m.write_message(ping(4)));

        assert!(!m.is_closed());
        m.close();
        m.close();
        assert!(m.is_closed());
        assert_eq!(1, closed.load());
        assert_eq!(Err(Error::ChannelClosed), m.write_message(ping(5)));

        // every `read_chan` is the same channel
        assert_eq!(Some(ping(2)), m.read_chan().recv().await);
        assert_eq!(Some(ping(4)), read_chan.recv().await);
        assert_eq!(None, read_chan.recv().await);
        assert_eq!(None, read_chan.recv().await);
    }

    // Go's `select` with a timer: a read of an open channel that nothing is written to
    // waits, and can be given up without losing what is written later.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_waits_for_a_message_and_can_be_given_up() {
        let m = MessageChannel::new_default("test".into());
        let read_chan = m.read_chan();
        let wait = Duration::from_millis(20);
        assert!(
            tokio::time::timeout(wait, read_chan.recv()).await.is_err(),
            "nothing was written"
        );
        assert_eq!(Ok(()), m.write_message(ping(1)));
        assert_eq!(Some(ping(1)), read_chan.recv().await);

        // the source that nothing is ever written to
        let source = NullMessageSource::new("test".into());
        let read_chan = source.read_chan();
        assert!(tokio::time::timeout(wait, read_chan.recv()).await.is_err());
        source.close();
        source.close();
        assert!(source.is_closed());
        assert_eq!(None, read_chan.recv().await);
    }
}
