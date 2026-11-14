// Ported from LiveKit, pkg/sfu/datachannel/datachannel_writer_test.go at v1.13.11.
// Copyright LiveKit, Inc. (the original file has no notice of its own). Licensed under the
// Apache License, Version 2.0.
// Changes: translated from Go to Rust; the mocks are channels that do not block, as those of
// musivum's layer do not. The original's `mockDataChannelWriter` holds a `Write` until
// `nextWriteCompleteAt` or the deadline; here the channel is taken until
// `next_write_complete_at` (it reports a byte that has not drained), drains then and tells
// its low-threshold callback, and the waiting and the deadline are the writer's, which is
// made to wait for a buffered amount of 0 as the original's mock makes it wait for its own
// write. The mock counts what it was sent, so that the original's "0 bytes written" can be
// asserted of a write that failed. The three tests of the original are followed by tests
// that are new and say so.

use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::go::atomic;

#[test]
fn test_data_channel_writer() {
    let mock_dc = MockDataChannelWriter::new();
    // slow threshold is 1000B/s
    let w = DataChannelWriter::new_reliable(mock_dc.clone(), Some(0), 8000);
    assert!(Arc::ptr_eq(&mock_dc, w.buffered_amount_getter()));
    let buf = vec![0u8; 2000];
    // write 2000 bytes so it should not drop in 2 seconds
    let t0 = time::now();
    let res = w.write(&buf);
    assert_eq!(Ok(2000), res);

    let t1 = time::now();
    mock_dc.set_next_write_complete_at(t0.add(time::SECOND * 3 / 2));
    let res = w.write(&buf[..10]);
    assert_eq!(Ok(10), res);
    assert!(time::since(t1) >= time::SECOND);

    // bitrate below slow threshold(2000bytes/3sec), should drop by timeout
    mock_dc.set_next_write_complete_at(t0.add(3 * time::SECOND));
    let res = w.write(&buf[..1000]);
    assert!(
        matches!(res, Err(Error::DataDroppedBySlowReader { .. })),
        "{res:?}"
    );
    // require.Equal(t, 0, n)
    assert_eq!(2010, mock_dc.written.load());
}

#[test]
fn test_data_channel_writer_no_slow_threshold() {
    let mock_dc = MockDataChannelWriter::new();
    let w = DataChannelWriter::new_reliable(mock_dc.clone(), Some(0), 0);
    let buf = vec![0u8; 2000];
    let res = w.write(&buf);
    assert_eq!(Ok(2000), res);
    mock_dc.set_next_write_complete_at(time::now().add(SINGLE_WRITE_TIMEOUT / 2));
    let res = w.write(&buf[..10]);
    assert_eq!(Ok(10), res);

    // slow threshold is 0, should not block & retry
    mock_dc.set_next_write_complete_at(time::now().add(SINGLE_WRITE_TIMEOUT * 2));
    let res = w.write(&buf[..1000]);
    assert_eq!(Err(Error::DeadlineExceeded), res);
    // require.Equal(t, 0, n)
    assert_eq!(2010, mock_dc.written.load());
}

#[test]
fn test_data_channel_writer_unreliable() {
    let mock_dc = MockLossyDataChannelWriter::new(8192);
    let w = DataChannelWriter::new_unreliable(mock_dc.clone(), None, 100 * time::MILLISECOND, 2000);
    for _ in 0..10 {
        let buf = vec![0u8; 128];
        let res = w.write(&buf);
        assert_eq!(Ok(128), res);
        std::thread::sleep((100 * time::MILLISECOND).to_std());
    }
    let buf = vec![0u8; 4096];
    let res = w.write(&buf);
    assert_eq!(Ok(4096), res);
    // should drop due to high buffered amount
    let res = w.write(&buf);
    assert!(
        matches!(res, Err(Error::DataDroppedByHighBufferedAmount { .. })),
        "{res:?}"
    );
}

// mockDataChannelWriter
#[derive(Default)]
struct MockDataChannelWriter {
    next_write_complete_at: Mutex<Time>,
    buffered: atomic::Int64,
    // New: the timer that drains the channel at `next_write_complete_at`, what the writer
    // set, and the bytes that were sent.
    drain: Mutex<Option<time::Timer>>,
    buffered_amount_low_threshold: atomic::Uint64,
    on_buffered_amount_low: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    written: atomic::Int64,
    closed: atomic::Bool,
}

impl MockDataChannelWriter {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_next_write_complete_at(self: &Arc<Self>, t: Time) {
        *self.next_write_complete_at.lock() = t;

        // The original's mock holds the next write until `t`, with the write counted as
        // buffered. A channel that does not block shows the same to a writer by not having
        // drained: one byte stays buffered until `t`.
        let wait = time::until(t);
        if wait <= Duration::ZERO {
            return;
        }
        self.buffered.store(1);
        let m = self.clone();
        let drain = time::after_func(wait, move || m.set_buffered(0));
        if let Some(earlier) = self.drain.lock().replace(drain) {
            earlier.stop();
        }
    }

    // New: sets what is buffered, and tells the callback when that is low.
    fn set_buffered(&self, buffered: i64) {
        self.buffered.store(buffered);
        if buffered as u64 <= self.buffered_amount_low_threshold.load() {
            let f = self.on_buffered_amount_low.lock().clone();
            if let Some(f) = f {
                f();
            }
        }
    }
}

impl BufferedAmountGetter for MockDataChannelWriter {
    fn buffered_amount(&self) -> u64 {
        self.buffered.load() as u64
    }
}

impl RawDataChannel for MockDataChannelWriter {
    // `Write` of the original's mock, without its waiting.
    fn send(&self, b: &[u8]) -> Result<(), webrtc::Error> {
        if self.closed.load() {
            return Err(webrtc::Error::ClosedPipe);
        }
        self.written.add(b.len() as i64);
        let wait = time::until(*self.next_write_complete_at.lock());
        if wait <= Duration::ZERO {
            // the write completes at once: nothing of it stays buffered
            return Ok(());
        }
        // buffered while the write is in flight, cleared once it completes
        self.buffered.add(b.len() as i64);
        Ok(())
    }

    fn set_buffered_amount_low_threshold(&self, th: u64) {
        self.buffered_amount_low_threshold.store(th);
    }

    fn on_buffered_amount_low(&self, f: Box<dyn Fn() + Send + Sync>) {
        *self.on_buffered_amount_low.lock() = Some(Arc::from(f));
    }

    fn close(&self) -> Result<(), webrtc::Error> {
        self.closed.store(true);
        Ok(())
    }
}

// mockLossyDataChannelWriter
struct MockLossyDataChannelWriter {
    buffered_amount: atomic::Int64,
    target_bitrate: i64,
    last_write_at: Mutex<Time>,
}

impl MockLossyDataChannelWriter {
    fn new(target_bitrate: i64) -> Arc<Self> {
        Arc::new(Self {
            buffered_amount: atomic::Int64::default(),
            target_bitrate,
            last_write_at: Mutex::new(time::now()),
        })
    }
}

impl BufferedAmountGetter for MockLossyDataChannelWriter {
    fn buffered_amount(&self) -> u64 {
        self.buffered_amount.load() as u64
    }
}

impl RawDataChannel for MockLossyDataChannelWriter {
    fn send(&self, b: &[u8]) -> Result<(), webrtc::Error> {
        let mut last_write_at = self.last_write_at.lock();
        self.buffered_amount.add(b.len() as i64);
        if time::now().before(*last_write_at) {
            return Ok(());
        }

        // drain buffer based on target bitrate
        let can_write_bytes = time::since(*last_write_at)
            .nanoseconds()
            .wrapping_mul(self.target_bitrate)
            / time::SECOND.nanoseconds()
            / 8;
        if self.buffered_amount.load() <= can_write_bytes {
            *last_write_at = last_write_at.add(Duration::from_nanos(
                time::SECOND.nanoseconds() * self.buffered_amount() as i64
                    / (self.target_bitrate / 8),
            ));
            self.buffered_amount.store(0);
        } else {
            *last_write_at = time::now();
            self.buffered_amount.add(-can_write_bytes);
        }
        Ok(())
    }

    // The original's `SetWriteDeadline` does nothing; neither does a writer that was made
    // not to wait call these.
    fn set_buffered_amount_low_threshold(&self, _th: u64) {}

    fn on_buffered_amount_low(&self, _f: Box<dyn Fn() + Send + Sync>) {}

    fn close(&self) -> Result<(), webrtc::Error> {
        Ok(())
    }
}

// New for musivum: a writer made without a threshold never waits, whatever the channel has
// buffered. That is pion without block write, which is what upstream runs with when no slow
// threshold is configured.
#[test]
fn a_writer_that_does_not_wait_sends_at_once() {
    let mock_dc = MockDataChannelWriter::new();
    mock_dc.set_buffered(10_000_000);
    let buf = vec![0u8; 1000];

    let w = DataChannelWriter::new_reliable(mock_dc.clone(), None, 0);
    let t0 = time::now();
    assert_eq!(Ok(1000), w.write(&buf));
    let w = DataChannelWriter::new_unreliable(mock_dc.clone(), None, Duration::ZERO, 0);
    assert_eq!(Ok(1000), w.write(&buf));
    assert!(time::since(t0) < SINGLE_WRITE_TIMEOUT);
    assert_eq!(2000, mock_dc.written.load());
    // the channel was left as it was
    assert_eq!(0, mock_dc.buffered_amount_low_threshold.load());
    assert!(mock_dc.on_buffered_amount_low.lock().is_none());

    // what the channel says to a write, and to closing, is passed on
    assert_eq!(Ok(()), w.close());
    assert_eq!(
        Err(Error::Channel(webrtc::Error::ClosedPipe)),
        w.write(&buf)
    );
}

// New for musivum: a write waits while the channel has more buffered than the threshold the
// writer was made with, goes out when the channel tells that it is at the threshold, and
// fails at the deadline when it is not told. An unreliable writer waits the same way.
#[test]
fn a_write_waits_for_the_buffered_amount_to_come_down_to_the_threshold() {
    let mock_dc = MockDataChannelWriter::new();
    let w = DataChannelWriter::new_unreliable(mock_dc.clone(), Some(4096), Duration::ZERO, 0);
    assert_eq!(4096, mock_dc.buffered_amount_low_threshold.load());
    let buf = vec![0u8; 100];

    // at the threshold: at once
    mock_dc.set_buffered(4096);
    let t0 = time::now();
    assert_eq!(Ok(100), w.write(&buf));
    assert!(time::since(t0) < SINGLE_WRITE_TIMEOUT);

    // above it: when the channel has drained to it
    mock_dc.set_buffered(4097);
    let drain = {
        let mock_dc = mock_dc.clone();
        std::thread::spawn(move || {
            std::thread::sleep((SINGLE_WRITE_TIMEOUT / 10).to_std());
            // not far enough: the writer looks, and goes on waiting
            mock_dc.buffered.store(4097);
            (mock_dc.on_buffered_amount_low.lock().clone().unwrap())();
            std::thread::sleep((SINGLE_WRITE_TIMEOUT / 10).to_std());
            mock_dc.set_buffered(4096);
        })
    };
    let t0 = time::now();
    assert_eq!(Ok(100), w.write(&buf));
    let waited = time::since(t0);
    assert!(waited >= SINGLE_WRITE_TIMEOUT / 5, "{waited}");
    drain.join().unwrap();

    // above it for longer than the deadline
    mock_dc.set_buffered(4097);
    let t0 = time::now();
    assert_eq!(Err(Error::DeadlineExceeded), w.write(&buf));
    assert!(time::since(t0) >= SINGLE_WRITE_TIMEOUT);
    assert_eq!(200, mock_dc.written.load());

    // a writer that was closed does not wait: the channel says what is the matter
    assert_eq!(Ok(()), w.close());
    let t0 = time::now();
    assert_eq!(
        Err(Error::Channel(webrtc::Error::ClosedPipe)),
        w.write(&buf)
    );
    assert!(time::since(t0) < SINGLE_WRITE_TIMEOUT);
}

// New for musivum: the texts of the errors are the original's, as a log shows them.
#[test]
fn the_errors_have_the_texts_of_the_original() {
    assert_eq!(
        "data dropped by slow reader: bitrate 7996, threshold 8000",
        Error::DataDroppedBySlowReader {
            bitrate: 7996,
            threshold: 8000
        }
        .to_string()
    );
    assert_eq!(
        "data dropped due to high buffered amount: bitrate 8160, buffered amount 4356, target latency limit 102, min buffered amount 2000",
        Error::DataDroppedByHighBufferedAmount {
            bitrate: 8160,
            buffered_amount: 4356,
            target_latency_limit: 102,
            min_buffered_amount: 2000
        }
        .to_string()
    );
    assert_eq!(
        "context deadline exceeded",
        Error::DeadlineExceeded.to_string()
    );
    assert_eq!(
        "io: read/write on closed pipe",
        Error::Channel(webrtc::Error::ClosedPipe).to_string()
    );
}

// New for musivum: the writer on channels of the connection layer, between two connections of
// this process on the loopback interface. Without the wait every write is with the channel
// at once and piles up there; with it a write goes to the channel only when what went before
// has drained to the threshold, which the channel tells the writer beside its own handler;
// and a write to a channel that was closed fails with pion/sctp's "stream closed", which is
// what upstream's transport acts on.
#[test]
fn test_data_channel_writer_over_a_connection() {
    use std::collections::HashMap;

    use bytes::Bytes;

    use crate::testutils::eventually;
    use crate::webrtc::peerconnection::{
        Configuration, DataChannel, DataChannelInit, DataChannelState, DtlsRole, Node, NodeConfig,
        PeerConnection, SettingEngine,
    };

    const WAIT: std::time::Duration = std::time::Duration::from_secs(10);
    const TICK: std::time::Duration = std::time::Duration::from_millis(2);
    const SIZE: usize = 4096;
    const COUNT: usize = 64;

    // A server as musivum is run, and a client on a socket of its own.
    let server = PeerConnection::new(
        &Node::new(NodeConfig::default()).unwrap(),
        Configuration {
            setting_engine: SettingEngine {
                lite: true,
                answering_dtls_role: DtlsRole::Server,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let client = PeerConnection::new(
        &Node::new(NodeConfig::default()).unwrap(),
        Configuration::default(),
    )
    .unwrap();

    // What arrives at the client, by the label of the channel.
    let received: Arc<Mutex<HashMap<String, Vec<Bytes>>>> = Arc::default();
    let incoming: Arc<Mutex<Vec<Arc<DataChannel>>>> = Arc::default();
    client.on_data_channel({
        let received = received.clone();
        let incoming = incoming.clone();
        move |channel| {
            let label = channel.label().to_string();
            let received = received.clone();
            channel.on_message(move |message| {
                received
                    .lock()
                    .entry(label.clone())
                    .or_default()
                    .push(message.data);
            });
            incoming.lock().push(channel);
        }
    });
    let count = |label: &str| received.lock().get(label).map_or(0, Vec::len);

    // LiveKit's channels, made by the server before its first offer.
    let reliable = server.create_data_channel("_reliable", None).unwrap();
    let lossy = server
        .create_data_channel(
            "_lossy",
            Some(DataChannelInit {
                ordered: Some(false),
                max_retransmits: Some(0),
                ..Default::default()
            }),
        )
        .unwrap();
    // The application's own handler for the low threshold of the reliable channel.
    let told = Arc::new(atomic::Int64::default());
    reliable.on_buffered_amount_low({
        let told = told.clone();
        move || {
            told.add(1);
        }
    });

    let offer = server.create_offer(None).unwrap();
    server.set_local_description(offer).unwrap();
    client
        .set_remote_description(server.local_description().unwrap())
        .unwrap();
    let answer = client.create_answer().unwrap();
    client.set_local_description(answer).unwrap();
    server
        .set_remote_description(client.local_description().unwrap())
        .unwrap();
    eventually(
        || {
            reliable.ready_state() == DataChannelState::Open
                && lossy.ready_state() == DataChannelState::Open
                && incoming.lock().len() == 2
        },
        WAIT,
        TICK,
    );

    let message = |i: usize| vec![i as u8; SIZE];

    // Without the wait: every write is taken at once, and what cannot go out yet is the
    // channel's to keep.
    let w = DataChannelWriter::new_reliable(reliable.clone(), None, 0);
    let mut most_buffered = 0;
    for i in 0..COUNT {
        assert_eq!(Ok(SIZE), w.write(&message(i)));
        most_buffered = most_buffered.max(reliable.buffered_amount());
    }
    assert!(most_buffered > 2 * SIZE as u64, "{most_buffered}");
    eventually(|| count("_reliable") == COUNT, WAIT, TICK);
    eventually(|| reliable.buffered_amount() == 0, WAIT, TICK);
    // The writer did not touch the channel's threshold or its handler.
    assert_eq!(0, reliable.buffered_amount_low_threshold());

    // An unreliable writer on the lossy channel, also without the wait.
    let lossy_writer = DataChannelWriter::new_unreliable(lossy.clone(), None, Duration::ZERO, 0);
    for i in 0..10 {
        assert_eq!(Ok(100), lossy_writer.write(&[i as u8; 100]));
    }
    eventually(|| count("_lossy") == 10, WAIT, TICK);

    // With the wait, for a buffered amount of 0: a write goes to the channel when all that
    // went before has left it, so there is never more than one message there. The writer is
    // told when that is so: a write that was not told waits for its 50 ms and looks again,
    // which on a busy machine may happen to a few and not to most.
    let w = DataChannelWriter::new_reliable(reliable.clone(), Some(0), 8000);
    let mut not_told = 0;
    for i in COUNT..2 * COUNT {
        let t0 = time::now();
        assert_eq!(Ok(SIZE), w.write(&message(i)));
        if time::since(t0) >= SINGLE_WRITE_TIMEOUT {
            not_told += 1;
        }
        let buffered = reliable.buffered_amount();
        assert!(buffered <= SIZE as u64, "{buffered}");
    }
    assert!(not_told < COUNT / 2, "{not_told}");
    eventually(|| count("_reliable") == 2 * COUNT, WAIT, TICK);
    // All of it arrived, in the order it was written.
    {
        let received = received.lock();
        for (i, data) in received["_reliable"].iter().enumerate() {
            assert_eq!(&message(i)[..], &data[..], "message {i}");
        }
        let mut firsts: Vec<u8> = received["_lossy"].iter().map(|data| data[0]).collect();
        firsts.sort();
        assert_eq!((0..10).collect::<Vec<u8>>(), firsts);
    }
    // The channel told its own handler too: the writer listens beside it.
    eventually(|| told.load() >= 1, WAIT, TICK);

    // A writer that was closed has closed its channel: a write fails with what the channel
    // says, at once.
    assert_eq!(Ok(()), w.close());
    let t0 = time::now();
    assert_eq!(
        Err(Error::Channel(webrtc::Error::StreamClosed)),
        w.write(&message(0))
    );
    assert!(time::since(t0) < SINGLE_WRITE_TIMEOUT);
    assert_eq!("stream closed", webrtc::Error::StreamClosed.to_string());

    // And so does a write to a channel whose connection was closed.
    server.close().unwrap();
    assert_eq!(
        Err(Error::Channel(webrtc::Error::StreamClosed)),
        lossy_writer.write(&message(0))
    );
    client.close().unwrap();
}
