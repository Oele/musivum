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
