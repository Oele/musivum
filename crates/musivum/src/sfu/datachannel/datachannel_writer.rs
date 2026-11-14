// Ported from LiveKit, pkg/sfu/datachannel/datachannel_writer.go at v1.13.11.
// Copyright LiveKit, Inc. (the original file has no notice of its own). Licensed under the
// Apache License, Version 2.0.
// Changes: translated from Go to Rust, and written against the data channel of musivum's
// connection layer where the original wraps a detached pion channel
// (`datachannel.ReadWriteCloserDeadliner`).
//
// Blocking with a deadline became buffered amount with a deadline. Upstream, when the
// transport has turned on pion's `EnableDataChannelBlockWrite` (it does when
// `DatachannelSlowThreshold` is above zero), `rawDC.Write` waits until what was written
// before has left SCTP's queue of data not sent yet, or until the deadline given to
// `SetWriteDeadline`, and then fails with `context.DeadlineExceeded` and has written
// nothing. A channel of musivum's layer never waits in `send`: what the other side has not
// acknowledged is counted in `buffered_amount`, and a callback tells when that has come
// down to a threshold. So the waiting is done here, in `write_with_deadline`, which stands
// where the original has `SetWriteDeadline` and `Write`: a write goes to the channel when
// the buffered amount is at or below the threshold the writer was made with, waits for the
// callback otherwise, and fails with `Error::DeadlineExceeded`, nothing sent, when the
// deadline comes first. A writer made without a threshold sends at once, as pion writes
// when block write is off. What is done with the outcome (the bitrate of what drained, the
// retries while the reader is not slow, the drop by latency) is the original's, line by line.
//
// What else differs:
// - `bufferGetter` and `rawDC` are one object, the channel, so the constructors take it
//   once, and in the place of `rawDC` whether writes wait and at what buffered amount
//   (`block_write`). `RawDataChannel` is what the writer needs of a channel; it is
//   implemented for the layer's `DataChannel` at the end of this file.
// - pion looks at the queue of the whole SCTP association (a write on one channel waits for
//   what another channel has pending) and at "sent once"; here it is the channel's own bytes
//   and "acknowledged", against a fixed threshold.
// - The writer takes the channel's low-threshold callback and its threshold for itself.
// - `Write` gives a count or an error: the original's count is 0 with every error.
// - The errors are the package's `Error`; the two `ErrX` carry the numbers the original
//   puts in the text it wraps them in.
// - After `close` a write does not wait: pion fails a write on a closed stream at once.
// - `write` blocks the calling thread, as the original blocks its goroutine. It must not
//   be called where it may wait from a callback of the channel's own connection: that
//   connection's task is what takes in the acknowledgements.

use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use super::bitrate::{BITRATE_DURATION, BITRATE_WINDOW, BitrateCalculator, BitrateMode};
use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::utils::mono;
use crate::webrtc;

const SINGLE_WRITE_TIMEOUT: Duration = Duration::from_millis(50);

/// The buffered amount up to which a writer that waits lets a write through, for a caller
/// that has no number of its own: 64 KiB, a usual low-water mark of a data channel. pion has
/// no such number (see the header); the smaller it is, the earlier a slow reader holds up
/// the writer.
pub const BLOCK_WRITE_BUFFERED_AMOUNT: u64 = 64 * 1024;

/// The errors of the package: its `ErrX` values with the original's messages, and what a
/// write to the channel can fail with.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// ErrDataDroppedBySlowReader, as the original wraps it.
    #[error("data dropped by slow reader: bitrate {bitrate}, threshold {threshold}")]
    DataDroppedBySlowReader { bitrate: i64, threshold: i64 },
    /// ErrDataDroppedByHighBufferedAmount, as the original wraps it.
    #[error(
        "data dropped due to high buffered amount: bitrate {bitrate}, buffered amount {buffered_amount}, target latency limit {target_latency_limit}, min buffered amount {min_buffered_amount}"
    )]
    DataDroppedByHighBufferedAmount {
        bitrate: i64,
        buffered_amount: u64,
        target_latency_limit: u64,
        min_buffered_amount: u64,
    },
    /// Go's `context.DeadlineExceeded`, which a write that could not go out in time fails
    /// with.
    #[error("context deadline exceeded")]
    DeadlineExceeded,
    /// What the channel said to a write or to closing it.
    #[error(transparent)]
    Channel(#[from] webrtc::Error),
}

pub trait BufferedAmountGetter {
    fn buffered_amount(&self) -> u64;
}

/// What the writer needs of a data channel, in the place of pion's detached channel
/// (`datachannel.ReadWriteCloserDeadliner`): a `send` that never waits, and to be told when
/// the buffered amount is low again.
pub trait RawDataChannel: BufferedAmountGetter + Send + Sync {
    /// Sends a message. It does not wait: what cannot go out yet is counted in
    /// `buffered_amount`.
    fn send(&self, data: &[u8]) -> Result<(), webrtc::Error>;

    /// Sets the buffered amount that counts as low.
    fn set_buffered_amount_low_threshold(&self, th: u64);

    /// Sets the function that is called when the buffered amount has come down to the low
    /// threshold from above it.
    fn on_buffered_amount_low(&self, f: Box<dyn Fn() + Send + Sync>);

    fn close(&self) -> Result<(), webrtc::Error>;
}

pub struct DataChannelWriter<T: RawDataChannel> {
    // `bufferGetter` and `rawDC` upstream
    raw_dc: Arc<T>,
    // the buffered amount up to which a write goes out, when writes wait
    block_write: Option<u64>,
    buffered_amount_low: Arc<BufferedAmountLow>,
    closed: atomic::Bool,
    slow_threshold: i64,
    rate: Option<BitrateCalculator>,
    reliable: bool,
    target_latency: Duration,
    min_buffered_amount: u64,
}

impl<T: RawDataChannel> DataChannelWriter<T> {
    // NewDataChannelWriterReliable creates a new DataChannelWriter for reliable data channel by
    // detaching it, when writing to the datachanel times out, it will block and retry if the
    // receiver's bitrate is above the slowThreshold or drop the data if it's below the threshold.
    // If the slowThreshold is 0, it will never retry on write timeout.
    //
    // `block_write` is pion's `EnableDataChannelBlockWrite`, which upstream's transport sets
    // when `DatachannelSlowThreshold` is above zero: `None` for writes that go out at once,
    // `Some(buffered amount)` for writes that wait until the channel has no more than that
    // buffered (`BLOCK_WRITE_BUFFERED_AMOUNT`).
    pub fn new_reliable(raw_dc: Arc<T>, block_write: Option<u64>, slow_threshold: i64) -> Self {
        let mut rate = None;
        if slow_threshold > 0 {
            rate = Some(BitrateCalculator::new(
                BITRATE_DURATION,
                BITRATE_WINDOW,
                BitrateMode::BusyOnly,
            ));
        }
        Self {
            buffered_amount_low: BufferedAmountLow::watch(&*raw_dc, block_write),
            raw_dc,
            block_write,
            closed: atomic::Bool::new(false),
            slow_threshold,
            rate,
            reliable: true,
            target_latency: Duration::ZERO,
            min_buffered_amount: 0,
        }
    }

    // NewDataChannelWriterUnreliable creates a new DataChannelWriter for unreliable data channel.
    // It will drop data when the buffered amount is too high to maintain the target latency.
    // The latency is estimated based on the bitrate in past 1 second. If targetLatency is 0, no
    // buffering control is applied.
    //
    // `block_write` is as for `new_reliable`.
    pub fn new_unreliable(
        raw_dc: Arc<T>,
        block_write: Option<u64>,
        target_latency: Duration,
        min_buffered_amount: u64,
    ) -> Self {
        let mut rate = None;
        if target_latency > Duration::ZERO {
            rate = Some(BitrateCalculator::new(
                BITRATE_DURATION,
                BITRATE_WINDOW,
                BitrateMode::BusyOnly,
            ));
        }
        Self {
            buffered_amount_low: BufferedAmountLow::watch(&*raw_dc, block_write),
            raw_dc,
            block_write,
            closed: atomic::Bool::new(false),
            slow_threshold: 0,
            rate,
            target_latency,
            min_buffered_amount,
            reliable: false,
        }
    }

    pub fn buffered_amount_getter(&self) -> &Arc<T> {
        &self.raw_dc
    }

    pub fn write(&self, p: &[u8]) -> Result<usize, Error> {
        if self.reliable {
            self.write_reliable(p)
        } else {
            self.write_unreliable(p)
        }
    }

    fn write_reliable(&self, p: &[u8]) -> Result<usize, Error> {
        loop {
            let res = self.write_with_deadline(p, time::now().add(SINGLE_WRITE_TIMEOUT));
            if self.slow_threshold == 0 {
                return res;
            }

            let now = mono::now();
            let n = res.as_ref().map_or(0, |n| *n);
            self.rate()
                .add_bytes(n as i64, self.raw_dc.buffered_amount() as i64, now);
            // retry if the write timed out on a non-slow receiver
            if res == Err(Error::DeadlineExceeded) {
                let (bitrate, ok) = self.rate().bitrate(now);
                if !ok || bitrate >= self.slow_threshold {
                    continue;
                } else {
                    return Err(Error::DataDroppedBySlowReader {
                        bitrate,
                        threshold: self.slow_threshold,
                    });
                }
            }

            return res;
        }
    }

    fn write_unreliable(&self, p: &[u8]) -> Result<usize, Error> {
        if self.target_latency == Duration::ZERO {
            return self.write_with_deadline(p, time::now().add(SINGLE_WRITE_TIMEOUT));
        }

        let (bitrate, ok) = self.rate().bitrate(time::now());
        if ok {
            // control buffer latency to target
            let buffered_amount = self.raw_dc.buffered_amount();
            let target_latency_limit = (bitrate.wrapping_mul(self.target_latency.nanoseconds())
                / 8
                / time::SECOND.nanoseconds()) as u64;
            if buffered_amount > target_latency_limit && buffered_amount > self.min_buffered_amount
            {
                return Err(Error::DataDroppedByHighBufferedAmount {
                    bitrate,
                    buffered_amount,
                    target_latency_limit,
                    min_buffered_amount: self.min_buffered_amount,
                });
            }
        }

        let res = self.write_with_deadline(p, time::now().add(SINGLE_WRITE_TIMEOUT));
        // Track drained bytes on every write (not just failures) so the bitrate
        // calculator has samples; otherwise Bitrate() has no data and the
        // latency-based drop threshold above collapses to the static minBufferedAmount.
        let n = res.as_ref().map_or(0, |n| *n);
        self.rate()
            .add_bytes(n as i64, self.raw_dc.buffered_amount() as i64, mono::now());

        res
    }

    pub fn close(&self) -> Result<(), Error> {
        self.closed.store(true);
        self.buffered_amount_low.notify();
        Ok(self.raw_dc.close()?)
    }

    // The original's `w.rate`, where it uses it without looking: the calculator is made when
    // the slow threshold (of a reliable writer) or the target latency (of an unreliable one)
    // is above zero, and used when it is not zero. With a value below zero the original
    // calls a method of a nil calculator and panics. So does this.
    fn rate(&self) -> &BitrateCalculator {
        self.rate
            .as_ref()
            .expect("a slow threshold or a target latency below zero has no bitrate calculator")
    }

    // `rawDC.SetWriteDeadline(deadline)` and `rawDC.Write(p)` of the original, see the header:
    // sends `p` when the channel has room for it, or when it comes to have room before the
    // deadline.
    fn write_with_deadline(&self, p: &[u8], deadline: Time) -> Result<usize, Error> {
        if let Some(threshold) = self.block_write {
            let has_room = || self.closed.load() || self.raw_dc.buffered_amount() <= threshold;
            if !self.buffered_amount_low.wait(has_room, deadline) {
                return Err(Error::DeadlineExceeded);
            }
        }
        self.raw_dc.send(p)?;
        Ok(p.len())
    }
}

// The calls of a channel's low-threshold callback, counted, for the writes that wait for one.
#[derive(Default)]
struct BufferedAmountLow {
    count: Mutex<u64>,
    cond: Condvar,
}

impl BufferedAmountLow {
    // Has the channel tell when its buffered amount has come down to the threshold, if
    // writes are to wait for that.
    fn watch<T: RawDataChannel + ?Sized>(raw_dc: &T, block_write: Option<u64>) -> Arc<Self> {
        let low = Arc::new(Self::default());
        if let Some(threshold) = block_write {
            raw_dc.set_buffered_amount_low_threshold(threshold);
            raw_dc.on_buffered_amount_low(Box::new({
                let low = low.clone();
                move || low.notify()
            }));
        }
        low
    }

    fn notify(&self) {
        *self.count.lock() += 1;
        self.cond.notify_all();
    }

    // Waits until `has_room` says yes, asking again each time the channel has told, and no
    // longer than until `deadline`. Whether there was room in the end.
    fn wait(&self, has_room: impl Fn() -> bool, deadline: Time) -> bool {
        loop {
            // The count is read before the channel is asked, and the channel is asked with
            // no lock held: a callback that comes in between is not lost.
            let seen = *self.count.lock();
            if has_room() {
                return true;
            }
            let mut count = self.count.lock();
            while *count == seen {
                if self
                    .cond
                    .wait_until(&mut count, deadline.to_instant())
                    .timed_out()
                {
                    drop(count);
                    return has_room();
                }
            }
        }
    }
}

impl BufferedAmountGetter for webrtc::peerconnection::DataChannel {
    fn buffered_amount(&self) -> u64 {
        webrtc::peerconnection::DataChannel::buffered_amount(self)
    }
}

impl RawDataChannel for webrtc::peerconnection::DataChannel {
    fn send(&self, data: &[u8]) -> Result<(), webrtc::Error> {
        webrtc::peerconnection::DataChannel::send(self, data)
    }

    fn set_buffered_amount_low_threshold(&self, th: u64) {
        webrtc::peerconnection::DataChannel::set_buffered_amount_low_threshold(self, th);
    }

    fn on_buffered_amount_low(&self, f: Box<dyn Fn() + Send + Sync>) {
        webrtc::peerconnection::DataChannel::on_buffered_amount_low(self, f);
    }

    fn close(&self) -> Result<(), webrtc::Error> {
        webrtc::peerconnection::DataChannel::close(self)
    }
}

#[cfg(test)]
#[path = "datachannel_writer_test.rs"]
mod datachannel_writer_test;
