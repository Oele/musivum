// Ported from LiveKit, pkg/sfu/buffer/rtcpreader.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the reader is made as an `Arc`, as the factory and a
// down track both hold it; `onClose`, a field that the original writes without a lock, has a
// lock of its own, and closing a reader that was not given one does nothing where the
// original would panic on the nil function; `io.EOF` is `Error::Eof`; `Read`, which reads
// nothing and is there to make the reader an `io.ReadWriteCloser` for pion, has no
// counterpart.

use std::sync::Arc;

use parking_lot::Mutex;

use super::Error;
use crate::go::atomic;

type OnPacket = Arc<dyn Fn(&[u8]) + Send + Sync>;
type OnClose = Arc<dyn Fn() + Send + Sync>;

pub struct RtcpReader {
    #[allow(dead_code)] // not read upstream either
    ssrc: u32,
    closed: atomic::Bool,
    on_packet: atomic::Value<OnPacket>, // func([]byte)
    on_close: Mutex<Option<OnClose>>,
}

impl RtcpReader {
    pub fn new(ssrc: u32) -> Arc<Self> {
        Arc::new(Self {
            ssrc,
            closed: atomic::Bool::new(false),
            on_packet: atomic::Value::new(),
            on_close: Mutex::new(None),
        })
    }

    #[inline(never)]
    pub fn write(&self, p: &[u8]) -> Result<usize, Error> {
        if self.closed.load() {
            return Err(Error::Eof);
        }
        if let Some(f) = self.on_packet.load() {
            f(p);
        }
        Ok(0)
    }

    pub fn on_close(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.on_close.lock() = Some(Arc::new(f));
    }

    pub fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true) {
            return Ok(());
        }

        let on_close = self.on_close.lock().clone();
        if let Some(on_close) = on_close {
            on_close();
        }
        Ok(())
    }

    pub fn on_packet(&self, f: impl Fn(&[u8]) + Send + Sync + 'static) {
        self.on_packet.store(Arc::new(f));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    // New for musivum: the original has no test of this file.
    #[test]
    fn packets_reach_the_callback_until_the_reader_is_closed() {
        let r = RtcpReader::new(1234);

        // without a callback a packet goes nowhere, and that is not an error
        assert_eq!(Ok(0), r.write(&[1, 2, 3]));

        let packets = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let p = packets.clone();
        r.on_packet(move |pkt| p.lock().push(pkt.to_vec()));
        let closes = Arc::new(AtomicUsize::new(0));
        let c = closes.clone();
        r.on_close(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        // the count of bytes written is zero in the original too
        assert_eq!(Ok(0), r.write(&[0x80, 0xc9, 0x00, 0x01]));
        assert_eq!(vec![vec![0x80, 0xc9, 0x00, 0x01]], *packets.lock());

        // a callback set later takes the place of the first
        let later = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let l = later.clone();
        r.on_packet(move |pkt| l.lock().push(pkt.to_vec()));
        assert_eq!(Ok(0), r.write(&[0x81]));
        assert_eq!(1, packets.lock().len());
        assert_eq!(vec![vec![0x81]], *later.lock());

        assert_eq!(Ok(()), r.close());
        assert_eq!(1, closes.load(Ordering::SeqCst));
        assert_eq!(Err(Error::Eof), r.write(&[0x82]));
        assert_eq!(1, later.lock().len());

        // closing again does not call back again
        assert_eq!(Ok(()), r.close());
        assert_eq!(1, closes.load(Ordering::SeqCst));

        // a reader that was not told what to do on close closes all the same
        let r = RtcpReader::new(5678);
        assert_eq!(Ok(()), r.close());
        assert_eq!(Err(Error::Eof), r.write(&[0x83]));
    }
}
