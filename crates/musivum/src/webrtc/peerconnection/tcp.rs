//! ICE over TCP (RFC 6544): what a stream carries and how it is read and written. Which
//! connection a stream belongs to is the node's business (`node.rs`).
//!
//! On a stream every packet (STUN, DTLS, SRTP and SRTCP, as on UDP) is a frame: two octets of
//! length in network order, then the packet (RFC 4571). The limits are pion's TCP mux's: the
//! first frame of a stream that was accepted, which has to be a binding request that names its
//! connection, is read into 512 octets, every later one into pion's receive MTU of 8192; a
//! frame that is longer ends the stream. What a connection writes goes into a buffer of
//! 4 MiB per stream (what LiveKit gives pion's TCP mux, `writeBufferSizeInBytes`), which a
//! task of the stream empties; a packet that does not fit is dropped, as pion's buffered
//! connection drops it.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Buf, Bytes, BytesMut};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The port of an active candidate: it dials from a port of the system's choosing, and says 9
/// (discard) in its place, as RFC 6544 asks (section 4.5).
pub(crate) const ACTIVE_TCP_PORT: u16 = 9;

/// The longest first frame of a stream that was accepted (pion's `TCPMuxDefault.handleConn`).
pub(super) const FIRST_FRAME_LIMIT: usize = 512;

/// The longest frame after that (pion's `receiveMTU`).
pub(super) const FRAME_LIMIT: usize = 8192;

/// How much a stream may have waiting to be written (LiveKit's `writeBufferSizeInBytes`).
pub(super) const WRITE_BUFFER: usize = 4 * 1024 * 1024;

/// How long a stream that was accepted may take to send its first binding request (pion's
/// `FirstStunBindTimeout`).
pub(super) const FIRST_STUN_BIND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// How long dialling a passive candidate may take.
pub(super) const DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a stream that is closed may go on writing what it has (pion's write deadline when
/// it closes a connection).
const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// How much is read from a stream at a time.
const READ_CHUNK: usize = 16 * 1024;

/// How much the writer of a stream puts into one write.
const WRITE_BATCH: usize = 64 * 1024;

/// A packet as a frame: its length, then the packet. `None` for a packet longer than a frame
/// can say.
pub(super) fn frame(packet: &[u8]) -> Option<Bytes> {
    let len = u16::try_from(packet.len()).ok()?;
    let mut framed = BytesMut::with_capacity(2 + packet.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(packet);
    Some(framed.freeze())
}

/// Takes the frames out of what a stream delivers.
#[derive(Default)]
pub(super) struct FrameReader {
    buf: BytesMut,
}

impl FrameReader {
    /// The next packet of the stream, `None` when the other side has closed it. A frame
    /// longer than `limit` is an error of kind `InvalidData`.
    pub(super) async fn next(
        &mut self,
        stream: &OwnedReadHalf,
        limit: usize,
    ) -> io::Result<Option<BytesMut>> {
        let mut chunk = [0u8; READ_CHUNK];
        loop {
            if self.buf.len() >= 2 {
                let len = u16::from_be_bytes([self.buf[0], self.buf[1]]) as usize;
                if len > limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("a frame of {len} octets, more than {limit}"),
                    ));
                }
                if self.buf.len() >= 2 + len {
                    self.buf.advance(2);
                    return Ok(Some(self.buf.split_to(len)));
                }
            }
            stream.readable().await?;
            match stream.try_read(&mut chunk) {
                Ok(0) => return Ok(None),
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => return Err(err),
            }
        }
    }
}

/// The sending end of a stream's write buffer. Dropping it lets the stream's writer write
/// what is waiting and close its half of the stream.
pub(super) struct Writer {
    tx: mpsc::UnboundedSender<Bytes>,
    queued: Arc<AtomicUsize>,
}

/// The other end, which the stream's writer takes.
pub(super) struct WriterQueue {
    rx: mpsc::UnboundedReceiver<Bytes>,
    queued: Arc<AtomicUsize>,
}

impl Writer {
    pub(super) fn new() -> (Writer, WriterQueue) {
        let (tx, rx) = mpsc::unbounded_channel();
        let queued = Arc::new(AtomicUsize::new(0));
        (
            Writer {
                tx,
                queued: queued.clone(),
            },
            WriterQueue { rx, queued },
        )
    }

    /// Puts a frame in the buffer. False when it does not fit or the stream has ended: the
    /// frame is lost, as a datagram the socket does not take.
    pub(super) fn write(&self, frame: Bytes) -> bool {
        let len = frame.len();
        if self.queued.fetch_add(len, Ordering::Relaxed) + len > WRITE_BUFFER {
            self.queued.fetch_sub(len, Ordering::Relaxed);
            return false;
        }
        if self.tx.send(frame).is_err() {
            self.queued.fetch_sub(len, Ordering::Relaxed);
            return false;
        }
        true
    }
}

/// Writes what is put in the buffer until the buffer is closed and empty, the stream fails,
/// or `closed` has been cancelled for longer than the grace a closing stream has.
pub(super) async fn write_loop(
    stream: OwnedWriteHalf,
    mut queue: WriterQueue,
    closed: CancellationToken,
) {
    let work = async {
        let mut batch = Vec::with_capacity(WRITE_BATCH);
        while let Some(frame) = queue.rx.recv().await {
            batch.clear();
            batch.extend_from_slice(&frame);
            let mut taken = frame.len();
            while batch.len() < WRITE_BATCH
                && let Ok(frame) = queue.rx.try_recv()
            {
                batch.extend_from_slice(&frame);
                taken += frame.len();
            }
            queue.queued.fetch_sub(taken, Ordering::Relaxed);
            if write_all(&stream, &batch).await.is_err() {
                return;
            }
        }
    };
    tokio::select! {
        _ = work => {}
        _ = async {
            closed.cancelled().await;
            tokio::time::sleep(CLOSE_GRACE).await;
        } => {}
    }
    // Dropping the half closes it: the other side reads the end of the stream.
}

async fn write_all(stream: &OwnedWriteHalf, mut data: &[u8]) -> io::Result<()> {
    while !data.is_empty() {
        stream.writable().await?;
        match stream.try_write(data) {
            Ok(n) => data = &data[n..],
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_packet_is_framed_with_its_length() {
        assert_eq!(&frame(&[1, 2, 3]).unwrap()[..], &[0, 3, 1, 2, 3]);
        assert_eq!(&frame(&[]).unwrap()[..], &[0, 0]);
        assert_eq!(frame(&vec![7; 65535]).unwrap().len(), 65537);
        assert_eq!(frame(&vec![7; 65536]), None);
    }

    /// Frames are taken apart however the stream cuts them, and a frame over the limit is an
    /// error before anything of it is read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frames_are_read_whole() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let writer = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_nodelay(true).unwrap();
            // Two frames in one write, then one in pieces, then one too long.
            stream.write_all(&[0, 2, 1, 2, 0, 1, 3]).unwrap();
            stream.write_all(&[0]).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20));
            stream.write_all(&[4, 9, 9]).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20));
            stream.write_all(&[9, 9]).unwrap();
            stream.write_all(&[0x20, 0x01]).unwrap();
            stream
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (read, _write) = stream.into_split();
        let mut reader = FrameReader::default();
        let frame = reader.next(&read, FRAME_LIMIT).await.unwrap().unwrap();
        assert_eq!(&frame[..], &[1u8, 2]);
        let frame = reader.next(&read, FRAME_LIMIT).await.unwrap().unwrap();
        assert_eq!(&frame[..], &[3u8]);
        let frame = reader.next(&read, FRAME_LIMIT).await.unwrap().unwrap();
        assert_eq!(&frame[..], &[9u8, 9, 9, 9]);
        let err = reader.next(&read, FRAME_LIMIT).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        drop(writer.join().unwrap());
        // After the other side closed: the end of the stream.
        let mut reader = FrameReader::default();
        assert!(reader.next(&read, FRAME_LIMIT).await.unwrap().is_none());
    }
}
