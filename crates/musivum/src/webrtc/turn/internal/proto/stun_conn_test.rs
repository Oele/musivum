// Ported from pion/turn, internal/proto/stun_conn_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The mock connection answers for the trait
// `net::Conn`; it has no deadlines (the port has none), so "nextConn Called" checks
// `LocalAddr` and `Close` only, and `Close` is the port's (it ends reads; the mock is not told).
// The address a read gives cannot be nil: the mock's remote address stands for it.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::*;
use crate::webrtc::turn::net::Addr;

#[derive(Default)]
struct MockConn {
    did_local_addr: Arc<AtomicBool>,
    did_remote_addr: Arc<AtomicBool>,
}

const MOCK_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);

impl AsyncRead for MockConn {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for MockConn {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Conn for MockConn {
    fn local_addr(&self) -> Addr {
        self.did_local_addr.store(true, Ordering::SeqCst);
        Addr::Tcp(MOCK_ADDR)
    }

    fn remote_addr(&self) -> Addr {
        self.did_remote_addr.store(true, Ordering::SeqCst);
        Addr::Tcp(MOCK_ADDR)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stun_conn() {
    // nextConn Called
    {
        let test_conn = MockConn::default();
        let did_local_addr = test_conn.did_local_addr.clone();
        let stun_conn = StunConn::new(Box::new(test_conn));

        assert_eq!(
            Addr::Tcp(MOCK_ADDR),
            stun_conn.local_addr(),
            "nextConn Called"
        );
        assert!(did_local_addr.load(Ordering::SeqCst), "nextConn Called");

        assert!(stun_conn.close().is_ok(), "nextConn Called");
        let mut buf = [0u8; 8];
        assert!(
            stun_conn.read_from(&mut buf).await.is_err(),
            "nextConn Called"
        );
    }

    // Invalid STUN Frames
    {
        let stun_conn = StunConn::new(Box::new(MockConn::default()));
        *stun_conn.buff.lock() = vec![0; STUN_HEADER_SIZE as usize + 1];

        let err = stun_conn.read_from(&mut []).await.unwrap_err();
        assert_eq!(
            Error::InvalidTurnFrame.to_string(),
            err.to_string(),
            "Invalid STUN Frames"
        );
    }

    // Invalid ChannelData size
    {
        let n = consume_single_turn_frame(&[
            0x40, 0x00, 0x00, 0x12, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ]);
        assert_eq!(
            Err(Error::IncompleteTurnFrame),
            n,
            "Invalid ChannelData size"
        );
    }

    // Padding
    {
        let test_conn = MockConn::default();
        let did_remote_addr = test_conn.did_remote_addr.clone();
        let stun_conn = StunConn::new(Box::new(test_conn));
        *stun_conn.buff.lock() = vec![0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        let (n, addr) = stun_conn.read_from(&mut []).await.unwrap();
        assert_eq!(n, 8, "Padding");
        assert_eq!(Addr::Tcp(MOCK_ADDR), addr, "Padding");
        assert!(did_remote_addr.load(Ordering::SeqCst), "Padding");
    }
}

#[test]
fn test_consume_single_turn_frame() {
    struct TestCase {
        data: Vec<u8>,
        err: Option<Error>,
    }
    let cases = [
        (
            "channel data",
            TestCase {
                data: vec![
                    0x40, 0x01, 0x00, 0x08, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
                ],
                err: None,
            },
        ),
        (
            "partial data less than channel header",
            TestCase {
                data: vec![1],
                err: Some(Error::IncompleteTurnFrame),
            },
        ),
        (
            "partial stun message",
            TestCase {
                data: vec![0x0, 0x16, 0x02, 0xDC, 0x21, 0x12, 0xA4, 0x42, 0x0, 0x0, 0x0],
                err: Some(Error::IncompleteTurnFrame),
            },
        ),
        (
            "stun message",
            TestCase {
                data: vec![
                    0x00, 0x16, 0x00, 0x02, 0x21, 0x12, 0xA4, 0x42, 0xf7, 0x43, 0x81, 0xa3, 0xc9,
                    0xcd, 0x88, 0x89, 0x70, 0x58, 0xac, 0x73, 0x00, 0x00,
                ],
                err: None,
            },
        ),
    ];

    for (name, c) in cases {
        let r = consume_single_turn_frame(&c.data);
        assert_eq!(c.err, r.err(), "{name}");
        if let Ok(n) = r {
            assert_eq!(c.data.len(), n, "{name}");
        }
    }
}
