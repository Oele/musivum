// Ported from pion/turn, internal/allocation/channel_bind_test.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. `newChannelBind` gives the allocation as well: a channel
// binding holds its allocation weakly here, so the test keeps it.

use std::sync::Arc;

use super::*;
use crate::go::time::Duration;
use crate::protocol::logger;
use crate::webrtc::turn::internal::allocation::{DEFAULT_PERMISSION_TIMEOUT, EventHandler};
use crate::webrtc::turn::internal::proto::MIN_CHANNEL_NUMBER;

#[tokio::test(flavor = "multi_thread")]
async fn test_channel_bind() {
    let (c, _a) = new_channel_bind(Duration::from_secs(2));
    let found = c
        .allocation
        .lock()
        .upgrade()
        .unwrap()
        .get_channel_by_number(c.number);
    assert!(
        found.is_some_and(|f| Arc::ptr_eq(&f, &c)),
        "GetChannelByNumber({:?}) shouldn't be nil after added to allocation",
        c.number
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_channel_bind_start() {
    let (c, _a) = new_channel_bind(Duration::from_secs(2));

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(
        c.allocation
            .lock()
            .upgrade()
            .unwrap()
            .get_channel_by_number(c.number)
            .is_none(),
        "GetChannelByNumber({:?}) should be nil after timeout",
        c.number
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_channel_bind_reset() {
    let (c, _a) = new_channel_bind(Duration::from_secs(3));

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    c.refresh(Duration::from_secs(3));
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(
        c.allocation
            .lock()
            .upgrade()
            .unwrap()
            .get_channel_by_number(c.number)
            .is_some(),
        "GetChannelByNumber({:?}) shouldn't be nil after refresh",
        c.number
    );
}

fn new_channel_bind(lifetime: Duration) -> (Arc<ChannelBind>, Arc<Allocation>) {
    let a = Arc::new(Allocation::new(
        None,
        None,
        EventHandler::default(),
        logger::get_logger(),
    ));

    let addr = Addr::Udp("0.0.0.0:0".parse().unwrap());
    let c = ChannelBind::new(
        ChannelNumber(MIN_CHANNEL_NUMBER),
        addr,
        logger::get_logger(),
    );

    let _ = a.add_channel_bind(c.clone(), lifetime, DEFAULT_PERMISSION_TIMEOUT);

    (c, a)
}
