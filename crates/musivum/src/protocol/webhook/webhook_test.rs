// Ported from LiveKit's protocol, webhook/webhook_test.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Each test has a server of its own on 127.0.0.1 and an
// ephemeral port, where the original's tests take turns on port 8765 of every interface and
// reach it as `localhost`; the listener that never accepts is on an ephemeral port too, not
// 9987. The test server is a small HTTP/1.1 server of tokio's sockets: a handler is a future
// it awaits before it answers 200, so a handler that sleeps delays the answer, as Go's
// buffered answer is delayed until the handler returns. What a handler of the original
// checks with `require` it hands to the test (a channel, or a list of failures that the test
// looks at), since a panic in the server's task would not fail the test. A handler cannot
// tell whether the client gave up (`r.Context().Err()`): the two that ask count either way,
// and nothing reads their count. `InitWebhookStats` has no counterpart (see `stats.rs`).
// `send` is given the notifier's parameters as they are under its lock.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::go::atomic;
use crate::go::time::{self, Duration, MILLISECOND, MINUTE, SECOND};
use crate::protocol::auth::SimpleKeyProvider;
use crate::protocol::livekit;
use crate::testutils::eventually_async;

const TEST_API_KEY: &str = "mykey";
const TEST_API_SECRET: &str = "mysecret";
const WEBHOOK_CHECK_INTERVAL: Duration = Duration(100 * MILLISECOND.0);

fn auth_provider() -> SimpleKeyProvider {
    SimpleKeyProvider::new(TEST_API_KEY, TEST_API_SECRET)
}

fn ev(event: &str) -> livekit::WebhookEvent {
    livekit::WebhookEvent {
        event: event.to_string(),
        ..Default::default()
    }
}

fn room_ev(event: &str, room_name: &str) -> livekit::WebhookEvent {
    livekit::WebhookEvent {
        event: event.to_string(),
        room: Some(livekit::Room {
            name: room_name.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn wait(condition: impl FnMut() -> bool, wait_for: Duration) {
    eventually_async(
        condition,
        wait_for.to_std(),
        WEBHOOK_CHECK_INTERVAL.to_std(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_web_hook() {
    // InitWebhookStats(prometheus.Labels{}): see the header

    let s = new_server().await;

    // test event payload
    {
        let notifier = new_test_notifier(&s.url());

        let event = livekit::WebhookEvent {
            event: EVENT_TRACK_PUBLISHED.to_string(),
            participant: Some(livekit::ParticipantInfo {
                identity: "test".to_string(),
                ..Default::default()
            }),
            track: Some(livekit::TrackInfo {
                sid: "TR_abcde".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        s.handler(move |r| {
            let _ = tx.send(r);
        });
        let check = |r: http::Request<Bytes>, expected_url: &str| {
            let decoded_event = receive_webhook_event(&r, &auth_provider()).unwrap();

            assert_eq!(event, decoded_event, "test event payload");
            assert_eq!(expected_url, r.uri().to_string(), "test event payload");
            assert_eq!(
                Some(DEFAULT_USER_AGENT),
                r.headers().get("User-Agent").and_then(|v| v.to_str().ok())
            );
        };
        notifier.queue_notify(&event, &[]).unwrap();
        check(rx.recv().await.unwrap(), "/");

        notifier
            .queue_notify(
                &event,
                &[with_extra_webhooks(vec![livekit::WebhookConfig {
                    url: format!("{}/wh", s.url()),
                    ..Default::default()
                }])],
            )
            .unwrap();
        check(rx.recv().await.unwrap(), "/wh");

        notifier.stop(false);
    }

    // overridden user agent
    {
        let notifier = UrlNotifier::new(UrlNotifierParams {
            url: s.url(),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            config: UrlNotifierConfig {
                queue_size: 20,
                ..Default::default()
            },
            http_client_params: HttpClientParams {
                user_agent: "LiveKitCloud".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });

        let ua = Arc::new(atomic::String::new(""));
        let u = ua.clone();
        s.handler(move |r| {
            u.store(
                r.headers()
                    .get("User-Agent")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or(""),
            );
        });
        notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]).unwrap();
        wait(|| &*ua.load() == "LiveKitCloud", 5 * SECOND).await;

        notifier.stop(false);
    }

    s.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_notifier_dropped() {
    // InitWebhookStats(prometheus.Labels{}): see the header

    let s = new_server().await;

    let url_notifier = new_test_notifier(&s.url());
    let total_dropped = Arc::new(atomic::Int32::default());
    let total_received = Arc::new(atomic::Int32::default());
    let (d, r) = (total_dropped.clone(), total_received.clone());
    let failures = s.failures();
    s.handler(
        move |req| match receive_webhook_event(&req, &auth_provider()) {
            Ok(decoded_event) => {
                r.inc();
                d.add(decoded_event.num_dropped);
            }
            Err(err) => failures.lock().push(err.to_string()),
        },
    );
    // send multiple notifications
    for _ in 0..10 {
        let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
        let _ = url_notifier.queue_notify(&ev(EVENT_PARTICIPANT_JOINED), &[]);
        let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
    }

    time::sleep(WEBHOOK_CHECK_INTERVAL).await;

    assert_eq!(30, total_dropped.load() + total_received.load());
    // at least one request dropped
    assert!(0 < total_dropped.load());
    s.assert_no_failures();

    url_notifier.stop(true);
    s.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_notifier_lifecycle() {
    // InitWebhookStats(prometheus.Labels{}): see the header

    let s = new_server().await;

    // start/stop without use
    {
        let url_notifier = new_test_notifier(&s.url());
        url_notifier.stop(false);
    }

    // stop allowing to drain
    {
        let url_notifier = new_test_notifier(&s.url());
        let num_called = s.counting_handler();
        for _ in 0..10 {
            let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
            let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
        }
        url_notifier.stop(false);
        wait(|| num_called.load() == 20, 5 * SECOND).await;
    }

    // force stop
    {
        let url_notifier = new_test_notifier(&s.url());
        let num_called = s.counting_handler();
        for _ in 0..10 {
            let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
            let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
        }
        url_notifier.stop(true);
        time::sleep(SECOND).await;
        assert!(20 > num_called.load(), "force stop: {}", num_called.load());
    }

    // times out after accepting connection
    {
        let url_notifier = UrlNotifier::new(UrlNotifierParams {
            url: s.url(),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            http_client_params: HttpClientParams {
                retry_wait_max: MILLISECOND,
                max_retries: 1,
                client_timeout: 100 * MILLISECOND,
                ..Default::default()
            },
            config: UrlNotifierConfig {
                queue_size: 20,
                ..Default::default()
            },
            ..Default::default()
        });

        let num_called = Arc::new(atomic::Int32::default());
        let n = num_called.clone();
        s.async_handler(move |_r| {
            let n = n.clone();
            async move {
                // delay the request to cause it to fail
                time::sleep(SECOND).await;
                // inc if not canceled: see the header
                n.inc();
            }
        });

        let params = url_notifier.inner().read().params.clone();
        let res = url_notifier
            .send(&mut ev(EVENT_ROOM_STARTED), &params)
            .await;
        assert!(res.is_err(), "{res:?}");

        url_notifier.stop(false);
    }

    // times out before connection
    {
        let ln = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url_notifier = UrlNotifier::new(UrlNotifierParams {
            url: format!("http://{}", ln.local_addr().unwrap()),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            http_client_params: HttpClientParams {
                retry_wait_max: MILLISECOND,
                max_retries: 1,
                client_timeout: 100 * MILLISECOND,
                ..Default::default()
            },
            ..Default::default()
        });

        let started_at = time::now();
        let params = url_notifier.inner().read().params.clone();
        let res = url_notifier
            .send(&mut ev(EVENT_ROOM_STARTED), &params)
            .await;
        assert!(res.is_err(), "{res:?}");
        assert!(time::since(started_at).seconds() < 2.0);

        url_notifier.stop(false);
        drop(ln);
    }

    s.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_notifier_filter() {
    // InitWebhookStats(prometheus.Labels{}): see the header

    let s = new_server().await;

    let cases: [(&str, FilterParams, bool, i32); 4] = [
        ("none", FilterParams::default(), true, 2),
        (
            "includes",
            FilterParams {
                include_events: vec![EVENT_ROOM_STARTED.to_string()],
                ..Default::default()
            },
            false,
            1,
        ),
        (
            "excludes",
            FilterParams {
                exclude_events: vec![EVENT_ROOM_STARTED.to_string()],
                ..Default::default()
            },
            false,
            1,
        ),
        (
            "includes + excludes",
            FilterParams {
                include_events: vec![EVENT_ROOM_STARTED.to_string()],
                exclude_events: vec![
                    EVENT_ROOM_STARTED.to_string(),
                    EVENT_ROOM_FINISHED.to_string(),
                ],
            },
            false,
            1,
        ),
    ];
    for (name, filter_params, force_ipv4, expected) in cases {
        let url_notifier = UrlNotifier::new(UrlNotifierParams {
            url: s.url(),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            filter_params,
            config: UrlNotifierConfig {
                queue_size: 20,
                ..Default::default()
            },
            // only "none" forces IPv4 upstream
            http_client_params: HttpClientParams {
                force_ipv4,
                ..Default::default()
            },
            ..Default::default()
        });

        let num_called = s.counting_handler();

        // includes + excludes: EventRoomStarted should be allowed as IncludeEvents take precedence
        let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
        let _ = url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
        eventually_async(
            || num_called.load() == expected,
            (5 * SECOND).to_std(),
            WEBHOOK_CHECK_INTERVAL.to_std(),
        )
        .await;
        let _ = name;

        url_notifier.stop(false);
    }

    s.stop();
}

fn new_test_notifier(url: &str) -> Arc<UrlNotifier> {
    UrlNotifier::new(UrlNotifierParams {
        url: url.to_string(),
        api_key: TEST_API_KEY.to_string(),
        api_secret: TEST_API_SECRET.to_string(),
        config: UrlNotifierConfig {
            queue_size: 20,
            ..Default::default()
        },
        ..Default::default()
    })
}

// --------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_resource_web_hook() {
    let s = new_server().await;

    // test event payload
    {
        let resource_url_notifier = new_default_notifier(
            &WebHookConfig {
                urls: vec![s.url()],
                api_key: TEST_API_KEY.to_string(),
                ..Default::default()
            },
            Arc::new(auth_provider()),
        )
        .unwrap();

        let event = livekit::WebhookEvent {
            event: EVENT_TRACK_PUBLISHED.to_string(),
            participant: Some(livekit::ParticipantInfo {
                identity: "test".to_string(),
                ..Default::default()
            }),
            track: Some(livekit::TrackInfo {
                sid: "TR_abcde".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        s.handler(move |r| {
            let _ = tx.send(r);
        });
        resource_url_notifier.queue_notify(&event, &[]).unwrap();
        let r = rx.recv().await.unwrap();
        let decoded_event = receive_webhook_event(&r, &auth_provider()).unwrap();

        assert_eq!(event, decoded_event);
        assert_eq!(
            Some(DEFAULT_USER_AGENT),
            r.headers().get("User-Agent").and_then(|v| v.to_str().ok())
        );

        resource_url_notifier.stop(false);
    }

    // overridden user agent
    {
        let resource_url_notifier = ResourceUrlNotifier::new(ResourceUrlNotifierParams {
            url: s.url(),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            config: ResourceUrlNotifierConfig {
                max_age: 200 * MILLISECOND,
                max_depth: 50,
            },
            http_client_params: HttpClientParams {
                user_agent: "LiveKitCloud".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });

        let ua = Arc::new(atomic::String::new(""));
        let u = ua.clone();
        s.handler(move |r| {
            u.store(
                r.headers()
                    .get("User-Agent")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or(""),
            );
        });
        resource_url_notifier
            .queue_notify(&ev(EVENT_ROOM_STARTED), &[])
            .unwrap();
        wait(|| &*ua.load() == "LiveKitCloud", 5 * SECOND).await;

        resource_url_notifier.stop(false);
    }

    s.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resource_url_notifier_dropped() {
    let s = new_server().await;

    // depth drop
    {
        let resource_url_notifier = new_test_resource_notifier(&s.url(), MINUTE, MINUTE, 5);
        let total_dropped = Arc::new(atomic::Int32::default());
        let total_received = Arc::new(atomic::Int32::default());
        let r = total_received.clone();
        let failures = s.failures();
        s.handler(
            move |req| match receive_webhook_event(&req, &auth_provider()) {
                Ok(_) => {
                    r.inc();
                }
                Err(err) => failures.lock().push(err.to_string()),
            },
        );
        // send multiple notifications
        for _ in 0..10 {
            for event in [
                EVENT_ROOM_STARTED,
                EVENT_PARTICIPANT_JOINED,
                EVENT_ROOM_FINISHED,
            ] {
                let err = resource_url_notifier.queue_notify(&ev(event), &[]);
                if err == Err(Error::QueueFull) {
                    total_dropped.inc();
                }
            }
        }

        time::sleep(WEBHOOK_CHECK_INTERVAL).await;

        wait(
            || total_dropped.load() + total_received.load() == 30,
            5 * SECOND,
        )
        .await;
        // at least one request dropped, but not all dropped
        assert!(0 < total_dropped.load(), "depth drop");
        assert!(0 < total_received.load(), "depth drop");
        s.assert_no_failures();

        resource_url_notifier.stop(true);
    }

    // age drop
    {
        let resource_url_notifier =
            new_test_resource_notifier(&s.url(), MINUTE, 10 * MILLISECOND, 500);
        let total_received = Arc::new(atomic::Int32::default());
        let r = total_received.clone();
        let failures = s.failures();
        s.async_handler(move |req| {
            let r = r.clone();
            let failures = failures.clone();
            async move {
                time::sleep(5 * MILLISECOND).await;
                match receive_webhook_event(&req, &auth_provider()) {
                    Ok(_) => {
                        r.inc();
                    }
                    Err(err) => failures.lock().push(err.to_string()),
                }
            }
        });
        // send multiple notifications
        for _ in 0..10 {
            let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
            time::sleep(MILLISECOND).await;
            let _ = resource_url_notifier.queue_notify(&ev(EVENT_PARTICIPANT_JOINED), &[]);
            time::sleep(MILLISECOND).await;
            let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
            time::sleep(MILLISECOND).await;
        }

        time::sleep(2 * WEBHOOK_CHECK_INTERVAL).await;

        // at least one request dropped
        assert!(30 > total_received.load(), "age drop");
        assert!(0 < total_received.load(), "age drop");
        s.assert_no_failures();

        resource_url_notifier.stop(true);
    }

    // resource queue timeout
    {
        let resource_url_notifier =
            new_test_resource_notifier(&s.url(), 5 * MILLISECOND, MINUTE, 500);
        let total_received = Arc::new(atomic::Int32::default());
        let r = total_received.clone();
        let failures = s.failures();
        s.handler(
            move |req| match receive_webhook_event(&req, &auth_provider()) {
                Ok(_) => {
                    r.inc();
                }
                Err(err) => failures.lock().push(err.to_string()),
            },
        );

        // check that resource queues change for the same event key
        for i in 0..3 {
            let mut rq: Option<Arc<resource_queue::ResourceQueue>> = None;

            let room_name = format!("room{i}");

            for event in [
                EVENT_ROOM_STARTED,
                EVENT_PARTICIPANT_JOINED,
                EVENT_PARTICIPANT_LEFT,
            ] {
                let _ = resource_url_notifier.queue_notify(&room_ev(event, &room_name), &[]);
                let rqi = resource_url_notifier
                    .inner()
                    .read()
                    .resource_queues
                    .get(&room_name)
                    .cloned();
                let rqi = rqi.expect("a queue of the room");
                let queue = rqi.value.resource_queue.clone();
                assert!(
                    !rq.as_ref().is_some_and(|rq| Arc::ptr_eq(rq, &queue)),
                    "{room_name}, {event}: the same queue"
                );
                rq = Some(queue);
                time::sleep(10 * MILLISECOND).await;
            }
            let _ = rq; // used in next loop iteration upstream
        }

        time::sleep(WEBHOOK_CHECK_INTERVAL).await;

        assert_eq!(9, total_received.load());
        s.assert_no_failures();

        resource_url_notifier.stop(true);
    }

    s.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resource_url_notifier_lifecycle() {
    let s = new_server().await;

    // start/stop without use
    {
        let resource_url_notifier =
            new_test_resource_notifier(&s.url(), MINUTE, 200 * MILLISECOND, 50);
        resource_url_notifier.stop(false);
    }

    // sweeper
    {
        let resource_url_notifier =
            new_test_resource_notifier(&s.url(), 200 * MILLISECOND, 200 * MILLISECOND, 50);
        let num_called = s.counting_handler();
        for i in 0..10 {
            let room_name = format!("room{i}");
            let _ =
                resource_url_notifier.queue_notify(&room_ev(EVENT_ROOM_STARTED, &room_name), &[]);

            let _ =
                resource_url_notifier.queue_notify(&room_ev(EVENT_ROOM_FINISHED, &room_name), &[]);
        }

        assert_eq!(
            10,
            resource_url_notifier.inner().read().resource_queues.len()
        );

        time::sleep(SECOND).await;

        // should have reaped after some time
        assert_eq!(
            0,
            resource_url_notifier.inner().read().resource_queues.len()
        );

        assert_eq!(20, num_called.load());
        resource_url_notifier.stop(false);
    }

    // stop allowing to drain
    {
        let resource_url_notifier =
            new_test_resource_notifier(&s.url(), MINUTE, 200 * MILLISECOND, 50);
        let num_called = s.counting_handler();
        for _ in 0..10 {
            let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
            let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
        }
        resource_url_notifier.stop(false);
        wait(|| num_called.load() == 20, 5 * SECOND).await;
    }

    // force stop
    {
        let resource_url_notifier =
            new_test_resource_notifier(&s.url(), MINUTE, 200 * MILLISECOND, 50);
        let num_called = s.counting_handler();
        for _ in 0..10 {
            let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
            let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
        }
        resource_url_notifier.stop(true);
        time::sleep(SECOND).await;
        assert!(20 > num_called.load(), "force stop: {}", num_called.load());
    }

    // times out after accepting connection
    {
        let params = ResourceUrlNotifierParams {
            url: s.url(),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            config: ResourceUrlNotifierConfig {
                max_age: 200 * MILLISECOND,
                max_depth: 50,
            },
            http_client_params: HttpClientParams {
                retry_wait_max: MILLISECOND,
                max_retries: 1,
                client_timeout: 100 * MILLISECOND,
                ..Default::default()
            },
            ..Default::default()
        };

        let resource_url_notifier = ResourceUrlNotifier::new(params.clone());

        let num_called = Arc::new(atomic::Int32::default());
        let n = num_called.clone();
        s.async_handler(move |_r| {
            let n = n.clone();
            async move {
                // delay the request to cause it to fail
                time::sleep(SECOND).await;
                // inc if not canceled: see the header
                n.inc();
            }
        });

        let res = resource_url_notifier
            .send(&ev(EVENT_ROOM_STARTED), &params)
            .await;
        assert!(res.is_err(), "{res:?}");

        resource_url_notifier.stop(false);
    }

    // times out before connection
    {
        let ln = std::net::TcpListener::bind("127.0.0.1:0").unwrap();

        let params = ResourceUrlNotifierParams {
            url: format!("http://{}", ln.local_addr().unwrap()),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            config: ResourceUrlNotifierConfig {
                max_age: 200 * MILLISECOND,
                max_depth: 50,
            },
            http_client_params: HttpClientParams {
                retry_wait_max: MILLISECOND,
                max_retries: 1,
                client_timeout: 100 * MILLISECOND,
                ..Default::default()
            },
            ..Default::default()
        };

        let resource_url_notifier = ResourceUrlNotifier::new(params.clone());

        let started_at = time::now();
        let res = resource_url_notifier
            .send(&ev(EVENT_ROOM_STARTED), &params)
            .await;
        assert!(res.is_err(), "{res:?}");
        assert!(time::since(started_at).seconds() < 2.0);

        resource_url_notifier.stop(false);
        drop(ln);
    }

    s.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resource_url_notifier_filter() {
    let s = new_server().await;

    let cases: [(&str, FilterParams, i32); 4] = [
        ("none", FilterParams::default(), 2),
        (
            "includes",
            FilterParams {
                include_events: vec![EVENT_ROOM_STARTED.to_string()],
                ..Default::default()
            },
            1,
        ),
        (
            "excludes",
            FilterParams {
                exclude_events: vec![EVENT_ROOM_STARTED.to_string()],
                ..Default::default()
            },
            1,
        ),
        (
            "includes + excludes",
            FilterParams {
                include_events: vec![EVENT_ROOM_STARTED.to_string()],
                exclude_events: vec![
                    EVENT_ROOM_STARTED.to_string(),
                    EVENT_ROOM_FINISHED.to_string(),
                ],
            },
            1,
        ),
    ];
    for (name, filter_params, expected) in cases {
        let resource_url_notifier = ResourceUrlNotifier::new(ResourceUrlNotifierParams {
            url: s.url(),
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            config: ResourceUrlNotifierConfig {
                max_age: 200 * MILLISECOND,
                max_depth: 50,
            },
            filter_params,
            ..Default::default()
        });

        let num_called = s.counting_handler();

        // includes + excludes: EventRoomStarted should be allowed as IncludeEvents take precedence
        let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_STARTED), &[]);
        let _ = resource_url_notifier.queue_notify(&ev(EVENT_ROOM_FINISHED), &[]);
        eventually_async(
            || num_called.load() == expected,
            (5 * SECOND).to_std(),
            WEBHOOK_CHECK_INTERVAL.to_std(),
        )
        .await;
        let _ = name;

        resource_url_notifier.stop(false);
    }

    s.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resource_url_notifier_queue_notify_error() {
    let resource_url_notifier = ResourceUrlNotifier::new(ResourceUrlNotifierParams {
        url: "http://127.0.0.1:1".to_string(),
        ..Default::default()
    });

    // a failed call must not keep the notifier locked
    let (errs_tx, mut errs) = tokio::sync::mpsc::channel(4);
    let n = resource_url_notifier.clone();
    std::thread::spawn(move || {
        for _ in 0..2 {
            let _ = errs_tx.blocking_send(n.queue_notify(&ev(EVENT_ROOM_STARTED), &[]));
        }
        n.set_keys(TEST_API_KEY, TEST_API_SECRET);
        for _ in 0..2 {
            let url = "http://127.0.0.1:1".to_string();
            let _ = errs_tx.blocking_send(n.queue_notify(
                &ev(EVENT_ROOM_STARTED),
                &[with_extra_webhooks(vec![
                    livekit::WebhookConfig {
                        url: url.clone(),
                        ..Default::default()
                    },
                    livekit::WebhookConfig {
                        url,
                        ..Default::default()
                    },
                ])],
            ));
        }
    });

    for i in 0..4 {
        match tokio::time::timeout(std::time::Duration::from_secs(5), errs.recv()).await {
            Ok(Some(err)) => {
                if i < 2 {
                    assert_eq!(Err(Error::NoKey), err);
                } else {
                    assert!(err.is_err());
                }
            }
            _ => panic!("QueueNotify blocked after {i} failed calls"),
        }
    }

    // not deferred, Stop would block on a notifier that is still locked
    resource_url_notifier.stop(true);
}

fn new_test_resource_notifier(
    url: &str,
    timeout: Duration,
    max_age: Duration,
    max_depth: i64,
) -> Arc<ResourceUrlNotifier> {
    ResourceUrlNotifier::new(ResourceUrlNotifierParams {
        url: url.to_string(),
        api_key: TEST_API_KEY.to_string(),
        api_secret: TEST_API_SECRET.to_string(),
        timeout,
        config: ResourceUrlNotifierConfig { max_age, max_depth },
        ..Default::default()
    })
}

// ---------------------------------------

type Handler =
    Arc<dyn Fn(http::Request<Bytes>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub(crate) struct TestServer {
    addr: SocketAddr,
    handler: Arc<RwLock<Option<Handler>>>,
    failures: Arc<Mutex<Vec<String>>>,
    stop: CancellationToken,
}

// newServer(testAddr) and Start of the original
pub(crate) async fn new_server() -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let s = TestServer {
        addr: listener.local_addr().unwrap(),
        handler: Arc::new(RwLock::new(None)),
        failures: Arc::new(Mutex::new(Vec::new())),
        stop: CancellationToken::new(),
    };
    let (handler, stop) = (s.handler.clone(), s.stop.clone());
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                conn = listener.accept() => {
                    let Ok((conn, _)) = conn else { return };
                    tokio::spawn(serve_conn(conn, handler.clone(), stop.clone()));
                }
            }
        }
    });
    s
}

impl TestServer {
    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// `s.handler = func(w, r) { ... }` for a handler that does not wait.
    pub(crate) fn handler(&self, f: impl Fn(http::Request<Bytes>) + Send + Sync + 'static) {
        let f = Arc::new(f);
        *self.handler.write() = Some(Arc::new(move |r| {
            f(r);
            Box::pin(async {})
        }));
    }

    /// A handler that waits before the answer.
    pub(crate) fn async_handler<F, Fut>(&self, f: F)
    where
        F: Fn(http::Request<Bytes>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        *self.handler.write() = Some(Arc::new(move |r| Box::pin(f(r))));
    }

    /// The handler that counts the requests, as most tests have it.
    pub(crate) fn counting_handler(&self) -> Arc<atomic::Int32> {
        let num_called = Arc::new(atomic::Int32::default());
        let n = num_called.clone();
        self.handler(move |_r| {
            n.inc();
        });
        num_called
    }

    /// Where handlers write what their `require` found wrong.
    pub(crate) fn failures(&self) -> Arc<Mutex<Vec<String>>> {
        self.failures.clone()
    }

    #[track_caller]
    pub(crate) fn assert_no_failures(&self) {
        let failures = self.failures.lock().clone();
        assert!(failures.is_empty(), "{failures:?}");
    }

    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }
}

// One connection: requests one after the other, each answered 200 once its handler is done.
async fn serve_conn(
    mut conn: TcpStream,
    handler: Arc<RwLock<Option<Handler>>>,
    stop: CancellationToken,
) {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let req = tokio::select! {
            _ = stop.cancelled() => return,
            req = read_request(&mut conn, &mut buf) => req,
        };
        let Some(req) = req else { return };
        let h = handler.read().clone();
        if let Some(h) = h {
            h(req).await;
        }
        if conn
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn read_request(conn: &mut TcpStream, buf: &mut Vec<u8>) -> Option<http::Request<Bytes>> {
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        let mut chunk = [0u8; 4096];
        let n = conn.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8(buf[..head_end].to_vec()).ok()?;
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split(' ');
    let (method, target) = (request_line.next()?, request_line.next()?);
    let mut builder = http::Request::builder().method(method).uri(target);
    let mut content_length = 0;
    for line in lines {
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().ok()?;
        }
        builder = builder.header(name, value);
    }
    let body_start = head_end + 4;
    while buf.len() < body_start + content_length {
        let mut chunk = [0u8; 4096];
        let n = conn.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = Bytes::copy_from_slice(&buf[body_start..body_start + content_length]);
    buf.drain(..body_start + content_length);
    builder.body(body).ok()
}
