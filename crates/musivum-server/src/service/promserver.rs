// Ported from LiveKit, pkg/service/server.go at v1.13.11: the Prometheus listener of
// `NewLivekitServer` and `Start`.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, on tokio and axum, and kept apart from server.rs,
// which calls it where the original has these lines.
// - `promServer` is `PromServer`. The original copies the deprecated `prometheus_port` into
//   `conf.Prometheus.Port`; here the configuration is shared and stays as it was read, and
//   `PromServer::port` is the port that follows from it.
// - `promhttp.Handler()` is `prom_handler`, written new after what client_golang v1.24.1's
//   handler does (Apache-2.0): it answers every path and every method with the metrics of
//   `musivum::telemetry::prometheus`, each with the node's labels (`prometheus.Init` gives
//   them upstream), in the text format with the content type `expfmt.Negotiate` settles on
//   (`musivum::telemetry::prometheus::encode_text` and `negotiate_text_format`), keeps only
//   the families a `name[]` query parameter names when it has any, and counts itself in
//   promhttp's two collectors (`promhttp_metric_handler_requests_total` by status code and
//   `promhttp_metric_handler_requests_in_flight`, without the node's labels, as upstream
//   registers them). Not here: the collectors of Go's runtime and of the process (`go_*`,
//   `process_*`) that Go's default registry has, the protobuf formats (the text format is
//   answered also where a client asks for protobuf first), and compression (an answer is
//   never gzipped, which a client that offers gzip takes as well).
// - The listeners are closed when the server stops; the original leaves them open to the
//   end of the process.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::middleware::from_fn;
use axum::response::{IntoResponse, Response};
use musivum::config::Config;
use musivum::protocol::logger;
use musivum::routing::LocalNode;
use musivum::telemetry::prometheus::{
    Counter, Gauge, MetricFamily, MetricVec, encode_text, gather, negotiate_text_format,
};
use musivum::warnw;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::basic_auth::gen_basic_auth_middleware;
use super::server::{Error, listen};

/// The listener of the metrics: the original's `promServer`, there when a port is
/// configured for it.
pub struct PromServer {
    port: u32,
    handler: axum::Router,
    shutdown: CancellationToken,
    local_addrs: Mutex<Vec<SocketAddr>>,
}

impl PromServer {
    /// What `NewLivekitServer` does for Prometheus: `None` when no port is configured, an
    /// error when only one of a user name and a password is.
    pub fn new(conf: &Config, current_node: &dyn LocalNode) -> Result<Option<PromServer>, Error> {
        let mut port = conf.prometheus.port;
        if conf.prometheus_port > 0 {
            warnw!(
                logger::get_logger(),
                "prometheus_port is deprecated, please switch to prometheus.port instead",
                nil
            );
            port = conf.prometheus_port;
        }

        if port == 0 {
            return Ok(None);
        }
        let username = &conf.prometheus.username;
        let password = &conf.prometheus.password;
        let mut prom_handler = prom_handler(vec![
            ("node_id", current_node.node_id().as_str().to_string()),
            (
                "node_type",
                current_node.node_type().as_str_name().to_string(),
            ),
        ]);
        if !username.is_empty() && !password.is_empty() {
            prom_handler = prom_handler.layer(from_fn(gen_basic_auth_middleware(
                username.clone(),
                password.clone(),
            )));
        } else if !username.is_empty() || !password.is_empty() {
            warnw!(
                logger::get_logger(),
                "prometheus username or password is set but not both, set both or nothing for unauthenticated access",
                nil
            );
            return Err(Error::PrometheusUsernameOrPassword);
        }
        Ok(Some(PromServer {
            port,
            handler: prom_handler,
            shutdown: CancellationToken::new(),
            local_addrs: Mutex::new(Vec::new()),
        }))
    }

    /// The port it listens on: `prometheus.port`, or the deprecated `prometheus_port` when
    /// that is set.
    pub fn port(&self) -> u32 {
        self.port
    }

    /// `net.Listen` on the address and the port, as `Start` does for every bind address.
    pub async fn listen(&self, addr: &str) -> io::Result<tokio::net::TcpListener> {
        listen(addr, self.port).await
    }

    /// `go s.promServer.Serve(promLn)` for each listener: serves until `shutdown`.
    pub fn serve(&self, listeners: Vec<tokio::net::TcpListener>) {
        for ln in listeners {
            if let Ok(addr) = ln.local_addr() {
                self.local_addrs.lock().push(addr);
            }
            let service = self.handler.clone().into_make_service();
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                axum::serve(ln, service)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
            });
        }
    }

    /// Stops serving: no new connections, the requests in progress are answered.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// The addresses it listens on, once it does.
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.local_addrs.lock().clone()
    }
}

// The collectors of promhttp's `InstrumentMetricHandler`, which count the scrapes. They are
// shared by every handler of the process, as upstream's are by the default registry.
static PROMHTTP_METRIC_HANDLER_REQUESTS_TOTAL: MetricVec<Counter, 1> =
    MetricVec::new(["code"], Counter::new);
static PROMHTTP_METRIC_HANDLER_REQUESTS_IN_FLIGHT: Gauge = Gauge::new();

/// `promhttp.Handler()` for musivum's metrics, with `const_labels` (the node's) on each of
/// them.
pub fn prom_handler(const_labels: Vec<(&'static str, String)>) -> axum::Router {
    // the codes promhttp's counter starts with
    for code in ["200", "500", "503"] {
        PROMHTTP_METRIC_HANDLER_REQUESTS_TOTAL.with_label_values([code]);
    }
    let const_labels = Arc::new(const_labels);
    axum::Router::new().fallback(move |headers: HeaderMap, uri: Uri| {
        let const_labels = const_labels.clone();
        async move {
            PROMHTTP_METRIC_HANDLER_REQUESTS_IN_FLIGHT.add(1.0);
            let res = metrics_response(&const_labels, &headers, &uri);
            PROMHTTP_METRIC_HANDLER_REQUESTS_IN_FLIGHT.sub(1.0);
            PROMHTTP_METRIC_HANDLER_REQUESTS_TOTAL
                .with_label_values([res.status().as_str()])
                .inc();
            res
        }
    })
}

fn metrics_response(
    const_labels: &[(&'static str, String)],
    headers: &HeaderMap,
    uri: &Uri,
) -> Response {
    let const_labels: Vec<(&'static str, &str)> = const_labels
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    let mut families: Vec<MetricFamily> = gather()
        .into_iter()
        .map(|family| family.with_const_labels(&const_labels))
        .collect();
    families.push(MetricFamily::of_vec(
        "promhttp_metric_handler_requests_total",
        "Total number of scrapes by HTTP status code.",
        &PROMHTTP_METRIC_HANDLER_REQUESTS_TOTAL,
    ));
    families.push(MetricFamily::of(
        "promhttp_metric_handler_requests_in_flight",
        "Current number of scrapes being served.",
        &PROMHTTP_METRIC_HANDLER_REQUESTS_IN_FLIGHT,
    ));

    let metric_names = query_values(uri.query().unwrap_or(""), b"name[]");
    if !metric_names.is_empty() {
        families.retain(|family| {
            metric_names
                .iter()
                .any(|name| name.as_slice() == family.name.as_bytes())
        });
    }

    // Go's `Header.Get`: the first of the headers
    let accept = headers
        .get(ACCEPT)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    (
        StatusCode::OK,
        [(CONTENT_TYPE, negotiate_text_format(&accept))],
        encode_text(&families),
    )
        .into_response()
}

// The values of `key` in the query, as Go's `URL.Query()` reads it: pairs separated by `&`,
// a pair with a `;` or that cannot be unescaped passed over, `+` a space.
fn query_values(query: &str, key: &[u8]) -> Vec<Vec<u8>> {
    let mut values = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() || pair.contains(';') {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let (Some(k), Some(v)) = (query_unescape(k), query_unescape(v)) else {
            continue;
        };
        if k == key {
            values.push(v);
        }
    }
    values
}

// Go's `url.QueryUnescape`.
fn query_unescape(s: &str) -> Option<Vec<u8>> {
    let s = s.as_bytes();
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                let hex = std::str::from_utf8(hex).ok()?;
                if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return None;
                }
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Some(out)
}

#[cfg(test)]
#[path = "promserver_test.rs"]
mod promserver_test;
