//! The HTTP client the notifiers send webhooks with, in place of the Go module
//! `github.com/hashicorp/go-retryablehttp` that the original sends them with.
//!
//! Written new for musivum: that module is under the MPL-2.0 and its source was not read.
//! What it does was taken from its documentation (a client with automatic retries and
//! exponential backoff; a retry on a connection error and on a 500-range status except 501;
//! when the retries run out, the body is closed and an error says how many attempts were
//! made) and from a Go test of musivum's that calls what it exports and looks at the results,
//! run at LiveKit's pin (v0.7.8):
//!
//! - `NewClient` waits at least 1 s and at most 30 s between attempts and retries 4 times;
//!   its HTTP client has no timeout of its own.
//! - The wait before retry `i` (from 0) is `min * 2^i`, at most `max`. A response with status
//!   429 or 503 and a `Retry-After` of whole seconds waits that long instead, also beyond
//!   `max`.
//! - A request is retried after an error of the transport, and after a response with status
//!   0, 429 or 500 to 599 except 501. It is not retried for a scheme the client does not
//!   send to, for a server certificate it does not trust, or after too many redirects. Any
//!   other response is returned as it is, whatever its status.
//! - When the attempts are over, `Do` returns no response and the error
//!   `POST <url> giving up after <n> attempt(s)`, followed by `: <the last error>` if there
//!   was one.
//!
//! The requests go out with reqwest, over rustls with the system's root certificates, as
//! HTTP/1.1 with header names in the case Go writes them. The original's transport would
//! negotiate HTTP/2 with a server that offers it over TLS; reqwest also sends `Accept: */*`
//! and Go `Accept-Encoding: gzip`. `ForceIPv4` of the notifiers, a dialer for `tcp4` upstream,
//! is a resolver that keeps the IPv4 addresses of a name, with sockets bound to an IPv4
//! address. A `Retry-After` given as a date is not read.
//!
//! The TLS configuration is made here and handed to reqwest, with ring's provider named, as
//! reqwest would make it otherwise (the system's root certificates, TLS 1.2 and 1.3, HTTP/1.1
//! by ALPN). reqwest takes the process's default provider where one is installed and ring
//! where none is; rustls panics where a configuration is made from the default and the build
//! has two providers and none was installed. A server that builds musivum in may have both
//! (`musivum_server::embed`): whatever it installs, the webhooks go out with ring, as the TLS
//! listener of the TURN server does.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use super::Error;
use crate::go::time::{self, Duration, SECOND};

const DEFAULT_RETRY_WAIT_MIN: Duration = SECOND;
const DEFAULT_RETRY_WAIT_MAX: Duration = Duration(30 * SECOND.0);
const DEFAULT_RETRY_MAX: i64 = 4;

/// What the notifiers set of the `*http.Client` that `retryablehttp.Client` wraps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HttpClient {
    /// `Timeout`: how long one attempt may take, zero for no limit.
    pub timeout: Duration,
    /// A transport that dials `tcp4`.
    pub force_ipv4: bool,
}

/// `retryablehttp.Client`.
pub struct Client {
    pub http_client: HttpClient,
    pub retry_wait_min: Duration,
    pub retry_wait_max: Duration,
    pub retry_max: i64,
    // made at the first request, from `http_client` as it is then
    built: OnceLock<Result<reqwest::Client, String>>,
}

/// `retryablehttp.NewClient()`.
pub fn new_client() -> Client {
    Client {
        http_client: HttpClient::default(),
        retry_wait_min: DEFAULT_RETRY_WAIT_MIN,
        retry_wait_max: DEFAULT_RETRY_WAIT_MAX,
        retry_max: DEFAULT_RETRY_MAX,
        built: OnceLock::new(),
    }
}

/// `retryablehttp.Request`: a request whose body can be sent again.
#[derive(Clone, Debug)]
pub struct Request {
    method: reqwest::Method,
    url: reqwest::Url,
    headers: HeaderMap,
    body: Bytes,
}

/// `retryablehttp.NewRequest(method, url, body)`: fails for a method or URL that cannot be
/// parsed.
pub fn new_request(method: &str, url: &str, body: Bytes) -> Result<Request, Error> {
    let method = reqwest::Method::from_bytes(method.as_bytes())
        .map_err(|err| Error::Http(format!("net/http: invalid method {method:?}: {err}")))?;
    let url =
        reqwest::Url::parse(url).map_err(|err| Error::Http(format!("parse {url:?}: {err}")))?;
    Ok(Request {
        method,
        url,
        headers: HeaderMap::new(),
        body,
    })
}

impl Request {
    /// `r.Header.Set(key, value)`.
    pub fn header_set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|err| Error::Http(format!("invalid header name {key:?}: {err}")))?;
        let value = HeaderValue::from_str(value)
            .map_err(|err| Error::Http(format!("invalid header value for {key:?}: {err}")))?;
        self.headers.insert(name, value);
        Ok(())
    }
}

/// What the notifiers read of an `*http.Response`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// `Status`, such as "200 OK".
    pub status: String,
    /// `StatusCode`.
    pub status_code: u16,
}

impl Response {
    fn of(res: &reqwest::Response) -> Response {
        let code = res.status();
        Response {
            status: match code.canonical_reason() {
                Some(reason) => format!("{} {reason}", code.as_u16()),
                None => code.as_u16().to_string(),
            },
            status_code: code.as_u16(),
        }
    }
}

impl Client {
    fn client(&self) -> Result<&reqwest::Client, Error> {
        self.built
            .get_or_init(|| build(self.http_client))
            .as_ref()
            .map_err(|err| Error::Http(err.clone()))
    }

    /// `c.Do(req)`: sends the request, again after an error or a server error, as the module
    /// header says. The body of the response is not read.
    pub async fn r#do(&self, req: &Request) -> Result<Response, Error> {
        let client = self.client()?;
        let mut attempt: i64 = 0;
        loop {
            attempt += 1;

            let mut builder = client
                .request(req.method.clone(), req.url.clone())
                .headers(req.headers.clone())
                .body(req.body.clone());
            if self.http_client.timeout > Duration::ZERO {
                builder = builder.timeout(self.http_client.timeout.to_std());
            }
            let (should_retry, res, err) = match builder.send().await {
                Ok(res) => (check_retry(res.status().as_u16()), Some(res), None),
                Err(err) => (!is_permanent(&err), None, Some(err)),
            };

            if !should_retry && let Some(res) = &res {
                return Ok(Response::of(res));
            }

            let remain = self.retry_max - (attempt - 1);
            if !should_retry || remain <= 0 {
                let mut text = format!(
                    "{} {} giving up after {attempt} attempt(s)",
                    req.method, req.url
                );
                if let Some(err) = &err {
                    text.push_str(": ");
                    text.push_str(&error_chain(err));
                }
                return Err(Error::Http(text));
            }

            let wait = backoff(
                self.retry_wait_min,
                self.retry_wait_max,
                attempt - 1,
                res.as_ref(),
            );
            drop(res);
            time::sleep(wait).await;
        }
    }
}

// Whether a response with this status is tried again.
fn check_retry(status_code: u16) -> bool {
    status_code == 0
        || status_code == 429
        || (status_code >= 500 && status_code != 501 && status_code <= 599)
}

// An error that another attempt cannot change.
fn is_permanent(err: &reqwest::Error) -> bool {
    if err.is_redirect() || err.is_builder() {
        return true;
    }
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = source {
        if invalid_certificate(e) {
            return true;
        }
        source = e.source();
    }
    false
}

// Whether the error is rustls's for a certificate it does not take. The TLS error comes inside
// an `io::Error`, maybe in another one: the `source` of an `io::Error` is not the error it
// carries but that error's source, so the errors carried are looked into here.
fn invalid_certificate(e: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(rustls::Error::InvalidCertificate(_)) = e.downcast_ref::<rustls::Error>() {
        return true;
    }
    match e
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
    {
        Some(carried) => invalid_certificate(carried),
        None => false,
    }
}

// The wait before retry `attempt_num` (from 0).
fn backoff(
    min: Duration,
    max: Duration,
    attempt_num: i64,
    res: Option<&reqwest::Response>,
) -> Duration {
    if let Some(res) = res
        && (res.status().as_u16() == 429 || res.status().as_u16() == 503)
        && let Some(seconds) = res
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<i64>().ok())
    {
        return Duration(seconds.saturating_mul(SECOND.0));
    }

    let mult = u32::try_from(attempt_num)
        .ok()
        .and_then(|n| 1i64.checked_shl(n))
        .and_then(|m| if m > 0 { min.0.checked_mul(m) } else { None });
    match mult {
        Some(sleep) if sleep <= max.0 => Duration(sleep),
        _ => max,
    }
}

// The error with what caused it, as Go's wrapped errors print.
fn error_chain(err: &reqwest::Error) -> String {
    let mut text = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(e) = source {
        text.push_str(": ");
        text.push_str(&e.to_string());
        source = e.source();
    }
    text
}

fn build(http_client: HttpClient) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .http1_title_case_headers()
        .use_preconfigured_tls(tls_config()?);
    if http_client.force_ipv4 {
        builder = builder
            .dns_resolver(Arc::new(Ipv4Resolver))
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }
    builder.build().map_err(|err| err.to_string())
}

// The configuration reqwest makes with its `rustls-tls-native-roots`, with ring's provider
// (see the module's description): the system's root certificates, an error only where it has
// some and none of them can be read; the versions rustls has; HTTP/1.1, the only version the
// client speaks.
fn tls_config() -> Result<rustls::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    let (mut valid, mut invalid) = (0, 0);
    for cert in native.certs {
        match roots.add(cert) {
            Ok(()) => valid += 1,
            Err(_) => invalid += 1,
        }
    }
    if valid == 0 && invalid > 0 {
        if native.errors.is_empty() {
            return Err("zero valid certificates found in native root store".to_string());
        }
        return Err(native.errors.iter().map(|err| format!("{err}\n")).collect());
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .map_err(|err| err.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

// Keeps the IPv4 addresses of a name, for `ForceIPv4`.
struct Ipv4Resolver;

impl reqwest::dns::Resolve for Ipv4Resolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((name.as_str(), 0))
                .await?
                .filter(SocketAddr::is_ipv4)
                .collect();
            let addrs: reqwest::dns::Addrs = Box::new(addrs.into_iter());
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go::time::MILLISECOND;

    // New for musivum: what the Go test of the module header saw of `DefaultBackoff` and
    // `DefaultRetryPolicy`.
    #[test]
    fn backoff_and_retry_policy_are_those_of_the_original() {
        let waits: Vec<Duration> = (0..8)
            .map(|i| backoff(SECOND, 30 * SECOND, i, None))
            .collect();
        assert_eq!(
            [1, 2, 4, 8, 16, 30, 30, 30].map(|s| s * SECOND).to_vec(),
            waits
        );
        let waits: Vec<Duration> = (0..8)
            .map(|i| backoff(10 * MILLISECOND, SECOND, i, None))
            .collect();
        assert_eq!(
            [10, 20, 40, 80, 160, 320, 640, 1000]
                .map(|ms| ms * MILLISECOND)
                .to_vec(),
            waits
        );
        assert_eq!(MILLISECOND, backoff(SECOND, MILLISECOND, 3, None));
        assert_eq!(30 * SECOND, backoff(SECOND, 30 * SECOND, 100, None));

        let retried: Vec<u16> = [200, 302, 400, 404, 429, 500, 501, 502, 503, 504, 0]
            .into_iter()
            .filter(|code| check_retry(*code))
            .collect();
        assert_eq!(vec![429, 500, 502, 503, 504, 0], retried);

        let c = new_client();
        assert_eq!(
            (SECOND, 30 * SECOND, 4, Duration::ZERO),
            (
                c.retry_wait_min,
                c.retry_wait_max,
                c.retry_max,
                c.http_client.timeout
            )
        );
    }

    // New for musivum: a request over TLS goes out with the configuration made here, whatever
    // providers the build has and none installed (the workspace's tests build rustls with
    // aws-lc-rs too, tests/embed), and is refused once for a certificate that no root of the
    // system's signed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_certificate_that_is_not_trusted_is_refused_without_a_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into()),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handshakes = Arc::new(AtomicUsize::new(0));
        tokio::spawn({
            let handshakes = handshakes.clone();
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    handshakes.fetch_add(1, Ordering::SeqCst);
                    let _ = acceptor.accept(stream).await;
                }
            }
        });

        let req = new_request(
            "POST",
            &format!("https://127.0.0.1:{port}/hook"),
            Bytes::from_static(b"{}"),
        )
        .unwrap();
        let err = new_client().r#do(&req).await.unwrap_err().to_string();
        assert!(err.contains("giving up after 1 attempt(s)"), "{err}");
        assert!(err.contains("certificate"), "{err}");
        assert_eq!(1, handshakes.load(Ordering::SeqCst));
    }
}
