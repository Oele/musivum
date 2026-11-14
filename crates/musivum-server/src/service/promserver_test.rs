// New for musivum: upstream has no test of its Prometheus listener. What the configuration
// makes of it, and the endpoint over a listener on the loopback interface, on a port of the
// system's choosing, with and without basic authentication.

use std::time::{Duration as StdDuration, Instant};

use musivum::routing;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;

const TEXT: &str = "text/plain; version=0.0.4; charset=utf-8; escaping=underscores";

fn new_prom_server(conf: &str) -> Result<Option<PromServer>, Error> {
    let conf = Config::new(conf, true).unwrap();
    let current_node = routing::LocalNodeImpl::new(None).unwrap();
    PromServer::new(&conf, current_node.as_ref())
}

// A server made from `conf` that serves on 127.0.0.1 with a port of 0 (its own port is
// never bound): the server and its address.
async fn start(conf: &str) -> (PromServer, SocketAddr, String) {
    let conf = Config::new(conf, true).unwrap();
    let current_node = routing::LocalNodeImpl::new(None).unwrap();
    let s = PromServer::new(&conf, current_node.as_ref())
        .unwrap()
        .unwrap();
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    s.serve(vec![ln]);
    let addr = s.local_addrs()[0];
    (s, addr, current_node.node_id().as_str().to_string())
}

struct Answer {
    status: u16,
    head: String,
    body: String,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.head.lines().find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim())
        })
    }
}

// A request of HTTP/1.1 on a connection of its own, with `headers` (each ending in "\r\n").
async fn request(addr: SocketAddr, method: &str, path: &str, headers: &str) -> Answer {
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(
        format!("{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n{headers}\r\n")
            .as_bytes(),
    )
    .await
    .unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    Answer {
        status: head[9..12].parse().unwrap(),
        head: head.to_string(),
        body: body.to_string(),
    }
}

#[test]
fn test_prom_server_from_the_configuration() {
    // no port: no server
    assert!(new_prom_server("").unwrap().is_none());

    let s = new_prom_server("prometheus:\n  port: 9100\n")
        .unwrap()
        .unwrap();
    assert_eq!(9100, s.port());

    // the deprecated prometheus_port is taken over prometheus.port (with a warning)
    let s = new_prom_server("prometheus_port: 9200\nprometheus:\n  port: 9100\n")
        .unwrap()
        .unwrap();
    assert_eq!(9200, s.port());
    let s = new_prom_server("prometheus_port: 9200\n").unwrap().unwrap();
    assert_eq!(9200, s.port());

    // a user name without a password, or a password without a user name
    for conf in [
        "prometheus:\n  port: 9100\n  username: prom\n",
        "prometheus:\n  port: 9100\n  password: secret\n",
    ] {
        let err = new_prom_server(conf).err().unwrap();
        assert!(matches!(err, Error::PrometheusUsernameOrPassword), "{conf}");
        assert_eq!(
            "prometheus username or password is set but not both, set both or nothing for unauthenticated access",
            err.to_string()
        );
    }
    // which is no matter without a port
    assert!(
        new_prom_server("prometheus:\n  username: prom\n")
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_endpoint() {
    let (s, addr, node_id) = start("prometheus:\n  port: 9100\n").await;

    let res = request(addr, "GET", "/metrics", "").await;
    assert_eq!(200, res.status, "{}", res.head);
    assert_eq!(Some(TEXT), res.header("content-type"));
    // every metric of LiveKit's has the node's labels
    let node_labels = format!("{{node_id=\"{node_id}\",node_type=\"SERVER\"}}");
    assert!(
        res.body
            .lines()
            .any(|line| line.starts_with(&format!("livekit_room_total{node_labels} "))),
        "{}",
        res.body
    );
    for line in res.body.lines() {
        if line.starts_with("livekit_") {
            assert!(
                line.contains(&format!("node_id=\"{node_id}\",node_type=\"SERVER\"")),
                "{line}"
            );
        }
    }
    // and the handler's own have not, as upstream
    assert!(
        res.body
            .contains("\npromhttp_metric_handler_requests_total{code=\"503\"} 0\n"),
        "{}",
        res.body
    );
    assert!(
        res.body
            .contains("\n# TYPE promhttp_metric_handler_requests_in_flight gauge\n")
    );

    // the escaping scheme is the one Prometheus asks for
    let res = request(
        addr,
        "GET",
        "/metrics",
        "Accept: application/openmetrics-text;version=1.0.0;escaping=allow-utf-8;q=0.6,text/plain;version=0.0.4;q=0.3,*/*;q=0.2\r\n",
    )
    .await;
    assert_eq!(
        Some("text/plain; version=0.0.4; charset=utf-8; escaping=allow-utf-8"),
        res.header("content-type")
    );

    // any path, any method
    for (method, path) in [("GET", "/"), ("GET", "/other/path"), ("POST", "/metrics")] {
        let res = request(addr, method, path, "").await;
        assert_eq!(200, res.status, "{method} {path}");
        assert!(res.body.contains("# TYPE livekit_room_total gauge\n"));
    }

    // name[] keeps the families it names
    let res = request(
        addr,
        "GET",
        "/metrics?name[]=livekit_room_total&name%5B%5D=promhttp_metric_handler_requests_in_flight&name[]=unknown",
        "",
    )
    .await;
    let help: Vec<&str> = res
        .body
        .lines()
        .filter(|line| line.starts_with("# HELP "))
        .collect();
    assert_eq!(
        vec![
            "# HELP livekit_room_total ",
            "# HELP promhttp_metric_handler_requests_in_flight Current number of scrapes being served."
        ],
        help
    );

    // the scrapes are counted
    let res = request(
        addr,
        "GET",
        "/metrics?name[]=promhttp_metric_handler_requests_total",
        "",
    )
    .await;
    let scrapes: u64 = res
        .body
        .lines()
        .find_map(|line| line.strip_prefix("promhttp_metric_handler_requests_total{code=\"200\"} "))
        .unwrap()
        .parse()
        .unwrap();
    assert!(scrapes >= 7, "{}", res.body);

    // stopped, it takes no connections
    s.shutdown();
    let deadline = Instant::now() + StdDuration::from_secs(5);
    while TcpStream::connect(addr).await.is_ok() {
        assert!(Instant::now() < deadline, "the server does not stop");
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_endpoint_with_basic_auth() {
    let (s, addr, _) =
        start("prometheus:\n  port: 9100\n  username: prom\n  password: secret\n").await;

    // "prom:secret", "prom:wrong", "other:secret"
    let right = "Authorization: Basic cHJvbTpzZWNyZXQ=\r\n";
    let wrong_password = "Authorization: Basic cHJvbTp3cm9uZw==\r\n";
    let wrong_username = "Authorization: Basic b3RoZXI6c2VjcmV0\r\n";
    for (headers, what) in [
        ("", "none"),
        (wrong_password, "a wrong password"),
        (wrong_username, "a wrong user name"),
        (
            "Authorization: Bearer cHJvbTpzZWNyZXQ=\r\n",
            "another scheme",
        ),
    ] {
        let res = request(addr, "GET", "/metrics", headers).await;
        assert_eq!(401, res.status, "{what}");
        assert_eq!(
            Some("Basic realm=\"Protected Area\""),
            res.header("www-authenticate"),
            "{what}"
        );
        assert_eq!("", res.body, "{what}");
    }

    let res = request(addr, "GET", "/metrics", right).await;
    assert_eq!(200, res.status);
    assert_eq!(Some(TEXT), res.header("content-type"));
    assert!(res.body.contains("# TYPE livekit_room_total gauge\n"));

    s.shutdown();
}
