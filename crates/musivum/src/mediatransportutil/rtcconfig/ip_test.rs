// New for musivum: the original has no tests of ip.go. The filters are tried on made-up
// lists of interfaces (the addresses are those set aside for documentation), and the STUN
// exchange against a server of the test's own on the loopback interface. Nothing here sends
// a datagram off the machine: where an address is validated, it is a loopback address.

use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Instant;

use rtc::stun::message::{BINDING_ERROR, BINDING_SUCCESS, Message, TransactionId};
use rtc::stun::xoraddr::XorMappedAddress;

use super::super::{InterfacesConfig, IpsConfig, interface_filter_from_conf, ip_filter_from_conf};
use super::*;

/// A STUN server of the test's own: on the loopback interface, on a port of the system's
/// choosing. It answers as the test says.
pub(crate) struct StunResponder {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<AtomicUsize>,
    thread: Option<JoinHandle<()>>,
}

impl StunResponder {
    /// `answer` is given the number of the request (from 0), the request and where it came
    /// from, and gives the datagrams to send back.
    pub(crate) fn start(
        answer: impl Fn(usize, &Message, SocketAddr) -> Vec<Vec<u8>> + Send + 'static,
    ) -> StunResponder {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_millis(20)))
            .unwrap();
        let addr = socket.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(AtomicUsize::new(0));
        let thread = std::thread::spawn({
            let stop = stop.clone();
            let requests = requests.clone();
            move || {
                let mut buf = [0u8; 2048];
                while !stop.load(Ordering::SeqCst) {
                    let Ok((n, from)) = socket.recv_from(&mut buf) else {
                        continue;
                    };
                    let mut req = Message::new();
                    if req.unmarshal_binary(&buf[..n]).is_err() || req.typ != BINDING_REQUEST {
                        continue;
                    }
                    let nth = requests.fetch_add(1, Ordering::SeqCst);
                    for datagram in answer(nth, &req, from) {
                        socket.send_to(&datagram, from).unwrap();
                    }
                }
            }
        });
        StunResponder {
            addr,
            stop,
            requests,
            thread: Some(thread),
        }
    }

    /// A server that tells everybody the address it sees them at.
    pub(crate) fn reflecting() -> StunResponder {
        StunResponder::start(|_, req, from| vec![binding_success(req.transaction_id, from)])
    }

    /// What goes into `stun_servers`.
    pub(crate) fn server(&self) -> String {
        self.addr.to_string()
    }

    pub(crate) fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for StunResponder {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A binding success response that gives `mapped` as the address of the sender.
pub(crate) fn binding_success(transaction_id: TransactionId, mapped: SocketAddr) -> Vec<u8> {
    let mut res = Message::new();
    res.build(&[
        Box::new(transaction_id),
        Box::new(BINDING_SUCCESS),
        Box::new(XorMappedAddress {
            ip: mapped.ip(),
            port: mapped.port(),
        }),
    ])
    .unwrap();
    res.raw
}

/// A port on the loopback interface that was free a moment ago.
pub(crate) fn free_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn iface(name: &str, up: bool, loopback: bool, addrs: &[&str]) -> Interface {
    Interface {
        name: name.to_string(),
        up,
        loopback,
        addrs: addrs.iter().map(|a| a.parse().unwrap()).collect(),
    }
}

/// A machine with two network cards and a bridge that is down.
fn made_up_interfaces() -> Vec<Interface> {
    vec![
        iface("lo", true, true, &["127.0.0.1", "::1"]),
        iface(
            "eth0",
            true,
            false,
            &["192.0.2.10", "2001:db8::10", "fe80::1"],
        ),
        iface(
            "eth1",
            true,
            false,
            &["169.254.7.7", "198.51.100.7", "224.0.0.5", "ff02::1"],
        ),
        iface("docker0", false, false, &["203.0.113.1"]),
    ]
}

#[test]
fn test_local_ip_addresses_from() {
    // New for musivum.
    let ifaces = made_up_interfaces();
    let local = |include_loopback, include_v6, if_filter: Option<&InterfaceFilter>, ip_filter| {
        local_ip_addresses_from(&ifaces, include_loopback, include_v6, if_filter, ip_filter)
            .map_err(|err| err.to_string())
    };
    let ok = |addrs: &[&str]| Ok(addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>());

    // Link-local addresses are left out, whether an interface is up is not asked.
    assert_eq!(
        ok(&["192.0.2.10", "2001:db8::10", "198.51.100.7", "203.0.113.1"]),
        local(false, true, None, None)
    );
    // Loopback addresses come last.
    assert_eq!(
        ok(&[
            "192.0.2.10",
            "2001:db8::10",
            "198.51.100.7",
            "203.0.113.1",
            "127.0.0.1",
            "::1"
        ]),
        local(true, true, None, None)
    );
    assert_eq!(
        ok(&["192.0.2.10", "198.51.100.7", "203.0.113.1"]),
        local(false, false, None, None)
    );
    assert_eq!(
        ok(&["192.0.2.10", "198.51.100.7", "203.0.113.1", "127.0.0.1"]),
        local(true, false, None, None)
    );

    // The filter on interfaces.
    let excludes = interface_filter_from_conf(&InterfacesConfig {
        excludes: vec!["docker0".to_string(), "eth1".to_string()],
        ..Default::default()
    });
    assert_eq!(
        ok(&["192.0.2.10", "2001:db8::10"]),
        local(false, true, Some(&excludes), None)
    );
    let includes = interface_filter_from_conf(&InterfacesConfig {
        includes: vec!["eth1".to_string()],
        ..Default::default()
    });
    assert_eq!(
        ok(&["198.51.100.7"]),
        local(false, true, Some(&includes), None)
    );

    // The filter on addresses.
    let ips = ip_filter_from_conf(&IpsConfig {
        includes: vec!["198.51.100.0/24".to_string(), "2001:db8::/32".to_string()],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        ok(&["2001:db8::10", "198.51.100.7"]),
        local(false, true, None, Some(&ips))
    );
    assert_eq!(ok(&["198.51.100.7"]), local(true, false, None, Some(&ips)));

    // With nothing else, the loopback addresses are given though they were not asked for.
    let lo = interface_filter_from_conf(&InterfacesConfig {
        includes: vec!["lo".to_string()],
        ..Default::default()
    });
    assert_eq!(
        ok(&["127.0.0.1", "::1"]),
        local(false, true, Some(&lo), None)
    );
    assert_eq!(ok(&["127.0.0.1"]), local(false, false, Some(&lo), None));

    // And with nothing at all, an error.
    let none = interface_filter_from_conf(&InterfacesConfig {
        includes: vec!["wlan0".to_string()],
        ..Default::default()
    });
    assert_eq!(
        Err("could not find local IP address".to_string()),
        local(true, true, Some(&none), None)
    );
}

#[cfg(unix)]
#[test]
fn test_interfaces() {
    // New for musivum: the list of the system has its loopback interface in it. (That the
    // list is Go's, in its order, was seen once by running both on one machine.)
    let ifaces = interfaces().unwrap();
    let lo = ifaces
        .iter()
        .find(|iface| iface.loopback)
        .expect("a loopback interface");
    assert!(lo.up);
    assert!(lo.addrs.contains(&"127.0.0.1".parse().unwrap()));
    // An interface is listed once, whatever the number of its addresses.
    for iface in &ifaces {
        assert_eq!(
            1,
            ifaces.iter().filter(|i| i.name == iface.name).count(),
            "{}",
            iface.name
        );
    }
    // The loopback address is among the local addresses when it is asked for, and last.
    let addresses = get_local_ip_addresses(true, false, None, None).unwrap();
    assert!(
        addresses.last().unwrap().starts_with("127."),
        "{addresses:?}"
    );
}

#[test]
fn test_get_external_ip() {
    // New for musivum.
    let responder = StunResponder::reflecting();
    let servers = [responder.server()];

    // No local address: the system chooses one, and nothing is validated.
    assert_eq!(
        "127.0.0.1",
        get_external_ip(&Context::background(), &servers, None).unwrap()
    );
    assert_eq!(
        "127.0.0.1",
        get_external_ipv4(&Context::background(), &servers, None).unwrap()
    );
    assert_eq!(2, responder.requests());

    // A local address: the request comes from there, and then the address the server gave
    // is tried from the outside, which here is the loopback interface.
    let local: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let recording = StunResponder::start({
        let seen = seen.clone();
        move |_, req, from| {
            seen.lock().push(from);
            vec![binding_success(req.transaction_id, from)]
        }
    });
    assert_eq!(
        "127.0.0.1",
        get_external_ip(&Context::background(), &[recording.server()], Some(local)).unwrap()
    );
    assert_eq!(vec![local], *seen.lock());

    // The address is the one the server names, whatever it is. (Not validated: that would
    // send to it.)
    let elsewhere = StunResponder::start(|_, req, _| {
        vec![binding_success(
            req.transaction_id,
            "[2001:db8::7]:1234".parse().unwrap(),
        )]
    });
    assert_eq!(
        "2001:db8::7",
        get_external_ip(&Context::background(), &[elsewhere.server()], None).unwrap()
    );

    assert_eq!(
        "STUN servers are required but not defined",
        get_external_ip(&Context::background(), &[], None)
            .unwrap_err()
            .to_string()
    );
    // IPv4 is asked for and the server has no such address: nothing is sent.
    assert_eq!(
        "no suitable address found",
        get_external_ipv4(&Context::background(), &["[::1]:9".to_string()], None)
            .unwrap_err()
            .to_string()
    );
}

#[test]
fn test_get_external_ip_answers() {
    // New for musivum: what the exchange makes of answers that are late, not for it, or
    // without an address.

    // The first request is lost: it is sent again after 300 ms. With it comes an answer to
    // another transaction, which is passed over.
    let lossy = StunResponder::start(|nth, req, from| {
        if nth == 0 {
            return vec![];
        }
        vec![
            binding_success(TransactionId::new(), "192.0.2.1:1".parse().unwrap()),
            b"not STUN".to_vec(),
            binding_success(req.transaction_id, from),
        ]
    });
    let start = Instant::now();
    assert_eq!(
        "127.0.0.1",
        get_external_ip(&Context::background(), &[lossy.server()], None).unwrap()
    );
    assert_eq!(2, lossy.requests());
    assert!(
        start.elapsed() >= STUN_RTO.to_std(),
        "{:?}",
        start.elapsed()
    );

    // An answer without the address is an error, and the next server is asked.
    let refusing = StunResponder::start(|_, req, _| {
        let mut res = Message::new();
        res.build(&[Box::new(req.transaction_id), Box::new(BINDING_ERROR)])
            .unwrap();
        vec![res.raw]
    });
    assert!(matches!(
        get_external_ip(&Context::background(), &[refusing.server()], None),
        Err(Error::Stun(_))
    ));
    let responder = StunResponder::reflecting();
    assert_eq!(
        "127.0.0.1",
        get_external_ip(
            &Context::background(),
            &[refusing.server(), responder.server()],
            None
        )
        .unwrap()
    );
    assert_eq!(2, refusing.requests());
    assert_eq!(1, responder.requests());
}

#[test]
fn test_get_external_ip_context() {
    // New for musivum: a server that does not answer is waited for until the deadline, or
    // until the context is cancelled.
    let silent = StunResponder::start(|_, _, _| vec![]);
    let servers = [silent.server()];

    let start = Instant::now();
    let ctx = Context::background().with_timeout(Duration::from_millis(400));
    assert!(matches!(
        get_external_ip(&ctx, &servers, None),
        Err(Error::DeadlineExceeded)
    ));
    assert!(
        start.elapsed() >= std::time::Duration::from_millis(400)
            && start.elapsed() < std::time::Duration::from_secs(3),
        "{:?}",
        start.elapsed()
    );
    // Once at the start and once after 300 ms.
    assert_eq!(2, silent.requests());

    let (ctx, cancel) = Context::background().with_cancel();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        cancel.cancel();
    });
    let start = Instant::now();
    assert!(matches!(
        get_external_ip(&ctx, &servers, None),
        Err(Error::Canceled)
    ));
    assert!(
        start.elapsed() < std::time::Duration::from_secs(3),
        "{:?}",
        start.elapsed()
    );
    canceller.join().unwrap();

    // An address that cannot be reached from the outside (the server names another
    // loopback address than the one that listens) is not validated: the deadline passes.
    let wrong = StunResponder::start(|_, req, from| {
        vec![binding_success(
            req.transaction_id,
            SocketAddr::new("127.0.0.2".parse().unwrap(), from.port()),
        )]
    });
    let local: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let ctx = Context::background().with_timeout(Duration::from_millis(400));
    assert!(matches!(
        get_external_ip(&ctx, &[wrong.server()], Some(local)),
        Err(Error::DeadlineExceeded)
    ));
    // Without validation the same answer is taken.
    assert_eq!(
        "127.0.0.2",
        get_external_ip_on(
            &Context::background(),
            &[wrong.server()],
            Some(local),
            false,
            Network::Udp
        )
        .unwrap()
    );
}

#[test]
fn test_determine_ip() {
    // New for musivum: the node's address through STUN, as `validate` finds it.
    let responder = StunResponder::reflecting();
    let mut conf = RtcConfig {
        use_external_ip: true,
        stun_servers: vec![responder.server()],
        udp_port: super::super::PortRange {
            start: free_port() as i64,
            end: 0,
        },
        ..Default::default()
    };
    conf.validate(true).unwrap();
    assert_eq!("127.0.0.1", conf.node_ip.v4);
    assert_eq!("", conf.node_ip.v6);
    assert!(conf.node_ip_auto_generated);
    assert_eq!(1, responder.requests());

    // It is tried three times, with half a second in between.
    let mut conf = RtcConfig {
        use_external_ip: true,
        require_ipv4: true,
        stun_servers: vec!["[::1]:9".to_string()],
        ..Default::default()
    };
    let start = Instant::now();
    assert_eq!(
        "could not resolve external IP: no suitable address found",
        conf.validate(true).unwrap_err().to_string()
    );
    assert!(
        start.elapsed() >= std::time::Duration::from_millis(1500),
        "{:?}",
        start.elapsed()
    );
    assert!(conf.node_ip.is_empty());
    assert!(!conf.node_ip_auto_generated);
}

#[test]
fn test_is_link_local() {
    // New for musivum.
    for (ip, expected) in [
        ("169.254.0.1", true),
        ("169.253.0.1", false),
        ("224.0.0.251", true),
        ("224.0.1.1", false),
        ("fe80::1", true),
        ("febf::1", true),
        ("fec0::1", false),
        ("ff02::fb", true),
        ("ff12::fb", true),
        ("ff05::2", false),
        ("2001:db8::1", false),
        ("127.0.0.1", false),
    ] {
        assert_eq!(
            expected,
            is_link_local(ip.parse::<IpAddr>().unwrap()),
            "{ip}"
        );
    }
}
