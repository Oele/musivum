//! netem-relay: stands between the browsers of the call test and the server under test, on
//! the media path, and impairs what passes it (see `lib.rs` for what it does to a packet).
//!
//!   netem-relay --front 127.0.0.2:39539 --server 127.0.0.1:39522 --ports 39523-39537
//!               --control 127.0.0.1:39520 [--seed 1] [--stdin-lifeline]
//!
//! The browsers are given `--front` as the server's ICE candidate, so they send their media
//! there. For every client address that sends to the front, the relay takes a port of
//! `--ports`, and sends that client's packets to the server from it; what the server answers
//! to that port goes back to the client from the front. Each mapping belongs to a client
//! (by name, see `tag`), and each client has a link of its own in each direction: `up` is
//! client to server, `down` server to client.
//!
//! Control, on `--control`: one command per line, one line in answer.
//!
//!   set <up|down|both> [client=<name>] [loss=<0..1>] [burst=<packets>] [delay=<ms>]
//!       [jitter=<ms>] [rate=<kbit/s>] [queue=<ms>] [blackout=<0|1>]
//!         replaces the settings of that direction: of that client, or of every client that
//!         has none of its own. What is not named is clean.               -> ok
//!   clear     every link clean again                                     -> ok
//!   tag <client port> <name>
//!             packets from and to that client port belong to <name>      -> ok
//!   stats     counters, as one line of JSON
//!   quit      exits
//!
//! With `--stdin-lifeline` the relay exits when its standard input closes, so that it does
//! not outlive the script that started it.

use musivum_netem::{Class, DropReason, Link, Micros, Settings, Verdict, classify, seed_for};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Dir {
    Up = 0,
    Down = 1,
}

/// Counters of one direction of one mapping.
#[derive(Default)]
struct Counters {
    /// Packets that reached the relay, by class.
    by_class: [AtomicU64; 5],
    bytes: AtomicU64,
    loss: AtomicU64,
    queue: AtomicU64,
    blackout: AtomicU64,
    /// Packets sent on, by class.
    out_by_class: [AtomicU64; 5],
    out_bytes: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    fn json(&self) -> String {
        let classes = |counts: &[AtomicU64; 5]| {
            let list: Vec<String> = Class::ALL
                .iter()
                .map(|&c| format!("\"{}\":{}", c.name(), counts[c as usize].load(Relaxed)))
                .collect();
            list.join(",")
        };
        format!(
            "{{\"in\":{{{}}},\"bytes\":{},\"loss\":{},\"queue\":{},\"blackout\":{},\"out\":{{{}}},\"out_bytes\":{},\"errors\":{}}}",
            classes(&self.by_class),
            self.bytes.load(Relaxed),
            self.loss.load(Relaxed),
            self.queue.load(Relaxed),
            self.blackout.load(Relaxed),
            classes(&self.out_by_class),
            self.out_bytes.load(Relaxed),
            self.errors.load(Relaxed),
        )
    }
}

/// One client address and the port it is relayed from.
struct Mapping {
    client: SocketAddr,
    back: UdpSocket,
    back_port: u16,
    tag: Mutex<String>,
    counters: [Counters; 2],
}

/// The settings and the links' state.
#[derive(Default)]
struct Net {
    /// For every client without settings of its own, by direction.
    all: [Settings; 2],
    /// A client's own settings, by direction.
    clients: HashMap<String, [Option<Settings>; 2]>,
    links: HashMap<(String, Dir), Link>,
}

enum Dest {
    Server,
    Client,
}

struct Item {
    at: Micros,
    seq: u64,
    mapping: Arc<Mapping>,
    dest: Dest,
    dir: Dir,
    data: Vec<u8>,
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    /// Reversed, so that the heap gives the earliest first.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

#[derive(Default)]
struct Schedule {
    heap: BinaryHeap<Item>,
    seq: u64,
}

struct Relay {
    start: Instant,
    seed: u64,
    front: UdpSocket,
    server: SocketAddr,
    free_ports: Mutex<Vec<u16>>,
    mappings: Mutex<HashMap<SocketAddr, Arc<Mapping>>>,
    tags: Mutex<HashMap<u16, String>>,
    net: Mutex<Net>,
    schedule: Mutex<Schedule>,
    due: Condvar,
    /// Packets from clients that got no mapping because the ports ran out.
    unmapped: AtomicU64,
    /// How late packets left against their schedule, in microseconds.
    late_max: AtomicU64,
    late_sum: AtomicU64,
    sent: AtomicU64,
}

const READ_TIMEOUT: Duration = Duration::from_millis(250);

impl Relay {
    fn now(&self) -> Micros {
        self.start.elapsed().as_micros() as Micros
    }

    /// The mapping of a client address, made on its first packet.
    fn mapping(self: &Arc<Self>, client: SocketAddr) -> Option<Arc<Mapping>> {
        let mut mappings = self.mappings.lock().unwrap();
        if let Some(m) = mappings.get(&client) {
            return Some(m.clone());
        }
        let back = loop {
            let port = self.free_ports.lock().unwrap().pop()?;
            let local = SocketAddr::new(self.server.ip(), port);
            match UdpSocket::bind(local) {
                Ok(s) => break s,
                Err(e) => eprintln!("netem-relay: cannot bind {local}: {e}"),
            }
        };
        let back_port = back.local_addr().ok()?.port();
        if let Err(e) = back
            .connect(self.server)
            .and_then(|_| back.set_read_timeout(Some(READ_TIMEOUT)))
        {
            eprintln!("netem-relay: cannot use port {back_port}: {e}");
            return None;
        }
        let tag = self
            .tags
            .lock()
            .unwrap()
            .get(&client.port())
            .cloned()
            .unwrap_or_default();
        let m = Arc::new(Mapping {
            client,
            back,
            back_port,
            tag: Mutex::new(tag),
            counters: Default::default(),
        });
        mappings.insert(client, m.clone());
        let relay = self.clone();
        let mapping = m.clone();
        thread::spawn(move || relay.receive_back(mapping));
        Some(m)
    }

    /// Client to server: everything that arrives at the front.
    fn receive_front(self: Arc<Self>) {
        let mut buf = vec![0u8; 65536];
        loop {
            match self.front.recv_from(&mut buf) {
                Ok((n, from)) => match self.mapping(from) {
                    Some(m) => self.admit(&m, Dir::Up, buf[..n].to_vec()),
                    None => {
                        self.unmapped.fetch_add(1, Relaxed);
                    }
                },
                Err(e) if is_timeout(&e) => {}
                Err(e) => eprintln!("netem-relay: front: {e}"),
            }
        }
    }

    /// Server to client: what the server sends to one mapping's port.
    fn receive_back(self: Arc<Self>, m: Arc<Mapping>) {
        let mut buf = vec![0u8; 65536];
        loop {
            match m.back.recv(&mut buf) {
                Ok(n) => self.admit(&m, Dir::Down, buf[..n].to_vec()),
                Err(e) if is_timeout(&e) => {}
                // An ICMP error for something sent to the server earlier.
                Err(_) => {
                    m.counters[Dir::Up as usize].errors.fetch_add(1, Relaxed);
                }
            }
        }
    }

    fn admit(&self, m: &Arc<Mapping>, dir: Dir, data: Vec<u8>) {
        let c = &m.counters[dir as usize];
        c.by_class[classify(&data) as usize].fetch_add(1, Relaxed);
        c.bytes.fetch_add(data.len() as u64, Relaxed);
        let tag = m.tag.lock().unwrap().clone();
        let verdict = {
            let mut net = self.net.lock().unwrap();
            let Net {
                all,
                clients,
                links,
            } = &mut *net;
            let settings = clients
                .get(&tag)
                .and_then(|own| own[dir as usize].as_ref())
                .unwrap_or(&all[dir as usize]);
            let seed = self.seed;
            let link = links
                .entry((tag, dir))
                .or_insert_with_key(|(tag, dir)| Link::new(seed_for(seed, tag, *dir == Dir::Up)));
            link.admit(settings, self.now(), data.len())
        };
        let dropped = match verdict {
            Verdict::Drop(DropReason::Loss) => &c.loss,
            Verdict::Drop(DropReason::Queue) => &c.queue,
            Verdict::Drop(DropReason::Blackout) => &c.blackout,
            Verdict::Deliver(at) => {
                let mut s = self.schedule.lock().unwrap();
                s.seq += 1;
                let seq = s.seq;
                s.heap.push(Item {
                    at,
                    seq,
                    mapping: m.clone(),
                    dest: if dir == Dir::Up {
                        Dest::Server
                    } else {
                        Dest::Client
                    },
                    dir,
                    data,
                });
                self.due.notify_one();
                return;
            }
        };
        dropped.fetch_add(1, Relaxed);
    }

    /// Sends every packet when it is due.
    fn send_due(self: Arc<Self>) {
        loop {
            let item = {
                let mut s = self.schedule.lock().unwrap();
                loop {
                    let now = self.now();
                    match s.heap.peek() {
                        None => s = self.due.wait(s).unwrap(),
                        Some(top) if top.at <= now => break s.heap.pop().unwrap(),
                        Some(top) => {
                            let wait = Duration::from_micros(top.at - now);
                            s = self.due.wait_timeout(s, wait).unwrap().0;
                        }
                    }
                }
            };
            let late = self.now().saturating_sub(item.at);
            self.late_max.fetch_max(late, Relaxed);
            self.late_sum.fetch_add(late, Relaxed);
            self.sent.fetch_add(1, Relaxed);
            let m = &item.mapping;
            let result = match item.dest {
                Dest::Server => m.back.send(&item.data),
                Dest::Client => self.front.send_to(&item.data, m.client),
            };
            let c = &m.counters[item.dir as usize];
            match result {
                Ok(n) => {
                    c.out_by_class[classify(&item.data) as usize].fetch_add(1, Relaxed);
                    c.out_bytes.fetch_add(n as u64, Relaxed);
                }
                Err(_) => {
                    c.errors.fetch_add(1, Relaxed);
                }
            }
        }
    }

    fn control(self: Arc<Self>, listener: TcpListener) {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let Ok(mut out) = stream.try_clone() else {
                continue;
            };
            for line in BufReader::new(stream).lines() {
                let Ok(line) = line else { break };
                let answer = match self.command(&line) {
                    Ok(answer) => answer,
                    Err(e) => format!("error {e}"),
                };
                if writeln!(out, "{answer}").is_err() {
                    break;
                }
            }
        }
    }

    fn command(&self, line: &str) -> Result<String, String> {
        let mut words = line.split_whitespace();
        match words.next() {
            Some("set") => {
                let dirs: &[Dir] = match words.next() {
                    Some("up") => &[Dir::Up],
                    Some("down") => &[Dir::Down],
                    Some("both") => &[Dir::Up, Dir::Down],
                    _ => return Err("set up|down|both ...".into()),
                };
                let mut words = words.peekable();
                let client = match words.peek().and_then(|w| w.strip_prefix("client=")) {
                    Some(name) => {
                        let name = check_name(name)?;
                        words.next();
                        Some(name)
                    }
                    None => None,
                };
                let settings = Settings::parse(words)?;
                let mut net = self.net.lock().unwrap();
                for &dir in dirs {
                    match &client {
                        Some(name) => {
                            net.clients.entry(name.clone()).or_default()[dir as usize] =
                                Some(settings.clone())
                        }
                        None => net.all[dir as usize] = settings.clone(),
                    }
                }
                Ok("ok".into())
            }
            Some("clear") => {
                let mut net = self.net.lock().unwrap();
                net.all = Default::default();
                net.clients.clear();
                Ok("ok".into())
            }
            Some("tag") => {
                let port: u16 = words
                    .next()
                    .and_then(|p| p.parse().ok())
                    .ok_or("tag <port> <name>")?;
                let name = check_name(words.next().ok_or("tag <port> <name>")?)?;
                self.tags.lock().unwrap().insert(port, name.clone());
                for m in self.mappings.lock().unwrap().values() {
                    if m.client.port() == port {
                        *m.tag.lock().unwrap() = name.clone();
                    }
                }
                Ok("ok".into())
            }
            Some("stats") => Ok(self.stats()),
            Some("quit") => std::process::exit(0),
            _ => Err(format!("unknown command: {line}")),
        }
    }

    fn stats(&self) -> String {
        let sent = self.sent.load(Relaxed);
        let mut s = format!(
            "{{\"uptime_ms\":{},\"unmapped\":{},\"late_us\":{{\"max\":{},\"mean\":{},\"packets\":{}}},",
            self.now() / 1000,
            self.unmapped.load(Relaxed),
            self.late_max.load(Relaxed),
            self.late_sum.load(Relaxed).checked_div(sent).unwrap_or(0),
            sent,
        );
        {
            let net = self.net.lock().unwrap();
            s += &format!(
                "\"settings\":{{\"up\":\"{}\",\"down\":\"{}\",\"clients\":{{",
                net.all[0], net.all[1]
            );
            let clients: Vec<String> = net
                .clients
                .iter()
                .map(|(name, own)| {
                    let show = |o: &Option<Settings>| {
                        o.as_ref().map(|s| s.to_string()).unwrap_or("-".into())
                    };
                    format!(
                        "\"{name}\":{{\"up\":\"{}\",\"down\":\"{}\"}}",
                        show(&own[0]),
                        show(&own[1])
                    )
                })
                .collect();
            s += &clients.join(",");
            s += "}},";
        }
        let mut mappings: Vec<Arc<Mapping>> =
            self.mappings.lock().unwrap().values().cloned().collect();
        mappings.sort_by_key(|m| m.back_port);
        let list: Vec<String> = mappings
            .iter()
            .map(|m| {
                format!(
                    "{{\"client\":\"{}\",\"tag\":\"{}\",\"port\":{},\"up\":{},\"down\":{}}}",
                    m.client,
                    m.tag.lock().unwrap(),
                    m.back_port,
                    m.counters[0].json(),
                    m.counters[1].json()
                )
            })
            .collect();
        s += &format!("\"mappings\":[{}]}}", list.join(","));
        s
    }
}

fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

/// A client's name: letters, digits, '-' and '_', so that it needs no quoting in JSON.
fn check_name(name: &str) -> Result<String, String> {
    if !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        Ok(name.to_string())
    } else {
        Err(format!("not a name: {name}"))
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("netem-relay: {e}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let mut front = None;
    let mut server = None;
    let mut ports = None;
    let mut control = None;
    let mut seed = 1u64;
    let mut lifeline = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--front" => front = Some(value()?),
            "--server" => server = Some(value()?),
            "--ports" => ports = Some(value()?),
            "--control" => control = Some(value()?),
            "--seed" => seed = value()?.parse().map_err(|_| "--seed: not a number")?,
            "--stdin-lifeline" => lifeline = true,
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    let usage = "usage: netem-relay --front ADDR:PORT --server ADDR:PORT --ports FIRST-LAST --control ADDR:PORT [--seed N] [--stdin-lifeline]";
    let (Some(front), Some(server), Some(ports), Some(control)) = (front, server, ports, control)
    else {
        return Err(usage.into());
    };
    let server: SocketAddr = server.parse().map_err(|e| format!("--server: {e}"))?;
    let (first, last) = ports.split_once('-').ok_or("--ports FIRST-LAST")?;
    let first: u16 = first.parse().map_err(|_| "--ports: not a number")?;
    let last: u16 = last.parse().map_err(|_| "--ports: not a number")?;
    // Taken from the end: the lowest port first.
    let free_ports: Vec<u16> = (first..=last).rev().collect();

    let front = UdpSocket::bind(&front).map_err(|e| format!("--front {front}: {e}"))?;
    front
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|e| e.to_string())?;
    let listener = TcpListener::bind(&control).map_err(|e| format!("--control {control}: {e}"))?;

    let relay = Arc::new(Relay {
        start: Instant::now(),
        seed,
        front,
        server,
        free_ports: Mutex::new(free_ports),
        mappings: Default::default(),
        tags: Default::default(),
        net: Default::default(),
        schedule: Default::default(),
        due: Condvar::new(),
        unmapped: AtomicU64::new(0),
        late_max: AtomicU64::new(0),
        late_sum: AtomicU64::new(0),
        sent: AtomicU64::new(0),
    });
    let r = relay.clone();
    thread::spawn(move || r.send_due());
    let r = relay.clone();
    thread::spawn(move || r.receive_front());
    if lifeline {
        thread::spawn(|| {
            let mut sink = [0u8; 256];
            let mut stdin = io::stdin();
            while matches!(stdin.read(&mut sink), Ok(n) if n > 0) {}
            std::process::exit(0);
        });
    }
    println!("netem-relay: ready");
    relay.control(listener);
    Ok(())
}
