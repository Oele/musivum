//! The impairment model of the netem relay: what happens to one packet on one link, that is
//! one direction of one client's path to the server. It is driven by the caller's clock
//! (microseconds), so that it can be tested in virtual time; `main.rs` puts it between
//! sockets.
//!
//! A packet that arrives on a link goes through, in this order:
//!
//! 1. a blackout, which drops everything;
//! 2. loss: independent (`burst` 1) or in bursts (`burst` > 1: a Gilbert model whose bad
//!    state loses every packet and lasts `burst` packets on average, entered so often that
//!    the mean loss rate is `loss`);
//! 3. a bandwidth cap: the link sends at `rate` kbit/s, counting 28 bytes of IPv4 and UDP
//!    header per packet, from a FIFO queue; a packet that would wait in the queue longer than
//!    `queue` ms is dropped (tail drop);
//! 4. delay with jitter: `delay` ms plus an offset that wanders between `-jitter` and
//!    `+jitter` ms (straight lines between random points, never steeper than 0.75 ms per ms,
//!    so the jitter alone never reorders packets);
//! 5. order: no packet leaves before one that arrived earlier on the same link.
//!
//! Every link has its own random generators, seeded from the relay's seed, the client's name
//! and the direction, so that a run with the same seed makes the same decisions for the same
//! sequence of packets.

use std::fmt;

/// Microseconds since the relay started.
pub type Micros = u64;

/// IPv4 and UDP headers: what a packet costs on a link beyond its payload.
pub const IP_UDP_OVERHEAD: usize = 28;

/// The queue of a bandwidth cap when none is given, in milliseconds.
pub const DEFAULT_QUEUE_MS: f64 = 300.0;

/// What one direction of a link does to packets. The default is a clean network.
#[derive(Clone, Debug, PartialEq)]
pub struct Settings {
    /// Mean loss rate, 0 to 1.
    pub loss: f64,
    /// Mean length of a loss burst in packets; 1 or less is independent loss.
    pub burst: f64,
    /// Fixed one-way delay, ms.
    pub delay_ms: f64,
    /// Amplitude of the delay's variation, ms.
    pub jitter_ms: f64,
    /// Bandwidth cap in kbit/s; 0 is none.
    pub rate_kbps: f64,
    /// The cap's queue, as the longest a packet may wait in it, ms.
    pub queue_ms: f64,
    /// Drop everything.
    pub blackout: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            loss: 0.0,
            burst: 1.0,
            delay_ms: 0.0,
            jitter_ms: 0.0,
            rate_kbps: 0.0,
            queue_ms: DEFAULT_QUEUE_MS,
            blackout: false,
        }
    }
}

impl Settings {
    /// Settings from `key=value` words: `loss=0.02 burst=8 delay=150 jitter=30 rate=500
    /// queue=300 blackout=1`. What is not named keeps its default.
    pub fn parse<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Settings, String> {
        let mut s = Settings::default();
        for word in words {
            let (key, value) = word
                .split_once('=')
                .ok_or_else(|| format!("not key=value: {word}"))?;
            let number: f64 = value.parse().map_err(|_| format!("not a number: {word}"))?;
            if !number.is_finite() || number < 0.0 {
                return Err(format!("out of range: {word}"));
            }
            match key {
                "loss" if number <= 1.0 => s.loss = number,
                "burst" => s.burst = number,
                "delay" => s.delay_ms = number,
                "jitter" => s.jitter_ms = number,
                "rate" => s.rate_kbps = number,
                "queue" => s.queue_ms = number,
                "blackout" => s.blackout = number != 0.0,
                "loss" => return Err(format!("out of range: {word}")),
                _ => return Err(format!("unknown setting: {key}")),
            }
        }
        if s.jitter_ms > s.delay_ms {
            return Err("jitter must not exceed delay".into());
        }
        Ok(s)
    }
}

impl fmt::Display for Settings {
    /// The settings that differ from a clean network, as `parse` reads them; `clean` if none.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = Settings::default();
        let mut words = Vec::new();
        if self.blackout {
            words.push("blackout=1".to_string());
        }
        if self.loss != d.loss {
            words.push(format!("loss={}", self.loss));
        }
        if self.burst != d.burst {
            words.push(format!("burst={}", self.burst));
        }
        if self.delay_ms != d.delay_ms {
            words.push(format!("delay={}", self.delay_ms));
        }
        if self.jitter_ms != d.jitter_ms {
            words.push(format!("jitter={}", self.jitter_ms));
        }
        if self.rate_kbps != d.rate_kbps {
            words.push(format!("rate={}", self.rate_kbps));
        }
        if self.queue_ms != d.queue_ms {
            words.push(format!("queue={}", self.queue_ms));
        }
        if words.is_empty() {
            f.write_str("clean")
        } else {
            f.write_str(&words.join(" "))
        }
    }
}

/// Why a packet was dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    Blackout,
    Loss,
    Queue,
}

/// What becomes of a packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Send it at this time.
    Deliver(Micros),
    Drop(DropReason),
}

/// SplitMix64: small, fast, and good enough for loss and jitter decisions.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform in [lo, hi).
    pub fn uniform(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next_f64()
    }
}

/// The seed of one link: the relay's seed, the client's name and the direction.
pub fn seed_for(seed: u64, client: &str, up: bool) -> u64 {
    // FNV-1a over the name, then mixed with the rest.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in client.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    let mut rng = Rng::new(seed ^ h ^ if up { 0x5555 } else { 0xaaaa });
    rng.next_u64()
}

/// The jitter: an offset that moves in straight lines between random points in
/// [-amplitude, +amplitude], one point every `step`, where the step is long enough that the
/// offset never changes faster than 0.75 ms per ms.
#[derive(Clone, Debug)]
struct Jitter {
    rng: Rng,
    amplitude_ms: f64,
    step_us: u64,
    index: u64,
    from: f64,
    to: f64,
}

/// How fast the jitter's offset may change, in ms per ms: below 1, so it never reorders.
const JITTER_MAX_SLOPE: f64 = 0.75;

impl Jitter {
    fn new(seed: u64) -> Jitter {
        Jitter {
            rng: Rng::new(seed),
            amplitude_ms: 0.0,
            step_us: 1,
            index: 0,
            from: 0.0,
            to: 0.0,
        }
    }

    /// The offset at `now`, in microseconds. `now` must not go backwards.
    fn offset_us(&mut self, amplitude_ms: f64, now: Micros) -> f64 {
        if amplitude_ms <= 0.0 {
            self.amplitude_ms = 0.0;
            return 0.0;
        }
        let k = |s: &Jitter| now / s.step_us;
        if amplitude_ms != self.amplitude_ms {
            self.amplitude_ms = amplitude_ms;
            let step_ms = (2.0 * amplitude_ms / JITTER_MAX_SLOPE).max(20.0);
            self.step_us = (step_ms * 1000.0) as u64;
            self.index = k(self);
            self.from = self.draw();
            self.to = self.draw();
        }
        let i = k(self);
        if i == self.index + 1 {
            self.from = self.to;
            self.to = self.draw();
        } else if i > self.index + 1 {
            self.from = self.draw();
            self.to = self.draw();
        }
        self.index = i;
        let u = (now - i * self.step_us) as f64 / self.step_us as f64;
        (self.from + (self.to - self.from) * u) * 1000.0
    }

    fn draw(&mut self) -> f64 {
        self.rng.uniform(-self.amplitude_ms, self.amplitude_ms)
    }
}

/// One direction of one client's path: its state between packets.
#[derive(Clone, Debug)]
pub struct Link {
    loss_rng: Rng,
    /// The Gilbert model's state: in a burst.
    bad: bool,
    /// When the cap has sent everything queued so far.
    busy_until: Micros,
    jitter: Jitter,
    /// When the last delivered packet leaves.
    last_out: Micros,
}

impl Link {
    pub fn new(seed: u64) -> Link {
        let mut seeds = Rng::new(seed);
        Link {
            loss_rng: Rng::new(seeds.next_u64()),
            bad: false,
            busy_until: 0,
            jitter: Jitter::new(seeds.next_u64()),
            last_out: 0,
        }
    }

    /// What becomes of a packet of `size` bytes (UDP payload) that arrives at `now`. `now`
    /// must not go backwards.
    pub fn admit(&mut self, s: &Settings, now: Micros, size: usize) -> Verdict {
        if s.blackout {
            return Verdict::Drop(DropReason::Blackout);
        }
        if self.lose(s) {
            return Verdict::Drop(DropReason::Loss);
        }
        let mut t = now;
        if s.rate_kbps > 0.0 {
            let start = self.busy_until.max(now);
            if (start - now) as f64 > s.queue_ms * 1000.0 {
                return Verdict::Drop(DropReason::Queue);
            }
            // bits / (kbit/s) is ms.
            let tx_us = ((size + IP_UDP_OVERHEAD) * 8) as f64 / s.rate_kbps * 1000.0;
            self.busy_until = start + tx_us.round() as u64;
            t = self.busy_until;
        }
        let delay_us = s.delay_ms * 1000.0 + self.jitter.offset_us(s.jitter_ms, now);
        let out = (t + delay_us.max(0.0).round() as u64).max(self.last_out);
        self.last_out = out;
        Verdict::Deliver(out)
    }

    fn lose(&mut self, s: &Settings) -> bool {
        if s.loss <= 0.0 {
            self.bad = false;
            return false;
        }
        if s.loss >= 1.0 {
            return true;
        }
        let u = self.loss_rng.next_f64();
        if s.burst <= 1.0 {
            return u < s.loss;
        }
        // Simple Gilbert model: the bad state loses everything and is left with probability
        // r = 1/burst per packet; it is entered with probability p such that the share of
        // time in it, p / (p + r), is the loss rate.
        let r = 1.0 / s.burst;
        let p = s.loss * r / (1.0 - s.loss);
        self.bad = if self.bad { u >= r } else { u < p };
        self.bad
    }
}

/// What a packet is, by its first bytes (RFC 7983, and RFC 5761 for RTCP).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Stun,
    Dtls,
    Rtp,
    Rtcp,
    Other,
}

impl Class {
    pub const ALL: [Class; 5] = [
        Class::Stun,
        Class::Dtls,
        Class::Rtp,
        Class::Rtcp,
        Class::Other,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Class::Stun => "stun",
            Class::Dtls => "dtls",
            Class::Rtp => "rtp",
            Class::Rtcp => "rtcp",
            Class::Other => "other",
        }
    }
}

pub fn classify(packet: &[u8]) -> Class {
    match packet.first() {
        Some(0..=3) => Class::Stun,
        Some(20..=63) => Class::Dtls,
        Some(128..=191) => match packet.get(1) {
            Some(192..=223) => Class::Rtcp,
            _ => Class::Rtp,
        },
        _ => Class::Other,
    }
}

#[cfg(test)]
mod tests;
