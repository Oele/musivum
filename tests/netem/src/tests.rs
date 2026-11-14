// The model's tests, in virtual time and with fixed seeds: what a link does to many packets
// is within tolerance of what its settings say.

use super::*;

const SEED: u64 = 20261114;

fn settings(words: &str) -> Settings {
    Settings::parse(words.split_whitespace()).unwrap()
}

/// Loss decisions for `n` packets, one per millisecond.
fn losses(s: &Settings, n: usize, seed: u64) -> Vec<bool> {
    let mut link = Link::new(seed);
    (0..n)
        .map(|i| link.admit(s, i as u64 * 1000, 1000) == Verdict::Drop(DropReason::Loss))
        .collect()
}

/// The loss rate and the mean length of runs of lost packets.
fn rate_and_mean_burst(lost: &[bool]) -> (f64, f64) {
    let total = lost.iter().filter(|&&l| l).count();
    let bursts = lost
        .iter()
        .zip(std::iter::once(&false).chain(lost.iter()))
        .filter(|&(&now, &before)| now && !before)
        .count();
    (
        total as f64 / lost.len() as f64,
        total as f64 / bursts as f64,
    )
}

#[test]
fn test_random_loss_rate_and_runs() {
    let lost = losses(&settings("loss=0.02"), 1_000_000, SEED);
    let (rate, burst) = rate_and_mean_burst(&lost);
    assert!((rate - 0.02).abs() < 0.001, "loss rate {rate}");
    // Independent losses: runs are geometric, of mean 1 / (1 - 0.02).
    assert!((burst - 1.0 / 0.98).abs() < 0.02, "mean run {burst}");
}

#[test]
fn test_burst_loss_rate_and_length() {
    let lost = losses(&settings("loss=0.05 burst=8"), 1_000_000, SEED);
    let (rate, burst) = rate_and_mean_burst(&lost);
    assert!((rate - 0.05).abs() < 0.003, "loss rate {rate}");
    assert!((burst - 8.0).abs() < 0.4, "mean burst {burst}");
    let longest = lost.split(|&l| !l).map(<[bool]>::len).max().unwrap_or(0);
    assert!(
        longest > 30,
        "bursts are geometric, some long: longest {longest}"
    );
}

#[test]
fn test_same_seed_same_decisions() {
    let s = settings("loss=0.05 burst=8 delay=150 jitter=30");
    let run = |seed| {
        let mut link = Link::new(seed);
        (0..10_000)
            .map(|i| link.admit(&s, i * 700, 1200))
            .collect::<Vec<_>>()
    };
    assert_eq!(run(SEED), run(SEED));
    assert_ne!(run(SEED), run(SEED + 1));
    assert_ne!(
        seed_for(SEED, "alice", true),
        seed_for(SEED, "alice", false)
    );
    assert_ne!(seed_for(SEED, "alice", true), seed_for(SEED, "bob", true));
    assert_eq!(seed_for(SEED, "bob", false), seed_for(SEED, "bob", false));
}

#[test]
fn test_fixed_delay() {
    let s = settings("delay=150");
    let mut link = Link::new(SEED);
    for i in 0..1000u64 {
        let now = i * 3_000;
        assert_eq!(Verdict::Deliver(now + 150_000), link.admit(&s, now, 1200));
    }
}

#[test]
fn test_delay_and_jitter_distribution() {
    let s = settings("delay=150 jitter=30");
    let mut link = Link::new(SEED);
    let mut delays = Vec::new();
    let mut last_out = 0;
    for i in 0..200_000u64 {
        let now = i * 1000;
        let Verdict::Deliver(out) = link.admit(&s, now, 1200) else {
            panic!("dropped without loss or cap");
        };
        assert!(out >= last_out, "reordered at packet {i}");
        last_out = out;
        delays.push((out - now) as f64 / 1000.0);
    }
    let min = delays.iter().cloned().fold(f64::MAX, f64::min);
    let max = delays.iter().cloned().fold(f64::MIN, f64::max);
    let mean = delays.iter().sum::<f64>() / delays.len() as f64;
    let sd = (delays.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / delays.len() as f64).sqrt();
    assert!(min >= 120.0 && max <= 180.0, "delays in [{min}, {max}]");
    assert!(
        min < 123.0 && max > 177.0,
        "the whole range is used: [{min}, {max}]"
    );
    assert!((mean - 150.0).abs() < 1.5, "mean delay {mean}");
    // Straight lines between independent uniform points: sd = amplitude * sqrt(2) / 3.
    assert!((sd - 30.0 * 2f64.sqrt() / 3.0).abs() < 1.5, "sd {sd}");
    // The jitter moves slower than time: packets 1 ms apart never differ by more than 0.75 ms.
    let steepest = delays
        .windows(2)
        .map(|w| (w[1] - w[0]).abs())
        .fold(0.0, f64::max);
    assert!(steepest <= 0.751, "steepest change {steepest} ms per ms");
}

/// 1222 bytes of payload are 10 000 bits on the wire.
const WIRE_10K_BITS: usize = 1250 - IP_UDP_OVERHEAD;

#[test]
fn test_cap_throughput_and_queue() {
    // 1000 kbit/s offered to a 500 kbit/s cap with a 300 ms queue, for 20 s.
    let s = settings("rate=500 queue=300");
    let mut link = Link::new(SEED);
    let (mut delivered, mut dropped) = (Vec::new(), 0);
    for i in 0..2000u64 {
        let now = i * 10_000;
        match link.admit(&s, now, WIRE_10K_BITS) {
            Verdict::Deliver(out) => delivered.push((now, out)),
            Verdict::Drop(DropReason::Queue) => dropped += 1,
            v => panic!("unexpected {v:?}"),
        }
    }
    // Half gets through, plus what the queue holds at the end.
    assert!(
        (1000..=1020).contains(&delivered.len()),
        "delivered {}",
        delivered.len()
    );
    assert_eq!(2000 - delivered.len(), dropped);
    let span = (delivered.last().unwrap().1 - delivered[0].1) as f64 / 1e6;
    let kbps = (delivered.len() - 1) as f64 * 10.0 / span;
    assert!((kbps - 500.0).abs() < 5.0, "throughput {kbps} kbit/s");
    // Waiting never exceeds the queue: 300 ms, plus the packet's own 20 ms on the wire.
    let worst = delivered.iter().map(|(a, o)| o - a).max().unwrap();
    assert!(worst <= 320_000, "longest {worst} us");
    assert!(worst >= 300_000, "the queue fills: longest {worst} us");
}

#[test]
fn test_cap_below_rate() {
    // 250 kbit/s offered to 500 kbit/s: nothing waits, each packet takes its 20 ms.
    let s = settings("rate=500");
    let mut link = Link::new(SEED);
    for i in 0..1000u64 {
        let now = i * 40_000;
        assert_eq!(
            Verdict::Deliver(now + 20_000),
            link.admit(&s, now, WIRE_10K_BITS)
        );
    }
}

#[test]
fn test_queue_holds_a_burst() {
    // 100 packets at once into 500 kbit/s (20 ms each) with a 300 ms queue: the first 16
    // start within 300 ms, the rest are dropped.
    let s = settings("rate=500 queue=300");
    let mut link = Link::new(SEED);
    let verdicts: Vec<_> = (0..100)
        .map(|_| link.admit(&s, 1_000_000, WIRE_10K_BITS))
        .collect();
    for (i, v) in verdicts.iter().enumerate() {
        if i < 16 {
            assert_eq!(
                Verdict::Deliver(1_000_000 + (i as u64 + 1) * 20_000),
                *v,
                "packet {i}"
            );
        } else {
            assert_eq!(Verdict::Drop(DropReason::Queue), *v, "packet {i}");
        }
    }
}

#[test]
fn test_cap_lifted_drains_in_order() {
    let capped = settings("rate=500 queue=300");
    let mut link = Link::new(SEED);
    let queued: Vec<_> = (0..10)
        .map(|_| link.admit(&capped, 0, WIRE_10K_BITS))
        .collect();
    assert_eq!(Verdict::Deliver(200_000), queued[9]);
    // Lifted: a new packet does not overtake what is still queued.
    assert_eq!(
        Verdict::Deliver(200_000),
        link.admit(&Settings::default(), 1000, 100)
    );
    assert_eq!(
        Verdict::Deliver(300_000),
        link.admit(&Settings::default(), 300_000, 100)
    );
}

#[test]
fn test_blackout() {
    let mut link = Link::new(SEED);
    let black = settings("blackout=1");
    for i in 0..100 {
        assert_eq!(
            Verdict::Drop(DropReason::Blackout),
            link.admit(&black, i, 100)
        );
    }
    assert_eq!(
        Verdict::Deliver(200),
        link.admit(&Settings::default(), 200, 100)
    );
}

#[test]
fn test_classify() {
    assert_eq!(Class::Stun, classify(&[0x00, 0x01, 0x00, 0x00]));
    assert_eq!(Class::Stun, classify(&[0x01, 0x01]));
    assert_eq!(Class::Dtls, classify(&[22, 0xfe, 0xfd]));
    assert_eq!(Class::Dtls, classify(&[23, 0xfe, 0xfd]));
    assert_eq!(Class::Rtp, classify(&[0x80, 96]));
    assert_eq!(Class::Rtp, classify(&[0x90, 0x80 | 96]));
    assert_eq!(Class::Rtcp, classify(&[0x80, 200]));
    assert_eq!(Class::Rtcp, classify(&[0x81, 205]));
    assert_eq!(Class::Other, classify(&[]));
    assert_eq!(Class::Other, classify(&[0x40]));
}

#[test]
fn test_settings() {
    let s = settings("loss=0.05 burst=8 delay=150 jitter=30 rate=500 queue=200 blackout=1");
    assert_eq!(
        "blackout=1 loss=0.05 burst=8 delay=150 jitter=30 rate=500 queue=200",
        s.to_string()
    );
    assert_eq!(s, settings(&s.to_string()));
    assert_eq!("clean", Settings::default().to_string());
    assert!(Settings::parse(["loss=2"]).is_err());
    assert!(Settings::parse(["speed=2"]).is_err());
    assert!(Settings::parse(["delay=10", "jitter=20"]).is_err());
    assert!(Settings::parse(["delay"]).is_err());
}
