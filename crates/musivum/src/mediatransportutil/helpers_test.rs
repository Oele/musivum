// Ported from LiveKit's mediatransportutil, helpers_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The original has one test; the others here are new and
// say so.

use super::*;

#[test]
fn test_time_to_ntp() {
    struct Args {
        ns: Time,
    }
    struct Test {
        name: &'static str,
        args: Args,
        want_ntp: u64,
    }
    let tests = [Test {
        name: "Must return correct NTP time",
        args: Args {
            ns: Time::unix(1602391458, 1234),
        },
        want_ntp: 16369753560730047668,
    }];
    for tt in tests {
        let got_ntp = to_ntp_time(tt.args.ns).0;
        assert_eq!(tt.want_ntp, got_ntp, "{}", tt.name);
    }
}

// New for musivum: the way back, which the original does not test.
#[test]
fn ntp_time_converts_back() {
    let t = Time::unix(1602391458, 1234);
    let back = to_ntp_time(t).time();
    assert!(back.sub(t).abs() <= Duration(1), "{back} against {t}");
    assert_eq!(NtpTime(1 << 32).duration(), time::SECOND);
    assert_eq!(NtpTime(1 << 31).duration(), 500 * time::MILLISECOND);
}

// New for musivum: the round trip time of a receiver report, which the original does not test.
#[test]
fn rtt_from_a_receiver_report() {
    let sent_at = time::now().add(-250 * time::MILLISECOND);
    let sr_ntp = to_ntp_time(sent_at);
    let lsr = ((sr_ntp.0 >> 16) & 0xFFFFFFFF) as u32;
    // The receiver held the report for 100 ms: the round trip is the other 150 ms.
    let report = ReceptionReport {
        last_sender_report: lsr,
        delay: (0.1 * 65536.0) as u32,
        ..Default::default()
    };
    let rtt = get_rtt_ms(&report, sr_ntp, sent_at).unwrap();
    assert!((149..=160).contains(&rtt), "rtt {rtt}");

    assert_eq!(
        get_rtt_ms(&ReceptionReport::default(), sr_ntp, sent_at),
        Err(Error::RttNoLastSenderReport)
    );
    let other = ReceptionReport {
        last_sender_report: lsr.wrapping_add(1),
        ..Default::default()
    };
    assert!(matches!(
        get_rtt_ms(&other, sr_ntp, sent_at),
        Err(Error::RttNotLastSenderReport(_))
    ));
    assert_eq!(
        get_rtt_ms(&report, sr_ntp, Time::ZERO),
        Err(Error::RttNoLastSendTime)
    );
    // A report that claims to have been held longer than the time that has passed.
    let late = ReceptionReport {
        last_sender_report: lsr,
        delay: 10 * 65536,
        ..Default::default()
    };
    assert!(matches!(
        get_rtt_ms(&late, sr_ntp, sent_at),
        Err(Error::RttAnachronousSenderReport(_))
    ));
}
