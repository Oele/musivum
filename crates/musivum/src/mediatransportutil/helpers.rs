// Ported from LiveKit's mediatransportutil, helpers.go at v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, and the two that the original
// wraps with details (`fmt.Errorf("%w, ...")`) carry those details as text.

use crate::go::time::{self, Duration, Time};
use crate::webrtc::rtcp::reception_report::ReceptionReport;

// time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
const NTP_EPOCH: Time = Time::unix(-2_208_988_800, 0);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct NtpTime(pub u64);

impl NtpTime {
    pub fn duration(self) -> Duration {
        let t = self.0;
        let sec = (t >> 32).wrapping_mul(1_000_000_000);
        let frac = (t & 0xffffffff).wrapping_mul(1_000_000_000);
        let mut nsec = frac >> 32;
        if frac as u32 >= 0x80000000 {
            nsec += 1;
        }
        Duration(sec.wrapping_add(nsec) as i64)
    }

    pub fn time(self) -> Time {
        NTP_EPOCH.add(self.duration())
    }
}

pub fn to_ntp_time(t: Time) -> NtpTime {
    let mut nsec = t.sub(NTP_EPOCH).nanoseconds() as u64;
    let sec = nsec / 1_000_000_000;
    nsec = (nsec.wrapping_sub(sec.wrapping_mul(1_000_000_000))) << 32;
    let mut frac = nsec / 1_000_000_000;
    if nsec % 1_000_000_000 >= 1_000_000_000 / 2 {
        frac += 1;
    }
    NtpTime(sec << 32 | frac)
}

// ------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("no last sender report")]
    RttNoLastSenderReport,
    #[error("not last sender report, {0}")]
    RttNotLastSenderReport(String),
    #[error("no last sent time")]
    RttNoLastSendTime,
    #[error("anachronous sender report, {0}")]
    RttAnachronousSenderReport(String),
}

fn get_rtt_ms_inner(
    report: &ReceptionReport,
    last_sr_ntp: NtpTime,
    last_sent_at: Time,
    ignore_last: bool,
) -> Result<u32, Error> {
    if report.last_sender_report == 0 {
        return Err(Error::RttNoLastSenderReport);
    }

    if !ignore_last && report.last_sender_report != ((last_sr_ntp.0 >> 16) & 0xFFFFFFFF) as u32 {
        return Err(Error::RttNotLastSenderReport(format!(
            "lastSR: 0x{:x}, received: 0x{:x}",
            ((last_sr_ntp.0 >> 16) & 0xFFFFFFFF) as u32,
            report.last_sender_report
        )));
    }

    if !ignore_last && last_sent_at.is_zero() {
        return Err(Error::RttNoLastSendTime);
    }

    // RTT calculation reference: https://datatracker.ietf.org/doc/html/rfc3550#section-6.4.1

    // middle 32-bits of current NTP time
    let wall = time::now();
    let mut now = wall;
    let mut time_since_last_sr = Duration(0);
    if !ignore_last {
        time_since_last_sr = time::since(last_sent_at);
        now = last_sr_ntp.time().add(time_since_last_sr);
    }
    let now_ntp = to_ntp_time(now);
    let now_ntp32 = (now_ntp.0 >> 16) as u32;
    if now_ntp32
        .wrapping_sub(report.last_sender_report)
        .wrapping_sub(report.delay)
        > (1 << 31)
    {
        return Err(Error::RttAnachronousSenderReport(format!(
            "lastSRNTP: {} / {}, lastSentAt: {}, since: {}, wall: {}, now: {}, nowNTP: {}, lsr: {}, dlsr: {} / {}, diff: {}",
            last_sr_ntp.0,
            last_sr_ntp.time(),
            last_sent_at,
            time_since_last_sr,
            wall,
            now,
            now_ntp.0,
            report.last_sender_report,
            report.delay,
            Duration::from_secs_f64(report.delay as f64 / 65536.),
            now_ntp32
                .wrapping_sub(report.last_sender_report)
                .wrapping_sub(report.delay) as i32,
        )));
    }

    let ntp_diff = now_ntp32
        .wrapping_sub(report.last_sender_report)
        .wrapping_sub(report.delay);
    Ok((ntp_diff as f64 * 1000.0 / 65536.0).ceil() as u32)
}

pub fn get_rtt_ms(
    report: &ReceptionReport,
    last_sr_ntp: NtpTime,
    last_sent_at: Time,
) -> Result<u32, Error> {
    get_rtt_ms_inner(report, last_sr_ntp, last_sent_at, false)
}

pub fn get_rtt_ms_from_receiver_report_only(report: &ReceptionReport) -> Result<u32, Error> {
    get_rtt_ms_inner(report, NtpTime(0), Time::ZERO, true)
}

#[cfg(test)]
#[path = "helpers_test.rs"]
mod helpers_test;
