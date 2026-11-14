// New for musivum: the original has no test of this file.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use super::*;
use crate::testutils::eventually_async;

type Written = Arc<Mutex<Vec<Vec<Box<dyn rtcp::Packet>>>>>;

fn stream(ssrc: u32) -> webrtc::StreamInfo {
    webrtc::StreamInfo {
        ssrc,
        ..Default::default()
    }
}

/// An object that reports its round trip times into the list, and a writer that keeps what
/// the object writes.
fn rtt_from_xr() -> (Arc<RttFromXr>, Arc<Mutex<Vec<u32>>>, Written) {
    let rtts = Arc::new(Mutex::new(Vec::new()));
    let r = RttFromXrFactory::new({
        let rtts = rtts.clone();
        move |rtt| rtts.lock().push(rtt)
    })
    .new_interceptor("");
    let written = Written::default();
    r.bind_rtcp_writer(Arc::new({
        let written = written.clone();
        move |pkts: &[Box<dyn rtcp::Packet>]| -> Result<(), webrtc::Error> {
            written.lock().push(pkts.to_vec());
            Ok(())
        }
    }));
    (r, rtts, written)
}

/// The middle 32 bits of the NTP time now: the unit is 1/65536 s.
fn now_ntp32() -> u32 {
    (util::to_ntp_time(time::now()).0 >> 16) as u32
}

fn dlrr(reports: &[(u32, u32, u32)]) -> Box<dyn rtcp::Packet> {
    Box::new(DLRRReportBlock {
        reports: reports
            .iter()
            .map(|&(ssrc, last_rr, dlrr)| DLRRReport {
                ssrc,
                last_rr,
                dlrr,
            })
            .collect(),
    })
}

fn xr(sender_ssrc: u32, reports: Vec<Box<dyn rtcp::Packet>>) -> Box<dyn rtcp::Packet> {
    Box::new(ExtendedReport {
        sender_ssrc,
        reports,
    })
}

/// The round trip time from a DLRR block, worked out by hand.
///
/// The block says: the receiver reference time report it answers was sent at `last_rr`, and
/// the answer was held back for `dlrr`. The test makes `last_rr` 1.5 s before now
/// (0x1_8000 units of 1/65536 s) and `dlrr` 0.5 s (0x8000 units), so
/// now - last_rr - dlrr = 0x1_8000 - 0x8000 = 0x1_0000 units = 65536 / 65536 s = 1000 ms.
/// The handler reads the clock a moment after the test did, and rounds up: the result is
/// 1000 ms plus that moment, which the test bounds with a second reading of the clock.
#[test]
fn test_rtt_from_dlrr() {
    let (r, rtts, written) = rtt_from_xr();

    let before = now_ntp32();
    let last_rr = before.wrapping_sub(0x1_8000);
    r.read_rtcp(&[xr(
        7,
        vec![
            dlrr(&[(1111, last_rr, 0x8000), (2222, last_rr, 0)]),
            // a second block is a second measurement: 1.5 s - 1.25 s = 250 ms
            dlrr(&[(1111, last_rr, 0x1_4000)]),
        ],
    )]);
    let moment = now_ntp32().wrapping_sub(before) as f64;

    let rtts = rtts.lock().clone();
    // only the first report of a block counts
    assert_eq!(2, rtts.len(), "{rtts:?}");
    let upper = |ms: f64| (ms + moment * 1000.0 / 65536.0).ceil() as u32;
    assert!(
        (1000..=upper(1000.0)).contains(&rtts[0]),
        "{rtts:?}, at most {}",
        upper(1000.0)
    );
    assert!(
        (250..=upper(250.0)).contains(&rtts[1]),
        "{rtts:?}, at most {}",
        upper(250.0)
    );
    // nothing is written in answer to a DLRR block
    assert!(written.lock().is_empty());

    // Only the first extended report of a compound packet is looked at.
    let (r, rtts, _) = rtt_from_xr();
    let last_rr = now_ntp32().wrapping_sub(0x1_0000);
    r.read_rtcp(&[
        Box::new(rtcp::receiver_report::ReceiverReport::default()),
        xr(7, vec![dlrr(&[(1111, last_rr, 0)])]),
        xr(7, vec![dlrr(&[(1111, last_rr, 0)])]),
    ]);
    assert_eq!(1, rtts.lock().len());
}

/// The exchange between two ends: one asks every second with a receiver reference time
/// report, the other answers at once with a DLRR block for each stream it sends, and the
/// first works out the round trip time from the answer.
#[tokio::test(flavor = "multi_thread")]
async fn test_exchange() {
    let wait = Duration::from_secs(10);
    let tick = Duration::from_millis(5);

    // The asking end receives two streams. Nothing is written before a stream is bound.
    let (asking, rtts, asked) = rtt_from_xr();
    asking.bind_remote_stream(&stream(1111));
    asking.bind_remote_stream(&stream(2222));
    let bound = time::now();
    eventually_async(|| !asked.lock().is_empty(), wait, tick).await;
    let waited = time::since(bound);
    assert!(
        waited >= time::Duration::from_millis(900),
        "the first report came after {waited:?}"
    );
    let request = asked.lock()[0].clone();
    assert_eq!(1, request.len());
    let report = request[0]
        .as_any()
        .downcast_ref::<ExtendedReport>()
        .expect("an extended report");
    // in the name of the first stream that was bound
    assert_eq!(1111, report.sender_ssrc);
    assert_eq!(1, report.reports.len());
    let rrt = report.reports[0]
        .as_any()
        .downcast_ref::<ReceiverReferenceTimeReportBlock>()
        .expect("a receiver reference time report");
    let sent = util::NtpTime(rrt.ntp_timestamp).time();
    assert!(
        time::since(sent) < time::Duration::from_secs(5) && !sent.after(time::now()),
        "{sent:?}"
    );

    // The answering end sends two streams, of which one is taken away again.
    let (answering, _, answered) = rtt_from_xr();
    answering.read_rtcp(&request);
    assert!(answered.lock().is_empty(), "nothing is sent: no answer");
    answering.bind_local_stream(&stream(5555));
    answering.bind_local_stream(&stream(6666));
    answering.bind_local_stream(&stream(7777));
    answering.unbind_local_stream(&stream(5555));
    answering.read_rtcp(&request);
    let answer = answered.lock()[0].clone();
    let expected: Vec<Box<dyn rtcp::Packet>> = [7777, 6666]
        .into_iter()
        .map(|ssrc| {
            xr(
                ssrc,
                vec![dlrr(&[(1111, (rrt.ntp_timestamp >> 16) as u32, 0)])],
            )
        })
        .collect();
    assert_eq!(expected, answer);
    assert!(rtts.lock().is_empty());

    // The answer gives the asking end its round trip time: what passed since it asked.
    let elapsed_ms = (time::since(sent).nanoseconds() as f64 / 1e6).ceil() as u32;
    asking.read_rtcp(&answer);
    let after_ms = (time::since(sent).nanoseconds() as f64 / 1e6).ceil() as u32;
    let got = rtts.lock().clone();
    assert_eq!(1, got.len());
    assert!(
        (elapsed_ms.saturating_sub(1)..=after_ms + 1).contains(&got[0]),
        "rtt {} ms, {elapsed_ms} to {after_ms} ms passed",
        got[0]
    );

    // With the first stream gone the next one's name is used, and with none nothing is
    // asked. Closing ends the asking for good.
    asking.unbind_remote_stream(&stream(1111));
    let count = asked.lock().len();
    eventually_async(|| asked.lock().len() > count, wait, tick).await;
    let next = asked.lock().last().unwrap().clone();
    assert_eq!(
        2222,
        next[0]
            .as_any()
            .downcast_ref::<ExtendedReport>()
            .unwrap()
            .sender_ssrc
    );
    asking.close();
    asking.close();
    let count = asked.lock().len();
    asking.bind_remote_stream(&stream(3333));
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(count, asked.lock().len());
}
