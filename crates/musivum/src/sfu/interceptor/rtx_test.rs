// New for musivum: the original has no test of this file. (Upstream tests the pairing through
// its transport, in `test/rtx_pairing_integration_test.go`; `sink_test.rs` has what that test
// does with packets, as far as it can be done here.)

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::*;
use crate::sfu::buffer::StreamInfoProbe;
use crate::webrtc::interceptor::RtpHeaderExtension;

type Pairs = Arc<Mutex<Vec<(u32, u32, String)>>>;
type Found = Arc<Mutex<Vec<u32>>>;

/// A prober that keeps the probes it is given, and refuses the SSRCs it was told to.
#[derive(Default)]
struct Prober {
    probes: Mutex<Vec<(u32, StreamInfoProbe)>>,
    refuse: Mutex<Vec<u32>>,
}

impl StreamInfoProber for Prober {
    fn set_stream_info_probe(&self, ssrc: u32, probe: Option<StreamInfoProbe>) -> bool {
        if self.refuse.lock().contains(&ssrc) {
            return false;
        }
        self.probes
            .lock()
            .push((ssrc, probe.expect("a probe is installed")));
        true
    }
}

fn factory(
    sim_tracks: &HashMap<u32, SimulcastTrackInfo>,
) -> (Arc<RtxInfoExtractorFactory>, Arc<Prober>, Pairs, Found) {
    let prober = Arc::new(Prober::default());
    let pairs = Pairs::default();
    let found = Found::default();
    let f = RtxInfoExtractorFactory::new(
        {
            let found = found.clone();
            move |info| found.lock().push(info.ssrc)
        },
        {
            let pairs = pairs.clone();
            move |repair, base, rsid| pairs.lock().push((repair, base, rsid.to_string()))
        },
        prober.clone(),
        sim_tracks,
        Logger::default(),
    );
    (f, prober, pairs, found)
}

fn pair(repair: u32, base: u32, rsid: &str) -> (u32, u32, String) {
    (repair, base, rsid.to_string())
}

fn extensions(uris: &[&str]) -> Vec<RtpHeaderExtension> {
    uris.iter()
        .enumerate()
        .map(|(i, uri)| RtpHeaderExtension {
            uri: uri.to_string(),
            id: i as i32 + 4,
        })
        .collect()
}

#[test]
fn test_set_stream_info_pairs_base_and_repair_in_either_order() {
    let (f, _, pairs, _) = factory(&HashMap::new());

    // the three layers of one media section, then their repair streams
    f.set_stream_info(1001, "0", "q", "");
    f.set_stream_info(1002, "0", "h", "");
    f.set_stream_info(1003, "0", "f", "");
    assert!(pairs.lock().is_empty());
    f.set_stream_info(2002, "0", "", "h");
    assert_eq!(vec![pair(2002, 1002, "h")], *pairs.lock());
    f.set_stream_info(2001, "0", "", "q");
    f.set_stream_info(2003, "0", "", "f");
    assert_eq!(
        vec![
            pair(2002, 1002, "h"),
            pair(2001, 1001, "q"),
            pair(2003, 1003, "f")
        ],
        *pairs.lock()
    );

    // the repair stream is seen before the stream it repairs
    f.set_stream_info(4001, "1", "", "q");
    assert_eq!(3, pairs.lock().len());
    f.set_stream_info(3001, "1", "q", "");
    assert_eq!(Some(&pair(4001, 3001, "q")), pairs.lock().last());

    // the same rid in another media section is another stream
    f.set_stream_info(5001, "2", "q", "");
    f.set_stream_info(6001, "3", "", "q");
    assert_eq!(4, pairs.lock().len());

    // what does not say which stream it is, is not kept: no mid, or neither rid nor rsid
    f.set_stream_info(7001, "", "q", "");
    f.set_stream_info(7002, "4", "", "");
    f.set_stream_info(8001, "", "", "q");
    f.set_stream_info(8002, "4", "", "q");
    assert_eq!(4, pairs.lock().len());
    f.set_stream_info(7002, "4", "q", "");
    assert_eq!(Some(&pair(8002, 7002, "q")), pairs.lock().last());
}

#[test]
fn test_set_stream_info_a_stream_not_yet_paired_can_be_corrected() {
    let (f, _, pairs, _) = factory(&HashMap::new());

    // a first report of the stream that turns out wrong: what is said later replaces it
    f.set_stream_info(1001, "0", "h", "");
    f.set_stream_info(1001, "0", "q", "");
    f.set_stream_info(2001, "0", "", "h");
    assert!(
        pairs.lock().is_empty(),
        "paired with a rid that was taken back"
    );
    f.set_stream_info(2002, "0", "", "q");
    assert_eq!(vec![pair(2002, 1001, "q")], *pairs.lock());

    // once paired, a stream stays as it is: the probe and the migration info (or the
    // transport, when the track is published) report the same stream, and the second report
    // must not pair it again
    f.set_stream_info(1001, "0", "h", "");
    f.set_stream_info(1001, "0", "q", "");
    f.set_stream_info(2002, "0", "", "q");
    assert_eq!(1, pairs.lock().len());

    // the repair stream that was waiting for "h" is still waiting, and gets its stream
    f.set_stream_info(1002, "0", "h", "");
    assert_eq!(
        vec![pair(2002, 1001, "q"), pair(2001, 1002, "h")],
        *pairs.lock()
    );
}

#[test]
fn test_seed_simulcast_tracks() {
    let track = |stream_id: &str, repair_ssrc: u32, is_repair_stream: bool| SimulcastTrackInfo {
        mid: "0".to_string(),
        stream_id: stream_id.to_string(),
        repair_ssrc,
        is_repair_stream,
    };

    // the stream names its repair stream, the repair stream has an entry of its own too
    let sim_tracks = HashMap::from([
        (1001, track("q", 2001, false)),
        (2001, track("q", 0, true)),
        // only the entry of the repair stream says which it is
        (1002, track("h", 0, false)),
        (2002, track("h", 0, true)),
        // the stream names a repair stream that has no entry
        (1003, track("f", 2003, false)),
        // entries that do not say enough are passed over
        (
            1004,
            SimulcastTrackInfo {
                mid: String::new(),
                ..track("x", 2004, false)
            },
        ),
        (1005, track("", 2005, false)),
    ]);
    let (f, _, pairs, _) = factory(&sim_tracks);
    let mut seeded = pairs.lock().clone();
    seeded.sort();
    assert_eq!(
        vec![
            pair(2001, 1001, "q"),
            pair(2002, 1002, "h"),
            pair(2003, 1003, "f")
        ],
        seeded
    );

    // what the packets say afterwards changes nothing
    f.set_stream_info(1001, "0", "q", "");
    f.set_stream_info(2001, "0", "", "q");
    assert_eq!(3, pairs.lock().len());
}

#[test]
fn test_bind_remote_stream() {
    let (f, prober, pairs, found) = factory(&HashMap::new());
    let extractor = f.new_interceptor("");

    let all = extensions(&[
        sdp::SDES_MID_URI,
        sdp::SDES_RTP_STREAM_ID_URI,
        SDES_REPAIR_RTP_STREAM_ID_URI,
    ]);
    let info = |ssrc: u32, rtp_header_extensions: Vec<RtpHeaderExtension>| webrtc::StreamInfo {
        ssrc,
        rtp_header_extensions,
        mime_type: "video/VP8".to_string(),
        ..Default::default()
    };

    // a stream is reported as found whatever its header extensions; a probe is installed
    // only where it can read all three
    extractor.bind_remote_stream(&info(
        1,
        extensions(&[sdp::SDES_MID_URI, sdp::SDES_RTP_STREAM_ID_URI]),
    ));
    extractor.bind_remote_stream(&info(
        2,
        extensions(&[sdp::SDES_MID_URI, SDES_REPAIR_RTP_STREAM_ID_URI]),
    ));
    extractor.bind_remote_stream(&info(
        3,
        extensions(&[sdp::SDES_RTP_STREAM_ID_URI, SDES_REPAIR_RTP_STREAM_ID_URI]),
    ));
    assert_eq!(vec![1, 2, 3], *found.lock());
    assert!(prober.probes.lock().is_empty());

    extractor.bind_remote_stream(&info(1001, all.clone()));
    extractor.bind_remote_stream(&info(2001, all.clone()));
    assert_eq!(vec![1, 2, 3, 1001, 2001], *found.lock());
    let probes = prober.probes.lock().clone();
    assert_eq!(
        vec![1001, 2001],
        probes.iter().map(|(ssrc, _)| *ssrc).collect::<Vec<_>>()
    );
    for (_, probe) in &probes {
        assert_eq!(
            (4, 5, 6, 10),
            (
                probe.mid_ext_id,
                probe.rid_ext_id,
                probe.rsid_ext_id,
                probe.tries
            )
        );
    }

    // what the probes find is what pairs the streams
    (probes[0].1.on_found)(1001, "0", "q", "");
    (probes[1].1.on_found)(2001, "0", "", "q");
    assert_eq!(vec![pair(2001, 1001, "q")], *pairs.lock());

    // a stream without a buffer cannot be probed: that is logged, and the stream was still
    // reported
    prober.refuse.lock().push(3001);
    let log = crate::testutils::capture_log(tracing::Level::WARN, || {
        extractor.bind_remote_stream(&info(3001, all.clone()));
    });
    assert!(
        log.contains("could not install stream info probe, rtx pairing will not work"),
        "{log}"
    );
    assert!(log.contains("ssrc=3001"), "{log}");
    assert_eq!(Some(&3001), found.lock().last());
    assert_eq!(2, prober.probes.lock().len());

    // a probe does not keep the factory alive
    drop(extractor);
    drop(f);
    (probes[0].1.on_found)(5001, "0", "q", "");
    (probes[1].1.on_found)(6001, "0", "", "q");
    assert_eq!(1, pairs.lock().len());
}
