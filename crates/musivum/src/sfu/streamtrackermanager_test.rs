// New for musivum: the original has no test of this file. What is expected in these tests is
// what the original does at the same steps: a Go test that takes them, laid over `pkg/sfu` at
// the pin with `go test -overlay`, printed the events and the states, and its lines are the
// texts here (which is why they are written the way Go writes slices).

use std::fmt::Display;

use parking_lot::Mutex;

use super::*;
use crate::protocol::logger;
use crate::sfu::rtpextension::dependencydescriptor as dd;
use crate::sfu::streamtracker::{StreamStatus, StreamTrackerWorker};
use crate::testutils;

// Go's `%v` of a slice.
fn go<T: Display>(v: &[T]) -> String {
    let v: Vec<String> = v.iter().map(ToString::to_string).collect();
    format!("[{}]", v.join(" "))
}

fn go_bitrates(brs: &Bitrates) -> String {
    let rows: Vec<String> = brs.iter().map(|row| go(row)).collect();
    go(&rows)
}

// A listener that writes down what it is told. Like the receiver, it asks the manager from
// inside every call: the calls come with the manager's lock released.
struct Recorder {
    s: Weak<StreamTrackerManager>,
    events: Mutex<Vec<String>>,
    // the bitrates of the reports, which are measured as time passes
    reports: Mutex<Vec<Bitrates>>,
}

impl Recorder {
    fn new(s: &Arc<StreamTrackerManager>) -> Arc<Recorder> {
        Arc::new(Recorder {
            s: Arc::downgrade(s),
            events: Mutex::default(),
            reports: Mutex::default(),
        })
    }

    // what `set_listener` takes
    fn listener(self: &Arc<Self>) -> Option<Weak<dyn StreamTrackerManagerListener>> {
        let listener: Weak<Recorder> = Arc::downgrade(self);
        Some(listener)
    }

    fn s(&self) -> Arc<StreamTrackerManager> {
        self.s.upgrade().expect("the manager is there")
    }

    fn add(&self, event: String) {
        self.events.lock().push(event);
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.events.lock())
    }

    // Waits until `n` events have come.
    async fn wait(&self, n: usize) {
        testutils::with_timeout_async(
            || {
                let events = self.events.lock();
                if events.len() >= n {
                    return String::new();
                }
                format!("waited for {n} events, have {events:?}")
            },
            None,
        )
        .await;
    }
}

impl StreamTrackerManagerListener for Recorder {
    fn on_available_layers_changed(&self) {
        let (al, _) = self.s().get_layered_bitrate();
        self.add(format!("available {}", go(&al)));
    }

    fn on_bitrate_availability_changed(&self) {
        self.s().get_layered_bitrate();
        self.add("bitrate-availability".to_string());
    }

    fn on_max_published_layer_changed(&self, max_published_layer: i32) {
        self.add(format!("max-published {max_published_layer}"));
    }

    fn on_max_temporal_layer_seen_changed(&self, max_temporal_layer_seen: i32) {
        self.add(format!(
            "max-temporal-seen {max_temporal_layer_seen} get {}",
            self.s().get_max_temporal_layer_seen()
        ));
    }

    fn on_max_available_layer_changed(&self, max_available_layer: i32) {
        self.add(format!("max-available {max_available_layer}"));
    }

    fn on_bitrate_report(&self, available_layers: &[i32], bitrates: Bitrates) {
        self.reports.lock().push(bitrates);
        self.add(format!(
            "bitrate-report {} {} distance {}",
            go(available_layers),
            go_bitrates(&bitrates),
            self.s().distance_to_desired()
        ));
    }
}

// A track info with one codec, which has these spatial layers.
fn track_info(
    r#type: livekit::TrackType,
    source: livekit::TrackSource,
    mime_type: &str,
    mode: livekit::video_layer::Mode,
    spatial_layers: &[i32],
    muted: bool,
) -> livekit::TrackInfo {
    livekit::TrackInfo {
        r#type: r#type as i32,
        source: source as i32,
        muted,
        codecs: vec![livekit::SimulcastCodecInfo {
            mime_type: mime_type.to_string(),
            layers: spatial_layers
                .iter()
                .map(|&spatial_layer| livekit::VideoLayer {
                    quality: spatial_layer,
                    spatial_layer,
                    ..Default::default()
                })
                .collect(),
            video_layer_mode: mode as i32,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn video_track_info(
    mime_type: &str,
    mode: livekit::video_layer::Mode,
    spatial_layers: &[i32],
    muted: bool,
) -> livekit::TrackInfo {
    track_info(
        livekit::TrackType::Video,
        livekit::TrackSource::Camera,
        mime_type,
        mode,
        spatial_layers,
        muted,
    )
}

// Trackers by packets that are active after one cycle with a packet, for the three layers.
fn config(cycle: [Duration; 3], bitrate: [Duration; 3]) -> StreamTrackerConfig {
    let packet = |cycle_duration| streamtracker::StreamTrackerPacketConfig {
        samples_required: 1,
        cycles_required: 1,
        cycle_duration,
    };
    StreamTrackerConfig {
        stream_tracker_type: StreamTrackerType::Packet,
        bitrate_report_interval: HashMap::from([(0, bitrate[0]), (1, bitrate[1]), (2, bitrate[2])]),
        packet_tracker: HashMap::from([
            (0, packet(cycle[0])),
            (1, packet(cycle[1])),
            (2, packet(cycle[2])),
        ]),
        ..Default::default()
    }
}

// `createDescriptorDependencyForTargets` of the original's `streamtracker_dd_test.go`: a
// descriptor for these layers, all of its decode targets active and required.
fn descriptor(max_spatial: i64, max_temporal: i64) -> buffer::ExtDependencyDescriptor {
    let mut targets: Vec<buffer::DependencyDescriptorDecodeTarget> = Vec::new();
    let mut mask: u32 = 0;
    for i in 0..=max_spatial {
        for j in 0..=max_temporal {
            targets.push(buffer::DependencyDescriptorDecodeTarget {
                target: targets.len() as i64,
                layer: buffer::VideoLayer {
                    spatial: i as i32,
                    temporal: j as i32,
                },
            });
            mask |= 1 << (targets.len() - 1) as u32;
        }
    }

    let dtis = vec![dd::DecodeTargetIndication::Required; targets.len()];

    buffer::ExtDependencyDescriptor {
        descriptor: Some(dd::DependencyDescriptor {
            active_decode_targets_bitmask: Some(mask),
            frame_dependencies: Some(Arc::new(dd::FrameDependencyTemplate {
                decode_target_indications: dtis,
                ..Default::default()
            })),
            ..Default::default()
        }),
        decode_targets: targets,
        active_decode_targets_updated: true,
        ..Default::default()
    }
}

// A packet of 1000 bytes in temporal layer 0.
fn packet(tracker: &Arc<dyn StreamTrackerWorker>) {
    tracker.observe(0, 1000, 900, false, 0, None);
}

// A packet with a descriptor for these layers. It has a payload and no bytes: the layers
// are told from it, and the worker has no bitrate to tell of.
fn dd_packet(tracker: &Arc<dyn StreamTrackerWorker>, max_spatial: i64, max_temporal: i64) {
    let ext_dd = descriptor(max_spatial, max_temporal);
    tracker.observe(0, 0, 100, false, 0, Some(&ext_dd));
}

// What the manager says of itself, in the words of the Go test.
fn state_of(s: &StreamTrackerManager) -> String {
    let (al, brs) = s.get_layered_bitrate();
    let trackers: Vec<String> = (0..=buffer::DEFAULT_MAX_LAYER_SPATIAL)
        .map(|layer| match s.get_tracker(layer) {
            Some(tracker) => tracker.status().to_string(),
            None => "none".to_string(),
        })
        .collect();
    let max_expected = s.inner.read().max_expected_layer;
    format!(
        "available {} bitrates {} trackers {} maxPublished {} maxExpected {} maxTemporalSeen {} distance {}",
        go(&al),
        go_bitrates(&brs),
        go(&trackers),
        s.get_max_published_layer(),
        max_expected,
        s.get_max_temporal_layer_seen(),
        s.distance_to_desired(),
    )
}

// The events since the last step, and the state now.
#[track_caller]
fn step(step: &str, r: &Recorder, events: &[&str], state: &str) {
    assert_eq!(r.take(), events, "{step}");
    assert_eq!(state_of(&r.s()), state, "{step}");
}

// A bitrate over a time that is measured as it passes comes out a little under what it is by
// the numbers (9988 for 10000 in the original): no bitrate where none is expected, else more
// than half of the expected one and not more than it.
#[track_caller]
fn assert_bitrates(actual: &Bitrates, expected: Bitrates, step: &str) {
    for (actual, expected) in actual.iter().flatten().zip(expected.iter().flatten()) {
        assert!(
            (*expected == 0 && *actual == 0) || (expected / 2 < *actual && actual <= expected),
            "{step}: got {actual} where {expected} is expected",
        );
    }
}

// A step whose bitrates are measured: they are compared with what they are by the numbers,
// and stand as `<measured>` in the texts.
#[track_caller]
fn measured_step(step: &str, r: &Recorder, expected: Bitrates, events: &[&str], state: &str) {
    let report = *r.reports.lock().last().expect("a report");
    assert_bitrates(&report, expected, step);
    let reported = go_bitrates(&report);
    let events_now: Vec<String> = r
        .take()
        .iter()
        .map(|event| event.replace(&reported, "<measured>"))
        .collect();
    assert_eq!(events_now, events, "{step}");
    let (_, brs) = r.s().get_layered_bitrate();
    assert_bitrates(&brs, expected, step);
    assert_eq!(
        state_of(&r.s()).replace(&go_bitrates(&brs), "<measured>"),
        state,
        "{step}"
    );
}

// Waits until nothing holds what `weak` points to any more.
async fn gone<T: ?Sized>(weak: &Weak<T>, what: &str) {
    testutils::with_timeout_async(
        || {
            if weak.upgrade().is_none() {
                return String::new();
            }
            format!("{what} is still there")
        },
        Some(std::time::Duration::from_secs(5)),
    )
    .await;
}

// ---------------------------------------------------

// The configuration as Go's `%+v` writes it.
fn go_config(config: &StreamTrackerConfig) -> String {
    fn go_map<V>(map: &HashMap<i32, V>, value: impl Fn(&V) -> String) -> String {
        let mut keys: Vec<&i32> = map.keys().collect();
        keys.sort();
        let entries: Vec<String> = keys
            .into_iter()
            .map(|key| format!("{key}:{}", value(&map[key])))
            .collect();
        format!("map[{}]", entries.join(" "))
    }

    format!(
        "{{StreamTrackerType:{} BitrateReportInterval:{} PacketTracker:{} FrameTracker:{}}}",
        match config.stream_tracker_type {
            StreamTrackerType::Packet => "packet",
            StreamTrackerType::Frame => "frame",
            StreamTrackerType::Unknown => "",
        },
        go_map(&config.bitrate_report_interval, |d| d.to_string()),
        go_map(&config.packet_tracker, |c| format!(
            "{{SamplesRequired:{} CyclesRequired:{} CycleDuration:{}}}",
            c.samples_required, c.cycles_required, c.cycle_duration
        )),
        go_map(&config.frame_tracker, |c| format!(
            "{{MinFPS:{}}}",
            c.min_fps
        )),
    )
}

#[test]
fn the_default_configurations_are_the_originals() {
    assert_eq!(
        go_config(&DEFAULT_STREAM_TRACKER_CONFIG_VIDEO),
        "{StreamTrackerType:packet BitrateReportInterval:map[0:1s 1:1s 2:1s] PacketTracker:map[0:{SamplesRequired:1 CyclesRequired:4 CycleDuration:500ms} 1:{SamplesRequired:5 CyclesRequired:20 CycleDuration:500ms} 2:{SamplesRequired:5 CyclesRequired:20 CycleDuration:500ms}] FrameTracker:map[0:{MinFPS:5} 1:{MinFPS:5} 2:{MinFPS:5}]}"
    );
    assert_eq!(
        go_config(&DEFAULT_STREAM_TRACKER_CONFIG_SCREENSHARE),
        "{StreamTrackerType:packet BitrateReportInterval:map[0:4s 1:4s 2:4s] PacketTracker:map[0:{SamplesRequired:1 CyclesRequired:1 CycleDuration:2s} 1:{SamplesRequired:1 CyclesRequired:1 CycleDuration:2s} 2:{SamplesRequired:1 CyclesRequired:1 CycleDuration:2s}] FrameTracker:map[0:{MinFPS:0.5} 1:{MinFPS:0.5} 2:{MinFPS:0.5}]}"
    );
    assert_eq!(
        DEFAULT_STREAM_TRACKER_MANAGER_CONFIG.video,
        *DEFAULT_STREAM_TRACKER_CONFIG_VIDEO
    );
    assert_eq!(
        DEFAULT_STREAM_TRACKER_MANAGER_CONFIG.screenshare,
        *DEFAULT_STREAM_TRACKER_CONFIG_SCREENSHARE
    );

    // the defaults of the types are Go's zero values
    assert_eq!(
        go_config(&StreamTrackerConfig::default()),
        "{StreamTrackerType: BitrateReportInterval:map[] PacketTracker:map[] FrameTracker:map[]}"
    );
    assert_eq!(
        StreamTrackerPacketConfig::default(),
        StreamTrackerPacketConfig {
            samples_required: 0,
            cycles_required: 0,
            cycle_duration: Duration::ZERO,
        }
    );
    assert_eq!(
        StreamTrackerFrameConfig::default(),
        StreamTrackerFrameConfig { min_fps: 0.0 }
    );
}

// Which trackers there are for a track of a source with a configuration: one by one for the
// layers -1 to 3, and then, in a manager of its own, those of a dependency descriptor.
#[tokio::test(flavor = "multi_thread")]
async fn trackers_are_made_as_the_configuration_for_the_source_says() {
    let hour = [time::HOUR; 3];
    let packet_config = config(hour, hour);
    let frame_config = StreamTrackerConfig {
        stream_tracker_type: StreamTrackerType::Frame,
        frame_tracker: HashMap::from([(
            0,
            streamtracker::StreamTrackerFrameConfig { min_fps: 5.0 },
        )]),
        ..packet_config.clone()
    };
    let no_type = StreamTrackerConfig {
        stream_tracker_type: StreamTrackerType::Unknown,
        ..frame_config.clone()
    };
    let interval_of_1_only = StreamTrackerConfig {
        bitrate_report_interval: HashMap::from([(1, time::HOUR)]),
        ..packet_config.clone()
    };
    let mut no_packet_config_1 = packet_config.clone();
    no_packet_config_1.packet_tracker.remove(&1);

    let video = |video: &StreamTrackerConfig| StreamTrackerManagerConfig {
        video: video.clone(),
        ..Default::default()
    };
    let screenshare = |screenshare: &StreamTrackerConfig| StreamTrackerManagerConfig {
        screenshare: screenshare.clone(),
        ..Default::default()
    };
    use livekit::TrackSource::{Camera, Microphone, ScreenShare, ScreenShareAudio, Unknown};
    for (source, cfg, expected) in [
        // camera, video packet
        (
            Camera,
            video(&packet_config),
            "false true true true false maxPublished 2 dd true true/true true/true true/true maxPublished 2",
        ),
        // camera, screenshare packet
        (
            Camera,
            screenshare(&packet_config),
            "false false false false false maxPublished -1 dd false false/false false/false false/false maxPublished -1",
        ),
        // screen share, video packet
        (
            ScreenShare,
            video(&packet_config),
            "false false false false false maxPublished -1 dd false false/false false/false false/false maxPublished -1",
        ),
        // screen share, screenshare packet
        (
            ScreenShare,
            screenshare(&packet_config),
            "false true true true false maxPublished 2 dd true true/true true/true true/true maxPublished 2",
        ),
        // unknown, video packet
        (
            Unknown,
            video(&packet_config),
            "false true true true false maxPublished 2 dd true true/true true/true true/true maxPublished 2",
        ),
        // microphone, video packet
        (
            Microphone,
            video(&packet_config),
            "false true true true false maxPublished 2 dd true true/true true/true true/true maxPublished 2",
        ),
        // screen share audio, video packet
        (
            ScreenShareAudio,
            video(&packet_config),
            "false true true true false maxPublished 2 dd true true/true true/true true/true maxPublished 2",
        ),
        // camera, video frame
        (
            Camera,
            video(&frame_config),
            "false true false false false maxPublished 0 dd true true/true true/true true/true maxPublished 2",
        ),
        // camera, video without type
        (
            Camera,
            video(&no_type),
            "false false false false false maxPublished -1 dd true true/true true/true true/true maxPublished 2",
        ),
        // camera, video interval of 1 only
        (
            Camera,
            video(&interval_of_1_only),
            "false false true false false maxPublished 1 dd false false/false false/false false/false maxPublished -1",
        ),
        // camera, video no packet config 1
        (
            Camera,
            video(&no_packet_config_1),
            "false true false true false maxPublished 2 dd true true/true true/true true/true maxPublished 2",
        ),
    ] {
        let ti = track_info(
            livekit::TrackType::Video,
            source,
            "video/vp8",
            livekit::video_layer::Mode::Unused,
            &[0, 1, 2],
            false,
        );
        let mut out: Vec<String> = Vec::new();
        // trackers one by one
        let s =
            StreamTrackerManager::new(logger::get_logger(), &ti, mime::MimeType::Vp8, 90000, &cfg);
        for layer in [-1, 0, 1, 2, 3] {
            out.push(s.add_tracker(layer).is_some().to_string());
        }
        out.push(format!("maxPublished {}", s.get_max_published_layer()));
        s.remove_all_trackers();
        s.close();
        // trackers of a dependency descriptor
        let s =
            StreamTrackerManager::new(logger::get_logger(), &ti, mime::MimeType::Vp8, 90000, &cfg);
        s.add_dependency_descriptor_trackers();
        out.push(format!("dd {}", s.inner.read().dd_tracker.is_some()));
        // Go looks at the type of each tracker. From here it shows in what they do: a
        // descriptor on the tracker of layer 1 makes all three layers active only if all
        // three are those of one dependency descriptor tracker.
        if let Some(tracker) = s.get_tracker(1) {
            dd_packet(&tracker, 2, 0);
        }
        for layer in [0, 1, 2] {
            let tracker = s.get_tracker(layer);
            let layered = tracker
                .as_ref()
                .is_some_and(|tracker| tracker.status() == StreamStatus::Active);
            out.push(format!("{}/{layered}", tracker.is_some()));
        }
        out.push(format!("maxPublished {}", s.get_max_published_layer()));
        s.remove_all_trackers();
        s.close();
        assert_eq!(out.join(" "), expected);
    }
}

// ---------------------------------------------------

// Trackers by packets for three layers. Only the tracker of layer 2 has a worker that ticks
// (every 50 ms): what comes by itself comes from that one, in an order that does not depend
// on how fast the test runs.
#[tokio::test(flavor = "multi_thread")]
async fn layers_of_a_simulcast_track_come_and_go() {
    let hour = time::HOUR;
    let cfg = StreamTrackerManagerConfig {
        video: config([hour, hour, 50 * time::MILLISECOND], [hour, hour, hour]),
        ..Default::default()
    };
    let unused = livekit::video_layer::Mode::Unused;
    let ti = video_track_info("video/vp8", unused, &[0, 1, 2], false);
    let s = StreamTrackerManager::new(logger::get_logger(), &ti, mime::MimeType::Vp8, 90000, &cfg);
    let r = Recorder::new(&s);
    step(
        "new",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [none none none] maxPublished -1 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    let log = testutils::capture_log(tracing::Level::ERROR, || {
        assert!(s.get_tracker(-1).is_none());
        assert!(s.get_tracker(3).is_none());
    });
    assert!(log.contains("unexpected layer layer=-1"), "{log}");
    assert!(log.contains("unexpected layer layer=3"), "{log}");
    assert!(s.add_tracker(-1).is_none());
    assert!(s.add_tracker(3).is_none());

    // no listener yet: nothing is said
    let t0 = s.add_tracker(0).unwrap();
    time::sleep(50 * time::MILLISECOND).await;
    step(
        "add 0 without listener",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped none none] maxPublished 0 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    s.set_listener(r.listener());
    assert!(Arc::ptr_eq(&s.add_tracker(0).unwrap(), &t0));
    let t2 = s.add_tracker(2).unwrap();
    r.wait(1).await;
    step(
        "add 2",
        &r,
        &["max-published 2"],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped none stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    let t1 = s.add_tracker(1).unwrap();
    time::sleep(50 * time::MILLISECOND).await;
    step(
        "add 1",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    packet(&t0);
    step(
        "packet on 0",
        &r,
        &["available [0]", "max-available 0"],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    packet(&t0);
    step(
        "packet on 0 again",
        &r,
        &[],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    t1.observe(0, 1000, 0, false, 0, None);
    step(
        "packet on 1 without payload",
        &r,
        &[],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    packet(&t1);
    step(
        "packet on 1",
        &r,
        &["available [0 1]", "max-available 1"],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    // layer 2 is the one with a worker that ticks: a packet starts it, and with no more
    // packets it stops
    packet(&t2);
    r.wait(4).await;
    step(
        "packet on 2, and no more",
        &r,
        &[
            "available [0 1 2]",
            "max-available 2",
            "available [0 1]",
            "max-available 1",
        ],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    // a packet brings it back at the next tick, and the tick after that it stops again
    packet(&t2);
    r.wait(4).await;
    step(
        "another packet on 2",
        &r,
        &[
            "available [0 1 2]",
            "max-available 2",
            "available [0 1]",
            "max-available 1",
        ],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    assert_eq!(s.set_max_expected_spatial_layer(0), 2);
    step(
        "max expected 0",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 0 maxTemporalSeen -1 distance 0",
    );
    assert_eq!(s.set_max_expected_spatial_layer(0), 0);
    // more is expected again: the trackers of the layers above are reset
    assert_eq!(s.set_max_expected_spatial_layer(2), 0);
    step(
        "max expected 2",
        &r,
        &["available [0]", "max-available 0"],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    packet(&t1);
    step(
        "packet on 1",
        &r,
        &["available [0 1]", "max-available 1"],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    assert_eq!(s.set_max_expected_spatial_layer(-1), 2);
    assert_eq!(s.set_max_expected_spatial_layer(1), -1);
    step(
        "max expected -1 then 1",
        &r,
        &["available [1]", "available []", "max-available -1"],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected 1 maxTemporalSeen -1 distance 0",
    );
    packet(&t0);
    packet(&t1);
    step(
        "packets on 0 and 1",
        &r,
        &[
            "available [0]",
            "max-available 0",
            "available [0 1]",
            "max-available 1",
        ],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 1 maxTemporalSeen -1 distance 0",
    );

    // the track info's layers lower what is expected, and do not raise it; and a track info
    // that is not muted resets every tracker
    s.update_track_info(Some(&video_track_info("video/vp8", unused, &[0], false)));
    step(
        "track info with layer 0",
        &r,
        &["available [1]", "available []", "max-available -1"],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected 0 maxTemporalSeen -1 distance 0",
    );
    s.update_track_info(Some(&ti));
    step(
        "track info with three layers",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected 0 maxTemporalSeen -1 distance 0",
    );
    s.update_track_info(None);
    step(
        "no track info",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    s.update_track_info(Some(&ti));
    step(
        "track info with three layers",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );

    // muted
    let muted = video_track_info("video/vp8", unused, &[0, 1, 2], true);
    s.update_track_info(Some(&muted));
    step(
        "muted",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    packet(&t0);
    packet(&t1);
    step(
        "muted, packets on 0 and 1",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    s.update_track_info(Some(&ti));
    step(
        "unmuted",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    packet(&t1);
    packet(&t0);
    step(
        "packets on 1 and 0",
        &r,
        &["available [1]", "max-available 1", "available [0 1]"],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );

    // removal: the layer stays available
    s.remove_tracker(1);
    step(
        "remove 1",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active none stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    packet(&t1);
    step(
        "packet on the removed 1",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active none stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    let n1 = s.add_tracker(1).unwrap();
    time::sleep(50 * time::MILLISECOND).await;
    assert!(!Arc::ptr_eq(&n1, &t1));
    step(
        "add 1",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    packet(&n1);
    step(
        "packet on the new 1",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    s.remove_tracker(1);
    let n1 = s.add_tracker(1).unwrap();
    s.update_track_info(Some(&muted));
    step(
        "muted with a new 1",
        &r,
        &["available [1]"],
        "available [1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    // a tracker that is added while muted starts paused
    s.remove_tracker(0);
    let n0 = s.add_tracker(0).unwrap();
    packet(&n0);
    step(
        "muted, packet on a new 0",
        &r,
        &[],
        "available [1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    s.update_track_info(Some(&ti));
    packet(&n0);
    packet(&n1);
    step(
        "unmuted, packets on 0 and 1",
        &r,
        &["available [0 1]"],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected -1 maxTemporalSeen -1 distance 0",
    );
    assert_eq!(s.set_max_expected_spatial_layer(0), -1);
    s.remove_all_trackers();
    step(
        "remove all",
        &r,
        &["available [1]"],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [none none none] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    packet(&n0);
    step(
        "packet on the removed 0",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [none none none] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    s.close();
}

// ---------------------------------------------------

// The trackers of a dependency descriptor. No worker ticks here (the bitrate report is an
// hour away): everything comes with the packet that brings it.
#[tokio::test(flavor = "multi_thread")]
async fn layers_of_a_stream_with_dependency_descriptors_come_and_go() {
    let hour = time::HOUR;
    let cfg = StreamTrackerManagerConfig {
        video: config([hour, hour, hour], [hour, hour, hour]),
        ..Default::default()
    };
    let svc = livekit::video_layer::Mode::MultipleSpatialLayersPerStream;
    let ti = video_track_info("video/av1", svc, &[0, 1, 2], false);
    let s = StreamTrackerManager::new(logger::get_logger(), &ti, mime::MimeType::Av1, 90000, &cfg);
    let r = Recorder::new(&s);
    s.set_listener(r.listener());
    step(
        "new",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [none none none] maxPublished -1 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    s.add_dependency_descriptor_trackers();
    r.wait(3).await;
    // the three calls come from three tasks, in any order
    r.events.lock().sort();
    step(
        "add dd trackers (sorted)",
        &r,
        &["max-published 0", "max-published 1", "max-published 2"],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    let trackers = || [0, 1, 2].map(|layer| s.get_tracker(layer).unwrap());
    let [t0, t1, t2] = trackers();
    s.add_dependency_descriptor_trackers();
    time::sleep(50 * time::MILLISECOND).await;
    for (tracker, again) in [&t0, &t1, &t2].into_iter().zip(trackers()) {
        assert!(Arc::ptr_eq(tracker, &again));
    }
    step(
        "add dd trackers again",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    dd_packet(&t0, 1, 1);
    step(
        "dd 2x2",
        &r,
        &[
            "available [0]",
            "max-available 0",
            "available [0 1]",
            "max-available 1",
        ],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    t0.observe(0, 0, 0, false, 0, Some(&descriptor(2, 2)));
    step(
        "dd 3x3 without payload",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    dd_packet(&t1, 2, 2);
    step(
        "dd 3x3",
        &r,
        &["available [0 1 2]", "max-available 2"],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    dd_packet(&t2, 0, 1);
    step(
        "dd 1x2",
        &r,
        &["available [0 2]", "available [0]", "max-available 0"],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    t0.observe(0, 0, 100, false, 0, None);
    step(
        "no dd",
        &r,
        &[],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    dd_packet(&t0, 2, 0);
    step(
        "dd 3x1",
        &r,
        &[
            "available [0 1]",
            "max-available 1",
            "available [0 1 2]",
            "max-available 2",
        ],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    // a reset is nothing to these trackers
    assert_eq!(s.set_max_expected_spatial_layer(0), 2);
    assert_eq!(s.set_max_expected_spatial_layer(2), 0);
    step(
        "max expected 0 then 2",
        &r,
        &[],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    // nor is a pause: it is the end of it that stops every layer
    let muted = video_track_info("video/av1", svc, &[0, 1, 2], true);
    s.update_track_info(Some(&muted));
    step(
        "muted",
        &r,
        &[],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    dd_packet(&t0, 0, 0);
    step(
        "muted, dd 1x1",
        &r,
        &[],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    s.update_track_info(Some(&ti));
    step(
        "unmuted",
        &r,
        &[
            "available [1 2]",
            "available [2]",
            "available []",
            "max-available -1",
        ],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    dd_packet(&t0, 1, 0);
    step(
        "dd 2x1",
        &r,
        &[
            "available [0]",
            "max-available 0",
            "available [0 1]",
            "max-available 1",
        ],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    // removing one layer's tracker stops the tracker of all of them
    s.remove_tracker(2);
    step(
        "remove 2",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active none] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    dd_packet(&t0, 2, 0);
    step(
        "dd 3x1 after the removal",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active none] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    // (the original's test says here that the new tracker is one of the descriptor's)
    s.add_tracker(2).unwrap();
    time::sleep(50 * time::MILLISECOND).await;
    step(
        "add 2",
        &r,
        &[],
        "available [0 1] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    s.remove_all_trackers();
    step(
        "remove all",
        &r,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [none none none] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    assert!(s.inner.read().dd_tracker.is_none());
    // trackers one by one now: by packets
    let p0 = s.add_tracker(0).unwrap();
    p0.observe(0, 0, 100, false, 0, None);
    step(
        "packet on a new 0",
        &r,
        &["available [0]", "max-available 0"],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active none none] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    // and the trackers of a descriptor on top: 0 stays what it is
    s.add_dependency_descriptor_trackers();
    time::sleep(50 * time::MILLISECOND).await;
    assert!(Arc::ptr_eq(&s.get_tracker(0).unwrap(), &p0));
    step(
        "add dd trackers",
        &r,
        &[],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    // seen from outside: a descriptor on 1 is for 1 and 2, and a reset stops the tracker by
    // packets only
    dd_packet(&s.get_tracker(1).unwrap(), 2, 0);
    step(
        "dd 3x1 on the new 1",
        &r,
        &[
            "available [0 1]",
            "max-available 1",
            "available [0 1 2]",
            "max-available 2",
        ],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    assert_eq!(s.set_max_expected_spatial_layer(-1), 2);
    assert_eq!(s.set_max_expected_spatial_layer(2), -1);
    step(
        "max expected -1 then 2",
        &r,
        &["available [1 2]"],
        "available [1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [stopped active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    s.remove_all_trackers();
    s.close();
}

// ---------------------------------------------------

// The trackers measure their bitrates 700 ms after their first packets and the managers
// report every second, so that the first report has the bitrates, with 300 ms to spare
// before it and 400 ms after it, and the second one has none.
#[tokio::test(flavor = "multi_thread")]
async fn bitrates_are_measured_by_the_trackers_and_reported_every_second() {
    let hour = time::HOUR;
    let interval = 700 * time::MILLISECOND;
    let cfg = StreamTrackerManagerConfig {
        video: config([hour, hour, hour], [interval, interval, interval]),
        ..Default::default()
    };
    let unused = livekit::video_layer::Mode::Unused;

    // a simulcast track, a stream with dependency descriptors and an audio track
    let ti = video_track_info("video/vp8", unused, &[0, 1, 2], false);
    let s = StreamTrackerManager::new(logger::get_logger(), &ti, mime::MimeType::Vp8, 90000, &cfg);
    let r = Recorder::new(&s);
    let svc = livekit::video_layer::Mode::MultipleSpatialLayersPerStream;
    let ti_dd = video_track_info("video/av1", svc, &[0, 1, 2], false);
    let s_dd = StreamTrackerManager::new(
        logger::get_logger(),
        &ti_dd,
        mime::MimeType::Av1,
        90000,
        &cfg,
    );
    let r_dd = Recorder::new(&s_dd);
    let ti_audio = track_info(
        livekit::TrackType::Audio,
        livekit::TrackSource::Microphone,
        "audio/opus",
        unused,
        &[],
        false,
    );
    let s_audio = StreamTrackerManager::new(
        logger::get_logger(),
        &ti_audio,
        mime::MimeType::Opus,
        48000,
        &cfg,
    );
    let r_audio = Recorder::new(&s_audio);

    // trackers before the listener: no word of the highest published layer
    let [t0, t1, t2] = [0, 1, 2].map(|layer| s.add_tracker(layer).unwrap());
    s_dd.add_dependency_descriptor_trackers();
    let d0 = s_dd.get_tracker(0).unwrap();
    s.set_listener(r.listener());
    s_dd.set_listener(r_dd.listener());
    s_audio.set_listener(r_audio.listener());

    // 875 bytes in 700 ms are 10 kbps
    t0.observe(0, 875, 100, false, 0, None);
    t0.observe(1, 2 * 875, 100, false, 0, None);
    t1.observe(0, 3 * 875, 100, false, 0, None);
    t1.observe(1, 4 * 875, 100, false, 0, None);
    t1.observe(2, 5 * 875, 100, false, 0, None);
    t2.observe(0, 6 * 875, 100, false, 0, None);
    step(
        "simulcast packets",
        &r,
        &[
            "available [0]",
            "max-available 0",
            "available [0 1]",
            "max-available 1",
            "available [0 1 2]",
            "max-available 2",
        ],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );
    d0.observe(0, 875, 100, false, 0, Some(&descriptor(1, 2)));
    d0.observe(0, 875, 100, false, 0, Some(&descriptor(1, 1)));
    d0.observe(0, 875, 100, false, 0, Some(&descriptor(0, 0)));
    step(
        "dd packets",
        &r_dd,
        &[
            "available [0]",
            "max-available 0",
            "available [0 1]",
            "max-available 1",
            "available [0]",
            "max-available 0",
        ],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0",
    );

    // the trackers measure after 700 ms, the managers report after a second
    r.wait(5).await;
    r_dd.wait(4).await;
    // measured by the original: [[9988 29965 0 0] [29969 69928 119877 0] [59943 0 0 0]]
    measured_step(
        "simulcast first report",
        &r,
        [
            [10000, 30000, 0, 0],
            [30000, 70000, 120000, 0],
            [60000, 0, 0, 0],
        ],
        &[
            "bitrate-availability",
            "bitrate-availability",
            "bitrate-availability",
            "max-temporal-seen 2 get 2",
            "bitrate-report [0 1 2] <measured> distance 0.6666666666666666",
        ],
        "available [0 1 2] bitrates <measured> trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen 2 distance 0.6666666666666666",
    );
    // measured by the original: [[29970 19980 9990 0] [0 0 0 0] [0 0 0 0]]
    measured_step(
        "dd first report",
        &r_dd,
        [[30000, 20000, 10000, 0], [0, 0, 0, 0], [0, 0, 0, 0]],
        &[
            "bitrate-availability",
            "bitrate-availability",
            "max-temporal-seen 2 get 2",
            "bitrate-report [0] <measured> distance 2",
        ],
        "available [0] bitrates <measured> trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen 2 distance 2",
    );
    // 700 ms later the trackers measure nothing, and the second report says so
    r.wait(4).await;
    r_dd.wait(3).await;
    step(
        "simulcast second report",
        &r,
        &[
            "bitrate-availability",
            "bitrate-availability",
            "bitrate-availability",
            "bitrate-report [0 1 2] [[0 0 0 0] [0 0 0 0] [0 0 0 0]] distance 0",
        ],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen 2 distance 0",
    );
    step(
        "dd second report",
        &r_dd,
        &[
            "bitrate-availability",
            "bitrate-availability",
            "bitrate-report [0] [[0 0 0 0] [0 0 0 0] [0 0 0 0]] distance 2",
        ],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen 2 distance 2",
    );
    // nobody reports for a track that is not video
    step(
        "audio",
        &r_audio,
        &[],
        "available [] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [none none none] maxPublished -1 maxExpected -1 maxTemporalSeen -1 distance 0",
    );

    s.close();
    s_dd.close();
    s_audio.close();
    time::sleep(1250 * time::MILLISECOND).await;
    step(
        "simulcast closed",
        &r,
        &[],
        "available [0 1 2] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active active active] maxPublished 2 maxExpected 2 maxTemporalSeen 2 distance 0",
    );
    step(
        "dd closed",
        &r_dd,
        &[],
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen 2 distance 2",
    );
    s.remove_all_trackers();
    s_dd.remove_all_trackers();
}

// ---------------------------------------------------

// A tracker that has these bitrates to tell of.
struct FakeTracker {
    brs: [i64; 4],
}

impl StreamTrackerWorker for FakeTracker {
    fn start(&self) {}
    fn stop(&self) {}
    fn reset(&self) {}
    fn on_status_changed(&self, _f: Box<dyn Fn(StreamStatus) + Send + Sync>) {}
    fn on_bitrate_available(&self, _f: Box<dyn Fn() + Send + Sync>) {}
    fn status(&self) -> StreamStatus {
        StreamStatus::Active
    }
    fn bitrate_temporal_cumulative(&self) -> Vec<i64> {
        self.brs.to_vec()
    }
    fn set_paused(&self, _paused: bool) {}
    fn observe(
        &self,
        _temporal_layer: i32,
        _pkt_size: usize,
        _payload_size: usize,
        _has_marker: bool,
        _ts: u32,
        _dd: Option<&buffer::ExtDependencyDescriptor>,
    ) {
    }
}

// A manager of a track whose info has one codec (`codec_mime`, with `layers`, in one stream
// if `svc`), with trackers that tell these bitrates, these layers available, a dependency
// descriptor tracker if `dd`, and this highest temporal layer seen; then the calls of
// `set_max_expected_spatial_layer`. In the second line of a row what the original then
// says: what those calls returned, the highest layer expected, and the layers, the bitrates
// and the distance to desired.
struct Case {
    mime: mime::MimeType,
    codec_mime: &'static str,
    svc: bool,
    layers: &'static [i32],
    muted: bool,
    dd: bool,
    trackers: [Option<[i64; 4]>; 3],
    available: &'static [i32],
    set_max_expected: &'static [i32],
    max_temporal_seen: i32,

    prevs: &'static [i32],
    max_expected: i32,
    layers_out: &'static [i32],
    bitrates: Bitrates,
    distance: f64,
}

#[tokio::test(flavor = "multi_thread")]
async fn layered_bitrates_and_the_distance_to_desired() {
    use mime::MimeType::{Vp8, Vp9};
    const A: Option<[i64; 4]> = Some([100, 300, 600, 1000]);
    const B: Option<[i64; 4]> = Some([2000, 5000, 0, 0]);
    const C: Option<[i64; 4]> = Some([40000, 0, 0, 0]);
    const Z: Option<[i64; 4]> = Some([0, 0, 0, 0]);
    // the rows as the Go test printed them
    #[rustfmt::skip]
    let cases = [
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [None, None, None], available: &[], set_max_expected: &[], max_temporal_seen: -1,
               prevs: &[], max_expected: 2, layers_out: &[], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: -1,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: -1, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: true, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[-1], max_temporal_seen: 3,
               prevs: &[2], max_expected: -1, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [None, None, None], available: &[], set_max_expected: &[], max_temporal_seen: 0,
               prevs: &[], max_expected: 2, layers_out: &[], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 3.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [None, None, None], available: &[], set_max_expected: &[], max_temporal_seen: 2,
               prevs: &[], max_expected: 2, layers_out: &[], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 3.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [None, None, None], available: &[], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 3.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0], muted: false, dd: false, trackers: [None, None, None], available: &[], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 0, layers_out: &[], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 1.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 3.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [Z, Z, Z], available: &[], set_max_expected: &[], max_temporal_seen: 1,
               prevs: &[], max_expected: 2, layers_out: &[], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 3.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [Z, Z, Z], available: &[0], set_max_expected: &[], max_temporal_seen: 2,
               prevs: &[], max_expected: 2, layers_out: &[0], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 2.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [Z, Z, Z], available: &[0, 1], set_max_expected: &[], max_temporal_seen: 2,
               prevs: &[], max_expected: 2, layers_out: &[0, 1], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 1.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [Z, Z, Z], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 2,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [None, None, None], available: &[1], set_max_expected: &[], max_temporal_seen: 0,
               prevs: &[], max_expected: 2, layers_out: &[1], bitrates: [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 1.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [B, Z, Z], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[2000, 5000, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [B, Z, None], available: &[0, 1], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1], bitrates: [[2000, 5000, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 1.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [0, 0, 0, 0]], distance: 1.5 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0], bitrates: [[100, 300, 600, 1000], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 2.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 2], bitrates: [[100, 300, 600, 1000], [0, 0, 0, 0], [40000, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[1], set_max_expected: &[], max_temporal_seen: 1,
               prevs: &[], max_expected: 2, layers_out: &[1], bitrates: [[0, 0, 0, 0], [2000, 5000, 0, 0], [0, 0, 0, 0]], distance: 1.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [C, None, None], available: &[0], set_max_expected: &[], max_temporal_seen: 2,
               prevs: &[], max_expected: 2, layers_out: &[0], bitrates: [[40000, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 2.6666666666666665 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [None, A, None], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[0, 0, 0, 0], [100, 300, 600, 1000], [0, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [B, B, B], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 1,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[2000, 5000, 0, 0], [2000, 5000, 0, 0], [2000, 5000, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, A, A], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 1,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [100, 300, 600, 1000], [100, 300, 600, 1000]], distance: -1.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[0], max_temporal_seen: 3,
               prevs: &[2], max_expected: 0, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: -1.25 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[1], max_temporal_seen: 3,
               prevs: &[2], max_expected: 1, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: -0.25 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[0, 2], max_temporal_seen: 3,
               prevs: &[2, 0], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 1, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: -0.25 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[0, 1], muted: false, dd: false, trackers: [A, None, None], available: &[0], set_max_expected: &[2], max_temporal_seen: 3,
               prevs: &[1], max_expected: 2, layers_out: &[0], bitrates: [[100, 300, 600, 1000], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 2.0 },
        Case { mime: Vp8, codec_mime: "video/vp8", svc: false, layers: &[1], muted: false, dd: false, trackers: [A, None, None], available: &[0], set_max_expected: &[], max_temporal_seen: 2,
               prevs: &[], max_expected: 1, layers_out: &[0], bitrates: [[100, 300, 600, 1000], [0, 0, 0, 0], [0, 0, 0, 0]], distance: 0.6666666666666666 },
        Case { mime: Vp8, codec_mime: "video/vp9", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: -1, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.0 },
        Case { mime: Vp8, codec_mime: "video/vp9", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[2], max_temporal_seen: 3,
               prevs: &[-1], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp9, codec_mime: "video/vp9", svc: true, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2100, 5300, 0, 0], [42100, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp9, codec_mime: "video/vp9", svc: true, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 2], bitrates: [[100, 300, 600, 1000], [0, 0, 0, 0], [40100, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp9, codec_mime: "video/vp9", svc: true, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, Z, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 2,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [0, 0, 0, 0], [40100, 0, 0, 0]], distance: 0.6666666666666666 },
        Case { mime: Vp9, codec_mime: "video/vp9", svc: true, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, A, A], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [200, 600, 1200, 2000], [300, 900, 1800, 3000]], distance: 0.0 },
        Case { mime: Vp9, codec_mime: "video/vp9", svc: true, layers: &[0, 1, 2], muted: false, dd: false, trackers: [None, A, A], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[0, 0, 0, 0], [100, 300, 600, 1000], [200, 600, 1200, 2000]], distance: 0.0 },
        Case { mime: Vp9, codec_mime: "video/vp9", svc: true, layers: &[0, 1, 2], muted: false, dd: true, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp9, codec_mime: "video/vp9", svc: false, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: 2, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.75 },
        Case { mime: Vp8, codec_mime: "video/vp9", svc: true, layers: &[0, 1, 2], muted: false, dd: false, trackers: [A, B, C], available: &[0, 1, 2], set_max_expected: &[], max_temporal_seen: 3,
               prevs: &[], max_expected: -1, layers_out: &[0, 1, 2], bitrates: [[100, 300, 600, 1000], [2000, 5000, 0, 0], [40000, 0, 0, 0]], distance: 0.0 },
    ];
    for (i, case) in cases.iter().enumerate() {
        let mode = if case.svc {
            livekit::video_layer::Mode::MultipleSpatialLayersPerStream
        } else {
            livekit::video_layer::Mode::Unused
        };
        // an audio track: no reporter
        let ti = track_info(
            livekit::TrackType::Audio,
            livekit::TrackSource::Camera,
            case.codec_mime,
            mode,
            case.layers,
            case.muted,
        );
        let s = StreamTrackerManager::new(
            logger::get_logger(),
            &ti,
            case.mime,
            90000,
            &StreamTrackerManagerConfig::default(),
        );
        {
            let mut inner = s.inner.write();
            for (layer, brs) in case.trackers.iter().enumerate() {
                if let Some(brs) = brs {
                    inner.trackers[layer] = Some(Arc::new(FakeTracker { brs: *brs }));
                }
            }
            inner.available_layers = case.available.to_vec();
            inner.max_temporal_layer_seen = case.max_temporal_seen;
            if case.dd {
                inner.dd_tracker = Some(streamtracker::StreamTrackerDependencyDescriptor::new(
                    streamtracker::StreamTrackerParams::default(),
                ));
            }
        }
        let prevs: Vec<i32> = case
            .set_max_expected
            .iter()
            .map(|&layer| s.set_max_expected_spatial_layer(layer))
            .collect();
        assert_eq!(prevs, case.prevs, "row {i}");
        assert_eq!(
            s.inner.read().max_expected_layer,
            case.max_expected,
            "row {i}"
        );
        let (al, brs) = s.get_layered_bitrate();
        assert_eq!(al, case.layers_out, "row {i}");
        assert_eq!(brs, case.bitrates, "row {i}");
        assert_eq!(s.distance_to_desired(), case.distance, "row {i}");
        s.close();
    }
}

// ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_highest_temporal_layer_seen_only_goes_up() {
    let ti = track_info(
        livekit::TrackType::Audio,
        livekit::TrackSource::Camera,
        "video/vp8",
        livekit::video_layer::Mode::Unused,
        &[0, 1, 2],
        false,
    );
    let s = StreamTrackerManager::new(
        logger::get_logger(),
        &ti,
        mime::MimeType::Vp8,
        90000,
        &StreamTrackerManagerConfig::default(),
    );
    let r = Recorder::new(&s);
    s.set_listener(r.listener());
    let none: &[&str] = &[];
    for (brs, events, get) in [
        ([[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], none, -1),
        (
            [[0, 5, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]],
            &["max-temporal-seen 1 get 1"][..],
            1,
        ),
        ([[0, 0, 0, 0], [0, 0, 0, 0], [7, 0, 0, 0]], none, 1),
        ([[0, 5, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], none, 1),
        (
            [[1, 1, 0, 0], [1, 1, 1, 0], [1, 0, 0, 0]],
            &["max-temporal-seen 2 get 2"][..],
            2,
        ),
        (
            [[0, 0, 0, 0], [0, 0, 0, 9], [0, 0, 0, 0]],
            &["max-temporal-seen 3 get 3"][..],
            3,
        ),
        ([[9, 9, 9, 9], [9, 9, 9, 9], [9, 9, 9, 9]], none, 3),
        ([[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], none, 3),
    ] {
        s.update_max_temporal_layer_seen(brs);
        assert_eq!(r.take(), events, "{brs:?}");
        assert_eq!(s.get_max_temporal_layer_seen(), get, "{brs:?}");
    }

    // without a listener it is kept all the same
    s.set_listener(None);
    s.inner.write().max_temporal_layer_seen = -1;
    s.update_max_temporal_layer_seen([[0, 0, 1, 0], [0; 4], [0; 4]]);
    assert_eq!(r.take(), none);
    assert_eq!(s.get_max_temporal_layer_seen(), 2);
    s.close();
}

// ---------------------------------------------------

// And for what is different here: the references between a manager, its trackers, its
// reporter and its listener, which the original leaves to the garbage collector.
#[tokio::test(flavor = "multi_thread")]
async fn a_manager_is_held_by_its_reporter_only_and_does_not_hold_its_listener() {
    let hour = time::HOUR;
    let cfg = StreamTrackerManagerConfig {
        video: config([hour, hour, hour], [hour, hour, hour]),
        ..Default::default()
    };
    let unused = livekit::video_layer::Mode::Unused;

    // a track that is not video has no reporter: the manager goes with its last holder,
    // callbacks of its trackers and all
    let ti = track_info(
        livekit::TrackType::Audio,
        livekit::TrackSource::Camera,
        "video/vp8",
        unused,
        &[0, 1, 2],
        false,
    );
    let s = StreamTrackerManager::new(logger::get_logger(), &ti, mime::MimeType::Vp8, 90000, &cfg);
    let r = Recorder::new(&s);
    s.set_listener(r.listener());
    let t0 = s.add_tracker(0).unwrap();
    r.wait(1).await;
    assert_eq!(r.take(), ["max-published 0"]);
    s.add_dependency_descriptor_trackers();
    r.wait(2).await;
    r.take();
    // the manager does not hold its listener: one that is gone is no listener
    let listener = Arc::downgrade(&r);
    drop(r);
    gone(&listener, "the listener").await;
    assert!(s.get_listener().is_none());
    packet(&t0);
    assert_eq!(
        state_of(&s),
        "available [0] bitrates [[0 0 0 0] [0 0 0 0] [0 0 0 0]] trackers [active stopped stopped] maxPublished 2 maxExpected 2 maxTemporalSeen -1 distance 0"
    );
    let manager = Arc::downgrade(&s);
    drop(s);
    gone(&manager, "the manager").await;
    // its trackers go on without it
    packet(&t0);
    t0.set_paused(true);
    assert_eq!(t0.status(), StreamStatus::Stopped);

    // a video track's reporter holds the manager until it is closed, as the goroutine does
    let ti = video_track_info("video/vp8", unused, &[0, 1, 2], false);
    let s = StreamTrackerManager::new(logger::get_logger(), &ti, mime::MimeType::Vp8, 90000, &cfg);
    s.add_tracker(0).unwrap();
    let manager = Arc::downgrade(&s);
    drop(s);
    time::sleep(50 * time::MILLISECOND).await;
    let s = manager.upgrade().expect("the reporter holds the manager");
    s.close();
    drop(s);
    gone(&manager, "the manager").await;
}
