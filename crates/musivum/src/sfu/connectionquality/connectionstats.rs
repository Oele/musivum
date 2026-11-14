// Ported from LiveKit, pkg/sfu/connectionquality/connectionstats.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `NewConnectionStats` is `ConnectionStats::new` and
// gives an `Arc`, which keeps a `Weak` of itself for the worker; the two providers are held
// as `Weak`: a provider is the receiver or the down track that owns these statistics, and
// one that is gone is treated as one that was not given, except that statistics made for a
// sender stay a sender's; `update_streaming_start` is handed the sender provider it asks;
// the providers give their maps by value, an empty one where the original gives nil, and the
// functions here hand them on the same way; the jitter that `to_aggregate_delta_info`
// discounts in a remote view, which the original writes through the pointer in the map, is
// written into this map's own copy of it (`Arc::make_mut`), where `get_stat` reads it;
// `on_stats_update` keeps its callback behind a lock and calls it with none held; the worker
// goroutine is a task that holds the statistics weakly, so that it ends also when they are
// dropped without `close`; its ticker is the runtime's interval and `done` is a
// `CancellationToken`; an update interval below zero panics in the worker task, where the
// original takes the process down.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::go::atomic;
use crate::go::time::{self, Duration, Time};
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::sfu::buffer;
use crate::sfu::rtpstats;

use super::scorer::{MIN_MOS, QualityScorer, QualityScorerParams, WindowStat};

pub const UPDATE_INTERVAL: Duration = Duration::from_secs(5);
const NO_RECEIVER_REPORT_TOO_LONG_THRESHOLD: Duration = Duration::from_secs(30);

pub trait ConnectionStatsReceiverProvider: Send + Sync {
    fn get_delta_stats(&self) -> HashMap<u32, buffer::StreamStatsWithLayers>;
    fn get_last_sender_report_time(&self) -> Time;
}

pub trait ConnectionStatsSenderProvider: Send + Sync {
    fn get_delta_stats_sender(&self) -> HashMap<u32, buffer::StreamStatsWithLayers>;
    fn get_primary_stream_last_receiver_report_time(&self) -> Time;
    fn get_primary_stream_packets_sent(&self) -> u64;
}

#[derive(Clone, Default)]
pub struct ConnectionStatsParams {
    pub update_interval: Duration,
    pub include_rtt: bool,
    pub include_jitter: bool,
    pub enable_bitrate_score: bool,
    pub receiver_provider: Option<Weak<dyn ConnectionStatsReceiverProvider>>,
    pub sender_provider: Option<Weak<dyn ConnectionStatsSenderProvider>>,
    pub logger: Logger,
}

type OnStatsUpdate = Arc<dyn Fn(&ConnectionStats, &livekit::AnalyticsStat) + Send + Sync>;

pub struct ConnectionStats {
    params: ConnectionStatsParams,

    codec_mime_type: atomic::Value<mime::MimeType>,

    is_started: atomic::Bool,
    is_video: atomic::Bool,

    on_stats_update: Mutex<Option<OnStatsUpdate>>,

    lock: RwLock<ConnectionStatsInner>,

    scorer: QualityScorer,

    done: CancellationToken,

    // the statistics themselves, for the worker that `start_at` starts
    this: Weak<ConnectionStats>,
}

// What `lock` guards.
#[derive(Default)]
struct ConnectionStatsInner {
    packets_sent: u64,
    streaming_started_at: Time,
}

impl ConnectionStats {
    pub fn new(params: ConnectionStatsParams) -> Arc<ConnectionStats> {
        Arc::new_cyclic(|this| ConnectionStats {
            scorer: QualityScorer::new(QualityScorerParams {
                include_rtt: params.include_rtt,
                include_jitter: params.include_jitter,
                enable_bitrate_score: params.enable_bitrate_score,
                logger: params.logger.clone(),
            }),
            params,
            codec_mime_type: atomic::Value::default(),
            is_started: atomic::Bool::new(false),
            is_video: atomic::Bool::new(false),
            on_stats_update: Mutex::new(None),
            lock: RwLock::new(ConnectionStatsInner::default()),
            done: CancellationToken::new(),
            this: this.clone(),
        })
    }

    pub fn start_at(&self, codec_mime_type: mime::MimeType, is_fec_enabled: bool, at: Time) {
        if self.is_started.swap(true) {
            return;
        }

        self.is_video
            .store(mime::is_mime_type_video(codec_mime_type));
        self.codec_mime_type.store(codec_mime_type);
        self.scorer
            .start_at(get_packet_loss_weight(codec_mime_type, is_fec_enabled), at);

        time::spawn(update_stats_worker(self.this.clone()));
    }

    pub fn start(&self, codec_mime_type: mime::MimeType, is_fec_enabled: bool) {
        self.start_at(codec_mime_type, is_fec_enabled, time::now());
    }

    pub fn close(&self) {
        self.done.cancel();
    }

    pub fn update_codec(&self, codec_mime_type: mime::MimeType, is_fec_enabled: bool) {
        self.is_video
            .store(mime::is_mime_type_video(codec_mime_type));
        self.codec_mime_type.store(codec_mime_type);
        self.scorer
            .update_packet_loss_weight(get_packet_loss_weight(codec_mime_type, is_fec_enabled));
    }

    pub fn on_stats_update(
        &self,
        f: impl Fn(&ConnectionStats, &livekit::AnalyticsStat) + Send + Sync + 'static,
    ) {
        *self.on_stats_update.lock() = Some(Arc::new(f));
    }

    pub fn update_mute_at(&self, is_muted: bool, at: Time) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.update_mute_at(is_muted, at);
    }

    pub fn update_mute(&self, is_muted: bool) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.update_mute(is_muted);
    }

    pub fn add_bitrate_transition_at(&self, bitrate: i64, at: Time) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.add_bitrate_transition_at(bitrate, at);
    }

    pub fn add_bitrate_transition(&self, bitrate: i64) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.add_bitrate_transition(bitrate);
    }

    pub fn update_layer_mute_at(&self, is_muted: bool, at: Time) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.update_layer_mute_at(is_muted, at);
    }

    pub fn update_layer_mute(&self, is_muted: bool) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.update_layer_mute(is_muted);
    }

    pub fn update_pause_at(&self, is_paused: bool, at: Time) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.update_pause_at(is_paused, at);
    }

    pub fn update_pause(&self, is_paused: bool) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.update_pause(is_paused);
    }

    pub fn add_layer_transition_at(&self, distance: f64, at: Time) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.add_layer_transition_at(distance, at);
    }

    pub fn add_layer_transition(&self, distance: f64) {
        if self.done.is_cancelled() {
            return;
        }

        self.scorer.add_layer_transition(distance);
    }

    pub fn get_score_and_quality(&self) -> (f32, livekit::ConnectionQuality) {
        self.scorer.get_mos_and_quality()
    }

    fn update_score_with_aggregate(
        &self,
        agg: Option<&rtpstats::RtpDeltaInfo>,
        last_rtcp_at: Time,
        at: Time,
    ) -> f32 {
        let mut stat = WindowStat::default();
        if let Some(agg) = agg {
            stat.started_at = agg.start_time;
            stat.duration = agg.end_time.sub(agg.start_time);
            stat.packets = agg.packets;
            stat.packets_padding = agg.packets_padding;
            stat.packets_lost = agg.packets_lost;
            stat.packets_missing = agg.packets_missing;
            stat.packets_out_of_order = agg.packets_out_of_order;
            stat.bytes = agg.bytes.wrapping_sub(agg.header_bytes); // only use media payload size
            stat.rtt_max = agg.rtt_max;
            stat.jitter_max = agg.jitter_max;

            stat.last_rtcp_at = last_rtcp_at;
        }
        if at.is_zero() {
            self.scorer.update(&stat);
        } else {
            self.scorer.update_at(&stat, at);
        }

        let (mos, _) = self.scorer.get_mos_and_quality();
        mos
    }

    fn update_score_from_receiver_report(
        &self,
        at: Time,
    ) -> (f32, HashMap<u32, buffer::StreamStatsWithLayers>) {
        let Some(sender_provider) = self.params.sender_provider.as_ref().and_then(Weak::upgrade)
        else {
            return (MIN_MOS, HashMap::new());
        };

        let streaming_started_at = self.update_streaming_start(sender_provider.as_ref(), at);
        if streaming_started_at.is_zero() {
            // not streaming, just return current score
            let (mos, _) = self.scorer.get_mos_and_quality();
            return (mos, HashMap::new());
        }

        let mut streams = sender_provider.get_delta_stats_sender();
        if streams.is_empty() {
            //  check for receiver report not received for a while
            let mut marker = sender_provider.get_primary_stream_last_receiver_report_time();
            if marker.is_zero() || streaming_started_at.after(marker) {
                marker = streaming_started_at;
            }
            if time::since(marker) > NO_RECEIVER_REPORT_TOO_LONG_THRESHOLD {
                // have not received receiver report for a long time when streaming, run with nil stat
                return (
                    self.update_score_with_aggregate(None, Time::ZERO, at),
                    HashMap::new(),
                );
            }

            // wait for receiver report, return current score
            let (mos, _) = self.scorer.get_mos_and_quality();
            return (mos, HashMap::new());
        }

        // delta stat duration could be large due to not receiving receiver report for a long time (for example, due to mute),
        // adjust to streaming start if necessary
        if streaming_started_at
            .after(sender_provider.get_primary_stream_last_receiver_report_time())
        {
            // last receiver report was before streaming started, wait for next one
            let (mos, _) = self.scorer.get_mos_and_quality();
            return (mos, streams);
        }

        let Some(mut agg) = to_aggregate_delta_info(&mut streams, true) else {
            // no receiver report in the window
            let (mos, _) = self.scorer.get_mos_and_quality();
            return (mos, streams);
        };
        if streaming_started_at.after(agg.start_time) {
            agg.start_time = streaming_started_at;
        }
        (
            self.update_score_with_aggregate(Some(&agg), Time::ZERO, at),
            streams,
        )
    }

    fn update_score_at(
        &self,
        at: Time,
    ) -> (f32, HashMap<u32, buffer::StreamStatsWithLayers>, bool) {
        if self.params.sender_provider.is_some() {
            // receiver report based quality scoring, use stats from receiver report for scoring
            let (score, streams) = self.update_score_from_receiver_report(at);
            return (score, streams, true);
        }

        let Some(receiver_provider) = self
            .params
            .receiver_provider
            .as_ref()
            .and_then(Weak::upgrade)
        else {
            return (MIN_MOS, HashMap::new(), false);
        };

        let mut streams = receiver_provider.get_delta_stats();
        if streams.is_empty() {
            let (mos, _) = self.scorer.get_mos_and_quality();
            return (mos, HashMap::new(), false);
        }

        let Some(mut agg) = to_aggregate_delta_info(&mut streams, false) else {
            // no receiver report in the window
            let (mos, _) = self.scorer.get_mos_and_quality();
            return (mos, streams, false);
        };
        // upstream PacketsLost is already net of late arrivals (including RTX repairs),
        // so the scorer must not discount out-of-order packets again
        agg.packets_out_of_order = 0;
        (
            self.update_score_with_aggregate(
                Some(&agg),
                receiver_provider.get_last_sender_report_time(),
                at,
            ),
            streams,
            false,
        )
    }

    fn update_streaming_start(
        &self,
        sender_provider: &dyn ConnectionStatsSenderProvider,
        at: Time,
    ) -> Time {
        let mut cs = self.lock.write();

        let packets_sent = sender_provider.get_primary_stream_packets_sent();
        if packets_sent > cs.packets_sent {
            if cs.streaming_started_at.is_zero() {
                // the start could be anywhere after last update, but using `at` as this is not required to be accurate
                if at.is_zero() {
                    cs.streaming_started_at = time::now();
                } else {
                    cs.streaming_started_at = at;
                }
            }
        } else {
            cs.streaming_started_at = Time::ZERO;
        }
        cs.packets_sent = packets_sent;

        cs.streaming_started_at
    }

    fn get_stat(&self) {
        let (score, streams, is_sender) = self.update_score_at(Time::ZERO);

        let on_stats_update = self.on_stats_update.lock().clone();
        if let Some(on_stats_update) = on_stats_update
            && !streams.is_empty()
        {
            let mut analytics_streams = Vec::with_capacity(streams.len());
            for (&ssrc, stream) in &streams {
                let Some(mut as_) = to_analytics_stream(
                    ssrc,
                    stream.rtp_stats.as_deref(),
                    stream.rtp_stats_remote_view.as_deref(),
                    is_sender,
                ) else {
                    continue;
                };

                //
                // add video layer if either
                //   1. Simulcast - even if there is only one layer per stream as it provides layer id
                //   2. A stream has multiple layers
                //
                if (streams.len() > 1 || stream.layers.len() > 1) && self.is_video.load() {
                    for (&layer, layer_stats) in &stream.layers {
                        let avl = to_analytics_video_layer(layer, layer_stats.as_deref());
                        if let Some(avl) = avl {
                            as_.video_layers.push(avl);
                        }
                    }
                }

                analytics_streams.push(as_);
            }

            if !analytics_streams.is_empty() {
                on_stats_update(
                    self,
                    &livekit::AnalyticsStat {
                        score,
                        streams: analytics_streams,
                        // stored by `start_at`, before the worker that calls this is started
                        mime: self.codec_mime_type.load().unwrap_or_default().to_string(),
                        ..Default::default()
                    },
                );
            }
        }
    }
}

async fn update_stats_worker(this: Weak<ConnectionStats>) {
    let Some(cs) = this.upgrade() else {
        return;
    };
    let mut interval = cs.params.update_interval;
    if interval == Duration::ZERO {
        interval = UPDATE_INTERVAL;
    }
    let done = cs.done.clone();
    drop(cs);

    // Go's `time.NewTicker(interval)`: the first tick comes after `interval`, and ticks that
    // the receiver is too slow for are dropped.
    let mut tk = tokio::time::interval_at(
        tokio::time::Instant::now() + interval.to_std(),
        interval.to_std(),
    );
    tk.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = done.cancelled() => {
                if let Some(cs) = this.upgrade() {
                    cs.get_stat();
                }
                return;
            }

            _ = tk.tick() => {
                let Some(cs) = this.upgrade() else {
                    return;
                };
                if cs.done.is_cancelled() {
                    return;
                }

                cs.get_stat();
            }
        }
    }
}

// -----------------------------------------------------------------------

// how much weight to give to packet loss rate when calculating score.
// It is codec dependent.
// For audio:
//
//	o Opus without FEC or RED suffers the most through packet loss, hence has the highest weight
//	o RED with two packet redundancy can absorb one out of every two packets lost, so packet loss is not as detrimental and therefore lower weight
//
// For video:
//
//	o No in-built codec repair available, hence same for all codecs
pub(super) fn get_packet_loss_weight(mime_type: mime::MimeType, is_fec_enabled: bool) -> f64 {
    let mut plw = 0.0;
    if mime_type == mime::MimeType::Opus {
        // 2.5%: fall to GOOD, 7.5%: fall to POOR
        plw = 8.0;
        if is_fec_enabled {
            // 3.75%: fall to GOOD, 11.25%: fall to POOR
            plw /= 1.5;
        }
    } else if mime_type == mime::MimeType::Red {
        // 5%: fall to GOOD, 15.0%: fall to POOR
        plw = 4.0;
        if is_fec_enabled {
            // 7.5%: fall to GOOD, 22.5%: fall to POOR
            plw /= 1.5;
        }
    } else if mime::is_mime_type_video(mime_type) {
        // 2%: fall to GOOD, 6%: fall to POOR
        plw = 10.0;
    }

    plw
}

fn to_aggregate_delta_info(
    streams: &mut HashMap<u32, buffer::StreamStatsWithLayers>,
    use_remote_view: bool,
) -> Option<rtpstats::RtpDeltaInfo> {
    let mut delta_info_list: Vec<Option<&rtpstats::RtpDeltaInfo>> =
        Vec::with_capacity(streams.len());
    for s in streams.values_mut() {
        if use_remote_view {
            if let Some(rtp_stats_remote_view) = s.rtp_stats_remote_view.as_mut() {
                // discount jitter from publisher side + internal processing while reporting downstream jitter
                if let Some(rtp_stats) = s.rtp_stats.as_ref() {
                    let rtp_stats_remote_view = Arc::make_mut(rtp_stats_remote_view);
                    rtp_stats_remote_view.jitter_max -= rtp_stats.jitter_max;
                    if rtp_stats_remote_view.jitter_max < 0.0 {
                        rtp_stats_remote_view.jitter_max = 0.0;
                    }
                }
                delta_info_list.push(Some(&**rtp_stats_remote_view));
            }
        } else if let Some(rtp_stats) = s.rtp_stats.as_ref() {
            delta_info_list.push(Some(&**rtp_stats));
        }
    }
    rtpstats::aggregate_rtp_delta_info(&delta_info_list)
}

fn to_analytics_stream(
    ssrc: u32,
    delta_stats: Option<&rtpstats::RtpDeltaInfo>,
    delta_stats_remote_view: Option<&rtpstats::RtpDeltaInfo>,
    is_sender: bool,
) -> Option<livekit::AnalyticsStream> {
    let delta_stats = delta_stats?;

    // discount the feed side loss when reporting forwarded track stats,
    // discount jitter from publisher side + internal processing while reporting downstream jitter
    let mut packets_lost = delta_stats.packets_lost;
    let mut rtt = delta_stats.rtt_max;
    let mut max_jitter = delta_stats.jitter_max;
    if let Some(delta_stats_remote_view) = delta_stats_remote_view {
        packets_lost = delta_stats_remote_view.packets_lost;
        if delta_stats_remote_view.packets_missing > packets_lost {
            packets_lost = 0;
        } else {
            packets_lost -= delta_stats_remote_view.packets_missing;
        }

        rtt = delta_stats_remote_view.rtt_max;
        max_jitter = delta_stats_remote_view.jitter_max;
    } else if is_sender {
        rtt = 0;
        max_jitter = 0.0;
    }

    Some(livekit::AnalyticsStream {
        start_time: Some(time::timestamppb_new(delta_stats.start_time)),
        end_time: Some(time::timestamppb_new(delta_stats.end_time)),
        ssrc,
        primary_packets: delta_stats.packets,
        primary_bytes: delta_stats.bytes,
        retransmit_packets: delta_stats.packets_duplicate,
        retransmit_bytes: delta_stats.bytes_duplicate,
        padding_packets: delta_stats.packets_padding,
        padding_bytes: delta_stats.bytes_padding,
        packets_lost,
        packets_out_of_order: delta_stats.packets_out_of_order,
        frames: delta_stats.frames,
        rtt,
        jitter: max_jitter as u32,
        nacks: delta_stats.nacks,
        plis: delta_stats.plis,
        firs: delta_stats.firs,
        ..Default::default()
    })
}

fn to_analytics_video_layer(
    layer: i32,
    layer_stats: Option<&rtpstats::RtpDeltaInfo>,
) -> Option<livekit::AnalyticsVideoLayer> {
    let layer_stats = layer_stats?;

    let avl = livekit::AnalyticsVideoLayer {
        layer,
        packets: layer_stats
            .packets
            .wrapping_add(layer_stats.packets_duplicate)
            .wrapping_add(layer_stats.packets_padding),
        bytes: layer_stats
            .bytes
            .wrapping_add(layer_stats.bytes_duplicate)
            .wrapping_add(layer_stats.bytes_padding),
        frames: layer_stats.frames,
    };
    if avl.packets == 0 || avl.bytes == 0 || avl.frames == 0 {
        return None;
    }

    Some(avl)
}

#[cfg(test)]
#[path = "connectionstats_test.rs"]
mod connectionstats_test;

#[cfg(test)]
#[path = "connectionstats_reference_test.rs"]
mod connectionstats_reference_test;
