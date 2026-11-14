// Ported from LiveKit, pkg/rtc/datadowntrack.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The down track is made as an `Arc`, which it hands to
// the data track as its sender. The fields `lock` guards are in the lock; the callback of
// `on_close` is kept as an `Arc`, since it is copied under the lock and called after it.
// `PublishDataTrack` of the params is a `Weak` (see below), so `publish_data_track` is an
// `Option`, `id` and `name` are empty once the track is gone, `new` fails with
// `Error::ReceiverClosed` for a track that is gone as it does for one that is closed, and
// `close` leaves out what it would tell the track. The `BytesTrackStats` of the params, a
// pointer that may be nil, is an `Option`.
// Who holds whom: the published data track holds its down tracks strongly (as subscriptions
// and in its spreader), so a down track holds the data track weakly. It holds the
// subscriber's transport strongly, and the callback of `on_close` until it is replaced. It
// holds its byte statistics, whose task holds them until `close` stops them.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::signalanddatastats::BytesTrackStats;
use crate::go::time;
use crate::protocol::datatrack;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::Error;
use crate::rtc::types;
use crate::{infow, warnw};

pub struct DataDownTrackParams {
    pub logger: Logger,
    pub subscriber_id: livekit::ParticipantId,
    pub publish_data_track: Weak<dyn types::DataTrack>,
    pub handle: u16,
    pub transport: Arc<dyn types::DataTrackTransport>,
    pub bytes_track_stats: Option<Arc<BytesTrackStats>>,
}

pub struct DataDownTrack {
    params: DataDownTrackParams,
    logger: Logger,
    created_at: i64,

    // what `lock sync.Mutex` guards upstream
    inner: Mutex<DataDownTrackInner>,
}

#[derive(Default)]
struct DataDownTrackInner {
    closed: bool,
    on_close: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl DataDownTrack {
    pub fn new(params: DataDownTrackParams) -> Result<Arc<Self>, Error> {
        let mut d = Self {
            params,
            logger: Logger::default(),
            created_at: time::now().unix_nano(),
            inner: Mutex::new(DataDownTrackInner::default()),
        };
        d.logger = d
            .params
            .logger
            .with_values(&[("name", &d.name()), ("handle", &d.handle())]);
        let d = Arc::new(d);

        // a track that nobody holds any more is a closed one
        let added = match d.publish_data_track() {
            Some(publish_data_track) => publish_data_track.add_data_down_track(d.clone()),
            None => Err(Error::ReceiverClosed),
        };
        if let Err(err) = added {
            warnw!(d.logger, "could not add data down track", err);
            return Err(err);
        }

        infow!(d.logger, "created data down track");
        Ok(d)
    }

    pub fn close(&self) {
        let on_close = {
            let mut inner = self.inner.lock();
            let on_close = inner.on_close.clone();
            if inner.closed {
                return;
            }
            inner.closed = true;
            on_close
        };

        infow!(self.logger, "closing data down track");
        if let Some(bytes_track_stats) = &self.params.bytes_track_stats {
            bytes_track_stats.stop();
        }
        if let Some(publish_data_track) = self.publish_data_track() {
            publish_data_track.delete_data_down_track(&self.subscriber_id());
        }

        if let Some(on_close) = on_close {
            on_close();
        }
    }

    pub fn on_close(&self, f: Option<Box<dyn Fn() + Send + Sync>>) {
        let f: Option<Arc<dyn Fn() + Send + Sync>> = f.map(Arc::from);
        let closed = {
            let mut inner = self.inner.lock();
            inner.on_close = f.clone();
            inner.closed
        };
        if closed && let Some(f) = f {
            f();
        }
    }

    pub fn handle(&self) -> u16 {
        self.params.handle
    }

    pub fn publish_data_track(&self) -> Option<Arc<dyn types::DataTrack>> {
        self.params.publish_data_track.upgrade()
    }

    pub fn id(&self) -> livekit::TrackId {
        self.publish_data_track()
            .map(|publish_data_track| publish_data_track.id())
            .unwrap_or_default()
    }

    pub fn name(&self) -> String {
        self.publish_data_track()
            .map(|publish_data_track| publish_data_track.name())
            .unwrap_or_default()
    }

    pub fn subscriber_id(&self) -> livekit::ParticipantId {
        // add `createdAt` to ensure repeated subscriptions from same subscriber to same publisher does not collide
        livekit::ParticipantId::from(format!("{}:{}", self.params.subscriber_id, self.created_at))
    }

    pub fn write_packet(&self, _data: &[u8], packet: &datatrack::Packet, _arrival_time: i64) {
        let mut forwarded_packet = packet.clone();
        forwarded_packet.handle = self.params.handle;
        let buf = match forwarded_packet.marshal() {
            Ok(buf) => buf,
            Err(err) => {
                warnw!(self.logger, "could not marshal data track message", err);
                return;
            }
        };
        if let Err(err) = self.params.transport.send_data_track_message(&buf) {
            warnw!(self.logger, "could not send data track message", err);
            return;
        }
        if let Some(bytes_track_stats) = &self.params.bytes_track_stats {
            bytes_track_stats.add_bytes(buf.len() as u64, true);
        }
    }

    pub fn update_subscription_options(
        &self,
        _subscription_options: Option<&livekit::DataTrackSubscriptionOptions>,
    ) {
        // DT-TODO
    }
}

impl types::DataDownTrack for DataDownTrack {
    fn close(&self) {
        DataDownTrack::close(self);
    }

    fn on_close(&self, f: Option<Box<dyn Fn() + Send + Sync>>) {
        DataDownTrack::on_close(self, f);
    }

    fn handle(&self) -> u16 {
        DataDownTrack::handle(self)
    }

    fn publish_data_track(&self) -> Option<Arc<dyn types::DataTrack>> {
        DataDownTrack::publish_data_track(self)
    }

    fn update_subscription_options(
        &self,
        subscription_options: Option<&livekit::DataTrackSubscriptionOptions>,
    ) {
        DataDownTrack::update_subscription_options(self, subscription_options);
    }
}

impl types::DataTrackSender for DataDownTrack {
    fn subscriber_id(&self) -> livekit::ParticipantId {
        DataDownTrack::subscriber_id(self)
    }

    fn write_packet(&self, data: &[u8], packet: &datatrack::Packet, arrival_time: i64) {
        DataDownTrack::write_packet(self, data, packet, arrival_time);
    }
}
