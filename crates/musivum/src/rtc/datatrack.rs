// Ported from LiveKit, pkg/rtc/datatrack.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The track is made as an `Arc` and keeps a `Weak` of
// itself, which it gives to the down tracks it makes. `lock` guards the subscriptions, so
// they are in the lock; the track's info, which the original lists under the lock but never
// changes and reads without it, is outside and is a copy of what `new` is given. The fuse is
// a cancellation token. The spreader holds `Arc<dyn DataTrackSender>`s, for which the two
// lines after the track say that a data track sender is a sender of `sfu/utils`.
// `errAlreadySubscribed` and `errReceiverClosed` are `Error::AlreadySubscribed` and
// `Error::ReceiverClosed`. The `BytesTrackStats` of the params, a pointer that may be nil,
// is an `Option`; the one `add_subscriber` makes for a down track is made without the
// subscriber's `Kind()`, `KindDetails()` and reporter (see signalanddatastats.rs), and with a
// function that asks the subscriber for its telemetry listener through a `Weak` (the stats
// would otherwise hold the subscriber, which holds its down tracks); once the subscriber is
// gone it gives a listener that does nothing. Not here yet, with a comment at its place:
// `IsParticipantExemptFromTrackPermissionsRestrictions` of `room.go`, which is not ported:
// it stands at the end of this file until `room.rs` has it.
// Who holds whom: the track holds its subscribers (the participants) and their down tracks
// strongly, the down tracks a second time in the spreader, until `remove_subscriber` or a
// down track's `close` takes them out; `close` of the track itself takes nothing out, as
// upstream. A down track holds the track weakly. The callback `participant_id` of the
// params is held for as long as the track: the publisher hands in one that does not hold
// the publisher strongly. The byte statistics of a down track hold its subscriber weakly.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::datadowntrack::{DataDownTrack, DataDownTrackParams};
use super::datatrack_stats::{DataTrackStats, DataTrackStatsParams};
use super::signalanddatastats::BytesTrackStats;
use crate::infow;
use crate::protocol::datatrack;
use crate::protocol::livekit;
use crate::protocol::logger::Logger;
use crate::rtc::Error;
use crate::rtc::types;
use crate::sfu::utils as sfuutils;

pub struct DataTrackParams {
    pub logger: Logger,
    pub participant_id: Box<dyn Fn() -> livekit::ParticipantId + Send + Sync>,
    pub participant_identity: livekit::ParticipantIdentity,
    pub bytes_track_stats: Option<Arc<BytesTrackStats>>,
}

struct SubscribedDataTrack {
    subscriber: Arc<dyn types::LocalParticipant>,
    data_down_track: Arc<dyn types::DataDownTrack>,
}

pub struct DataTrack {
    params: DataTrackParams,

    logger: Logger,

    dti: livekit::DataTrackInfo,
    // what `lock sync.Mutex` guards upstream
    subscribed_tracks: Mutex<HashMap<livekit::ParticipantId, SubscribedDataTrack>>,

    down_track_spreader: sfuutils::DownTrackSpreader<Arc<dyn types::DataTrackSender>>,

    stats: DataTrackStats,

    closed: CancellationToken,

    this: Weak<DataTrack>,
}

// What the down track spreader (`sfu/utils`) asks of the down tracks it holds: upstream any
// type with the method will do, here a data track sender is one by these lines.
impl sfuutils::Sender for dyn types::DataTrackSender {
    fn subscriber_id(&self) -> livekit::ParticipantId {
        types::DataTrackSender::subscriber_id(self)
    }
}

impl DataTrack {
    pub fn new(params: DataTrackParams, dti: &livekit::DataTrackInfo) -> Arc<Self> {
        let logger = params
            .logger
            .with_values(&[("name", &dti.name), ("handle", &dti.pub_handle)]);
        let d = Arc::new_cyclic(|this: &Weak<DataTrack>| DataTrack {
            params,
            dti: dti.clone(),
            subscribed_tracks: Mutex::new(HashMap::new()),
            down_track_spreader: sfuutils::DownTrackSpreader::new(
                sfuutils::DownTrackSpreaderParams {
                    threshold: 20,
                    logger: logger.clone(),
                },
            ),
            stats: DataTrackStats::new(DataTrackStatsParams {
                logger: logger.clone(),
            }),
            logger,
            closed: CancellationToken::new(),
            this: this.clone(),
        });
        infow!(d.logger, "created data track", "dataTrackInfo" => d.dti);
        d
    }

    pub fn close(&self) {
        infow!(self.logger, "closing data track");
        self.closed.cancel();

        self.stats.close();
        if let Some(bytes_track_stats) = &self.params.bytes_track_stats {
            bytes_track_stats.stop();
        }
    }

    pub fn publisher_id(&self) -> livekit::ParticipantId {
        (self.params.participant_id)()
    }

    pub fn publisher_identity(&self) -> livekit::ParticipantIdentity {
        self.params.participant_identity.clone()
    }

    pub fn to_proto(&self) -> livekit::DataTrackInfo {
        self.dti.clone()
    }

    pub fn pub_handle(&self) -> u16 {
        self.dti.pub_handle as u16
    }

    pub fn id(&self) -> livekit::TrackId {
        livekit::TrackId::from(&self.dti.sid)
    }

    pub fn name(&self) -> String {
        self.dti.name.clone()
    }

    pub fn add_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
    ) -> Result<Option<Arc<dyn types::DataDownTrack>>, Error> {
        let mut subscribed_tracks = self.subscribed_tracks.lock();

        if subscribed_tracks.contains_key(&sub.id()) {
            return Err(Error::AlreadySubscribed);
        }

        let bytes_stats = BytesTrackStats::new(&sub.get_country(), &self.id(), &sub.id(), {
            // `sub.GetTelemetryListener`: see the header
            let sub = Arc::downgrade(&sub);
            move || -> Arc<dyn types::ParticipantTelemetryListener> {
                match sub.upgrade() {
                    Some(sub) => sub.get_telemetry_listener(),
                    None => Arc::new(types::NullParticipantTelemetryListener {}),
                }
            }
        });
        let data_down_track = match DataDownTrack::new(DataDownTrackParams {
            logger: sub.get_logger().with_values(&[("trackID", &self.id())]),
            subscriber_id: sub.id(),
            publish_data_track: self.this.clone(),
            handle: sub.get_next_subscribed_data_track_handle(),
            transport: sub.get_data_track_transport(),
            bytes_track_stats: Some(bytes_stats.clone()),
        }) {
            Ok(data_down_track) => data_down_track,
            Err(err) => {
                bytes_stats.stop();
                return Err(err);
            }
        };

        subscribed_tracks.insert(
            sub.id(),
            SubscribedDataTrack {
                subscriber: sub,
                data_down_track: data_down_track.clone(),
            },
        );
        Ok(Some(data_down_track))
    }

    pub fn remove_subscriber(&self, sub_id: &livekit::ParticipantId) {
        let subscribed_track = self.subscribed_tracks.lock().remove(sub_id);

        if let Some(subscribed_track) = subscribed_track {
            subscribed_track.data_down_track.close();
        }
    }

    pub fn is_subscriber(&self, sub_id: &livekit::ParticipantId) -> bool {
        let subscribed_tracks = self.subscribed_tracks.lock();

        subscribed_tracks.contains_key(sub_id)
    }

    pub fn revoke_disallowed_subscribers(
        &self,
        allowed_subscriber_identities: &[livekit::ParticipantIdentity],
    ) -> Vec<livekit::ParticipantIdentity> {
        let mut revoked_subscriber_identities = Vec::new();

        let mut disallowed = HashMap::new();
        {
            let subscribed_tracks = self.subscribed_tracks.lock();
            for (sub_id, subscribed_track) in subscribed_tracks.iter() {
                if is_participant_exempt_from_track_permissions_restrictions(
                    subscribed_track.subscriber.as_ref(),
                ) {
                    continue;
                }

                if !allowed_subscriber_identities.contains(&subscribed_track.subscriber.identity())
                {
                    disallowed.insert(sub_id.clone(), subscribed_track.subscriber.identity());
                }
            }
        }

        for (sub_id, sub_identity) in disallowed {
            infow!(
                self.logger,
                "revoking data track subscription",
                "subscriber" => sub_identity,
                "subscriberID" => sub_id,
            );
            self.remove_subscriber(&sub_id);
            revoked_subscriber_identities.push(sub_identity);
        }

        revoked_subscriber_identities
    }

    pub fn add_data_down_track(&self, dts: Arc<dyn types::DataTrackSender>) -> Result<(), Error> {
        if self.closed.is_cancelled() {
            return Err(Error::ReceiverClosed);
        }

        if self
            .down_track_spreader
            .has_down_track(&dts.subscriber_id())
        {
            infow!(
                self.logger,
                "subscriberID already exists, replacing data downtrack",
                "subscriberID" => dts.subscriber_id(),
            );
        }

        self.down_track_spreader.store(dts.clone());
        infow!(self.logger, "data downtrack added", "subscriberID" => dts.subscriber_id());
        Ok(())
    }

    pub fn delete_data_down_track(&self, subscriber_id: &livekit::ParticipantId) {
        self.down_track_spreader.free(subscriber_id);
        infow!(self.logger, "data downtrack deleted", "subscriberID" => subscriber_id);
    }

    pub fn handle_packet(&self, data: &[u8], packet: &datatrack::Packet, arrival_time: i64) {
        self.stats.update(packet, arrival_time, data.len());
        if let Some(bytes_track_stats) = &self.params.bytes_track_stats {
            bytes_track_stats.add_bytes(data.len() as u64, false);
        }

        self.down_track_spreader.broadcast(|dts| {
            dts.write_packet(data, packet, arrival_time);
        });
    }
}

impl types::DataTrack for DataTrack {
    fn id(&self) -> livekit::TrackId {
        DataTrack::id(self)
    }

    fn pub_handle(&self) -> u16 {
        DataTrack::pub_handle(self)
    }

    fn name(&self) -> String {
        DataTrack::name(self)
    }

    fn to_proto(&self) -> livekit::DataTrackInfo {
        DataTrack::to_proto(self)
    }

    fn publisher_id(&self) -> livekit::ParticipantId {
        DataTrack::publisher_id(self)
    }

    fn publisher_identity(&self) -> livekit::ParticipantIdentity {
        DataTrack::publisher_identity(self)
    }

    fn add_subscriber(
        &self,
        sub: Arc<dyn types::LocalParticipant>,
    ) -> Result<Option<Arc<dyn types::DataDownTrack>>, Error> {
        DataTrack::add_subscriber(self, sub)
    }

    fn remove_subscriber(&self, participant_id: &livekit::ParticipantId) {
        DataTrack::remove_subscriber(self, participant_id);
    }

    fn is_subscriber(&self, sub_id: &livekit::ParticipantId) -> bool {
        DataTrack::is_subscriber(self, sub_id)
    }

    fn revoke_disallowed_subscribers(
        &self,
        allowed_subscriber_identities: &[livekit::ParticipantIdentity],
    ) -> Vec<livekit::ParticipantIdentity> {
        DataTrack::revoke_disallowed_subscribers(self, allowed_subscriber_identities)
    }

    fn add_data_down_track(&self, sender: Arc<dyn types::DataTrackSender>) -> Result<(), Error> {
        DataTrack::add_data_down_track(self, sender)
    }

    fn delete_data_down_track(&self, subscriber_id: &livekit::ParticipantId) {
        DataTrack::delete_data_down_track(self, subscriber_id);
    }

    fn handle_packet(&self, data: &[u8], packet: &datatrack::Packet, arrival_time: i64) {
        DataTrack::handle_packet(self, data, packet, arrival_time);
    }

    fn close(&self) {
        DataTrack::close(self);
    }
}

// `IsParticipantExemptFromTrackPermissionsRestrictions` of room.go, which is not ported yet:
// this one goes when `room.rs` has it.
fn is_participant_exempt_from_track_permissions_restrictions(
    p: &dyn types::LocalParticipant,
) -> bool {
    // egress/recorder participants bypass permissions as auto-egress does not
    // have enough context to check permissions
    p.is_recorder()
}

#[cfg(test)]
#[path = "datatrack_test.rs"]
mod datatrack_test;
