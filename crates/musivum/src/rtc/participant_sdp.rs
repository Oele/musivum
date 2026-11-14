// Ported from LiveKit, pkg/rtc/participant_sdp.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The methods are those of `ParticipantImpl`
// (participant.rs), in an `impl` block of their own. The parsed descriptions are the WebRTC
// library's (`webrtc::sdp`, see its module), and the media descriptions a method hands back
// are references into the parsed offer it was given, where Go hands back pointers into it. What
// else differs:
// - `processUnmatch` logs "track without codecs" with the pending lock still held: the
//   original reads the pending track for the line after letting go of the lock.
// - `parsedAnswer.GetPayloadTypeForCodec(sdp.Codec{Name: "opus"})` of pion/sdp is not in the
//   port's `webrtc::sdp`: `get_payload_type_for_codec` at the end of this file is a copy of it
//   for a codec that has only a name (its error is pion's text), and `attribute_string` a copy
//   of pion's `Attribute.String()`, which is private to `webrtc::sdp` (see the report).
// - `parsedAnswer.Marshal()` of the library cannot fail: its error branch has no counterpart.
// - The library's attribute keeps its value as an `Option`: a value that is not there is
//   taken as the empty one, as pion has it, when Opus parameters are added to it.

use super::participant::{ParticipantImpl, as_media_track, downcast_media_track, equal_fold};
use super::{configure_receiver_codecs, is_red_enabled};
use crate::protocol::codecs::mime;
use crate::protocol::livekit;
use crate::protocol::sdp as lksdp;
use crate::webrtc::peerconnection::SessionDescription;
use crate::webrtc::sdp;
use crate::{debugw, errorw, infow, warnw};

impl ParticipantImpl {
    #[allow(clippy::type_complexity)]
    pub(super) fn populate_sdp_cid<'a>(
        &self,
        parsed_offer: &'a sdp::SessionDescription,
    ) -> (
        Vec<&'a sdp::MediaDescription>,
        Vec<&'a sdp::MediaDescription>,
    ) {
        let process_unmatch = |unmatches: &[&sdp::MediaDescription],
                               track_type: livekit::TrackType| {
            for unmatch in unmatches {
                let Some(stream_id) = lksdp::extract_stream_id(unmatch) else {
                    continue;
                };

                let sdp_codecs = match lksdp::codecs_from_media_description(unmatch) {
                    Ok(sdp_codecs) if !sdp_codecs.is_empty() => sdp_codecs,
                    Ok(_) => {
                        errorw!(
                            self.pub_logger,
                            "extract codecs from media section failed", nil,
                            "media" => unmatch,
                            "parsedOffer" => parsed_offer,
                        );
                        continue;
                    }
                    Err(err) => {
                        errorw!(
                            self.pub_logger,
                            "extract codecs from media section failed", err,
                            "media" => unmatch,
                            "parsedOffer" => parsed_offer,
                        );
                        continue;
                    }
                };

                let mut pt = self.pending_tracks_lock.write();
                if pt.is_released_track_cid_locked(&stream_id) {
                    continue;
                }

                let (signal_cid, info, _, migrated, _) =
                    pt.get_pending_track(&stream_id, track_type, false);
                if migrated {
                    continue;
                }

                let Some(mut info) = info else {
                    drop(pt);

                    // could be already published track and the unmatch could be a back up codec publish
                    let mut num_unmatched_tracks = 0;
                    let mut unmatched_track = None;
                    let mut unmatched_sdp_mime_type = mime::MimeType::Unknown;

                    let mut found = false;
                    for sdp_codec in &sdp_codecs {
                        let sdp_mime_type =
                            mime::normalize_mime_type_codec(&sdp_codec.name).to_mime_type();
                        for published_track in self.up_track_manager.get_published_tracks() {
                            let (sig_cid, sdp_cid) = as_media_track(published_track.clone())
                                .get_cids_for_mime_type(sdp_mime_type);
                            if !sig_cid.is_empty() && sdp_cid.is_empty() {
                                // a back up codec has a SDP cid match
                                if sig_cid == stream_id {
                                    found = true;
                                    break;
                                } else {
                                    num_unmatched_tracks += 1;
                                    unmatched_track = Some(published_track);
                                    unmatched_sdp_mime_type = sdp_mime_type;
                                }
                            }
                        }
                        if found {
                            break;
                        }
                    }
                    if !found && let Some(unmatched_track) = unmatched_track {
                        if num_unmatched_tracks != 1 {
                            warnw!(
                                self.pub_logger,
                                "too many unmatched tracks", nil,
                                "media" => unmatch,
                                "parsedOffer" => parsed_offer,
                            );
                        }
                        as_media_track(unmatched_track.clone())
                            .update_codec_sdp_cid(unmatched_sdp_mime_type, &stream_id);
                        debugw!(
                            self.pub_logger,
                            "published track SDP cid updated",
                            "trackID" => unmatched_track.id(),
                            "track" => unmatched_track.to_proto(),
                        );
                    }
                    continue;
                };

                if info.codecs.is_empty() {
                    warnw!(
                        self.pub_logger,
                        "track without codecs", nil,
                        "trackID" => info.sid,
                        "pendingTrack" => pt.pending_tracks.get(&signal_cid),
                        "media" => unmatch,
                        "parsedOffer" => parsed_offer,
                    );
                    continue;
                }

                let mut found = false;
                let mut updated = false;
                for sdp_codec in &sdp_codecs {
                    if mime::normalize_mime_type_codec(&sdp_codec.name)
                        == mime::get_mime_type_codec(&info.codecs[0].mime_type)
                    {
                        // set SdpCid only if different from SignalCid
                        if stream_id != info.codecs[0].cid {
                            info.codecs[0].sdp_cid = stream_id.clone();
                            updated = true;
                        }
                        found = true;
                        break;
                    }
                }

                // not using SimulcastCodec, i. e. mime type not available till track publish
                //
                // set SdpCid only if different from SignalCid
                if !found && info.codecs.len() == 1 && stream_id != info.codecs[0].cid {
                    info.codecs[0].sdp_cid = stream_id.clone();
                    updated = true;
                }

                if updated {
                    if let Some(pti) = pt.pending_tracks.get_mut(&signal_cid) {
                        pti.track_infos[0] = info.clone();
                    }
                    debugw!(
                        self.pub_logger,
                        "pending track SDP cid updated",
                        "signalCid" => signal_cid,
                        "trackID" => info.sid,
                        "pendingTrack" => pt.pending_tracks.get(&signal_cid),
                    );
                }
            }
        };

        let unmatch_audios = match self
            .transport_manager
            .get_unmatch_media_for_offer(parsed_offer, "audio")
        {
            Ok(unmatch_audios) => unmatch_audios,
            Err(err) => {
                warnw!(self.pub_logger, "could not get unmatch audios", err);
                return (Vec::new(), Vec::new());
            }
        };

        let unmatch_videos = match self
            .transport_manager
            .get_unmatch_media_for_offer(parsed_offer, "video")
        {
            Ok(unmatch_videos) => unmatch_videos,
            Err(err) => {
                warnw!(self.pub_logger, "could not get unmatch videos", err);
                return (Vec::new(), Vec::new());
            }
        };

        process_unmatch(&unmatch_audios, livekit::TrackType::Audio);
        process_unmatch(&unmatch_videos, livekit::TrackType::Video);
        (unmatch_audios, unmatch_videos)
    }

    pub(super) fn set_codec_preferences_for_publisher(
        &self,
        parsed_offer: &sdp::SessionDescription,
        unmatch_audios: &[&sdp::MediaDescription],
        unmatch_videos: &[&sdp::MediaDescription],
    ) {
        let unprocessed_unmatch_audios = self.set_codec_preferences_for_publisher_media(
            parsed_offer,
            unmatch_audios,
            livekit::TrackType::Audio,
        );
        self.set_codec_preferences_opus_red_for_publisher(
            parsed_offer,
            &unprocessed_unmatch_audios,
        );
        let _ = self.set_codec_preferences_for_publisher_media(
            parsed_offer,
            unmatch_videos,
            livekit::TrackType::Video,
        );
    }

    fn set_codec_preferences_for_publisher_media<'a>(
        &self,
        _parsed_offer: &sdp::SessionDescription,
        unmatches: &[&'a sdp::MediaDescription],
        track_type: livekit::TrackType,
    ) -> Vec<&'a sdp::MediaDescription> {
        let mut unprocessed = Vec::with_capacity(unmatches.len());
        for &unmatch in unmatches {
            let mid = lksdp::get_mid_value(unmatch);
            if mid.is_empty() {
                unprocessed.push(unmatch);
                continue;
            }
            let Some(transceiver) = self.transport_manager.get_publisher_rtp_transceiver(&mid)
            else {
                unprocessed.push(unmatch);
                continue;
            };

            let Some(stream_id) = lksdp::extract_stream_id(unmatch) else {
                unprocessed.push(unmatch);
                continue;
            };

            let ti = {
                let pt = self.pending_tracks_lock.read();
                match self.get_published_track_by_sdp_cid(&stream_id) {
                    Some(mt) => Some(mt.to_proto()),
                    None => pt.get_pending_track(&stream_id, track_type, false).1,
                }
            };

            let Some(ti) = ti else {
                unprocessed.push(unmatch);
                continue;
            };

            let mut mime_type = String::new();
            for c in &ti.codecs {
                if c.cid == stream_id || c.sdp_cid == stream_id {
                    mime_type = c.mime_type.clone();
                    break;
                }
            }
            if mime_type.is_empty() && !ti.codecs.is_empty() {
                mime_type = ti.codecs[0].mime_type.clone();
            }

            if mime_type.is_empty() {
                unprocessed.push(unmatch);
                continue;
            }

            configure_receiver_codecs(
                &transceiver,
                &mime_type,
                self.params
                    .client_info
                    .comply_with_codec_order_in_sdp_answer(),
            );
        }

        unprocessed
    }

    fn set_codec_preferences_opus_red_for_publisher(
        &self,
        parsed_offer: &sdp::SessionDescription,
        unmatch_audios: &[&sdp::MediaDescription],
    ) {
        for &unmatch_audio in unmatch_audios {
            let mid = lksdp::get_mid_value(unmatch_audio);
            if mid.is_empty() {
                continue;
            }
            let Some(transceiver) = self.transport_manager.get_publisher_rtp_transceiver(&mid)
            else {
                continue;
            };

            let Some(stream_id) = lksdp::extract_stream_id(unmatch_audio) else {
                continue;
            };

            let ti = {
                let pt = self.pending_tracks_lock.read();
                pt.get_pending_track(&stream_id, livekit::TrackType::Audio, false)
                    .1
            };
            let Some(ti) = ti else {
                continue;
            };

            let codecs = match lksdp::codecs_from_media_description(unmatch_audio) {
                Ok(codecs) => codecs,
                Err(err) => {
                    errorw!(
                        self.pub_logger,
                        "extract codecs from media section failed", err,
                        "media" => unmatch_audio,
                        "parsedOffer" => parsed_offer,
                    );
                    continue;
                }
            };

            let mut opus_payload: u8 = 0;
            for codec in &codecs {
                if mime::is_mime_type_codec_string_opus(&codec.name) {
                    opus_payload = codec.payload_type;
                    break;
                }
            }
            if opus_payload == 0 {
                continue;
            }

            let prefer_red = is_red_enabled(&ti);
            // if RED is enabled for this track, prefer RED codec in offer
            for codec in &codecs {
                // codec contain opus/red
                if prefer_red
                    && mime::is_mime_type_codec_string_red(&codec.name)
                    && codec.fmtp.contains(&(opus_payload as i64).to_string())
                {
                    configure_receiver_codecs(&transceiver, "audio/red", true);
                    break;
                }
            }
        }
    }

    // configure publisher answer for audio track's dtx and stereo settings
    pub(super) fn configure_publisher_answer(
        &self,
        mut answer: SessionDescription,
    ) -> SessionDescription {
        let Some(parsed_offer) = self.transport_manager.last_publisher_offer_parsed() else {
            return answer;
        };

        let Ok(mut parsed_answer) = answer.unmarshal() else {
            return answer;
        };

        // payload type lookup scans the whole answer, resolve it once
        let opus_pt =
            get_payload_type_for_codec(&parsed_answer, mime::MimeTypeCodec::Opus.as_str());
        let mut changed = false;
        for m in &mut parsed_answer.media_descriptions {
            match m.media_name.media.as_str() {
                "audio" => {
                    if sdp::attribute(&m.attributes, sdp::ATTR_KEY_INACTIVE).is_some() {
                        continue;
                    }
                    let Some(mid) = sdp::attribute(&m.attributes, sdp::ATTR_KEY_MID) else {
                        continue;
                    };
                    let mid = mid.to_string();
                    // find track info from offer's stream id
                    let mut ti: Option<livekit::TrackInfo> = None;
                    for om in &parsed_offer.media_descriptions {
                        if sdp::attribute(&om.attributes, sdp::ATTR_KEY_INACTIVE).is_some() {
                            continue;
                        }
                        if sdp::attribute(&om.attributes, sdp::ATTR_KEY_MID) == Some(mid.as_str()) {
                            let Some(stream_id) = lksdp::extract_stream_id(om) else {
                                continue;
                            };
                            match self
                                .get_published_track_by_sdp_cid(&stream_id)
                                .and_then(downcast_media_track)
                            {
                                None => {
                                    let pt = self.pending_tracks_lock.read();
                                    ti = pt
                                        .get_pending_track(
                                            &stream_id,
                                            livekit::TrackType::Audio,
                                            false,
                                        )
                                        .1;
                                }
                                Some(track) => {
                                    ti = Some(track.to_proto());
                                }
                            }
                            break;
                        }
                    }

                    let Some(ti) = ti else {
                        // no need to configure
                        continue;
                    };

                    let opus_pt = match &opus_pt {
                        Ok(opus_pt) => *opus_pt,
                        Err(err) => {
                            infow!(self.pub_logger, "failed to get opus payload type", "error" => err, "trackID" => ti.sid);
                            continue;
                        }
                    };

                    let prefix = format!("fmtp:{opus_pt}");
                    for attr in &mut m.attributes {
                        if attribute_string(attr).starts_with(&prefix) {
                            let value = attr.value.get_or_insert_with(String::new);
                            if !ti
                                .audio_features
                                .contains(&(livekit::AudioTrackFeature::TfNoDtx as i32))
                            {
                                value.push_str(";usedtx=1");
                            } else {
                                *value = value.replace(";usedtx=1", "");
                            }
                            if ti
                                .audio_features
                                .contains(&(livekit::AudioTrackFeature::TfStereo as i32))
                            {
                                value.push_str(";stereo=1;maxaveragebitrate=510000");
                            } else {
                                *value = value.replace(";stereo=1", "");
                            }
                            changed = true;
                        }
                    }
                }

                _ => continue,
            }
        }

        if !changed {
            return answer;
        }

        answer.sdp = parsed_answer.marshal();
        answer
    }
}

// pion/sdp's `GetPayloadTypeForCodec` for a wanted codec that has a name and nothing else
// (`codecsMatch` then compares the names only): the payload type of a codec of the description
// with that name. Its error is pion's `errCodecNotFound`. A copy (see the header).
fn get_payload_type_for_codec(
    desc: &sdp::SessionDescription,
    name: &str,
) -> Result<u8, &'static str> {
    let codecs = sdp::get_codec_map(&desc.media_descriptions);

    for (payload_type, codec) in codecs {
        if name.is_empty() || equal_fold(&codec.name, name) {
            return Ok(payload_type);
        }
    }

    Err("codec not found")
}

// pion/sdp's `Attribute.String()`. A copy (see the header).
fn attribute_string(a: &sdp::Attribute) -> String {
    match a.value.as_deref() {
        Some(value) if !value.is_empty() => format!("{}:{}", a.key, value),
        _ => a.key.clone(),
    }
}
