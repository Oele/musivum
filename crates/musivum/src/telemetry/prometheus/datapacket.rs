// Ported from LiveKit, pkg/telemetry/prometheus/datapacket.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. Only the functions that ported code calls are here,
// more are added as they are needed. The collectors are musivum's own (see the module's
// description) and are statics, so there is no `initDataPacketStats` to call first; the
// constant labels `node_id` and `node_type` are left to whoever serves the metrics.

use super::{Histogram, MetricFamily, MetricVec};
use crate::protocol::livekit;

// promDataPacketStreamLabels
pub static PROM_DATA_PACKET_STREAM_DEST_COUNT: MetricVec<Histogram<9>, 2> =
    MetricVec::new(["type", "mime_type"], || {
        Histogram::new([1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 15.0, 25.0, 50.0])
    });
pub static PROM_DATA_PACKET_STREAM_SIZE: MetricVec<Histogram<10>, 2> =
    MetricVec::new(["type", "mime_type"], || {
        Histogram::new([
            128.0, 512.0, 2048.0, 8192.0, 32768.0, 131072.0, 524288.0, 2097152.0, 8388608.0,
            33554432.0,
        ])
    });

const PROM_DATA_PACKET_STREAM_MIME_TYPES: [&str; 5] =
    ["text", "image", "application", "audio", "video"];

// The collectors of this file under the names upstream registers them with.
pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    families.push(MetricFamily::of_vec(
        "livekit_datapacket_stream_dest_count",
        "",
        &PROM_DATA_PACKET_STREAM_DEST_COUNT,
    ));
    families.push(MetricFamily::of_vec(
        "livekit_datapacket_stream_bytes",
        "",
        &PROM_DATA_PACKET_STREAM_SIZE,
    ));
}

pub fn record_data_packet_stream(h: &livekit::data_stream::Header, dest_count: usize) {
    let stream_type = match &h.content_header {
        Some(livekit::data_stream::header::ContentHeader::TextHeader(_)) => "text",
        Some(livekit::data_stream::header::ContentHeader::ByteHeader(_)) => "bytes",
        None => "unknown",
    };

    let mut mime_type = h.mime_type.to_lowercase();
    if let Some(i) = mime_type.find('/') {
        mime_type.truncate(i);
    }
    if !PROM_DATA_PACKET_STREAM_MIME_TYPES.contains(&mime_type.as_str()) {
        mime_type = "unknown".to_string();
    }

    PROM_DATA_PACKET_STREAM_DEST_COUNT
        .with_label_values([stream_type, &mime_type])
        .observe(dest_count as f64);
    if let Some(total_length) = h.total_length {
        PROM_DATA_PACKET_STREAM_SIZE
            .with_label_values([stream_type, &mime_type])
            .observe(total_length as f64);
    }
}

// New: the original has no test of this file. The statics are shared with every test of the
// crate that reports to them while this one runs, so it expects "at least".
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_header_counts_under_its_type_and_mime_type() {
        let dest = |t: &str, m: &str| {
            PROM_DATA_PACKET_STREAM_DEST_COUNT
                .with_label_values([t, m])
                .snapshot()
        };
        let size = |t: &str, m: &str| {
            PROM_DATA_PACKET_STREAM_SIZE
                .with_label_values([t, m])
                .snapshot()
        };
        let (text, bytes, unknown) = (
            dest("text", "image"),
            size("bytes", "unknown"),
            dest("unknown", "audio"),
        );

        record_data_packet_stream(
            &livekit::data_stream::Header {
                mime_type: "Image/PNG".to_string(),
                content_header: Some(livekit::data_stream::header::ContentHeader::TextHeader(
                    Default::default(),
                )),
                ..Default::default()
            },
            3,
        );
        record_data_packet_stream(
            &livekit::data_stream::Header {
                mime_type: "model/gltf".to_string(),
                total_length: Some(1000),
                content_header: Some(livekit::data_stream::header::ContentHeader::ByteHeader(
                    Default::default(),
                )),
                ..Default::default()
            },
            1,
        );
        record_data_packet_stream(
            &livekit::data_stream::Header {
                mime_type: "audio".to_string(),
                ..Default::default()
            },
            1,
        );

        // the mime type is lowered and cut at the slash
        assert!(dest("text", "image").sum >= text.sum + 3.0);
        // a mime type that is none of the five is unknown, a length is recorded when there is one
        assert!(size("bytes", "unknown").sum >= bytes.sum + 1000.0);
        // without a content header the type is unknown
        assert!(dest("unknown", "audio").count > unknown.count);
    }
}
