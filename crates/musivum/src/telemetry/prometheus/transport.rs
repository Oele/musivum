//! The counters of the WebRTC layer's nodes, the sockets every peer connection shares. New
//! for musivum: LiveKit has no such metrics, and pion's UDP mux drops a datagram from an
//! address it knows no connection for without counting it. Here such datagrams are counted,
//! which shows from the outside whether a client's packets reach the server and find their
//! connection.

use super::{MetricFamily, Sample, Value};
use crate::webrtc::peerconnection::Node;

pub(super) fn collect(families: &mut Vec<MetricFamily>) {
    let totals = Node::totals();
    let sample = |outcome: &str, count: u64| Sample {
        label_values: vec![outcome.to_string()],
        value: Value::Counter(count),
    };
    families.push(MetricFamily {
        name: "musivum_node_datagrams_total",
        help: "Datagrams of the WebRTC sockets: received, of those without a connection for \
               their address (dropped) or with a full queue (dropped), and sent, or not taken \
               by the socket.",
        label_names: vec!["outcome"],
        samples: vec![
            sample("received", totals.datagrams_received),
            sample("without_route", totals.datagrams_without_route),
            sample("queue_full", totals.datagrams_queue_full),
            sample("sent", totals.datagrams_sent),
            sample("not_sent", totals.datagrams_not_sent),
        ],
    });
    families.push(MetricFamily {
        name: "musivum_node_tcp_streams_total",
        help: "ICE over TCP streams: given to a connection, ended, refused.",
        label_names: vec!["outcome"],
        samples: vec![
            sample("opened", totals.tcp_streams_opened),
            sample("closed", totals.tcp_streams_closed),
            sample("refused", totals.tcp_streams_refused),
        ],
    });
}
