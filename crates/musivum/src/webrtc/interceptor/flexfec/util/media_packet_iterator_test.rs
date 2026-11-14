// Ported from pion/interceptor, pkg/flexfec/util/media_packet_iterator_test.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; `assert.NotNil` on a new iterator has no counterpart
// (there is no nil one), so `test_new_media_packet_iterator` only makes one; that `reset`
// gives the iterator back is a comparison of addresses.

use super::*;

fn create_test_media_packets() -> Vec<rtp::Packet> {
    [1, 2, 3, 4, 5]
        .into_iter()
        .map(|sequence_number| rtp::Packet {
            header: rtp::Header {
                sequence_number,
                ssrc: 1,
                ..Default::default()
            },
            ..Default::default()
        })
        .collect()
}

#[test]
fn test_new_media_packet_iterator() {
    let media_packets = create_test_media_packets();
    let covered_indices = vec![1, 2, 4];
    let _iterator = MediaPacketIterator::new(&media_packets, covered_indices);
}

#[test]
fn test_media_packet_iterator_reset() {
    let media_packets = create_test_media_packets();
    let covered_indices = vec![1, 2, 4];
    let mut iterator = MediaPacketIterator::new(&media_packets, covered_indices);

    // Advance the iterator
    let _ = iterator.next();
    let _ = iterator.next();

    // Reset the iterator
    let result: *const MediaPacketIterator = iterator.reset();

    // After reset, HasNext should be true again
    assert!(iterator.has_next());

    // Reset should return the iterator for chaining
    assert!(
        std::ptr::eq(&iterator, result),
        "Reset should return the iterator for chaining"
    );
}

#[test]
fn test_media_packet_iterator_has_next() {
    let media_packets = create_test_media_packets();
    let covered_indices = vec![1, 2, 4];
    let mut iterator = MediaPacketIterator::new(&media_packets, covered_indices);

    // Initially, HasNext should be true
    assert!(iterator.has_next());

    // Advance to the last element
    let _ = iterator.next();
    let _ = iterator.next();
    assert!(iterator.has_next());

    // Advance past the last element
    let _ = iterator.next();
    assert!(!iterator.has_next());
}

#[test]
fn test_media_packet_iterator_next() {
    let media_packets = create_test_media_packets();
    let covered_indices = vec![1, 2, 4];
    let mut iterator = MediaPacketIterator::new(&media_packets, covered_indices);

    // First call to Next should return the first covered packet
    let packet = iterator.next();
    assert!(packet.is_some());
    assert_eq!(2u16, packet.unwrap().sequence_number);

    // Second call to Next should return the second covered packet
    let packet = iterator.next();
    assert!(packet.is_some());
    assert_eq!(3u16, packet.unwrap().sequence_number);

    // Third call to Next should return the third covered packet
    let packet = iterator.next();
    assert!(packet.is_some());
    assert_eq!(5u16, packet.unwrap().sequence_number);

    // Fourth call to Next should return nil
    let packet = iterator.next();
    assert!(packet.is_none());
}

#[test]
fn test_media_packet_iterator_first() {
    let media_packets = create_test_media_packets();
    let covered_indices = vec![1, 2, 4];
    let mut iterator = MediaPacketIterator::new(&media_packets, covered_indices);

    // First should return the first covered packet
    let packet = iterator.first();
    assert!(packet.is_some());
    assert_eq!(2u16, packet.unwrap().sequence_number);

    // First should not advance the iterator, so Next should still return the first packet
    let next_packet = iterator.next();
    assert!(next_packet.is_some());
    assert_eq!(2u16, next_packet.unwrap().sequence_number);

    // Even after advancing the iterator, First should still return the first packet
    let _ = iterator.next();
    let packet = iterator.first();
    assert!(packet.is_some());
    assert_eq!(2u16, packet.unwrap().sequence_number);
}

#[test]
fn test_media_packet_iterator_empty_covered_indices() {
    let media_packets = create_test_media_packets();
    let covered_indices = vec![];
    let mut iterator = MediaPacketIterator::new(&media_packets, covered_indices);

    assert!(!iterator.has_next());
    assert!(iterator.next().is_none());
}
