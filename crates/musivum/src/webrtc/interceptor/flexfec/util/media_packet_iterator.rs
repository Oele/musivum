// Ported from pion/interceptor, pkg/flexfec/util/media_packet_iterator.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the iterator borrows the list of media packets, and
// `next` gives a reference to the packet in that list where the original gives a pointer to
// a copy of it; where the original gives nil there is `None`.

use crate::webrtc::rtp;

/// MediaPacketIterator supports iterating through a list of media packets protected by
/// a specific Fec packet.
#[derive(Debug)]
pub struct MediaPacketIterator<'a> {
    media_packets: &'a [rtp::Packet],
    covered_indices: Vec<u32>,
    next_index: usize,
}

impl<'a> MediaPacketIterator<'a> {
    /// NewMediaPacketIterator returns a new MediaPacketIterator.
    pub fn new(media_packets: &'a [rtp::Packet], covered_indices: Vec<u32>) -> Self {
        Self {
            media_packets,
            covered_indices,
            next_index: 0,
        }
    }

    /// Reset sets the starting iterating index back to 0.
    pub fn reset(&mut self) -> &mut Self {
        self.next_index = 0;

        self
    }

    /// HasNext indicates whether or not there are more media packets
    /// that can be iterated through.
    pub fn has_next(&self) -> bool {
        self.next_index < self.covered_indices.len()
    }

    /// Next returns the next media packet to iterate through.
    // The name is the original's; this is not the `Iterator` of the standard library.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&'a rtp::Packet> {
        if self.next_index == self.covered_indices.len() {
            return None;
        }
        let packet = &self.media_packets[self.covered_indices[self.next_index] as usize];
        self.next_index += 1;

        Some(packet)
    }

    /// First returns the first media packet to iterate through.
    pub fn first(&self) -> Option<&'a rtp::Packet> {
        if self.covered_indices.is_empty() {
            return None;
        }

        Some(&self.media_packets[self.covered_indices[0] as usize])
    }
}

#[cfg(test)]
#[path = "media_packet_iterator_test.rs"]
mod media_packet_iterator_test;
