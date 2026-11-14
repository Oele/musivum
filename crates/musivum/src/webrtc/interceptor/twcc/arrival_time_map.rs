// Ported from pion/interceptor, pkg/twcc/arrival_time_map.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the buffer that is nil before the first packet is an
// empty vector.

pub(super) const MIN_CAPACITY: usize = 128;
pub(super) const MAX_NUMBER_OF_PACKETS: i64 = 1 << 15;

// packetArrivalTimeMap is adapted from Chrome's implementation of TWCC, and keeps track
// of the arrival times of packets. It is used by the TWCC interceptor to build feedback
// packets.
// See https://source.chromium.org/chromium/chromium/src/+/refs/heads/main:third_party/webrtc/modules/remote_bitrate_estimator/packet_arrival_map.h;drc=b5cd13bb6d5d157a5fbe3628b2dd1c1e106203c6
#[derive(Default)]
pub(super) struct PacketArrivalTimeMap {
    // arrivalTimes is a circular buffer, where the packet with sequence number sn is stored
    // in slot sn % len(arrivalTimes)
    pub(super) arrival_times: Vec<i64>,

    // The unwrapped sequence numbers for the range of valid sequence numbers in arrivalTimes.
    // beginSequenceNumber is inclusive, and endSequenceNumber is exclusive.
    begin_sequence_number: i64,
    end_sequence_number: i64,
}

impl PacketArrivalTimeMap {
    /// AddPacket records the fact that the packet with sequence number sequenceNumber arrived
    /// at arrivalTime.
    #[allow(clippy::manual_range_contains)] // the comparisons as the original has them
    pub(super) fn add_packet(&mut self, sequence_number: i64, arrival_time: i64) {
        if self.arrival_times.is_empty() {
            // First packet
            self.reallocate(MIN_CAPACITY);
            self.begin_sequence_number = sequence_number;
            self.end_sequence_number = sequence_number + 1;
            let index = self.index(sequence_number);
            self.arrival_times[index] = arrival_time;

            return;
        }

        if sequence_number >= self.begin_sequence_number
            && sequence_number < self.end_sequence_number
        {
            // The packet is within the buffer, no need to resize.
            let index = self.index(sequence_number);
            self.arrival_times[index] = arrival_time;

            return;
        }

        if sequence_number < self.begin_sequence_number {
            // The packet goes before the current buffer. Expand to add packet,
            // but only if it fits within the maximum number of packets.
            let new_size = (self.end_sequence_number - sequence_number) as usize;
            if new_size > MAX_NUMBER_OF_PACKETS as usize {
                // Don't expand the buffer back for this packet, as it would remove newer received
                // packets.
                return;
            }
            self.adjust_to_size(new_size);
            let index = self.index(sequence_number);
            self.arrival_times[index] = arrival_time;
            self.set_not_received(sequence_number + 1, self.begin_sequence_number);
            self.begin_sequence_number = sequence_number;

            return;
        }

        // The packet goes after the buffer.
        let new_end_sequence_number = sequence_number + 1;

        if new_end_sequence_number >= self.end_sequence_number + MAX_NUMBER_OF_PACKETS {
            // All old packets have to be removed.
            self.begin_sequence_number = sequence_number;
            self.end_sequence_number = new_end_sequence_number;
            let index = self.index(sequence_number);
            self.arrival_times[index] = arrival_time;

            return;
        }

        if self.begin_sequence_number < new_end_sequence_number - MAX_NUMBER_OF_PACKETS {
            // Remove oldest entries.
            self.begin_sequence_number = new_end_sequence_number - MAX_NUMBER_OF_PACKETS;
        }

        self.adjust_to_size((new_end_sequence_number - self.begin_sequence_number) as usize);

        // Packets can be received out of order. If this isn't the next expected packet,
        // add enough placeholders to fill the gap.
        self.set_not_received(self.end_sequence_number, sequence_number);
        self.end_sequence_number = new_end_sequence_number;
        let index = self.index(sequence_number);
        self.arrival_times[index] = arrival_time;
    }

    fn set_not_received(&mut self, start_inclusive: i64, end_exclusive: i64) {
        for sn in start_inclusive..end_exclusive {
            let index = self.index(sn);
            self.arrival_times[index] = -1;
        }
    }

    /// BeginSequenceNumber returns the first valid sequence number in the map.
    pub(super) fn begin_sequence_number(&self) -> i64 {
        self.begin_sequence_number
    }

    /// EndSequenceNumber returns the first sequence number after the last valid sequence number in the map.
    pub(super) fn end_sequence_number(&self) -> i64 {
        self.end_sequence_number
    }

    /// FindNextAtOrAfter returns the sequence number and timestamp of the first received packet that has a sequence number
    /// greater or equal to sequenceNumber.
    pub(super) fn find_next_at_or_after(&self, sequence_number: i64) -> (i64, i64, bool) {
        for seq in self.clamp(sequence_number)..self.end_sequence_number {
            let arrival_time = self.get(seq);
            if arrival_time >= 0 {
                return (seq, arrival_time, true);
            }
        }

        (-1, -1, false)
    }

    /// EraseTo erases all elements from the beginning of the map until sequenceNumber.
    #[allow(dead_code)] // not called by the recorder, upstream neither: kept with its tests
    pub(super) fn erase_to(&mut self, sequence_number: i64) {
        if sequence_number < self.begin_sequence_number {
            return;
        }
        if sequence_number >= self.end_sequence_number {
            // Erase all.
            self.begin_sequence_number = self.end_sequence_number;

            return;
        }
        // Remove some
        self.begin_sequence_number = sequence_number;
        self.adjust_to_size((self.end_sequence_number - self.begin_sequence_number) as usize);
    }

    /// RemoveOldPackets removes packets from the beginning of the map as long as they are before
    /// sequenceNumber and with an age older than arrivalTimeLimit.
    pub(super) fn remove_old_packets(&mut self, sequence_number: i64, arrival_time_limit: i64) {
        let check_to = sequence_number.min(self.end_sequence_number);
        while self.begin_sequence_number < check_to
            && self.get(self.begin_sequence_number) <= arrival_time_limit
        {
            self.begin_sequence_number += 1;
        }
        self.adjust_to_size((self.end_sequence_number - self.begin_sequence_number) as usize);
    }

    /// HasReceived returns whether a packet with the sequence number has been received.
    pub(super) fn has_received(&self, sequence_number: i64) -> bool {
        self.get(sequence_number) >= 0
    }

    /// Clamp returns sequenceNumber clamped to [beginSequenceNumber, endSequenceNumber].
    pub(super) fn clamp(&self, sequence_number: i64) -> i64 {
        if sequence_number < self.begin_sequence_number {
            return self.begin_sequence_number;
        }
        if self.end_sequence_number < sequence_number {
            return self.end_sequence_number;
        }

        sequence_number
    }

    #[allow(clippy::manual_range_contains)] // the comparisons as the original has them
    fn get(&self, sequence_number: i64) -> i64 {
        if sequence_number < self.begin_sequence_number
            || sequence_number >= self.end_sequence_number
        {
            return -1;
        }

        self.arrival_times[self.index(sequence_number)]
    }

    fn index(&self, sequence_number: i64) -> usize {
        // Sequence number might be negative, and we always guarantee that arrivalTimes
        // length is a power of 2, so it's easier to use "&" instead of "%"
        (sequence_number & (self.capacity() as i64 - 1)) as usize
    }

    fn adjust_to_size(&mut self, new_size: usize) {
        if new_size > self.capacity() {
            let mut new_capacity = self.capacity();
            while new_capacity < new_size {
                new_capacity *= 2;
            }
            self.reallocate(new_capacity);
        }
        if self.capacity() > MIN_CAPACITY.max(new_size * 4) {
            let mut new_capacity = self.capacity();
            while new_capacity >= 2 * new_size.max(MIN_CAPACITY) {
                new_capacity /= 2;
            }
            self.reallocate(new_capacity);
        }
    }

    pub(super) fn capacity(&self) -> usize {
        self.arrival_times.len()
    }

    fn reallocate(&mut self, new_capacity: usize) {
        let mut new_buffer = vec![0i64; new_capacity];
        for sn in self.begin_sequence_number..self.end_sequence_number {
            new_buffer[(sn & (new_capacity as i64 - 1)) as usize] = self.get(sn);
        }
        self.arrival_times = new_buffer;
    }
}

#[cfg(test)]
#[path = "arrival_time_map_test.rs"]
mod arrival_time_map_test;
