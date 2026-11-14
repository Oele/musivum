// Ported from pion/interceptor, pkg/twcc/arrival_time_map_test.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; the subtests are blocks that carry their name in the
// assertion messages.

use super::*;

#[test]
fn test_arrival_time_map() {
    {
        let name = "consistent when empty";
        let arrival_time_map = PacketArrivalTimeMap::default();
        assert_eq!(
            arrival_time_map.begin_sequence_number(),
            arrival_time_map.end_sequence_number(),
            "{name}"
        );
        assert!(!arrival_time_map.has_received(0), "{name}");
        assert_eq!(0, arrival_time_map.clamp(-5), "{name}");
        assert_eq!(0, arrival_time_map.clamp(5), "{name}");
    }

    {
        let name = "inserts first item into map";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 10);
        assert_eq!(42, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(43, arrival_time_map.end_sequence_number(), "{name}");

        assert!(!arrival_time_map.has_received(41), "{name}");
        assert!(arrival_time_map.has_received(42), "{name}");
        assert!(!arrival_time_map.has_received(43), "{name}");
        assert!(!arrival_time_map.has_received(44), "{name}");

        assert_eq!(42, arrival_time_map.clamp(-100), "{name}");
        assert_eq!(42, arrival_time_map.clamp(42), "{name}");
        assert_eq!(43, arrival_time_map.clamp(100), "{name}");
    }

    {
        let name = "inserts with gaps";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 0);
        arrival_time_map.add_packet(45, 11);
        assert_eq!(42, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(46, arrival_time_map.end_sequence_number(), "{name}");

        assert!(!arrival_time_map.has_received(41), "{name}");
        assert!(arrival_time_map.has_received(42), "{name}");
        assert!(!arrival_time_map.has_received(43), "{name}");
        assert!(!arrival_time_map.has_received(44), "{name}");
        assert!(arrival_time_map.has_received(45), "{name}");
        assert!(!arrival_time_map.has_received(46), "{name}");

        assert_eq!(0, arrival_time_map.get(42), "{name}");
        assert!(arrival_time_map.get(43) < 0, "{name}");
        assert!(arrival_time_map.get(44) < 0, "{name}");
        assert_eq!(11, arrival_time_map.get(45), "{name}");

        assert_eq!(42, arrival_time_map.clamp(-100), "{name}");
        assert_eq!(44, arrival_time_map.clamp(44), "{name}");
        assert_eq!(46, arrival_time_map.clamp(100), "{name}");
    }

    {
        let name = "find next at or after with gaps";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 0);
        arrival_time_map.add_packet(45, 11);

        let (seq, ts, ok) = arrival_time_map.find_next_at_or_after(42);
        assert_eq!(42, seq, "{name}");
        assert_eq!(0, ts, "{name}");
        assert!(ok, "{name}");

        let (seq, ts, ok) = arrival_time_map.find_next_at_or_after(43);
        assert_eq!(45, seq, "{name}");
        assert_eq!(11, ts, "{name}");
        assert!(ok, "{name}");
    }

    {
        let name = "inserts within buffer";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(45, 11);

        arrival_time_map.add_packet(43, 12);
        arrival_time_map.add_packet(44, 13);

        assert!(!arrival_time_map.has_received(41), "{name}");
        assert!(arrival_time_map.has_received(42), "{name}");
        assert!(arrival_time_map.has_received(43), "{name}");
        assert!(arrival_time_map.has_received(44), "{name}");
        assert!(arrival_time_map.has_received(45), "{name}");
        assert!(!arrival_time_map.has_received(46), "{name}");

        assert_eq!(10, arrival_time_map.get(42), "{name}");
        assert_eq!(12, arrival_time_map.get(43), "{name}");
        assert_eq!(13, arrival_time_map.get(44), "{name}");
        assert_eq!(11, arrival_time_map.get(45), "{name}");
    }

    {
        let name = "grows buffer and removes old";
        let mut arrival_time_map = PacketArrivalTimeMap::default();

        let large_seq_num: i64 = 42 + MAX_NUMBER_OF_PACKETS;
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(43, 11);
        arrival_time_map.add_packet(44, 12);
        arrival_time_map.add_packet(45, 13);
        arrival_time_map.add_packet(large_seq_num, 12);

        assert_eq!(43, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(
            large_seq_num + 1,
            arrival_time_map.end_sequence_number(),
            "{name}"
        );

        assert!(!arrival_time_map.has_received(41), "{name}");
        assert!(!arrival_time_map.has_received(42), "{name}");
        assert!(arrival_time_map.has_received(43), "{name}");
        assert!(arrival_time_map.has_received(44), "{name}");
        assert!(arrival_time_map.has_received(45), "{name}");
        assert!(!arrival_time_map.has_received(46), "{name}");
        assert!(arrival_time_map.has_received(large_seq_num), "{name}");
        assert!(!arrival_time_map.has_received(large_seq_num + 1), "{name}");
    }

    {
        let name = "sequence number jump deletes all";
        let mut arrival_time_map = PacketArrivalTimeMap::default();

        let large_seq_num: i64 = 42 + 2 * MAX_NUMBER_OF_PACKETS;
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(large_seq_num, 12);

        assert_eq!(
            large_seq_num,
            arrival_time_map.begin_sequence_number(),
            "{name}"
        );
        assert_eq!(
            large_seq_num + 1,
            arrival_time_map.end_sequence_number(),
            "{name}"
        );

        assert!(!arrival_time_map.has_received(42), "{name}");
        assert!(arrival_time_map.has_received(large_seq_num), "{name}");
        assert!(!arrival_time_map.has_received(large_seq_num + 1), "{name}");
    }

    {
        let name = "expands before beginning";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(-1000, 13);
        assert_eq!(-1000, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(43, arrival_time_map.end_sequence_number(), "{name}");

        assert!(!arrival_time_map.has_received(-1001), "{name}");
        assert!(arrival_time_map.has_received(-1000), "{name}");
        assert!(!arrival_time_map.has_received(-999), "{name}");
        assert!(arrival_time_map.has_received(42), "{name}");
        assert!(!arrival_time_map.has_received(43), "{name}");
    }

    {
        let name = "expanding before beginning keeps received";
        let mut arrival_time_map = PacketArrivalTimeMap::default();

        let small_seq_num: i64 = 42 - 2 * MAX_NUMBER_OF_PACKETS;
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(small_seq_num, 13);

        assert_eq!(42, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(43, arrival_time_map.end_sequence_number(), "{name}");
    }

    {
        let name = "erase to removes elements";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(43, 11);
        arrival_time_map.add_packet(44, 12);
        arrival_time_map.add_packet(45, 13);

        arrival_time_map.erase_to(44);

        assert_eq!(44, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(46, arrival_time_map.end_sequence_number(), "{name}");

        assert!(!arrival_time_map.has_received(43), "{name}");
        assert!(arrival_time_map.has_received(44), "{name}");
        assert!(arrival_time_map.has_received(45), "{name}");
        assert!(!arrival_time_map.has_received(46), "{name}");
    }

    {
        let name = "erases in empty map";
        let mut arrival_time_map = PacketArrivalTimeMap::default();

        assert_eq!(
            arrival_time_map.begin_sequence_number(),
            arrival_time_map.end_sequence_number(),
            "{name}"
        );

        arrival_time_map.erase_to(arrival_time_map.end_sequence_number());
        assert_eq!(
            arrival_time_map.begin_sequence_number(),
            arrival_time_map.end_sequence_number(),
            "{name}"
        );
    }

    {
        let name = "is tolerant to wrong arguments for erase";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(43, 11);

        arrival_time_map.erase_to(1);

        assert_eq!(42, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(44, arrival_time_map.end_sequence_number(), "{name}");

        arrival_time_map.erase_to(100);

        assert_eq!(44, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(44, arrival_time_map.end_sequence_number(), "{name}");
    }

    {
        let name = "erase all remembers beginning sequence number";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(42, 10);
        arrival_time_map.add_packet(43, 11);
        arrival_time_map.add_packet(44, 12);
        arrival_time_map.add_packet(45, 13);

        arrival_time_map.erase_to(46);
        arrival_time_map.add_packet(50, 10);

        assert_eq!(46, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(51, arrival_time_map.end_sequence_number(), "{name}");

        assert!(!arrival_time_map.has_received(45), "{name}");
        assert!(!arrival_time_map.has_received(46), "{name}");
        assert!(!arrival_time_map.has_received(47), "{name}");
        assert!(!arrival_time_map.has_received(48), "{name}");
        assert!(!arrival_time_map.has_received(49), "{name}");
        assert!(arrival_time_map.has_received(50), "{name}");
        assert!(!arrival_time_map.has_received(51), "{name}");
    }

    {
        let name = "erase to missing sequence number";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(37, 10);
        arrival_time_map.add_packet(39, 11);
        arrival_time_map.add_packet(40, 12);
        arrival_time_map.add_packet(41, 13);

        arrival_time_map.erase_to(38);

        arrival_time_map.add_packet(42, 40);

        assert_eq!(38, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(43, arrival_time_map.end_sequence_number(), "{name}");

        assert!(!arrival_time_map.has_received(37), "{name}");
        assert!(!arrival_time_map.has_received(38), "{name}");
        assert!(arrival_time_map.has_received(39), "{name}");
        assert!(arrival_time_map.has_received(40), "{name}");
        assert!(arrival_time_map.has_received(41), "{name}");
        assert!(arrival_time_map.has_received(42), "{name}");
        assert!(!arrival_time_map.has_received(43), "{name}");
    }

    {
        let name = "remove old packets";
        let mut arrival_time_map = PacketArrivalTimeMap::default();
        arrival_time_map.add_packet(37, 10);
        arrival_time_map.add_packet(39, 11);
        arrival_time_map.add_packet(40, 12);
        arrival_time_map.add_packet(41, 13);

        arrival_time_map.remove_old_packets(42, 11);

        assert_eq!(40, arrival_time_map.begin_sequence_number(), "{name}");
        assert_eq!(42, arrival_time_map.end_sequence_number(), "{name}");

        assert!(!arrival_time_map.has_received(39), "{name}");
        assert!(arrival_time_map.has_received(40), "{name}");
        assert!(arrival_time_map.has_received(41), "{name}");
        assert!(!arrival_time_map.has_received(42), "{name}");
    }

    {
        let name = "shrinks buffer when necessary";
        let mut m = PacketArrivalTimeMap::default();
        let large_seq_num: i64 = 100 + MAX_NUMBER_OF_PACKETS - 1;
        m.add_packet(100, 10);
        m.add_packet(large_seq_num, 11);

        m.erase_to(large_seq_num - 1);

        assert_eq!(large_seq_num - 1, m.begin_sequence_number(), "{name}");
        assert_eq!(large_seq_num + 1, m.end_sequence_number(), "{name}");

        assert_eq!(MIN_CAPACITY, m.capacity(), "{name}");
    }

    {
        let name = "find next at or after with invalid sequence";
        let mut m = PacketArrivalTimeMap::default();
        m.add_packet(100, 10);

        let (_, _, ok) = m.find_next_at_or_after(101);
        assert!(!ok, "{name}");
    }
}
