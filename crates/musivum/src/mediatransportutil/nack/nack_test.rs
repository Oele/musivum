// Ported from LiveKit's mediatransportutil, pkg/nack/nack_test.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

use super::*;

// []NackQueueInterface{NewNACKQueue(NackQueueParamsDefault), NewNACKQueueSafe(NackQueueParamsDefault)}
fn nack_queues() -> [Box<dyn NackQueueInterface>; 2] {
    [
        Box::new(NackQueue::new(NACK_QUEUE_PARAMS_DEFAULT)),
        Box::new(NackQueueSafe::new(NACK_QUEUE_PARAMS_DEFAULT)),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nack_queue_pairs() {
    struct PairsResult {
        pairs: Vec<NackPair>,
        num_seq_nums_nacked: usize,
    }

    struct Test {
        name: &'static str,
        args: Vec<u16>,
        want: PairsResult,
    }
    let tests = [
        Test {
            name: "Must return correct single pairs pair",
            args: vec![1, 2, 4, 5],
            want: PairsResult {
                pairs: vec![NackPair {
                    packet_id: 1,
                    lost_packets: 13,
                }],
                num_seq_nums_nacked: 4,
            },
        },
        Test {
            name: "Must return correct pair wrap",
            args: vec![65533, 2, 4, 5, 30, 32],
            want: PairsResult {
                pairs: vec![
                    NackPair {
                        packet_id: 65533,
                        lost_packets: (1 << 7) + (1 << 6) + (1 << 4),
                    },
                    NackPair {
                        packet_id: 30,
                        lost_packets: 1 << 1,
                    },
                ],
                num_seq_nums_nacked: 6,
            },
        },
        Test {
            name: "Must return 2 pairs pair",
            args: vec![1, 2, 4, 5, 20, 22, 24, 27],
            want: PairsResult {
                pairs: vec![
                    NackPair {
                        packet_id: 1,
                        lost_packets: 13,
                    },
                    NackPair {
                        packet_id: 20,
                        lost_packets: 74,
                    },
                ],
                num_seq_nums_nacked: 8,
            },
        },
    ];
    for tt in tests {
        for mut n in nack_queues() {
            for sn in &tt.args {
                n.push(*sn);
            }
            time::sleep(100 * time::MILLISECOND).await;
            let (got, num_seq_nums_nacked) = n.pairs();
            assert_eq!(tt.want.pairs, got, "{}", tt.name);
            assert_eq!(
                tt.want.num_seq_nums_nacked, num_seq_nums_nacked,
                "{}",
                tt.name
            );
        }
    }
}

#[test]
fn test_nack_queue_push() {
    struct Args {
        sn: Vec<u16>,
    }
    struct Test {
        name: &'static str,
        args: Args,
        want: Vec<u16>,
    }
    let tests = [Test {
        name: "Must keep packet order",
        args: Args {
            sn: vec![1, 3, 4, 5, 7, 8],
        },
        want: vec![1, 3, 4, 5, 7, 8],
    }];
    for tt in tests {
        for mut n in nack_queues() {
            for sn in &tt.args.sn {
                n.push(*sn);
            }
            let mut new_sn: Vec<u16> = Vec::new();
            for nack in n.nacks() {
                new_sn.push(nack.seq_num);
            }
            assert_eq!(tt.want, new_sn, "{}", tt.name);
        }
    }
}

#[test]
fn test_nack_queue_remove() {
    struct Args {
        sn: Vec<u16>,
    }
    struct Test {
        name: &'static str,
        args: Args,
        want: Vec<u16>,
    }
    let tests = [Test {
        name: "Must keep packet order",
        args: Args {
            sn: vec![1, 3, 4, 5, 7, 8],
        },
        want: vec![1, 3, 4, 7, 8],
    }];
    for tt in tests {
        for mut n in nack_queues() {
            for sn in &tt.args.sn {
                n.push(*sn);
            }
            n.remove(5);
            let mut new_sn: Vec<u16> = Vec::new();
            for nack in n.nacks() {
                new_sn.push(nack.seq_num);
            }
            assert_eq!(tt.want, new_sn, "{}", tt.name);
        }
    }
}
