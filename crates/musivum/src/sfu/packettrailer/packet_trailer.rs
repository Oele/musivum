// Ported from LiveKit, pkg/sfu/packettrailer/packet_trailer.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust.

pub const MAGIC: [u8; 4] = *b"LKTS";

const XOR_BYTE: u8 = 0xFF;

const ENVELOPE_SIZE: usize = 5; // 1B trailer_len + 4B magic

// StripTrailer returns the number of bytes to strip from the end of an RTP
// payload if it contains an LKTS trailer. The trailer is located by checking
// for the "LKTS" magic suffix and then reading the XORed trailer_len byte
// immediately before it. isEndOfFrame must be set only for packets ending an
// encoded frame, i. e. where a trailer could have been appended. Returns 0 if
// absent or ineligible.
pub fn strip_trailer(payload: &[u8], is_end_of_frame: bool) -> usize {
    if !is_end_of_frame || payload.len() < ENVELOPE_SIZE {
        return 0;
    }

    let tail = &payload[payload.len() - 4..];
    if tail[0] != MAGIC[0] || tail[1] != MAGIC[1] || tail[2] != MAGIC[2] || tail[3] != MAGIC[3] {
        return 0;
    }

    let trailer_len = (payload[payload.len() - 5] ^ XOR_BYTE) as usize;
    if trailer_len < ENVELOPE_SIZE || trailer_len > payload.len() {
        return 0;
    }

    trailer_len
}

#[cfg(test)]
#[path = "packet_trailer_test.rs"]
mod packet_trailer_test;
