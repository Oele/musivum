// Ported from pion/interceptor, pkg/flexfec/flexfec_encoder.go at v0.1.49.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust; only the constants and the two interfaces are here,
// which the FlexFEC-03 encoder stands on. `FlexEncoder20` is not ported: nothing here uses it,
// and the original says of it that it "is WIP, contains bugs and no tests". An encoder is
// changed by encoding (its sequence number moves on), so `encode_fec` takes `&mut self` and a
// factory gives a `Box`.

use crate::webrtc::rtp;

/// BaseRTPHeaderSize represents the minium RTP packet header size in bytes.
pub const BASE_RTP_HEADER_SIZE: usize = 12;
/// BaseFecHeaderSize represents the minium FEC payload's header size including the
/// required first mask.
pub const BASE_FEC_HEADER_SIZE: usize = 12;

/// EncoderFactory is an interface for generic FEC encoders.
pub trait EncoderFactory: Send + Sync {
    fn new_encoder(&self, payload_type: u8, ssrc: u32) -> Box<dyn FlexEncoder>;
}

/// FlexEncoder is the interface that FecInterceptor uses to encode Fec packets.
pub trait FlexEncoder: Send + Sync {
    fn encode_fec(
        &mut self,
        media_packets: &[rtp::Packet],
        num_fec_packets: u32,
    ) -> Vec<rtp::Packet>;
}
