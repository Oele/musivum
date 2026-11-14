// Ported from LiveKit, pkg/sfu/codecmunger/codecmunger.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the errors are one enum, which also carries the error
// of marshalling a VP8 payload descriptor that the mungers pass on; the state, `any` in the
// original, is the enum `State` of the two types that travel through it, and nil is `None`;
// a munger has no lock (the forwarder guards it), so what changes it takes `&mut self`;
// `update_and_get` returns its three values in a `Result`, so there are no sizes and no
// header next to an error; the padding is a `Vec<u8>`, empty where the original returns nil;
// `as_any` is added for the places where the original asks for the type of a munger
// (`NewVP8FromOther`, and the forwarder's `f.codecMunger.(*codecmunger.DDTemporal)`).

use std::any::Any;

use crate::mediatransportutil::codec;
use crate::protocol::livekit;
use crate::sfu::buffer;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("not VP8")]
    NotVp8,
    #[error("out-of-order VP8 picture id not found in cache")]
    OutOfOrderVp8PictureIdCacheMiss,
    #[error("filtered temporal layer")]
    FilteredTemporalLayer,
    #[error("filtered out-of-order packet")]
    FilteredOutOfOrder,

    // The error of `codec.VP8`'s `MarshalTo` and `Marshal`, returned as it is.
    #[error(transparent)]
    Codec(#[from] codec::Error),
}

// MaxHeaderSize is the largest codec header the mungers produce (VP8: 6 bytes).
pub const MAX_HEADER_SIZE: usize = 8;

/// What `get_state` gives and `seed_state` takes, `any` in the original: one variant for
/// each type that `VP8.SeedState` knows there. Both hold the protocol's own types, so that
/// the forwarder passes its state on as it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    // *livekit.RTPForwarderState_Vp8Munger: the `codec_munger` of a forwarder's state
    CodecMunger(livekit::rtp_forwarder_state::CodecMunger),
    // *livekit.VP8MungerState
    Vp8MungerState(livekit::Vp8MungerState),
}

pub trait CodecMunger: Send + Sync {
    fn get_state(&self) -> Option<State>;
    fn seed_state(&mut self, state: Option<State>);

    fn set_last(&mut self, ext_pkt: &buffer::ExtPacket);
    fn update_offsets(&mut self, ext_pkt: &buffer::ExtPacket);

    // UpdateAndGet returns the incoming codec header size and the munged
    // header by value, so the per-packet path does not allocate.
    fn update_and_get(
        &mut self,
        ext_pkt: &buffer::ExtPacket,
        sn_out_of_order: bool,
        sn_has_gap: bool,
        max_temporal: i32,
    ) -> Result<(usize, [u8; MAX_HEADER_SIZE], usize), Error>;

    fn update_and_get_padding(&mut self, new_picture: bool) -> Result<Vec<u8>, Error>;

    /// The munger as itself, for `downcast_ref`: Go's type switch and type assertion on the
    /// interface.
    fn as_any(&self) -> &dyn Any;
}
