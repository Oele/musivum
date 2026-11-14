# rtc-rtp, with one field more

This directory is the crate `rtc-rtp` 0.21.1 of webrtc-rs (https://github.com/webrtc-rs/rtc,
MIT or Apache-2.0: the two licence files here are that repository's) as published on
crates.io, with one change, `MUSIVUM.patch`:

- `Header` has a field `padding_size`. Unmarshalling a packet fills it in; marshalling a
  packet with `padding` set and a non-zero `padding_size` writes that many padding octets
  instead of padding to a multiple of four.

Why: the WebRTC library musivum stands on (`rtc`) carries RTP as this crate's `Packet`, and
LiveKit's logic, which musivum is a port of, sends padding of a size it chooses (up to 255
octets, to probe for bandwidth) and counts the padding it receives. pion/rtp, which LiveKit
uses, has the same field (`Header.PaddingSize`).

It is used through `[patch.crates-io]`. To move to a newer rtc-rtp: replace the sources with
the new version's and apply `MUSIVUM.patch`. If the change is accepted upstream, this
directory goes away; it has not been proposed there yet.
