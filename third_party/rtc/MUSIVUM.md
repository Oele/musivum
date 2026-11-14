# rtc, with a sender's codecs as pion has them

This directory is the crate `rtc` 0.21.1 of webrtc-rs (https://github.com/webrtc-rs/rtc, MIT
or Apache-2.0: the two licence files here are that repository's) as published on crates.io,
with one change, `MUSIVUM.patch`. Of the published crate only `src`, `Cargo.toml` (unchanged),
the licences and the README are here: its examples, tests, documents and scripts are not
needed to build it as a dependency.

The change, in two places:

- `RTCRtpReceiverInternal::get_codecs` (`src/rtp_transceiver/rtp_receiver/internal.rs`), the
  codecs of a transceiver that has codec preferences, keeps each preferred codec as it was
  given (its fmtp line and its payload type; of its RTCP feedback what the media engine has
  for that codec; the media engine's payload type where it has none), as pion's
  `RTPTransceiver.getCodecs` does. The crate gave the media engine's codec that matched the
  preference instead. This function is what the description is written from, and what a
  receiver's and (with the next point) a sender's parameters give.
- `RTCRtpSenderInternal::get_parameters` (`src/rtp_transceiver/rtp_sender/internal.rs`) gives
  the transceiver's codecs, as pion's `RTPSender.GetParameters` does: the media engine's codecs
  of the sender's kind until codec preferences are set. The crate gave the preferences only,
  and so no codecs at all until they were set.

Why: LiveKit, which musivum is a port of, reads a sender's codecs before it has set any
preferences (`configureSenderCodecs`, `configureSenderAudio` in `pkg/rtc/transport.go`), and
sets preferences whose parameters have to reach the description: `sprop-stereo=1` in Opus's
fmtp line, NACK taken away, and in an answer the payload type the offer gave Opus. Neither can
be done above the crate: its media engine cannot be read once a connection has it, and a
local description has to be set as the crate wrote it. What pion at LiveKit's pin writes for
the same was checked with a Go test laid over the reference, and is what musivum's
`test_codec_preferences_reach_the_description` expects.

It is used through `[patch.crates-io]`. To move to a newer rtc: replace the sources with the
new version's and apply `MUSIVUM.patch`. If the change is accepted upstream, this directory
goes away; it has not been proposed there yet.
