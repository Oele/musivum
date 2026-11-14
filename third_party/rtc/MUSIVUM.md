# rtc, with a transceiver's codecs as pion has them, and pion's DTLS retransmission setting

This directory is the crate `rtc` 0.21.1 of webrtc-rs (https://github.com/webrtc-rs/rtc, MIT
or Apache-2.0: the two licence files here are that repository's) as published on crates.io,
with two changes, `MUSIVUM.patch`. Of the published crate only `src`, `Cargo.toml` (unchanged),
the licences and the README are here: its examples, tests, documents and scripts are not
needed to build it as a dependency.

The first change, a transceiver's codecs, in five places:

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
- The same `get_codecs` adds nothing to what the preferences say, and takes away an RTX codec
  whose codec is not there, from the preferences and from the media engine's codecs alike:
  `filter_unattached_rtx` (`src/rtp_transceiver/rtp_sender/rtp_codec.rs`, pion's
  `filterUnattachedRTX`, in the place of `is_repair_codec`). The crate added the media
  engine's repair codecs (the RTX of a kept codec, FEC, RED) to preferences that left them
  out. `RTCRtpTransceiverInternal::set_codec_preferences` (`src/rtp_transceiver/internal.rs`)
  filters what it keeps the same way, as pion's `RTPTransceiver.SetCodecPreferences`.
- `RTCPeerConnection::set_remote_description` (`src/peer_connection/mod.rs`) leaves the codec
  preferences of a transceiver that was there before the description (a remote offer's media
  section it was matched to, or a remote answer's) as they are, as pion does: it sets them
  from the description only for a transceiver that a remote offer creates
  (`setCodecPreferencesFromRemoteDescription`). The crate replaced them with the other side's
  codecs at every remote description, so a renegotiation lost what had been set on them. For
  such a transceiver it now calls `RTCRtpTransceiverInternal::set_remote_codecs_from_remote_description`
  (`src/rtp_transceiver/internal.rs`, split off `set_codec_preferences_from_remote_description`,
  which uses it), which gives what the description says to the receiver only and makes the
  sender's parameters anew.
- `RTCRtpReceiverInternal::get_codec_preferences` and `set_remote_codecs` (with the field
  `remote_codecs`): the crate maps incoming payload types (and RTX to its stream, and the
  payload types of its statistics) through `get_codec_preferences` of the receiver
  (`peer_connection/handler/interceptor.rs`, `handler/endpoint.rs`,
  `peer_connection/internal.rs`), which were the preferences it set from every remote
  description. It now gives the codecs the latest remote description gave the receiver's
  media section, and the preferences only until there is one; the receiver's parameters are
  still made from the preferences. pion maps incoming payload types through its media
  engine, which has what was negotiated.

Why: LiveKit, which musivum is a port of, reads a sender's codecs before it has set any
preferences (`configureSenderCodecs`, `configureSenderAudio` in `pkg/rtc/transport.go`), and
sets preferences whose parameters have to reach the description: `sprop-stereo=1` in Opus's
fmtp line, NACK taken away, in an answer the payload type the offer gave Opus, FlexFEC left
out when it is off (`filterCodecs` in `pkg/rtc/mediaengine.go`). It sets them once, and they
have to hold for every later offer and answer: a browser answers without `sprop-stereo`, and a
client's next offer carries its own codecs, NACK and no stereo. None of this can be done above
the crate: its media engine cannot be read once a connection has it, and a local description
has to be set as the crate wrote it. What pion at LiveKit's pin writes for the same steps was
checked with Go tests laid over the reference (`pkg/rtc`, with LiveKit's own
`configureSenderAudio`), and is what musivum's `test_codec_preferences_reach_the_description`,
`test_codec_preferences_outlive_an_answer`, `test_codec_preferences_outlive_an_offer` and
`test_codec_preferences_leave_repair_codecs_out` expect;
`test_media_after_a_renegotiation_has_its_codec` sends media both ways after a renegotiation
and checks the codec it is announced with (all in
`crates/musivum/src/webrtc/peerconnection/peerconnection_test.rs`).

As with pion, a sender's repair streams follow the media engine, not the preferences: a
sender keeps its RTX and FEC SSRCs where the media engine has RTX or FEC, so preferences that
leave them out make a description with `a=ssrc-group:FID` or `FEC-FR` and no such codec (pion
writes the same).

The second change: `SettingEngineBuilder::with_dtls_retransmission_interval`
(`src/peer_connection/configuration/setting_engine.rs`), pion's
`SetDTLSRetransmissionInterval`, passed to the DTLS transport (`RTCDtlsTransportConfig`,
`src/peer_connection/internal.rs`) and from there to the dtls crate's `with_flight_interval`
(`src/peer_connection/transport/dtls/mod.rs`). The crate built its DTLS configuration with the
dtls crate's default, a flight sent again after a second, seven times at most; LiveKit sets
100 ms. With 30% of the packets lost while two browsers joined (`tests/netem`,
`setup-loss-30`), LiveKit was connected in under a second and musivum not within the ten
seconds the client waits after ICE ("could not establish pc connection"); with 100 ms it
connects in 0.9 s. Unset, nothing changes.

It is used through `[patch.crates-io]`. To move to a newer rtc: replace the sources with the
new version's and apply `MUSIVUM.patch` (made with `diff -ru a/src b/src` against the
published crate). If the change is accepted upstream, this directory goes away; it has not
been proposed there yet.
