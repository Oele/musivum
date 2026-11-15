# rtc-dtls, with pion's retransmission backoff

This directory is the crate `rtc-dtls` 0.21.1 of webrtc-rs (https://github.com/webrtc-rs/rtc,
MIT or Apache-2.0: the two licence files here are that repository's) as published on
crates.io, with one change, `MUSIVUM.patch`. Of the published crate `src`, `examples` and
`benches` (which its unchanged `Cargo.toml` names), the licences and the README are here.

The change: the handshake waits for an answer to a flight the configured interval the first
time and twice as long at every retransmission, to 60 seconds at most, and the configured
interval again with the next flight, as pion's handshaker does (`handshaker.go`, pion/dtls
v3.1.5): `DTLSConn::retransmit_interval` in `src/handshaker.rs`, used where `send` arms the
retransmit timer. The crate waited the configured interval every time.

Why: the crate gives up a flight after seven retransmissions (`maximum_retransmit_number`),
so with a fixed interval the interval is also the handshake's whole budget divided by eight.
LiveKit sets pion's interval to 100 ms, which pion's backoff makes safe; musivum set the same
through `rtc` (`third_party/rtc`, `with_dtls_retransmission_interval`) to recover from a
lost packet quickly, and without the backoff a handshake had 0.8 s in all. An iPhone behind
a TURN relay on a mobile network did not answer in time, and the handshake failed with
`ErrInvalidFsmTransition` ("connect timeout after ICE connected"). With the backoff the
seven retransmissions cover 25.5 s, and a lost packet is still sent again after 100 ms.

Test: `a_short_retransmission_interval_backs_off_until_a_slow_server_answers` in
`src/endpoint.rs` (a client at 100 ms whose server answers only after two seconds; the waits
are 100, 200, 400, 800, 1600 ms and the handshake completes). It fails on the published
crate with `ErrInvalidFsmTransition`.

It is used through `[patch.crates-io]`. To move to a newer rtc-dtls: replace the sources with
the new version's and apply `MUSIVUM.patch`. If the change is accepted upstream, this
directory goes away; it has not been proposed there yet.
