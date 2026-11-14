# rtc-ice, with a controlled agent that follows the nomination

This directory is the crate `rtc-ice` 0.21.1 of webrtc-rs (https://github.com/webrtc-rs/rtc,
MIT or Apache-2.0: the two licence files here are that repository's) as published on
crates.io, with one change, `MUSIVUM.patch`. Of the published crate `src`, `examples` (which
its unchanged `Cargo.toml` names, so that `cargo test` runs here), the licences and the README
are here.

The change is in how a controlled agent (musivum's side of every peer connection a client
opens, and of those it opens when the client answers as controlling) takes the controlling
agent's nominations, `ControlledSelector::handle_binding_request` and
`handle_success_response` in `src/agent/agent_selector.rs`. It now follows pion's controlled
selector, which LiveKit uses (pion/ice, as LiveKit builds it: livekit/ice v4.4.0-warp.3,
`selection.go`):

- A nomination (a check with USE-CANDIDATE) moves the selected pair to its pair when another
  pair is selected already: for a full agent when the new pair's priority is higher, for a
  lite agent always (pion's `shouldSwitchSelectedPair` and `needsToCheckPriorityOnNominated`;
  renomination, which rtc does not parse, is left out). The crate kept the first selected
  pair for good.
- A nomination of a pair whose check by this agent has not succeeded yet is remembered on the
  pair (`nominate_on_binding_success`, pion's `nominateOnBindingSuccess`) and selects the pair
  when that check succeeds, by the same rule (there a pair of equal priority too, as pion's
  `HandleSuccessResponse`). The crate dropped it.
- Every check is answered, a nomination too; the crate left a nomination of a pair it had not
  checked yet unanswered.
- A lite agent puts a nominated pair straight into the valid list (RFC 8445, Section 7.3.2)
  and sends no checks of its own (Section 7); the crate checked back. A full agent checks a
  pair back only while the pair is not valid yet or nothing is selected, as pion does.

Why: a client's first check usually arrives before its candidates do, so the first pair it
nominates has a peer-reflexive remote; it may then nominate a better pair, of a candidate it
trickled (another address family, a server-reflexive address). The crate went on sending
DTLS and media to the first pair. On a network that drops what comes in on a path the client
no longer uses (a phone's hotspot was one), ICE was connected and the connection never was;
on loopback the packets still arrived, so the browser tests did not see it.

`src/agent/agent_selector_musivum_test.rs` has the tests: a nomination that moves from the
first, peer-reflexive pair to a trickled one; a full agent that keeps a pair of higher
priority; a lite agent that follows every nomination without checks of its own; a check
without nomination that leaves the selected pair. Each fails on the published crate.

Not taken over from pion: the replacement of a peer-reflexive candidate by a signalled one at
the same address (RFC 8838, Section 11.4, pion's `replaceRedundantPeerReflexiveCandidates`).
Both stay; the pair found for an address is the first, so the peer-reflexive one goes on being
used for that address, which sends to the same place.

It is used through `[patch.crates-io]`. To move to a newer rtc-ice: replace the sources with
the new version's and apply `MUSIVUM.patch`. If the change is accepted upstream, this
directory goes away; it has not been proposed there yet.
