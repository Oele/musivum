# The WebRTC library: `rtc`

Decided on 2026-11-14: musivum stands on **`rtc`**, the protocol core of webrtc-rs (0.21.1,
MIT or Apache-2.0), with its RTP crate patched by one field and five places of its own
patched. This page says why, what was run to find out, what musivum's connection layer on it
does, and what was not tested.

## What the library has to do

From the survey (`docs/port-map.md`): LiveKit answers and generates NACKs, writes and reads
RTX, generates and reads transport-wide feedback, estimates bandwidth, paces and probes, all
in its own code, and that code is what the port is for. So the library has to

1. hand over every RTP and RTCP packet of a connection as it arrived, and send the RTP and
   RTCP it is given as they are, on any SSRC of the connection;
2. not do retransmission, feedback, estimation or pacing itself, or let that be turned off;
3. do the rest: ICE (lite, one UDP port for all connections, restarts, TCP), DTLS, SRTP,
   SDP offer and answer in both directions with simulcast, data channels.

pion does this for LiveKit through two doors: interceptors, and a buffer factory that gets
every packet right after decryption.

## The three candidates

**str0m 0.25** (read, not run). Built for forwarding servers, and good at it in its own
way: it keeps what point 2 says the library must not keep. Its RTP mode gives the
application incoming RTP (`Event::RtpPacket`) and takes RTP to send as fields
(`StreamTx::write_rtp`), but retransmission is its own (`StreamTx::set_rtx_cache`, the
`nackable` flag), reports and NACKs are generated inside, transport-wide feedback is
generated and consumed inside, bandwidth estimation and pacing are its own
(`RtcConfig::enable_bwe`, `Event::EgressBitrateEstimate`). Incoming RTCP does not reach the
application except as a copy for debugging (`RtcConfig::enable_raw_packets`: "should not be
enabled outside of tests and troubleshooting"), and the application can send two kinds of
RTCP (`DirectApi::send_pli_feedback`, `send_app_specific_feedback`). On str0m, musivum
would keep LiveKit's layer selection and stream allocation and replace its retransmission,
feedback, estimation, pacing, probing and statistics from reports with str0m's: a different
server with LiveKit's signalling, not a port. That decided it without running it. It stays
the fallback if `rtc` fails in a way that cannot be mended.

**rtc 0.21** (run). webrtc-rs's core since 0.20: no I/O of its own, pion's vocabulary
(`RTCPeerConnection`, `MediaEngine`, `SettingEngine`, transceivers, an interceptor
registry). No interceptor is registered unless asked for, and an application can add its
own at any position, which is pion's first door and, placed closest to the wire, the second
one too.

**webrtc 0.21** (read, not run). The asynchronous interface on top of `rtc`: a handle per
peer connection with a background task that drives the core and does the I/O. It binds
sockets of its own for every connection (`PeerConnectionBuilder::with_udp_addrs`), so all
connections of a server cannot share one UDP port, which is how LiveKit is usually run and
what a homeserver with calls built in wants from its firewall. musivum drives `rtc` itself,
as the experiment does. The 0.17 line of `webrtc`, the older direct port of pion, is in
maintenance.

## The experiment

`experiments/weblib`: a forwarder on `rtc` of about 1,000 lines that does by hand what
LiveKit's code does (rewrites RTP, answers NACKs with RTX on the repair stream, numbers
packets for transport-wide feedback, passes PLIs on, sends REMB, pads on the RTX stream,
echoes data channels), with none of the library's interceptors and one of its own; and a
driver that runs two pages of headless Chrome 157 against it, one publishing the fake camera
(VP8, three simulcast layers) and microphone, one receiving on a connection the server
offers. `experiments/weblib/run.sh rtc-forwarder` runs it; all traffic is on the loopback
interface.

All 22 checks pass, in six runs out of six after one correction to the experiment itself:
at first the forwarder passed a layer's RTP timestamps through, each layer counts from a
start of its own, and whenever a switch made time jump backwards the browser threw the
frames away without a word (two runs out of three). That is what LiveKit's `RTPMunger`
exists for, and nothing the library does.

| What | Result |
|---|---|
| ICE lite, DTLS, two connections on one UDP port (told apart by the ICE user name, then by address) | connects |
| Answer to the browser's offer with simulcast; offer made by the server with RTX and a data channel | accepted by Chrome |
| The three simulcast layers | handed over, each with its rid |
| Header extensions of incoming RTP | handed over as bytes by id |
| RTCP from the publisher (SR, SDES) and from the subscriber (RR, NACK, PLI, transport-wide feedback) | handed over, once an interceptor of the program's own marks it for delivery |
| RTP written by the program with its own sequence numbers, payload type, SSRC and transport-wide sequence number | sent; the browser decodes it and reports on the numbers the program chose |
| Switching the forwarded layer (PLI written by the program, key frame arrives) | works |
| 5% of the forwarded packets dropped on purpose | the browser NACKs, the program answers with RTX it builds itself on the repair SSRC, the browser counts every one as a retransmission received |
| A NACK written by the program to the publisher | the publisher's RTX arrives at the program's interceptor as RTX |
| Padding-only packets of 255 bytes on the RTX stream | sent at that size (with the patch below); the subscriber keeps decoding |
| Data channel opened by the browser (unordered, no retransmissions) and one opened by the server | both echo |

Firefox 158 was put through a shorter run of the same (`driver/firefox-check.js`): it
connects in both directions, its three simulcast layers arrive with their rid, video
forwarded to it decodes, and its RTCP, transport-wide feedback included, is handed over. Two
things about the test and not about the library showed there: Firefox does not pair its own
addresses with a loopback address, so for it the forwarder's media socket has to be on
another interface (`BIND_IP`); and browsers number their codecs differently (Chrome offers
VP8 as payload type 96, Firefox as 120), which the first version of the forwarder had
hard-coded.

## What `rtc` lacks, and what is done about it

1. **Its RTP packet cannot carry a padding size.** `rtc-rtp` pads to a multiple of four
   bytes when marshalling and forgets the size when unmarshalling. LiveKit probes with
   padding of up to 255 bytes and counts padding received. `third_party/rtc-rtp` is the
   crate with one field more (`Header::padding_size`, as pion has it), used through
   `[patch.crates-io]`; the whole of `rtc` builds against it unchanged. To be proposed
   upstream; not proposed yet.
2. **RTCP stops at the interceptors.** By design: an application gets RTCP when one of its
   own interceptors marks it (`Attribute::DeliverToApplication`). musivum's own interceptor
   is where the ported code takes RTP and RTCP anyway.
3. **RTX from a publisher is unwrapped above the interceptors**, and its padding-only
   packets are dropped there. LiveKit's buffer unwraps RTX itself and counts it as such. At
   the interceptor closest to the wire the packets are still as they arrived, so that is
   where musivum takes them: the counterpart of pion's buffer factory.
4. **`RTCRtpSender::write_rtp` takes only the track's own SSRC.** RTX and padding on the
   repair SSRC go through `RTCPeerConnection::handle_write`, which takes them.
5. **A transceiver's codecs are not pion's.** `rtc` gives a sender no codecs until codec
   preferences are set (pion: the transceiver's, the media engine's of its kind until
   then); writes into the description, and gives a receiver, the media engine's codec that
   matches a preference instead of the preference, so what LiveKit sets on one
   (`sprop-stereo=1` in Opus's fmtp line, NACK taken away, the payload type an offer gave)
   did not reach the description; adds the media engine's repair codecs (RTX of a kept
   codec, FEC, RED) to preferences that leave them out (pion adds none, and drops an RTX
   whose codec is gone); and replaces a transceiver's preferences with the other side's
   codecs at every remote description, offer or answer (pion only for a transceiver a
   remote offer creates), so preferences did not outlive a renegotiation. `third_party/rtc`
   is `rtc` 0.21.1 with all of this as pion has it, in five places of about 150 changed
   lines (`MUSIVUM.md`, `MUSIVUM.patch`), used through `[patch.crates-io]`. Incoming payload
   types are mapped through the codecs the latest remote description gave the receiver's
   media section, where `rtc` used the preferences it had set from it (pion uses its media
   engine's negotiated codecs). It cannot be done above the crate: its media engine cannot
   be read once a connection has it, and a local description has to be set as the crate
   wrote it. To be proposed upstream; not proposed yet.

None of these changes the structure of the port.

## The connection layer, and what building it found

`crates/musivum/src/webrtc/peerconnection/` is musivum's peer connection on `rtc`: a node
with one UDP socket for all its connections, a `PeerConnection` with pion's method names
and callbacks, and the door LiveKit's buffer factory came through (`PacketSink`: every
incoming RTP packet as bytes and every RTCP packet, RTX, FEC and padding included).
`crates/musivum/src/webrtc/track.rs` has the traits the ported `pkg/sfu` meets it through
(`TrackLocal`, `TrackLocalWriter`, `TrackLocalContext`, `TrackRemote`, `RtpSender`,
`RtpReceiver`, `RtpTransceiver`), without a dependency on `rtc`. Its first twelve tests run two
connections against each other in one process (media and RTCP both ways, RTX and padding,
two clients on one port, data channels, renegotiation, ICE restart, callbacks that call
back in, close).

How it runs: one mutex per connection around `rtc`'s connection; every callback, the sink
and `bind`/`unbind` run on the connection's own task from an ordered queue with no lock
held; a write takes the mutex and has its datagrams on the socket before it returns; one
reader task for the node routes datagrams into a queue of 1,024 per connection. A callback
that blocks stalls that connection's reading and timers; a panic in a callback ends the
connection's task without a close event.

Found in `rtc` 0.21.1 while building it (the places named are under `src/` of the crate):

1. **The RTX stream of a simulcast layer is not paired with its layer**
   (`peer_connection/handler/interceptor.rs`, `bind_by_rid`, which has a TODO for the
   repair stream id). The RTX packets reach the sink, but no stream is announced for that
   SSRC and `TrackRemote::rtx_ssrc()` stays 0. LiveKit learns the pairing from the first
   RTX packets itself (`pkg/sfu/interceptor/rtx.go`), which is the part to port for it.
2. **A data channel added by a later negotiation never opens**: the SCTP association is
   only begun when DTLS completes (`peer_connection/handler/sctp.rs`). LiveKit creates its channels before the first offer.
3. A channel the remote opened reports no retransmission limits
   (`data_channel/internal.rs`, `accept`).
4. `rtc` has no public getters for the connection's states (`peer_connection/mod.rs`; the
   layer tracks them from events) and sets the gathering state back on every local
   description. An ICE restart reuses credentials that were fixed with
   `with_ice_credentials` (`peer_connection/internal.rs`), so a node must not fix them.
5. `set_local_description` insists on the text `create_offer` or `create_answer` gave
   (`peer_connection/internal.rs`):
   the SDP cannot be changed before it is set locally. LiveKit at the pin never does that
   (all five `SetLocalDescription` calls of `pkg/rtc/transport.go` pass what
   `CreateOffer` or `CreateAnswer` returned; pion requires it too). What it changes by hand
   is the copy that is signalled to the client after setting (candidates filtered out,
   Opus parameters in the publisher's answer) and remote descriptions before setting, both
   of which `rtc` allows. `tr.SetMid(mid)`, used for migration only, has no public
   counterpart in `rtc`.
6. A rollback removes transceivers by index (`peer_connection/internal.rs`); passed
   through, untested.
7. Not there yet in the layer: detached data channels with deadlines (LiveKit writes with a
   deadline; here `send` never blocks and `buffered_amount` is what the writer has to go
   by), what migration between nodes needs, the selected candidate pair other than through
   the statistics, pion's own congestion control interceptors (not used by default).

Added since, when the ports that use the layer asked for them: the selected candidate pair
(an event and an accessor; `rtc` has only an internal event, so the layer looks after each
STUN message and each ICE state change), a setting engine of the layer's own that can be
copied and changed per connection (`rtc`'s cannot be read or changed once built), the
addresses one connection advertises when they differ from the node's, ICE servers in the
configuration (kept and validated; nothing gathers from them), a distinct error for sending
on a closed data channel and any number of listeners for a channel's low threshold, and
`rtc`'s ICE and STUN types under the layer's names, so that nothing outside `webrtc/` names
the library. The layer has fifteen tests of its own now.

Still not there, each with what it would take:

- **Server reflexive and relay candidates** from ICE servers: a gatherer (a STUN and TURN
  client per connection). **mDNS**: a multicast socket and routing of its answers.
- **Announcing a track before its first packet** (`SetFireOnTrackBeforeFirstRTP`): `rtc`
  announces on the first packet only.
- **Detached data channels**: the ported writer waits on the buffered amount instead.
- **Migration between nodes** (`SetMid`, `SetSender`): left out with migration itself.
- For the packet sink: it now hears of local streams and of the connection's close, and
  buffers take the socket's arrival time. The connection can tell the codec of a payload
  type (`get_rtp_parameters_by_payload_type`), but the sink does not ask it yet: the repair
  stream of a simulcast layer is still announced to its user when it is paired, normally at
  its first packet.
- Nothing bounds the number of SSRCs a peer can make buffers for; pion has the same
  exposure.

Third round: a sender's track can be replaced on its transceiver without a negotiation
(`replace_track`, used by LiveKit to reuse a transceiver for the next subscription), and a
transceiver can be stopped through the seam. Unlike pion's, the replacement binds and
unbinds on the connection's task after the call returns, so a failure to bind the new track
is not returned by `replace_track`: it is reported where the track reports its binding.

Found then: **`rtc` forgets every receiver's stream at each negotiation** (`start_rtp` in
`peer_connection/internal.rs` resets the receiver's track). At the first packet after a
renegotiation the stream is bound again at the sink (`bind_remote_stream` a second time, no
unbind in between). The layer passes `on_track` on once per stream, as pion fires `OnTrack`
(found by the first call, where LiveKit's participant published the same track twice); the
sink has to expect the second bind. A transceiver stopped between a renegotiation and the
stream's next packet unbinds nothing at the sink.

Added since: **ICE over TCP** (RFC 6544), with no change to `rtc`. Its ICE agent tells TCP
from UDP by the protocol it is given, answers a check that came over a stream as a
peer-reflexive candidate of that stream, and marks everything it sends (DTLS, SRTP and SCTP
included) with the selected pair's protocol. A node made with `Node::new_with_tcp`
(LiveKit's `rtc.tcp_port`; `WebRtcConfig::new_node` does it from the configuration) listens
on a TCP port as well; its connections advertise passive TCP host candidates there unless
their network types leave TCP out (`force_tcp` leaves UDP out). A stream goes to the
connection named in its first binding request, and its packets then take the same way as
datagrams. Framing (RFC 4571) and limits are pion's TCP mux's: a first frame of at most 512
bytes within 30 s, later frames of at most 8192, and a write buffer of 4 MiB per stream; a
stream that breaks them is closed without touching the others, and a connection's streams
close with it. Active TCP (dialling the other side's passive candidates, once each) is there
for clients and the layer's tests; it is off unless `SettingEngine::active_tcp` is set,
where pion's is on unless disabled (LiveKit disables it on servers). Not there:
simultaneous-open, and keeping a stream with an unknown user name
until a connection asks for it (pion's mux does; here it is refused, which is safe because a
connection's user name is registered before its description is signalled). `rtc`'s agent
finds a remote candidate by address alone (`find_remote_candidate`), so a peer whose UDP
candidate and TCP stream share address and port is reported with a mixed pair. `rtc-shared`'s
own framing helper (`tcp_framing`) has no frame limit, which is why the layer has its own.

Added since: **the node's addresses and ports**. A node listens at each of its addresses
(`NodeConfig::addresses`, each with what it is advertised as), IPv4 and IPv6, with a UDP
socket of its own for each address and port and, with TCP, a TCP listener at each address,
as LiveKit's mux binds them (`CreateUDPMuxesFromPorts`). One socket on all addresses would
answer from the address the system picks for the way back, and a client takes no answer
from an address it did not ask (RFC 8445 7.2.5.2.1; `rtc`'s agent checks it). With several
ports (`udp_port` as a range: its first ports, as many as processors; `port_range_start`/
`_end`: the first free ports of the range, as many) each connection is given one socket at
each address in turn (`NewMultiPortsUDPMux`); a standalone port (`use_stun_port_as_ice`,
3478) is every connection's. A connection advertises the node's candidates of the network
types its settings allow, so `Udp6`/`Tcp6` and the transport's IPv6 exclusion act as
upstream. Writes go through a duplicate of each socket: tokio's `try_send_to` does not try
before the runtime has seen a new socket writable. The reading runtime has a thread per
processor. Different from LiveKit: a port range gives shared ports, not a port per
connection; TCP listens per address, not on all addresses (a stream at an address no
candidate names connects in neither).

Found when the transport was ported onto the layer (`rtc/transport_impl.rs`), the first
done since, the others each with a stand-in on the transport's side:

- **A sender's codecs** (done since): a transceiver made from a kind to send has a sender
  without a track (pion's has a track of its own that writes nothing), which `replace_track`
  can give one; a sender's codecs and the description are pion's through the patch above,
  also across renegotiations (preferences outlive later offers and answers; repair codecs
  they leave out stay out; checked against what pion writes for the same steps, and media
  sent after a renegotiation arrives with its codec). Still different:
  `get_rtp_parameters_by_payload_type` answers from the transceivers' codecs, so where
  preferences are set it gives the preferred codec, where pion's media engine gives the
  negotiated one; and, as with pion, a sender keeps its RTX and FEC streams where the media
  engine has those codecs, so preferences without them give a description with
  `a=ssrc-group:FID` or `FEC-FR` and no such codec.
- **No ICE transport object.** The selected pair comes from the layer's event; the RTT from
  the statistics, which have none under ICE lite.
- **Gathering is synchronous**, so the transport's paths that wait for gathering to end are
  kept and never taken.
- **Settings `rtc` lacks:** DTLS curves, skipping HelloVerify, DTLS retransmission interval
  and handshake timeout, closing by DTLS, SRTP replay protection off, SCTP zero checksum.

## Not tested

- Clients other than Chrome: Safari and WebKit (Element X on iOS among them) and LiveKit's
  native SDKs. Chrome, Element Call in Chrome and Element X on Android were run.
- Networks other than one host: address translation and ICE restarts when an address
  changes. Loss, delay and bandwidth caps are tested through a relay in user space
  (`tests/netem`); TURN and ICE over TCP with a browser on loopback and behind a homeserver
  on one host.
- Load beyond 100 subscribers of a track and 10 calls of 4 (`tests/load`).
- H.264, VP9 and AV1 with their layer modes, RED and FlexFEC: the library carries them as
  payload; what matters is in the ported code.
- How long `rtc` has been used in production. Its sans-I/O core dates from 2026; it is
  developed actively (0.21.1 is a month old) and is what webrtc-rs itself now stands on.
  That is younger than str0m, and is the risk taken with this choice.

## What follows for the port

- `crate::webrtc::rtp` (ported from pion/rtp) stays the packet type of the ported code; the
  adapter converts to and from `rtc`'s at the interceptor, where one copy of the header is
  made per packet.
- `crate::webrtc::rtcp` is `rtc-rtcp`, the same types `rtc` hands over.
- `pkg/rtc/transport.go` is ported against `RTCPeerConnection`, whose negotiation calls have
  pion's shape; the connections of a node are driven from tasks that own one UDP socket.
- LiveKit's two pion interceptors (`pkg/sfu/interceptor`) and its buffer factory become one
  interceptor of musivum's.
