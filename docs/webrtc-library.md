# The WebRTC library: `rtc`

Step 3 of `docs/PORTING.md`. Decided on 2026-11-14: musivum stands on **`rtc`**, the
protocol core of webrtc-rs (0.21.1, MIT or Apache-2.0), with its RTP crate patched by one
field. This page says why, what was run to find out, and what was not.

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

**webrtc 0.21** (not looked at further). The asynchronous interface on top of `rtc`. musivum
drives connections itself and has no use for a second layer; the 0.17 line, the older direct
port of pion, is in maintenance.

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

## What `rtc` lacks, and what is done about it

1. **Its RTP packet cannot carry a padding size.** `rtc-rtp` pads to a multiple of four
   bytes when marshalling and forgets the size when unmarshalling. LiveKit probes with
   padding of up to 255 bytes and counts padding received. `third_party/rtc-rtp` is the
   crate with one field more (`Header::padding_size`, as pion has it), used through
   `[patch.crates-io]`; the whole of `rtc` builds against it unchanged. To be proposed
   upstream, when the owner says so.
2. **RTCP stops at the interceptors.** By design: an application gets RTCP when one of its
   own interceptors marks it (`Attribute::DeliverToApplication`). musivum's own interceptor
   is where the ported code takes RTP and RTCP anyway.
3. **RTX from a publisher is unwrapped above the interceptors**, and its padding-only
   packets are dropped there. LiveKit's buffer unwraps RTX itself and counts it as such. At
   the interceptor closest to the wire the packets are still as they arrived, so that is
   where musivum takes them: the counterpart of pion's buffer factory.
4. **`RTCRtpSender::write_rtp` takes only the track's own SSRC.** RTX and padding on the
   repair SSRC go through `RTCPeerConnection::handle_write`, which takes them.

None of these changes the structure of the port.

## Not tested

- Other clients: Firefox, Safari, LiveKit's native SDKs (which Element X uses). First with
  the real signalling in place (step 5).
- A network that is not the loopback interface: address translation, loss and delay, ICE
  restarts when the address changes, TCP candidates, TURN. `rtc` has examples for ICE
  restart and for ICE over TCP; a TURN server that LiveKit embeds is a separate matter and
  stays an open question of the plan.
- Load: many connections and tracks on one socket, CPU per packet.
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
