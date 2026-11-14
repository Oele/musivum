# musivum

A media server for voice and video calls that speaks [LiveKit](https://livekit.io)'s
protocol, written in Rust: a port of the part of LiveKit's server that receives each
participant's audio and video and forwards it to the others (its selective forwarding unit).

*Opus musivum* is the Latin name for mosaic work.

## Why

Matrix calls (MatrixRTC, as Element Call and Element X make them) send their media through a
LiveKit server, and today that means one implementation, in Go, run as a separate service
next to the homeserver. musivum is meant to be

- a second implementation that existing clients can use unchanged, because it speaks the same
  protocol;
- a library, so that a homeserver written in Rust can have calls built in instead of a second
  service to install, configure and proxy
  ([asaroton](https://github.com/Oele/asaroton) is the one it is written for);
- a standalone server as well, for everybody else.

## Status

Nothing runs yet. The project was set up on 2026-11-14; [docs/PORTING.md](docs/PORTING.md)
says what is ported, in which order and how it is checked.

## A port, not a new design

LiveKit's server is open source under the Apache License 2.0, and years of work on call
quality are in it: which video quality each participant gets as their connection changes,
how lost packets are recovered, how a switch between qualities goes without a freeze. musivum
takes that logic over, file by file, together with its tests, instead of working it out again
from the protocol. What LiveKit's server does for things other than calls (recording,
ingest, telephony, agents, running as a cluster) is left out.

The version it is ported from is pinned in [upstream.toml](upstream.toml). Files that are
ported say at their top which file of LiveKit they come from.

## License

Apache License 2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE). musivum is a derived work of
LiveKit, © LiveKit, Inc., and is not affiliated with or endorsed by LiveKit, Inc.
