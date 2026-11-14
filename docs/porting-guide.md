# How a file is ported

The rules in `CLAUDE.md` say what may be ported and what a ported file has to be. This page
is the craft: how Go is written in Rust here, so that the files of the port read alike and
each can be followed as a diff against its original. `docs/port-map.md` says where a file
goes; `mediatransportutil/utils/wraparound.rs` with `wraparound_test.rs` is a file done this
way, to look at first.

## Where things are

- LiveKit at the pin: `reference/livekit` (`pkg/sfu`, `pkg/rtc`, ...).
- What LiveKit is built from, at the versions its `go.mod` names: `reference/gomod`
  (`github.com/livekit/mediatransportutil@...`, `github.com/livekit/protocol@...`,
  `github.com/pion/...`, `github.com/livekit/webrtc-pion/v4@...` which is the `pion/webrtc`
  LiveKit uses). Only open what `CLAUDE.md` allows; when in doubt, look at the module's
  `LICENSE` first.
- `reference/` is not committed and is not in a git worktree: use the absolute path of the
  main checkout.
- `scripts/go-reference-test.sh -run TestName -v ./pkg/sfu` runs an upstream test at the pin.
  That is how "what does the original do here" is answered.

## The header

```rust
// Ported from LiveKit, pkg/sfu/rtpmunger.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; <what else differs from the original, if anything>.
```

For the other origins the first two lines are:

```rust
// Ported from LiveKit's mediatransportutil, pkg/bucket/bucket.go at
// v0.0.0-20261014192331-dd8e31a0adc0.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.

// Ported from LiveKit's protocol, codecs/mime/mimetype.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.

// Ported from pion/rtp, packet.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
```

Take the copyright year from the original file. The "Changes" line is honest and short: say
what a reader comparing the two files would trip over (a type that became an enum, a method
that moved, something left out and why). A test file has the same header, naming the test
file it comes from. A file written new for musivum has no header.

## Names

- The same names in Rust's spelling: `snake_case` functions and fields, `CamelCase` types,
  `SCREAMING_CASE` constants. Acronyms are words: `RtpMunger`, `Vp8`, `Ssrc`, `Dd`, `Bwe`,
  `Nack`, `Pli`, `Rtx`, `TrackId`; in snake case `rtp_munger`, `ssrc`, `ext_sn`.
- Nothing is renamed for taste. `GetX()` stays `get_x()`, `IsX()` stays `is_x()`,
  `NewFoo(params)` is `Foo::new(params)`, a second constructor `NewFooBar` is
  `Foo::new_bar`.
- The functions stay in the order of the original, in one `impl` block where the original
  has methods one after the other. Comments of the original are kept, word for word, where
  they still apply.
- A Go package is a Rust module, a Go file a Rust file in it. The module file (`mod.rs`)
  re-exports what the Go package exports (`pub use rtpmunger::*;`), so that
  `sfu.RTPMunger` is `sfu::RtpMunger` and files do not have to know each other's names.

## Types

| Go | Rust |
|---|---|
| `uint8` ... `uint64`, `int8` ... `int64`, `float32`, `float64` | the same widths |
| `int`, `uint` | 64 bit in Go. `usize` where it is a length, an index or a count that is used as one; `i64`, or the width of what it is compared and converted with, where it is arithmetic that can go negative. Never narrower than the values need. |
| `time.Time`, `time.Duration`, `time.Now()`, `mono.Now()`, `time.AfterFunc` | `crate::go::time::{Time, Duration, now, after_func, ...}`; `mono.UnixNano()` is `crate::protocol::utils::mono::unix_nano()` |
| `atomic.Bool`, `atomic.Int32`, ... of `go.uber.org/atomic` and of `sync/atomic` | `crate::go::atomic::{Bool, Int32, ...}` |
| `sync.Mutex`, `sync.RWMutex` | `parking_lot::Mutex`, `RwLock`, see below |
| `[]byte` | `&[u8]` taken, `bytes::Bytes` kept (packets, payloads), `Vec<u8>` built |
| `string` | `&str` taken, `String` kept |
| `map[K]V` | `HashMap<K, V>`; Go's iteration order is random, so nothing depends on it |
| `[]T`, `[N]T` | `Vec<T>` or `&[T]`, `[T; N]` |
| a pointer that may be nil | `Option<Arc<T>>`, `Option<Box<T>>` or `Option<&T>`, whichever the ownership is |
| a struct embedded in another (`type ExtPacket struct { VideoLayer; ... }`) | a field named after the type in snake case (`video_layer: VideoLayer`); `ep.Spatial` is `ep.video_layer.spatial` |
| `error` | `Result<T, Error>` with one `Error` enum per package (`thiserror`), its variants the package's `ErrX` values, its messages the original's |
| `interface{ ... }` | a trait, `Send + Sync`; held as `Arc<dyn Trait>` |
| `func(...)` fields and parameters | `Box<dyn Fn(...) + Send + Sync>` kept, `impl Fn(...)` taken |
| an `iota` enumeration | an `enum`, with `Display` where the original has `String()`, giving the same text |
| `logger.Logger` | `crate::protocol::logger::Logger` |
| `rtp.Packet`, `rtp.Header` | `crate::webrtc::rtp::{Packet, Header}` |
| `rtcp.X` | `crate::webrtc::rtcp::...` (the crate `rtc-rtcp`) |
| `webrtc.RTPCodecParameters` and the like | `crate::webrtc::...` |
| `mime.MimeType` | `crate::protocol::codecs::mime::MimeType` |
| `livekit.X` (a message of the protocol) | `crate::protocol::livekit::X` (generated by prost: fields are public, an absent message is `None`, an enum field is an `i32` with an accessor of the field's name that gives the enum) |
| `livekit.TrackID` and the other identifiers | `crate::protocol::livekit::TrackId`, ... |

## Arithmetic

This is where a translation goes wrong without anybody noticing.

- Go's integers wrap silently. Rust's panic in a debug build and wrap in a release build.
  Wherever the original can wrap (sequence numbers, RTP timestamps, anything `uint16` or
  `uint32` that is added to or subtracted from, differences of unsigned values) write
  `wrapping_add`, `wrapping_sub`, `wrapping_mul`. When in doubt, wrap: that is what the
  original does.
- A Go conversion between integer types truncates or reinterprets: `uint16(x)` is
  `x as u16`, `int32(u)` is `u as i32`. Keep every conversion the original makes, in the
  same place.
- Comparisons of differences such as `(a - b) < (1 << 15)` on unsigned types are wrapping
  subtractions followed by the comparison.
- Float to integer: Go's result for a value out of range is undefined, Rust's `as`
  saturates. Keep `as`.
- Integer division and remainder truncate towards zero in both languages.
- A shift by at least the width of the type gives zero in Go and panics in Rust: use
  `checked_shl(..).unwrap_or(0)` where the count is not a constant.
- `time.Duration` is signed: `crate::go::time::Duration` is too. Do not convert to
  `std::time::Duration` except to hand a wait to the runtime (`to_std()`).
- Go's zero values: every field of a Go struct starts as zero, so the Rust struct derives
  or implements `Default` to the same values, and a Go struct literal that leaves fields out
  is written with `..Default::default()`.

## Locks, shared objects and callbacks

- A Go struct with its own lock keeps a lock of its own, and its methods take `&self`. The
  fields the lock guards move to an inner struct:

  ```rust
  pub struct Foo {
      params: FooParams,        // read-only after construction: outside the lock
      logger: Logger,
      closed: atomic::Bool,     // atomics: outside the lock
      inner: Mutex<FooInner>,   // what `lock sync.Mutex` guards upstream
  }
  ```

  A function the original calls with the lock held (often named `...Locked`) becomes a
  method of the inner struct, or takes `&mut FooInner`. A `sync.RWMutex` is an `RwLock` with
  `read()` where the original takes `RLock()`.
- A Go struct without a lock, which its owner guards, has `&mut self` methods for what
  changes it.
- A lock is held for the same stretch as in the original, and never across a call out of
  the object that the original makes after unlocking. Where the original copies a callback
  or a value under the lock and uses it after unlocking, do the same: it is how deadlocks are
  avoided there.
- parking_lot's locks are not reentrant, like Go's.
- An object several others hold (a receiver, a down track, a buffer) is made as an `Arc`,
  by a `new` that returns `Arc<Self>`; a reference back to an owner is a `Weak` or a
  callback.
- A goroutine that waits (a worker with a ticker, a channel or a sleep) is a task:
  `crate::go::time::spawn(async move { ... })`, waiting with `crate::go::time::sleep`. A
  `chan struct{}` that is closed to stop a worker is a `tokio::sync::Notify` or a
  `tokio_util::sync::CancellationToken`; a `core.Fuse` is the latter.
- `defer` is a scope guard or the code written out at each return; a `defer` that unlocks is
  just the guard going out of scope.

## Logging

```rust
use crate::{debugw, infow, warnw, errorw};

debugw!(self.logger, "stream started", "layer" => layer, "bitrate" => bitrate);
warnw!(self.logger, "could not write", err, "size" => size);   // err is anything Display
warnw!(self.logger, "odd state", nil, "state" => state);       // Go passed nil
```

The message and the keys stay as they are, so that logs of both servers can be compared.
Values are written with `Debug`. `MarshalLogObject` of the original becomes `Debug`
(derived where that gives the same fields); a wrapper type that exists only to log something
lazily is not needed, since the macros do not evaluate values when the level is off.

## Tests

- `foo_test.go` becomes `foo_test.rs` next to `foo.rs`, which ends with

  ```rust
  #[cfg(test)]
  #[path = "foo_test.rs"]
  mod foo_test;
  ```

  A test file that has no source file of its name is attached to the module file the same
  way.
- Every test function is ported, under its name in snake case (`TestWrapAroundUint16` is
  `test_wrap_around_uint16`), in the original order, with the same inputs and the same
  expected values. A `t.Run("name", ...)` is a block that carries the name in its assertion
  messages, or a loop over the table as in `wraparound_test.rs`. Benchmarks are left out and
  listed in the report.
- `require.Equal(t, expected, actual)` is `assert_eq!(expected, actual)`, in that order.
  `require.InDelta` is `crate::testutils::assert_in_delta`. `require.Eventually` is
  `crate::testutils::eventually`.
- A test that needs the runtime (tasks, timers, sleeps) is
  `#[tokio::test(flavor = "multi_thread")]`.
- Test helpers of the original that live in non-test packages (`pkg/sfu/testutils`) are
  ported as modules compiled for tests only, in the same place.
- When a ported test fails, the port is wrong until shown otherwise. Run the upstream test
  to see what the original does. An expected value is never changed to make a test pass.
- When a ported test passes, check that it can fail: change something in the code under test
  that the test is about, see it fail, change it back. A test that cannot fail is a
  translation mistake in the test.
- A test of what the code logs captures it with `crate::testutils::capture_log` or
  `LogCapture`, never with a subscriber of its own installed for the test's thread: that
  loses lines now and then (the reason is written at `install_log_capture`).
- An assertion on allocations per call (`testing.AllocsPerRun`) is left out, with a comment
  at its place: counting them needs `unsafe`. The rest of such a test is kept.
- If something of the original cannot be ported as it is (a test that needs pion's
  internals, a helper that has no counterpart) say so in a comment at that place and in the
  report. Do not leave it out silently and do not write a different test under the same
  name.

## Fakes of interfaces

`counterfeiter:generate` on an interface becomes `#[cfg_attr(test, musivum_fakes::fake)]` on
the trait (`#[musivum_fakes::fake]` in a test-only module). Next to the trait, with its
visibility, that makes `FakeTrait` (`Default`, `Send + Sync`, `Debug`, implements the trait)
and `FakeTraitState`. A test makes `Arc::new(FakeTrait::default())`, hands a clone to the
code under test as `Arc<dyn Trait>` and keeps the `Arc` to configure and question the fake.
The full text and compiled examples are in the rustdoc of `musivum_fakes::fake`.

For every method `x` (returning `R`), all on `&self`:

    x_returns(&self, value: R)               // all later calls; takes a stub away
    x_returns_with(&self, value: impl Fn() -> R + Send + Sync + 'static)
    x_returns_on_call(&self, i: usize, value: R)      // call i, from 0; takes a stub away
    x_calls(&self, stub: impl Fn(args..) -> R + Send + Sync + 'static)
    x_stub(&self, stub: ..)                  // the same: Go's `fake.XStub = f`
    x_call_count(&self) -> usize
    x_args_for_call(&self, i: usize) -> A    // the argument if one, a tuple if several

No `x_args_for_call` for a method without parameters, no `x_returns*` for one that returns
nothing. `r#type` gives `type_returns`. A call is recorded first; then the stub answers if
set, else the value for this call, else the value for all calls, else the zero value. A stub
runs with no lock held and may call back into the fake.

| Go | Rust |
|---|---|
| `p := &typesfakes.FakeLocalParticipant{}` | `let p = Arc::new(FakeLocalParticipant::default());` |
| `p.IdentityReturns("alice")` | `p.identity_returns("alice".into());` |
| `p.GetAudioLevelReturns(20, true)` | `p.get_audio_level_returns((20.0, true));` |
| `r.ReadRTPReturns(0, io.EOF)` | `r.read_rtp_returns(Err(Error::Eof));` or `r.read_rtp_returns_with(\|\| Err(Error::Eof));` |
| `r.AddOnReadyCalls(func(f func()) { f() })` | `r.add_on_ready_calls(\|f\| f());` |
| `p.CloseStub = func(bool, types.ParticipantCloseReason, bool) error { ...; return nil }` | `p.close_stub(move \|_, _, _\| { ...; Ok(()) });` |
| `require.Equal(t, 2, p.SubscribeToTrackCallCount())` | `assert_eq!(2, p.subscribe_to_track_call_count());` |
| `require.Equal(t, req.Key, publisher.GetDataBlobArgsForCall(0))` | `assert_eq!(req.key, publisher.get_data_blob_args_for_call(0));` |
| `_, got, _, _ := fp.SendDataMessageArgsForCall(0)` | `let (_, got, _, _) = fp.send_data_message_args_for_call(0);` |
| `p := participants[0].(*typesfakes.FakeLocalParticipant)` | `let p = fakes::downcast::<FakeLocalParticipant>(participants[0].clone());` (the trait needs `Any` as a supertrait) |

Arguments (what `x_args_for_call` gives a clone of):
- `T` by value: `T`. An `Arc<dyn Trait>`, `Option<Arc<..>>` or `Arc<dyn Fn..>` is the same
  object, which the test can call.
- `&T`: `T` (by `Clone`). `&str`: `String`. `&[T]`: `Vec<T>`. `Option<&T>`, `Option<&str>`,
  `Option<&[T]>`: the `Option` of those.
- Not recorded: `&mut ..`, `&dyn ..`, `Box<dyn ..>` (also in `Option` or `Pin`), a type that
  still borrows (`&[&str]`, `Cow<'_, str>`), and a parameter marked
  `#[cfg_attr(test, fake(skip))]`. Its place in the tuple holds
  `testutils::fakes::NotRecorded`. A stub still gets every argument as the method got it.
- A recorded type must be `Clone + Send + 'static`. Otherwise the compiler says "the fake
  cannot record an argument of type `X`" at the parameter; mark it with `fake(skip)`, or
  make the type `Clone`, or pass it in an `Arc`.

Return values:
- Nothing set: `Default::default()`; for `Result<T, E>` it is `Ok(T::default())` (Go's nil
  error). A type with neither (`Arc<dyn Trait>`, `Box<dyn ..>`, an enum without a default)
  compiles, and the call panics with a message that names the fake, the method and the
  calls that set a value.
- `x_returns(value)` takes `R` itself. A `Clone` value is returned on every call; so is an
  `Ok(v)` with `v: Clone`, whatever the error type. Anything else (a non-`Clone` `Err`, a
  `Box<dyn ..>`) is returned once, and a second call panics naming `x_returns_with`.
- `x_returns_with(|| ..)` makes a value per call. Error types need not be `Clone`.
- A return type that borrows is kept as `'static`: `fn name(&self) -> &str` takes
  `name_returns("a name")`.

Supertraits (Go's embedded interfaces):
- `trait LocalParticipant: Participant` needs `Participant` faked too. The fake of the
  subtrait implements both, and the inherited methods are configured directly on it
  (`p.identity_returns(..)`). The traits may be in different modules, any number of levels
  deep; upcasting is unaffected.
- Passed over: `Send`, `Sync`, `Sized`, `Unpin`, `Any`, `Debug`, lifetimes, and paths
  starting with `std`, `core` or `alloc`. Every other supertrait is taken to be faked;
  `#[cfg_attr(test, musivum_fakes::fake(extends(A, B)))]` names the ones to embed instead.
- With several faked supertraits, the first is configured directly; the others are reached
  with `fake.embedded::<FakeOtherState>().x_returns(..)`. Two faked supertraits that share
  a faked ancestor do not compile.

Not faked, each with a compile error that says so: generic traits and methods, `impl Trait`
in a signature, associated types, methods without `self` or with `Self` in a signature. A
method with a default body can keep it with `#[cfg_attr(test, fake(keep))]`. The fakes are
for traits of the `musivum` crate (the macro refers to `crate::testutils::fakes`). A trait
whose fakes a test has to get back from an `Arc<dyn Trait>` has `Any` as a supertrait.
Counterfeiter's `Invocations()` has no counterpart.

## The interfaces of `pkg/rtc` (`rtc/types/interfaces.rs`)

How a method of `types/interfaces.go` reads in Rust. The same rules hold for code that
implements or calls these traits; the header of the file has them at greater length.

- **Shared objects.** Anything behind an interface, a `*sfu.DownTrack`, or pion's sender,
  transceiver and local track is an `Arc<dyn Trait>` (`Arc<sfu::DownTrack>`), by value both
  ways; an `Option` of it where an implementation returns nil or a caller passes nil or
  checks for it. `add_subscriber` returns `Result<Option<Arc<..>>, Error>`: `Ok(None)` is
  the original's "nil, nil".
- **Protocol messages.** Taken as `&livekit::X` (whoever keeps it clones it; `Option<&..>`
  where a caller passes nil); returned owned, `Option` where nil is returned; returned as
  `Arc<..>` only where the original hands out the object it keeps and replaces whole
  (`get_client_info`, `claim_grants`, `subscription_permission`, the data blobs).
- **Small things.** Identifiers: `&livekit::TrackId` taken, by value returned. `string`:
  `&str` / `String`. `[]T`: `&[T]` / `Vec<T>`. Maps: `&HashMap` / `HashMap`. By value:
  numbers, enums, `MimeType`, `TimedVersion`, `Time`, `Duration`, and structs Go passes by
  value. `int` is `usize` for a count, `i32` for a layer.
- **Errors.** `Result<_, crate::rtc::Error>`; several results are a tuple in Go's order,
  inside the `Result` when the last is the error. Another package's error that is passed on
  is a transparent variant of `rtc::Error` (`Routing`, `Signalling`, `Sfu`, `Webrtc`, ...).
- **Callbacks.** `Box<dyn Fn(..) + Send + Sync>`, written out in the signature (the fake
  macro does not see through a type alias); an `Option` where nil is passed. A participant
  is handed to a callback as `Arc<dyn LocalParticipant>`.
- **Channels.** `<-chan struct{}` is a `tokio_util::sync::CancellationToken`.
- **`x.(*T)`.** The traits that are asserted upstream have `Any` as a supertrait:
  `Arc<dyn Trait>` coerces to `Arc<dyn Any + Send + Sync>`, then `downcast`; in tests
  `fakes::downcast::<FakeX>(..)`.
- **`x.(types.LocalMediaTrack)` and `p.(types.LocalParticipant)`** are
  `track.clone().as_local_media_track()` and `p.clone().as_local_participant()`, each an
  `Option<Arc<dyn ..>>`, from two one-method supertraits (`AsLocalMediaTrack`,
  `AsLocalParticipant`) that every implementor writes (`Some(self)` or `None`).
- **Fakes.** All seventeen traits have one. A method whose return type has no zero value
  (a plain `Arc<dyn ..>`) must be given a return before the code under test calls it.
  `rtc::testutils` has `new_mock_participant` and `new_mock_track`, as upstream.

## Running things

- Everything cargo runs here (test binaries, `cargo run`) goes through
  `scripts/capped-run.sh` (`.cargo/config.toml`): 8 GB of memory at most, and for a test
  binary 30 minutes at most. A loop that allocates without end, by mistake or by a
  mutation made on purpose, then fails instead of taking the machine's memory. It happened:
  a mutation check without a cap had a 45 GB test binary killed by the kernel on a machine
  that also runs other services.
- Anything run outside cargo that could run away (a Go harness, a script that replays
  generated inputs) gets the same by hand: `ulimit -d 8388608` in the shell that starts it
  (`-d`, not `-v`: a limit on address space keeps programs with many threads from starting
  them), and `timeout`.
- Scratch files go in the repository's `target/` or under `~/.cache/musivum`, not in `/tmp`.
- Test servers listen on the loopback interface, on ephemeral ports or ports from 39000 to
  39999 that are free; never LiveKit's default ports (7880, 7881, 5349, 3478, 50000 to
  60000), so that a LiveKit on the same machine is not disturbed.

## What not to do on the way

- Do not fix what looks wrong in the original. Port it as it is and say so in the file's
  header, where it explains the code.
- Do not improve: no different data structure because it is nicer in Rust, no merged or
  split functions, no reordering. The exception is what Rust forces (ownership, a lock that
  cannot be held across a call), and that goes in the header's "Changes".
- No `unsafe` (the workspace forbids it). No new dependency without saying so in the report.
- No `todo!()`, `unimplemented!()` or stubs left behind. If a function depends on something
  that is not ported yet, say so in the report instead of inventing it.

## Before it counts as done

`cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace`, and a line in `docs/port-status.md`.
