//! [`#[fake]`](macro@fake): fakes of traits for musivum's tests, which work like the ones
//! counterfeiter generates from LiveKit's interfaces. Everything is written down at the macro.

use proc_macro2::{Group, Ident, Span, TokenStream, TokenTree};
use quote::{ToTokens, format_ident, quote};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::visit_mut::VisitMut;
use syn::{
    Attribute, Error, FnArg, GenericArgument, GenericParam, ItemTrait, Lifetime, Meta, Pat,
    PatIdent, PatType, Path, PathArguments, ReturnType, Signature, Token, TraitBound,
    TraitBoundModifier, TraitItem, TraitItemFn, Type, TypeBareFn, TypeParamBound, TypePath,
    TypeReference, Visibility,
};

/// Makes a fake of the trait it is on: an implementation for tests that records every call
/// and returns what the test tells it to.
///
/// It is what [counterfeiter] generates from LiveKit's interfaces (`sfufakes.FakeTrackSender`,
/// `typesfakes.FakeLocalParticipant`, ...) and it works the same way, so that a test written
/// against those fakes is ported line by line: a call is recorded, a method nothing was said
/// about returns the zero value, and a test configures what it cares about.
///
/// [counterfeiter]: https://github.com/maxbrunsfeld/counterfeiter
///
/// # Use
///
/// On a trait of the library, for the test build only (`musivum-fakes` is a
/// dev-dependency):
///
/// ```text
/// #[cfg_attr(test, musivum_fakes::fake)]
/// pub trait TrackSender: Send + Sync {
///     fn id(&self) -> String;
///     fn write_rtp(&self, p: &ExtPacket, layer: i32) -> i32;
///     fn set_receiver(&self, r: Arc<dyn TrackReceiver>);
///     fn close(&self);
/// }
/// ```
///
/// In a module that is only compiled for tests it is just `#[musivum_fakes::fake]`. Next to
/// the trait, and as visible as the trait, there are then
///
/// - `FakeTrackSender`, which is `Default`, `Send + Sync` and `Debug` and implements
///   `TrackSender`;
/// - `FakeTrackSenderState`, what the fake keeps. A test does not name it, except for
///   [the second of several faked supertraits](#supertraits).
///
/// For every method `x` of the trait the fake has, in the order a test usually meets them
/// (`R` is what `x` returns):
///
/// | method of the fake | what it does | counterfeiter |
/// |---|---|---|
/// | `x_returns(&self, value: R)` | every later call returns `value`; takes a stub away again | `XReturns(v)` |
/// | `x_returns_with(&self, value: impl Fn() -> R + Send + Sync + 'static)` | every later call returns what the closure makes; for values that are not `Clone` | |
/// | `x_returns_on_call(&self, i: usize, value: R)` | call `i` (from 0) returns `value`; takes a stub away again | `XReturnsOnCall(i, v)` |
/// | `x_calls(&self, stub: impl Fn(args..) -> R + Send + Sync + 'static)` | every later call is answered by the stub | `XCalls(f)` |
/// | `x_stub(&self, stub: ..)` | the same | `fake.XStub = f` |
/// | `x_call_count(&self) -> usize` | how often `x` was called | `XCallCount()` |
/// | `x_args_for_call(&self, i: usize) -> ..` | the arguments of call `i`: the argument itself if `x` has one, a tuple if it has several | `XArgsForCall(i)` |
///
/// A method without parameters has no `x_args_for_call`, a method that returns nothing no
/// `x_returns`, `x_returns_with` and `x_returns_on_call`: as in counterfeiter. `r#type` gets
/// `type_returns`. All of them take `&self`: a test configures and questions the fake while
/// the code under test holds it as an `Arc<dyn Trait>`.
///
/// When a call comes in it is recorded first. Then, as in counterfeiter: the stub answers
/// if there is one; else the value for this call if one was set; else the value for all
/// calls; else the [zero value](#what-a-method-returns). The stub runs with no lock of the
/// fake held, so it may call back into the fake, into the method it is the stub of too.
///
/// ```standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// use std::sync::Arc;
///
/// #[derive(Clone, Debug, Default, PartialEq)]
/// pub struct ExtPacket {
///     pub sequence_number: u16,
/// }
///
/// pub trait TrackReceiver: Send + Sync {}
///
/// #[musivum_fakes::fake]
/// pub trait TrackSender: Send + Sync {
///     fn id(&self) -> String;
///     fn write_rtp(&self, p: &ExtPacket, layer: i32) -> i32;
///     fn set_receiver(&self, r: Arc<dyn TrackReceiver>);
///     fn close(&self);
/// }
///
/// fn main() {
///     let fake = Arc::new(FakeTrackSender::default());
///     // What the code under test holds.
///     let sender: Arc<dyn TrackSender> = fake.clone();
///
///     // Nothing was said: the zero values, and the calls are recorded.
///     assert_eq!(sender.id(), "");
///     assert_eq!(sender.write_rtp(&ExtPacket { sequence_number: 1 }, 2), 0);
///     sender.close();
///     assert_eq!(fake.close_call_count(), 1);
///     assert_eq!(fake.write_rtp_call_count(), 1);
///     let (packet, layer) = fake.write_rtp_args_for_call(0);
///     assert_eq!((packet.sequence_number, layer), (1, 2));
///
///     // The order in which a call is answered.
///     fake.id_returns("TR_1".to_owned());
///     fake.write_rtp_returns(1);
///     fake.write_rtp_returns_on_call(2, 7);
///     let packet = ExtPacket { sequence_number: 40 };
///     assert_eq!(sender.id(), "TR_1");
///     assert_eq!(sender.write_rtp(&packet, 0), 1); // call 1
///     assert_eq!(sender.write_rtp(&packet, 0), 7); // call 2
///     assert_eq!(sender.write_rtp(&packet, 0), 1);
///     fake.write_rtp_calls(|p, layer| i32::from(p.sequence_number) + layer);
///     assert_eq!(sender.write_rtp(&packet, 2), 42);
///     fake.write_rtp_returns(0); // takes the stub away again
///     assert_eq!(sender.write_rtp(&packet, 2), 0);
/// }
/// ```
///
/// # From a counterfeiter test line to Rust
///
/// | Go | Rust |
/// |---|---|
/// | `p := &typesfakes.FakeLocalParticipant{}` | `let p = Arc::new(FakeLocalParticipant::default());` |
/// | `p.IdentityReturns("alice")` | `p.identity_returns("alice".into());` |
/// | `p.GetAudioLevelReturns(20, true)` | `p.get_audio_level_returns((20.0, true));` (two results are a tuple) |
/// | `r.ReadRTPReturns(0, io.EOF)` | `r.read_rtp_returns(Err(Error::Eof));` if the error is `Clone` or one call is enough, else `r.read_rtp_returns_with(\|\| Err(Error::Eof));` (a result with an `error` is a `Result`) |
/// | `r.AddOnReadyCalls(func(f func()) { f() })` | `r.add_on_ready_calls(\|f\| f());` |
/// | `p.CloseStub = func(bool, types.ParticipantCloseReason, bool) error { close(closing); return nil }` | `p.close_stub(move \|_, _, _\| { closing.cancel(); Ok(()) });` |
/// | `require.Equal(t, 2, p.SubscribeToTrackCallCount())` | `assert_eq!(2, p.subscribe_to_track_call_count());` |
/// | `require.Equal(t, req.Key, publisher.GetDataBlobArgsForCall(0))` | `assert_eq!(req.key, publisher.get_data_blob_args_for_call(0));` |
/// | `_, got, _, _ := fp.SendDataMessageArgsForCall(0)` | `let (_, got, _, _) = fp.send_data_message_args_for_call(0);` |
/// | `p := participants[0].(*typesfakes.FakeLocalParticipant)` | `let p = fakes::downcast::<FakeLocalParticipant>(participants[0].clone());` (see [below](#the-fake-behind-a-trait-object)) |
///
/// # What is recorded of the arguments
///
/// The fake keeps a copy of every argument, which `x_args_for_call` gives a clone of. What
/// the copy is follows from how the parameter is written:
///
/// | parameter | recorded as |
/// |---|---|
/// | `T` by value | `T`, by `Clone`. An `Arc<dyn Trait>`, an `Option<Arc<..>>` and a closure passed as `Arc<dyn Fn(..) + Send + Sync>` are that: the same object, which the test can call |
/// | `&T` | `T`, by `Clone`: a message of the protocol, a `&ParticipantId`, a `&Arc<..>` |
/// | `&str` | `String` |
/// | `&[T]` | `Vec<T>` |
/// | `Option<&T>`, `Option<&str>`, `Option<&[T]>` | `Option<T>`, `Option<String>`, `Option<Vec<T>>` |
/// | `&mut ..`, `&dyn ..`, `Box<dyn ..>` (also in an `Option` or a `Pin`), and a type that still borrows after that (`&[&str]`, `Cow<'_, str>`) | not recorded |
/// | a parameter marked `#[fake(skip)]` | not recorded |
///
/// An argument that is not recorded leaves a `NotRecorded` (of `testutils::fakes`) at its
/// place in what `x_args_for_call` gives, so that the places of the others stay what they are
/// in Go. A stub gets every argument, as the method got it, recorded or not: a stub writes
/// into a `&mut [u8]` and calls a `Box<dyn FnOnce()>`.
///
/// What is recorded has to be `Clone + Send + 'static`. For a parameter whose type is not,
/// the compiler says "the fake cannot record an argument of type `..`" at the parameter, and
/// the parameter is then marked. In the library, where the macro is behind `cfg_attr`, the
/// mark is too: `#[cfg_attr(test, fake(skip))]`.
///
/// ```standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// use std::sync::Arc;
/// use testutils::fakes::NotRecorded;
///
/// /// Not `Clone`.
/// pub struct DownTrack;
///
/// #[musivum_fakes::fake]
/// pub trait Listener: Send + Sync {
///     fn on_data(&self, sender: &str, payload: &[u8], sequence: u32);
///     fn read(&self, buf: &mut [u8], layer: u8) -> usize;
///     fn add_on_ready(&self, f: Box<dyn FnOnce() + Send>);
///     fn add_on_close(&self, f: Arc<dyn Fn() -> u8 + Send + Sync>);
///     fn on_resume(&self, #[fake(skip)] dt: &DownTrack, layer: i32);
/// }
///
/// fn main() {
///     let fake = FakeListener::default();
///
///     fake.on_data("alice", &[1, 2], 7);
///     let (sender, payload, sequence): (String, Vec<u8>, u32) = fake.on_data_args_for_call(0);
///     assert_eq!((sender.as_str(), payload, sequence), ("alice", vec![1, 2], 7));
///
///     // A stub gets what is not recorded too.
///     fake.read_calls(|buf, layer| {
///         buf.fill(layer);
///         buf.len()
///     });
///     let mut buf = [0u8; 3];
///     assert_eq!(fake.read(&mut buf, 5), 3);
///     assert_eq!(buf, [5, 5, 5]);
///     assert_eq!(fake.read_args_for_call(0), (NotRecorded, 5));
///
///     fake.add_on_ready_calls(|f| f());
///     let (ready, is_ready) = std::sync::mpsc::channel();
///     fake.add_on_ready(Box::new(move || ready.send(()).unwrap()));
///     is_ready.try_recv().unwrap();
///
///     // A closure in an `Arc` is recorded, and can be called from the test.
///     fake.add_on_close(Arc::new(|| 3));
///     let on_close = fake.add_on_close_args_for_call(0);
///     assert_eq!(on_close(), 3);
///
///     fake.on_resume(&DownTrack, 1);
///     let (_, layer) = fake.on_resume_args_for_call(0);
///     assert_eq!(layer, 1);
/// }
/// ```
///
/// Without the mark:
///
/// ```compile_fail,standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// pub struct DownTrack;
///
/// #[musivum_fakes::fake]
/// pub trait Listener: Send + Sync {
///     // error: the fake cannot record an argument of type `DownTrack`
///     fn on_resume(&self, dt: &DownTrack, layer: i32);
/// }
///
/// fn main() {}
/// ```
///
/// # What a method returns
///
/// A method nothing was said about returns the zero value, as counterfeiter returns Go's:
///
/// - `Default::default()` of the return type: `0`, `false`, `""`, `None`, an empty `Vec`, the
///   default of a struct or an enum, a tuple of those;
/// - for a `Result<T, E>` the `Ok` of `T`'s default, as Go's zero value of `(T, error)` has
///   a nil error. `E` does not have to be anything;
/// - a return type that has neither (an `Arc<dyn Trait>`, a `Box<dyn ..>`, an enum without a
///   default) compiles all the same. Calling such a method without having said what it
///   returns panics: "FakeTrackReceiver::get_red_receiver was called, but the test set no
///   return value for it and `Arc<dyn TrackReceiver>` has no default: use
///   get_red_receiver_returns(..), get_red_receiver_returns_with(..) or
///   get_red_receiver_calls(..)". The call is counted before that.
///
/// `x_returns(value)` and `x_returns_on_call(i, value)` take the return type itself, so that
/// `None`, a number and an `Arc` of a fake for an `Arc<dyn Trait>` are written as they are.
/// What then happens with the value follows from its type:
///
/// - it is `Clone`: every call gets a clone;
/// - it is a `Result` whose `Ok` type is `Clone` and it is an `Ok`: the same. So
///   `x_returns(Ok(3))` does not need an error type that is `Clone`;
/// - anything else (an `Err` that is not `Clone`, a `Box<dyn ..>`): it is returned once, which
///   is what `x_returns_on_call` needs anyway. A second call panics and says to use
///   `x_returns_with(|| ..)`, which makes a value for every call.
///
/// A value has to be `Send` to be kept (`x_returns` of one that is not panics and says so);
/// `x_returns_with` and `x_calls` can make any value.
///
/// A return type that borrows is kept with `'static` for its lifetimes: for
/// `fn name(&self) -> &str` it is `name_returns("a name")`, for
/// `fn info(&self) -> Option<&TrackInfo>` a `Some` of a `&'static TrackInfo`
/// (`Box::leak(Box::new(..))` in a test).
///
/// ```standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// use std::sync::Arc;
///
/// /// Not `Clone`.
/// #[derive(Debug)]
/// pub enum Error {
///     Eof,
///     Io(std::io::Error),
/// }
///
/// #[musivum_fakes::fake]
/// pub trait TrackReceiver: Send + Sync {
///     fn stream_id(&self) -> String;
///     fn get_audio_level(&self) -> (f64, bool);
///     fn read_rtp(&self, buf: &mut [u8], layer: u8, esn: u64) -> Result<usize, Error>;
///     fn get_red_receiver(&self) -> Arc<dyn TrackReceiver>;
///     fn name(&self) -> &str;
/// }
///
/// fn main() {
///     let r = Arc::new(FakeTrackReceiver::default());
///     let mut buf = [0u8; 8];
///
///     // Zero values.
///     assert_eq!(r.get_audio_level(), (0.0, false));
///     assert_eq!(r.read_rtp(&mut buf, 0, 1).unwrap(), 0);
///     assert_eq!(r.name(), "");
///
///     r.get_audio_level_returns((20.0, true));
///     assert_eq!(r.get_audio_level(), (20.0, true));
///     r.name_returns("camera");
///     assert_eq!(r.name(), "camera");
///
///     // The receiver itself, as upstream's tests have it.
///     r.stream_id_returns("stream".to_owned());
///     r.get_red_receiver_returns(r.clone());
///     assert_eq!(r.get_red_receiver().get_red_receiver().stream_id(), "stream");
///
///     // An `Ok` again and again, an `Err` that is not `Clone` once, or made for every call.
///     r.read_rtp_returns(Ok(5));
///     assert_eq!(r.read_rtp(&mut buf, 0, 2).unwrap(), 5);
///     assert_eq!(r.read_rtp(&mut buf, 0, 3).unwrap(), 5);
///     r.read_rtp_returns(Err(Error::Io(std::io::Error::other("closed"))));
///     assert!(matches!(r.read_rtp(&mut buf, 0, 4), Err(Error::Io(_))));
///     r.read_rtp_returns_with(|| Err(Error::Eof));
///     assert!(matches!(r.read_rtp(&mut buf, 0, 5), Err(Error::Eof)));
///     assert!(matches!(r.read_rtp(&mut buf, 0, 6), Err(Error::Eof)));
///
///     // No zero value and nothing said.
///     let unset = FakeTrackReceiver::default();
///     let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
///         unset.get_red_receiver();
///     }));
///     assert!(panic.is_err());
///     assert_eq!(unset.get_red_receiver_call_count(), 1);
/// }
/// ```
///
/// # Supertraits
///
/// Go embeds interfaces, and counterfeiter's `FakeLocalParticipant` has the methods of
/// `Participant` too. Here `LocalParticipant: Participant` is faked when `Participant` is:
/// the fake of the subtrait embeds the fake of the supertrait, implements both traits, and
/// has the `x_returns` family of the inherited methods as well. The two traits can be in
/// different modules; the module of the subtrait needs nothing in scope but the supertrait.
/// It works through any number of levels, and upcasting (`Arc<dyn LocalParticipant>` to
/// `Arc<dyn Participant>`) is what it is for any trait and reaches the same fake.
///
/// Which supertraits are embedded:
///
/// - `Send`, `Sync`, `Sized`, `Unpin`, `UnwindSafe`, `RefUnwindSafe`, `Any`, `Debug`,
///   lifetimes, and every trait written with a path that starts at `std`, `core` or `alloc`
///   are passed over: a fake is all of the first eight by itself;
/// - every other supertrait is taken to be a faked trait. If it is not, the compiler says
///   "`dyn X` has no fake that the fake of a subtrait could embed";
/// - `#[fake(extends(Participant, Listener))]` (in the library
///   `#[cfg_attr(test, musivum_fakes::fake(extends(..)))]`) names the ones to embed instead,
///   written as they are after the colon. What is not named is passed over, and a supertrait
///   that is passed over and is not one of the eight is implemented for the fake by hand
///   (`impl fmt::Display for FakeSupervised { .. }`).
///
/// With several faked supertraits the first is the one whose methods are configured directly
/// on the fake, and the only one whose own faked supertraits are embedded too. The state of
/// another one is reached with `fake.embedded::<FakeListenerState>().on_event_returns(..)`.
/// Two supertraits that share a faked supertrait of their own are not supported (there would
/// be two of it), and a supertrait has to be dyn-compatible to be embedded.
///
/// ```standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// use std::sync::Arc;
///
/// mod participant {
///     #[derive(Clone, Copy, Debug, Default, PartialEq)]
///     pub enum State {
///         #[default]
///         Joining,
///         Active,
///     }
///
///     #[musivum_fakes::fake]
///     pub trait Participant: Send + Sync {
///         fn identity(&self) -> String;
///         fn state(&self) -> State;
///     }
/// }
///
/// mod local_participant {
///     // `State` is not known here, and does not have to be.
///     use super::participant::Participant;
///
///     #[musivum_fakes::fake]
///     pub trait LocalParticipant: Participant + Send + Sync {
///         fn is_ready(&self) -> bool;
///         fn subscribe_to_track(&self, track_id: &str);
///     }
/// }
///
/// use local_participant::{FakeLocalParticipant, LocalParticipant};
/// use participant::{Participant, State};
///
/// fn main() {
///     let p = Arc::new(FakeLocalParticipant::default());
///     p.identity_returns("alice".to_owned());
///     p.state_returns(State::Active);
///     p.is_ready_returns(true);
///
///     let local: Arc<dyn LocalParticipant> = p.clone();
///     let participant: Arc<dyn Participant> = local.clone();
///     assert!(local.is_ready());
///     assert_eq!(local.identity(), "alice");
///     assert_eq!(participant.state(), State::Active);
///     assert_eq!(p.identity_call_count(), 1);
///     assert_eq!(p.state_call_count(), 1);
/// }
/// ```
///
/// A supertrait that is not faked:
///
/// ```compile_fail,standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// pub trait Participant: Send + Sync {
///     fn identity(&self) -> String;
/// }
///
/// // error: `(dyn Participant + 'static)` has no fake that the fake of a subtrait could embed
/// #[musivum_fakes::fake]
/// pub trait LocalParticipant: Participant {
///     fn is_ready(&self) -> bool;
/// }
///
/// fn main() {}
/// ```
///
/// # The fake behind a trait object
///
/// Upstream's tests take a fake back out of an interface value:
/// `p := participants[0].(*typesfakes.FakeLocalParticipant)`. A trait that has `Any` among
/// its supertraits (`pub trait LocalParticipant: Participant + Any + Send + Sync`) allows the
/// same: `testutils::fakes::downcast::<FakeLocalParticipant>(participants[0].clone())` gives
/// the `Arc<FakeLocalParticipant>`, and panics if something else is behind the trait object.
/// Where a test made the fake itself, keeping the `Arc` of it is simpler.
///
/// ```standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// use std::any::Any;
/// use std::sync::Arc;
/// use testutils::fakes;
///
/// #[musivum_fakes::fake]
/// pub trait LocalParticipant: Any + Send + Sync {
///     fn is_ready(&self) -> bool;
/// }
///
/// fn main() {
///     let participants: Vec<Arc<dyn LocalParticipant>> =
///         vec![Arc::new(FakeLocalParticipant::default())];
///
///     let p = fakes::downcast::<FakeLocalParticipant>(participants[0].clone());
///     p.is_ready_returns(true);
///     assert!(participants[0].is_ready());
/// }
/// ```
///
/// # Other shapes of methods
///
/// - A method with a default body is faked like any other, so that its calls are counted.
///   Marked `#[fake(keep)]` (in the library `#[cfg_attr(test, fake(keep))]`) it is left out
///   of the fake, which then has the default body.
/// - Any receiver (`&self`, `&mut self`, `self`, `self: Arc<Self>`), lifetime parameters,
///   `where Self: Sized`, `async fn` (the stub is an ordinary closure) and `#[cfg(..)]` on a
///   method are supported.
/// - An associated constant with a default is left as it is.
///
/// Not supported, each with a compile error at its place that says so and what to do:
///
/// - a trait with generic parameters or a `where` clause, an `unsafe` trait;
/// - associated types, and associated constants without a default;
/// - a method without `self`; a method with type or const parameters or `impl Trait`
///   parameters; a return type `impl Trait` or `!`; `Self` in a parameter or in the return
///   type; bounds on lifetime parameters; more than 12 parameters. A method like that can
///   stay in a faked trait if it has a default body and is marked `#[fake(keep)]`;
/// - two methods whose names run into each other (`close` and `close_calls`).
///
/// ```standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// #[musivum_fakes::fake]
/// pub trait Limits: Send + Sync {
///     fn limit(&self) -> u32;
///
///     // Faked: the default body is not used.
///     fn double(&self) -> u32 {
///         self.limit() * 2
///     }
///
///     // Kept, and it could not be faked: it is generic.
///     #[fake(keep)]
///     fn within<T: Into<u32>>(&self, value: T) -> bool
///     where
///         Self: Sized,
///     {
///         value.into() <= self.limit()
///     }
/// }
///
/// fn main() {
///     let fake = FakeLimits::default();
///     fake.limit_returns(10);
///     assert_eq!(fake.double(), 0);
///     assert_eq!(fake.double_call_count(), 1);
///     assert!(fake.within(10u8));
///     assert!(!fake.within(11u8));
/// }
/// ```
///
/// A method that cannot be faked:
///
/// ```compile_fail,standalone_crate
/// # mod testutils { pub mod fakes { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../musivum/src/testutils/fakes.rs")); } }
/// #[musivum_fakes::fake]
/// pub trait Limits: Send + Sync {
///     // error: `#[fake]` cannot fake the generic method `within`: a stub and the recorded
///     // arguments need concrete types. ..
///     fn within<T: Into<u32>>(&self, value: T) -> bool;
/// }
///
/// fn main() {}
/// ```
///
/// # Where the pieces are
///
/// The generated code is small: per method a field, the implementation and one-line methods
/// that pass on to `testutils::fakes` of the `musivum` crate
/// (`crates/musivum/src/testutils/fakes.rs`), where the work is done. The macro refers to
/// that module as `crate::testutils::fakes`, so it makes fakes in the `musivum` crate only,
/// and its tests are there too (`testutils/fakes_test.rs`). The generated code needs no
/// `unsafe` and no `allow` at the place of use.
///
/// `crates/musivum-fakes/compile-time.py` measures what a fake of 150 methods costs to
/// compile: about 0.3 s to check and 0.6 to 0.9 s to build as a test binary, which is 2 ms
/// and 4 to 6 ms for a method.
#[proc_macro_attribute]
pub fn fake(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    expand(attr.into(), item.into()).into()
}

// ------------------------------------------------

/// The module the generated code is built from, `crates/musivum/src/testutils/fakes.rs`.
fn runtime() -> TokenStream {
    quote!(crate::testutils::fakes)
}

/// The same module under the name it is imported by where most of the generated code is, to
/// keep that code short.
fn runtime_inside() -> TokenStream {
    quote!(__rt)
}

/// The supertraits a fake does not have to embed anything for: markers, and what
/// `testutils::fakes::Fake` implements for every fake.
const PASSED_OVER: [&str; 8] = [
    "Send",
    "Sync",
    "Sized",
    "Unpin",
    "UnwindSafe",
    "RefUnwindSafe",
    "Any",
    "Debug",
];

/// Tuples are `Clone` up to this many elements, and the recorded arguments of a call are a
/// tuple.
const MAX_PARAMETERS: usize = 12;

const KEEP_HINT: &str = "give it a default body and mark it `#[cfg_attr(test, fake(keep))]` \
     (`#[fake(keep)]` where the macro is not behind `cfg_attr`): the fake then has the default \
     body";

fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item_trait: ItemTrait = match syn::parse2(item.clone()) {
        Ok(item_trait) => item_trait,
        Err(_) => {
            let err = Error::new(Span::call_site(), "`#[fake]` goes on a trait").to_compile_error();
            return quote! { #item #err };
        }
    };
    let mut errors = Errors::default();
    let options = match syn::parse2::<Options>(attr) {
        Ok(options) => options,
        Err(err) => {
            errors.push(err);
            Options::default()
        }
    };
    // Also takes the `fake(..)` attributes out of the trait, so do it before giving up.
    let model = Model::build(&mut item_trait, &options, &mut errors);
    match errors.all {
        Some(err) => {
            let err = err.to_compile_error();
            quote! { #item_trait #err }
        }
        None => {
            let generated = model.generate();
            quote! { #item_trait #generated }
        }
    }
}

#[derive(Default)]
struct Errors {
    all: Option<Error>,
    count: usize,
}

impl Errors {
    fn push(&mut self, err: Error) {
        self.count += 1;
        match &mut self.all {
            Some(errors) => errors.combine(err),
            None => self.all = Some(err),
        }
    }

    fn add(&mut self, at: &impl ToTokens, message: impl std::fmt::Display) {
        self.push(Error::new_spanned(at, message));
    }
}

// ------------------------------------------------
// What is written at the macro, on a method and on a parameter

#[derive(Default)]
struct Options {
    /// `extends(A, B)`: the supertraits whose fakes are embedded, instead of all that are
    /// not passed over.
    extends: Option<Vec<Path>>,
}

impl Parse for Options {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut options = Options::default();
        while !input.is_empty() {
            let word: Ident = input.call(Ident::parse_any)?;
            if word == "extends" {
                let content;
                syn::parenthesized!(content in input);
                let paths = Punctuated::<Path, Token![,]>::parse_terminated(&content)?;
                options.extends = Some(paths.into_iter().collect());
            } else {
                return Err(Error::new(
                    word.span(),
                    format!(
                        "unknown option `{word}`: `#[fake]` takes `extends(SuperTrait, ..)`, \
                         the supertraits whose fakes the fake embeds"
                    ),
                ));
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        Ok(options)
    }
}

/// Takes `#[fake(word, ..)]` and `#[cfg_attr(condition, fake(word, ..))]` out of the
/// attributes and gives the words. The condition is not looked at: the macro runs, so the
/// build is the one the attributes are for.
fn take_fake_words(attrs: &mut Vec<Attribute>, errors: &mut Errors) -> Vec<Ident> {
    fn is_fake(path: &Path) -> bool {
        path.segments.last().is_some_and(|s| s.ident == "fake")
    }
    fn words_of(meta: &Meta, errors: &mut Errors) -> Vec<Ident> {
        let parsed = match meta {
            Meta::List(list) => {
                list.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)
            }
            _ => Err(Error::new_spanned(
                meta,
                "expected `fake(skip)` or `fake(keep)`",
            )),
        };
        match parsed {
            Ok(words) => words.into_iter().collect(),
            Err(err) => {
                errors.push(err);
                Vec::new()
            }
        }
    }

    let mut words = Vec::new();
    attrs.retain(|attr| {
        if is_fake(attr.path()) {
            words.extend(words_of(&attr.meta, errors));
            return false;
        }
        if attr.path().is_ident("cfg_attr") {
            let Ok(metas) = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            else {
                return true;
            };
            let fakes: Vec<&Meta> = metas.iter().skip(1).filter(|m| is_fake(m.path())).collect();
            if fakes.is_empty() {
                return true;
            }
            if metas.len() != 2 {
                errors.add(
                    attr,
                    "`fake(..)` has to be in a `cfg_attr` of its own: `#[cfg_attr(test, fake(..))]`",
                );
            }
            for meta in fakes {
                words.extend(words_of(meta, errors));
            }
            return false;
        }
        true
    });
    words
}

// ------------------------------------------------
// Looking at types

fn peel(ty: &Type) -> &Type {
    match ty {
        Type::Paren(paren) => peel(&paren.elem),
        Type::Group(group) => peel(&group.elem),
        _ => ty,
    }
}

/// Whether the word occurs anywhere in the tokens (`Self`, `impl`).
fn mentions(tokens: TokenStream, word: &str) -> bool {
    tokens.into_iter().any(|tree| match tree {
        TokenTree::Ident(ident) => ident == word,
        TokenTree::Group(group) => mentions(group.stream(), word),
        _ => false,
    })
}

/// Whether a type borrows: has a reference or a lifetime other than `'static` in it. What is
/// inside `Fn(..)`, `fn(..)` and `for<'a> ..` does not count: the lifetimes there belong to
/// the calls of the closure.
fn borrows(ty: &Type) -> bool {
    struct Borrows(bool);
    impl<'ast> Visit<'ast> for Borrows {
        fn visit_type_reference(&mut self, _: &'ast TypeReference) {
            self.0 = true;
        }
        fn visit_lifetime(&mut self, lifetime: &'ast Lifetime) {
            if lifetime.ident != "static" {
                self.0 = true;
            }
        }
        fn visit_parenthesized_generic_arguments(
            &mut self,
            _: &'ast syn::ParenthesizedGenericArguments,
        ) {
        }
        fn visit_type_bare_fn(&mut self, _: &'ast TypeBareFn) {}
        fn visit_trait_bound(&mut self, bound: &'ast TraitBound) {
            if bound.lifetimes.is_none() {
                syn::visit::visit_trait_bound(self, bound);
            }
        }
    }
    let mut visitor = Borrows(false);
    visitor.visit_type(ty);
    visitor.0
}

/// The type with every lifetime made `'static`: what a fake can keep of a return type that
/// borrows from `self` or from an argument.
fn with_static_lifetimes(ty: &Type) -> Type {
    struct Staticify;
    impl VisitMut for Staticify {
        fn visit_type_reference_mut(&mut self, reference: &mut TypeReference) {
            reference.lifetime = Some(Lifetime::new("'static", reference.and_token.span));
            self.visit_type_mut(&mut reference.elem);
        }
        fn visit_lifetime_mut(&mut self, lifetime: &mut Lifetime) {
            *lifetime = Lifetime::new("'static", lifetime.span());
        }
        fn visit_parenthesized_generic_arguments_mut(
            &mut self,
            _: &mut syn::ParenthesizedGenericArguments,
        ) {
        }
        fn visit_type_bare_fn_mut(&mut self, _: &mut TypeBareFn) {}
        fn visit_trait_bound_mut(&mut self, bound: &mut TraitBound) {
            if bound.lifetimes.is_none() {
                syn::visit_mut::visit_trait_bound_mut(self, bound);
            }
        }
    }
    let mut ty = ty.clone();
    Staticify.visit_type_mut(&mut ty);
    ty
}

/// The first type argument of `Name<..>`, if the type is that.
fn type_argument<'a>(ty: &'a Type, names: &[&str]) -> Option<&'a Type> {
    let Type::Path(TypePath { qself: None, path }) = peel(ty) else {
        return None;
    };
    let last = path.segments.last()?;
    if !names.iter().any(|name| last.ident == name) {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &last.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

/// `Box<dyn ..>`, also in an `Option` or a `Pin`: never `Clone`.
fn is_boxed_dyn(ty: &Type) -> bool {
    if let Some(inner) = type_argument(ty, &["Box"]) {
        return matches!(peel(inner), Type::TraitObject(_));
    }
    match type_argument(ty, &["Option", "Pin"]) {
        Some(inner) => is_boxed_dyn(inner),
        None => false,
    }
}

/// The tokens with one span for all of them: an error of the compiler about generated code
/// then points at the place in the trait that it is about.
fn respan(tokens: TokenStream, span: Span) -> TokenStream {
    tokens
        .into_iter()
        .map(|tree| match tree {
            TokenTree::Group(group) => {
                let mut respanned = Group::new(group.delimiter(), respan(group.stream(), span));
                respanned.set_span(span);
                TokenTree::Group(respanned)
            }
            mut tree => {
                tree.set_span(span);
                tree
            }
        })
        .collect()
}

/// How an argument is recorded: the type that is kept and the expression that makes it from
/// the argument. `None` for an argument that is not recorded. `at` is where the parameter
/// is: "the fake cannot record an argument of type .." is said about the expression.
fn recording(
    ty: &Type,
    arg: &Ident,
    rt: &TokenStream,
    at: Span,
) -> Option<(TokenStream, TokenStream)> {
    // What is behind a `&`: the owned type and the function that copies into it.
    let owned = |elem: &Type| -> Option<(TokenStream, TokenStream)> {
        match peel(elem) {
            Type::Path(path) if path.qself.is_none() && path.path.is_ident("str") => {
                Some((quote!(::std::string::String), quote!(#rt::rec_str)))
            }
            Type::Slice(slice) if !borrows(&slice.elem) => {
                let elem = &slice.elem;
                Some((quote!(::std::vec::Vec<#elem>), quote!(#rt::rec_slice)))
            }
            Type::TraitObject(_) => None,
            elem if borrows(elem) => None,
            elem => Some((elem.to_token_stream(), quote!(#rt::rec))),
        }
    };

    let ty = peel(ty);
    let (recorded, expression) = if let Type::Reference(reference) = ty {
        if reference.mutability.is_some() {
            return None;
        }
        let (owned, rec) = owned(&reference.elem)?;
        (owned, quote!(#rec(#arg)))
    } else if let Some(Type::Reference(reference)) = type_argument(ty, &["Option"]).map(peel) {
        if reference.mutability.is_some() {
            return None;
        }
        let (owned, rec) = owned(&reference.elem)?;
        (
            quote!(::core::option::Option<#owned>),
            quote!(#arg.map(#rec)),
        )
    } else if is_boxed_dyn(ty) || borrows(ty) {
        return None;
    } else {
        (ty.to_token_stream(), quote!(#rt::rec(&#arg)))
    };
    Some((recorded, respan(expression, at)))
}

// ------------------------------------------------
// The trait as the macro sees it

struct Model {
    /// The `#[cfg(..)]` of the trait: everything made for it gets them too.
    cfgs: Vec<Attribute>,
    vis: Visibility,
    trait_ident: Ident,
    /// The supertraits as written: the bounds of the implementation for the fake.
    supertraits: Punctuated<TypeParamBound, Token![+]>,
    /// The supertraits whose fakes are embedded.
    embedded: Vec<Path>,
    methods: Vec<Method>,
    /// Whether `dyn Trait` can be written, which is where the fake of a subtrait finds this
    /// one.
    dyn_compatible: bool,
}

struct Method {
    /// The `#[cfg(..)]` of the method: everything made for it gets them too.
    cfgs: Vec<Attribute>,
    /// The signature for the implementation: the parameters are named `__a0`, `__a1`, ...
    sig: Signature,
    /// The name without `r#`, for the names made from it.
    name: String,
    args: Vec<Arg>,
    /// What is returned, with `'static` for every lifetime. `None` for a method that returns
    /// nothing: it gets no `x_returns`.
    ret: Option<Type>,
    /// `for<'a, 'b>` of a stub, if the method has lifetime parameters.
    hrtb: TokenStream,
}

struct Arg {
    ident: Ident,
    ty: Type,
    /// The recorded type and the expression that records, or `None`.
    recorded: Option<(TokenStream, TokenStream)>,
}

impl Model {
    fn build(item: &mut ItemTrait, options: &Options, errors: &mut Errors) -> Model {
        let rt = runtime_inside();
        if let Some(unsafety) = &item.unsafety {
            errors.add(unsafety, "`#[fake]` does not support an `unsafe` trait");
        }
        if let Some(auto) = &item.auto_token {
            errors.add(auto, "`#[fake]` does not support an `auto` trait");
        }
        if !item.generics.params.is_empty() {
            errors.add(
                &item.generics,
                "`#[fake]` does not support a trait with generic parameters: the choices a \
                 fake makes by type (what is recorded, the zero value) need concrete types",
            );
        }
        if let Some(where_clause) = &item.generics.where_clause {
            errors.add(
                where_clause,
                "`#[fake]` does not support a `where` clause on the trait: write the bounds \
                 as supertraits",
            );
        }

        let mut dyn_compatible = true;

        // The supertraits.
        let mut embedded = Vec::new();
        for bound in &item.supertraits {
            let TypeParamBound::Trait(bound) = bound else {
                continue;
            };
            if !matches!(bound.modifier, TraitBoundModifier::None) || bound.lifetimes.is_some() {
                continue;
            }
            let last = bound.path.segments.last().map(|s| s.ident.to_string());
            if last.as_deref() == Some("Sized") {
                dyn_compatible = false;
            }
            let embed = match &options.extends {
                Some(extends) => extends.iter().any(|path| same_path(path, &bound.path)),
                None => {
                    let first = bound.path.segments.first().map(|s| s.ident.to_string());
                    let from_std = matches!(first.as_deref(), Some("std" | "core" | "alloc"));
                    let marker = last.is_some_and(|last| PASSED_OVER.contains(&last.as_str()));
                    !from_std && !marker
                }
            };
            if embed {
                embedded.push(bound.path.clone());
            }
        }
        for path in options.extends.iter().flatten() {
            if !embedded.iter().any(|e| same_path(e, path)) {
                errors.add(
                    path,
                    format!(
                        "`{}` is not a supertrait of `{}`: `extends(..)` names supertraits, as \
                         they are written after the colon",
                        path.to_token_stream(),
                        item.ident
                    ),
                );
            }
        }

        // The items.
        let mut methods = Vec::new();
        let mut kept = Vec::new();
        for trait_item in &mut item.items {
            match trait_item {
                TraitItem::Fn(function) => {
                    let words = take_fake_words(&mut function.attrs, errors);
                    let mut keep = false;
                    for word in &words {
                        if word == "keep" {
                            keep = true;
                        } else {
                            errors.add(
                                word,
                                format!(
                                    "unknown `fake({word})` on a method: a method takes \
                                     `fake(keep)`, a parameter `fake(skip)`"
                                ),
                            );
                        }
                    }
                    // The parameters lose their `fake(..)` whether the method is faked or not.
                    let mut skipped = Vec::new();
                    for input in &mut function.sig.inputs {
                        let attrs = match input {
                            FnArg::Receiver(receiver) => &mut receiver.attrs,
                            FnArg::Typed(typed) => &mut typed.attrs,
                        };
                        let words = take_fake_words(attrs, errors);
                        let mut skip = false;
                        for word in &words {
                            if word == "skip" && matches!(input, FnArg::Typed(_)) {
                                skip = true;
                            } else {
                                errors.add(
                                    word,
                                    format!(
                                        "unknown `fake({word})` on a parameter: a parameter \
                                         other than `self` takes `fake(skip)`, a method \
                                         `fake(keep)`"
                                    ),
                                );
                            }
                        }
                        skipped.push(skip);
                    }
                    if keep {
                        if function.default.is_none() {
                            errors.add(
                                &function.sig.ident,
                                format!(
                                    "`fake(keep)` keeps the default body of a method, and `{}` \
                                     has none",
                                    function.sig.ident
                                ),
                            );
                        }
                        if !kept_is_dyn_compatible(&function.sig) {
                            dyn_compatible = false;
                        }
                        kept.push(function.sig.ident.unraw().to_string());
                        continue;
                    }
                    if function.sig.asyncness.is_some() {
                        dyn_compatible = false;
                    }
                    if let Some(method) = Method::build(function, &skipped, &rt, errors) {
                        methods.push(method);
                    }
                }
                TraitItem::Const(constant) => {
                    dyn_compatible = false;
                    if constant.default.is_none() {
                        errors.add(
                            &constant.ident,
                            format!(
                                "`#[fake]` does not support an associated constant without a \
                                 default: the fake could not choose a value for `{}`",
                                constant.ident
                            ),
                        );
                    }
                }
                TraitItem::Type(ty) => {
                    errors.add(
                        &ty.ident,
                        format!(
                            "`#[fake]` does not support associated types: the fake could not \
                             choose a type for `{}`",
                            ty.ident
                        ),
                    );
                }
                other => {
                    errors.add(
                        other,
                        "`#[fake]` cannot look into this item: only methods and associated \
                         constants with a default can be in a faked trait",
                    );
                }
            }
        }

        // The names the fake gets must not run into each other.
        let mut taken: Vec<String> = kept;
        taken.extend(methods.iter().map(|m| m.name.clone()));
        for method in &methods {
            for generated in method.generated_names() {
                if taken.contains(&generated) {
                    errors.add(
                        &method.sig.ident,
                        format!(
                            "the fake of `{}` gets a method `{generated}`, and the trait has a \
                             method of that name too: one of the two has to be renamed",
                            method.name
                        ),
                    );
                }
            }
        }

        Model {
            cfgs: cfgs_of(&item.attrs),
            vis: item.vis.clone(),
            trait_ident: item.ident.clone(),
            supertraits: item.supertraits.clone(),
            embedded,
            methods,
            dyn_compatible,
        }
    }
}

fn cfgs_of(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg"))
        .cloned()
        .collect()
}

fn same_path(a: &Path, b: &Path) -> bool {
    a.to_token_stream().to_string() == b.to_token_stream().to_string()
}

fn has_self_sized_bound(sig: &Signature) -> bool {
    sig.generics
        .where_clause
        .as_ref()
        .is_some_and(|where_clause| {
            where_clause.predicates.iter().any(|predicate| {
                let tokens = predicate.to_token_stream().to_string();
                tokens.starts_with("Self :") && mentions(predicate.to_token_stream(), "Sized")
            })
        })
}

/// Whether a method that keeps its default body leaves `dyn Trait` possible.
fn kept_is_dyn_compatible(sig: &Signature) -> bool {
    if has_self_sized_bound(sig) {
        return true;
    }
    let generic = sig
        .generics
        .params
        .iter()
        .any(|param| !matches!(param, GenericParam::Lifetime(_)));
    let in_signature = |word: &str| {
        sig.inputs.iter().any(|input| match input {
            FnArg::Typed(typed) => mentions(typed.ty.to_token_stream(), word),
            FnArg::Receiver(_) => false,
        }) || mentions(sig.output.to_token_stream(), word)
    };
    sig.receiver().is_some()
        && sig.asyncness.is_none()
        && !generic
        && !in_signature("impl")
        && !in_signature("Self")
}

impl Method {
    fn build(
        function: &TraitItemFn,
        skipped: &[bool],
        rt: &TokenStream,
        errors: &mut Errors,
    ) -> Option<Method> {
        let sig = &function.sig;
        let ident = &sig.ident;
        let before = errors.count;

        if sig.receiver().is_none() {
            errors.add(
                ident,
                format!(
                    "`#[fake]` cannot fake `{ident}`: it has no `self`, so there is no fake to \
                     record the call in; {KEEP_HINT}"
                ),
            );
        }

        // Generic over lifetimes only.
        let mut lifetimes = Vec::new();
        for param in &sig.generics.params {
            match param {
                GenericParam::Lifetime(lifetime) => {
                    if !lifetime.bounds.is_empty() {
                        errors.add(
                            lifetime,
                            format!(
                                "`#[fake]` cannot fake `{ident}`: a bound on a lifetime \
                                 parameter cannot be written for a stub; {KEEP_HINT}"
                            ),
                        );
                    }
                    lifetimes.push(lifetime.lifetime.clone());
                }
                other => errors.add(
                    other,
                    format!(
                        "`#[fake]` cannot fake the generic method `{ident}`: a stub and the \
                         recorded arguments need concrete types. Take a `Box<dyn ..>`, an \
                         `Arc<dyn ..>` or a `&dyn ..` instead, or {KEEP_HINT}"
                    ),
                ),
            }
        }
        let hrtb = if lifetimes.is_empty() {
            TokenStream::new()
        } else {
            quote!(for<#(#lifetimes),*>)
        };

        // The parameters.
        let mut impl_sig = sig.clone();
        let mut args = Vec::new();
        for (index, input) in impl_sig.inputs.iter_mut().enumerate() {
            let typed = match input {
                FnArg::Receiver(receiver) => {
                    receiver.attrs.clear();
                    continue;
                }
                FnArg::Typed(typed) => typed,
            };
            let ty = (*typed.ty).clone();
            if mentions(ty.to_token_stream(), "impl") {
                errors.add(
                    &ty,
                    format!(
                        "`#[fake]` cannot fake `{ident}`: `impl Trait` makes it a generic \
                         method, and a stub and the recorded arguments need concrete types. \
                         Take a `Box<dyn ..>`, an `Arc<dyn ..>` or a `&dyn ..` instead, or \
                         {KEEP_HINT}"
                    ),
                );
            } else if mentions(ty.to_token_stream(), "Self") {
                errors.add(
                    &ty,
                    format!(
                        "`#[fake]` cannot fake `{ident}`: `Self` in a parameter would be the \
                         fake itself; {KEEP_HINT}"
                    ),
                );
            }
            let arg = format_ident!("__a{}", args.len());
            let recorded = if skipped.get(index).copied().unwrap_or(false) {
                None
            } else {
                recording(&ty, &arg, rt, typed.pat.span())
            };
            *typed = PatType {
                attrs: Vec::new(),
                pat: Box::new(Pat::Ident(PatIdent {
                    attrs: Vec::new(),
                    by_ref: None,
                    mutability: None,
                    ident: arg.clone(),
                    subpat: None,
                })),
                colon_token: typed.colon_token,
                ty: typed.ty.clone(),
            };
            args.push(Arg {
                ident: arg,
                ty,
                recorded,
            });
        }
        if args.len() > MAX_PARAMETERS {
            errors.add(
                ident,
                format!(
                    "`#[fake]` cannot fake `{ident}`: it has more than {MAX_PARAMETERS} \
                     parameters; {KEEP_HINT}"
                ),
            );
        }

        // What is returned.
        let ret = match &sig.output {
            ReturnType::Default => None,
            ReturnType::Type(_, ty) => match peel(ty) {
                Type::Tuple(tuple) if tuple.elems.is_empty() => None,
                Type::Never(_) => {
                    errors.add(
                        ty,
                        format!(
                            "`#[fake]` cannot fake `{ident}`: a method that never returns has \
                             nothing a fake could return; {KEEP_HINT}"
                        ),
                    );
                    None
                }
                ty if mentions(ty.to_token_stream(), "impl") => {
                    errors.add(
                        ty,
                        format!(
                            "`#[fake]` cannot fake `{ident}`: the return type `impl Trait` has \
                             no name, so the fake cannot keep a value of it. Return a \
                             `Box<dyn ..>`, or {KEEP_HINT}"
                        ),
                    );
                    None
                }
                ty if mentions(ty.to_token_stream(), "Self") => {
                    errors.add(
                        ty,
                        format!(
                            "`#[fake]` cannot fake `{ident}`: `Self` in the return type would \
                             be the fake itself; {KEEP_HINT}"
                        ),
                    );
                    None
                }
                ty => Some(with_static_lifetimes(ty)),
            },
        };

        if errors.count != before {
            return None;
        }
        Some(Method {
            cfgs: cfgs_of(&function.attrs),
            sig: impl_sig,
            name: ident.unraw().to_string(),
            args,
            ret,
            hrtb,
        })
    }

    fn name_with(&self, suffix: &str) -> Ident {
        format_ident!("{}_{}", self.name, suffix, span = self.sig.ident.span())
    }

    /// The names of the methods the fake gets for this one.
    fn generated_names(&self) -> Vec<String> {
        let mut suffixes = vec!["call_count", "calls", "stub"];
        if !self.args.is_empty() {
            suffixes.push("args_for_call");
        }
        if self.ret.is_some() {
            suffixes.extend(["returns", "returns_with", "returns_on_call"]);
        }
        suffixes
            .into_iter()
            .map(|suffix| format!("{}_{suffix}", self.name))
            .collect()
    }
}

// ------------------------------------------------
// What is generated

impl Model {
    fn generate(&self) -> TokenStream {
        let rt = runtime();
        let inside = runtime_inside();
        let trait_cfgs = &self.cfgs;
        let vis = &self.vis;
        let trait_ident = &self.trait_ident;
        let fake_ident = format_ident!("Fake{}", trait_ident);
        let state_ident = format_ident!("Fake{}State", trait_ident);
        let fake_name = fake_ident.to_string();

        let super_fields: Vec<Ident> = (0..self.embedded.len())
            .map(|i| format_ident!("__super{}", i))
            .collect();
        let super_states: Vec<TokenStream> = self
            .embedded
            .iter()
            .map(|path| respan(quote!(<dyn #path as #rt::Faked>::State), path.span()))
            .collect();

        let mut fields = Vec::new();
        let mut trait_methods = Vec::new();
        let mut state_methods = Vec::new();
        for method in &self.methods {
            let cfgs = &method.cfgs;
            let ident = &method.sig.ident;
            let sig = &method.sig;
            let hrtb = &method.hrtb;
            let qualified = format!("{fake_name}::{}", method.name);

            let arg_idents: Vec<&Ident> = method.args.iter().map(|arg| &arg.ident).collect();
            let arg_types: Vec<&Type> = method.args.iter().map(|arg| &arg.ty).collect();
            let recorded_types: Vec<TokenStream> = method
                .args
                .iter()
                .map(|arg| match &arg.recorded {
                    Some((ty, _)) => ty.clone(),
                    None => quote!(#rt::NotRecorded),
                })
                .collect();
            let recordings: Vec<TokenStream> = method
                .args
                .iter()
                .map(|arg| match &arg.recorded {
                    Some((_, expr)) => expr.clone(),
                    None => quote!(#rt::NotRecorded),
                })
                .collect();
            // One argument is recorded as itself, several as a tuple.
            let (recorded_type, recording) = if method.args.len() == 1 {
                (recorded_types[0].clone(), recordings[0].clone())
            } else {
                (quote!((#(#recorded_types),*)), quote!((#(#recordings),*)))
            };

            let (ret, arrow) = match &method.ret {
                Some(ret) => (quote!(#ret), quote!(-> (#ret))),
                None => (quote!(()), TokenStream::new()),
            };
            fields.push(quote! {
                #(#cfgs)*
                #ident: #rt::Method<
                    #recorded_type,
                    dyn #hrtb ::core::ops::Fn(#(#arg_types),*) #arrow
                        + ::core::marker::Send + ::core::marker::Sync,
                    #ret,
                >,
            });

            // From here on inside the block that has the module as `__rt`.
            let rt = &inside;
            let stub_bounds = quote! {
                #hrtb #rt::Fn(#(#arg_types),*) #arrow + #rt::Send + #rt::Sync + 'static
            };
            let call = quote! {
                #rt::state::<#state_ident, __F>(&self).#ident.call(#recording)
            };
            let body = if method.ret.is_some() {
                quote! {
                    match #call {
                        #rt::Outcome::Stub(__stub) => __stub(#(#arg_idents),*),
                        #rt::Outcome::Value(__value) => __value,
                        #rt::Outcome::Unset => (&&&#rt::Zero::<#ret>::NEW).zero(#qualified),
                    }
                }
            } else {
                quote! {
                    if let #rt::Outcome::Stub(__stub) = #call {
                        __stub(#(#arg_idents),*)
                    }
                }
            };
            trait_methods.push(quote! {
                #(#cfgs)*
                #sig {
                    #body
                }
            });

            let call_count = method.name_with("call_count");
            let calls = method.name_with("calls");
            let stub = method.name_with("stub");
            state_methods.push(quote! {
                #(#cfgs)*
                #[inline]
                pub fn #call_count(&self) -> usize {
                    self.#ident.call_count()
                }
                #(#cfgs)*
                #[inline]
                pub fn #calls(&self, stub: impl #stub_bounds) {
                    self.#ident.calls(#rt::Arc::new(stub))
                }
                #(#cfgs)*
                #[inline]
                pub fn #stub(&self, stub: impl #stub_bounds) {
                    self.#ident.calls(#rt::Arc::new(stub))
                }
            });
            if !method.args.is_empty() {
                let args_for_call = method.name_with("args_for_call");
                state_methods.push(quote! {
                    #(#cfgs)*
                    #[inline]
                    #[track_caller]
                    pub fn #args_for_call(&self, i: usize) -> #recorded_type {
                        (&&self.#ident).args_for_call(i, #qualified)
                    }
                });
            }
            if method.ret.is_some() {
                let returns = method.name_with("returns");
                let returns_with = method.name_with("returns_with");
                let returns_on_call = method.name_with("returns_on_call");
                state_methods.push(quote! {
                    #(#cfgs)*
                    #[inline]
                    pub fn #returns(&self, value: #ret) {
                        self.#ident.returns((&&&&#rt::Given::new(value)).producer(#qualified))
                    }
                    #(#cfgs)*
                    #[inline]
                    pub fn #returns_with(
                        &self,
                        value: impl #rt::Fn() #arrow + #rt::Send + #rt::Sync + 'static,
                    ) {
                        self.#ident.returns(#rt::Arc::new(value))
                    }
                    #(#cfgs)*
                    #[inline]
                    pub fn #returns_on_call(&self, i: usize, value: #ret) {
                        self.#ident
                            .returns_on_call(i, (&&&&#rt::Given::new(value)).producer(#qualified))
                    }
                });
            }
        }

        // Where the fake of a subtrait finds this one.
        let faked = if self.dyn_compatible {
            quote! {
                impl #rt::Faked for dyn #trait_ident {
                    type State = #state_ident;
                }
            }
        } else {
            TokenStream::new()
        };

        // The states of the supertraits: everything the first one has is passed on, so that
        // a chain of supertraits is found whole; of the others their own state.
        //
        // All of it has the span of the supertrait, so that the compiler's "has no fake that
        // the fake of a subtrait could embed" points at the supertrait wherever it finds it.
        let mut embeddings = Vec::new();
        let mut defaults = Vec::new();
        for (index, path) in self.embedded.iter().enumerate() {
            let field = &super_fields[index];
            let state = &super_states[index];
            let embedding = if index == 0 {
                quote! {
                    impl<__S> #rt::Has<__S> for #state_ident
                    where
                        #state: #rt::Has<__S>,
                    {
                        fn state(&self) -> &__S {
                            #rt::Has::state(&self.#field)
                        }
                    }
                    impl ::core::ops::Deref for #state_ident {
                        type Target = #state;

                        fn deref(&self) -> &Self::Target {
                            &self.#field
                        }
                    }
                }
            } else {
                quote! {
                    impl #rt::Has<#state> for #state_ident {
                        fn state(&self) -> &#state {
                            &self.#field
                        }
                    }
                }
            };
            embeddings.push(respan(embedding, path.span()));
            defaults.push(respan(
                quote!(#field: ::core::default::Default::default(),),
                path.span(),
            ));
        }
        for method in &self.methods {
            let cfgs = &method.cfgs;
            let ident = &method.sig.ident;
            defaults.push(quote!(#(#cfgs)* #ident: #inside::Default::default(),));
        }

        let supertraits = &self.supertraits;
        let self_bounds = if supertraits.is_empty() {
            TokenStream::new()
        } else {
            quote!(Self: #supertraits,)
        };

        let fake_doc = format!(
            " The fake of [`{trait_ident}`] that `#[musivum_fakes::fake]` makes: every call is \
             recorded, and a test says with `x_returns(value)`, `x_returns_on_call(i, value)`, \
             `x_calls(stub)`, `x_call_count()` and `x_args_for_call(i)` of [`{state_ident}`] \
             what a method `x` does and asks what it got."
        );
        let state_doc = format!(
            " What [`{fake_ident}`] keeps for every method of [`{trait_ident}`], and the \
             methods a test configures the fake with."
        );

        quote! {
            #(#trait_cfgs)*
            #[doc = #fake_doc]
            #[allow(dead_code)]
            #vis type #fake_ident = #rt::Fake<#state_ident>;

            #(#trait_cfgs)*
            #[doc = #state_doc]
            #[allow(dead_code, unused_parens, non_snake_case, clippy::type_complexity)]
            #vis struct #state_ident {
                #(#super_fields: #super_states,)*
                #(#fields)*
            }

            #(#trait_cfgs)*
            #[allow(
                dead_code,
                unused_parens,
                unused_variables,
                non_snake_case,
                missing_docs,
                clippy::all,
                clippy::pedantic,
                clippy::nursery
            )]
            const _: () = {
                #[allow(unused_imports)]
                use #rt::{
                    self as #inside, ArgsByClone as _, ArgsNotClone as _, GivenClone as _,
                    GivenNotSend as _, GivenOnce as _, GivenResult as _, ZeroByDefault as _,
                    ZeroByOk as _, ZeroByPanic as _,
                };

                #faked

                impl #rt::Has<#state_ident> for #state_ident {
                    fn state(&self) -> &Self {
                        self
                    }
                }

                impl #inside::Default for #state_ident {
                    fn default() -> Self {
                        Self {
                            #(#defaults)*
                        }
                    }
                }

                #(#embeddings)*

                impl<__F> #trait_ident for #inside::Fake<__F>
                where
                    __F: #inside::Has<#state_ident>,
                    #self_bounds
                {
                    #(#trait_methods)*
                }

                impl #state_ident {
                    #(#state_methods)*
                }
            };
        }
    }
}

#[cfg(test)]
mod tests;
