//! LiveKit's `pkg/rtc/dynacast`: telling a publisher which simulcast layers and codecs anybody
//! is subscribed to, so that it can stop encoding the ones nobody watches. Per codec a manager
//! keeps the highest quality any subscriber wants, waits before lowering it, and notifies its
//! listener when that changes.

mod dynacastmanageraudio;
mod dynacastmanagerbase;
mod dynacastmanagervideo;
mod dynacastqualityaudio;
mod dynacastqualityvideo;
mod interfaces;

pub use dynacastmanageraudio::*;
pub use dynacastmanagervideo::*;
pub use interfaces::*;

// The package exports nothing of these three files: their names are for the files here.
use dynacastmanagerbase::*;
use dynacastqualityaudio::*;
use dynacastqualityvideo::*;

#[cfg(test)]
#[path = "dynacastmanager_test.rs"]
mod dynacastmanager_test;
