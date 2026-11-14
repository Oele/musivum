//! LiveKit's `pkg/rtc/supervisor`: watches that what a participant started comes about in
//! time. A participant's supervisor keeps a monitor per publication, which is told what
//! happens to it (announced, muted, published, cleared) and reports a publish that did not
//! arrive within its wait; the supervisor checks its monitors on a timer and calls back for
//! each one that reports an error.

mod participant_supervisor;
mod publication_monitor;

pub use participant_supervisor::*;
pub use publication_monitor::*;
