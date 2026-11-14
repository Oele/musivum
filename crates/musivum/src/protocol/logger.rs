//! The logger the ported code writes to.
//!
//! It stands in for the Go package `github.com/livekit/protocol/logger` and is written new:
//! the same calls (a message with keys and values; values that stay with a logger), sent to
//! `tracing`. Code logs through the macros [`debugw!`](crate::debugw), [`infow!`](crate::infow),
//! [`warnw!`](crate::warnw) and [`errorw!`](crate::errorw), which do not format their
//! arguments unless the level is enabled:
//!
//! ```
//! use musivum::protocol::logger;
//! use musivum::{debugw, warnw};
//!
//! let logger = logger::get_logger().with_values(&[("trackID", &"TR_abc")]);
//! debugw!(logger, "forwarding", "layer" => 2, "bitrate" => 1_200_000);
//! warnw!(logger, "could not send", "the buffer is full", "size" => 1200);
//! warnw!(logger, "odd, but no error", nil);
//! ```

use std::fmt::{self, Write as _};
use std::sync::{Arc, LazyLock};

/// A logger with the values that go on every line it writes. Cheap to clone.
#[derive(Clone)]
pub struct Logger {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    name: String,
    component: String,
    // The values that stay with the logger, already formatted: ` key=value key=value`.
    values: String,
}

/// The four levels LiveKit's logger has.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

/// Go's `logger.GetLogger()`: the logger without values that everything starts from.
pub fn get_logger() -> Logger {
    static ROOT: LazyLock<Logger> = LazyLock::new(|| Logger {
        inner: Arc::new(Inner::default()),
    });
    ROOT.clone()
}

impl Default for Logger {
    fn default() -> Self {
        get_logger()
    }
}

impl fmt::Debug for Logger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Logger({}{})", self.inner.name, self.inner.values)
    }
}

fn write_values(out: &mut String, kvs: &[(&str, &dyn fmt::Debug)]) {
    for (k, v) in kvs {
        let _ = write!(out, " {k}={v:?}");
    }
}

impl Logger {
    /// A logger that adds these keys and values to every line.
    pub fn with_values(&self, kvs: &[(&str, &dyn fmt::Debug)]) -> Logger {
        let mut values = self.inner.values.clone();
        write_values(&mut values, kvs);
        Logger {
            inner: Arc::new(Inner {
                name: self.inner.name.clone(),
                component: self.inner.component.clone(),
                values,
            }),
        }
    }

    pub fn with_name(&self, name: &str) -> Logger {
        let name = if self.inner.name.is_empty() {
            name.to_string()
        } else {
            format!("{}.{}", self.inner.name, name)
        };
        Logger {
            inner: Arc::new(Inner {
                name,
                component: self.inner.component.clone(),
                values: self.inner.values.clone(),
            }),
        }
    }

    pub fn with_component(&self, component: &str) -> Logger {
        Logger {
            inner: Arc::new(Inner {
                name: self.inner.name.clone(),
                component: component.to_string(),
                values: self.inner.values.clone(),
            }),
        }
    }

    /// Upstream samples repeated lines of some loggers; here every line is written.
    pub fn with_item_sampler(&self) -> Logger {
        self.clone()
    }

    pub fn without_sampler(&self) -> Logger {
        self.clone()
    }

    pub fn enabled(&self, level: Level) -> bool {
        match level {
            Level::Debug => tracing::enabled!(target: "musivum", tracing::Level::DEBUG),
            Level::Info => tracing::enabled!(target: "musivum", tracing::Level::INFO),
            Level::Warn => tracing::enabled!(target: "musivum", tracing::Level::WARN),
            Level::Error => tracing::enabled!(target: "musivum", tracing::Level::ERROR),
        }
    }

    /// Writes one line. The macros call this after checking the level.
    pub fn log(
        &self,
        level: Level,
        msg: &str,
        err: Option<&dyn fmt::Display>,
        kvs: &[(&str, &dyn fmt::Debug)],
    ) {
        let mut line = String::with_capacity(msg.len() + self.inner.values.len() + 16 * kvs.len());
        line.push_str(msg);
        if let Some(err) = err {
            let _ = write!(line, " error={err}");
        }
        line.push_str(&self.inner.values);
        write_values(&mut line, kvs);
        let (name, component) = (self.inner.name.as_str(), self.inner.component.as_str());
        match level {
            Level::Debug => {
                tracing::debug!(target: "musivum", logger = name, component, "{line}")
            }
            Level::Info => tracing::info!(target: "musivum", logger = name, component, "{line}"),
            Level::Warn => tracing::warn!(target: "musivum", logger = name, component, "{line}"),
            Level::Error => {
                tracing::error!(target: "musivum", logger = name, component, "{line}")
            }
        }
    }

    pub fn debugw(&self, msg: &str, kvs: &[(&str, &dyn fmt::Debug)]) {
        if self.enabled(Level::Debug) {
            self.log(Level::Debug, msg, None, kvs);
        }
    }

    pub fn infow(&self, msg: &str, kvs: &[(&str, &dyn fmt::Debug)]) {
        if self.enabled(Level::Info) {
            self.log(Level::Info, msg, None, kvs);
        }
    }

    pub fn warnw(
        &self,
        msg: &str,
        err: Option<&dyn fmt::Display>,
        kvs: &[(&str, &dyn fmt::Debug)],
    ) {
        if self.enabled(Level::Warn) {
            self.log(Level::Warn, msg, err, kvs);
        }
    }

    pub fn errorw(
        &self,
        msg: &str,
        err: Option<&dyn fmt::Display>,
        kvs: &[(&str, &dyn fmt::Debug)],
    ) {
        if self.enabled(Level::Error) {
            self.log(Level::Error, msg, err, kvs);
        }
    }
}

/// Go's `logger.Debugw(msg, "key", value, ...)`, as `debugw!(logger, msg, "key" => value, ...)`.
#[macro_export]
macro_rules! debugw {
    ($logger:expr, $msg:expr $(, $k:expr => $v:expr)* $(,)?) => {{
        let logger = &$logger;
        if logger.enabled($crate::protocol::logger::Level::Debug) {
            logger.log(
                $crate::protocol::logger::Level::Debug,
                $msg,
                None,
                &[$(($k, &$v as &dyn ::std::fmt::Debug)),*],
            );
        }
    }};
}

/// Go's `logger.Infow(msg, "key", value, ...)`.
#[macro_export]
macro_rules! infow {
    ($logger:expr, $msg:expr $(, $k:expr => $v:expr)* $(,)?) => {{
        let logger = &$logger;
        if logger.enabled($crate::protocol::logger::Level::Info) {
            logger.log(
                $crate::protocol::logger::Level::Info,
                $msg,
                None,
                &[$(($k, &$v as &dyn ::std::fmt::Debug)),*],
            );
        }
    }};
}

/// Go's `logger.Warnw(msg, err, "key", value, ...)`. The error is anything that can be
/// displayed, or `nil`.
#[macro_export]
macro_rules! warnw {
    ($logger:expr, $msg:expr, nil $(, $k:expr => $v:expr)* $(,)?) => {{
        let logger = &$logger;
        if logger.enabled($crate::protocol::logger::Level::Warn) {
            logger.log(
                $crate::protocol::logger::Level::Warn,
                $msg,
                None,
                &[$(($k, &$v as &dyn ::std::fmt::Debug)),*],
            );
        }
    }};
    ($logger:expr, $msg:expr, $err:expr $(, $k:expr => $v:expr)* $(,)?) => {{
        let logger = &$logger;
        if logger.enabled($crate::protocol::logger::Level::Warn) {
            logger.log(
                $crate::protocol::logger::Level::Warn,
                $msg,
                Some(&$err as &dyn ::std::fmt::Display),
                &[$(($k, &$v as &dyn ::std::fmt::Debug)),*],
            );
        }
    }};
}

/// Go's `logger.Errorw(msg, err, "key", value, ...)`. The error is anything that can be
/// displayed, or `nil`.
#[macro_export]
macro_rules! errorw {
    ($logger:expr, $msg:expr, nil $(, $k:expr => $v:expr)* $(,)?) => {{
        let logger = &$logger;
        if logger.enabled($crate::protocol::logger::Level::Error) {
            logger.log(
                $crate::protocol::logger::Level::Error,
                $msg,
                None,
                &[$(($k, &$v as &dyn ::std::fmt::Debug)),*],
            );
        }
    }};
    ($logger:expr, $msg:expr, $err:expr $(, $k:expr => $v:expr)* $(,)?) => {{
        let logger = &$logger;
        if logger.enabled($crate::protocol::logger::Level::Error) {
            logger.log(
                $crate::protocol::logger::Level::Error,
                $msg,
                Some(&$err as &dyn ::std::fmt::Display),
                &[$(($k, &$v as &dyn ::std::fmt::Debug)),*],
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::capture_log as capture;

    #[test]
    fn a_line_has_the_message_the_error_and_all_values() {
        let out = capture(tracing::Level::DEBUG, || {
            let logger = get_logger()
                .with_name("sfu")
                .with_values(&[("trackID", &"TR_abc"), ("layer", &2)]);
            crate::debugw!(logger, "forwarding", "bitrate" => 1200, "mime" => "video/vp8");
            crate::warnw!(logger, "could not send", "buffer full", "size" => 3);
            crate::errorw!(logger, "no error here", nil);
            crate::infow!(logger, "plain");
        });
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 4, "{out}");
        assert!(lines[0].starts_with("DEBUG"), "{out}");
        assert!(
            lines[0]
                .contains("forwarding trackID=\"TR_abc\" layer=2 bitrate=1200 mime=\"video/vp8\""),
            "{out}"
        );
        assert!(lines[0].contains("logger=\"sfu\""), "{out}");
        assert!(lines[1].starts_with(" WARN"), "{out}");
        assert!(
            lines[1].contains("could not send error=buffer full trackID="),
            "{out}"
        );
        assert!(lines[1].contains(" layer=2 size=3 "), "{out}");
        assert!(
            lines[2].starts_with("ERROR") && !lines[2].contains("error="),
            "{out}"
        );
        assert!(lines[3].starts_with(" INFO"), "{out}");
    }

    #[test]
    fn arguments_are_not_formatted_below_the_level() {
        struct Loud;
        impl fmt::Debug for Loud {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("formatted although debug is off");
            }
        }
        let out = capture(tracing::Level::INFO, || {
            let logger = get_logger();
            crate::debugw!(logger, "quiet", "value" => Loud);
            logger.debugw("quiet", &[("value", &Loud)]);
            crate::infow!(logger, "heard");
        });
        assert_eq!(out.lines().count(), 1, "{out}");
    }
}
