//! The log of the server, set up from the configuration's `logging` section: what LiveKit's
//! `config.InitLoggerFromConfig` does (protocol's `logger.InitFromConfig`, which builds a zap
//! logger), done with `tracing-subscriber` for the `tracing` events that musivum's logger
//! (`musivum::protocol::logger`) writes. Written new.
//!
//! What is kept of the original:
//! - `level`, read as zap reads it (`ParseZapLevel`): `debug`, `info`, `warn` (or `warning`),
//!   `error`, and `dpanic`, `panic` and `fatal` as `error`, in any case; anything else, and
//!   nothing, is `info`.
//! - `component_levels`, resolved as `ResolveComponentLevel` does: the level of the
//!   component, else of the component it is under (`sfu.downtrack`, then `sfu`), else
//!   `level`. musivum's logger names its component in the field `component` of an event.
//!   Events of other crates (the WebRTC library through `log`, the HTTP server) are taken
//!   for component `pion`: they are what pion is to LiveKit, and `pion_level` (which
//!   `Config::new` turns into the levels of `pion` and `transport.pion`) sets them.
//! - `json`: one JSON object per line; otherwise lines for a console, as zap's development
//!   encoder writes them. Both go to standard error.
//!
//! Left out: sampling (`sample` and the settings after it), which musivum's logger does not
//! do; the logger's name (`livekit`), which musivum's logger has per logger; updates of the
//! configuration while the server runs.

use std::collections::HashMap;
use std::fmt;
use std::io::IsTerminal;

use musivum::config::LoggingConfig;
use musivum::config::logger::Config;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::fmt::format::debug_fn;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

// The target of the events of musivum's logger.
const MUSIVUM_TARGET: &str = "musivum";
// The component the events of other crates are counted under.
const PION_COMPONENT: &str = "pion";

/// `config.InitLoggerFromConfig`: the log from the configuration's `logging` section.
pub fn init_logger_from_config(config: &LoggingConfig) {
    init_from_config(&config.config);
}

/// `logger.InitFromConfig`: sets up the process's log from `conf`. The first call does; a later one changes nothing
/// (the original replaces its logger).
pub fn init_from_config(conf: &Config) {
    let levels = ComponentLevels::new(conf);
    // A console line has the message (which comes first), then the other fields; an empty
    // `logger` or `component` is left out, and so are the fields `log` adds.
    let fields = debug_fn(|writer, field, value| {
        let text = format!("{value:?}");
        match field.name() {
            "message" => write!(writer, "{value:?}"),
            name if text == "\"\"" || name.starts_with("log.") => Ok(()),
            name => write!(writer, " {name}={text}"),
        }
    });
    let result = if conf.json {
        tracing_subscriber::registry()
            .with(levels)
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .flatten_event(true)
                    .with_writer(std::io::stderr),
            )
            .try_init()
    } else {
        tracing_subscriber::registry()
            .with(levels)
            .with(
                tracing_subscriber::fmt::layer()
                    .fmt_fields(fields)
                    .with_target(false)
                    .with_ansi(std::io::stderr().is_terminal())
                    .with_writer(std::io::stderr),
            )
            .try_init()
    };
    // Already set up: the first stays.
    let _ = result;
}

/// `ParseZapLevel`, with zap's `Level.UnmarshalText` (zapcore/level.go of zap v1.28.0, MIT):
/// in any case, `warning` for `warn`, and `info` for what it does not know.
pub fn parse_zap_level(level: &str) -> LevelFilter {
    match level.to_lowercase().as_str() {
        "debug" => LevelFilter::DEBUG,
        "warn" | "warning" => LevelFilter::WARN,
        "error" | "dpanic" | "panic" | "fatal" => LevelFilter::ERROR,
        _ => LevelFilter::INFO,
    }
}

/// The levels of the components, as a filter of events.
pub struct ComponentLevels {
    level: LevelFilter,
    components: HashMap<String, LevelFilter>,
    // The most verbose of all: what is above it is off whatever its component.
    max: LevelFilter,
}

impl ComponentLevels {
    pub fn new(conf: &Config) -> ComponentLevels {
        let level = parse_zap_level(&conf.level);
        let components: HashMap<String, LevelFilter> = conf
            .component_levels
            .iter()
            .map(|(component, level)| (component.clone(), parse_zap_level(level)))
            .collect();
        let max = components.values().copied().fold(level, LevelFilter::max);
        ComponentLevels {
            level,
            components,
            max,
        }
    }

    /// `ResolveComponentLevel`: the level of the component or of the nearest one it is under,
    /// else the level of all.
    pub fn resolve_component_level(&self, mut component: &str) -> LevelFilter {
        loop {
            if let Some(level) = self.components.get(component) {
                return *level;
            }
            match component.rfind('.') {
                Some(i) => component = &component[..i],
                None => return self.level,
            }
        }
    }

    fn event_level(&self, event: &Event<'_>) -> LevelFilter {
        if event.metadata().target() != MUSIVUM_TARGET {
            return self.resolve_component_level(PION_COMPONENT);
        }
        let mut visitor = ComponentVisitor(String::new());
        event.record(&mut visitor);
        self.resolve_component_level(&visitor.0)
    }
}

struct ComponentVisitor(String);

impl Visit for ComponentVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "component" {
            self.0 = value.to_string();
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

impl<S: Subscriber> Layer<S> for ComponentLevels {
    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        *metadata.level() <= self.max
    }

    fn event_enabled(&self, event: &Event<'_>, _ctx: Context<'_, S>) -> bool {
        *event.metadata().level() <= self.event_level(event)
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(self.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: the levels as zap reads them, and their resolution by component.
    #[test]
    fn levels_are_read_as_zap_reads_them() {
        assert_eq!(LevelFilter::DEBUG, parse_zap_level("debug"));
        assert_eq!(LevelFilter::WARN, parse_zap_level("WARN"));
        assert_eq!(LevelFilter::WARN, parse_zap_level("warning"));
        assert_eq!(LevelFilter::ERROR, parse_zap_level("fatal"));
        assert_eq!(LevelFilter::INFO, parse_zap_level(""));
        assert_eq!(LevelFilter::INFO, parse_zap_level("verbose"));
    }

    #[test]
    fn a_component_takes_the_level_of_the_nearest_one_above_it() {
        let conf = Config {
            level: "warn".to_string(),
            component_levels: HashMap::from([
                ("sfu".to_string(), "debug".to_string()),
                ("sfu.downtrack".to_string(), "error".to_string()),
                ("pion".to_string(), "error".to_string()),
            ]),
            ..Default::default()
        };
        let levels = ComponentLevels::new(&conf);
        assert_eq!(
            LevelFilter::ERROR,
            levels.resolve_component_level("sfu.downtrack")
        );
        assert_eq!(
            LevelFilter::ERROR,
            levels.resolve_component_level("sfu.downtrack.x")
        );
        assert_eq!(
            LevelFilter::DEBUG,
            levels.resolve_component_level("sfu.buffer")
        );
        assert_eq!(LevelFilter::WARN, levels.resolve_component_level("rtc"));
        assert_eq!(LevelFilter::WARN, levels.resolve_component_level(""));
        assert_eq!(LevelFilter::DEBUG, levels.max);
    }
}
