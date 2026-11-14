// Ported from LiveKit, cmd/server/main.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, for one node.
// - The command line is clap's where the original's is urfave/cli's: `Cli` with `Flags` (the
//   original's `baseFlags`, global as urfave's are) and `Commands`. A flag's environment
//   variable is read when the flag is not given (`Flags::with_env`), as urfave reads its
//   `Sources`; set and empty counts as set, as there. clap's errors are clap's texts; a wrong
//   command line exits with 1, as the original's does, and help and the version with 0.
// - Left out: the flags of Redis (`--redis-host`, `--redis-password`) and of Go's profiler
//   (`--cpuprofile`, `--memprofile`), the flags `config.GenerateCLIFlags` makes for every key
//   of the configuration (not ported in `musivum::config`), Jaeger, `prometheus.Init` (the
//   counters of `musivum::telemetry::prometheus` need none), `rand.Seed`, and `rtc.Recover`
//   around `main` (a panic ends the program with Rust's message). Of the subcommands,
//   `list-nodes` (the nodes of a cluster) and `help-verbose` (the generated flags) have no
//   counterpart; `generate-keys`, `ports` and the hidden `create-join-token` are in
//   commands.rs.
// - The text of `--dev` says what development mode does here: there are no Go profiling
//   handlers.
// - `getConfig` takes the function that sets up the log (`config.InitLoggerFromConfig` in the
//   original), so that tests can leave the process's log alone. It blocks (the node's
//   address may be looked up): `startServer` calls it before the runtime is started.
// - `startServer` starts a tokio runtime for the server after the configuration, the node
//   and the server are made; the signals are tokio's.
// - `getConfigString` names the file in its error, as Go's `os.ReadFile` does.

mod commands;

use std::collections::HashMap;
use std::io;
use std::net::IpAddr;

use clap::{Args, Parser, Subcommand};
use musivum::config::{BaseFlags, Config, LoggingConfig};
use musivum::infow;
use musivum::protocol::logger;
use musivum::routing::LocalNodeImpl;
use musivum_server::service::initialize_server;
use tokio::signal::unix::{SignalKind, signal};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(
    name = "musivum-server",
    about = "High performance WebRTC server",
    long_about = "High performance WebRTC server: musivum, LiveKit's server for one node.\n\nrun without subcommands to start the server",
    version
)]
struct Cli {
    #[command(flatten)]
    flags: Flags,
    #[command(subcommand)]
    command: Option<Commands>,
}

/// The original's `baseFlags`.
#[derive(Args, Clone, Debug, Default, PartialEq)]
struct Flags {
    /// IP address to listen on, use flag multiple times to specify multiple addresses
    #[arg(long, global = true, value_delimiter = ',', value_name = "string")]
    bind: Option<Vec<String>>,
    /// path to LiveKit config file
    #[arg(long, global = true, value_name = "string")]
    config: Option<String>,
    /// LiveKit config in YAML, typically passed in as an environment var in a container [$LIVEKIT_CONFIG]
    #[arg(long, global = true, value_name = "string")]
    config_body: Option<String>,
    /// path to file that contains API keys/secrets
    #[arg(long, global = true, value_name = "string")]
    key_file: Option<String>,
    /// api keys (key: secret\n) [$LIVEKIT_KEYS]
    #[arg(long, global = true, value_name = "string")]
    keys: Option<String>,
    /// region of the current node. Used by regionaware node selector [$LIVEKIT_REGION]
    #[arg(long, global = true, value_name = "string")]
    region: Option<String>,
    /// IP address of the current node, used to advertise to clients. Automatically determined by default [$NODE_IP]
    #[arg(long, global = true, value_name = "string")]
    node_ip: Option<String>,
    /// UDP port(s) to use for WebRTC traffic [$UDP_PORT]
    #[arg(long, global = true, value_name = "string")]
    udp_port: Option<String>,
    /// tls cert file for TURN server [$LIVEKIT_TURN_CERT]
    #[arg(long, global = true, value_name = "string")]
    turn_cert: Option<String>,
    /// tls key file for TURN server [$LIVEKIT_TURN_KEY]
    #[arg(long, global = true, value_name = "string")]
    turn_key: Option<String>,
    /// sets log-level to debug, and placeholder keys bound to loopback when none are given. insecure for production
    #[arg(
        long,
        global = true,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        value_name = "bool"
    )]
    dev: Option<bool>,
    /// disables strict config parsing
    #[arg(long, global = true, hide = true)]
    disable_strict_config: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// generates an API key and secret pair
    GenerateKeys,
    /// print ports that server is configured to use
    Ports,
    // this subcommand is deprecated, token generation is provided by CLI
    /// create a room join token for development use
    #[command(hide = true)]
    CreateJoinToken {
        /// name of room to join
        #[arg(long, value_name = "string")]
        room: String,
        /// identity of participant that holds the token
        #[arg(long, value_name = "string")]
        identity: String,
        /// creates a hidden participant that can only subscribe
        #[arg(long)]
        recorder: bool,
    },
}

impl Flags {
    /// The flags with what the environment has for those that urfave reads from it, where
    /// they are not given.
    fn with_env(mut self, env: impl Fn(&str) -> Option<String>) -> Flags {
        let from_env = |flag: &mut Option<String>, name: &str| {
            if flag.is_none() {
                *flag = env(name);
            }
        };
        from_env(&mut self.config_body, "LIVEKIT_CONFIG");
        from_env(&mut self.keys, "LIVEKIT_KEYS");
        from_env(&mut self.region, "LIVEKIT_REGION");
        from_env(&mut self.node_ip, "NODE_IP");
        from_env(&mut self.udp_port, "UDP_PORT");
        from_env(&mut self.turn_cert, "LIVEKIT_TURN_CERT");
        from_env(&mut self.turn_key, "LIVEKIT_TURN_KEY");
        self
    }

    // What `config.NewConfig` reads of the command line: the flags given.
    fn base_flags(&self) -> BaseFlags {
        BaseFlags {
            dev: self.dev,
            key_file: self.key_file.clone(),
            keys: self.keys.clone(),
            region: self.region.clone(),
            turn_cert: self.turn_cert.clone(),
            turn_key: self.turn_key.clone(),
            node_ip: self.node_ip.clone(),
            udp_port: self.udp_port.clone(),
            bind: self.bind.clone(),
        }
    }
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            // help and the version go to standard output and are not errors
            let code = if err.use_stderr() { 1 } else { 0 };
            let _ = err.print();
            std::process::exit(code);
        }
    };
    let flags = cli.flags.with_env(|name| std::env::var(name).ok());

    let mut out = io::stdout();
    let res = match cli.command {
        None => start_server(&flags),
        Some(Commands::GenerateKeys) => commands::generate_keys(&mut out),
        Some(Commands::Ports) => commands::print_ports(&flags, &mut out),
        Some(Commands::CreateJoinToken {
            room,
            identity,
            recorder,
        }) => commands::create_token(&flags, &room, &identity, recorder, &mut out),
    };
    if let Err(err) = res {
        println!("{err}");
        std::process::exit(1);
    }
}

fn get_config(c: &Flags, init_logger: impl FnOnce(&LoggingConfig)) -> Result<Config, Error> {
    let conf_string = get_config_string(
        c.config.as_deref().unwrap_or(""),
        c.config_body.as_deref().unwrap_or(""),
    )?;

    let strict_mode = !c.disable_strict_config;

    let mut conf = Config::new_with_flags(&conf_string, strict_mode, Some(&c.base_flags()))?;
    init_logger(&conf.logging);

    if conf.development {
        infow!(logger::get_logger(), "starting in development mode");

        if conf.keys.is_empty() {
            infow!(
                logger::get_logger(),
                "no keys provided, using placeholder keys",
                "API Key" => "devkey",
                "API Secret" => "secret",
            );
            conf.keys = HashMap::from([("devkey".to_string(), "secret".to_string())]);
            let mut should_match_rtc_ip = false;
            // when dev mode and using shared keys, we'll bind to localhost by default
            if conf.bind_addresses.is_empty() {
                conf.bind_addresses = vec!["127.0.0.1".to_string(), "::1".to_string()];
            } else {
                // if non-loopback addresses are provided, then we'll match RTC IP to bind address
                // our IP discovery ignores loopback addresses
                for addr in &conf.bind_addresses {
                    if let Ok(ip) = addr.parse::<IpAddr>()
                        && !ip.to_canonical().is_loopback()
                        && !ip.is_unspecified()
                    {
                        should_match_rtc_ip = true;
                    }
                }
            }
            if should_match_rtc_ip {
                for bind_addr in conf.bind_addresses.clone() {
                    conf.rtc
                        .rtc_config
                        .ips
                        .includes
                        .push(format!("{bind_addr}/24"));
                }
            }
        }
    }
    Ok(conf)
}

fn start_server(c: &Flags) -> Result<(), Error> {
    let mut conf = get_config(c, musivum_server::logger::init_logger_from_config)?;

    // validate API key length
    conf.validate_keys()?;

    conf.load_turn_secrets()?;

    let current_node = LocalNodeImpl::new(Some(&conf))?;

    let server = initialize_server(conf, current_node)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        let mut quit = signal(SignalKind::quit())?;
        let s = server.clone();
        tokio::spawn(async move {
            for i in 0..2 {
                let sig = tokio::select! {
                    _ = interrupt.recv() => "interrupt",
                    _ = terminate.recv() => "terminated",
                    _ = quit.recv() => "quit",
                };
                let force = i > 0;
                infow!(
                    logger::get_logger(),
                    "exit requested, shutting down",
                    "signal" => sig,
                    "force" => force,
                );
                let s = s.clone();
                tokio::spawn(async move { s.stop(force).await });
            }
        });

        server.start().await?;
        Ok::<(), Error>(())
    })
}

fn get_config_string(config_file: &str, in_config_body: &str) -> io::Result<String> {
    if !in_config_body.is_empty() || config_file.is_empty() {
        return Ok(in_config_body.to_string());
    }

    std::fs::read_to_string(config_file)
        .map_err(|err| io::Error::new(err.kind(), format!("open {config_file}: {err}")))
}

#[cfg(test)]
#[path = "main_test.rs"]
mod main_test;
