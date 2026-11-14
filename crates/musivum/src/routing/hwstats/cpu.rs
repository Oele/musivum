// Ported from LiveKit's protocol, utils/hwstats/cpu.go, cpu_all.go and cpu_null.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The monitor of processes is not ported (see the
// module's description): `procCallback`, `ProcMemoryEntry`, `GroupMemory`, `ProcStats`,
// `procEntry`, `aggregateMemoryStats`, `NewProcMonitor` and `monitorProcesses`. The platform's
// monitor, which changes its last sample when it is asked for the idle CPUs, is behind a
// lock. `NewCPUStats` returns an `Arc`; its worker is a task that holds it until `Stop`. The
// `osStatCPUMonitor` of `cpu_all.go` (Linux without cgroup files) and the
// `nullStatCPUMonitor` of `cpu_null.go` (every system but Linux here) are at the end.
// `core.Throttle` of frostbyte73/core (MIT) is `Throttle` below, ported with it.
// `strconv.ParseFloat` is Rust's, which takes neither hexadecimal nor underscores.

use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::{Error, num_cpu};
use crate::go::atomic;
use crate::go::time::{self, Duration};
use crate::protocol::logger;
use crate::{errorw, infow};

// This object returns cgroup quota aware cpu stats. On other systems than Linux,
// it falls back to full system stats

pub const CPU_REQUEST_ENV_VAR: &str = "LIVEKIT_CPU_REQUEST";

static EFFECTIVE_CPU_REQUEST: OnceLock<f64> = OnceLock::new();

// EffectiveCPURequest returns the value of LIVEKIT_CPU_REQUEST when set and valid.
// The result is parsed at most once; use for stats normalization and optional
// GOMAXPROCS tuning (see utils/hwstats/maxprocs).
pub fn effective_cpu_request() -> f64 {
    *EFFECTIVE_CPU_REQUEST.get_or_init(parse_cpu_request_env)
}

fn parse_cpu_request_env() -> f64 {
    let cpu_req = std::env::var(CPU_REQUEST_ENV_VAR).unwrap_or_default();
    if cpu_req.is_empty() {
        return 0.0;
    }
    match cpu_req.parse::<f64>() {
        // `n <= 0` is false for NaN, which is taken, as in the original
        Ok(n) if n.is_nan() || n > 0.0 => n,
        _ => 0.0,
    }
}

pub(super) trait PlatformCpuMonitor: Send {
    fn get_cpu_idle(&mut self) -> Result<f64, Error>;
    fn num_cpu(&self) -> f64;
}

pub struct CpuStats {
    idle_cpus: atomic::Float64,
    // set by the first sample, the load reads as 0 until then
    sampled: atomic::Bool,
    platform: Mutex<Box<dyn PlatformCpuMonitor>>,

    idle_callback: Option<Box<dyn Fn(f64) + Send + Sync>>,
    warning_throttle: Throttle,
    close_chan: CancellationToken,
}

impl CpuStats {
    pub fn new(
        idle_update_callback: Option<Box<dyn Fn(f64) + Send + Sync>>,
    ) -> Result<Arc<CpuStats>, Error> {
        let p = new_platform_cpu_monitor()?;

        let c = Arc::new(CpuStats {
            idle_cpus: atomic::Float64::new(0.0),
            sampled: atomic::Bool::new(false),
            platform: Mutex::new(p),
            idle_callback: idle_update_callback,
            warning_throttle: Throttle::new(time::MINUTE),
            close_chan: CancellationToken::new(),
        });

        time::spawn(c.clone().monitor_cpu_load());

        Ok(c)
    }

    pub fn get_cpu_idle(&self) -> f64 {
        self.idle_cpus.load()
    }

    pub fn num_cpu(&self) -> f64 {
        let n = effective_cpu_request();
        if n > 0.0 {
            return n;
        }
        self.platform.lock().num_cpu()
    }

    pub fn get_cpu_load(&self) -> f64 {
        let n_cpu = self.num_cpu();
        if n_cpu <= 0.0 || !self.sampled.load() {
            return 0.0;
        }
        // idle 0 is a saturated node, not a missing sample
        // (`min(1, max(0, x))`, which is NaN for NaN in Go, as `clamp` is)
        (1.0 - self.get_cpu_idle() / n_cpu).clamp(0.0, 1.0)
    }

    fn store_idle(&self, idle: f64) {
        self.idle_cpus.store(idle);
        self.sampled.store(true);
    }

    pub fn stop(&self) {
        self.close_chan.cancel();
    }

    async fn monitor_cpu_load(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = self.close_chan.cancelled() => return,
                _ = time::sleep(time::SECOND) => self.sample_cpu_idle(),
            }
        }
    }

    fn sample_cpu_idle(&self) {
        let mut platform = self.platform.lock();
        let idle = match platform.get_cpu_idle() {
            Ok(idle) => idle,
            Err(err) => {
                errorw!(logger::get_logger(), "failed retrieving CPU idle", err);
                return;
            }
        };

        self.store_idle(idle);
        let idle_ratio = idle / platform.num_cpu();
        drop(platform);

        if idle_ratio < 0.1 {
            self.warning_throttle.call(
                move || infow!(logger::get_logger(), "high cpu load", "load" => 1.0 - idle_ratio),
            );
        }

        if let Some(idle_callback) = &self.idle_callback {
            idle_callback(idle);
        }
    }
}

#[cfg(target_os = "linux")]
pub(super) fn new_platform_cpu_monitor() -> Result<Box<dyn PlatformCpuMonitor>, Error> {
    super::cpu_linux::new_platform_cpu_monitor()
}

// ---------------------------------------------------------------------------
// cpu_all.go

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) struct OsStatCpuMonitor {
    prev: super::osstat::CpuStats,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl OsStatCpuMonitor {
    pub(super) fn new() -> Result<OsStatCpuMonitor, Error> {
        let stats = super::osstat::cpu_get()?;

        Ok(OsStatCpuMonitor { prev: stats })
    }
}

impl PlatformCpuMonitor for OsStatCpuMonitor {
    fn get_cpu_idle(&mut self) -> Result<f64, Error> {
        let next = super::osstat::cpu_get()?;
        let idle_ratio = next.idle.wrapping_sub(self.prev.idle) as f64
            / next.total.wrapping_sub(self.prev.total) as f64;
        self.prev = next;

        Ok(self.num_cpu() * idle_ratio)
    }

    fn num_cpu(&self) -> f64 {
        num_cpu() as f64
    }
}

// ---------------------------------------------------------------------------
// cpu_null.go: every system but Linux

#[cfg(not(target_os = "linux"))]
struct NullStatCpuMonitor;

#[cfg(not(target_os = "linux"))]
impl PlatformCpuMonitor for NullStatCpuMonitor {
    fn get_cpu_idle(&mut self) -> Result<f64, Error> {
        Ok(num_cpu() as f64)
    }

    fn num_cpu(&self) -> f64 {
        num_cpu() as f64
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn new_platform_cpu_monitor() -> Result<Box<dyn PlatformCpuMonitor>, Error> {
    errorw!(
        logger::get_logger(),
        "CPU monitoring unsupported on current platform. Server capacity management will be disabled",
        nil
    );

    Ok(Box::new(NullStatCpuMonitor))
}

// ---------------------------------------------------------------------------
// frostbyte73/core, throttle.go at v0.1.1 (MIT): `core.Throttle`.

// Throttle is a function throttler that takes a function as its argument.
// If ready, it will execute immediately, and it will always wait the specified duration
// between executions. If multiple functions are added within the same execution window,
// only the last function added will be executed.
pub(super) struct Throttle {
    t: Arc<ThrottleInner>,
}

struct ThrottleInner {
    period: Duration,
    m: Mutex<ThrottleState>,
}

struct ThrottleState {
    ready: bool,
    // `timer`, which the original keeps and never reads, is not kept
    next: Option<Box<dyn FnOnce() + Send>>,
}

impl Throttle {
    pub(super) fn new(period: Duration) -> Throttle {
        Throttle {
            t: Arc::new(ThrottleInner {
                period,
                m: Mutex::new(ThrottleState {
                    ready: true,
                    next: None,
                }),
            }),
        }
    }

    pub(super) fn call(&self, f: impl FnOnce() + Send + 'static) {
        ThrottleInner::add(&self.t, Box::new(f));
    }
}

impl ThrottleInner {
    fn add(t: &Arc<ThrottleInner>, f: Box<dyn FnOnce() + Send>) {
        let mut m = t.m.lock();
        let ready = m.ready;
        let mut f = Some(f);
        if ready {
            m.ready = false;
            ThrottleInner::after_period(t);
        } else {
            m.next = f.take();
        }
        drop(m);

        if let Some(f) = f {
            f();
        }
    }

    fn after_period(t: &Arc<ThrottleInner>) {
        let weak = Arc::downgrade(t);
        time::after_func(t.period, move || {
            if let Some(t) = weak.upgrade() {
                ThrottleInner::execute(&t);
            }
        });
    }

    fn execute(t: &Arc<ThrottleInner>) {
        let mut m = t.m.lock();
        let f = m.next.take();
        if f.is_some() {
            ThrottleInner::after_period(t);
        } else {
            m.ready = true;
        }
        drop(m);

        if let Some(f) = f {
            f();
        }
    }
}

#[cfg(test)]
#[path = "cpu_test.rs"]
mod cpu_test;

#[cfg(test)]
#[path = "cpu_request_test.rs"]
mod cpu_request_test;
