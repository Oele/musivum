// Ported from LiveKit's protocol, utils/hwstats/cpu_test.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. `TestAggregateMemoryStats` is not ported: it tests the
// monitor of processes, which is not ported. The CPU stats of `TestGetCPUIdle` are stopped
// at the end, where the original leaves their worker running.

use super::*;
use crate::testutils::assert_in_delta;

#[test]
fn test_get_cpu_idle() {
    let st = CpuStats::new(None).unwrap();

    let idle = st.get_cpu_idle();
    assert!(idle >= 0.0);
    st.stop();
}

pub(super) struct FakeCpuMonitor {
    pub(super) idle: Arc<Mutex<f64>>,
    pub(super) n: f64,
}

impl PlatformCpuMonitor for FakeCpuMonitor {
    fn get_cpu_idle(&mut self) -> Result<f64, Error> {
        Ok(*self.idle.lock())
    }
    fn num_cpu(&self) -> f64 {
        self.n
    }
}

#[test]
fn test_get_cpu_load() {
    let idle = Arc::new(Mutex::new(0.0));
    let platform = FakeCpuMonitor {
        idle: idle.clone(),
        n: 8.0,
    };
    let st = CpuStats {
        idle_cpus: atomic::Float64::new(0.0),
        sampled: atomic::Bool::new(false),
        platform: Mutex::new(Box::new(platform)),
        idle_callback: None,
        warning_throttle: Throttle::new(time::MINUTE),
        close_chan: CancellationToken::new(),
    };

    // no sample yet
    assert_eq!(0.0, st.get_cpu_load());

    struct Case {
        idle: f64,
        load: f64,
    }
    for c in [
        Case {
            idle: 8.0,
            load: 0.0,
        },
        Case {
            idle: 4.0,
            load: 0.5,
        },
        Case {
            idle: 0.01,
            load: 0.99875,
        },
        // a saturated node has its idle clamped to 0
        Case {
            idle: 0.0,
            load: 1.0,
        },
    ] {
        *idle.lock() = c.idle;
        st.sample_cpu_idle();
        assert_in_delta(c.load, st.get_cpu_load(), 1e-9);
    }
}
