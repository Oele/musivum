// Ported from LiveKit's protocol, utils/hwstats/cpu_request_test.go at
// v1.53.1-0.20261023175527-46332a036e25 (a file without a notice of its own, of a repository
// licensed under the Apache License, Version 2.0).
// Copyright LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The subprocess is this test binary again, asked for
// `test_subprocess_worker` alone; it prints its result as a line of JSON, as the original.

use std::process::Command;

use super::*;
use crate::testutils::assert_in_delta;

// ---------------------------------------------------------------------------
// Mock platform monitor
// ---------------------------------------------------------------------------

struct MockPlatform {
    n_cpu: f64,
}

impl PlatformCpuMonitor for MockPlatform {
    fn get_cpu_idle(&mut self) -> Result<f64, Error> {
        Ok(self.n_cpu)
    }
    fn num_cpu(&self) -> f64 {
        self.n_cpu
    }
}

fn new_test_cpu_stats(plat: MockPlatform) -> CpuStats {
    CpuStats {
        idle_cpus: atomic::Float64::new(0.0),
        sampled: atomic::Bool::new(false),
        platform: Mutex::new(Box::new(plat)),
        idle_callback: None,
        warning_throttle: Throttle::new(time::MINUTE),
        close_chan: CancellationToken::new(),
    }
}

// ---------------------------------------------------------------------------
// NumCPU without override (no env var set in this process)
// ---------------------------------------------------------------------------

#[test]
fn test_num_cpu_no_override() {
    let c = new_test_cpu_stats(MockPlatform { n_cpu: 64.0 });
    assert_eq!(64.0, c.num_cpu());

    let c2 = new_test_cpu_stats(MockPlatform { n_cpu: 8.5 });
    assert_eq!(8.5, c2.num_cpu());
}

// ---------------------------------------------------------------------------
// Subprocess tests for EffectiveCPURequest and NumCPU with override
//
// sync.Once means we can only test one env-var value per process.
// Each test case spawns a subprocess with the desired env.
// ---------------------------------------------------------------------------

const SUBPROCESS_ENV_KEY: &str = "HWSTATS_TEST_SUBPROCESS";

#[derive(serde::Serialize, serde::Deserialize)]
struct SubprocessResult {
    effective_cpu: f64,
    num_cpu: f64,
    platform_cpu: f64,
}

#[test]
fn test_effective_cpu_request() {
    struct Test {
        name: &'static str,
        env_value: &'static str,
        expected_cpu: f64,
    }
    let tests = [
        Test {
            name: "valid integer",
            env_value: "4",
            expected_cpu: 4.0,
        },
        Test {
            name: "valid float",
            env_value: "3.33",
            expected_cpu: 3.33,
        },
        Test {
            name: "valid large",
            env_value: "15",
            expected_cpu: 15.0,
        },
        Test {
            name: "valid fractional",
            env_value: "0.5",
            expected_cpu: 0.5,
        },
        Test {
            name: "empty",
            env_value: "",
            expected_cpu: 0.0,
        },
        Test {
            name: "negative",
            env_value: "-1",
            expected_cpu: 0.0,
        },
        Test {
            name: "zero",
            env_value: "0",
            expected_cpu: 0.0,
        },
        Test {
            name: "invalid string",
            env_value: "abc",
            expected_cpu: 0.0,
        },
        Test {
            name: "invalid mixed",
            env_value: "4abc",
            expected_cpu: 0.0,
        },
    ];

    for tt in tests {
        let result = run_subprocess(tt.name, tt.env_value);
        assert_in_delta(tt.expected_cpu, result.effective_cpu, 0.001);
    }
}

fn run_subprocess(name: &str, cpu_request_env: &str) -> SubprocessResult {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--exact",
        "protocol::utils::hwstats::cpu::cpu_request_test::test_subprocess_worker",
        "--nocapture",
        "--test-threads=1",
    ]);

    // Build clean env
    cmd.env_remove(CPU_REQUEST_ENV_VAR);
    cmd.env(SUBPROCESS_ENV_KEY, "1");
    if !cpu_request_env.is_empty() {
        cmd.env(CPU_REQUEST_ENV_VAR, cpu_request_env);
    }

    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{name}: subprocess failed: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let out = String::from_utf8_lossy(&out.stdout);
    for line in out.split('\n') {
        let line = line.trim();
        if line.starts_with('{') {
            return serde_json::from_str(line).unwrap();
        }
    }
    panic!("{name}: no JSON result in subprocess output: {out}");
}

// TestSubprocessWorker runs inside spawned subprocesses only.
#[test]
fn test_subprocess_worker() {
    if std::env::var(SUBPROCESS_ENV_KEY).as_deref() != Ok("1") {
        // only runs as subprocess
        return;
    }

    let plat = MockPlatform {
        n_cpu: num_cpu() as f64,
    };
    let platform_cpu = plat.num_cpu();
    let c = new_test_cpu_stats(plat);

    let result = SubprocessResult {
        effective_cpu: effective_cpu_request(),
        num_cpu: c.num_cpu(),
        platform_cpu,
    };

    // on a line of its own: libtest has written the test's name without a line's end
    println!("\n{}", serde_json::to_string(&result).unwrap());
}
