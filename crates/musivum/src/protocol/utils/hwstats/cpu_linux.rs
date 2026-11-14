// Ported from LiveKit's protocol, utils/hwstats/cpu_linux.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The cgroup version is probed in a fixed order (v1,
// then v2) where the original ranges over a map, whose order Go makes random; it matters
// only on a system that has both files. The expression `usage_usec ([0-9]+)` is matched by
// hand. `getHostCPUCount` and `getPageSize` are left out with the monitor of processes,
// which alone uses them. The errors of the system and of the number parsers have Rust's
// texts; reading an empty file panics, as the original does when it cuts off the last byte.

use super::cpu::{OsStatCpuMonitor, PlatformCpuMonitor};
use super::{Error, num_cpu};
use crate::go::time;
use crate::infow;
use crate::protocol::logger;

const CPU_STATS_PATH_V1: &str = "/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage";
const CPU_STATS_PATH_V2: &str = "/sys/fs/cgroup/cpu.stat";

const NUM_CPU_PATH_V1_PERIOD: &str = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";
const NUM_CPU_PATH_V1_QUOTA: &str = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
const NUM_CPU_PATH_V2: &str = "/sys/fs/cgroup/cpu.max";

trait CpuInfoGetter: Send {
    fn get_total_cpu_time(&self) -> Result<i64, Error>;
    fn num_cpu(&self) -> Result<f64, Error>;
}

struct CgroupCpuMonitor {
    last_sample_time: i64,
    last_total_cpu_time: i64,
    n_cpu: f64,

    cg: Box<dyn CpuInfoGetter>,
}

pub(super) fn new_platform_cpu_monitor() -> Result<Box<dyn PlatformCpuMonitor>, Error> {
    // probe for the cgroup version
    let mut cg: Option<Box<dyn CpuInfoGetter>> = None;
    for (k, v) in [
        (
            CPU_STATS_PATH_V1,
            new_cpu_info_getter_v1 as fn() -> Box<dyn CpuInfoGetter>,
        ),
        (CPU_STATS_PATH_V2, new_cpu_info_getter_v2),
    ] {
        let e = file_exists(k)?;
        if e {
            cg = Some(v());
            break;
        }
    }
    let Some(cg) = cg else {
        infow!(
            logger::get_logger(),
            "failed reading cgroup specific cpu stats, falling back to system wide implementation"
        );
        return Ok(Box::new(OsStatCpuMonitor::new()?));
    };

    let cpu = cg.get_total_cpu_time()?;

    let n_cpu = cg.num_cpu()?;

    Ok(Box::new(CgroupCpuMonitor {
        last_sample_time: time::now().unix_nano(),
        last_total_cpu_time: cpu,
        n_cpu,
        cg,
    }))
}

impl PlatformCpuMonitor for CgroupCpuMonitor {
    fn get_cpu_idle(&mut self) -> Result<f64, Error> {
        let next = self.cg.get_total_cpu_time()?;
        let t = time::now().unix_nano();

        let duration = t.wrapping_sub(self.last_sample_time);
        let cpu_time = next.wrapping_sub(self.last_total_cpu_time);

        let busy_ratio = cpu_time as f64 / duration as f64;
        let mut idle_ratio = self.n_cpu - busy_ratio;

        // Clamp the value as we do not get all the timestamps at the same time
        if idle_ratio > self.n_cpu {
            idle_ratio = self.n_cpu;
        } else if idle_ratio < 0.0 {
            idle_ratio = 0.0;
        }

        self.last_sample_time = t;
        self.last_total_cpu_time = next;

        Ok(idle_ratio)
    }

    fn num_cpu(&self) -> f64 {
        self.n_cpu
    }
}

struct CpuInfoGetterV1;

fn new_cpu_info_getter_v1() -> Box<dyn CpuInfoGetter> {
    Box::new(CpuInfoGetterV1)
}

impl CpuInfoGetter for CpuInfoGetterV1 {
    fn get_total_cpu_time(&self) -> Result<i64, Error> {
        let b = std::fs::read(CPU_STATS_PATH_V1)?;

        // Skip the trailing EOL
        let i = String::from_utf8_lossy(&b[..b.len() - 1]).parse::<i64>()?;

        Ok(i)
    }

    fn num_cpu(&self) -> Result<f64, Error> {
        let quota = match read_int_from_file(NUM_CPU_PATH_V1_QUOTA) {
            Ok(quota) => quota,
            Err(err) => {
                if err.is_not_exist() {
                    // File may not exist in case of no quota
                    return Ok(num_cpu() as f64);
                }

                return Err(err);
            }
        };

        if quota < 0 {
            // default
            return Ok(num_cpu() as f64);
        }

        let period = read_int_from_file(NUM_CPU_PATH_V1_PERIOD)?;

        if period <= 0 {
            // default
            return Ok(num_cpu() as f64);
        }

        Ok(quota as f64 / period as f64)
    }
}

struct CpuInfoGetterV2;

fn new_cpu_info_getter_v2() -> Box<dyn CpuInfoGetter> {
    Box::new(CpuInfoGetterV2)
}

// usageRegex.FindSubmatch(b): the digits after the first `usage_usec ` that has any.
fn find_usage_usec(b: &str) -> Option<&str> {
    const KEY: &str = "usage_usec ";
    let mut rest = b;
    while let Some(at) = rest.find(KEY) {
        let after = &rest[at + KEY.len()..];
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        if digits > 0 {
            return Some(&after[..digits]);
        }
        rest = &rest[at + 1..];
    }
    None
}

impl CpuInfoGetter for CpuInfoGetterV2 {
    fn get_total_cpu_time(&self) -> Result<i64, Error> {
        let b = std::fs::read(CPU_STATS_PATH_V2)?;
        let b = String::from_utf8_lossy(&b);

        let Some(m) = find_usage_usec(&b) else {
            return Err(Error::Other("could not parse cpu stats".to_string()));
        };

        let i = m.parse::<i64>()?;

        // Caller expects time in ns
        Ok(i.wrapping_mul(1000))
    }

    fn num_cpu(&self) -> Result<f64, Error> {
        let b = match std::fs::read(NUM_CPU_PATH_V2) {
            Ok(b) => b,
            Err(err) => {
                let err = Error::from(err);
                if err.is_not_exist() {
                    // File may not exist in case of no quota
                    return Ok(num_cpu() as f64);
                }
                return Err(err);
            }
        };

        let b = String::from_utf8_lossy(&b);
        let s = b.strip_suffix('\n').unwrap_or(&b);

        let m: Vec<&str> = s.split(' ').collect();
        if m.len() <= 1 {
            return Err(Error::Other("could not parse cpu stats".to_string()));
        }

        if m[0] == "max" {
            // No quota
            return Ok(num_cpu() as f64);
        }

        let quota = m[0].parse::<i64>()?;

        let period = m[1].parse::<i64>()?;

        Ok(quota as f64 / period as f64)
    }
}

pub(super) fn file_exists(path: &str) -> Result<bool, Error> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

fn read_int_from_file(filename: &str) -> Result<i64, Error> {
    let b = std::fs::read(filename)?;
    let b = String::from_utf8_lossy(&b);

    // Remove trailing new line if any
    let s = b.strip_suffix('\n').unwrap_or(&b);

    // Remove trailing space if any
    let s = s.strip_suffix(' ').unwrap_or(s);

    Ok(s.parse::<i64>()?)
}
