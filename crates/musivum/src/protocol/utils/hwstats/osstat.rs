// Ported from mackerelio/go-osstat at v0.2.8: loadavg/loadavg.go, loadavg/loadavg_unix_nocgo.go,
// cpu/cpu_linux.go, memory/memory_linux.go and memory/memory_other.go.
// Copyright 2017-2019 Hatena Co., Ltd. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the three packages are one module (`loadavg.Get` is
// `loadavg_get`, `cpu.Get` is `cpu_get`, `memory.Get` is `memory_get`), and only Linux is
// read: elsewhere each returns an error (upstream reads other systems in their own ways).
// The load average is read from `/proc/loadavg`, as the original does when it is built
// without cgo (with cgo it asks `getloadavg(3)`, which gives the same numbers). A line of
// `/proc/stat` that is `cpu` and nothing more is not counted, where the original panics
// reading its fourth byte. The errors of the system have the text Rust gives them.

use super::Error;

// Stats represents load average values
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LoadavgStats {
    pub loadavg1: f64,
    pub loadavg5: f64,
    pub loadavg15: f64,
}

// Get load average
pub fn loadavg_get() -> Result<LoadavgStats, Error> {
    #[cfg(target_os = "linux")]
    {
        // Reference: man 5 proc, loadavg_proc_show in Linux source code
        let out = std::fs::read_to_string("/proc/loadavg")?;
        collect_loadavg_stats(&out)
    }
    #[cfg(not(target_os = "linux"))]
    Err(Error::Other("failed to get loadavg".to_string()))
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn collect_loadavg_stats(out: &str) -> Result<LoadavgStats, Error> {
    // fmt.Fscanf(out, "%f %f %f", ...)
    let mut fields = out.split_whitespace().map(str::parse::<f64>);
    match (fields.next(), fields.next(), fields.next()) {
        (Some(Ok(loadavg1)), Some(Ok(loadavg5)), Some(Ok(loadavg15))) => Ok(LoadavgStats {
            loadavg1,
            loadavg5,
            loadavg15,
        }),
        _ => Err(Error::Other(
            "unexpected format of /proc/loadavg".to_string(),
        )),
    }
}

// Stats represents cpu statistics for linux
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuStats {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    pub iowait: u64,
    pub irq: u64,
    pub softirq: u64,
    pub steal: u64,
    pub guest: u64,
    pub guest_nice: u64,
    pub total: u64,
    pub cpu_count: i64,
    pub stat_count: i64,
}

// Get cpu statistics
pub fn cpu_get() -> Result<CpuStats, Error> {
    #[cfg(target_os = "linux")]
    {
        // Reference: man 5 proc, Documentation/filesystems/proc.txt in Linux source code
        let out = std::fs::read_to_string("/proc/stat")?;
        collect_cpu_stats(&out)
    }
    #[cfg(not(target_os = "linux"))]
    Err(Error::Other(format!(
        "cpu statistics not implemented for: {}",
        std::env::consts::OS
    )))
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn collect_cpu_stats(out: &str) -> Result<CpuStats, Error> {
    let mut lines = out.lines();
    let mut cpu = CpuStats::default();

    let names = [
        "user",
        "nice",
        "system",
        "idle",
        "iowait",
        "irq",
        "softirq",
        "steal",
        "guest",
        "guest_nice",
    ];

    let Some(first) = lines.next() else {
        return Err(Error::Other("failed to scan /proc/stat".to_string()));
    };

    let val_strs: Vec<&str> = first.split_whitespace().skip(1).collect();
    cpu.stat_count = val_strs.len() as i64;
    for (i, val_str) in val_strs.iter().enumerate() {
        let Ok(val) = val_str.parse::<u64>() else {
            return Err(Error::Other(format!(
                "failed to scan {} from /proc/stat",
                names[i]
            )));
        };
        let ptr = match i {
            0 => &mut cpu.user,
            1 => &mut cpu.nice,
            2 => &mut cpu.system,
            3 => &mut cpu.idle,
            4 => &mut cpu.iowait,
            5 => &mut cpu.irq,
            6 => &mut cpu.softirq,
            7 => &mut cpu.steal,
            8 => &mut cpu.guest,
            9 => &mut cpu.guest_nice,
            // `cpuStats[i]` out of range: Go panics
            _ => panic!("index out of range [{i}] with length 10"),
        };
        *ptr = val;
        cpu.total = cpu.total.wrapping_add(val);
    }

    // Since cpustat[CPUTIME_USER] includes cpustat[CPUTIME_GUEST], subtract the duplicated values from total.
    // https://github.com/torvalds/linux/blob/4ec9f7a18/kernel/sched/cputime.c#L151-L158
    cpu.total = cpu.total.wrapping_sub(cpu.guest);
    // cpustat[CPUTIME_NICE] includes cpustat[CPUTIME_GUEST_NICE]
    cpu.total = cpu.total.wrapping_sub(cpu.guest_nice);

    for line in lines {
        if line.starts_with("cpu") && line.as_bytes().get(3).is_some_and(u8::is_ascii_digit) {
            cpu.cpu_count += 1;
        }
    }

    Ok(cpu)
}

// Stats represents memory statistics for linux
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryStats {
    pub total: u64,
    pub used: u64,
    pub buffers: u64,
    pub cached: u64,
    pub free: u64,
    pub available: u64,
    pub active: u64,
    pub inactive: u64,
    pub swap_total: u64,
    pub swap_used: u64,
    pub swap_cached: u64,
    pub swap_free: u64,
    pub mapped: u64,
    pub shmem: u64,
    pub slab: u64,
    pub page_tables: u64,
    pub committed: u64,
    pub vmalloc_used: u64,
    pub mem_available_enabled: bool,
}

// Get memory statistics
pub fn memory_get() -> Result<MemoryStats, Error> {
    #[cfg(target_os = "linux")]
    {
        // Reference: man 5 proc, Documentation/filesystems/proc.txt in Linux source code
        let out = std::fs::read_to_string("/proc/meminfo")?;
        Ok(collect_memory_stats(&out))
    }
    #[cfg(not(target_os = "linux"))]
    Err(Error::Other(format!(
        "memory statistics not implemented for: {}",
        std::env::consts::OS
    )))
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn collect_memory_stats(out: &str) -> MemoryStats {
    let mut memory = MemoryStats::default();
    for line in out.lines() {
        let Some(i) = line.find(':') else {
            continue;
        };
        let fld = &line[..i];
        let ptr = match fld {
            "MemTotal" => &mut memory.total,
            "MemFree" => &mut memory.free,
            "MemAvailable" => &mut memory.available,
            "Buffers" => &mut memory.buffers,
            "Cached" => &mut memory.cached,
            "Active" => &mut memory.active,
            "Inactive" => &mut memory.inactive,
            "SwapCached" => &mut memory.swap_cached,
            "SwapTotal" => &mut memory.swap_total,
            "SwapFree" => &mut memory.swap_free,
            "Mapped" => &mut memory.mapped,
            "Shmem" => &mut memory.shmem,
            "Slab" => &mut memory.slab,
            "PageTables" => &mut memory.page_tables,
            "Committed_AS" => &mut memory.committed,
            "VmallocUsed" => &mut memory.vmalloc_used,
            _ => continue,
        };
        // strings.TrimSpace(strings.TrimRight(line[i+1:], "kB"))
        let val = line[i + 1..].trim_end_matches(['k', 'B']).trim();
        if let Ok(v) = val.parse::<u64>() {
            *ptr = v.wrapping_mul(1024);
        }
        if fld == "MemAvailable" {
            memory.mem_available_enabled = true;
        }
    }

    memory.swap_used = memory.swap_total.wrapping_sub(memory.swap_free);

    if memory.mem_available_enabled {
        memory.used = memory.total.wrapping_sub(memory.available);
    } else {
        memory.used = memory
            .total
            .wrapping_sub(memory.free)
            .wrapping_sub(memory.buffers)
            .wrapping_sub(memory.cached);
    }

    memory
}
