//! LiveKit's protocol package `utils/hwstats`, as far as a node's statistics need it: the
//! load of the CPUs and the memory in use, aware of the quotas of a cgroup.
//!
//! Ported: the CPU monitor of `cpu.go`, `cpu_linux.go` and
//! `cpu_all.go`, the memory of `memory.go`, `memory_linux.go` and `memory_all.go`, and of
//! `mackerelio/go-osstat` (Apache-2.0) what those read on Linux (`osstat.rs`). Not ported:
//! the monitor of processes (`NewProcMonitor`, `monitorProcesses`, `aggregateMemoryStats`,
//! `ProcStats` and the rest of it, which egress uses and a server does not), with its test,
//! and `maxprocs`. On other systems than Linux the CPU load is 0 (`cpu_null.go`, which
//! upstream uses everywhere but Linux and macOS) and the memory is an error, which the
//! node's statistics take as 0.
//!
//! [`num_cpu`] is written new: Go's `runtime.NumCPU()`.

mod cpu;
#[cfg(target_os = "linux")]
mod cpu_linux;
mod errors;
mod memory;
#[cfg(target_os = "linux")]
mod memory_linux;
pub mod osstat;

pub use cpu::*;
pub use errors::*;
pub use memory::*;
#[cfg(all(test, target_os = "linux"))]
pub(crate) use memory_linux::TEST_PATHS_LOCK;

/// Go's `runtime.NumCPU()`: the number of logical CPUs the process may run on.
///
/// Go counts the CPUs of the process's affinity mask when the program starts; on Linux that
/// mask is read here from `/proc/self/status`. Elsewhere, and where that cannot be read, it
/// is what the standard library says may run in parallel, which on Linux would also take the
/// quota of a cgroup into account, as Go does not.
pub fn num_cpu() -> i64 {
    #[cfg(target_os = "linux")]
    if let Some(n) = affinity_count() {
        return n;
    }
    std::thread::available_parallelism().map_or(1, |n| n.get() as i64)
}

// The CPUs of `Cpus_allowed`, a mask in hexadecimal digits with commas between the words.
#[cfg(target_os = "linux")]
fn affinity_count() -> Option<i64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let mask = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed:"))?;
    let mut count = 0i64;
    for c in mask.trim().chars().filter(|&c| c != ',') {
        count += i64::from(c.to_digit(16)?.count_ones());
    }
    (count > 0).then_some(count)
}
