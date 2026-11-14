// Ported from LiveKit's protocol, utils/hwstats/memory_linux.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. The paths, package variables that the tests point
// elsewhere, are statics behind locks. The cgroup version is probed in a fixed order (v1,
// then v2) where Go ranges over a map in random order. The getters share the system wide
// getter through an `Arc`; `cgroupMemoryGetter` keeps it, as the original does, without
// using it. The errors of the system and of the number parser have Rust's texts; reading an
// empty file panics, as the original does when it cuts off the last byte.

use std::io::BufRead;
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;

use super::Error;
use super::cpu_linux::file_exists;
use super::memory::{OsStatMemoryGetter, PlatformMemoryGetter};
use crate::infow;
use crate::protocol::logger;

type PathVar = LazyLock<RwLock<String>, fn() -> RwLock<String>>;

pub(super) static MEM_USAGE_PATH_V1: PathVar =
    LazyLock::new(|| RwLock::new("/sys/fs/cgroup/memory/memory.usage_in_bytes".to_string()));
pub(super) static MEM_LIMIT_PATH_V1: PathVar =
    LazyLock::new(|| RwLock::new("/sys/fs/cgroup/memory/memory.limit_in_bytes".to_string()));
pub(super) static MEM_STAT_PATH_V1: PathVar =
    LazyLock::new(|| RwLock::new("/sys/fs/cgroup/memory/memory.stat".to_string()));

pub(super) static MEM_CURRENT_PATH_V2: PathVar =
    LazyLock::new(|| RwLock::new("/sys/fs/cgroup/memory.current".to_string()));
pub(super) static MEM_MAX_PATH_V2: PathVar =
    LazyLock::new(|| RwLock::new("/sys/fs/cgroup/memory.max".to_string()));
pub(super) static MEM_STAT_PATH_V2: PathVar =
    LazyLock::new(|| RwLock::new("/sys/fs/cgroup/memory.stat".to_string()));

/// Held by the tests that point the paths elsewhere, and by those that make the memory
/// getter, which probes them: Go runs the tests of a package one after another.
#[cfg(test)]
pub(crate) static TEST_PATHS_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

pub(super) const TOTAL_INACTIVE_FILE_V1: &str = "total_inactive_file";
pub(super) const INACTIVE_FILE_V2: &str = "inactive_file";

trait MemInfoGetter: Send + Sync {
    fn get_memory(&self) -> Result<(u64, u64), Error>;
}

struct CgroupMemoryGetter {
    cg: Box<dyn MemInfoGetter>,
    _os_stat: Arc<OsStatMemoryGetter>,
}

pub(super) fn new_platform_memory_getter() -> Result<Box<dyn PlatformMemoryGetter>, Error> {
    // backup getter when max is not available,
    // will get from /proc/meminfo which will be node level max though
    let os_stat = Arc::new(OsStatMemoryGetter::new()?);

    // probe for the cgroup version
    let mut cg: Option<Box<dyn MemInfoGetter>> = None;
    for (k, v) in [
        (
            MEM_USAGE_PATH_V1.read().clone(),
            new_mem_info_getter_v1 as fn(Arc<OsStatMemoryGetter>) -> Box<dyn MemInfoGetter>,
        ),
        (MEM_CURRENT_PATH_V2.read().clone(), new_mem_info_getter_v2),
    ] {
        let e = file_exists(&k)?;
        if e {
            cg = Some(v(os_stat.clone()));
            break;
        }
    }
    let Some(cg) = cg else {
        infow!(
            logger::get_logger(),
            "failed reading cgroup specific memory stats, falling back to system wide implementation"
        );
        return Ok(Box::new(
            Arc::try_unwrap(os_stat).unwrap_or(OsStatMemoryGetter),
        ));
    };

    Ok(Box::new(CgroupMemoryGetter {
        cg,
        _os_stat: os_stat,
    }))
}

impl PlatformMemoryGetter for CgroupMemoryGetter {
    fn get_memory(&self) -> Result<(u64, u64), Error> {
        self.cg.get_memory()
    }
}

// -----------------------------------------

pub(super) struct MemInfoGetterV1 {
    os_stat: Arc<OsStatMemoryGetter>,
}

fn new_mem_info_getter_v1(os_stat: Arc<OsStatMemoryGetter>) -> Box<dyn MemInfoGetter> {
    Box::new(MemInfoGetterV1 { os_stat })
}

impl MemInfoGetter for MemInfoGetterV1 {
    fn get_memory(&self) -> Result<(u64, u64), Error> {
        let mut usage = read_value_from_file(&MEM_USAGE_PATH_V1.read())?;
        if let Ok(total_inactive_file) =
            read_value_of_key_from_file(&MEM_STAT_PATH_V1.read(), TOTAL_INACTIVE_FILE_V1)
            && usage > total_inactive_file
        {
            usage -= total_inactive_file;
        }

        let total = read_value_from_file(&MEM_LIMIT_PATH_V1.read())?;

        // fallback if limit from cgroup is more than physical available memory
        // when limit is not set explicitly, it could be very high
        let (_, total1) = self.os_stat.get_memory()?;
        if total > total1 {
            return Ok((usage, total1));
        }

        Ok((usage, total))
    }
}

// --------------------------------------------

pub(super) struct MemInfoGetterV2 {
    os_stat: Arc<OsStatMemoryGetter>,
}

fn new_mem_info_getter_v2(os_stat: Arc<OsStatMemoryGetter>) -> Box<dyn MemInfoGetter> {
    Box::new(MemInfoGetterV2 { os_stat })
}

impl MemInfoGetter for MemInfoGetterV2 {
    fn get_memory(&self) -> Result<(u64, u64), Error> {
        let mut usage = read_value_from_file(&MEM_CURRENT_PATH_V2.read())?;
        if let Ok(inactive_file) =
            read_value_of_key_from_file(&MEM_STAT_PATH_V2.read(), INACTIVE_FILE_V2)
            && usage > inactive_file
        {
            usage -= inactive_file;
        }

        let total = match read_value_from_file(&MEM_MAX_PATH_V2.read()) {
            Ok(total) => total,
            Err(_) => {
                // when memory limit not set, it has the string "max"
                let (_, total) = self.os_stat.get_memory()?;
                total
            }
        };

        Ok((usage, total))
    }
}

// ---------------------------------

fn read_value_from_file(file: &str) -> Result<u64, Error> {
    let b = std::fs::read(file)?;

    // Skip the trailing EOL
    Ok(String::from_utf8_lossy(&b[..b.len() - 1]).parse::<u64>()?)
}

fn read_value_of_key_from_file(file: &str, key: &str) -> Result<u64, Error> {
    let fd = std::fs::File::open(file)?;

    for line in std::io::BufReader::new(fd).lines() {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                return Err(Error::Other(format!(
                    "scan error for {file}, key: {key}: {err}"
                )));
            }
        };
        let parts: Vec<&str> = line.split(' ').collect();
        if parts.len() != 2 {
            continue;
        }

        if parts[0] != key {
            continue;
        }

        let val = parts[1].trim();
        if let Ok(v) = val.parse::<u64>() {
            return Ok(v);
        }
    }

    Ok(0)
}

#[cfg(test)]
#[path = "memory_linux_test.rs"]
mod memory_linux_test;
