// Ported from LiveKit's protocol, utils/hwstats/memory.go, memory_all.go and
// memory_nonlinux.go at v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the three files are one. `getMemory`'s two results
// are a tuple, used then total, as in Go's order.

use super::Error;

// This object returns cgroup quota aware memory stats. On other systems than Linux,
// it falls back to full system stats

pub(super) trait PlatformMemoryGetter: Send + Sync {
    fn get_memory(&self) -> Result<(u64, u64), Error>;
}

pub struct MemoryStats {
    platform: Box<dyn PlatformMemoryGetter>,
}

impl MemoryStats {
    pub fn new() -> Result<MemoryStats, Error> {
        let p = new_platform_memory_getter()?;

        Ok(MemoryStats { platform: p })
    }

    pub fn get_memory(&self) -> Result<(u64, u64), Error> {
        self.platform.get_memory()
    }
}

#[cfg(target_os = "linux")]
fn new_platform_memory_getter() -> Result<Box<dyn PlatformMemoryGetter>, Error> {
    super::memory_linux::new_platform_memory_getter()
}

// memory_nonlinux.go
#[cfg(not(target_os = "linux"))]
fn new_platform_memory_getter() -> Result<Box<dyn PlatformMemoryGetter>, Error> {
    Ok(Box::new(OsStatMemoryGetter::new()?))
}

// ---------------------------------------------------------------------------
// memory_all.go

pub(super) struct OsStatMemoryGetter;

impl OsStatMemoryGetter {
    pub(super) fn new() -> Result<OsStatMemoryGetter, Error> {
        Ok(OsStatMemoryGetter)
    }
}

impl PlatformMemoryGetter for OsStatMemoryGetter {
    fn get_memory(&self) -> Result<(u64, u64), Error> {
        let stats = super::osstat::memory_get()?;
        Ok((stats.used, stats.total))
    }
}
