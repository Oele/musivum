// Ported from LiveKit's protocol, utils/hwstats/memory_linux_test.go at
// v1.53.1-0.20261023175527-46332a036e25 (a file without a notice of its own, of a repository
// licensed under the Apache License, Version 2.0).
// Copyright LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; `t.TempDir()` is a directory under the build's
// `target`, and `t.Cleanup` a guard that points the paths back and removes it. The subtests
// are blocks, each with its own guard. Each test holds `TEST_PATHS_LOCK`, so that no other
// test probes the paths while they point at its files.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

const TEST_CGROUP_CHARGED: u64 = 3 << 30;
const TEST_CGROUP_INACTIVE_FILE: u64 = 1 << 30;
const TEST_CGROUP_WORKING_SET: u64 = TEST_CGROUP_CHARGED - TEST_CGROUP_INACTIVE_FILE;

struct Cleanup {
    dir: PathBuf,
    restore: Vec<(&'static PathVar, String)>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for (ptr, orig) in self.restore.drain(..) {
            *ptr.write() = orig;
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// writeCgroupFiles points the given path variables at freshly written files for the duration of the test.
fn write_cgroup_files(
    usage_path: &'static PathVar,
    limit_path: &'static PathVar,
    stat_path: &'static PathVar,
    limit: &str,
    inactive_file_key: &str,
) -> Cleanup {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir =
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/tmp")).join(format!(
            "memory_linux_test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut cleanup = Cleanup {
        dir: dir.clone(),
        restore: Vec::new(),
    };

    let files = [
        (usage_path, format!("{TEST_CGROUP_CHARGED}\n")),
        (limit_path, format!("{limit}\n")),
        (
            stat_path,
            format!("anon 1024\n{inactive_file_key} {TEST_CGROUP_INACTIVE_FILE}\n"),
        ),
    ];
    for (ptr, content) in files {
        let orig = ptr.read().clone();
        let name = PathBuf::from(&orig).file_name().unwrap().to_owned();
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        *ptr.write() = path.to_str().unwrap().to_string();
        cleanup.restore.push((ptr, orig));
    }
    cleanup
}

fn node_memory() -> u64 {
    let os_stat = OsStatMemoryGetter::new().unwrap();
    let (_, total) = os_stat.get_memory().unwrap();
    total
}

#[test]
fn test_mem_info_getter_v2() {
    let _paths = TEST_PATHS_LOCK.lock();
    let node_total = node_memory();
    let os_stat = Arc::new(OsStatMemoryGetter::new().unwrap());
    let cg = new_mem_info_getter_v2(os_stat);

    // limit set
    {
        // below physical memory so the cgroup limit is reported as is
        let limit = node_total / 2;
        let _cleanup = write_cgroup_files(
            &MEM_CURRENT_PATH_V2,
            &MEM_MAX_PATH_V2,
            &MEM_STAT_PATH_V2,
            &limit.to_string(),
            INACTIVE_FILE_V2,
        );
        let (usage, total) = cg.get_memory().unwrap();
        assert_eq!(TEST_CGROUP_WORKING_SET, usage, "limit set");
        assert_eq!(limit, total, "limit set");
    }

    // unlimited keeps cgroup usage
    {
        let _cleanup = write_cgroup_files(
            &MEM_CURRENT_PATH_V2,
            &MEM_MAX_PATH_V2,
            &MEM_STAT_PATH_V2,
            "max",
            INACTIVE_FILE_V2,
        );
        let (usage, total) = cg.get_memory().unwrap();
        assert_eq!(TEST_CGROUP_WORKING_SET, usage, "unlimited");
        assert_eq!(node_total, total, "unlimited");
    }
}

#[test]
fn test_mem_info_getter_v1() {
    let _paths = TEST_PATHS_LOCK.lock();
    let node_total = node_memory();
    let os_stat = Arc::new(OsStatMemoryGetter::new().unwrap());
    let cg = new_mem_info_getter_v1(os_stat);

    // limit set
    {
        // below physical memory so the cgroup limit is reported as is
        let limit = node_total / 2;
        let _cleanup = write_cgroup_files(
            &MEM_USAGE_PATH_V1,
            &MEM_LIMIT_PATH_V1,
            &MEM_STAT_PATH_V1,
            &limit.to_string(),
            TOTAL_INACTIVE_FILE_V1,
        );
        let (usage, total) = cg.get_memory().unwrap();
        assert_eq!(TEST_CGROUP_WORKING_SET, usage, "limit set");
        assert_eq!(limit, total, "limit set");
    }

    // limit above physical keeps cgroup usage
    {
        // cgroup v1 reports an unlimited cgroup as a limit far above physical memory
        let _cleanup = write_cgroup_files(
            &MEM_USAGE_PATH_V1,
            &MEM_LIMIT_PATH_V1,
            &MEM_STAT_PATH_V1,
            "9223372036854771712",
            TOTAL_INACTIVE_FILE_V1,
        );
        let (usage, total) = cg.get_memory().unwrap();
        assert_eq!(TEST_CGROUP_WORKING_SET, usage, "limit above physical");
        assert_eq!(node_total, total, "limit above physical");
    }
}
