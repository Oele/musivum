#!/usr/bin/env python3
"""What a big fake costs to compile.

Writes a scratch crate with a trait of N methods (150 unless given), the shapes and the
variety of types of an interface like LiveKit's LocalParticipant, and times how long the
crate takes to check and to build as a test binary, with `#[musivum_fakes::fake]` on the
trait and without. The crate is written to target/fakes-compile-time of the repository; the
trait is not kept anywhere else.

Two times are given: by the clock, and the processor time of cargo and the compiler. On a
machine that is busy with other work the second says more.

    crates/musivum-fakes/compile-time.py [N]
"""

import os
import resource
import subprocess
import sys
import time

N = int(sys.argv[1]) if len(sys.argv) > 1 else 150
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SCRATCH = os.path.join(REPO, "target", "fakes-compile-time")
MESSAGES = 20

# One method of each shape in turn; {m} is a message type that differs from method to method.
SHAPES = [
    "fn m{i}(&self) -> String;",
    "fn m{i}(&self) -> bool;",
    "fn m{i}(&self, id: &str) -> Option<Arc<{m}>>;",
    "fn m{i}(&self, update: &{m}, from_admin: bool) -> Result<(), Error>;",
    "fn m{i}(&self, other: Arc<dyn Other>, reason: Reason);",
    "fn m{i}(&self) -> (f64, bool);",
    "fn m{i}(&self, data: &[u8], kind: i32, ids: Vec<String>) -> Result<{m}, Error>;",
    "fn m{i}(&self, on_change: Box<dyn Fn(&{m}) + Send + Sync>);",
    "fn m{i}(&self);",
    "fn m{i}(&self, attributes: &HashMap<String, String>) -> Arc<dyn Other>;",
    "fn m{i}(&self, message: {m}) -> {m};",
    "fn m{i}(&self, track: Option<Arc<dyn Other>>, layer: i32, force: bool) -> Vec<{m}>;",
]


def lib_rs(fake):
    out = [
        "pub mod testutils {",
        '    #[path = "%s/crates/musivum/src/testutils/fakes.rs"]' % REPO,
        "    pub mod fakes;",
        "}",
        "use std::collections::HashMap;",
        "use std::sync::Arc;",
        "#[derive(Debug)] pub struct Error;",
        "#[derive(Clone, Copy, Debug, Default, PartialEq)] pub enum Reason { #[default] None, Other }",
        "pub trait Other: Send + Sync { fn id(&self) -> String; }",
    ]
    for k in range(MESSAGES):
        out.append(
            "#[derive(Clone, Debug, Default, PartialEq)] pub struct Message%d { pub a: u32, pub b: String }"
            % k
        )
    if fake:
        out.append("#[musivum_fakes::fake]")
    out.append("pub trait Big: Send + Sync {")
    for i in range(N):
        shape = SHAPES[i % len(SHAPES)]
        out.append("    " + shape.format(i=i, m="Message%d" % (i % MESSAGES)))
    out.append("}")
    if fake:
        out.append(
            """
#[test]
fn the_big_fake_works() {
    let fake = Arc::new(FakeBig::default());
    let big: Arc<dyn Big> = fake.clone();
    fake.m0_returns("zero".to_owned());
    fake.m1_returns(true);
    fake.m3_calls(|_, from_admin| if from_admin { Ok(()) } else { Err(Error) });
    assert_eq!(big.m0(), "zero");
    assert!(big.m1());
    assert!(big.m2("a").is_none());
    assert!(big.m3(&Message3::default(), true).is_ok());
    big.m8();
    assert_eq!(fake.m8_call_count(), 1);
    assert_eq!(fake.m3_args_for_call(0), (Message3::default(), true));
}
"""
        )
    return "\n".join(out) + "\n"


def cargo(*args):
    # Not incremental: the file is only touched between the runs, and an incremental build
    # would take nearly everything from its cache.
    env = dict(
        os.environ,
        CARGO_TARGET_DIR=os.path.join(SCRATCH, "target"),
        CARGO_INCREMENTAL="0",
    )
    started = time.monotonic()
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    subprocess.run(
        ["nice", "-n", "10", "cargo", *args, "-j", "4", "--quiet"],
        cwd=SCRATCH,
        env=env,
        check=True,
    )
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime
    return time.monotonic() - started, cpu


def main():
    os.makedirs(os.path.join(SCRATCH, "src"), exist_ok=True)
    with open(os.path.join(SCRATCH, "Cargo.toml"), "w") as f:
        f.write(
            '[package]\nname = "fakes-compile-time"\nversion = "0.0.0"\nedition = "2024"\n\n'
            '[dependencies]\nmusivum-fakes = { path = "%s/crates/musivum-fakes" }\n\n[workspace]\n'
            % REPO
        )
    lib = os.path.join(SCRATCH, "src", "lib.rs")
    times = {}
    for fake in (False, True):
        with open(lib, "w") as f:
            f.write(lib_rs(fake))
        # Once to have the dependencies built, then the crate alone, five times each.
        cargo("build", "--tests")
        for step, args in (("check", ["check", "--tests"]), ("build", ["build", "--tests"])):
            runs = []
            for _ in range(5):
                os.utime(lib)
                runs.append(cargo(*args))
            times[(fake, step)] = [min(run[k] for run in runs) for k in (0, 1)]
    cargo("test")
    print("a trait of %d methods, not incremental, best of 5:" % N)
    for step in ("check", "build"):
        for k, kind in ((0, "by the clock"), (1, "processor time")):
            without, with_fake = times[(False, step)][k], times[(True, step)][k]
            print(
                "  cargo %s --tests, %s: %.2f s without the fake, %.2f s with it: %.2f s for the fake, %.1f ms per method"
                % (
                    step,
                    kind,
                    without,
                    with_fake,
                    with_fake - without,
                    (with_fake - without) * 1000 / N,
                )
            )


main()
