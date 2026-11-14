#!/usr/bin/env python3
# What changed upstream, between the pinned version of LiveKit (upstream.toml) and a newer
# one, in the files musivum's Rust files are ported from. docs/PORTING.md ("Following
# upstream") has the procedure this is a step of. Run it as scripts/upstream-diff.sh.
#
#   scripts/upstream-diff.sh [--from <tag>] <new-tag>
#       Fetches LiveKit at the pinned tag (or <tag>) and at <new-tag>, shallow, into
#       target/upstream/livekit.git, and the Go modules whose version moved between the two
#       go.mod files into target/upstream/gomod (as `go mod download` does). Reads the header of
#       every Rust file ("// Ported from ..."), and writes into target/upstream/<new-tag>/:
#       report.md, summary.md (the report's beginning, for an issue), diffs/ (per Rust file
#       the upstream diff of its origin files, and of the .proto files) and upstream.toml (the
#       pin moved to <new-tag>).
#   scripts/upstream-diff.sh --bump-unchanged <new-tag>
#       Does the same, then rewrites the version in each header whose origin files did not
#       change between the pin and <new-tag>, and in no other.
#
# Python's standard library only (run with `python3 -I`); needs git, and go for modules.

import argparse
import datetime
import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORK = ROOT / "target" / "upstream"
PROTOBUFS = "crates/musivum-protocol/protobufs"

# The packages of LiveKit's server that musivum ports (docs/PORTING.md): a file that is new in
# one of them gets a decision, ported or left out. In LiveKit's modules, and in LiveKit's other
# directories, the directories that have a ported file count as ported.
LIVEKIT_PACKAGES = (
    "pkg/sfu/",
    "pkg/rtc/",
    "pkg/service/",
    "pkg/routing/",
    "pkg/telemetry/",
    "pkg/config/",
)

LIVEKIT = "livekit"  # the key of LiveKit's own repository among the sources

# ---------------------------------------------------------------------------------------------
# Headers

# A Go module version: v1.13.11, v1.53.1-0.20261023175527-46332a036e25, v4.2.18-warp.7,
# v8.1.3+incompatible.
VERSION = r"(?<![\w./-])v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]*[0-9A-Za-z])?"

# A header begins a comment line (`//`, or `//!` for a module's documentation) with one of
# these, and runs to the end of its first sentence, or to the line that starts with
# "Copyright", "Licensed" or "Changes:", whichever comes first.
TRIGGER = re.compile(
    r"^(?P<prefix>[ \t]*(?P<marker>//!?)[ ]?)(?P<text>(?:Ported from|Ported in part from|Follows) .*)$"
)
LEAD = re.compile(r"(?:Ported from|Ported in part from|Follows) ")
COMMENT = re.compile(r"^[ \t]*(?P<marker>//!?)(?!/)[ ]?")
BLOCK_END = re.compile(r"^(?:Copyright|Licensed|Changes:|$)")
SENTENCE_END = re.compile(r"\.(?=\s|$)")

# Where a header names a source. Each form is followed by what makes it a source rather than
# a word of the sentence: "LiveKit, pkg/...", "pion/rtp, ...", "mackerelio/go-osstat at v0.2.8",
# "pion/turn v5.0.13", "psrpc v0.8.2's errors.go", "twitchtv/twirp's protoc-gen-twirp v8.1.3".
SOURCE = re.compile(
    r"(?<![\w/.-])(?:"
    r"(?P<webrtc>pion/webrtc) \(as LiveKit builds it: [\w./-]+ (?P<webrtc_version>"
    + VERSION
    + r")\)"
    r"|(?P<alias>LiveKit's protocol|LiveKit's mediatransportutil|LiveKit)(?=,| at v)"
    r"|(?P<alias_of_protocol>the protocol's)(?= )"
    r"|(?P<alias_bare>psrpc|Twirp)(?= v\d)"
    r"|(?P<path>(?:pion|livekit|frostbyte73|jxskiss|mackerelio|golang-jwt|twitchtv)/[\w.-]+?"
    r"|gopkg\.in/[\w.-]+?)(?<!\.go)(?=,| at v| v\d|'s )"
    r")"
)
ALIASES = {
    "LiveKit": LIVEKIT,
    "LiveKit's protocol": "livekit/protocol",
    "the protocol's": "livekit/protocol",
    "LiveKit's mediatransportutil": "livekit/mediatransportutil",
    "psrpc": "livekit/psrpc",
    "Twirp": "twitchtv/twirp",
}
GO_FILE = re.compile(r"(?<![\w./-])[\w.-]+(?:/[\w.-]+)*\.go(?![\w.])")
DIRECTORY = re.compile(r"(?<![\w./-])[\w-]+(?:/[\w.-]+)+")


@dataclass
class Clause:
    """One source a header names, with the files of it and the version."""

    source: str  # LIVEKIT, "owner/name" of a module on GitHub, or "gopkg.in/..."
    version: str | None
    span: tuple[int, int] | None  # where the version is written, as offsets in the file
    files: list[str] = field(default_factory=list)
    dirs: list[str] = field(default_factory=list)  # a package named as a whole


@dataclass
class Header:
    rust: str  # the Rust file, relative to the repository
    line: int  # 1-based
    text: str  # the header's sentence, without the comment markers
    clauses: list[Clause] = field(default_factory=list)
    problem: str | None = None


def parse_headers(rust: str, content: str) -> list[Header]:
    """Every header in a Rust file: the one at its top, and those of sections ported apart."""
    lines = content.split("\n")
    starts, at = [], 0
    for line in lines:
        starts.append(at)
        at += len(line) + 1
    headers, i = [], 0
    while i < len(lines):
        m = TRIGGER.match(lines[i])
        if not m:
            i += 1
            continue
        chars: list[str] = []
        offsets: list[int] = []  # the offset in the file of each character of the text
        j = i
        while j < len(lines):
            if j == i:
                body = m.start("text")
            else:
                c = COMMENT.match(lines[j])
                if not c or c["marker"] != m["marker"]:
                    break
                body = c.end()
                if BLOCK_END.match(lines[j][body:]) or TRIGGER.match(lines[j]):
                    break
                chars.append(" ")
                offsets.append(-1)
            for k, ch in enumerate(lines[j][body:].rstrip()):
                chars.append(ch)
                offsets.append(starts[j] + body + k)
            j += 1
        text = "".join(chars)
        end = SENTENCE_END.search(text)
        if end:
            text = text[: end.start()]
        headers.append(parse_sentence(rust, i + 1, text, offsets))
        i = j
    return headers


def parse_sentence(rust: str, line: int, text: str, offsets: list[int]) -> Header:
    header = Header(rust, line, text)
    lead = LEAD.match(text)
    sources = list(SOURCE.finditer(text))
    if not sources or sources[0].start() != lead.end():
        header.problem = "it does not begin with a source this script knows"
        return header
    for n, s in enumerate(sources):
        end = sources[n + 1].start() if n + 1 < len(sources) else len(text)
        body_at = s.end()
        body = text[body_at:end]
        if s["webrtc"]:
            key = "pion/webrtc"
            v_at, version = s.start("webrtc_version"), s["webrtc_version"]
        else:
            name = s["alias"] or s["alias_of_protocol"] or s["alias_bare"] or s["path"]
            key = ALIASES.get(name, name)
            v = re.search(VERSION, body)
            v_at, version = (body_at + v.start(), v.group()) if v else (None, None)
        clause = Clause(key, version, None)
        if v_at is not None:
            clause.span = (offsets[v_at], offsets[v_at + len(version) - 1] + 1)
        else:
            # "and the server options of the protocol's utils/xtwirp": the version said before.
            earlier = [c for c in header.clauses if c.source == key and c.version]
            if not earlier:
                header.problem = f"it names no version of {name}"
                return header
            clause.version = earlier[-1].version
        directory = ""
        for f in GO_FILE.finditer(body):
            path = f.group()
            if "/" in path:
                directory = path.rsplit("/", 1)[0]
            elif directory:
                # "utils/hwstats/cpu.go, cpu_all.go and cpu_null.go"
                path = f"{directory}/{path}"
            clause.files.append(path)
        rest = GO_FILE.sub(" ", body)
        clause.dirs = [d.group().rstrip(".") for d in DIRECTORY.finditer(rest)]
        header.clauses.append(clause)
    if not any(c.files or c.dirs for c in header.clauses):
        header.problem = "it names no file"
    return header


def read_headers() -> list[Header]:
    out = run(["git", "-C", str(ROOT), "ls-files", "-z", "--", "*.rs"])
    headers = []
    for rust in sorted(p for p in out.split("\0") if p):
        content = (ROOT / rust).read_text(encoding="utf-8")
        headers.extend(parse_headers(rust, content))
    return headers


# ---------------------------------------------------------------------------------------------
# Sources: LiveKit at a tag, a Go module at a version


def run(cmd: list[str], cwd: Path | None = None, env: dict | None = None) -> str:
    r = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True)
    if r.returncode != 0:
        sys.exit(f"upstream-diff: {' '.join(cmd)} failed:\n{r.stderr.decode(errors='replace')}")
    return r.stdout.decode()


class Tree:
    """The files of one version of a source, by path."""

    label: str

    def has(self, path: str) -> bool:
        return path in self.files

    def is_dir(self, path: str) -> bool:
        return any(f.startswith(path + "/") for f in self.files)

    def same(self, other: "Tree", path: str) -> bool:
        return self.digest(path) == other.digest(path)


class GitTree(Tree):
    def __init__(self, git: Path, tag: str):
        self.git, self.tag, self.label = git, tag, f"livekit@{tag}"
        self.files: dict[str, str] = {}
        for entry in run(["git", "-C", str(git), "ls-tree", "-r", "-z", tag]).split("\0"):
            if entry:
                meta, path = entry.split("\t", 1)
                _, kind, obj = meta.split()
                if kind == "blob":
                    self.files[path] = obj

    def digest(self, path: str) -> str:
        return self.files[path]

    def read(self, path: str) -> bytes:
        r = subprocess.run(
            ["git", "-C", str(self.git), "cat-file", "blob", self.files[path]], capture_output=True
        )
        return r.stdout


class DirTree(Tree):
    def __init__(self, root: Path, label: str):
        self.root, self.label = root, label
        self.files: dict[str, str | None] = {
            p.relative_to(root).as_posix(): None for p in root.rglob("*") if p.is_file()
        }

    def digest(self, path: str) -> str:
        if self.files[path] is None:
            self.files[path] = hashlib.sha256(self.read(path)).hexdigest()
        return self.files[path]

    def read(self, path: str) -> bytes:
        return (self.root / path).read_bytes()


class LiveKitRepository:
    """A bare repository with LiveKit's tags fetched one commit deep."""

    def __init__(self, url: str):
        self.url, self.git = url, WORK / "livekit.git"
        if not (self.git / "HEAD").exists():
            self.git.mkdir(parents=True, exist_ok=True)
            run(["git", "init", "--quiet", "--bare", str(self.git)])
        self.trees: dict[str, GitTree] = {}

    def fetch(self, tag: str) -> str:
        ref = f"refs/tags/{tag}"
        have = subprocess.run(
            ["git", "-C", str(self.git), "rev-parse", "--verify", "--quiet", ref + "^{commit}"],
            capture_output=True,
        )
        if have.returncode != 0:
            print(f"fetching LiveKit {tag}", file=sys.stderr)
            run(["git", "-C", str(self.git), "fetch", "--quiet", "--depth", "1", "--no-tags"]
                + [self.url, f"+{ref}:{ref}"])
        return run(["git", "-C", str(self.git), "rev-parse", ref + "^{commit}"]).strip()

    def tree(self, tag: str) -> GitTree:
        if tag not in self.trees:
            self.trees[tag] = GitTree(self.git, tag)
        return self.trees[tag]

    def renames(self, old: str, new: str) -> tuple[dict[str, str], set[str]]:
        """What git sees as renamed (old path to new path), and what was added."""
        out = run(["git", "-C", str(self.git), "diff", "--name-status", "-M", "-z", old, new])
        tokens, renamed, added, i = out.split("\0"), {}, set(), 0
        while i < len(tokens) and tokens[i]:
            status = tokens[i]
            if status[0] in "RC":
                if status[0] == "R":
                    renamed[tokens[i + 1]] = tokens[i + 2]
                else:
                    added.add(tokens[i + 2])
                i += 3
            else:
                if status[0] == "A":
                    added.add(tokens[i + 1])
                i += 2
        return renamed, added


class GoModules:
    """Go modules as `go mod download` fetches them, into target/upstream/gomod."""

    cache = WORK / "gomod"

    def __init__(self):
        self.trees: dict[tuple[str, str], DirTree] = {}

    def tree(self, path: str, version: str) -> DirTree:
        if (path, version) not in self.trees:
            print(f"fetching {path}@{version}", file=sys.stderr)
            env = dict(
                os.environ,
                GOMODCACHE=str(self.cache),
                GOFLAGS="-modcacherw",
                GOTOOLCHAIN="local",
                GOWORK="off",
            )
            try:
                out = run(["go", "mod", "download", "-json", f"{path}@{version}"], WORK, env)
            except FileNotFoundError:
                sys.exit("upstream-diff: go is needed to fetch the Go modules that moved")
            info = json.loads(out)
            self.trees[(path, version)] = DirTree(Path(info["Dir"]), f"{path}@{version}")
        return self.trees[(path, version)]


@dataclass
class Module:
    required: str  # the version go.mod requires
    path: str  # what it is built from: the module, or what a replace directive puts there
    version: str | None  # None: replaced by a directory


def parse_gomod(text: str) -> dict[str, Module]:
    """The modules a go.mod names, with its replace directives applied."""
    required: dict[str, str] = {}
    replaced: dict[tuple[str, str | None], tuple[str, str | None]] = {}
    block = None
    for raw in text.splitlines():
        line = raw.split("//", 1)[0].strip()
        if not line:
            continue
        if line == ")":
            block = None
            continue
        words = line.split()
        if words[0] in ("require", "replace", "exclude", "retract", "tool", "ignore", "godebug"):
            if words[1:] == ["("]:
                block = words[0]
                continue
            kind, words = words[0], words[1:]
        elif block:
            kind = block
        else:
            continue
        if kind == "require" and len(words) >= 2:
            required[words[0]] = words[1]
        elif kind == "replace" and "=>" in words:
            arrow = words.index("=>")
            old, new = words[:arrow], words[arrow + 1 :]
            replaced[(old[0], old[1] if len(old) > 1 else None)] = (
                new[0],
                new[1] if len(new) > 1 else None,
            )
    modules = {}
    for path, version in required.items():
        to = replaced.get((path, version)) or replaced.get((path, None))
        if to:
            modules[path] = Module(version, to[0], to[1])
        else:
            modules[path] = Module(version, path, version)
    return modules


def bare(version: str | None) -> str | None:
    """A version as a header writes it: v8.1.3+incompatible is v8.1.3."""
    return version.removesuffix("+incompatible") if version else version


def module_of(key: str, version: str | None, *gomods: dict[str, Module]) -> str | None:
    """The module path a source of a header is, by LiveKit's go.mod files."""
    for gomod in gomods:
        candidates = [
            p
            for p in gomod
            if p == key
            or p == f"github.com/{key}"
            or re.fullmatch(re.escape(f"github.com/{key}") + r"/v\d+", p)
        ]
        if len(candidates) > 1 and version:
            # github.com/pion/turn/v5 for v5.0.13; no suffix for v0 and v1
            major = int(re.match(r"v(\d+)", version)[1])
            suffix = re.compile(rf"/v{major}$" if major >= 2 else r"(?<!/v\d)(?<!/v\d\d)$")
            candidates = [p for p in candidates if suffix.search(p)] or candidates
        if candidates:
            return candidates[0]
    return None


# ---------------------------------------------------------------------------------------------
# What changed


@dataclass
class Change:
    """An origin file (or a package named as a whole) between the two versions."""

    source: str
    path: str
    status: str  # unchanged, changed, added, deleted, renamed, missing, unknown
    renamed_to: str | None = None
    plus: int = 0
    minus: int = 0
    diff: str = ""
    note: str = ""

    @property
    def changed(self) -> bool:
        return self.status != "unchanged"


def unified(old: Tree, new: Tree, old_path: str | None, new_path: str | None) -> tuple[str, int, int]:
    """git's diff of a file between the two versions, and the lines it adds and removes."""
    git = ["git", "-c", "core.quotePath=false", "diff", "--no-color", "--no-ext-diff"]
    if isinstance(old, GitTree) and isinstance(new, GitTree):
        paths = sorted({p for p in (old_path, new_path) if p})
        prefixes = [f"--src-prefix=a/{old.label}/", f"--dst-prefix=b/{new.label}/"]
        cmd, cwd = git[:1] + ["-C", str(old.git)] + git[1:] + ["-M"] + prefixes, None
        cmd += [old.tag, new.tag, "--"] + paths
    else:
        # In the module cache the paths are github.com/owner/name@version/file: a good label.
        cwd = GoModules.cache

        def name(tree, path):
            if path is None or not tree.has(path):
                return "/dev/null"
            return (tree.root / path).relative_to(cwd).as_posix()

        cmd = git + ["--no-index", "--", name(old, old_path), name(new, new_path)]
    r = subprocess.run(cmd, cwd=cwd, capture_output=True)
    if r.returncode not in (0, 1):
        sys.exit(f"upstream-diff: {' '.join(cmd)} failed:\n{r.stderr.decode(errors='replace')}")
    diff = r.stdout.decode("utf-8", errors="replace")
    plus = minus = 0
    in_hunk = False
    for line in diff.splitlines():
        if line.startswith("diff --git"):
            in_hunk = False
        elif line.startswith("@@"):
            in_hunk = True
        elif in_hunk and line.startswith("+"):
            plus += 1
        elif in_hunk and line.startswith("-"):
            minus += 1
    return diff, plus, minus


@dataclass
class Side:
    """One source between the two versions."""

    key: str
    module: str | None  # the module path in go.mod; None for LiveKit
    old_version: str | None
    new_version: str | None
    pinned_version: str | None
    old: Tree | None = None
    new: Tree | None = None
    renamed: dict[str, str] = field(default_factory=dict)
    added: set[str] = field(default_factory=set)

    @property
    def moved(self) -> bool:
        return self.old_version != self.new_version

    @property
    def label(self) -> str:
        return "LiveKit" if self.key == LIVEKIT else self.module or self.key


class Analysis:
    def __init__(self, new_tag: str, from_tag: str | None):
        config = tomllib.loads((ROOT / "upstream.toml").read_text())
        livekit = config["livekit"]
        self.pin_tag, self.pin_commit = livekit["tag"], livekit["commit"]
        self.pinned_modules = livekit.get("modules", {})
        self.old_tag, self.new_tag = from_tag or self.pin_tag, new_tag
        self.repo = LiveKitRepository(livekit["repository"])
        tags = dict.fromkeys([self.pin_tag, self.old_tag, new_tag])
        self.commits = {t: self.repo.fetch(t) for t in tags}
        self.gomod = {
            t: parse_gomod(self.repo.tree(t).read("go.mod").decode()) for t in self.commits
        }
        self.modules = GoModules()
        self.sides: dict[str, Side] = {}
        self.changes: dict[tuple[str, str], Change] = {}
        self.headers = read_headers()
        self.problems: list[tuple[Header, str]] = [(h, h.problem) for h in self.headers if h.problem]
        self.stale: list[tuple[Header, Clause, str]] = []  # headers that do not name the pin
        for header in self.headers:
            if header.problem:
                continue
            for clause in header.clauses:
                side = self.side(clause.source, clause.version)
                if side is None:
                    why = f"{clause.source} is not a module of LiveKit's go.mod"
                    self.problems.append((header, why))
                    continue
                if bare(clause.version) != bare(side.pinned_version):
                    self.stale.append((header, clause, side.pinned_version or "?"))
                for path in clause.files:
                    self.change(side, path)
                for path in clause.dirs:
                    self.change_dir(side, path)

    # The sources

    def side(self, key: str, version: str | None) -> Side | None:
        if key in self.sides:
            return self.sides[key]
        if key == LIVEKIT:
            side = Side(key, None, self.old_tag, self.new_tag, self.pin_tag)
            side.old, side.new = self.repo.tree(self.old_tag), self.repo.tree(self.new_tag)
            side.renamed, side.added = self.repo.renames(self.old_tag, self.new_tag)
        else:
            gomods = [self.gomod[self.pin_tag], self.gomod[self.old_tag], self.gomod[self.new_tag]]
            path = module_of(key, version, *gomods)
            if path is None:
                return None
            pin = self.gomod[self.pin_tag].get(path)
            old = self.gomod[self.old_tag].get(path)
            new = self.gomod[self.new_tag].get(path)
            side = Side(
                key,
                path,
                old.version if old else None,
                new.version if new else None,
                pin.version if pin else None,
            )
            if side.moved and old and new and old.version and new.version:
                side.old = self.modules.tree(old.path, old.version)
                side.new = self.modules.tree(new.path, new.version)
                gone = set(side.old.files) - set(side.new.files)
                side.added = set(side.new.files) - set(side.old.files)
                by_name: dict[str, list[str]] = {}
                for f in side.added:
                    by_name.setdefault(f.rsplit("/", 1)[-1], []).append(f)
                for f in gone:
                    if len(by_name.get(f.rsplit("/", 1)[-1], [])) == 1:
                        side.renamed[f] = by_name[f.rsplit("/", 1)[-1]][0]
        self.sides[key] = side
        return side

    def change(self, side: Side, path: str) -> Change:
        if (side.key, path) in self.changes:
            return self.changes[(side.key, path)]
        change = Change(side.label, path, "unchanged")
        if not side.moved:
            pass  # the same version on both sides: nothing to fetch, nothing changed
        elif side.old is None or side.new is None:
            change.status = "unknown"
            change.note = (
                f"the module is {side.old_version} at {self.old_tag} "
                f"and {side.new_version} at {self.new_tag}"
            )
        elif not side.old.has(path):
            change.status = "added" if side.new.has(path) else "missing"
        elif not side.new.has(path):
            change.status = "renamed" if path in side.renamed else "deleted"
            change.renamed_to = side.renamed.get(path)
        elif not side.old.same(side.new, path):
            change.status = "changed"
        if change.status in ("added", "changed", "renamed"):
            change.diff, change.plus, change.minus = unified(
                side.old, side.new, path if side.old.has(path) else None, change.renamed_to or path
            )
        self.changes[(side.key, path)] = change
        return change

    def change_dir(self, side: Side, path: str) -> Change:
        if (side.key, path + "/") in self.changes:
            return self.changes[(side.key, path + "/")]
        change = Change(side.label, path + "/", "unchanged")
        if side.moved and side.old and side.new:
            if not side.old.is_dir(path):
                change.status = "missing"
            else:
                inside = sorted(
                    f
                    for f in set(side.old.files) | set(side.new.files)
                    if f.rsplit("/", 1)[0] == path and f.endswith(".go")
                )
                for f in inside:
                    before, after = side.old.has(f), side.new.has(f)
                    if not (before and after and side.old.same(side.new, f)):
                        diff, plus, minus = unified(
                            side.old, side.new, f if before else None, f if after else None
                        )
                        change.diff += diff
                        change.plus += plus
                        change.minus += minus
                if change.diff:
                    change.status = "changed"
        elif side.moved:
            change.status = "unknown"
        self.changes[(side.key, path + "/")] = change
        return change

    # What a header and a Rust file come to

    def header_changes(self, header: Header) -> list[Change]:
        out = []
        for clause in header.clauses:
            side = self.sides.get(clause.source)
            if side is None:
                continue
            out += [self.changes[(side.key, f)] for f in clause.files]
            out += [self.changes[(side.key, d + "/")] for d in clause.dirs]
            if not clause.files and not clause.dirs and side.moved:
                note = f"{side.old_version} to {side.new_version}: the header names no file of it"
                out.append(Change(side.label, "(the module as a whole)", "unknown", note=note))
        return out

    def unchanged(self, header: Header) -> bool:
        return not header.problem and not any(c.changed for c in self.header_changes(header)) and all(
            clause.source in self.sides for clause in header.clauses
        )

    def by_rust_file(self) -> dict[str, list[Header]]:
        files: dict[str, list[Header]] = {}
        for header in self.headers:
            files.setdefault(header.rust, []).append(header)
        return files

    def origin_dirs(self) -> dict[str, set[str]]:
        dirs: dict[str, set[str]] = {}
        for header in self.headers:
            for clause in header.clauses:
                for f in clause.files:
                    dirs.setdefault(clause.source, set()).add(f.rsplit("/", 1)[0] if "/" in f else "")
                for d in clause.dirs:
                    dirs.setdefault(clause.source, set()).add(d)
        return dirs

    def new_files(self) -> list[tuple[str, str]]:
        """Files new upstream in the packages musivum ports, that no header names."""
        named = {(c.source, f) for h in self.headers for c in h.clauses for f in c.files}
        dirs = self.origin_dirs()
        out = []
        for key, side in sorted(self.sides.items()):
            if side.old is None or side.new is None or not side.moved:
                continue
            targets = set(side.renamed.values())
            for f in sorted(side.added - targets):
                if not f.endswith(".go") or (key, f) in named:
                    continue
                directory = f.rsplit("/", 1)[0] if "/" in f else ""
                ported = key == LIVEKIT and f.startswith(LIVEKIT_PACKAGES)
                if ported or directory in dirs.get(key, set()):
                    out.append((side.label, f))
        return out

    def protos(self) -> tuple[list[Change], list[str], list[str]]:
        """The copied .proto files that changed, the .proto files new upstream, and the copies
        that are not what the pinned version of the protocol has."""
        side = self.side("livekit/protocol", None)
        copies = sorted(
            p.relative_to(ROOT / PROTOBUFS).as_posix() for p in (ROOT / PROTOBUFS).rglob("*.proto")
        )
        changed, new, differing = [], [], []
        if side and side.moved and side.old and side.new:
            for rel in copies:
                c = self.change(side, f"protobufs/{rel}")
                if c.changed:
                    changed.append(c)
            new = sorted(
                f for f in side.added if f.startswith("protobufs/") and f.endswith(".proto")
            )
            pinned = self.gomod[self.pin_tag].get(side.module)
            tree = self.modules.tree(pinned.path, pinned.version) if pinned and pinned.version else None
            for rel in copies:
                ours = (ROOT / PROTOBUFS / rel).read_bytes()
                if tree and (not tree.has(f"protobufs/{rel}") or tree.read(f"protobufs/{rel}") != ours):
                    differing.append(rel)
        return changed, new, differing

    def mentions(self) -> tuple[list[tuple[str, int, str]], dict[str, int]]:
        """Where the old versions are named outside the headers: lines of Rust files, and the
        number of mentions in other files of the repository."""
        versions = {self.old_tag}
        versions |= {bare(s.old_version) for s in self.sides.values() if s.moved and s.old_version}
        pattern = re.compile(
            r"(?<![\w.])(?:"
            + "|".join(re.escape(v) for v in sorted(versions))
            + r")(?![0-9A-Za-z]|[.+-][0-9A-Za-z])"
        )
        spans = {}
        for header in self.headers:
            for clause in header.clauses:
                if clause.span:
                    spans.setdefault(header.rust, set()).add(clause.span[0])
        lines, others = [], {}
        for path in run(["git", "-C", str(ROOT), "ls-files", "-z"]).split("\0"):
            if not path or not (ROOT / path).is_file():
                continue
            try:
                content = (ROOT / path).read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            found = [m for m in pattern.finditer(content) if m.start() not in spans.get(path, ())]
            if not found:
                continue
            if path.endswith(".rs"):
                for m in found:
                    n = content.count("\n", 0, m.start())
                    lines.append((path, n + 1, content.split("\n")[n].strip()))
            else:
                others[path] = len(found)
        return lines, others

    def new_pin(self) -> str:
        """upstream.toml with the pin moved to the new tag."""
        text = (ROOT / "upstream.toml").read_text()
        text = re.sub(r'(?m)^tag = "[^"]*"', f'tag = "{self.new_tag}"', text, count=1)
        commit = self.commits[self.new_tag]
        text = re.sub(r'(?m)^commit = "[^"]*"', f'commit = "{commit}"', text, count=1)
        for path in self.pinned_modules:
            new = self.gomod[self.new_tag].get(path)
            line = re.compile(r'(?m)^"' + re.escape(path) + r'" = \{ version = "[^"]*"')
            if new is None:
                text = line.sub(lambda m: "# not in LiveKit's go.mod any more: " + m.group(), text)
                continue
            text = line.sub(f'"{path}" = {{ version = "{new.version}"', text)
            replaced = re.compile(r'(?m)^("' + re.escape(path) + r'" = \{.*?), replaced_by = "[^"]*"')
            text = replaced.sub(lambda m: m[1], text)
            if new.path != path:
                text = line.sub(lambda m: f'{m.group()}, replaced_by = "{new.path}"', text)
        return text


# ---------------------------------------------------------------------------------------------
# The report


def size(plus: int, minus: int) -> str:
    return f"+{plus} −{minus}"


def describe(c: Change) -> str:
    if c.status == "renamed":
        return f"renamed to `{c.renamed_to}`, {size(c.plus, c.minus)}"
    if c.status in ("changed", "added"):
        return f"{c.status}, {size(c.plus, c.minus)}"
    return c.status + (f" ({c.note})" if c.note else "")


def write_report(a: Analysis) -> tuple[str, str]:
    out_dir = WORK / a.new_tag
    if out_dir.exists():
        for p in sorted(out_dir.rglob("*"), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
    (out_dir / "diffs").mkdir(parents=True, exist_ok=True)

    files = a.by_rust_file()
    changed_files: list[tuple[str, list[Change]]] = []
    unchanged_files: list[str] = []
    for rust, headers in files.items():
        if any(h.problem for h in headers):
            continue
        changes = [c for h in headers for c in a.header_changes(h) if c.changed]
        if changes:
            seen, unique = set(), []
            for c in changes:
                if (c.source, c.path) not in seen:
                    seen.add((c.source, c.path))
                    unique.append(c)
            changed_files.append((rust, unique))
            diff = "".join(c.diff for c in unique)
            if diff:
                target = out_dir / "diffs" / (rust + ".diff")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(diff)
        elif all(a.unchanged(h) for h in headers):
            unchanged_files.append(rust)
    changed_files.sort(key=lambda f: (-sum(c.plus + c.minus for c in f[1]), f[0]))
    origins_changed = {(c.source, c.path): c for _, cs in changed_files for c in cs}
    gone = [c for c in origins_changed.values() if c.status in ("deleted", "renamed")]
    new_files = a.new_files()
    protos_changed, protos_new, protos_differing = a.protos()
    for c in protos_changed:
        target = out_dir / "diffs" / (c.path + ".diff")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(c.diff)
    lines, others = a.mentions()
    (out_dir / "upstream.toml").write_text(a.new_pin())

    title = f"LiveKit {a.new_tag}"
    head = [f"# {title}", ""]
    head.append(
        f"What changed upstream between LiveKit {a.old_tag} and {a.new_tag} "
        f"(`{a.commits[a.new_tag][:12]}`) in the files musivum is ported from, by "
        f"`scripts/upstream-diff.sh` on {datetime.date.today().isoformat()}. "
        f"The pin (`upstream.toml`) is {a.pin_tag}."
    )
    if a.old_tag != a.pin_tag:
        head.append(f"Compared from {a.old_tag} (`--from`), not from the pin.")
    plus = sum(c.plus for c in origins_changed.values())
    minus = sum(c.minus for c in origins_changed.values())
    head += [
        "",
        "## Summary",
        "",
        f"- Rust files with a header: {len(files)} ({len(a.headers)} headers).",
        f"- Rust files whose origins changed: **{len(changed_files)}** "
        f"({len(origins_changed)} origin files, {size(plus, minus)} lines).",
        f"- Origin files deleted or renamed: {len(gone)}.",
        f"- New upstream files in the ported packages: {len(new_files)}.",
        f"- Copied `.proto` files that changed: {len(protos_changed)}; new upstream: {len(protos_new)}.",
        f"- Headers it could not parse: {len(a.problems)}; headers that do not name the pinned "
        f"version: {len(a.stale)}.",
        f"- Rust files whose origins did not change: {len(unchanged_files)} "
        f"(`scripts/upstream-diff.sh --bump-unchanged {a.new_tag}` moves their headers).",
        "",
        "## Modules",
        "",
        f"| Module | {a.old_tag} | {a.new_tag} | |",
        "|---|---|---|---|",
    ]
    listed = set()
    gomod_old, gomod_new = a.gomod[a.old_tag], a.gomod[a.new_tag]
    module_rows = list(a.pinned_modules) + sorted(
        s.module for s in a.sides.values() if s.module and s.module not in a.pinned_modules
    )
    for path in module_rows:
        if path in listed:
            continue
        listed.add(path)
        old, new = gomod_old.get(path), gomod_new.get(path)
        def show(m: Module | None) -> str:
            if m is None:
                return "—"
            return m.version if m.path == path else f"{m.path} {m.version}"

        moved = "**moved**" if (old and old.version) != (new and new.version) else ""
        head.append(f"| `{path}` | {show(old)} | {show(new)} | {moved} |")
    head.append("")

    def changed_table(limit: int | None) -> list[str]:
        rows = ["| Rust file | Origin file | Change |", "|---|---|---|"]
        for n, (rust, cs) in enumerate(changed_files):
            if limit is not None and n == limit:
                rows.append(f"| … and {len(changed_files) - limit} more | | |")
                break
            for k, c in enumerate(cs):
                rows.append(
                    f"| {f'`{rust}`' if k == 0 else ''} | {c.source} `{c.path}` | {describe(c)} |"
                )
        return rows

    summary = head + ["## Rust files whose origins changed", "", "Largest first."]
    summary += [""] + changed_table(40) + [""]
    if new_files:
        summary += ["## New upstream files in the ported packages", ""]
        summary += [f"- {s} `{f}`" for s, f in new_files[:40]]
        if len(new_files) > 40:
            summary.append(f"- … and {len(new_files) - 40} more")
        summary.append("")
    if gone:
        summary += ["## Origin files deleted or renamed", ""]
        summary += [f"- {c.source} `{c.path}`: {describe(c)}" for c in gone] + [""]
    if protos_changed or protos_new:
        summary += ["## .proto files", ""]
        summary += [f"- `{c.path}`: {describe(c)}" for c in protos_changed]
        summary += [f"- `{p}`: new upstream, not copied" for p in protos_new] + [""]
    if a.problems:
        summary += ["## Headers it could not parse", ""]
        summary += [f"- `{h.rust}:{h.line}`: {why}" for h, why in a.problems] + [""]

    report = head + [
        "## Rust files whose origins changed",
        "",
        "Largest first. The upstream diff of each Rust file's origin files is in "
        "`diffs/<Rust file>.diff`.",
        "",
    ]
    report += changed_table(None) + [""]
    report += ["## Origin files deleted or renamed", ""]
    report += [
        f"- {c.source} `{c.path}`: {describe(c)}; ported in "
        + ", ".join(f"`{r}`" for r, cs in changed_files if c in cs)
        for c in gone
    ] or ["None."]
    report += ["", "## New upstream files in the ported packages", ""]
    report += [
        "Files that are new in a package musivum ports and that no header names: each needs a "
        "decision, ported or left out.",
        "",
    ]
    report += [f"- {s} `{f}`" for s, f in new_files] or ["None."]
    report += ["", "## .proto files", ""]
    protocol = a.sides.get("livekit/protocol")
    if protocol and protocol.moved:
        report.append(
            f"The protocol moves from {protocol.old_version} to {protocol.new_version}. "
            f"`{PROTOBUFS}` has copies of its `.proto` files (its README says how they are moved)."
        )
        report.append("")
        report += [f"- `{c.path}`: {describe(c)} (`diffs/{c.path}.diff`)" for c in protos_changed] or [
            "- None of the copied files changed."
        ]
        report += [f"- `{p}`: new upstream, not copied" for p in protos_new]
        if protos_differing:
            report += ["", f"Copies that are not what the protocol at the pin has: "]
            report += [f"- `{PROTOBUFS}/{p}`" for p in protos_differing]
    else:
        report.append("The protocol did not move.")
    report += ["", "## Headers it could not parse", ""]
    report += [f"- `{h.rust}:{h.line}`: {why}. \"{h.text}\"" for h, why in a.problems] or ["None."]
    report += ["", "## Headers that do not name the pinned version", ""]
    report += [
        "These are not moved by `--bump-unchanged`.",
        "",
    ]
    report += [
        f"- `{h.rust}:{h.line}`: {c.source} {c.version}, the pin has {v}" for h, c, v in a.stale
    ] or ["None."]
    missing = [c for c in a.changes.values() if c.status == "missing"]
    if missing:
        report += ["", "Origin files that are not there at " + a.old_tag + ":", ""]
        report += [f"- {c.source} `{c.path}`" for c in missing]
    report += ["", "## Other mentions of the old versions", ""]
    report += [
        f"Where {a.old_tag} or an old version of a module that moved is named outside a header's "
        "version: these are not moved by `--bump-unchanged`.",
        "",
    ]
    report += [f"- `{p}:{n}`: {t}" for p, n, t in lines] or ["- No Rust file."]
    report += [f"- `{p}`: {n} times" for p, n in sorted(others.items())]
    report += ["", "## The pin", ""]
    toml = a.pinned_modules
    report.append(
        f"`target/upstream/{a.new_tag}/upstream.toml` is `upstream.toml` with the pin moved to "
        f"{a.new_tag} (`{a.commits[a.new_tag]}`) and its modules to what that version's go.mod names."
    )
    if a.commits[a.pin_tag] != a.pin_commit:
        report.append(
            f"\n**`upstream.toml` says {a.pin_commit} for {a.pin_tag}; "
            f"the tag is at {a.commits[a.pin_tag]}.**"
        )
    for path, entry in toml.items():
        m = a.gomod[a.pin_tag].get(path)
        if m is None or m.version != entry.get("version"):
            report.append(
                f"\n**`upstream.toml` has {path} {entry.get('version')}; LiveKit's go.mod at "
                f"{a.pin_tag}: {m.version if m else 'not there'}.**"
            )
    report += ["", "## Rust files whose origins did not change", ""]
    report += [f"- `{r}`" for r in sorted(unchanged_files)] or ["None."]
    report.append("")
    (out_dir / "report.md").write_text("\n".join(report))
    (out_dir / "summary.md").write_text("\n".join(summary))
    return title, "\n".join(summary)


# ---------------------------------------------------------------------------------------------
# --bump-unchanged


def bump_unchanged(a: Analysis) -> None:
    edits: dict[str, list[tuple[int, int, str]]] = {}
    moved, kept = 0, []
    for header in a.headers:
        if not a.unchanged(header):
            kept.append(header)
            continue
        changes = []
        for clause in header.clauses:
            side = a.sides[clause.source]
            new = bare(side.new_version)
            if clause.span and side.moved and bare(clause.version) == bare(side.old_version) and new:
                changes.append((clause.span[0], clause.span[1], new))
        if changes:
            edits.setdefault(header.rust, []).extend(changes)
            moved += 1
    for rust, spans in edits.items():
        path = ROOT / rust
        content = path.read_text(encoding="utf-8")
        for start, end, version in sorted(set(spans), reverse=True):
            content = content[:start] + version + content[end:]
        path.write_text(content, encoding="utf-8")
    print(f"moved {moved} headers in {len(edits)} files to {a.new_tag} and its modules' versions")
    if kept:
        print(f"left as they are, {len(kept)} headers whose origins changed or that could not be read:")
        for h in kept:
            print(f"  {h.rust}:{h.line}")


# ---------------------------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="scripts/upstream-diff.sh",
        description="What changed upstream in the files musivum ports from (docs/PORTING.md).",
    )
    parser.add_argument(
        "new_tag", metavar="<new-tag>", help="the LiveKit release to compare with, e.g. v1.13.12"
    )
    parser.add_argument("--from", dest="from_tag", metavar="<tag>",
                        help="compare from this tag instead of the pinned one (a trial)")
    parser.add_argument("--bump-unchanged", action="store_true",
                        help="move the headers whose origins did not change to <new-tag>")
    args = parser.parse_args()
    if args.bump_unchanged and args.from_tag:
        parser.error("--bump-unchanged moves headers from the pin; it takes no --from")
    WORK.mkdir(parents=True, exist_ok=True)
    analysis = Analysis(args.new_tag, args.from_tag)
    title, summary = write_report(analysis)
    print(f"{title}: target/upstream/{args.new_tag}/report.md", file=sys.stderr)
    if args.bump_unchanged:
        bump_unchanged(analysis)
    else:
        print(summary.split("## Rust files whose origins changed")[0].rstrip())


if __name__ == "__main__":
    main()
