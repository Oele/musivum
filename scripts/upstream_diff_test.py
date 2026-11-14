#!/usr/bin/env python3
# Tests of upstream_diff.py's reading of headers and go.mod files, without the network:
# python3 -I scripts/upstream_diff_test.py
#
# One test reads every header in the tree: a header form the parser does not know fails it,
# so that the weekly check never leaves a ported file out without saying so.

import importlib.util
import sys
import unittest
from pathlib import Path

sys.dont_write_bytecode = True  # no __pycache__ in scripts/
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("upstream_diff", HERE / "upstream_diff.py")
ud = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ud)

PROTOCOL = "v1.53.1-0.20261023175527-46332a036e25"

# Headers in the tree that the parser cannot read, by "file:line". None at present.
EXPECTED_UNPARSED: set[str] = set()


def parse(text: str):
    headers = ud.parse_headers("x.rs", text)
    assert len(headers) == 1, headers
    return headers[0]


def clauses(header):
    return [(c.source, c.version, c.files, c.dirs) for c in header.clauses]


class HeaderForms(unittest.TestCase):
    def test_one_file(self):
        h = parse(
            "// Ported from LiveKit, pkg/sfu/rtpmunger.go at v1.13.11.\n"
            "// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.\n"
            "// Changes: translated from Go to Rust; more.go at v9.9.9 is not an origin.\n"
        )
        self.assertIsNone(h.problem)
        self.assertEqual(clauses(h), [("livekit", "v1.13.11", ["pkg/sfu/rtpmunger.go"], [])])

    def test_version_on_the_next_line_and_its_place(self):
        text = (
            "// Ported from LiveKit's protocol, utils/timeoutqueue.go at\n"
            f"// {PROTOCOL}.\n"
            "// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.\n"
        )
        h = parse(text)
        self.assertEqual(clauses(h), [("livekit/protocol", PROTOCOL, ["utils/timeoutqueue.go"], [])])
        start, end = h.clauses[0].span
        self.assertEqual(text[start:end], PROTOCOL)
        self.assertEqual(text.count("\n", 0, start), 1)

    def test_modules_of_pion_and_others(self):
        for text, want in [
            ("// Ported from pion/turn, internal/server/turn.go at v5.0.13.", ("pion/turn", "v5.0.13")),
            ("// Ported from pion/sdp, util.go at v3.0.20.", ("pion/sdp", "v3.0.20")),
            ("// Ported from frostbyte73/core, throttle.go at v0.1.1.", ("frostbyte73/core", "v0.1.1")),
            (
                "// Ported from LiveKit's mediatransportutil, pkg/bucket/bucket.go at\n"
                "// v0.0.0-20261014192331-dd8e31a0adc0.",
                ("livekit/mediatransportutil", "v0.0.0-20261014192331-dd8e31a0adc0"),
            ),
        ]:
            h = parse(text)
            self.assertIsNone(h.problem, text)
            self.assertEqual((h.clauses[0].source, h.clauses[0].version), want)

    def test_files_of_one_directory(self):
        h = parse(
            "// Ported from LiveKit's protocol, utils/hwstats/cpu.go, cpu_all.go and cpu_null.go at\n"
            f"// {PROTOCOL}.\n"
        )
        self.assertEqual(
            h.clauses[0].files,
            ["utils/hwstats/cpu.go", "utils/hwstats/cpu_all.go", "utils/hwstats/cpu_null.go"],
        )

    def test_parts_of_files_and_a_comma_before_the_version(self):
        h = parse(
            "// Ported from LiveKit, pkg/metric/metric_config.go, with the configuration structs of\n"
            "// pkg/metric/metric_timestamper.go, metrics_collector.go and metrics_reporter.go, at v1.13.11.\n"
        )
        self.assertEqual(
            clauses(h),
            [
                (
                    "livekit",
                    "v1.13.11",
                    [
                        "pkg/metric/metric_config.go",
                        "pkg/metric/metric_timestamper.go",
                        "pkg/metric/metrics_collector.go",
                        "pkg/metric/metrics_reporter.go",
                    ],
                    [],
                )
            ],
        )

    def test_version_before_the_files(self):
        h = parse(
            "// Ported from mackerelio/go-osstat at v0.2.8: loadavg/loadavg.go,\n"
            "// cpu/cpu_linux.go and memory/memory_other.go.\n"
        )
        self.assertEqual(
            clauses(h),
            [
                (
                    "mackerelio/go-osstat",
                    "v0.2.8",
                    ["loadavg/loadavg.go", "cpu/cpu_linux.go", "memory/memory_other.go"],
                    [],
                )
            ],
        )

    def test_pion_webrtc_as_livekit_builds_it_and_a_second_source(self):
        h = parse(
            "// Ported from pion/webrtc (as LiveKit builds it: livekit/webrtc-pion v4.2.18-warp.7),\n"
            "// track_local.go and errors.go; `StreamInfo` is from\n"
            "// pion/interceptor, streaminfo.go at v0.1.49 (its `RTPHeaderExtension` is in\n"
            "// `webrtc::interceptor`).\n"
            "// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.\n"
        )
        self.assertEqual(
            clauses(h),
            [
                ("pion/webrtc", "v4.2.18-warp.7", ["track_local.go", "errors.go"], []),
                ("pion/interceptor", "v0.1.49", ["streaminfo.go"], []),
            ],
        )

    def test_several_sources_a_package_and_an_inherited_version(self):
        h = parse(
            "// Ported from LiveKit, pkg/service/twirp.go at v1.13.11; and the server of the room\n"
            "// service from LiveKit's protocol, livekit/livekit_room.twirp.go (and the helpers of the\n"
            "// package's generated code in cloud_replay.twirp.go) at\n"
            f"// {PROTOCOL}, which\n"
            "// twitchtv/twirp's protoc-gen-twirp v8.1.3 generated, with what of Twirp v8.1.3 it uses\n"
            "// (errors.go, server_options.go, context.go) and the server options of the protocol's\n"
            "// utils/xtwirp.\n"
        )
        self.assertIsNone(h.problem)
        self.assertEqual(
            clauses(h),
            [
                ("livekit", "v1.13.11", ["pkg/service/twirp.go"], []),
                (
                    "livekit/protocol",
                    PROTOCOL,
                    ["livekit/livekit_room.twirp.go", "livekit/cloud_replay.twirp.go"],
                    [],
                ),
                ("twitchtv/twirp", "v8.1.3", [], []),
                ("twitchtv/twirp", "v8.1.3", ["errors.go", "server_options.go", "context.go"], []),
                ("livekit/protocol", PROTOCOL, [], ["utils/xtwirp"]),
            ],
        )
        self.assertIsNone(h.clauses[4].span)

    def test_a_module_named_with_its_version(self):
        h = parse(
            "// Ported from LiveKit's protocol, rpc/room.psrpc.go and the topics of rpc/typed_api.go at\n"
            f"// {PROTOCOL}; with what of psrpc v0.8.2's\n"
            "// errors.go and request options the calls here use.\n"
        )
        self.assertEqual(
            clauses(h),
            [
                ("livekit/protocol", PROTOCOL, ["rpc/room.psrpc.go", "rpc/typed_api.go"], []),
                ("livekit/psrpc", "v0.8.2", ["errors.go"], []),
            ],
        )

    def test_ported_in_part_and_follows(self):
        h = parse(
            "// Ported in part from golang-jwt/jwt, the JWT library the original is built on: parser.go,\n"
            "// validator.go and errors.go at v5.3.1.\n"
        )
        self.assertEqual(
            clauses(h), [("golang-jwt/jwt", "v5.3.1", ["parser.go", "validator.go", "errors.go"], [])]
        )
        h = parse(
            "// Follows gopkg.in/yaml.v3 at v3.0.1, decode.go and resolve.go: what that decoder does when\n"
            "// LiveKit reads its configuration into Go values.\n"
            "// Copyright (c) 2011-2019 Canonical Ltd. Licensed under the Apache License, Version 2.0.\n"
        )
        self.assertEqual(clauses(h), [("gopkg.in/yaml.v3", "v3.0.1", ["decode.go", "resolve.go"], [])])

    def test_module_documentation_ends_with_its_first_sentence(self):
        h = parse(
            "//! pion/turn's server.\n"
            "//!\n"
            "//! Ported from pion/turn v5.0.13 (the version LiveKit's `go.mod` names): `server.go`,\n"
            "//! `lt_cred.go` and the internal packages the server uses (`allocation`). Not ported: the\n"
            "//! client (`client.go`, `internal/client`).\n"
        )
        self.assertEqual(clauses(h), [("pion/turn", "v5.0.13", ["server.go", "lt_cred.go"], [])])
        self.assertEqual(h.line, 3)

    def test_a_section_of_a_file(self):
        headers = ud.parse_headers(
            "x.rs",
            "// Ported from LiveKit, pkg/telemetry/prometheus/rooms.go at v1.13.11.\n"
            "\n"
            "mod tests {\n"
            "    // Ported from LiveKit, pkg/telemetry/prometheus/rooms_test.go at v1.13.11: the original's\n"
            "    // two tests. They run one after the other.\n"
            "    static LOCK: () = ();\n"
            "}\n",
        )
        self.assertEqual([h.line for h in headers], [1, 4])
        self.assertEqual(headers[1].clauses[0].files, ["pkg/telemetry/prometheus/rooms_test.go"])

    def test_what_is_not_read_is_said(self):
        h = parse("// Ported from somewhere/else, foo.go at v1.0.0.\n")
        self.assertEqual(h.problem, "it does not begin with a source this script knows")
        h = parse("// Ported from LiveKit, pkg/sfu/foo.go.\n")
        self.assertEqual(h.problem, "it names no version of LiveKit")
        h = parse("// Ported from LiveKit at v1.13.11: what its tests do.\n")
        self.assertEqual(h.problem, "it names no file")

    def test_prose_is_not_a_header(self):
        self.assertEqual(
            ud.parse_headers(
                "x.rs",
                "// The two traits are those of `sfu/bwe/bwe.rs`: that file is ported from LiveKit,\n"
                "// pkg/sfu/bwe/bwe.go at v1.13.11.\n"
                "/// Ported from LiveKit, pkg/sfu/x.go at v1.13.11.\n",
            ),
            [],
        )


class GoMod(unittest.TestCase):
    TEXT = """module github.com/livekit/livekit-server

go 1.25.0

require (
\tgithub.com/livekit/protocol v1.53.1-0.20261023175527-46332a036e25
\tgithub.com/pion/sdp/v3 v3.0.20
\tgithub.com/pion/webrtc/v4 v4.2.18
\tgithub.com/twitchtv/twirp v8.1.3+incompatible
)

require github.com/golang-jwt/jwt/v5 v5.3.1 // indirect

replace github.com/pion/webrtc/v4 => github.com/livekit/webrtc-pion/v4 v4.2.18-warp.7
"""

    def test_requires_and_replaces(self):
        mods = ud.parse_gomod(self.TEXT)
        self.assertEqual(mods["github.com/pion/sdp/v3"].version, "v3.0.20")
        webrtc = mods["github.com/pion/webrtc/v4"]
        self.assertEqual((webrtc.path, webrtc.version), ("github.com/livekit/webrtc-pion/v4", "v4.2.18-warp.7"))
        self.assertEqual(mods["github.com/golang-jwt/jwt/v5"].version, "v5.3.1")
        self.assertEqual(ud.bare(mods["github.com/twitchtv/twirp"].version), "v8.1.3")

    def test_sources_of_headers_are_modules(self):
        mods = ud.parse_gomod(self.TEXT)
        for key, version, path in [
            ("pion/sdp", "v3.0.20", "github.com/pion/sdp/v3"),
            ("pion/webrtc", "v4.2.18-warp.7", "github.com/pion/webrtc/v4"),
            ("golang-jwt/jwt", "v5.3.1", "github.com/golang-jwt/jwt/v5"),
            ("twitchtv/twirp", "v8.1.3", "github.com/twitchtv/twirp"),
            ("livekit/protocol", PROTOCOL, "github.com/livekit/protocol"),
        ]:
            self.assertEqual(ud.module_of(key, version, mods), path)

    def test_the_major_version_decides_between_modules(self):
        mods = ud.parse_gomod(
            "require (\n\tgithub.com/pion/turn/v4 v4.1.1\n\tgithub.com/pion/turn/v5 v5.0.13\n"
            "\tgithub.com/pion/dtls v1.5.4\n\tgithub.com/pion/dtls/v3 v3.0.7\n)\n"
        )
        self.assertEqual(ud.module_of("pion/turn", "v5.0.13", mods), "github.com/pion/turn/v5")
        self.assertEqual(ud.module_of("pion/turn", "v4.1.1", mods), "github.com/pion/turn/v4")
        self.assertEqual(ud.module_of("pion/dtls", "v1.5.4", mods), "github.com/pion/dtls")
        self.assertEqual(ud.module_of("pion/dtls", "v3.0.7", mods), "github.com/pion/dtls/v3")


class TheTree(unittest.TestCase):
    def test_every_header_is_read(self):
        headers = ud.read_headers()
        self.assertGreater(len(headers), 400)
        unparsed = {f"{h.rust}:{h.line}": h.problem for h in headers if h.problem}
        self.assertEqual(set(unparsed), EXPECTED_UNPARSED, unparsed)
        for h in headers:
            if not h.problem:
                self.assertTrue(all(c.version for c in h.clauses), h)


if __name__ == "__main__":
    unittest.main()
