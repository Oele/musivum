#!/usr/bin/env python3
"""Makes config-sample-uncommented.yaml from the config-sample.yaml of LiveKit's server.

    make-sample.py <livekit>/config-sample.yaml > config-sample-uncommented.yaml

The sample documents most options as comments. This takes the comment sign off every line
that is an option (`# name: value`, `# - item`), one level of it, so that the options of
the sample are all set; what is prose stays a comment. Three things are not as in the
sample, because the file has to load, and load without asking the network:

- `debug_handler_port` stays a comment: it is not a key of the configuration (the key is
  `debug_handler`).
- `node_ip` has an address where the sample has a placeholder.
- `use_external_ip` is false (the sample says of `node_ip`: "for this to take effect, set
  use_external_ip to false"), so that no STUN server is asked.
"""
import re
import sys

OPTION = re.compile(r"^\s*(- )?[A-Za-z_0-9]+:(\s.*)?$|^\s*- \S+$")
COMMENTED = re.compile(r"^(\s*)# (.*)$")

lines = open(sys.argv[1]).read().split("\n")

# LiveKit's notice stays as it is, at the top.
head = 0
while lines[head].startswith("#"):
    head += 1
out = lines[:head]
out.append("")
out.append("# This is config-sample.yaml of LiveKit's server at v1.13.11, changed for musivum by")
out.append("# make-sample.py: the options it has as comments are set, `debug_handler_port` (not a")
out.append("# key of the configuration) stays a comment, `node_ip` has an address for its")
out.append("# placeholder and `use_external_ip` is false.")

skip = 0
for line in lines[head:]:
    m = COMMENTED.match(line)
    if skip:
        skip -= 1
    elif line == "# debug_handler_port:":
        skip = 1
    elif m and OPTION.match(m.group(2)):
        line = m.group(1) + m.group(2)
    line = line.replace("node_ip: <external-ip-of-node>", "node_ip: 192.0.2.10")
    if line.strip() == "use_external_ip: true":
        line = line.replace("true", "false")
    out.append(line)

sys.stdout.write("\n".join(out))
