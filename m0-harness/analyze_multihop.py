#!/usr/bin/env python3
"""Self-authored rate per hop, for the 3-hop gateway->upstream->fake-upstream chain.

Groups by (svc, event, destination) rather than just (svc, event): the gateway
and upstream hops run the identical Traefik binary, and OBI's text printer
labels both simply "svc=[traefik go]" -- destination port is the only reliable
way to tell them apart for HTTPClient (outbound) events. For HTTP (inbound)
events this analysis found OBI mislabels the upstream hop's own destination
port (should be 9081, shows an unrelated ephemeral-looking value instead) --
a new, undiagnosed symptom, reported as observed rather than explained.
"""

from __future__ import annotations

import json
import re
import sys

OBI_LINE = re.compile(
    r"^\S+ \S+ \(.*?\) (?P<event>\w+)\(subType=\d+\) (?P<status>\d+) \S+ \S+\(\S*\) "
    r"\[.*?\]->\[.*?(?P<dest>\S+:\d+)\] .*?svc=\[(?P<svc>\S+) \S+\] "
    r"traceparent=\[\d{2}-(?P<tid>[0-9a-f]{32})-"
)


def load_ground_truth(path: str) -> set[str]:
    ids = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                ids.add(json.loads(line)["trace_id"])
    return ids


def analyze(lines: list[str], ground_truth: set[str]) -> dict[tuple[str, str, str], list[int]]:
    counts: dict[tuple[str, str, str], list[int]] = {}
    for line in lines:
        m = OBI_LINE.match(line)
        if not m or m.group("event") not in ("HTTP", "HTTPClient"):
            continue
        key = (m.group("svc"), m.group("event"), m.group("dest"))
        tb = counts.setdefault(key, [0, 0])
        tb[0] += 1
        if m.group("tid") not in ground_truth:
            tb[1] += 1
    return counts


if __name__ == "__main__":
    obi_log, gt_jsonl, start, end = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    with open(obi_log) as f:
        lines = f.readlines()[start:end]
    gt = load_ground_truth(gt_jsonl)
    counts = analyze(lines, gt)
    print(f"ground truth trace_ids: {len(gt)}")
    for (svc, event, dest), (total, bad) in sorted(counts.items(), key=lambda kv: -kv[1][0]):
        pct = 100 * bad / total if total else 0
        print(f"{svc:10s} {event:12s} -> {dest:22s} total={total:5d} self_authored={bad:5d} ({pct:.1f}%)")
