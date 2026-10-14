#!/usr/bin/env python3
"""Join OBI's OTEL_EBPF_TRACE_PRINTER=text output against load-gen's ground truth.

Written to make the concurrency-sweep self-authored-rate measurement
(m0-harness/README.md, "Dose-response...") re-auditable, unlike the original
ad hoc single-pass analysis that produced the retracted "hard cliff" claim
(see the README's "process lesson" section) — this script is the artifact
that lets someone else re-run the same join.

For each OBI HTTP/HTTPClient line, extract the trace_id from its
`traceparent=[...]` field and check whether load-gen actually issued that
trace_id in this run. A trace_id OBI reports that load-gen never issued is
"self-authored" -- not sourced from any real request, most likely a stale or
misattributed read under concurrent stream handling.

Usage: analyze_obi_log.py OBI_LOG LOAD_GEN_JSONL [START_LINE] [END_LINE]
START_LINE/END_LINE (0-indexed, END exclusive) slice OBI_LOG to the window of
one sweep level, since OBI's own leading timestamp is batched, not per-event,
and cannot be used to window a continuously-running OBI process by time
(documented pitfall, README "OTEL_EBPF_TRACE_PRINTER=text leading timestamp").
Exits 1 if any HTTP/HTTPClient line in the window could not be parsed.
"""

from __future__ import annotations

import json
import re
import sys

OBI_LINE = re.compile(
    r"^\S+ \S+ \(.*?\) (?P<event>\w+)\(subType=\d+\) (?P<status>\d+) \S+ \S+\(\S*\) "
    r"\[.*?\]->\[.*?\] .*?svc=\[(?P<svc>\S+) \S+\] "
    r"traceparent=\[\d{2}-(?P<tid>[0-9a-f]{32})-"
)


def load_ground_truth(path: str) -> set[str]:
    ids = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ids.add(json.loads(line)["trace_id"])
    return ids


def analyze(obi_lines: list[str], ground_truth: set[str]) -> dict[tuple[str, str], list[int]]:
    counts: dict[tuple[str, str], list[int]] = {}
    for line in obi_lines:
        m = OBI_LINE.match(line)
        if not m or m.group("event") not in ("HTTP", "HTTPClient"):
            continue
        key = (m.group("svc"), m.group("event"))
        total_bad = counts.setdefault(key, [0, 0])
        total_bad[0] += 1
        if m.group("tid") not in ground_truth:
            total_bad[1] += 1
    return counts


def count_unparsed(lines: list[str]) -> int:
    """HTTP/HTTPClient events OBI_LINE cannot read: an empty route, method or
    traceparent is valid printer output but does not match. They would
    otherwise vanish from the denominator instead of invalidating the run."""
    return sum(
        1
        for line in lines
        if ("HTTP(subType=" in line or "HTTPClient(subType=" in line) and not OBI_LINE.match(line)
    )


def main(argv: list[str]) -> int:
    obi_log, gt_jsonl = argv[0], argv[1]
    start_line = int(argv[2]) if len(argv) > 2 else 0
    end_line = int(argv[3]) if len(argv) > 3 else None

    with open(obi_log) as f:
        lines = f.readlines()[start_line:end_line]
    gt = load_ground_truth(gt_jsonl)
    counts = analyze(lines, gt)

    print(f"ground truth trace_ids: {len(gt)}")
    for (svc, event), (total, bad) in sorted(counts.items()):
        pct = 100 * bad / total if total else 0
        print(f"{svc:15s} {event:12s} total={total:5d} self_authored={bad:5d} ({pct:.1f}%)")
    unparsed = count_unparsed(lines)
    if unparsed:
        print(f"INVALID: {unparsed} HTTP/HTTPClient lines not parsed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
