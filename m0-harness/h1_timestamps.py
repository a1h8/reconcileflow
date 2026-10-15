#!/usr/bin/env python3
"""A1: are per-event timestamps precise enough for timing-based pairing?

Protocol and decision rule are fixed before any run (private spec, A1 protocol).
Each observation source is paired with load-gen's ground truth WITHOUT using time:
OBI spans by trace id, the http-signal-probe by order (only valid at concurrency 1).

offset   = observed time - load-gen time (same request)
residual = offset - median(offset)   # absorbs clock conversion and loopback transit

The residual is the source's timestamp error plus real transit/scheduling variance:
an upper bound on the error, and the noise a pairing between two points must absorb.

Usage:
  h1_timestamps.py <load-gen.jsonl> <obi-json.log> [<probe.jsonl>] [--port 8443]
"""

from __future__ import annotations

import json
import math
import statistics
import sys

HOLDS_P99_US = 100.0
PARTIAL_P99_US = 1000.0
MIN_DISTINCT_RATIO = 0.99


def load_gen_rows(path: str) -> list[dict]:
    with open(path) as f:
        rows = [json.loads(line) for line in f if line.strip()]
    return [r for r in rows if r.get("side", "load-gen") == "load-gen" and not r.get("error")]


def obi_server_spans(lines, port: int) -> list[dict]:
    """Server spans observed on ``port`` from OBI's JSON printer (one JSON array per line)."""
    spans = []
    for line in lines:
        line = line.strip()
        if not line.startswith("["):
            continue  # OBI's own log lines share stdout
        for span in json.loads(line):
            if span.get("kind") == "SPAN_KIND_SERVER" and int(span.get("hostPort", 0)) == port:
                spans.append(span)
    return spans


def summarize(residuals: list[float], distinct: int, n: int) -> dict:
    if not residuals:
        return {"n": 0}
    abs_sorted = sorted(abs(r) for r in residuals)
    p99 = abs_sorted[max(0, math.ceil(0.99 * len(abs_sorted)) - 1)]
    return {
        "n": len(residuals),
        "distinct_ratio": distinct / n if n else 0.0,
        "sd_us": statistics.pstdev(residuals),
        "p50_abs_us": statistics.median(abs_sorted),
        "p99_abs_us": p99,
        "max_abs_us": abs_sorted[-1],
    }


def verdict(summary: dict) -> str:
    if summary.get("n", 0) == 0:
        return "NOT_SCORED"
    if summary["distinct_ratio"] < MIN_DISTINCT_RATIO:
        return "FAILS"
    if summary["p99_abs_us"] <= HOLDS_P99_US:
        return "HOLDS"
    if summary["p99_abs_us"] <= PARTIAL_P99_US:
        return "PARTIAL"
    return "FAILS"


def _residuals(offsets: list[float]) -> list[float]:
    median = statistics.median(offsets)
    return [o - median for o in offsets]


def score_obi(rows: list[dict], spans: list[dict]) -> dict:
    """Pair by trace id. A trace id seen on several server spans is not scored (ambiguous)."""
    by_tid: dict[str, list[dict]] = {}
    for span in spans:
        by_tid.setdefault(span.get("traceID", ""), []).append(span)
    truth = {r["trace_id"]: r for r in rows}
    paired = [(truth[t], s[0]) for t, s in by_tid.items() if t in truth and len(s) == 1]
    out = {"load_gen_rows": len(rows), "server_spans": len(spans), "paired": len(paired)}
    for field, ref in (
        ("start", lambda r: r["timestamp_ns"] / 1000),
        ("handlerStart", lambda r: r["timestamp_ns"] / 1000),
        ("end", lambda r: (r["timestamp_ns"] + r["duration_ns"]) / 1000),
    ):
        values = [int(s[field]) for _, s in paired]
        offsets = [int(s[field]) - ref(r) for r, s in paired]
        out[field] = summarize(
            _residuals(offsets) if offsets else [], len(set(values)), len(values)
        )
        out[field]["verdict"] = verdict(out[field])
    return out


def score_probe(rows: list[dict], events: list[dict]) -> dict:
    """Pair by order: valid only at concurrency 1, and only if counts match exactly."""
    out = {"load_gen_rows": len(rows), "probe_events": len(events)}
    if len(rows) != len(events) or not rows:
        out["entry"] = {"n": 0, "verdict": "NOT_SCORED", "reason": "counts differ"}
        return out
    rows = sorted(rows, key=lambda r: r["timestamp_ns"])
    events = sorted(events, key=lambda e: e["timestamp_ns"])
    offsets = [
        (e["timestamp_ns"] - r["timestamp_ns"]) / 1000 for r, e in zip(rows, events, strict=True)
    ]
    values = [e["timestamp_ns"] for e in events]
    out["entry"] = summarize(_residuals(offsets), len(set(values)), len(values))
    out["entry"]["verdict"] = verdict(out["entry"])
    return out


def score_kernel_pair(rows: list[dict], spans: list[dict], events: list[dict]) -> dict:
    """OBI handlerStart vs probe entry on the same request (concurrency 1 only).

    The probe is paired with load-gen by order, OBI by trace id: neither pairing uses time.
    Both are kernel-side observations of the same handler entry, so their disagreement
    bounds the timestamp error of each, without the client-side reference noise.
    """
    if len(rows) != len(events) or not rows:
        return {"n": 0, "verdict": "NOT_SCORED", "reason": "counts differ"}
    rows = sorted(rows, key=lambda r: r["timestamp_ns"])
    events = sorted(events, key=lambda e: e["timestamp_ns"])
    probe_us = {r["trace_id"]: e["timestamp_ns"] / 1000 for r, e in zip(rows, events, strict=True)}
    by_tid: dict[str, list[dict]] = {}
    for span in spans:
        by_tid.setdefault(span.get("traceID", ""), []).append(span)
    offsets = [
        int(s[0]["handlerStart"]) - probe_us[t]
        for t, s in by_tid.items()
        if t in probe_us and len(s) == 1
    ]
    values = [int(s[0]["handlerStart"]) for t, s in by_tid.items() if t in probe_us and len(s) == 1]
    out = summarize(_residuals(offsets) if offsets else [], len(set(values)), len(values))
    out["verdict"] = verdict(out)
    return out


def id_consistency(rows: list[dict], spans: list[dict]) -> dict:
    """Duplicates and missing ids. Cannot see a swap whose id sets stay equal."""
    counts: dict[str, int] = {}
    for span in spans:
        counts[span.get("traceID", "")] = counts.get(span.get("traceID", ""), 0) + 1
    ordered = sorted(rows, key=lambda r: r["timestamp_ns"])
    missing = [i for i, r in enumerate(ordered) if counts.get(r["trace_id"], 0) == 0]
    tids = {r["trace_id"] for r in rows}
    return {
        "missing": len(missing),
        "missing_positions": missing[:20],
        "duplicated": sum(1 for t in tids if counts.get(t, 0) > 1),
        "spans_with_unknown_id": sum(n for t, n in counts.items() if t not in tids),
    }


def main(argv: list[str]) -> None:
    port = 8443
    if "--port" in argv:
        i = argv.index("--port")
        port = int(argv[i + 1])
        argv = argv[:i] + argv[i + 2 :]
    rows = load_gen_rows(argv[1])
    with open(argv[2]) as f:
        spans = obi_server_spans(f, port)
    result = {"obi": score_obi(rows, spans), "ids": id_consistency(rows, spans)}
    if len(argv) > 3:
        with open(argv[3]) as f:
            events = [json.loads(line) for line in f if line.strip()]
        result["probe"] = score_probe(rows, events)
        result["kernel_pair"] = score_kernel_pair(rows, spans, events)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main(sys.argv)
