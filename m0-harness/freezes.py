#!/usr/bin/env python3
"""Where do the timing freezes come from: the host, or the agent?

Uses only references that owe nothing to the agent: load-gen's own send and
receive times, and fake-upstream's arrival and answer times. Per request:

- arrival delay = fake-upstream arrival - load-gen send;
- answer delay = load-gen receive - fake-upstream answer.

A freeze is a group of at least FREEZE_MIN requests whose delay exceeds
FREEZE_DELAY_US and that are released in the same millisecond (arrival at
fake-upstream for the arrival delay, receipt at load-gen for the answer
delay): requests ready at different times, let go together.

With CPU_SAMPLE=1, the run directory also holds per-core and per-process CPU
use, summarised here.

Usage: freezes.py <run-dir> [--json]
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

FREEZE_DELAY_US = 20_000
FREEZE_MIN = 10
BUSY = 99.0  # a core this busy, or more, counts as saturated
TRUTH = re.compile(r"truth-([0-9a-f]+)")


def quantile(values: list[int], p: float) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


def _jsonl(path: Path) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def delays(cell: Path) -> dict[str, list[tuple[int, int]]]:
    """(delay in µs, release time in ns) per kind, for every measured request."""
    sent = {r["trace_id"]: r for r in _jsonl(cell / "lg.jsonl")}
    out: dict[str, list[tuple[int, int]]] = {"arrival": [], "answer": []}
    for row in _jsonl(cell / "fu.jsonl"):
        match = TRUTH.search(row.get("path", ""))
        if not match or match[1] not in sent or not row.get("arrival_ns"):
            continue
        lg = sent[match[1]]
        received = lg["timestamp_ns"] + lg["duration_ns"]
        out["arrival"].append(((row["arrival_ns"] - lg["timestamp_ns"]) // 1000, row["arrival_ns"]))
        out["answer"].append(((received - row["timestamp_ns"]) // 1000, received))
    return out


def freezes(samples: list[tuple[int, int]]) -> dict[int, int]:
    """Freezes: release millisecond (since the epoch) -> number of requests."""
    groups = Counter(at // 1_000_000 for delay, at in samples if delay > FREEZE_DELAY_US)
    return {ms: n for ms, n in groups.items() if n >= FREEZE_MIN}


def _clock(at_ns: int) -> str:
    return datetime.fromtimestamp(at_ns / 1e9).strftime("%H:%M:%S")


def cpu(cell: Path, freeze_seconds: set[str]) -> dict | None:
    cores_file, procs_file = cell / "cpu-cores.txt", cell / "cpu-procs.txt"
    if not cores_file.exists():
        return None
    busy = defaultdict(dict)  # second -> core -> busy %
    for line in cores_file.read_text().splitlines():
        parts = line.split()
        if len(parts) == 12 and parts[1].isdigit():
            busy[parts[0]][int(parts[1])] = 100.0 - float(parts[11])
    seconds = sorted(busy)
    saturated = [s for s in seconds if max(busy[s].values()) >= BUSY]
    per_proc = defaultdict(list)
    if procs_file.exists():
        for line in procs_file.read_text().splitlines():
            parts = line.split()
            if len(parts) >= 10 and parts[2].isdigit():
                per_proc[parts[9]].append(float(parts[7]))
    top = sorted(per_proc.items(), key=lambda kv: -sum(kv[1]) / max(len(seconds), 1))[:6]
    return {
        "seconds": len(seconds),
        "seconds_with_a_saturated_core": len(saturated),
        "freeze_seconds_with_a_saturated_core": sum(1 for s in freeze_seconds if s in saturated),
        "freeze_seconds": len(freeze_seconds),
        "busiest_core_p50": (
            round(quantile([max(busy[s].values()) for s in seconds], 0.5), 1) if seconds else None
        ),
        "top_processes_mean_cpu": {
            name: round(sum(v) / max(len(seconds), 1), 1) for name, v in top
        },
    }


def analyze_cell(cell: Path) -> dict:
    report = {}
    freeze_seconds = set()
    for kind, samples in delays(cell).items():
        if not samples:
            report[kind] = None
            continue
        values = [d for d, _ in samples]
        found = freezes(samples)
        sizes = sorted(found.values(), reverse=True)
        freeze_seconds |= {_clock(ms * 1_000_000) for ms in found}
        report[kind] = {
            "n": len(values),
            "p50_us": quantile(values, 0.5),
            "p99_us": quantile(values, 0.99),
            "p999_us": quantile(values, 0.999),
            "max_us": max(values),
            "freezes": len(sizes),
            "largest_freeze": sizes[0] if sizes else 0,
        }
    report["cpu"] = cpu(cell, freeze_seconds)
    return report


def main(argv: list[str]) -> None:
    run_dir = Path(argv[1])
    cells = sorted(p.parent for p in run_dir.glob("*/fu.jsonl"))
    report = {cell.name: analyze_cell(cell) for cell in cells}
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    for name, cell in report.items():
        print(f"== {name}")
        for kind in ("arrival", "answer"):
            r = cell[kind]
            if r is None:
                print(f"   {kind}: no arrival time in fu.jsonl (fake-upstream predates it)")
                continue
            print(
                f"   {kind:7s} delay µs: p50 {r['p50_us']} p99 {r['p99_us']} p99.9 {r['p999_us']} "
                f"max {r['max_us']} | freezes {r['freezes']} (largest {r['largest_freeze']})"
                f" n={r['n']}"
            )
        c = cell["cpu"]
        if c is None:
            print("   cpu: not sampled (CPU_SAMPLE=1)")
        else:
            print(
                f"   cpu: {c['seconds_with_a_saturated_core']}/{c['seconds']} s"
                f" with a core >= {BUSY:.0f}%; freeze seconds with one:"
                f" {c['freeze_seconds_with_a_saturated_core']}/{c['freeze_seconds']}"
                f"; busiest core p50 {c['busiest_core_p50']}%"
            )
            print(f"   top processes, mean %CPU: {c['top_processes_mean_cpu']}")


if __name__ == "__main__":
    main(sys.argv)
