#!/usr/bin/env python3
"""Run reconcileflow's timing path on a harness run, and score every link.

The chain of a cell is declared as a topology with no identifier at all:
load-gen (its own send and receive times) -> hop-1 ... hop-n (agent) ->
fake-upstream (agent). Each record becomes a Timed observation carrying only
its point, side, start and end; reconcileflow.correlation.timing links them
by time, with the parameters calibration fixed (calibrate_timing.py). Only
then is the truth id in each request's path read, to score the links: it
never reaches the module, and neither does any trace id.

Per join:
- correct / wrong: links between two observations of the same / of different
  requests (wrong links are silent errors);
- ambiguous, unmatched, no_candidate, oversized: measured requests seen on a
  side and left undecided, counted on the upstream side;
- both_seen: measured requests seen on both sides;
- density: rate x 2 x window of the first signal, against D_MAX;
- component sizes and observations processed per second.

Usage: timing_path.py <run-dir> <params.toml> [--json]
"""

from __future__ import annotations

import glob
import json
import re
import sys
import time
import tomllib
from collections import Counter
from itertools import pairwise
from pathlib import Path

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.timing import JoinParams, Signal, Timed, timed_links
from reconcileflow.correlation.topology import Topology, load

SERVER, CLIENT = "SPAN_KIND_SERVER", "SPAN_KIND_CLIENT"
TRUTH = re.compile(r"/truth-([0-9a-f]+)")
D_MAX = 1


def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def cell_meta(cell_dir: Path) -> dict:
    meta = json.loads((cell_dir / "cell.json").read_text())
    line = (cell_dir.parent / "cells.txt").read_text()
    concurrency = re.search(r"\bc=(\d+)", line)
    load = re.search(r"\bload=(\d+)", line)
    return {
        "hops": meta["hops"],
        "port": meta["fake_upstream_port"],
        "agent": meta.get("agent", "none"),
        "concurrency": int(concurrency[1]) if concurrency else None,
        "load": int(load[1]) if load else 0,  # cores kept busy by cpu-load (LOAD_CORES)
    }


def points(hops: int) -> list[str]:
    return ["load-gen", *(f"hop-{k}" for k in range(1, hops + 1)), "fake-upstream"]


def topology_for(hops: int) -> Topology:
    """The lab chain with every identifier withheld: hops can only be timed."""

    def point(visibility: str, timestamps: str) -> dict:
        return {
            "host": "lab",
            "visibility": visibility,
            "clock": "lab",
            "timestamps": timestamps,
            "inbound": [],
            "outbound": [],
        }

    names = points(hops)
    declared = {n: point("kernel", "kernel_monotonic") for n in names}
    declared["load-gen"] = point("app", "precise_sync")
    hop_list = [{"from": a, "to": b, "pooling": True} for a, b in pairwise(names)]
    return load({"points": declared, "hops": hop_list})


def records(cell_dir: Path, meta: dict) -> tuple[list[dict], set[str]]:
    """Every measured record: point, side, truth, start/handler start/end in µs.

    Warm-up and tail traffic is dropped by truth id first.
    """
    rows = _jsonl(cell_dir / "lg.jsonl")
    measured = {r["trace_id"] for r in rows}
    out = []
    for seq, r in enumerate(rows):
        start = r["timestamp_ns"] // 1000
        end = (r["timestamp_ns"] + r.get("duration_ns", 0)) // 1000
        out.append(
            {
                "source": "lg.jsonl",
                "seq": seq,
                "point": "load-gen",
                "side": "outbound",
                "truth": r["trace_id"],
                "start": start,
                "handler": start,
                "end": end,
            }
        )
    excluded = set()
    for path in [*glob.glob(str(cell_dir / "warmup-*.jsonl")), cell_dir / "tail.jsonl"]:
        excluded |= {r["trace_id"] for r in _jsonl(Path(path))}
    agent_out = cell_dir / "agent.out"
    lines = agent_out.read_text().splitlines() if agent_out.exists() else []
    seq = 0
    for line in lines:
        if not line.startswith("["):
            continue
        for span in json.loads(line):
            seq += 1
            attrs = span.get("attributes", {})
            match = TRUTH.search(attrs.get("url", ""))
            if not match or match[1] in excluded or match[1] not in measured:
                continue
            place = _place(span, attrs, meta)
            if place is None:
                continue
            start = int(span["start"])
            out.append(
                {
                    "source": "agent.out",
                    "seq": seq,
                    "point": place[0],
                    "side": place[1],
                    "truth": match[1],
                    "start": start,
                    "handler": int(span.get("handlerStart", start)),
                    "end": int(span["end"]),
                }
            )
    return out, measured


def _place(span: dict, attrs: dict, meta: dict) -> tuple[str, str] | None:
    kind = span.get("kind")
    if kind == SERVER and int(span.get("hostPort", 0)) == meta["port"]:
        return "fake-upstream", "inbound"
    for k in range(1, meta["hops"] + 1):
        if kind == SERVER and attrs.get("serverAddr") == f"hop-{k}":
            return f"hop-{k}", "inbound"
        if kind == CLIENT and attrs.get("clientAddr") == f"hop-{k}":
            return f"hop-{k}", "outbound"
    return None


def join_params(table: dict) -> tuple[JoinParams, str]:
    """A join's parameters from its TOML table, and which receiver start it uses."""
    signals = tuple(Signal(s["name"], s["offset_us"], s["window_us"]) for s in table["signals"])
    params = JoinParams(signals, table["margin_us"], table.get("max_component", 32))
    return params, table.get("receiver_start", "start")


def params_for(params_file: Path, meta: dict) -> dict[str, tuple[JoinParams, str]]:
    """The joins calibrated for this cell's agent and concurrency, under its load."""
    data = tomllib.loads(params_file.read_text())
    calibrated = data.get("load_cores", 0)
    if calibrated != meta["load"]:
        raise ValueError(
            f"{params_file}: calibrated under a load of {calibrated} cores,"
            f" the run had {meta['load']}"
        )
    cell = data.get(meta["agent"], {}).get(f"c{meta['concurrency']}", {})
    return {where: join_params(table) for where, table in cell.items()}


def rate(rows: list[dict]) -> float:
    """Measured requests per second, from load-gen's own send and receive times."""
    lg = [r for r in rows if r["point"] == "load-gen"]
    span_us = max(r["end"] for r in lg) - min(r["start"] for r in lg)
    return len(lg) / (span_us / 1e6) if span_us > 0 else 0.0


def analyze_cell(cell_dir: Path, params_file: Path) -> dict:
    meta = cell_meta(cell_dir)
    topology = topology_for(meta["hops"])
    rows, measured = records(cell_dir, meta)
    joins = params_for(params_file, meta)
    requests_per_s = rate(rows)
    report = {
        "meta": meta,
        "measured": len(measured),
        "rate_per_s": round(requests_per_s, 1),
        "joins": {},
    }
    for where, (params, receiver_start) in sorted(joins.items()):
        src, dst = where.split(" -> ")
        report["joins"][where] = score_join(
            topology, rows, where, src, dst, params, receiver_start, requests_per_s
        )
    return report


def score_join(topology, rows, where, src, dst, params, receiver_start, requests_per_s) -> dict:
    side_rows = [
        r for r in rows if (r["point"], r["side"]) in {(src, "outbound"), (dst, "inbound")}
    ]
    truth = {(r["source"], r["seq"]): r["truth"] for r in side_rows}
    obs = []
    for r in side_rows:
        start = (
            r["handler"] if receiver_start == "handlerStart" and r["point"] == dst else r["start"]
        )
        obs.append(Timed(r["source"], r["seq"], r["point"], r["side"], start, r["end"]))
    began = time.perf_counter()
    result = timed_links(topology, obs, {where: params})
    elapsed = time.perf_counter() - began
    correct = sum(1 for x in result.links if truth[x.upstream] == truth[x.downstream])
    up_truths = {r["truth"] for r in side_rows if r["point"] == src}
    down_truths = {r["truth"] for r in side_rows if r["point"] == dst}
    both = len(up_truths & down_truths)
    statuses = Counter(u.status.name.lower() for u in result.undecided if u.side == "outbound")
    sizes = sorted(n for _, n in result.components)
    first = params.signals[0]
    density = requests_per_s * 2 * first.window_us / 1e6
    return {
        "links": len(result.links),
        "correct": correct,
        "wrong": len(result.links) - correct,
        "both_seen": both,
        **{
            s.name.lower(): statuses.get(s.name.lower(), 0)
            for s in Status
            if s is not Status.MATCHED
        },
        "density": round(density, 3),
        "feasible": density <= D_MAX,
        "component_p50": sizes[len(sizes) // 2] if sizes else 0,
        "component_p99": sizes[min(len(sizes) - 1, int(0.99 * len(sizes)))] if sizes else 0,
        "component_max": sizes[-1] if sizes else 0,
        "observations_per_s": round(len(obs) / elapsed) if elapsed > 0 else None,
    }


def main(argv: list[str]) -> None:
    run_dir, params_file = Path(argv[1]), Path(argv[2])
    cells = sorted(p.parent for p in run_dir.glob("*/lg.jsonl"))
    report = {cell.name: analyze_cell(cell, params_file) for cell in cells}
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    for name, cell in report.items():
        meta = cell["meta"]
        print(
            f"== {name}: agent={meta['agent']} c={meta['concurrency']} measured={cell['measured']}"
            f" rate={cell['rate_per_s']}/s"
        )
        if not cell["joins"]:
            print("   no calibrated join for this agent and concurrency")
        for where, r in cell["joins"].items():
            print(
                f"   {where}: links {r['links']} correct {r['correct']} wrong {r['wrong']}"
                f" | both seen {r['both_seen']} ambiguous {r['ambiguous']} unmatched"
                f" {r['unmatched']} no_candidate {r['no_candidate']} oversized {r['oversized']}"
            )
            print(
                f"      density {r['density']} ({'feasible' if r['feasible'] else 'above D_MAX'})"
                f" components p50/p99/max {r['component_p50']}/{r['component_p99']}"
                f"/{r['component_max']} | {r['observations_per_s']} obs/s"
            )
    wrong = sum(r["wrong"] for c in report.values() for r in c["joins"].values())
    print(f"wrong links in the run: {wrong}")


if __name__ == "__main__":
    main(sys.argv)
