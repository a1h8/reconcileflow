#!/usr/bin/env python3
"""Calibrate the timing path on calibration runs (phase 3, step 0).

On every true pair of a join (paired by the truth id in the path: this is
calibration, never evaluation), measure the gap between the two sides for
each candidate signal, then apply the rule fixed before any calibration run
(docs: correlation-phase3-spec.md §3):

- offset = median gap; residual = |gap - offset|;
- signals retained: the first is start or end, whichever has the smaller
  residual p99.9 (duration alone does not place a request in time); the
  second is the next quietest of the other two, kept only if its p99.9 is at
  most 3 times the first's (at most two);
- window = residual p99.9 of each retained signal, rounded up to the µs;
- margin = residual p99 of the first retained signal;
- with several runs per cell (--runs-per-cell N, round 4: 2), signals,
  receiver start and offsets come from the pooled true pairs; each window is
  the largest of the runs' p99.9 and the margin the largest of their p99
  (residuals around the pooled offset), and the rate the largest of theirs;
- local density guard: local_span_us = LOCAL_SPAN_US, always written;
- receiver start: the downstream span's start, or its handler start, whichever
  gives the smaller p99.9 for its best signal;
- density = rate x 2 x window of the first signal; feasible iff <= D_MAX = 1.

Joins: J1 hop-1 -> fake-upstream (judged), J3 load-gen -> hop-1 (reported);
J2, the crossing of hop-1 (inbound -> outbound), is reported only, with the
share of pairs whose outbound interval lies inside the inbound one.

Usage: calibrate_timing.py <run-dir>... [--runs-per-cell N] [--write params.toml]
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

from timing_path import D_MAX, cell_meta, rate, records

SIGNALS = ("start", "end", "duration")
LOCATING = ("start", "end")  # signals that place a request in time
CANDIDATE_RECEIVER_STARTS = ("start", "handlerStart")
LOCAL_SPAN_US = 20_000  # round 4: the local density guard looks ±20 ms around a pair
JOINS = {
    "hop-1 -> fake-upstream": (("hop-1", "outbound"), ("fake-upstream", "inbound")),
    "load-gen -> hop-1": (("load-gen", "outbound"), ("hop-1", "inbound")),
}


def quantile(values: list[int], p: float) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


def gap(name: str, a: dict, b: dict, b_start: int) -> int:
    if name == "start":
        return b_start - a["start"]
    if name == "end":
        return a["end"] - b["end"]
    return (a["end"] - a["start"]) - (b["end"] - b_start)


def pairs(rows: list[dict], up: tuple[str, str], down: tuple[str, str]) -> list[tuple[dict, dict]]:
    """True pairs of a join: one record per side and per truth id."""
    ups = {r["truth"]: r for r in rows if (r["point"], r["side"]) == up}
    downs = {r["truth"]: r for r in rows if (r["point"], r["side"]) == down}
    return [(ups[t], downs[t]) for t in sorted(ups.keys() & downs.keys())]


def _gaps(true_pairs, name: str, receiver_start: str) -> list[int]:
    return [
        gap(name, a, b, b["handler"] if receiver_start == "handlerStart" else b["start"])
        for a, b in true_pairs
    ]


def residual_quantile(true_pairs, name: str, receiver_start: str, offset: int, p: float) -> int:
    """Quantile p of |gap - offset| over these pairs, for a given offset."""
    return quantile([abs(g - offset) for g in _gaps(true_pairs, name, receiver_start)], p)


def signal_stats(true_pairs, name: str, receiver_start: str) -> dict:
    gaps = _gaps(true_pairs, name, receiver_start)
    offset = statistics.median_low(gaps)
    residual = [abs(g - offset) for g in gaps]
    return {
        "offset_us": offset,
        "p50": quantile(residual, 0.5),
        "p99": quantile(residual, 0.99),
        "p999": quantile(residual, 0.999),
        "max": max(residual),
    }


def choose(true_pairs) -> dict:
    """Apply the pre-registered rule to one join's true pairs."""
    options = []
    for receiver_start in CANDIDATE_RECEIVER_STARTS:
        stats = {n: signal_stats(true_pairs, n, receiver_start) for n in SIGNALS}
        ranked = sorted(SIGNALS, key=lambda n: (stats[n]["p999"], SIGNALS.index(n)))
        # Duration does not place a request in time: it may only come second.
        ranked.remove(first := next(n for n in ranked if n in LOCATING))
        ranked.insert(0, first)
        options.append((stats[ranked[0]]["p999"], receiver_start, stats, ranked))
    _, receiver_start, stats, ranked = min(options, key=lambda o: (o[0], o[1] != "start"))
    kept = [ranked[0]]
    if stats[ranked[1]]["p999"] <= 3 * stats[ranked[0]]["p999"]:
        kept.append(ranked[1])
    return {
        "receiver_start": receiver_start,
        "signals": [
            {
                "name": n,
                "offset_us": stats[n]["offset_us"],
                "window_us": max(1, math.ceil(stats[n]["p999"])),
            }
            for n in kept
        ],
        "margin_us": stats[kept[0]]["p99"],
        "max_component": 32,
        "stats": stats,
    }


def choose_across(per_run: list[list[tuple[dict, dict]]]) -> dict:
    """The rule on the pooled pairs of a cell's runs, windows and margin from the
    noisiest run. With a single run this is choose()."""
    chosen = choose([pair for run in per_run for pair in run])
    rs = chosen["receiver_start"]
    for s in chosen["signals"]:
        widest = max(
            residual_quantile(run, s["name"], rs, s["offset_us"], 0.999) for run in per_run
        )
        s["window_us"] = max(1, math.ceil(widest))
    first = chosen["signals"][0]
    chosen["margin_us"] = max(
        residual_quantile(run, first["name"], rs, first["offset_us"], 0.99) for run in per_run
    )
    chosen["local_span_us"] = LOCAL_SPAN_US
    return chosen


def contained(true_pairs) -> float:
    """Share of pairs whose downstream interval lies inside the upstream one."""
    inside = sum(1 for a, b in true_pairs if a["start"] <= b["start"] and b["end"] <= a["end"])
    return inside / len(true_pairs) if true_pairs else 0.0


def calibrate_cell(cell_dirs: list[Path]) -> dict:
    """One cell (agent, concurrency, load) from its calibration runs."""
    meta = cell_meta(cell_dirs[0])
    runs = [records(d, cell_meta(d))[0] for d in cell_dirs]
    requests_per_s = max(rate(rows) for rows in runs)
    out = {"meta": meta, "rate_per_s": round(requests_per_s, 1), "joins": {}}
    for where, (up, down) in JOINS.items():
        per_run = [pairs(rows, up, down) for rows in runs]
        true_pairs = [pair for run in per_run for pair in run]
        if not true_pairs:
            continue
        chosen = choose_across(per_run)
        density = requests_per_s * 2 * chosen["signals"][0]["window_us"] / 1e6
        chosen |= {
            "pairs": len(true_pairs),
            "contained": round(contained(true_pairs), 4),
            "density": round(density, 3),
            "feasible": density <= D_MAX,
        }
        out["joins"][where] = chosen
    crossing = [
        p for rows in runs for p in pairs(rows, ("hop-1", "inbound"), ("hop-1", "outbound"))
    ]
    if crossing:
        out["crossing hop-1"] = {
            "pairs": len(crossing),
            "contained": round(contained(crossing), 4),
            "stats": {n: signal_stats(crossing, n, "start") for n in SIGNALS},
        }
    return out


def fingerprint(cell_dir: Path) -> str:
    digest = hashlib.sha256()
    for name in ("lg.jsonl", "agent.out"):
        path = cell_dir / name
        if path.exists():
            digest.update(path.read_bytes())
    return digest.hexdigest()


def _toml_list(values) -> str:
    return "[" + ", ".join(f'"{v}"' for v in values) + "]"


def load_of(calibrations: list[tuple[list[Path], dict]]) -> int:
    """The one load all calibration runs ran under; mixing loads is refused."""
    loads = {cal["meta"]["load"] for _, cal in calibrations}
    if len(loads) > 1:
        raise ValueError(f"calibration runs under different loads: {sorted(loads)} cores")
    return loads.pop() if loads else 0


def to_toml(calibrations: list[tuple[list[Path], dict]]) -> str:
    lines = [
        "# Timing-path parameters, from calibration runs (calibrate_timing.py).",
        f"load_cores = {load_of(calibrations)}  # cores kept busy by cpu-load during calibration",
        "",
    ]
    for cell_dirs, cal in calibrations:
        meta = cal["meta"]
        for where, j in cal["joins"].items():
            lines += [
                f'[{meta["agent"]}.c{meta["concurrency"]}."{where}"]',
                f"calibration = {_toml_list(cell_dirs)}",
                f"sha256 = {_toml_list(fingerprint(d) for d in cell_dirs)}",
                f'receiver_start = "{j["receiver_start"]}"',
                f"margin_us = {j['margin_us']}",
                f"local_span_us = {j['local_span_us']}",
                f"max_component = {j['max_component']}",
                f"density = {j['density']}",
                f"feasible = {str(j['feasible']).lower()}",
                "signals = ["
                + ", ".join(
                    f'{{ name = "{s["name"]}", offset_us = {s["offset_us"]}, '
                    f"window_us = {s['window_us']} }}"
                    for s in j["signals"]
                )
                + "]",
                "",
            ]
    return "\n".join(lines)


def main(argv: list[str]) -> None:
    args = argv[1:]
    write = None
    if "--write" in args:
        i = args.index("--write")
        write = Path(args[i + 1])
        args = args[:i] + args[i + 2 :]
    per_cell = 1
    if "--runs-per-cell" in args:
        i = args.index("--runs-per-cell")
        per_cell = int(args[i + 1])
        args = args[:i] + args[i + 2 :]
    cells: dict[tuple, list[Path]] = {}
    for run_dir in map(Path, args):
        for cell_dir in sorted(p.parent for p in run_dir.glob("*/lg.jsonl")):
            meta = cell_meta(cell_dir)
            key = (meta["agent"], meta["concurrency"], meta["load"], meta["hops"])
            cells.setdefault(key, []).append(cell_dir)
    for key, dirs in cells.items():
        if len(dirs) != per_cell:
            raise ValueError(f"cell {key}: {len(dirs)} runs, needs {per_cell} calibration runs")
    calibrations = [(dirs, calibrate_cell(dirs)) for dirs in cells.values()]
    load = load_of(calibrations)
    for cell_dirs, cal in calibrations:
        meta = cal["meta"]
        print(
            f"== {', '.join(map(str, cell_dirs))}:"
            f" agent={meta['agent']} c={meta['concurrency']} load={load}"
            f" rate={cal['rate_per_s']}/s"
        )
        for where, j in cal["joins"].items():
            kept = ", ".join(
                f"{s['name']} m={s['offset_us']} w={s['window_us']}" for s in j["signals"]
            )
            print(
                f"   {where}: {j['pairs']} pairs, receiver {j['receiver_start']}, {kept},"
                f" margin {j['margin_us']} | contained {j['contained']}"
                f" | density {j['density']} ({'feasible' if j['feasible'] else 'above D_MAX'})"
            )
            for n, s in j["stats"].items():
                print(
                    f"      {n:8s} offset {s['offset_us']:8d}  residual p50 {s['p50']}"
                    f" p99 {s['p99']} p99.9 {s['p999']} max {s['max']}"
                )
        if "crossing hop-1" in cal:
            c = cal["crossing hop-1"]
            print(f"   crossing hop-1: {c['pairs']} pairs, contained {c['contained']}")
    if write:
        write.write_text(to_toml(calibrations))
        print(f"written: {write}")
    if "--json" in argv:
        print(json.dumps([cal for _, cal in calibrations], indent=2, default=str))


if __name__ == "__main__":
    main(sys.argv)
