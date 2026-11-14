#!/usr/bin/env python3
"""Gate of the patched agent (phase 4, 4b): is the HTTP/2 stream number usable,
and at what cost? (docs/target/correlation-phase4-spec.md §4.2)

Read on J1 (hop-1 outbound -> fake-upstream inbound), the join whose two ends
are both agent spans, against the truth id in each request's path. On every
run of the patched agent:

- G1: every measured request seen on both sides carries a stream number on
  both, and the two are equal. 100 %, or the gate fails.
- G2: on each side, no (four-tuple, stream number) is reported for two
  different requests.
- G3 (four-tuple half): both sides report the same four-tuple for every
  request, as the shipped agent does (C2). The time half is reported against
  the references (gap medians and p99), not judged.

Against the runs of the shipped agent (--reference, same cell):

- G4a: for every patched probe run on both sides, mean cost per run (kernel
  eBPF statistics, BPF_STATS=1: run time / run count between bpf-before.json
  and bpf-after.json, summed over the runs) of the patched agent at most the
  shipped agent's + 10 %. No probe judged: fail.
- G4b: agent CPU seconds per 1,000 measured requests; the patched runs' mean
  within the min-max range of the shipped runs. Fewer than 3 runs on either
  side: not judged, and the gate fails.

Warm-up and tail requests are left out by truth id, as in timing_path.py.

Usage: stream_gate.py <patched-run>... --reference <shipped-run>... [--json]
"""

from __future__ import annotations

import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

from timing_path import TRUTH, _jsonl, _place, cell_meta

J1 = (("hop-1", "outbound"), ("fake-upstream", "inbound"))
PATCHED_PROBES = (
    "obi_uprobe_http2Server_processHeaders",
    "obi_uprobe_http2serverConn_newWriterAndRequest_returns",
    "obi_uprobe_http2serverConn_runHandler",
    "obi_uprobe_ServeHTTPReturns",
    "obi_uprobe_roundTripReturn",
    "obi_uprobe_http2WriteHeaders",
    "obi_uprobe_http2WriteHeaders_vendored",
)
PROBE_TOLERANCE = 0.10  # G4a, fixed in the spec before any gate run
MIN_RUNS = 3  # G4b


def j1_spans(cell: Path) -> dict[tuple[str, str], dict[str, dict]]:
    """J1's agent spans of measured requests, by side, then by truth id."""
    meta = cell_meta(cell)
    measured = {r["trace_id"] for r in _jsonl(cell / "lg.jsonl")}
    excluded = set()
    for path in [*glob.glob(str(cell / "warmup-*.jsonl")), cell / "tail.jsonl"]:
        excluded |= {r["trace_id"] for r in _jsonl(Path(path))}
    out: dict[tuple[str, str], dict[str, dict]] = {side: {} for side in J1}
    for line in (cell / "agent.out").read_text().splitlines():
        if not line.startswith("["):
            continue
        for span in json.loads(line):
            attrs = span.get("attributes", {})
            match = TRUTH.search(attrs.get("url", ""))
            if not match or match[1] in excluded or match[1] not in measured:
                continue
            place = _place(span, attrs, meta)
            if place in out:
                out[place][match[1]] = {
                    "tuple": (
                        span.get("peer"),
                        span.get("peerPort"),
                        span.get("host"),
                        span.get("hostPort"),
                    ),
                    "stream": int(attrs.get("http2StreamId", 0)),
                    "start": int(span["start"]),
                    "end": int(span["end"]),
                }
    return out


def _quantile(values: list[int], p: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


def gaps(spans: dict[tuple[str, str], dict[str, dict]]) -> dict[str, int | None]:
    up, down = spans[J1[0]], spans[J1[1]]
    both = sorted(set(up) & set(down))
    start = [down[t]["start"] - up[t]["start"] for t in both]
    end = [up[t]["end"] - down[t]["end"] for t in both]
    return {
        "start_p50": _quantile(start, 0.5),
        "start_p99": _quantile(start, 0.99),
        "end_p50": _quantile(end, 0.5),
        "end_p99": _quantile(end, 0.99),
    }


def stream_checks(cell: Path) -> dict:
    """G1, G2 and G3's four-tuple half on one run."""
    spans = j1_spans(cell)
    up, down = spans[J1[0]], spans[J1[1]]
    both = sorted(set(up) & set(down))
    with_both = [t for t in both if up[t]["stream"] and down[t]["stream"]]
    equal = [t for t in with_both if up[t]["stream"] == down[t]["stream"]]
    reused = 0
    for side in (up, down):
        owners = defaultdict(set)
        for truth, s in side.items():
            if s["stream"]:
                owners[s["tuple"], s["stream"]].add(truth)
        reused += sum(1 for v in owners.values() if len(v) > 1)
    same_tuple = sum(1 for t in both if up[t]["tuple"] == down[t]["tuple"])
    return {
        "run": cell.parent.name,
        "both_seen": len(both),
        "stream_on_both_sides": len(with_both),
        "stream_equal": len(equal),
        "G1": bool(both) and len(equal) == len(both),
        "keys_reused_by_two_requests": reused,
        "G2": bool(both) and reused == 0,
        "same_four_tuple": same_tuple,
        "G3_four_tuple": bool(both) and same_tuple == len(both),
        "gaps": gaps(spans),
    }


def probe_totals(cell: Path) -> dict[str, tuple[int, int]] | None:
    """Per patched probe: (run time ns, run count) while the measured requests
    ran; None without the kernel statistics of BPF_STATS=1."""
    before, after = cell / "bpf-before.json", cell / "bpf-after.json"
    if not (before.exists() and after.exists()):
        return None

    def sums(path: Path) -> dict[str, tuple[int, int]]:
        out: dict[str, tuple[int, int]] = {}
        for prog in json.loads(path.read_text()):
            name = prog.get("name")
            if name in PATCHED_PROBES:
                t, n = out.get(name, (0, 0))
                out[name] = (t + prog.get("run_time_ns", 0), n + prog.get("run_cnt", 0))
        return out

    b, a = sums(before), sums(after)
    return {
        name: (a[name][0] - b.get(name, (0, 0))[0], a[name][1] - b.get(name, (0, 0))[1])
        for name in a
    }


def probe_costs(cells: list[Path]) -> dict[str, float] | None:
    """Mean ns per run of each patched probe, summed over the runs."""
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for cell in cells:
        run = probe_totals(cell)
        if run is None:
            return None
        for name, (t, n) in run.items():
            totals[name][0] += t
            totals[name][1] += n
    return {name: t / n for name, (t, n) in totals.items() if n > 0}


def cpu_per_thousand(cell: Path) -> float | None:
    """Agent CPU seconds per 1,000 measured requests (pidstat, one-second samples)."""
    procs = cell / "cpu-procs.txt"
    requests = len(_jsonl(cell / "lg.jsonl"))
    if not procs.exists() or not requests:
        return None
    rows = [line.split() for line in procs.read_text().splitlines()]
    samples = [float(p[7]) for p in rows if len(p) >= 10 and p[2].isdigit() and p[9] == "agent"]
    return sum(samples) / 100 / requests * 1000


def gate(patched: list[Path], shipped: list[Path]) -> dict:
    runs = [stream_checks(cell) for cell in patched]
    ours, theirs = probe_costs(patched), probe_costs(shipped)
    judged = {}
    if ours is not None and theirs is not None:
        for name in PATCHED_PROBES:
            if name in ours and name in theirs:
                judged[name] = {
                    "patched_ns": round(ours[name], 1),
                    "shipped_ns": round(theirs[name], 1),
                    "ok": ours[name] <= theirs[name] * (1 + PROBE_TOLERANCE),
                }
    cpu_ours = [cpu_per_thousand(c) for c in patched]
    cpu_theirs = [cpu_per_thousand(c) for c in shipped]
    enough = (
        len(patched) >= MIN_RUNS
        and len(shipped) >= MIN_RUNS
        and None not in cpu_ours
        and None not in cpu_theirs
    )
    mean_ours = sum(cpu_ours) / len(cpu_ours) if enough else None
    g4b = enough and min(cpu_theirs) <= mean_ours <= max(cpu_theirs)
    return {
        "runs": runs,
        "reference_gaps": [gaps(j1_spans(c)) for c in shipped],
        "G1": bool(runs) and all(r["G1"] for r in runs),
        "G2": bool(runs) and all(r["G2"] for r in runs),
        "G3_four_tuple": bool(runs) and all(r["G3_four_tuple"] for r in runs),
        "probes": judged,
        "G4a": bool(judged) and all(p["ok"] for p in judged.values()),
        "cpu_per_1k_patched": cpu_ours,
        "cpu_per_1k_shipped": cpu_theirs,
        "G4b": bool(g4b),
    }


def main(argv: list[str]) -> None:
    args = [a for a in argv if a != "--json"]
    if "--reference" not in args:
        sys.exit(__doc__)
    i = args.index("--reference")
    patched = [Path(a) / "n1" for a in args[:i]]
    shipped = [Path(a) / "n1" for a in args[i + 1 :]]
    if not patched or not shipped:
        sys.exit(__doc__)
    result = gate(patched, shipped)
    if "--json" in argv:
        print(json.dumps(result, indent=1))
        return
    for r in result["runs"]:
        print(
            f"{r['run']}: J1 both seen {r['both_seen']}, stream equal {r['stream_equal']}, "
            f"keys reused {r['keys_reused_by_two_requests']}, "
            f"same four-tuple {r['same_four_tuple']}, gaps µs {r['gaps']}"
        )
    for g in result["reference_gaps"]:
        print(f"reference gaps µs {g}")
    for name, p in result["probes"].items():
        print(
            f"{name}: {p['patched_ns']} ns vs {p['shipped_ns']} ns "
            + ("ok" if p["ok"] else "TOO SLOW")
        )
    print(
        f"CPU s / 1,000 requests: patched {result['cpu_per_1k_patched']} "
        f"shipped {result['cpu_per_1k_shipped']}"
    )
    for g in ("G1", "G2", "G3_four_tuple", "G4a", "G4b"):
        print(f"{g}: {'PASS' if result[g] else 'FAIL'}")
    keys = ("G1", "G2", "G3_four_tuple", "G4a", "G4b")
    print("gate: " + ("PASS" if all(result[g] for g in keys) else "FAIL"))


if __name__ == "__main__":
    main(sys.argv[1:])
