#!/usr/bin/env python3
"""Score every observation point of the cells written by scripts/hn-propagation.sh.

For a chain of n hops the points are, in order along the request: for each hop
k, its inbound server span (``serverAddr = hop-k``) and its outbound client span
(``clientAddr = hop-k``); then fake-upstream's inbound server span. Scoring is
analyze_run.evaluate_json, with warm-up and tail traffic excluded by id.

The "wire" check is independent of the agent's spans: what fake-upstream itself
read from the headers it received -- the traceparent trace id and, when load-gen
sent one, the baggage. fake-upstream's records are joined to load-gen's by the
truth id in the request path, never by trace id, so the check still holds when
traceparent is lost on the way. Cells run without an agent have no points,
only the wire check.

Usage: hn_analyze.py <run-dir> [--json]
"""

from __future__ import annotations

import glob
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from analyze_run import TRUTH, evaluate_json

SERVER, CLIENT = "SPAN_KIND_SERVER", "SPAN_KIND_CLIENT"


def _ids(paths) -> set[str]:
    out: set[str] = set()
    for path in paths:
        with open(path) as f:
            out |= {json.loads(line)["trace_id"] for line in f if line.strip()}
    return out


def points(hops: int, fake_upstream_port: int) -> list[tuple[str, dict]]:
    """Observation points along the request, as (label, evaluate_json selection)."""
    out = []
    for k in range(1, hops + 1):
        out.append((f"hop-{k} in", {"kind": SERVER, "attrs": {"serverAddr": f"hop-{k}"}}))
        out.append((f"hop-{k} out", {"kind": CLIENT, "attrs": {"clientAddr": f"hop-{k}"}}))
    out.append(("fake-upstream in", {"kind": SERVER, "port": fake_upstream_port}))
    return out


def wire(rows: list[dict], fu_records: list[dict]) -> dict:
    """What fake-upstream read from the headers, per measured request.

    Joined by the truth id in fake-upstream's logged path. Records from runs
    that predate the path field fall back to a trace-id count.
    """
    by_id = {r["trace_id"]: r for r in rows}
    if not any(rec.get("path") for rec in fu_records):
        seen = Counter(rec.get("trace_id", "") for rec in fu_records)
        return {"received_once": sum(1 for t in by_id if seen[t] == 1), "measured": len(by_id)}
    received: dict[str, list[dict]] = {}
    for rec in fu_records:
        match = TRUTH.search(rec.get("path", ""))
        if match and match[1] in by_id:
            received.setdefault(match[1], []).append(rec)
    once = {t: recs[0] for t, recs in received.items() if len(recs) == 1}
    out = {
        "measured": len(by_id),
        "reached_once": len(once),
        "received_once": sum(1 for t, rec in once.items() if rec.get("trace_id") == t),
        "traceparent_absent": sum(1 for rec in once.values() if not rec.get("trace_id")),
    }
    if any(r.get("baggage") for r in rows):
        out["baggage_exact"] = sum(
            1 for t, rec in once.items() if rec.get("baggage") == by_id[t]["baggage"]
        )
        out["baggage_absent"] = sum(1 for rec in once.values() if not rec.get("baggage"))
    return out


def analyze_cell(cell_dir: Path) -> dict:
    meta = json.loads((cell_dir / "cell.json").read_text())
    with open(cell_dir / "lg.jsonl") as f:
        rows = [json.loads(line) for line in f if line.strip()]
    excluded = frozenset(
        _ids([*glob.glob(str(cell_dir / "warmup-*.jsonl")), cell_dir / "tail.jsonl"])
    )
    agent_out = cell_dir / "agent.out"
    lines = agent_out.read_text().splitlines(keepends=True) if agent_out.exists() else []
    selections = points(meta["hops"], meta["fake_upstream_port"]) if lines else []
    scored = []
    for label, selection in selections:
        result = evaluate_json(
            rows,
            lines,
            port=selection.get("port"),
            kind=selection["kind"],
            excluded=excluded,
            attrs=selection.get("attrs"),
        )
        scored.append((label, result))
    with open(cell_dir / "fu.jsonl") as f:
        fu_records = [json.loads(line) for line in f if line.strip()]
    return {"meta": meta, "points": scored, "wire": wire(rows, fu_records)}


def first_failure(scored: list[tuple[str, dict]]) -> str | None:
    for label, r in scored:
        if r["status"] != "VALID" or any(
            r[key] for key in ("wrong_known_trace", "unknown_trace", "missing_requests")
        ):
            return label
    return None


def main(argv: list[str]) -> None:
    run_dir = Path(argv[1])
    cells = sorted(p.parent for p in run_dir.glob("*/cell.json"))
    report = {}
    for cell_dir in cells:
        result = analyze_cell(cell_dir)
        report[cell_dir.name] = result
        if "--json" in argv:
            continue
        meta = result["meta"]
        print(
            f"== {cell_dir.name}: hops={meta['hops']} kind={meta.get('hop_kind', 'traefik')} "
            f"strip_hop={meta['strip_hop']} agent={meta.get('agent', '?')} "
            f"propagation={meta.get('context_propagation', 'agent-default')}"
        )
        print(
            f"   {'point':18s} {'status':7s} {'correct':>8s} {'swapped':>8s} {'unknown':>8s} "
            f"{'missing':>8s} {'dup':>5s} {'selected':>9s} {'excluded':>9s}"
        )
        for label, r in result["points"]:
            print(
                f"   {label:18s} {r['status']:7s} {r['correct']:8d} {r['wrong_known_trace']:8d} "
                f"{r['unknown_trace']:8d} {r['missing_requests']:8d} {r['duplicate_requests']:5d} "
                f"{r['selected_events']:9d} {r['excluded_outside_run']:9d}"
            )
        w = result["wire"]
        line = f"   wire: fake-upstream read the right id once for {w['received_once']}"
        line += f"/{w['measured']}"
        if "reached_once" in w:
            line += (
                f" (reached once {w['reached_once']}, traceparent absent {w['traceparent_absent']})"
            )
        print(line)
        if "baggage_exact" in w:
            print(
                f"   wire: baggage exact {w['baggage_exact']}/{w['measured']}, "
                f"absent {w['baggage_absent']}"
            )
        print(f"   first point not clean: {first_failure(result['points']) or 'none'}")
    if "--json" in argv:
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main(sys.argv)
