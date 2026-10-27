#!/usr/bin/env python3
"""Run reconcileflow's exact path on a harness run, and score every link.

The chain of a cell is declared as a topology (phase 1): load-gen (its own log
of what it sent) -> hop-1 ... hop-n (agent) -> fake-upstream (agent). Each
record becomes an Observation; reconcileflow.correlation.exact links them on
equal trace ids. Only then is the truth id in each request's path read, to
score the links: it never reaches the module.

fake-upstream's own log is not a point: it would observe the same side of the
same point as the agent, so every request would be seen twice there.

Per hop and per crossing:
- correct: a link between two observations of the same request;
- wrong: a link between two different requests;
- lost: a measured request seen with the key on both sides, without a link;
- keyless: a measured request seen on both sides, the key missing on one;
- collisions: values carried by more than one observation on a side;
- self-authored: correct links whose value is not the id the client sent.

Usage: exact_path.py <run-dir> [--json]
"""

from __future__ import annotations

import glob
import json
import re
import sys
from collections import defaultdict
from itertools import pairwise
from pathlib import Path

from analyze_run import TRUTH

from reconcileflow.correlation.exact import Observation, exact_links
from reconcileflow.correlation.topology import Topology, load

SERVER, CLIENT = "SPAN_KIND_SERVER", "SPAN_KIND_CLIENT"
NO_TRACE = re.compile(r"^0*$")


def topology_for(hops: int, kinds: list[str], propagation: str) -> Topology:
    """The lab chain as a declared topology: what each point records and copies."""

    def timed(visibility: str, timestamps: str) -> dict:
        return {"host": "lab", "visibility": visibility, "clock": "lab", "timestamps": timestamps}

    points = {"load-gen": {**timed("app", "precise_sync"), "outbound": ["traceparent"]}}
    for k, kind in enumerate(kinds, start=1):
        point = {
            **timed("kernel", "kernel_monotonic"),
            "inbound": ["traceparent", "tuple5"],
            "outbound": ["traceparent", "tuple5"],
        }
        # Traefik forwards headers itself; a relay copies none, so only an agent
        # writing headers carries the context through it.
        if kind == "traefik":
            point["propagates"] = {"traceparent": "proxy"}
        elif propagation == "headers":
            point["propagates"] = {"traceparent": "agent"}
        points[f"hop-{k}"] = point
    points["fake-upstream"] = {
        **timed("kernel", "kernel_monotonic"),
        "inbound": ["traceparent", "tuple5"],
    }
    names = ["load-gen", *(f"hop-{k}" for k in range(1, hops + 1)), "fake-upstream"]
    hop_list = [{"from": a, "to": b, "pooling": True} for a, b in pairwise(names)]
    return load({"points": points, "hops": hop_list})


def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def cell_meta(cell_dir: Path) -> dict:
    """hops, hop kinds, fake-upstream port, propagation and mask of a cell."""
    if (cell_dir / "cell.json").exists():
        meta = json.loads((cell_dir / "cell.json").read_text())
        return {
            "hops": meta["hops"],
            "kinds": meta.get("hop_kinds") or [meta.get("hop_kind", "traefik")] * meta["hops"],
            "port": meta["fake_upstream_port"],
            "propagation": meta.get("context_propagation", "agent-default"),
            "masked": bool(meta.get("lg_mask", 0)),
        }
    # One-hop runs (h2-propagation.sh): load-gen -> fake-upstream, settings in cells.txt.
    line = next(
        (
            ln
            for ln in (cell_dir.parent / "cells.txt").read_text().splitlines()
            if ln.startswith(f"{cell_dir.name}:")
        ),
        "",
    )
    propagation = re.search(r"propagation=(\S+)", line)
    return {
        "hops": 0,
        "kinds": [],
        "port": int((cell_dir / "port").read_text()),
        "propagation": propagation[1] if propagation else "agent-default",
        "masked": "-mask" in line,
    }


def observations(cell_dir: Path, meta: dict) -> tuple[list[Observation], dict, set[str]]:
    """Observations of the measured requests, their truth, and the measured truth ids.

    Warm-up and tail traffic is dropped by truth id before anything else, as in
    hn_analyze.py. The truth map is returned apart: it is for scoring only.
    """
    rows = _jsonl(cell_dir / "lg.jsonl")
    measured = {r["trace_id"] for r in rows}
    excluded = set()
    for path in [*glob.glob(str(cell_dir / "warmup-*.jsonl")), cell_dir / "tail.jsonl"]:
        excluded |= {r["trace_id"] for r in _jsonl(Path(path))}
    out: list[Observation] = []
    truth: dict[tuple[str, int], str] = {}
    for seq, row in enumerate(rows):
        keys = {} if meta["masked"] else {"traceparent": row["trace_id"]}
        out.append(Observation("lg.jsonl", seq, "load-gen", "outbound", keys))
        truth["lg.jsonl", seq] = row["trace_id"]
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
            trace = span.get("traceID", "")
            keys = {} if NO_TRACE.match(trace) else {"traceparent": trace}
            out.append(Observation("agent.out", seq, *place, keys))
            truth["agent.out", seq] = match[1]
    return out, truth, measured


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


def score(topology: Topology, obs: list[Observation], truth: dict, measured: set[str]) -> dict:
    result = exact_links(topology, obs)
    joins = {f"{h.src} -> {h.dst}": h for h in topology.hops if h.key is not None}
    joins |= {c.point: c for c in topology.crossings if c.key is not None}
    sides = {}
    for where, item in joins.items():
        if " -> " in where:
            sides[where] = ((item.src, "outbound"), (item.dst, "inbound"))
        else:
            sides[where] = ((where, "inbound"), (where, "outbound"))
    seen_on = defaultdict(set)  # (point, side) -> truth ids observed there
    keyed_on = defaultdict(set)  # (point, side, key) -> truth ids observed with the key
    for o in obs:
        seen_on[o.point, o.side].add(truth[o.source, o.seq])
        for key in o.keys:
            keyed_on[o.point, o.side, key].add(truth[o.source, o.seq])
    report = {}
    for where in sorted(joins):
        links = [link for link in result.links if link.where == where]
        correct = [ln for ln in links if truth[ln.upstream] == truth[ln.downstream]]
        linked = {truth[ln.upstream] for ln in correct}
        up, down = sides[where]
        key = joins[where].key
        both = seen_on[up] & seen_on[down] & measured
        keyed = keyed_on[(*up, key)] & keyed_on[(*down, key)] & measured
        report[where] = {
            "key": key,
            "links": len(links),
            "correct": len(correct),
            "wrong": len(links) - len(correct),
            "lost": len(keyed - linked),
            "keyless": len(both - keyed),
            "both_seen": len(both),
            "collisions": sum(1 for c in result.collisions if c.where == where),
            "unlinked": sum(1 for w, _ in result.unlinked if w == where),
            "self_authored": sum(1 for ln in correct if ln.value != truth[ln.upstream]),
        }
    return {"joins": report, "duplicates_dropped": result.duplicates_dropped}


def analyze_cell(cell_dir: Path) -> dict:
    meta = cell_meta(cell_dir)
    topology = topology_for(meta["hops"], meta["kinds"], meta["propagation"])
    obs, truth, measured = observations(cell_dir, meta)
    return {"meta": meta, "measured": len(measured), **score(topology, obs, truth, measured)}


def cells(run_dir: Path) -> list[Path]:
    return sorted(p.parent for p in run_dir.glob("*/lg.jsonl"))


def main(argv: list[str]) -> None:
    run_dir = Path(argv[1])
    report = {cell.name: analyze_cell(cell) for cell in cells(run_dir)}
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    for name, cell in report.items():
        meta = cell["meta"]
        print(
            f"== {name}: hops={meta['hops']} kinds={','.join(meta['kinds']) or '-'} "
            f"propagation={meta['propagation']} masked={int(meta['masked'])} "
            f"measured={cell['measured']} duplicates_dropped={cell['duplicates_dropped']}"
        )
        print(
            f"   {'join':28s} {'links':>6s} {'correct':>8s} {'wrong':>6s} {'lost':>6s} "
            f"{'keyless':>8s} {'both':>6s} {'collis.':>8s} {'self-auth':>10s}"
        )
        for where, r in cell["joins"].items():
            print(
                f"   {where:28s} {r['links']:6d} {r['correct']:8d} {r['wrong']:6d} "
                f"{r['lost']:6d} {r['keyless']:8d} {r['both_seen']:6d} {r['collisions']:8d} "
                f"{r['self_authored']:10d}"
            )
    wrong = sum(r["wrong"] for c in report.values() for r in c["joins"].values())
    print(f"wrong links in the run: {wrong}")


if __name__ == "__main__":
    main(sys.argv)
