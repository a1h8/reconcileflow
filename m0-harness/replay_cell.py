#!/usr/bin/env python3
"""Replay kept harness cells through reconcileflow's stream correlation, and
score every final decision.

Each record of a cell becomes a journal entry: the observation, at the time its
request ended. The truth id in each request's path is read only to score the
outcome and to inject a fault: it never reaches the engine as a key.

A hop gets its second signal, the connection and the HTTP/2 stream, only if its
data can carry one. The decision is made per hop and reported with its reason:
- load-gen records a local ordinal, not the protocol's stream number;
- every entry port reported must be the port the point listens on: a constant
  pair of ports proves nothing, since a pooled connection has one legitimately;
- a stream number must exist on both ends.

The bench runs on one host, so the connection key is the two ports.

The journal is rebuilt in event order: a cell does not say when each observation
reached a collector. Disorder and faults are injected, never observed. What
falls past the declared window is counted, never dropped.

Usage: replay_cell.py <run-dir> [--window-us W] [--shift-us S] [--seed N]
       [--reuse-every K] [--hide POINT:SIDE] [--json]
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import random
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path

from analyze_run import TRUTH
from exact_path import NO_TRACE, _jsonl, _place, cell_meta, cells, topology_for

from reconcileflow.correlation.exact import Identity, Observation
from reconcileflow.correlation.flow import (
    Advance,
    FlowConfig,
    FlowResult,
    Kind,
    Seen,
    Status,
    replay_flow,
)
from reconcileflow.correlation.topology import Topology

NO_REAL_STREAM = "load-gen records no protocol stream number"
FALSE_ENTRY_PORT = "the entry port reported differs from the listening port"
NO_LISTENING_PORT = "no listening port declared"
NO_STREAM = "no stream number on both ends"

DEFAULT_WINDOW_US = 1_000_000


@dataclass(frozen=True, slots=True)
class Faults:
    reuse_every: int = 0  # every k-th request carries the previous one's id, past load-gen
    hide: tuple[str, str] | None = None  # (point, side) whose observations are not delivered
    shift_us: int = 0  # delivery shifted by less than this, tolerated as lateness
    seed: int = 0


NO_FAULTS = Faults()


@dataclass(frozen=True, slots=True)
class _Row:
    source: str
    seq: int
    point: str
    side: str
    truth: str
    trace: str | None
    end_us: int
    client_port: str | None = None
    server_port: str | None = None
    stream: str | None = None


@dataclass(frozen=True, slots=True)
class Cell:
    meta: dict
    topology: Topology
    config: FlowConfig
    journal: tuple
    truth: dict[Identity, str]  # observation -> request; never given to the engine
    signals: dict[str, str | None]  # hop -> why it has no second signal (None: it has one)
    measured: int
    reused: frozenset[Identity]


def _rows(cell_dir: Path, meta: dict) -> tuple[list[_Row], list[str]]:
    """Every measured record, and the measured requests in load-gen's order.

    Warm-up and tail traffic is dropped by truth id first, as in exact_path.py.
    """
    log = _jsonl(cell_dir / "lg.jsonl")
    order = [r["trace_id"] for r in log]
    measured = set(order)
    excluded = set()
    for path in [*glob.glob(str(cell_dir / "warmup-*.jsonl")), cell_dir / "tail.jsonl"]:
        excluded |= {r["trace_id"] for r in _jsonl(Path(path))}
    rows = [
        _Row(
            "lg.jsonl",
            seq,
            "load-gen",
            "outbound",
            r["trace_id"],
            None if meta["masked"] else r["trace_id"],
            (r["timestamp_ns"] + r.get("duration_ns", 0)) // 1000,
        )
        for seq, r in enumerate(log)
    ]
    # The earliest kept runs name this file after the agent.
    agent_out = next(
        (p for p in (cell_dir / "agent.out", cell_dir / "obi.out") if p.exists()), None
    )
    lines = agent_out.read_text().splitlines() if agent_out else []
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
            stream = attrs.get("http2StreamId")
            rows.append(
                _Row(
                    "agent.out",
                    seq,
                    *place,
                    match[1],
                    None if NO_TRACE.match(trace) else trace,
                    int(span["end"]),
                    span.get("peerPort"),
                    span.get("hostPort"),
                    stream if stream and stream != "0" else None,
                )
            )
    return rows, [t for t in order if t not in excluded]


def _chain(hops: int) -> list[str]:
    return ["load-gen", *(f"hop-{k}" for k in range(1, hops + 1)), "fake-upstream"]


def _listening_port(point: str, meta: dict, hop_ports: list[int]) -> int | None:
    if point == "fake-upstream":
        return meta["port"]
    k = int(point.removeprefix("hop-"))
    return hop_ports[k - 1] if k <= len(hop_ports) else None


def _second_signals(rows: list[_Row], meta: dict, hop_ports: list[int]) -> dict[str, str | None]:
    """For each hop, why its data cannot carry a second signal (None: they can)."""
    out = {}
    for src, dst in pairwise(_chain(meta["hops"])):
        where = f"{src} -> {dst}"
        port = None if src == "load-gen" else _listening_port(dst, meta, hop_ports)
        sends = [r for r in rows if r.point == src and r.side == "outbound"]
        receives = [r for r in rows if r.point == dst and r.side == "inbound"]
        if src == "load-gen":
            out[where] = NO_REAL_STREAM
        elif port is None:
            out[where] = NO_LISTENING_PORT
        elif any(r.server_port != str(port) for r in receives):
            out[where] = FALSE_ENTRY_PORT
        elif not (any(r.stream for r in sends) and any(r.stream for r in receives)):
            out[where] = NO_STREAM
        else:
            out[where] = None
    return out


def _signal_hop(row: _Row, hops: int) -> str | None:
    """The hop this side of a point is an end of."""
    chain = _chain(hops)
    i = chain.index(row.point)
    if row.side == "outbound":
        return f"{row.point} -> {chain[i + 1]}"
    return f"{chain[i - 1]} -> {row.point}"


def load_cell(cell_dir: Path, window_us: int, faults: Faults = NO_FAULTS) -> Cell:
    """The cell as a topology, a configuration and a journal, with the truth apart."""
    cell_dir = Path(cell_dir)
    meta = cell_meta(cell_dir)
    raw = cell_dir / "cell.json"
    hop_ports = json.loads(raw.read_text()).get("hop_ports") or [] if raw.exists() else []
    rows, order = _rows(cell_dir, meta)
    signals = _second_signals(rows, meta, hop_ports)
    admitted = {where for where, reason in signals.items() if reason is None}
    if faults.hide is not None:
        kept = [r for r in rows if (r.point, r.side) != faults.hide]
        if len(kept) == len(rows):
            raise ValueError(f"nothing to hide at {faults.hide!r}")
        rows = kept
    carried = {}
    if faults.reuse_every:
        for i in range(faults.reuse_every, len(order), faults.reuse_every):
            carried[order[i]] = order[i - 1]
    entries: list[tuple[float, Seen]] = []
    truth: dict[Identity, str] = {}
    reused = set()
    deliver = random.Random(faults.seed)
    for row in sorted(rows, key=lambda r: (r.end_us, r.source, r.seq)):
        identity = (row.source, row.seq)
        truth[identity] = row.truth
        keys = {}
        if row.point != "load-gen" and row.truth in carried:
            keys["traceparent"] = carried[row.truth]
            reused.add(identity)
        elif row.trace is not None:
            keys["traceparent"] = row.trace
        if _signal_hop(row, meta["hops"]) in admitted:
            keys["tuple5"] = f"{row.client_port}>{row.server_port}"
            if row.stream:
                keys["h2_stream_id"] = row.stream
        observation = Observation(row.source, row.seq, row.point, row.side, keys)
        delay = deliver.random() * faults.shift_us if faults.shift_us else 0
        entries.append((row.end_us + delay, Seen(observation, row.end_us)))
    entries.sort(key=lambda e: e[0])
    sources = sorted({r.source for r in rows})
    end = max(r.end_us for r in rows) + window_us + faults.shift_us + 1
    journal = (*(seen for _, seen in entries), *(Advance(s, end) for s in sources))
    topology = topology_for(
        meta["hops"], meta["kinds"], meta["propagation"], second_signal=admitted
    )
    config = FlowConfig(tuple(sources), window_us, lateness_us=faults.shift_us)
    return Cell(meta, topology, config, journal, truth, signals, len(order), frozenset(reused))


def score(cell: Cell, result: FlowResult) -> dict:
    """Judge the final decisions against the truth the engine never saw."""
    truth = cell.truth
    joins = {f"{h.src} -> {h.dst}": h for h in cell.topology.hops if h.key or h.observed}
    joins |= {c.point: c for c in cell.topology.crossings if c.key}
    report = {
        where: dict.fromkeys(
            ("linked", "correct", "wrong", "collided", "unlinked", "conflicted"), 0
        )
        for where in joins
    }
    effective = result.effective()
    finals = {k: d for k, d in effective.items() if d.status is Status.FINAL}
    names = {
        Kind.COLLIDED: "collided",
        Kind.UNLINKED: "unlinked",
        Kind.CONFLICTED: "conflicted",
    }
    for (where, identity), d in finals.items():
        if d.kind is Kind.LINKED:
            right = {truth[p] for p in d.partners} == {truth[identity]}
            report[where]["linked"] += 1
            report[where]["correct" if right else "wrong"] += 1
        elif d.kind in names:
            report[where][names[d.kind]] += 1
    corroboration: dict[str, int] = {}
    for (_, c), count in result.signal_counts().items():
        corroboration[c.value] = corroboration.get(c.value, 0) + count
    on_reused = [d for (_, identity), d in finals.items() if identity in cell.reused]
    return {
        "hops": cell.meta["hops"],
        "measured": cell.measured,
        "window_us": cell.config.window_us,
        "lateness_us": cell.config.lateness_us,
        "signals": cell.signals,
        "joins": report,
        "wrong": sum(j["wrong"] for j in report.values()),
        "open": sum(d.status is not Status.FINAL for d in effective.values()),
        "after_window": sum(d.kind is Kind.VALUE_AFTER_FINAL for d in finals.values()),
        "late": sum(d.kind is Kind.LATE for d in finals.values()),
        "corroboration": corroboration,
        "duplicates": result.duplicates,
        "dossiers": len(result.dossiers),
        "reuse": {
            "injected": len({truth[i] for i in cell.reused}),
            "linked": sum(d.kind is Kind.LINKED for d in on_reused),
            "collided": sum(d.kind is Kind.COLLIDED for d in on_reused),
        },
        "digest": hashlib.sha256(repr((result.decisions, result.dossiers)).encode()).hexdigest(),
        "finals": sorted(
            (repr(k), d.kind.value, d.value, repr(d.partners)) for k, d in finals.items()
        ),
    }


def analyze_cell(cell_dir: Path, window_us: int, faults: Faults = NO_FAULTS) -> dict:
    cell = load_cell(cell_dir, window_us, faults)
    return score(cell, replay_flow(cell.topology, cell.journal, cell.config))


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--window-us", type=int, default=DEFAULT_WINDOW_US)
    parser.add_argument("--shift-us", type=int, default=0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--reuse-every", type=int, default=0)
    parser.add_argument("--hide", help="POINT:SIDE whose observations are not delivered")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    hide = tuple(args.hide.split(":", 1)) if args.hide else None
    faults = Faults(args.reuse_every, hide, args.shift_us, args.seed)
    report = {c.name: analyze_cell(c, args.window_us, faults) for c in cells(args.run_dir)}
    for cell in report.values():
        del cell["finals"]
    if args.json:
        print(json.dumps(report, indent=2))
        return
    for name, cell in report.items():
        print(
            f"== {name}: hops={cell['hops']} measured={cell['measured']} open={cell['open']} "
            f"after_window={cell['after_window']} late={cell['late']} "
            f"digest={cell['digest'][:16]}"
        )
        for where, reason in cell["signals"].items():
            print(f"   second signal {where}: {reason or 'yes'}")
        print(f"   corroboration: {cell['corroboration']}")
        for where, j in cell["joins"].items():
            print(f"   {where:28s} {j}")
    print(f"wrong links in the run: {sum(c['wrong'] for c in report.values())}")


if __name__ == "__main__":
    main(sys.argv[1:])
