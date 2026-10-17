#!/usr/bin/env python3
"""Union-find correlator: blocks built from the candidate graph, not from ranges.

`correlate_by_duration.py --bucket N` partitions both sides into fixed,
disjoint ranges and loses every true pair that straddles a boundary (T=65-70%).
Its sliding window keeps those pairs but gives up on blocks entirely.

This keeps blocks and drops the boundaries. An edge links a left (load-gen)
record to a right (OBI) record only when BOTH constraints hold: time within
``window`` and duration within ``tol`` (scored by the engine's own
`rules.m2_tolerant`). Union-find over those edges gives the connected
components: disjoint by construction, so the engine's one-block-per-record
invariant holds, and a true pair can never be split across two of them.

Unioning on overlapping time windows alone would not work: at c=200 the gap
between consecutive events (~0.5 ms) is far below any usable window, so every
window overlaps the next and the whole run collapses into one component. The
duration constraint is what keeps components small.

Components, exact assignment and ambiguity are reconcileflow's own
(`reconcileflow.correlation.assign`); this script only builds the candidate
edges and scores the result against the truth. A record the data do not decide
is AMBIGUOUS rather than picked by sort order, including one left alone by the
optimum while another assignment as good would match it. A component larger
than ``max_component`` is not guessed at: all its records are OVERSIZED, same
stance as the engine's BLOCK_TOO_LARGE.
"""

from __future__ import annotations

import bisect
import math
import sys
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from reconcileflow.correlation import Edge, Status, assign
from reconcileflow.models import Match, Record, Tolerance
from reconcileflow.rules import m2_tolerant

_PLACEHOLDER_DATE = date(2000, 1, 1)
_WEIGHT_SCALE = 1_000_000  # rules quantise scores to 1e-6, so this is exact


@dataclass(frozen=True, slots=True)
class Event:
    id: str
    t: int  # comparable time coordinate on both sides (ns, or rank)
    duration_ns: int


@dataclass
class Result:
    matches: list[Match] = field(default_factory=list)
    ambiguous: set[str] = field(default_factory=set)  # left ids
    oversized: set[str] = field(default_factory=set)  # left ids
    component_sizes: list[int] = field(default_factory=list)  # records per component
    candidates: dict[str, set[str]] = field(default_factory=dict)  # left id -> right ids


def _record(event: Event) -> Record:
    return Record(
        id=event.id, account="1", amount=Decimal(event.duration_ns), value_date=_PLACEHOLDER_DATE
    )


def candidate_edges(
    lefts: list[Event], rights: list[Event], window: int, tol: Tolerance
) -> list[Match]:
    """Every (left, right) pair inside the time window AND the duration tolerance."""
    rights = sorted(rights, key=lambda e: (e.t, e.id))
    times = [e.t for e in rights]
    right_records = [_record(e) for e in rights]
    edges: list[Match] = []
    for left in sorted(lefts, key=lambda e: e.id):
        lo = bisect.bisect_left(times, left.t - window)
        hi = bisect.bisect_right(times, left.t + window)
        edges.extend(m2_tolerant([_record(left)], right_records[lo:hi], tol))
    return edges


def correlate(
    lefts: list[Event],
    rights: list[Event],
    window: int,
    tol: Tolerance,
    max_component: int = 32,
    margin: Decimal = Decimal(0),
) -> Result:
    """Candidate edges from time and duration, decided by reconcileflow's assignment core.

    ``margin`` is a score difference (same unit as ``Match.score``). Scores are
    quantised to 1e-6, so the conversion to integer weights is exact.
    """
    result = Result()
    edges = candidate_edges(lefts, rights, window, tol)
    by_pair = {(e.left_id, e.right_ids[0]): e for e in edges}
    for left, right in by_pair:
        result.candidates.setdefault(left, set()).add(right)
    decided = assign(
        [e.id for e in lefts],
        [e.id for e in rights],
        [Edge(left, right, int(m.score * _WEIGHT_SCALE)) for (left, right), m in by_pair.items()],
        margin=int(margin * _WEIGHT_SCALE),
        max_component=max_component,
    )
    result.component_sizes = [len(c.left) + len(c.right) for c in decided.components]
    for d in decided.left:
        if d.status is Status.MATCHED:
            result.matches.append(by_pair[d.id, d.partner])
        elif d.status is Status.AMBIGUOUS:
            result.ambiguous.add(d.id)
        elif d.status is Status.OVERSIZED:
            result.oversized.add(d.id)
    return result


def _percentile(values: list[int], q: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]  # nearest rank


def evaluate(result: Result, truth: dict[str, str], total_left: int) -> dict:
    """R, T and the unresolved split, every denominator = all left records.

    ``truth`` maps a left id to its true right id (absent: no true counterpart
    was observed). R is "the true right is among this left's candidates".
    """
    recalled = sum(1 for left, right in truth.items() if right in result.candidates.get(left, ()))
    correct = sum(1 for m in result.matches if truth.get(m.left_id) == m.right_ids[0])
    return {
        "total_left": total_left,
        "R": recalled / total_left if total_left else 0.0,
        "T": correct / total_left if total_left else 0.0,
        "matched": len(result.matches),
        "wrong": len(result.matches) - correct,
        "ambiguous": len(result.ambiguous),
        "oversized": len(result.oversized),
        "no_candidate": total_left - len(result.candidates),
        "component_p50": _percentile(result.component_sizes, 0.50),
        "component_p95": _percentile(result.component_sizes, 0.95),
        "component_max": max(result.component_sizes, default=0),
    }


def main(argv: list[str]) -> None:
    """Same inputs and time proxy (rank) as `correlate_by_duration.py --window`."""
    from correlate_by_duration import _sorted_sides

    obi_log, load_gen_jsonl = argv[1], argv[2]
    start, end, window = int(argv[3]), int(argv[4]), int(argv[5])
    rows, observed = _sorted_sides(obi_log, load_gen_jsonl, start, end)
    lefts = [Event(r["trace_id"], i, r["duration_ns"]) for i, r in enumerate(rows)]
    rights = [Event(f"obi-{i}", i, dur) for i, (dur, _tid) in enumerate(observed)]
    truth = {tid: f"obi-{i}" for i, (_dur, tid) in enumerate(observed)}
    for abs_ns in (500_000, 1_000_000, 2_000_000, 5_000_000):
        tol = Tolerance(amount_abs=Decimal(abs_ns), date_days=0, max_aggregate_size=1)
        metrics = evaluate(correlate(lefts, rights, window, tol), truth, len(lefts))
        print(f"window=±{window} tol={abs_ns}ns: {metrics}")


if __name__ == "__main__":
    main(sys.argv)
