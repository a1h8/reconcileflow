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

Each component is resolved exactly (max-weight assignment), not greedily, and
a pair whose removal leaves the optimum unchanged is reported AMBIGUOUS rather
than picked by sort order. A component larger than ``max_component`` is not
guessed at: all its records are reported OVERSIZED, same stance as the
engine's BLOCK_TOO_LARGE.
"""

from __future__ import annotations

import bisect
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from reconcileflow.models import Match, Record, Tolerance
from reconcileflow.rules import m2_tolerant

_PLACEHOLDER_DATE = date(2000, 1, 1)
_WEIGHT_SCALE = 1_000_000  # rules quantise scores to 1e-6, so this is exact
_FORBIDDEN = 10**15


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


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:  # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # Smaller id becomes the root: the partition is independent of
            # edge order, so components come out the same on any input order.
            if rb < ra:
                ra, rb = rb, ra
            self.parent[rb] = ra


def components(edges: list[Match]) -> list[list[Match]]:
    """Connected components of the bipartite candidate graph, deterministically ordered."""
    uf = _UnionFind()
    for e in edges:
        uf.union("L:" + e.left_id, "R:" + e.right_ids[0])
    groups: dict[str, list[Match]] = defaultdict(list)
    for e in edges:
        groups[uf.find("L:" + e.left_id)].append(e)
    return [sorted(groups[k], key=lambda e: (e.left_id, e.right_ids)) for k in sorted(groups)]


def _min_cost_assignment(cost: list[list[int]]) -> tuple[int, list[int]]:
    """Hungarian algorithm, O(n^3), square matrix. Returns (total, row -> column)."""
    n = len(cost)
    u, v = [0] * (n + 1), [0] * (n + 1)
    p, way = [0] * (n + 1), [0] * (n + 1)
    for i in range(1, n + 1):
        p[0], j0 = i, 0
        minv, used = [_FORBIDDEN * 4] * (n + 1), [False] * (n + 1)
        while p[j0]:
            used[j0] = True
            i0, delta, j1 = p[j0], _FORBIDDEN * 4, 0
            for j in range(1, n + 1):
                if not used[j]:
                    cur = cost[i0 - 1][j - 1] - u[i0] - v[j]
                    if cur < minv[j]:
                        minv[j], way[j] = cur, j0
                    if minv[j] < delta:
                        delta, j1 = minv[j], j
            for j in range(n + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
        while j0:
            j1 = way[j0]
            p[j0], j0 = p[j1], j1
    row_to_col = [0] * n
    for j in range(1, n + 1):
        if p[j]:
            row_to_col[p[j] - 1] = j - 1
    return sum(cost[i][row_to_col[i]] for i in range(n)), row_to_col


def _solve(lefts: list[str], rights: list[str], weight: dict[tuple[str, str], int]):
    """Max-weight bipartite matching where leaving a record unmatched is allowed.

    Padded to (L+R) x (L+R): each left may take its own dummy column, each right
    its own dummy row, both at cost 0. Non-edges are forbidden.
    """
    nl, nr = len(lefts), len(rights)
    size = nl + nr
    cost = [[_FORBIDDEN] * size for _ in range(size)]
    for i, left in enumerate(lefts):
        for j, right in enumerate(rights):
            if (left, right) in weight:
                cost[i][j] = -weight[(left, right)]
        cost[i][nr + i] = 0
    for j in range(nr):
        cost[nl + j][j] = 0
        for k in range(nl):
            cost[nl + j][nr + k] = 0
    total, row_to_col = _min_cost_assignment(cost)
    pairs = [(lefts[i], rights[row_to_col[i]]) for i in range(nl) if row_to_col[i] < nr]
    return total, pairs


def resolve(component: list[Match], margin: Decimal = Decimal(0)) -> tuple[list[Match], set[str]]:
    """Optimal assignment of one component, plus the left ids it cannot decide.

    ``margin`` is a score difference (same unit as ``Match.score``): an alternative
    assignment within ``margin`` of the optimum is treated as just as good. With
    measurement noise, an exact tie is too weak a test -- a near tie is a guess.
    """
    by_pair = {(e.left_id, e.right_ids[0]): e for e in component}
    weight = {k: int(e.score * _WEIGHT_SCALE) for k, e in by_pair.items()}
    lefts = sorted({k[0] for k in by_pair})
    rights = sorted({k[1] for k in by_pair})
    best, pairs = _solve(lefts, rights, weight)
    slack = int(margin * _WEIGHT_SCALE)
    chosen, ambiguous = [], set()
    for pair in pairs:
        # Unique optimum test: forbid this exact pair; if another assignment
        # comes within the margin of the same total, the data do not decide.
        without = {k: w for k, w in weight.items() if k != pair}
        alternative, _ = _solve(lefts, rights, without)
        if alternative - best <= slack:
            ambiguous.add(pair[0])
        else:
            chosen.append(by_pair[pair])
    return chosen, ambiguous


def correlate(
    lefts: list[Event],
    rights: list[Event],
    window: int,
    tol: Tolerance,
    max_component: int = 32,
    margin: Decimal = Decimal(0),
) -> Result:
    result = Result()
    edges = candidate_edges(lefts, rights, window, tol)
    for e in edges:
        result.candidates.setdefault(e.left_id, set()).add(e.right_ids[0])
    for component in components(edges):
        left_ids = {e.left_id for e in component}
        right_ids = {e.right_ids[0] for e in component}
        result.component_sizes.append(len(left_ids) + len(right_ids))
        if max(len(left_ids), len(right_ids)) > max_component:
            result.oversized |= left_ids
            continue
        chosen, ambiguous = resolve(component, margin)
        result.matches.extend(chosen)
        result.ambiguous |= ambiguous
    result.matches.sort(key=lambda m: m.left_id)
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
