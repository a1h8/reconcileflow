"""Assignment: from a candidate graph to one decision per record.

The caller builds the candidate edges (time windows, nesting, durations: a
hop-specific matter). This module only decides which records go together, and
says so when the data do not decide.

1. Connected components of the candidate graph: disjoint by construction, so
   a true pair is never split, and each component is solved on its own.
2. Each component gets an optimal assignment (Hungarian algorithm): a greedy
   pick strands records an exact solution would match.
3. Each record is then **decided** only if its partner, or the absence of one,
   is the same in every assignment within ``margin`` of the optimum. Otherwise
   it is AMBIGUOUS: a near tie is a guess, not a fact.

Every record passed in leaves with exactly one decision, both sides included:
a record cannot silently fall out of the counts. Weights are integers, so the
arithmetic is exact, and the result does not depend on input order (records,
components and edges are all put in a total order first).
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import IntEnum

# Keeps every sum exact and bounded; scores quantised to 1e-6 fit with room.
MAX_WEIGHT = 10**9


@dataclass(frozen=True, slots=True)
class Edge:
    """A candidate pair. ``weight`` > 0: larger is more plausible."""

    left: str
    right: str
    weight: int


class Status(IntEnum):
    """One status per record, mutually exclusive."""

    MATCHED = 1  # same partner in every near-optimal assignment
    UNMATCHED = 2  # no partner in any near-optimal assignment
    AMBIGUOUS = 3  # the near-optimal assignments disagree
    OVERSIZED = 4  # component above max_component: reported, not resolved
    NO_CANDIDATE = 5  # no edge at all
    DENSE = 6  # set by the timing path's local density guard, never by assign()


@dataclass(frozen=True, slots=True)
class Decision:
    id: str
    status: Status
    partner: str | None  # set only when MATCHED
    component: int | None  # index into Result.components; None if NO_CANDIDATE


@dataclass(frozen=True, slots=True)
class Component:
    left: tuple[str, ...]
    right: tuple[str, ...]
    best: int | None  # optimal total weight; None if OVERSIZED


@dataclass(frozen=True, slots=True)
class Result:
    left: tuple[Decision, ...]  # sorted by id
    right: tuple[Decision, ...]  # sorted by id
    components: tuple[Component, ...]  # ordered by smallest record


def assign(
    lefts: Iterable[str],
    rights: Iterable[str],
    edges: Iterable[Edge],
    *,
    margin: int = 0,
    max_component: int = 32,
) -> Result:
    """Decide every record of ``lefts`` and ``rights`` from the candidate ``edges``.

    ``margin`` is in weight units: an assignment whose total is at least the
    optimum minus ``margin`` counts as just as good. ``max_component`` bounds
    the records per side of a component that is solved.
    """
    lefts, rights, weights = _validated(lefts, rights, edges, margin, max_component)
    decided: dict[tuple[str, str], Decision] = {}
    components: list[Component] = []
    for index, (left_ids, right_ids) in enumerate(_components(weights)):
        if max(len(left_ids), len(right_ids)) > max_component:
            components.append(Component(left_ids, right_ids, None))
            for side, ids in (("L", left_ids), ("R", right_ids)):
                for x in ids:
                    decided[side, x] = Decision(x, Status.OVERSIZED, None, index)
            continue
        best, left_dec, right_dec = _resolve(left_ids, right_ids, weights, margin)
        components.append(Component(left_ids, right_ids, best))
        for x, (status, partner) in left_dec.items():
            decided["L", x] = Decision(x, status, partner, index)
        for x, (status, partner) in right_dec.items():
            decided["R", x] = Decision(x, status, partner, index)
    return Result(
        left=tuple(_decision(decided, "L", x) for x in lefts),
        right=tuple(_decision(decided, "R", x) for x in rights),
        components=tuple(components),
    )


def _decision(decided, side: str, x: str) -> Decision:
    return decided.get((side, x)) or Decision(x, Status.NO_CANDIDATE, None, None)


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _validated(lefts, rights, edges, margin, max_component):
    if not _is_int(margin) or margin < 0:
        raise ValueError(f"margin must be an integer >= 0, got {margin!r}")
    if not _is_int(max_component) or max_component < 1:
        raise ValueError(f"max_component must be an integer >= 1, got {max_component!r}")
    lefts, rights = list(lefts), list(rights)
    for side, ids in (("left", lefts), ("right", rights)):
        if len(set(ids)) != len(ids):
            raise ValueError(f"duplicate {side} id")
    known_l, known_r = set(lefts), set(rights)
    weights: dict[tuple[str, str], int] = {}
    for e in edges:
        if e.left not in known_l:
            raise ValueError(f"unknown left id in edge: {e.left!r}")
        if e.right not in known_r:
            raise ValueError(f"unknown right id in edge: {e.right!r}")
        if (e.left, e.right) in weights:
            raise ValueError(f"duplicate edge: {e.left!r} -> {e.right!r}")
        if not _is_int(e.weight) or not 1 <= e.weight <= MAX_WEIGHT:
            raise ValueError(f"weight must be an integer in [1, {MAX_WEIGHT}], got {e.weight!r}")
        weights[e.left, e.right] = e.weight
    return sorted(lefts), sorted(rights), weights


def _components(weights: dict[tuple[str, str], int]):
    """Connected components as (left ids, right ids), ordered by smallest record.

    A breadth-first walk from each record not yet reached, taken in sorted
    order: the partition, and the order components come out in, do not depend
    on edge order. Every loop is bounded, so no defect can hang it.
    """
    neighbours = defaultdict(set)
    for left, right in weights:
        neighbours["L", left].add(("R", right))
        neighbours["R", right].add(("L", left))
    reached = set()
    out = []
    for start in sorted(neighbours):
        if start in reached:
            continue
        reached.add(start)
        members = [start]
        # A breadth-first queue that grows while walked. Read by index up to
        # the number of records, so the walk ends even if the queue misbehaves.
        for k in range(len(neighbours)):
            if k == len(members):
                break
            for other in sorted(neighbours[members[k]]):
                if other not in reached:
                    reached.add(other)
                    members.append(other)
        left_ids = tuple(sorted(x for side, x in members if side == "L"))
        right_ids = tuple(sorted(x for side, x in members if side == "R"))
        out.append((left_ids, right_ids))
    return out


def _resolve(left_ids, right_ids, weights, margin):
    """Optimal total, and (status, partner) for every record of one component.

    Square cost matrix of size L + R: row i < L is left i, row L + j is right j's
    "alone" row; column j < R is right j, column R + i is left i's "alone"
    column. Staying alone costs 0, an edge costs -weight, anything else is
    forbidden. A forbidden cell costs more than the sum of all weights, so an
    assignment through one is worse than leaving everything alone.
    """
    nl, nr = len(left_ids), len(right_ids)
    size = nl + nr
    forbidden = 1 + sum(weights[x, y] for x in left_ids for y in right_ids if (x, y) in weights)
    cost = [[forbidden] * size for _ in range(size)]
    for i, x in enumerate(left_ids):
        for j, y in enumerate(right_ids):
            if (x, y) in weights:
                cost[i][j] = -weights[x, y]
        cost[i][nr + i] = 0
    for j in range(nr):
        cost[nl + j][j] = 0
        for i in range(nl):
            cost[nl + j][nr + i] = 0

    total, column = _min_cost_assignment(cost)
    partner_of_left = {i: column[i] for i in range(nl) if column[i] < nr}
    partner_of_right = {j: i for i, j in partner_of_left.items()}

    def near_optimal_without(row: int, col: int) -> bool:
        # Forbid one cell of the optimum: the record's current decision. Every
        # record keeps a feasible alternative (its alone cell, or the partner
        # its edge gives it), so the new optimum never goes through a
        # forbidden cell; it is within the margin iff the data do not decide.
        saved, cost[row][col] = cost[row][col], forbidden
        alternative, _ = _min_cost_assignment(cost)
        cost[row][col] = saved
        return alternative - total <= margin

    left_dec, right_dec = {}, {}
    for i, x in enumerate(left_ids):
        j = partner_of_left.get(i)
        if j is None:
            ambiguous = near_optimal_without(i, nr + i)
            left_dec[x] = (Status.AMBIGUOUS, None) if ambiguous else (Status.UNMATCHED, None)
        elif near_optimal_without(i, j):
            left_dec[x] = right_dec[right_ids[j]] = (Status.AMBIGUOUS, None)
        else:
            left_dec[x] = (Status.MATCHED, right_ids[j])
            right_dec[right_ids[j]] = (Status.MATCHED, x)
    for j, y in enumerate(right_ids):
        if j not in partner_of_right:
            ambiguous = near_optimal_without(nl + j, j)
            right_dec[y] = (Status.AMBIGUOUS, None) if ambiguous else (Status.UNMATCHED, None)
    return -total, left_dec, right_dec


def _min_cost_assignment(cost: list[list[int]]) -> tuple[int, list[int]]:
    """Hungarian algorithm, O(n^3), square matrix. Returns (total, row -> column)."""
    n = len(cost)
    u, v = [0] * (n + 1), [0] * (n + 1)
    p, way = [0] * (n + 1), [0] * (n + 1)
    for i in range(1, n + 1):
        p[0], j0 = i, 0
        minv, used = [math.inf] * (n + 1), [False] * (n + 1)
        # Each step adds a new column to the tree, so a free one is reached in
        # at most n steps and the next check breaks. Bounded, so a defect fails
        # loudly instead of hanging.
        for _ in range(n + 1):
            if not p[j0]:
                break
            used[j0] = True
            i0, delta, j1 = p[j0], math.inf, 0
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
        else:  # pragma: no cover - unreachable: guarded by the oracle tests
            raise AssertionError("assignment did not converge")  # pragma: no mutate
        while j0:
            j1 = way[j0]
            p[j0], j0 = p[j1], j1
    row_to_col = [0] * n
    for j in range(1, n + 1):
        row_to_col[p[j] - 1] = j - 1
    return sum(cost[i][row_to_col[i]] for i in range(n)), row_to_col
