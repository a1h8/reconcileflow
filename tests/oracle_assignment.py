"""Exhaustive oracle for reconcileflow.correlation.assignment.

Imports nothing from reconcileflow: a shared helper would move the oracle along
with a defect in the code under test. It enumerates every assignment (every set
of edges with no record in common) and reads each record's status straight off
the definition: decided if its partner, or the absence of one, is the same in
every assignment within ``margin`` of the best.

It works on the whole graph, not per component. Per record the two agree: a
deviation confined to one component costs the same globally, and a global
deviation within the margin is within it on every component it touches, since
no component can do better than its own optimum.
"""

from __future__ import annotations

from collections import defaultdict


def assignments(edges: dict[tuple[str, str], int], lefts: list[str]):
    """Yield (value, {left: right}) for every assignment."""
    by_left = defaultdict(list)
    for (left, right), weight in edges.items():
        by_left[left].append((right, weight))

    def walk(i: int, used: frozenset, value: int, chosen: dict):
        if i == len(lefts):
            yield value, dict(chosen)
            return
        left = lefts[i]
        yield from walk(i + 1, used, value, chosen)
        for right, weight in by_left[left]:
            if right not in used:
                chosen[left] = right
                yield from walk(i + 1, used | {right}, value + weight, chosen)
                del chosen[left]

    yield from walk(0, frozenset(), 0, {})


def statuses(lefts, rights, edges: dict[tuple[str, str], int], margins):
    """best value and per-margin statuses: {margin: (left_status, right_status)}.

    A status is ("NO_CANDIDATE", None), ("UNMATCHED", None), ("AMBIGUOUS", None)
    or ("MATCHED", partner).
    """
    lefts, rights = sorted(lefts), sorted(rights)
    with_edge_l = {left for left, _ in edges}
    with_edge_r = {right for _, right in edges}
    every = list(assignments(edges, lefts))
    best = max(value for value, _ in every)
    out = {}
    for margin in margins:
        near = [chosen for value, chosen in every if value >= best - margin]
        left_status, right_status = {}, {}
        for left in lefts:
            if left not in with_edge_l:
                left_status[left] = ("NO_CANDIDATE", None)
                continue
            seen = {chosen.get(left) for chosen in near}
            left_status[left] = _decided(seen)
        for right in rights:
            if right not in with_edge_r:
                right_status[right] = ("NO_CANDIDATE", None)
                continue
            seen = set()
            for chosen in near:
                partners = [left for left, r in chosen.items() if r == right]
                seen.add(partners[0] if partners else None)
            right_status[right] = _decided(seen)
        out[margin] = (left_status, right_status)
    return best, out


def _decided(seen: set):
    if len(seen) > 1:
        return ("AMBIGUOUS", None)
    (only,) = seen
    return ("UNMATCHED", None) if only is None else ("MATCHED", only)
