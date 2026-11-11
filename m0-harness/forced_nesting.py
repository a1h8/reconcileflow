"""Links forced by the recipe (phase 4 spec 8.11): pure logic, no score.

Only where no context is propagated (no traceparent): inside one service,
which outgoing call belongs to which incoming request, from timings alone.

Per parent type P (a request into a service), the recipe, learnt on the
calibration: the call types P makes exactly once in every calibration
request, and the pairs of call types always in the same order (one ended
before the other starts). A request's candidates: one call per type, each
nested in it (same pod within one service), all distinct, in the recipe's
order. Requests that share a candidate call form a group. A group is refused
whole when, for a call type, its calls do not number its requests
(incomplete), when no assignment exists, or when the search passes the
budget. Otherwise a link is accepted only if every assignment of the group
holds it: never the best one, never a guess.

Usage: forced_nesting.py <level-dir> --theirs <converted e2e json>
       [--calibration truth|isolated]
"""

from __future__ import annotations

import bisect
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import jaeger_nesting as jn

MIN_CALIBRATION = 20  # requests needed to learn a recipe
BUDGET = 100_000  # search nodes per group


def _nested(a, b, same_service):
    return (
        a["start"] <= b["start"]
        and b["end"] <= a["end"]
        and (not same_service or a["pod"] == b["pod"])
    )


def _calibration(spans, parent_type, call_types, first, calibration):
    """[(request, {call type: [calls]})] to learn the recipe from."""
    of_type = defaultdict(list)
    for s in spans:
        of_type[s["type"]].append(s)
    parents = sorted(of_type[parent_type], key=lambda s: s["start"])
    if calibration == "truth":
        kids = defaultdict(lambda: defaultdict(list))
        for c in call_types:
            for b in of_type[c]:
                kids[b["parent"]][c].append(b)
        return [(a, kids[a["key"]]) for a in parents if a["trace"] in first]
    # isolated: no other request of the type overlaps it in its pod; the calls
    # nested in it are then its own.
    out = []
    by_pod = defaultdict(list)
    for a in parents:
        by_pod[a["pod"]].append(a)
    calls = {c: sorted(of_type[c], key=lambda s: s["start"]) for c in call_types}
    starts = {c: [b["start"] for b in calls[c]] for c in call_types}
    for same_pod in by_pod.values():
        latest = -float("inf")
        for i, a in enumerate(same_pod):
            alone = a["start"] >= latest and (
                i + 1 == len(same_pod) or same_pod[i + 1]["start"] > a["end"]
            )
            latest = max(latest, a["end"])
            if not alone:
                continue
            got = {}
            for c in call_types:
                lo = bisect.bisect_left(starts[c], a["start"])
                hi = bisect.bisect_right(starts[c], a["end"])
                got[c] = [b for b in calls[c][lo:hi] if _nested(a, b, a["type"][0] == c[0])]
            out.append((a, got))
    return out


def recipe(spans, found, first, calibration):
    """{parent type: {"calls": [call types], "order": [(before, after)],
    "calibration": n}}; a call type called by two parent types is left out."""
    callers = Counter(c for _, c in found)
    out = {}
    for parent_type in sorted({a for a, _ in found}):
        call_types = [c for a, c in found if a == parent_type and callers[c] == 1]
        cal = _calibration(spans, parent_type, call_types, first, calibration)
        if len(cal) < MIN_CALIBRATION:
            continue
        calls = [c for c in call_types if all(len(got.get(c, [])) == 1 for _, got in cal)]
        order = [
            (ci, cj)
            for ci in calls
            for cj in calls
            if ci != cj and all(got[ci][0]["end"] <= got[cj][0]["start"] for _, got in cal)
        ]
        if calls:
            out[parent_type] = {"calls": calls, "order": order, "calibration": len(cal)}
    return out


class _Stop(Exception):
    pass


def _solve(variables, domain, order_of, end, start, budget):
    """Links held by every assignment of one group: {variable: call}.
    variables: [(request, call type)]; domain: {variable: [calls]};
    order_of: {variable: [(other variable, before)]}, before True when the
    variable's call must end before the other's starts. Raises _Stop(cause)."""
    assigned, used = {}, set()
    common = None
    nodes = 0

    def fits(v, x):
        for w, before in order_of.get(v, ()):
            y = assigned.get(w)
            if y is not None and (end[x] > start[y] if before else end[y] > start[x]):
                return False
        return True

    def search():
        nonlocal common, nodes
        nodes += 1
        if nodes > budget:
            raise _Stop("budget")
        best = None
        for v in variables:
            if v in assigned:
                continue
            options = [x for x in domain[v] if x not in used and fits(v, x)]
            if not options:
                return
            if best is None or len(options) < len(best[1]):
                best = (v, options)
                if len(options) == 1:
                    break
        if best is None:  # a full assignment
            if common is None:
                common = dict(assigned)
            else:
                common = {v: x for v, x in common.items() if assigned[v] == x}
            if not common:
                raise _Stop("no_forced_link")
            return
        v, options = best
        for x in options:
            assigned[v] = x
            used.add(x)
            search()
            del assigned[v]
            used.discard(x)

    limit = sys.getrecursionlimit()
    sys.setrecursionlimit(max(limit, 4 * len(variables) + 1000))
    try:
        search()
    finally:
        sys.setrecursionlimit(limit)
    if common is None:
        raise _Stop("no_solution")
    return common


def forced_links(spans, found, first, calibration, budget=BUDGET):
    """Forced links {(f"j{i}a", request key): call key}, as jaeger_nesting's
    _link gives, for the joins of a recipe; and counts."""
    recipes = recipe(spans, found, first, calibration)
    join_index = {join: i for i, join in enumerate(found)}
    of_type = defaultdict(list)
    for s in spans:
        of_type[s["type"]].append(s)
    end = {s["key"]: s["end"] for s in spans}
    start = {s["key"]: s["start"] for s in spans}
    linked = {}
    stats = {"recipes": {}, "groups": 0, "refused_groups": Counter(), "refused_links": 0}
    for parent_type, r in recipes.items():
        stats["recipes"]["/".join(parent_type)] = {
            "calls": ["/".join(c) for c in r["calls"]],
            "order": [["/".join(a), "/".join(b)] for a, b in r["order"]],
            "calibration": r["calibration"],
        }
        calls = {c: sorted(of_type[c], key=lambda s: s["start"]) for c in r["calls"]}
        starts = {c: [b["start"] for b in calls[c]] for c in r["calls"]}
        domain, root = {}, {}

        def find(x, root=root):
            while root.setdefault(x, x) != x:
                root[x] = root[root[x]]
                x = root[x]
            return x

        for a in of_type[parent_type]:
            find(("p", a["key"]))
            for c in r["calls"]:
                lo = bisect.bisect_left(starts[c], a["start"])
                hi = bisect.bisect_right(starts[c], a["end"])
                same = parent_type[0] == c[0]
                options = [b["key"] for b in calls[c][lo:hi] if _nested(a, b, same)]
                domain[a["key"], c] = options
                for x in options:
                    root[find(("c", x))] = find(("p", a["key"]))
        groups = defaultdict(lambda: {"parents": [], "calls": defaultdict(set)})
        for a in of_type[parent_type]:
            groups[find(("p", a["key"]))]["parents"].append(a["key"])
        for (a, c), options in domain.items():
            groups[find(("p", a))]["calls"][c].update(options)
        for g in groups.values():
            stats["groups"] += 1
            n = len(g["parents"])
            variables = [(a, c) for a in g["parents"] for c in r["calls"]]
            if any(len(g["calls"][c]) != n for c in r["calls"]):
                stats["refused_groups"]["incomplete"] += 1
                stats["refused_links"] += len(variables)
                continue
            order_of = defaultdict(list)
            for a in g["parents"]:
                for ci, cj in r["order"]:
                    order_of[a, ci].append(((a, cj), True))
                    order_of[a, cj].append(((a, ci), False))
            try:
                held = _solve(variables, domain, order_of, end, start, budget)
            except _Stop as stop:
                stats["refused_groups"][stop.args[0]] += 1
                stats["refused_links"] += len(variables)
                continue
            stats["refused_links"] += len(variables) - len(held)
            for (a, c), x in held.items():
                linked[f"j{join_index[parent_type, c]}a", a] = x
    stats["refused_groups"] = dict(stats["refused_groups"])
    return linked, stats


def combined(spans, found, first, calibration):
    """Forced links on the joins of a recipe; elsewhere the engine's links."""
    forced, stats = forced_links(spans, found, first, calibration)
    recipes = recipe(spans, found, first, calibration)
    in_recipe = {
        f"j{i}a" for i, (a, c) in enumerate(found) if a in recipes and c in recipes[a]["calls"]
    }
    engine, _, _, _ = jn._link(spans, found, first, calibration)
    out = {k: v for k, v in engine.items() if k[0] not in in_recipe}
    out.update(forced)
    return out, stats


def main(argv):
    level = Path(argv[1])
    calibration = argv[argv.index("--calibration") + 1] if "--calibration" in argv else "truth"
    theirs = json.loads(Path(argv[argv.index("--theirs") + 1]).read_text())
    kept = {}

    def linker(spans, found, first, cal):
        linked, kept["stats"] = combined(spans, found, first, cal)
        return linked

    report = jn.aligned(level, theirs, calibration, linker=linker)
    report["forced"] = kept["stats"]
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(sys.argv)
