"""The recipe inside a point: links forced by the calls a request makes.

Where nothing stronger links a request into a point to the calls it sends out
(no propagated header, no execution context), timings alone remain. Taken call
by call, they leave many requests open under load. Taken together they say
much more: a request makes the calls of its recipe, each once, each within its
own life, in a fixed order; and a call belongs to one request only.

Phase 4 spec 8.12 (the rule of 8.11, measured on published traces first):

- ``Recipe``: the hops a point calls **exactly once** per request, and the
  pairs always in the same order (one call ended before the other starts).
  Declared, or learnt from calibration requests by ``learn_recipe``: a hop is
  in the recipe only if every calibration request made it once, a pair only
  if every one kept its order; at least ``minimum`` requests.
- ``recipe_links``: a request's candidates are one call per hop of the
  recipe, each nested in the request, all distinct, in the recipe's order.
  A request with no candidate on some hop can take no call: it is set aside
  (``NO_CANDIDATE``) before anything is counted. The others that share a
  candidate call form a group. A group is refused whole when, for a hop, its
  calls do not number its requests, or when it touches a declared loss
  (``INCOMPLETE``); when no complete assignment exists (``NO_SOLUTION``).
  Otherwise options no complete assignment can hold are pruned (perfect
  matchings per hop, the recipe's order per request) until nothing changes;
  a request left with one option holds that link, the others are
  ``AMBIGUOUS``. With one hop, these are exactly the links every complete
  assignment holds; with more, never one that some assignment does not hold.
  No weight, no score: never the best one, never a guess. Polynomial: groups
  of tens of thousands of requests are decided.

The result does not depend on the order of the input: observations are
sorted by time and identity before anything is decided.

Not wired into any path yet. Measured on agent spans of the lab's Traefik
crossing: no wrong link in 44 runs, coverage 99.7% and more up to 20
concurrent requests, near 0 above, where a few calls stamped outside their
request, duplicated requests or unreported losses leave no complete
assignment. Open risk: a misstamped call that still leaves a complete
assignment could force a wrong link; not seen, not ruled out.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.timing import DerivedLink, Result, Timed, Undecided

MINIMUM = 20  # calibration requests needed to learn a recipe

Identity = tuple[str, int]


@dataclass(frozen=True, slots=True)
class Recipe:
    point: str
    calls: tuple[str, ...]  # hops called exactly once per request, sorted
    order: tuple[tuple[str, str], ...]  # (before, after): before ends first
    calibration: int  # requests it was learnt from (0: declared)
    source: str  # where it came from


def learn_recipe(
    point: str,
    examples: Sequence[tuple[Timed, Mapping[str, Sequence[Timed]]]],
    source: str,
    minimum: int = MINIMUM,
) -> Recipe | None:
    """The recipe of ``point`` from calibration requests and their own calls
    by hop; None below ``minimum`` requests or when no hop is always made once."""
    if len(examples) < minimum:
        return None
    hops = sorted({hop for _, got in examples for hop in got})
    calls = tuple(h for h in hops if all(len(got.get(h, ())) == 1 for _, got in examples))
    if not calls:
        return None
    order = tuple(
        (a, b)
        for a in calls
        for b in calls
        if a != b and all(got[a][0].end_us <= got[b][0].start_us for _, got in examples)
    )
    return Recipe(point, calls, order, len(examples), source)


def _key(o: Timed):
    return (o.start_us, o.end_us, o.source, o.seq)


def _id(o: Timed) -> Identity:
    return (o.source, o.seq)


def recipe_links(
    inbound: Iterable[Timed],
    outbound: Mapping[str, Iterable[Timed]],
    recipe: Recipe,
    lossy: Iterable[tuple[float, float]] = (),
) -> Result:
    """Links from the requests into ``recipe.point`` (``inbound``) to their
    calls on each hop of the recipe (``outbound[hop]``). Each link is a
    ``DerivedLink`` whose ``where`` is the hop; ``Undecided`` names the
    request (side "inbound") or the call (side "outbound") and why."""
    requests = sorted(inbound, key=_key)
    calls = {h: sorted(outbound.get(h, ()), key=_key) for h in recipe.calls}
    starts = {h: [c.start_us for c in calls[h]] for h in recipe.calls}
    losses = sorted(lossy)
    undecided: list[Undecided] = []

    domain: dict[tuple[Identity, str], list[Identity]] = {}
    kept = []
    for r in requests:
        options = {}
        for h in recipe.calls:
            lo = bisect_left(starts[h], r.start_us)
            hi = bisect_right(starts[h], r.end_us)
            options[h] = [_id(c) for c in calls[h][lo:hi] if c.end_us <= r.end_us]
        if all(options.values()):
            kept.append(r)
            domain.update({(_id(r), h): o for h, o in options.items()})
        else:  # it can take no complete set of calls: set aside before counting
            undecided += [
                Undecided(h, _id(r), "inbound", Status.NO_CANDIDATE) for h in recipe.calls
            ]

    root: dict = {}

    def find(x):
        while root.setdefault(x, x) != x:
            root[x] = root[root[x]]
            x = root[x]
        return x

    for r in kept:
        find(("r", _id(r)))
        for h in recipe.calls:
            for c in domain[_id(r), h]:
                root[find(("c", h, c))] = find(("r", _id(r)))
    groups: dict = defaultdict(lambda: {"requests": [], "calls": defaultdict(list)})
    for r in kept:
        groups[find(("r", _id(r)))]["requests"].append(r)
    for h in recipe.calls:
        for c in calls[h]:
            node = ("c", h, _id(c))
            if node in root:
                groups[find(node)]["calls"][h].append(c)
            else:
                undecided.append(Undecided(h, _id(c), "outbound", Status.NO_CANDIDATE))

    start = {(h, _id(c)): c.start_us for h in recipe.calls for c in calls[h]}
    end = {(h, _id(c)): c.end_us for h in recipe.calls for c in calls[h]}
    links: list[DerivedLink] = []

    def refuse(g, status, held):
        taken = {(h, c) for (_, h), c in held.items()}
        for r in g["requests"]:
            for h in recipe.calls:
                if (_id(r), h) not in held:
                    undecided.append(Undecided(h, _id(r), "inbound", status))
        for h in recipe.calls:
            for c in g["calls"][h]:
                if (h, _id(c)) not in taken:
                    undecided.append(Undecided(h, _id(c), "outbound", status))

    for g in groups.values():
        n = len(g["requests"])
        low = g["requests"][0].start_us
        high = max(r.end_us for r in g["requests"])
        touched = any(a <= high and low <= b for a, b in losses)
        if touched or any(len(g["calls"][h]) != n for h in recipe.calls):
            refuse(g, Status.INCOMPLETE, {})
            continue
        ids = [_id(r) for r in g["requests"]]
        held = _forced(
            ids, recipe, {(r, h): list(domain[r, h]) for r in ids for h in recipe.calls}, start, end
        )
        if held is None:
            refuse(g, Status.NO_SOLUTION, {})
            continue
        for (r, h), c in held.items():
            links.append(DerivedLink(h, r, c, 0))
        refuse(g, Status.AMBIGUOUS, held)

    return Result(
        links=tuple(sorted(links, key=lambda x: (x.where, x.upstream))),
        undecided=tuple(sorted(undecided, key=lambda x: (x.where, x.side, x.observation))),
        duplicates_dropped=0,
    )


def _forced(requests, recipe, domain, start, end) -> dict | None:
    """Prune, until nothing changes, every option that no complete assignment
    can hold: per hop, an option in no perfect matching of requests to calls
    (a call serves one request); per request, an option with no partner in
    the recipe's order. Only impossible options go, so the truth, when the
    group is complete, is never pruned. A request left with one option on a
    hop holds that link. With one hop this is exact: the links held by every
    complete assignment. None when no complete assignment exists."""
    after = defaultdict(list)
    before = defaultdict(list)
    for a, b in recipe.order:
        after[a].append(b)
        before[b].append(a)
    changed = True
    while changed:
        changed = False
        for h in recipe.calls:
            kept = _in_some_perfect_matching(requests, h, domain)
            if kept is None:
                return None
            for r in requests:
                if len(kept[r]) != len(domain[r, h]):
                    domain[r, h] = kept[r]
                    changed = True
        for r in requests:
            for a in recipe.calls:
                for b in after[a]:
                    latest = max(start[b, y] for y in domain[r, b])
                    ok = [x for x in domain[r, a] if end[a, x] <= latest]
                    if len(ok) != len(domain[r, a]):
                        domain[r, a] = ok
                        changed = True
                for b in before[a]:
                    earliest = min(end[b, y] for y in domain[r, b])
                    ok = [x for x in domain[r, a] if earliest <= start[a, x]]
                    if len(ok) != len(domain[r, a]):
                        domain[r, a] = ok
                        changed = True
                if not domain[r, a]:
                    return None
    return {
        (r, h): domain[r, h][0] for r in requests for h in recipe.calls if len(domain[r, h]) == 1
    }


def _in_some_perfect_matching(requests, hop, domain) -> dict | None:
    """Per request, the options of ``hop`` that some perfect matching holds
    (an edge is in one iff it is in the matching found, or both its ends lie
    in one strongly connected component of the alternating graph); None when
    no perfect matching exists."""
    right: dict = {}
    adj = []
    for r in requests:
        adj.append([right.setdefault(c, len(right)) for c in domain[r, hop]])
    n_left, n_right = len(adj), len(right)
    match_l = _hopcroft_karp(adj, n_right)
    if any(v == -1 for v in match_l) or n_right != n_left:
        return None
    # Alternating graph: left u -> its matched right; right v -> every left
    # that has v as an unmatched option.
    graph = [[] for _ in range(n_left + n_right)]
    for u, vs in enumerate(adj):
        graph[u].append(n_left + match_l[u])
        for v in vs:
            if v != match_l[u]:
                graph[n_left + v].append(u)
    comp = _components(graph)
    names = {v: c for c, v in right.items()}
    return {
        r: [names[v] for v in adj[u] if v == match_l[u] or comp[u] == comp[n_left + v]]
        for u, r in enumerate(requests)
    }


def _hopcroft_karp(adj, n_right):
    """Maximum matching of a bipartite graph, left u -> rights adj[u]."""
    n_left = len(adj)
    match_l, match_r = [-1] * n_left, [-1] * n_right
    while True:
        dist = [-1] * n_left
        queue = deque(u for u in range(n_left) if match_l[u] == -1)
        for u in queue:
            dist[u] = 0
        free = False
        while queue:
            u = queue.popleft()
            for v in adj[u]:
                w = match_r[v]
                if w == -1:
                    free = True
                elif dist[w] == -1:
                    dist[w] = dist[u] + 1
                    queue.append(w)
        if not free:
            return match_l
        nxt = [0] * n_left
        for s in range(n_left):
            if match_l[s] != -1:
                continue
            stack, via = [s], []
            while stack:
                u = stack[-1]
                if nxt[u] < len(adj[u]):
                    v = adj[u][nxt[u]]
                    nxt[u] += 1
                    w = match_r[v]
                    if w == -1:
                        for x, y in zip(stack, [*via, v], strict=True):
                            match_l[x], match_r[y] = y, x
                        break
                    if dist[w] == dist[u] + 1:
                        stack.append(w)
                        via.append(v)
                else:
                    dist[u] = -2  # dead end in this phase
                    stack.pop()
                    if via:
                        via.pop()


def _components(graph) -> list[int]:
    """Strongly connected component of each node (iterative Tarjan)."""
    n = len(graph)
    index, low, comp = [-1] * n, [0] * n, [-1] * n
    on, stack, counter, count = [False] * n, [], 0, 0
    for root in range(n):
        if index[root] != -1:
            continue
        work = [(root, 0)]
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on[root] = True
        while work:
            u, i = work[-1]
            if i < len(graph[u]):
                work[-1] = (u, i + 1)
                v = graph[u][i]
                if index[v] == -1:
                    index[v] = low[v] = counter
                    counter += 1
                    stack.append(v)
                    on[v] = True
                    work.append((v, 0))
                elif on[v]:
                    low[u] = min(low[u], index[v])
            else:
                work.pop()
                if work:
                    low[work[-1][0]] = min(low[work[-1][0]], low[u])
                if low[u] == index[u]:
                    while True:
                        v = stack.pop()
                        on[v] = False
                        comp[v] = count
                        if v == u:
                            break
                    count += 1
    return comp
