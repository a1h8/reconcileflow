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

Answer sizes (spec 8.17), for a hop whose answer the point forwards: the
point's answer minus its call's answer lies in a window (``Sizes``); a call
outside it is no candidate, an unknown size removes nothing. The window is
declared, or learnt by ``learn_window`` from links the timings alone hold
(no truth): a wrong one among them can only widen it or remove it. Where
the window leaves a request, or a call, of a group formed by the timings
with no option, it is contradicted there: the whole group is refused
(``SIZE_MISMATCH``). Otherwise groups are formed again on the options left.
Open risk: a point that rewrites one answer, where another call falls in
the window by chance, trips nothing and could force a wrong link.

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


@dataclass(frozen=True, slots=True)
class Sizes:
    """Answer sizes, for the hops whose answer the point forwards (spec 8.17):
    the point's answer to a request minus its call's answer on ``hop`` lies
    in ``windows[hop]`` = (low, high), bytes. A size not given removes nothing."""

    inbound: Mapping[Identity, int]  # request -> bytes the point answered
    outbound: Mapping[str, Mapping[Identity, int]]  # hop -> call -> bytes it got back
    windows: Mapping[str, tuple[int, int]]


WINDOW_MINIMUM = 200  # links needed to learn a window
WINDOW_MARGIN = 32  # bytes added on each side of what the links show
WINDOW_SPAN = 256  # wider than this, the point does not forward: no window


def learn_window(
    links: Iterable[tuple[Identity, Identity]],
    inbound: Mapping[Identity, int],
    outbound: Mapping[Identity, int],
    minimum: int = WINDOW_MINIMUM,
    margin: int = WINDOW_MARGIN,
    span: int = WINDOW_SPAN,
) -> tuple[int, int] | None:
    """The window of one hop from links already held without sizes (no
    truth): the gaps they show, widened by ``margin``. None below ``minimum``
    links with both sizes, or when the gaps spread over more than ``span``.
    A wrong link among them can only widen the window or remove it."""
    gaps = [inbound[r] - outbound[c] for r, c in links if r in inbound and c in outbound]
    if len(gaps) < minimum or max(gaps) - min(gaps) > span:
        return None
    return (min(gaps) - margin, max(gaps) + margin)


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
    sizes: Sizes | None = None,
) -> Result:
    """Links from the requests into ``recipe.point`` (``inbound``) to their
    calls on each hop of the recipe (``outbound[hop]``). Each link is a
    ``DerivedLink`` whose ``where`` is the hop; ``Undecided`` names the
    request (side "inbound") or the call (side "outbound") and why."""
    requests = sorted(inbound, key=_key)
    calls = {h: sorted(outbound.get(h, ()), key=_key) for h in recipe.calls}
    starts = {h: [c.start_us for c in calls[h]] for h in recipe.calls}
    losses = _union(lossy)
    loss_ends = [b for _, b in losses]

    def touches_loss(low, high) -> bool:
        k = bisect_left(loss_ends, low)  # the first loss that ends at or after low
        return k < len(losses) and losses[k][0] <= high

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

    def grouped(members, dom):
        """Requests sharing a candidate call, through ``dom``."""
        root: dict = {}

        def find(x):
            while root.setdefault(x, x) != x:
                root[x] = root[root[x]]
                x = root[x]
            return x

        for r in members:
            find(("r", _id(r)))
            for h in recipe.calls:
                for c in dom[_id(r), h]:
                    root[find(("c", h, c))] = find(("r", _id(r)))
        out: dict = defaultdict(lambda: {"requests": [], "calls": defaultdict(list)})
        for r in members:
            out[find(("r", _id(r)))]["requests"].append(r)
        for h in recipe.calls:
            for c in calls[h]:
                node = ("c", h, _id(c))
                if node in root:
                    out[find(node)]["calls"][h].append(c)
        return list(out.values())

    groups = grouped(kept, domain)
    in_some = {(h, c) for (_, h), o in domain.items() for c in o}
    for h in recipe.calls:
        for c in calls[h]:
            if (h, _id(c)) not in in_some:
                undecided.append(Undecided(h, _id(c), "outbound", Status.NO_CANDIDATE))

    if sizes is not None:
        # The window removes options; where it leaves a request or a call of a
        # time group with none, it is contradicted there: the group is refused.
        sized = {}
        for (r, h), o in domain.items():
            low, high = sizes.windows.get(h, (None, None))
            mine = sizes.inbound.get(r)
            theirs = sizes.outbound.get(h, {})
            if low is None or mine is None:
                sized[r, h] = o
            else:
                sized[r, h] = [c for c in o if c not in theirs or low <= mine - theirs[c] <= high]
        still = {(h, c) for (_, h), o in sized.items() for c in o}
        life = {_id(r): (r.start_us, r.end_us) for r in requests}
        life |= {(h, _id(c)): (c.start_us, c.end_us) for h in recipe.calls for c in calls[h]}
        # A contradiction on what a declared loss touches is the loss's doing
        # (a call gone, a life changed): those are refused by the loss
        # anyway (spec 8.18.10); any other is the window's, and refuses.
        kept_groups = []
        for g in groups:
            broken = any(
                not sized[_id(r), h] and not touches_loss(*life[_id(r)])
                for r in g["requests"]
                for h in recipe.calls
            ) or any(
                (h, _id(c)) not in still and not touches_loss(*life[h, _id(c)])
                for h in recipe.calls
                for c in g["calls"][h]
            )
            if broken:
                for r in g["requests"]:
                    undecided.extend(
                        Undecided(h, _id(r), "inbound", Status.SIZE_MISMATCH) for h in recipe.calls
                    )
                for h in recipe.calls:
                    undecided.extend(
                        Undecided(h, _id(c), "outbound", Status.SIZE_MISMATCH)
                        for c in g["calls"][h]
                    )
            else:
                kept_groups += g["requests"]
                undecided.extend(  # left with no request by its size: the loss's doing
                    Undecided(h, _id(c), "outbound", Status.INCOMPLETE)
                    for h in recipe.calls
                    for c in g["calls"][h]
                    if (h, _id(c)) not in still
                )
        domain = sized
        groups = grouped(sorted(kept_groups, key=_key), domain)

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

    for g in groups:
        n = len(g["requests"])
        low = g["requests"][0].start_us
        high = max(r.end_us for r in g["requests"])
        touched = touches_loss(low, high)
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


def _union(intervals) -> list[tuple[float, float]]:
    """Closed intervals merged where they meet: disjoint, sorted."""
    out: list[list[float]] = []
    for a, b in sorted(intervals):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


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
