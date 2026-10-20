"""Timing path: link the two sides of a hop by when they happened.

When no identifier is shared across a hop, the two sides of one request can
only be paired by time. For every hop whose ceiling is ``nested_timing`` and
for which calibrated parameters are given:

1. Candidates. An upstream observation (the hop's ``outbound`` side) and a
   downstream one (``inbound``) are a candidate pair only if, for every
   signal retained by calibration, their gap is within the signal's window
   of its usual offset. Gaps: start ``b.start - a.start``, end
   ``a.end - b.end``, duration ``(a.end - a.start) - (b.end - b.start)``.
   Weight: ``1 + sum(window - |gap - offset|)``, larger when closer to the
   usual offset.
2. Decision. The assignment core (``assignment.assign``) decides per
   component, with the calibrated margin: a near tie is AMBIGUOUS, never a
   guess. Only MATCHED pairs become links; every other observation is
   reported with its status, both sides included.
3. Local density guard (``local_span_us`` > 0). After the decision, a MATCHED
   pair is withdrawn, both sides DENSE, when either side is crowded: with n
   other observations on that side within ``local_span_us`` of it (upstream:
   by start; downstream: by the first signal's reference), crowded means
   ``n * w1 > local_span_us``, w1 the first signal's window. That is the
   density ``rate x 2 x w1`` measured around the pair instead of over the
   whole input, above 1. In a burst, true pairs leave their windows while
   neighbours enter them; the margin cannot see a partner that is missing.

A link found this way is ``derived``: an inference, never an authored fact.
Hops are joined one at a time; nothing here unions chains across hops.
Duplicates are dropped first, as on the exact path. The result does not
depend on the order of the observations.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .assignment import MAX_WEIGHT, Edge, Status, assign
from .topology import LinkKind, Topology

SIDES = ("inbound", "outbound")
SIGNALS = ("start", "end", "duration")
Identity = tuple[str, int]  # (source, seq)


@dataclass(frozen=True, slots=True)
class Timed:
    source: str  # where it comes from (file, stream)
    seq: int  # rank in its source: (source, seq) is its identity
    point: str  # a point of the topology
    side: str  # "inbound" | "outbound"
    start_us: int
    end_us: int  # >= start_us


@dataclass(frozen=True, slots=True)
class Signal:
    name: str  # "start" | "end" | "duration"
    offset_us: int  # usual gap (calibration median)
    window_us: int  # > 0: largest accepted distance from the offset


@dataclass(frozen=True, slots=True)
class JoinParams:
    signals: tuple[Signal, ...]  # one or two
    margin_us: int  # >= 0: near-optimal assignments within this are a tie
    max_component: int = 32
    local_span_us: int = 0  # >= 0: local density guard half-span; 0 turns it off


@dataclass(frozen=True, slots=True)
class Candidate:
    upstream: Identity
    downstream: Identity
    weight: int


@dataclass(frozen=True, slots=True)
class DerivedLink:
    where: str  # "src -> dst"
    upstream: Identity
    downstream: Identity
    weight: int


@dataclass(frozen=True, slots=True)
class Undecided:
    where: str
    observation: Identity
    side: str  # "outbound" (upstream) | "inbound" (downstream)
    status: Status  # AMBIGUOUS, UNMATCHED, OVERSIZED or NO_CANDIDATE


@dataclass(frozen=True, slots=True)
class Result:
    links: tuple[DerivedLink, ...]  # sorted by (where, upstream)
    undecided: tuple[Undecided, ...]  # sorted by (where, side, observation)
    duplicates_dropped: int
    components: tuple[tuple[str, int], ...] = ()  # (where, records), in assign's order


def candidates(
    upstream: Iterable[Timed], downstream: Iterable[Timed], params: JoinParams
) -> list[Candidate]:
    """Every (upstream, downstream) pair inside every window, sorted."""
    _validate_params(params)
    first = params.signals[0]
    downs = sorted(downstream, key=lambda b: (_reference(first.name, b), b.source, b.seq))
    refs = [_reference(first.name, b) for b in downs]
    out = []
    for a in upstream:
        low, high = _range(first, a)
        for b in downs[bisect.bisect_left(refs, low) : bisect.bisect_right(refs, high)]:
            gaps = [(s, _gap(s.name, a, b)) for s in params.signals]
            if all(abs(g - s.offset_us) <= s.window_us for s, g in gaps):
                weight = 1 + sum(s.window_us - abs(g - s.offset_us) for s, g in gaps)
                out.append(Candidate((a.source, a.seq), (b.source, b.seq), weight))
    return sorted(out, key=lambda c: (c.upstream, c.downstream))


def timed_links(
    topology: Topology, observations: Iterable[Timed], params: Mapping[str, JoinParams]
) -> Result:
    """Join every timing-derived hop of ``topology`` that has parameters."""
    timing_hops = {
        f"{h.src} -> {h.dst}": h for h in topology.hops if h.link is LinkKind.NESTED_TIMING
    }
    for where, p in params.items():
        if where not in timing_hops:
            raise ValueError(f"{where!r} is not a timing-derived hop of the topology")
        _validate_params(p)
    distinct, dropped = _deduplicated(topology, observations)
    by_side: dict[tuple[str, str], list[Timed]] = defaultdict(list)
    for obs in distinct:
        by_side[obs.point, obs.side].append(obs)

    links, undecided, components = [], [], []
    for where in sorted(params):
        hop, p = timing_hops[where], params[where]
        ups, downs = by_side[hop.src, "outbound"], by_side[hop.dst, "inbound"]
        found = candidates(ups, downs, p)
        # Identities become ids in a total order, independent of input order.
        up_ids = {i: f"u{n:09d}" for n, i in enumerate(sorted((o.source, o.seq) for o in ups))}
        down_ids = {i: f"d{n:09d}" for n, i in enumerate(sorted((o.source, o.seq) for o in downs))}
        back = {v: k for k, v in (up_ids | down_ids).items()}
        weights = {(c.upstream, c.downstream): c.weight for c in found}
        decided = assign(
            up_ids.values(),
            down_ids.values(),
            [Edge(up_ids[c.upstream], down_ids[c.downstream], c.weight) for c in found],
            margin=p.margin_us,
            max_component=p.max_component,
        )
        components += [(where, len(c.left) + len(c.right)) for c in decided.components]
        crowded = _crowding(ups, downs, p)
        for d in decided.left:
            if d.status is not Status.MATCHED:
                undecided.append(Undecided(where, back[d.id], "outbound", d.status))
                continue
            pair = (back[d.id], back[d.partner])
            if p.local_span_us and crowded(*pair):
                undecided.append(Undecided(where, pair[0], "outbound", Status.DENSE))
                undecided.append(Undecided(where, pair[1], "inbound", Status.DENSE))
            else:
                links.append(DerivedLink(where, *pair, weights[pair]))
        for d in decided.right:
            if d.status is not Status.MATCHED:
                undecided.append(Undecided(where, back[d.id], "inbound", d.status))
    return Result(
        links=tuple(sorted(links, key=lambda x: (x.where, x.upstream))),
        undecided=tuple(sorted(undecided, key=lambda x: (x.where, x.side, x.observation))),
        duplicates_dropped=dropped,
        components=tuple(components),
    )


def _crowding(ups: list[Timed], downs: list[Timed], p: JoinParams):
    """Whether a matched (upstream, downstream) pair sits where it is too dense
    (meaningful only when ``local_span_us`` > 0)."""
    span = p.local_span_us
    first = p.signals[0]
    up_at = {(o.source, o.seq): o.start_us for o in ups}
    down_at = {(o.source, o.seq): _reference(first.name, o) for o in downs}
    up_sorted, down_sorted = sorted(up_at.values()), sorted(down_at.values())

    def others(ordered: list[int], x: int) -> int:
        return bisect.bisect_right(ordered, x + span) - bisect.bisect_left(ordered, x - span) - 1

    def crowded(a: Identity, b: Identity) -> bool:
        n = max(others(up_sorted, up_at[a]), others(down_sorted, down_at[b]))
        return n * first.window_us > span

    return crowded


def _gap(name: str, a: Timed, b: Timed) -> int:
    if name == "start":
        return b.start_us - a.start_us
    if name == "end":
        return a.end_us - b.end_us
    return (a.end_us - a.start_us) - (b.end_us - b.start_us)


def _reference(name: str, b: Timed) -> int:
    """The downstream instant (or duration) the first signal searches on."""
    if name == "start":
        return b.start_us
    if name == "end":
        return b.end_us
    return b.end_us - b.start_us


def _range(signal: Signal, a: Timed) -> tuple[int, int]:
    """Bounds on the downstream reference for ``a``, from ``|gap - offset| <= window``."""
    m, w = signal.offset_us, signal.window_us
    if signal.name == "start":  # gap = b.start - a.start
        return a.start_us + m - w, a.start_us + m + w
    if signal.name == "end":  # gap = a.end - b.end
        return a.end_us - m - w, a.end_us - m + w
    duration = a.end_us - a.start_us  # gap = a.duration - b.duration
    return duration - m - w, duration - m + w


def _is_int(x) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def _validate_params(p: JoinParams) -> None:
    if not 1 <= len(p.signals) <= 2:
        raise ValueError(f"a join takes one or two signals, got {len(p.signals)}")
    names = [s.name for s in p.signals]
    if len(set(names)) != len(names):
        raise ValueError(f"a signal is given twice: {names}")
    for s in p.signals:
        if s.name not in SIGNALS:
            raise ValueError(f"unknown signal {s.name!r}")
        if not (_is_int(s.offset_us) and _is_int(s.window_us)):
            raise ValueError(f"signal {s.name!r}: offset and window must be integer µs")
        if s.window_us <= 0:
            raise ValueError(f"signal {s.name!r}: window must be > 0, got {s.window_us}")
    if 1 + sum(s.window_us for s in p.signals) > MAX_WEIGHT:
        raise ValueError(f"windows too wide: the weight would exceed {MAX_WEIGHT}")
    if not (_is_int(p.local_span_us) and p.local_span_us >= 0):
        raise ValueError(f"local_span_us must be an integer >= 0, got {p.local_span_us!r}")
    # margin_us and max_component are checked by assign(), with the same message.


def _deduplicated(topology: Topology, observations: Iterable[Timed]):
    points = {p.name for p in topology.points}
    seen: dict[Identity, tuple] = {}
    distinct: list[Timed] = []
    dropped = 0
    for obs in observations:
        _validate(obs, points)
        identity = (obs.source, obs.seq)
        content = (obs.point, obs.side, obs.start_us, obs.end_us)
        if identity in seen:
            if seen[identity] != content:
                raise ValueError(f"observation {identity!r} delivered with different content")
            dropped += 1
            continue
        seen[identity] = content
        distinct.append(obs)
    return distinct, dropped


def _validate(obs: Timed, points: set[str]) -> None:
    if not isinstance(obs.source, str):
        raise ValueError(f"source must be a string, got {obs.source!r}")
    if not (_is_int(obs.seq) and _is_int(obs.start_us) and _is_int(obs.end_us)):
        raise ValueError(f"seq, start_us and end_us must be integers in {obs!r}")
    if obs.point not in points:
        raise ValueError(f"unknown point {obs.point!r}")
    if obs.side not in SIDES:
        raise ValueError(f"side must be inbound or outbound, got {obs.side!r}")
    if obs.end_us < obs.start_us:
        raise ValueError(f"observation {(obs.source, obs.seq)!r} ends before it starts")
