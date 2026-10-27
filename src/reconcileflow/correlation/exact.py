"""Exact path: link observations of one request by equal propagated identifiers.

For every hop whose ceiling is ``shared_header`` or ``message_key``, and every
crossing whose ceiling is ``propagated``, the two sides are joined on the
identifier the topology names (``HopCeiling.key``, ``CrossingCeiling.key``).
A link is an ``authored`` fact only when the value is seen **exactly once** on
each side.

1. Duplicates first. An observation delivered twice (same source and sequence
   number, same content) counts once. Without this, every redelivery would
   look like a collision and push requests off the exact path for nothing.
2. Then collisions. A value carried by more than one distinct observation on
   a side -- an identifier swapped or reused -- links nothing: all the
   observations sharing it are reported, so a swapping source shows up
   instead of producing false facts.

The result does not depend on the order of the observations.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .topology import IDENTIFIERS, Topology

SIDES = ("inbound", "outbound")
Identity = tuple[str, int]  # (source, seq)


@dataclass(frozen=True, slots=True)
class Observation:
    source: str  # where it comes from (file, stream)
    seq: int  # rank in its source: (source, seq) is its identity
    point: str  # a point of the topology
    side: str  # "inbound" | "outbound"
    keys: Mapping[str, str]  # identifier -> value recorded


@dataclass(frozen=True, slots=True)
class Link:
    where: str  # "src -> dst" for a hop, the point name for a crossing
    key: str
    value: str
    upstream: Identity
    downstream: Identity


@dataclass(frozen=True, slots=True)
class Collision:
    where: str
    key: str
    value: str
    upstream: tuple[Identity, ...]  # distinct observations sharing the value
    downstream: tuple[Identity, ...]


@dataclass(frozen=True, slots=True)
class Result:
    links: tuple[Link, ...]  # sorted by (where, key, value)
    collisions: tuple[Collision, ...]  # sorted by (where, key, value)
    unlinked: tuple[tuple[str, Identity], ...]  # (where, observation) without a partner
    duplicates_dropped: int


def exact_links(topology: Topology, observations: Iterable[Observation]) -> Result:
    """Join every authored hop and crossing of ``topology`` on its identifier."""
    distinct, dropped = _deduplicated(topology, observations)
    by_side: dict[tuple[str, str], list[Observation]] = defaultdict(list)
    for obs in distinct:
        by_side[obs.point, obs.side].append(obs)

    joins = [
        (f"{hop.src} -> {hop.dst}", hop.key, (hop.src, "outbound"), (hop.dst, "inbound"))
        for hop in topology.hops
        if hop.key is not None
    ] + [
        (c.point, c.key, (c.point, "inbound"), (c.point, "outbound"))
        for c in topology.crossings
        if c.key is not None
    ]
    links, collisions, unlinked = [], [], []
    for where, key, up_side, down_side in joins:
        up = _by_value(by_side[up_side], key)
        down = _by_value(by_side[down_side], key)
        for value in sorted(up.keys() | down.keys()):
            ups, downs = up.get(value, []), down.get(value, [])
            if len(ups) > 1 or len(downs) > 1:
                collisions.append(Collision(where, key, value, tuple(ups), tuple(downs)))
            elif ups and downs:
                links.append(Link(where, key, value, ups[0], downs[0]))
            else:
                unlinked.append((where, (ups or downs)[0]))
    return Result(
        links=tuple(sorted(links, key=lambda x: (x.where, x.key, x.value))),
        collisions=tuple(sorted(collisions, key=lambda x: (x.where, x.key, x.value))),
        unlinked=tuple(sorted(unlinked)),
        duplicates_dropped=dropped,
    )


def _by_value(observations: list[Observation], key: str) -> dict[str, list[Identity]]:
    out: dict[str, list[Identity]] = defaultdict(list)
    for obs in observations:
        if key in obs.keys:
            out[obs.keys[key]].append((obs.source, obs.seq))
    return {value: sorted(ids) for value, ids in out.items()}


def _deduplicated(topology: Topology, observations: Iterable[Observation]):
    points = {p.name for p in topology.points}
    seen: dict[Identity, tuple] = {}
    distinct: list[Observation] = []
    dropped = 0
    for obs in observations:
        _validate(obs, points)
        identity = (obs.source, obs.seq)
        content = (obs.point, obs.side, tuple(sorted(obs.keys.items())))
        if identity in seen:
            if seen[identity] != content:
                raise ValueError(f"observation {identity!r} delivered with different content")
            dropped += 1
            continue
        seen[identity] = content
        distinct.append(obs)
    return distinct, dropped


def _validate(obs: Observation, points: set[str]) -> None:
    if not isinstance(obs.source, str):
        raise ValueError(f"source must be a string, got {obs.source!r}")
    if not isinstance(obs.seq, int) or isinstance(obs.seq, bool):
        raise ValueError(f"seq must be an integer, got {obs.seq!r}")
    if obs.point not in points:
        raise ValueError(f"unknown point {obs.point!r}")
    if obs.side not in SIDES:
        raise ValueError(f"side must be inbound or outbound, got {obs.side!r}")
    for key, value in obs.keys.items():
        if key not in IDENTIFIERS:
            raise ValueError(f"unknown identifier {key!r}")
        if not isinstance(value, str) or not value:
            raise ValueError(f"empty value for {key!r} in observation {(obs.source, obs.seq)!r}")
