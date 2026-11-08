"""Connection path: the timing path inside one connection (phase 4).

The timing path (``timing.py``) cannot tell two requests apart once they fall
in the same window. The connection they travel on can: the two ends of one
TCP connection report the same four-tuple. This path partitions each hop's
observations by a key, then runs the timing path, unchanged, inside each
partition:

- ``key="connection"`` (4a): the four-tuple. On HTTP/1.1 a connection carries
  one request at a time, so inside it each observation has at most one
  candidate in its window. No rank on the connection is counted: a lost
  observation leaves its partner NO_CANDIDATE and moves nothing else.
- ``key="h2-stream"`` (4b): the four-tuple and the HTTP/2 stream number, the
  same at both ends of a connection and never reused on it. Time still has to
  fit: a key present on both sides with no pair inside the windows is
  KEY_TIME_CONFLICT. Time only catches a wrong key whose partner is outside the
  window; the stream number is otherwise trusted as the agent reported it.
- ``key="tcp-seq"`` (4c): the four-tuple and the offset of the message's first
  byte in the TCP stream, the same at both ends whatever the clocks, and given
  to one message only. Time only checks, as for ``h2-stream``: a key present on
  both sides with no pair inside the windows is KEY_TIME_CONFLICT; a wrong
  offset whose partner is inside the window is not caught.

Windows and margin are the timing path's own. The local density guard is not
applied with ``key="connection"`` (4a'): its neighbours on one HTTP/1.1
connection are the requests before and after, which cannot be mistaken for it;
two requests at once on it stay AMBIGUOUS through the margin. Nor with
``key="tcp-seq"``: an offset designates one message. With ``key="h2-stream"``
the guard is kept and counts neighbours on the same key only.
``Result.params`` gives the parameters actually applied. An observation with no
usable key (empty address, port outside 1..65535, no stream number for
``h2-stream``, no offset for ``tcp-seq``) is NO_KEY and never linked here;
falling back to the timing path for it is the caller's choice, not this
module's.

A link found this way is ``derived``: the key narrows the candidates, time
decides. Hops are joined one at a time. Duplicates are dropped first. The
result does not depend on the order of the observations.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace

from .assignment import Status
from .timing import DerivedLink, JoinParams, Timed, Undecided, _is_int, _validate, timed_links
from .topology import Topology

KEYS = ("connection", "h2-stream", "tcp-seq")
UNGUARDED = ("connection", "tcp-seq")  # keys inside which the density guard is not applied
CHECKED = ("h2-stream", "tcp-seq")  # keys whose time misfit is a KEY_TIME_CONFLICT
MAX_STREAM = 2**31 - 1  # HTTP/2 stream identifiers are 31 bits


@dataclass(frozen=True, slots=True)
class Conn:
    client_addr: str
    client_port: int
    server_addr: str
    server_port: int


@dataclass(frozen=True, slots=True)
class Connected:
    timed: Timed
    conn: Conn | None  # None: the source gave no four-tuple
    stream: int = 0  # HTTP/2 stream number; 0: unknown or not HTTP/2
    offset: int | None = None  # first byte of the message in the TCP stream; None: unknown


@dataclass(frozen=True, slots=True)
class Result:
    key: str  # "connection" | "h2-stream": what narrowed the candidates
    links: tuple[DerivedLink, ...]  # sorted by (where, upstream)
    undecided: tuple[Undecided, ...]  # sorted by (where, side, observation)
    duplicates_dropped: int
    components: tuple[tuple[str, int], ...] = ()  # (where, records), partitions in key order
    params: tuple[tuple[str, JoinParams], ...] = ()  # (where, parameters applied), by where


def connection_links(
    topology: Topology,
    observations: Iterable[Connected],
    params: Mapping[str, JoinParams],
    key: str = "connection",
) -> Result:
    """Join every timing-derived hop of ``topology`` that has parameters, inside each key."""
    if key not in KEYS:
        raise ValueError(f"key must be one of {KEYS}, got {key!r}")
    timed_links(topology, (), params)  # refuses bad params and hops, observations or not
    distinct, dropped = _deduplicated(topology, observations)
    applied = {
        where: replace(p, local_span_us=0) if key in UNGUARDED else p for where, p in params.items()
    }

    links, undecided, components = [], [], []
    for where in sorted(params):
        src, dst = where.split(" -> ")
        groups: dict[tuple, list[Connected]] = defaultdict(list)
        for obs in distinct:
            side = obs.timed.side
            if (obs.timed.point, side) not in ((src, "outbound"), (dst, "inbound")):
                continue
            k = _key(obs, key)
            if k is None:
                undecided.append(Undecided(where, _identity(obs), side, Status.NO_KEY))
            else:
                groups[k].append(obs)
        for k in sorted(groups):
            group = groups[k]
            found = timed_links(topology, [o.timed for o in group], {where: applied[where]})
            both_sides = len({o.timed.side for o in group}) == 2
            links += found.links
            components += found.components
            for u in found.undecided:
                if key in CHECKED and both_sides and u.status is Status.NO_CANDIDATE:
                    u = Undecided(u.where, u.observation, u.side, Status.KEY_TIME_CONFLICT)
                undecided.append(u)
    return Result(
        key=key,
        links=tuple(sorted(links, key=lambda x: (x.where, x.upstream))),
        undecided=tuple(sorted(undecided, key=lambda x: (x.where, x.side, x.observation))),
        duplicates_dropped=dropped,
        components=tuple(components),
        params=tuple(sorted(applied.items())),
    )


def _identity(obs: Connected) -> tuple[str, int]:
    return obs.timed.source, obs.timed.seq


def _key(obs: Connected, key: str) -> tuple | None:
    c = obs.conn
    if c is None or not (c.client_addr and c.server_addr):
        return None
    if not (1 <= c.client_port <= 65_535 and 1 <= c.server_port <= 65_535):
        return None
    four = (c.client_addr, c.client_port, c.server_addr, c.server_port)
    if key == "connection":
        return four
    if key == "tcp-seq":
        return None if obs.offset is None else (*four, obs.offset)
    return (*four, obs.stream) if 1 <= obs.stream <= MAX_STREAM else None


def _deduplicated(topology: Topology, observations: Iterable[Connected]):
    points = {p.name for p in topology.points}
    seen: dict[tuple[str, int], tuple] = {}
    distinct: list[Connected] = []
    dropped = 0
    for obs in observations:
        _validate_connected(obs, points)
        identity = _identity(obs)
        content = (obs.timed, obs.conn, obs.stream, obs.offset)
        if identity in seen:
            if seen[identity] != content:
                raise ValueError(f"observation {identity!r} delivered with different content")
            dropped += 1
            continue
        seen[identity] = content
        distinct.append(obs)
    return distinct, dropped


def _validate_connected(obs: Connected, points: set[str]) -> None:
    _validate(obs.timed, points)
    c = obs.conn
    if c is not None:
        if not isinstance(c, Conn):
            raise ValueError(f"conn must be a Conn or None, got {c!r}")
        if not (isinstance(c.client_addr, str) and isinstance(c.server_addr, str)):
            raise ValueError(f"addresses must be strings in {c!r}")
        if not (_is_int(c.client_port) and _is_int(c.server_port)):
            raise ValueError(f"ports must be integers in {c!r}")
    if not (_is_int(obs.stream) and obs.stream >= 0):
        raise ValueError(f"stream must be an integer >= 0, got {obs.stream!r}")
    if obs.offset is not None and not (_is_int(obs.offset) and obs.offset >= 0):
        raise ValueError(f"offset must be an integer >= 0 or None, got {obs.offset!r}")
