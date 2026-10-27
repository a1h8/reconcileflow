"""Topology model: the strongest evidence each link can reach, from the declaration alone.

A topology is declared, not discovered: observation points (what each one sees
and records, on which host, against which clock) and the hops between them.
Before any data, this module computes the **ceiling** of every hop and of every
crossing inside a point -- the strongest evidence class it can ever reach -- and
refuses at load time a declaration that is inconsistent, or whose computed
ceiling is weaker than the one it expects.

Every refusal is reported at once, each with an enumerated reason: a list of
what to fix, not the first error found.

Three rules come from measurements, not from first principles:

- an untrusted entry whose agent copies ``traceparent`` lets an outside id in
  (the agent re-injects a header the entry stripped): refused;
- neither agent carries ``baggage``: declaring it is refused;
- a point that sets its own header: what its agent reports on the outbound side
  is not what went on the wire, unless the point logs it itself.
"""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, IntEnum
from fractions import Fraction

# Highest accepted density (expected requests within one timing window) for a
# hop that can only be derived from timing. Fixed before the code (phase 1 spec).
D_MAX = 1


class Evidence(IntEnum):
    """Evidence classes, ordered: a larger value is stronger evidence."""

    UNRESOLVED = 0
    DERIVED = 1
    OBSERVED = 2
    AUTHORED = 3


class LinkKind(Enum):
    SHARED_HEADER = "shared_header"
    MESSAGE_KEY = "message_key"
    SAME_CONNECTION = "same_connection"
    SAME_SOCKET = "same_socket"
    NESTED_TIMING = "nested_timing"
    PROPAGATED = "propagated"  # crossing: the point copies the header through
    INSIDE_PROCESS = "inside_process"  # crossing: execution context seen by the agent
    LOGGED_TIMING = "logged_timing"  # crossing: the point's own timestamps
    NONE = "none"


class Reason(IntEnum):
    """Refusal reasons, and what is missing when a ceiling falls short."""

    # The declaration itself
    MISSING_FIELD = 1
    UNKNOWN_FIELD = 2
    UNKNOWN_VALUE = 3
    UNKNOWN_IDENTIFIER = 4
    INVALID_VALUE = 5
    UNKNOWN_POINT = 6
    DUPLICATE_HOP = 7
    # Rules
    CEILING_BELOW_EXPECTED = 10
    DENSITY_ABOVE_BOUND = 11
    UNTRUSTED_ENTRY_PROPAGATED = 12
    AGENT_BAGGAGE = 13
    AGENT_NOT_PRESENT = 14
    FANOUT_UNBOUNDED = 15
    # What a link kind lacks (listed in a CEILING_BELOW_EXPECTED refusal)
    NO_SHARED_HEADER = 20
    NO_MESSAGE_KEY = 21
    MISSING_TUPLE5 = 22
    MISSING_NAT_MAPPING = 23
    MISSING_STREAM_ID = 24
    MISSING_SOCKET_COOKIE = 25
    CROSS_HOST_SOCKET = 26
    ASYNC_RELATION = 27
    NOT_TIMESTAMPED = 28
    NO_SHARED_CLOCK = 29


IDENTIFIERS = frozenset(
    {
        "traceparent",
        "baggage",
        "request_id",
        "message_id",
        "tuple5",
        "h2_stream_id",
        "socket_cookie",
    }
)
CARRIED = frozenset({"traceparent", "baggage", "request_id", "message_id"})  # propagates, sets
HEADERS = ("request_id", "traceparent")  # identify one request on the wire
VISIBILITY = frozenset({"kernel", "log", "app", "none"})
PROPAGATORS = frozenset({"agent", "app", "proxy"})
RELATIONS = frozenset({"call", "async", "fanout"})
NATS = frozenset({"none", "snat", "dnat"})
NAT_MAPPINGS = frozenset({"none", "conntrack", "logged_both_sides"})
# Timestamp precision of each class, in nanoseconds.
PRECISION_NS = {
    "kernel_monotonic": 1_000,
    "precise_sync": 10_000,
    "network_sync": 1_000_000,
    "log_ms": 1_000_000,
    "log_s": 1_000_000_000,
}
POINT_FIELDS = frozenset(
    {
        "host",
        "visibility",
        "clock",
        "timestamps",
        "inbound",
        "outbound",
        "propagates",
        "sets",
        "trusts_incoming",
    }
)
HOP_FIELDS = frozenset(
    {
        "from",
        "to",
        "relation",
        "nat",
        "nat_mapping",
        "pooling",
        "rate",
        "noise_us",
        "expect",
        "max_fanout",
    }
)


@dataclass(frozen=True, slots=True)
class Point:
    name: str
    host: str
    visibility: str
    clock: str | None
    timestamps: str | None
    inbound: frozenset[str]
    outbound: frozenset[str]
    propagates: tuple[tuple[str, str], ...]  # (identifier, who copies it), sorted
    sets: frozenset[str]
    trusts_incoming: bool


@dataclass(frozen=True, slots=True)
class Hop:
    src: str
    dst: str
    relation: str
    nat: str
    nat_mapping: str
    pooling: bool
    rate: int | None
    noise_us: int | None
    expect: Evidence | None
    max_fanout: int | None


@dataclass(frozen=True, slots=True)
class HopCeiling:
    src: str
    dst: str
    relation: str
    evidence: Evidence
    link: LinkKind
    density: Fraction | None  # only for a timing-derived hop with a declared rate
    max_fanout: int | None
    key: str | None  # identifier the exact path joins on (shared_header, message_key)


@dataclass(frozen=True, slots=True)
class CrossingCeiling:
    point: str
    evidence: Evidence
    link: LinkKind
    key: str | None  # identifier the point copies through (propagated)


@dataclass(frozen=True, slots=True)
class Topology:
    points: tuple[Point, ...]  # sorted by name
    hops: tuple[HopCeiling, ...]  # sorted by (src, dst)
    crossings: tuple[CrossingCeiling, ...]  # sorted by point


@dataclass(frozen=True, slots=True)
class Refusal:
    reason: Reason
    where: str  # a point name, "src -> dst", or "" for the declaration itself
    detail: str
    missing: tuple[Reason, ...] = ()


class TopologyError(ValueError):
    def __init__(self, refusals: list[Refusal]):
        self.refusals = tuple(sorted(refusals, key=lambda r: (r.reason, r.where, r.detail)))
        n = len(self.refusals)
        lines = [
            f"{r.reason.name} at {r.where or 'declaration'}: {r.detail}" for r in self.refusals
        ]
        super().__init__(f"{n} refusal{'s' if n > 1 else ''}: " + "; ".join(lines))


def loads(text: str) -> Topology:
    """Load a TOML declaration."""
    try:
        declaration = tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        raise TopologyError([Refusal(Reason.INVALID_VALUE, "", f"not TOML: {e}")]) from None
    return load(declaration)


def load(declaration: Mapping) -> Topology:
    """Validate a declaration and compute its ceilings, or raise TopologyError."""
    refusals: list[Refusal] = []
    points, hops = _parse(declaration, refusals)
    for point in points.values():
        _check_point_rules(point, refusals)
    hop_ceilings = []
    for hop in hops:
        if hop.src in points and hop.dst in points:
            ceiling = _hop_ceiling(hop, points[hop.src], points[hop.dst])
            _check_hop_rules(hop, ceiling, points[hop.src], points[hop.dst], refusals)
            hop_ceilings.append(ceiling)
    if refusals:
        raise TopologyError(refusals)
    sources = {h.src for h in hops}
    targets = {h.dst for h in hops}
    crossings = [_crossing_ceiling(points[name]) for name in sorted(sources & targets)]
    return Topology(
        points=tuple(points[name] for name in sorted(points)),
        hops=tuple(sorted(hop_ceilings, key=lambda c: (c.src, c.dst))),
        crossings=tuple(crossings),
    )


# --- parsing ------------------------------------------------------------------


class _Fields:
    """Reads one table, recording every refusal instead of stopping at the first."""

    def __init__(self, table: Mapping, where: str, allowed: frozenset, refusals: list):
        self.table, self.where, self.refusals = table, where, refusals
        for key in sorted(set(table) - allowed):
            self.refuse(Reason.UNKNOWN_FIELD, f"unknown field {key!r}")

    def refuse(self, reason: Reason, detail: str) -> None:
        self.refusals.append(Refusal(reason, self.where, detail))

    def text(self, key: str, required: bool = True, default=None):
        if key not in self.table:
            if required:
                self.refuse(Reason.MISSING_FIELD, f"{key} is required")
            return default
        value = self.table[key]
        if not isinstance(value, str):
            self.refuse(Reason.INVALID_VALUE, f"{key} must be a string, got {value!r}")
            return default
        return value

    def choice(self, key: str, allowed: frozenset, required: bool = True, default=None):
        value = self.text(key, required, default)
        if value is not default and value not in allowed:
            self.refuse(
                Reason.UNKNOWN_VALUE, f"{key} = {value!r}, expected one of {sorted(allowed)}"
            )
            return default
        return value

    def flag(self, key: str, default: bool) -> bool:
        value = self.table.get(key, default)
        if not isinstance(value, bool):
            self.refuse(Reason.INVALID_VALUE, f"{key} must be true or false, got {value!r}")
            return default
        return value

    def integer(self, key: str, minimum: int):
        if key not in self.table:
            return None
        value = self.table[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            self.refuse(
                Reason.INVALID_VALUE, f"{key} must be an integer >= {minimum}, got {value!r}"
            )
            return None
        return value

    def identifiers(self, key: str, allowed: frozenset) -> frozenset[str]:
        value = self.table.get(key, [])
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            self.refuse(Reason.INVALID_VALUE, f"{key} must be a list of identifiers")
            return frozenset()
        if len(set(value)) != len(value):
            self.refuse(Reason.INVALID_VALUE, f"{key} lists an identifier twice")
        unknown = sorted(set(value) - allowed)
        for name in unknown:
            self.refuse(Reason.UNKNOWN_IDENTIFIER, f"{key}: unknown identifier {name!r}")
        return frozenset(value) - set(unknown)


def _parse(declaration: Mapping, refusals: list) -> tuple[dict[str, Point], list[Hop]]:
    top = _Fields(declaration, "", frozenset({"points", "hops"}), refusals)
    raw_points = declaration.get("points")
    if raw_points is None:
        top.refuse(Reason.MISSING_FIELD, "points is required")
        raw_points = {}
    elif not isinstance(raw_points, Mapping) or not raw_points:
        top.refuse(Reason.INVALID_VALUE, "points must be a non-empty table")
        raw_points = {}
    points = {}
    for name in sorted(raw_points):
        if not isinstance(raw_points[name], Mapping):
            refusals.append(Refusal(Reason.INVALID_VALUE, name, "a point must be a table"))
            continue
        point = _parse_point(name, raw_points[name], refusals)
        if point is not None:
            points[name] = point

    raw_hops = declaration.get("hops", [])
    if not isinstance(raw_hops, list):
        top.refuse(Reason.INVALID_VALUE, "hops must be a list of tables")
        raw_hops = []
    hops, seen = [], set()
    for raw in raw_hops:
        if not isinstance(raw, Mapping):
            top.refuse(Reason.INVALID_VALUE, "a hop must be a table")
            continue
        hop = _parse_hop(raw, set(raw_points), refusals)
        if hop is None:
            continue
        if (hop.src, hop.dst) in seen:
            refusals.append(
                Refusal(Reason.DUPLICATE_HOP, f"{hop.src} -> {hop.dst}", "declared twice")
            )
            continue
        seen.add((hop.src, hop.dst))
        hops.append(hop)
    return points, hops


def _parse_point(name: str, raw: Mapping, refusals: list) -> Point | None:
    before = len(refusals)
    f = _Fields(raw, name, POINT_FIELDS, refusals)
    host = f.text("host")
    visibility = f.choice("visibility", VISIBILITY)
    timed = visibility not in (None, "none")
    clock = f.text("clock", required=timed)
    timestamps = f.choice("timestamps", frozenset(PRECISION_NS), required=timed)
    inbound = f.identifiers("inbound", IDENTIFIERS)
    outbound = f.identifiers("outbound", IDENTIFIERS)
    sets = f.identifiers("sets", CARRIED)
    trusts_incoming = f.flag("trusts_incoming", True)
    propagates = raw.get("propagates", {})
    if not isinstance(propagates, Mapping):
        f.refuse(Reason.INVALID_VALUE, "propagates must be a table: identifier = who copies it")
        propagates = {}
    carried = []
    for key in sorted(propagates):
        if key not in CARRIED:
            f.refuse(Reason.UNKNOWN_IDENTIFIER, f"propagates: unknown identifier {key!r}")
        elif propagates[key] not in PROPAGATORS:
            f.refuse(Reason.UNKNOWN_VALUE, f"propagates.{key} = {propagates[key]!r}")
        else:
            carried.append((key, propagates[key]))
    if len(refusals) > before:
        return None
    return Point(
        name, host, visibility, clock, timestamps, inbound, outbound, tuple(carried), sets,
        trusts_incoming,
    )  # fmt: skip


def _parse_hop(raw: Mapping, known: set, refusals: list) -> Hop | None:
    before = len(refusals)
    src, dst = raw.get("from"), raw.get("to")
    f = _Fields(raw, f"{src or '?'} -> {dst or '?'}", HOP_FIELDS, refusals)
    for key, name in (("from", src), ("to", dst)):
        if name is None:
            f.refuse(Reason.MISSING_FIELD, f"{key} is required")
        elif name not in known:
            f.refuse(Reason.UNKNOWN_POINT, f"{key} = {name!r} is not a declared point")
    if src is not None and src == dst:
        f.refuse(Reason.INVALID_VALUE, "a hop joins two different points")
    relation = f.choice("relation", RELATIONS, required=False, default="call")
    nat = f.choice("nat", NATS, required=False, default="none")
    nat_mapping = f.choice("nat_mapping", NAT_MAPPINGS, required=False, default="none")
    pooling = f.flag("pooling", False)
    rate = f.integer("rate", 1)
    noise_us = f.integer("noise_us", 0)
    expect = f.choice("expect", frozenset(e.name.lower() for e in Evidence), required=False)
    max_fanout = f.integer("max_fanout", 1)
    if relation == "fanout" and "max_fanout" not in raw:
        f.refuse(Reason.FANOUT_UNBOUNDED, "a fanout hop needs max_fanout")
    if relation != "fanout" and "max_fanout" in raw:
        f.refuse(Reason.INVALID_VALUE, "max_fanout applies to a fanout hop only")
    if len(refusals) > before:
        return None
    return Hop(
        src, dst, relation, nat, nat_mapping, pooling, rate, noise_us,
        None if expect is None else Evidence[expect.upper()], max_fanout,
    )  # fmt: skip


# --- ceilings -------------------------------------------------------------------


def _wire_header(src: Point, dst: Point) -> str | None:
    """The first header identifier recorded on both ends of the wire, as it was on it."""
    for header in HEADERS:
        # A point that sets its own header: its agent's outbound record still
        # carries the incoming one (measured), so only the point's own log counts.
        replaced = header in src.sets and src.visibility == "kernel"
        if header in src.outbound and header in dst.inbound and not replaced:
            return header
    return None


def _missing(hop: Hop, src: Point, dst: Point) -> dict[LinkKind, list[Reason]]:
    """For each link kind, every condition the hop does not meet (empty: it holds)."""
    both = src.outbound & dst.inbound
    shared = [] if _wire_header(src, dst) is not None else [Reason.NO_SHARED_HEADER]
    key = []
    if hop.relation != "async" or "message_id" not in both:
        key.append(Reason.NO_MESSAGE_KEY)
    connection, socket, timing = [], [], []
    if hop.relation == "async":
        connection.append(Reason.ASYNC_RELATION)
        socket.append(Reason.ASYNC_RELATION)
        timing.append(Reason.ASYNC_RELATION)
    if "tuple5" not in both:
        connection.append(Reason.MISSING_TUPLE5)
    if hop.nat != "none" and hop.nat_mapping == "none":
        connection.append(Reason.MISSING_NAT_MAPPING)
    if hop.pooling and "h2_stream_id" not in both:
        connection.append(Reason.MISSING_STREAM_ID)
    if "socket_cookie" not in both:
        socket.append(Reason.MISSING_SOCKET_COOKIE)
    if src.host != dst.host:
        socket.append(Reason.CROSS_HOST_SOCKET)
    if "none" in (src.visibility, dst.visibility):
        timing.append(Reason.NOT_TIMESTAMPED)
    elif src.clock != dst.clock:
        timing.append(Reason.NO_SHARED_CLOCK)
    return {
        LinkKind.SHARED_HEADER: shared,
        LinkKind.MESSAGE_KEY: key,
        LinkKind.SAME_CONNECTION: connection,
        LinkKind.SAME_SOCKET: socket,
        LinkKind.NESTED_TIMING: timing,
    }


# Strongest first: the ceiling is the first link kind whose conditions all hold.
_HOP_LINKS = (
    (LinkKind.SHARED_HEADER, Evidence.AUTHORED),
    (LinkKind.MESSAGE_KEY, Evidence.AUTHORED),
    (LinkKind.SAME_CONNECTION, Evidence.OBSERVED),
    (LinkKind.SAME_SOCKET, Evidence.OBSERVED),
    (LinkKind.NESTED_TIMING, Evidence.DERIVED),
)


def _hop_ceiling(hop: Hop, src: Point, dst: Point) -> HopCeiling:
    missing = _missing(hop, src, dst)
    evidence, link = Evidence.UNRESOLVED, LinkKind.NONE
    for kind, level in _HOP_LINKS:
        if not missing[kind]:
            evidence, link = level, kind
            break
    density = None
    if link is LinkKind.NESTED_TIMING and hop.rate is not None:
        window_ns = max(
            PRECISION_NS[src.timestamps],
            PRECISION_NS[dst.timestamps],
            1_000 * (hop.noise_us or 0),
        )
        density = Fraction(hop.rate * window_ns, 1_000_000_000)
    key = {LinkKind.SHARED_HEADER: _wire_header(src, dst), LinkKind.MESSAGE_KEY: "message_id"}
    return HopCeiling(
        hop.src, hop.dst, hop.relation, evidence, link, density, hop.max_fanout, key.get(link)
    )


def _crossing_ceiling(point: Point) -> CrossingCeiling:
    copied = {name for name, _who in point.propagates}
    for header in (*HEADERS, "message_id"):
        if (
            header in copied
            and header in point.inbound
            and header in point.outbound
            and header not in point.sets
        ):
            return CrossingCeiling(point.name, Evidence.AUTHORED, LinkKind.PROPAGATED, header)
    if point.visibility == "kernel":
        return CrossingCeiling(point.name, Evidence.OBSERVED, LinkKind.INSIDE_PROCESS, None)
    if point.visibility in ("log", "app"):
        return CrossingCeiling(point.name, Evidence.DERIVED, LinkKind.LOGGED_TIMING, None)
    return CrossingCeiling(point.name, Evidence.UNRESOLVED, LinkKind.NONE, None)


# --- rules ----------------------------------------------------------------------


def _check_point_rules(point: Point, refusals: list) -> None:
    by_agent = sorted(name for name, who in point.propagates if who == "agent")
    if by_agent and point.visibility != "kernel":
        refusals.append(
            Refusal(
                Reason.AGENT_NOT_PRESENT,
                point.name,
                "an agent propagates on a point it does not see",
            )
        )
    if "baggage" in by_agent:
        refusals.append(
            Refusal(Reason.AGENT_BAGGAGE, point.name, "the agent does not carry baggage (measured)")
        )
    if not point.trusts_incoming:
        for name in by_agent:
            if name not in point.sets:
                refusals.append(
                    Refusal(
                        Reason.UNTRUSTED_ENTRY_PROPAGATED,
                        point.name,
                        f"the agent re-injects an outside {name} (measured); set it here instead",
                    )
                )


def _check_hop_rules(hop: Hop, ceiling: HopCeiling, src: Point, dst: Point, refusals: list) -> None:
    where = f"{hop.src} -> {hop.dst}"
    if ceiling.density is not None and ceiling.density > D_MAX:
        refusals.append(
            Refusal(
                Reason.DENSITY_ABOVE_BOUND,
                where,
                f"density {float(ceiling.density):g} > {D_MAX}: timing cannot carry this rate",
            )
        )
    if hop.expect is not None and ceiling.evidence < hop.expect:
        missing = _missing(hop, src, dst)
        lacking = sorted(
            {
                reason
                for kind, level in _HOP_LINKS
                if level == hop.expect
                for reason in missing[kind]
            }
        )
        refusals.append(
            Refusal(
                Reason.CEILING_BELOW_EXPECTED,
                where,
                f"expected {hop.expect.name.lower()}, at most {ceiling.evidence.name.lower()};"
                f" missing: {', '.join(r.name.lower() for r in lacking)}",
                tuple(lacking),
            )
        )
