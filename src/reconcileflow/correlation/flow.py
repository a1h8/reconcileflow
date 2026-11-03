"""Correlation as a stream (steps A and B: authored and observed links), with a
replayable proof.

Observations from n declared sources arrive in a journal, each with its event
time on a common reference. Observations that share a join identifier value
form a group; a group is re-joined with ``exact.exact_links`` as it grows, and
its decisions (linked, collided, unlinked) are provisional until no on-time
observation can change them: the watermark, the slowest declared source's
progress minus the tolerated lateness, has passed the group's latest event time
plus the window. Then they become final together, and the group's proof — a
dossier — is emitted: the observations, every final decision, the fingerprints
of the topology and the settings, the engine version, the journal positions.

- a link waits: a value seen once on each side is a link only until another
  observation carries it, which makes all of them a collision. Waiting for the
  horizon is the price of never finalising a link that a reused or injected
  identifier would have turned into a collision;
- an observation under the watermark is a final LATE decision, and changes
  nothing final; one that carries a value of an already closed group, on time
  but past the window, is a final VALUE_AFTER_FINAL decision: in a batch it
  would have collided, so it is told rather than linked;
- a declared source that has never reported holds the watermark at the
  journal's first event time; one that lags the fastest by more than a declared
  bound stops holding it, and what closes meanwhile is final and marked forced;
- a hop whose two ends record the connection (and the stream, when pooled)
  has a second signal next to the propagated identifier. Both naming the same
  pair corroborate the link; naming different pairs, or the second recorded on
  both ends with other values, is CONFLICTED: no link. A second signal missing
  on one end leaves the link standing, marked SIGNAL_ABSENT: never silent. A
  connection comes back legitimately, so only authored values are told after
  their group closed;
- SUPERSEDED is derived, never written; the same journal yields the same bytes.
"""

from __future__ import annotations

import hashlib
import heapq
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum
from fractions import Fraction

from ..stream import _engine_version
from .exact import (
    Identity,
    Observation,
    _composite_of,
    _validate,
    exact_links,
    observed_key,
    observed_links,
)
from .topology import Evidence, Topology

_SEP = "\x1f"  # as in models._digest: no field can run into the next


def _digest(*parts: str) -> str:
    return hashlib.sha256(_SEP.join(parts).encode()).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class Seen:
    observation: Observation
    at: int  # event time, microseconds, on the common reference
    captured_at: datetime | None = None  # data for observation (J4), never read here


@dataclass(frozen=True, slots=True)
class Advance:
    """The source is complete up to event time ``until``."""

    source: str
    until: int


Entry = Seen | Advance


@dataclass(frozen=True, slots=True)
class FlowConfig:
    sources: tuple[str, ...]  # declared up front, so a silent one is known
    window_us: int  # how long after an event another observation of it may come
    lateness_us: int = 0
    max_silence_us: int | None = None  # None: a silent source holds everything

    def config_id(self) -> str:
        return _digest(
            "flow",
            "sources",
            *sorted(self.sources),
            "window",
            str(self.window_us),
            "lateness",
            str(self.lateness_us),
            "max_silence",
            str(self.max_silence_us),
        )


class Status(Enum):
    PROVISIONAL = "PROVISIONAL"
    FINAL = "FINAL"
    SUPERSEDED = "SUPERSEDED"  # derived by FlowResult.status, never written


class Kind(Enum):
    LINKED = "LINKED"
    COLLIDED = "COLLIDED"
    UNLINKED = "UNLINKED"
    LATE = "LATE"
    VALUE_AFTER_FINAL = "VALUE_AFTER_FINAL"  # carries a value of an already closed group
    CONFLICTED = "CONFLICTED"


class Corroboration(Enum):
    """How a hop's second signal stands against its first (step B)."""

    CORROBORATED = "CORROBORATED"
    SIGNAL_ABSENT = "SIGNAL_ABSENT"
    SIGNAL_UNMATCHED = "SIGNAL_UNMATCHED"
    SIGNAL_OTHER_PARTNER = "SIGNAL_OTHER_PARTNER"


Where = str  # a join: "src -> dst" for a hop, the point name for a crossing; "" for LATE
BusinessKey = tuple[Where, Identity]


@dataclass(frozen=True, slots=True)
class FlowDecision:
    decision_id: str
    business_key: BusinessKey
    chain_version: int
    status: Status
    kind: Kind
    key: str | None  # the identifier joined on
    value: str | None
    partners: tuple[Identity, ...]  # the other side's observations at this join
    decided_at: int  # position among distinct journal entries
    final_at: int | None
    forced: bool
    supersedes: str | None
    evidence: Evidence | None = None
    corroboration: Corroboration | None = None
    missing_on: tuple[Identity, ...] = ()


@dataclass(frozen=True, slots=True)
class Dossier:
    """The proof for one closed group: everything needed to redo it."""

    dossier_id: str  # content only: the same group gives the same id in any order
    observations: tuple[Identity, ...]  # sorted
    decisions: tuple[str, ...]  # the final decision ids, sorted
    forced: bool
    topology_id: str
    config_id: str
    engine_version: str
    positions: tuple[int, ...]  # journal positions of the observations, in identity order
    closed_at: int


@dataclass(frozen=True, slots=True)
class FlowResult:
    decisions: tuple[FlowDecision, ...]  # in emission order
    dossiers: tuple[Dossier, ...]  # in closing order
    duplicates: int

    def status(self, decision: FlowDecision) -> Status:
        """SUPERSEDED when another decision replaces it, else its own status."""
        if any(d.supersedes == decision.decision_id for d in self.decisions):
            return Status.SUPERSEDED
        return decision.status

    def effective(self) -> Mapping[BusinessKey, FlowDecision]:
        """For each business key, its last decision."""
        return {d.business_key: d for d in self.decisions}

    def signal_counts(self) -> Mapping[tuple[Where, Corroboration], int]:
        """Final decisions by join and corroboration: series for observation (J4)."""
        counts: dict[tuple[Where, Corroboration], int] = {}
        for (where, _), d in self.effective().items():
            if d.status is Status.FINAL and d.corroboration is not None:
                counts[where, d.corroboration] = counts.get((where, d.corroboration), 0) + 1
        return counts


@dataclass(frozen=True, slots=True)
class _Proposal:
    kind: Kind
    key: str | None
    value: str | None
    partners: tuple[Identity, ...]
    evidence: Evidence | None = None
    corroboration: Corroboration | None = None
    missing_on: tuple[Identity, ...] = ()

    @property
    def fingerprint(self) -> str:
        partners = [f"{source}{_SEP}{seq}" for source, seq in self.partners]
        # The evidence follows from the key and the topology (fingerprinted apart);
        # a join with one signal hashes as in step A, so its ids do not move.
        second = []
        if self.corroboration is not None:
            missing = [f"{source}{_SEP}{seq}" for source, seq in self.missing_on]
            second = ["corroboration", self.corroboration.value, *missing]
        return _digest(self.kind.value, self.key or "", self.value or "", *partners, *second)

    @classmethod
    def of(cls, decision: FlowDecision) -> _Proposal:
        return cls(
            decision.kind,
            decision.key,
            decision.value,
            decision.partners,
            decision.evidence,
            decision.corroboration,
            decision.missing_on,
        )


def _canonical(value):
    """A representation that does not depend on set ordering."""
    if is_dataclass(value):
        return (
            type(value).__name__,
            tuple((f.name, _canonical(getattr(value, f.name))) for f in fields(value)),
        )
    if isinstance(value, frozenset):
        return tuple(sorted(_canonical(v) for v in value))
    if isinstance(value, tuple):
        return tuple(_canonical(v) for v in value)
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, Fraction):
        return str(value)
    return value


def topology_id(topology: Topology) -> str:
    """A fingerprint of the topology that does not depend on set ordering."""
    return _digest("topology", repr(_canonical(topology)))


def _check(config: FlowConfig) -> None:
    sources = config.sources
    if not (
        isinstance(sources, tuple)
        and sources
        and all(isinstance(s, str) and s for s in sources)
        and len(set(sources)) == len(sources)
    ):
        raise ValueError(f"sources must be a non-empty tuple of distinct names, got {sources!r}")
    for name, minimum in (("window_us", 0), ("lateness_us", 0)):
        value = getattr(config, name)
        if not (isinstance(value, int) and not isinstance(value, bool) and value >= minimum):
            raise ValueError(f"{name} must be an integer >= {minimum}, got {value!r}")
    bound = config.max_silence_us
    if bound is not None and not (
        isinstance(bound, int) and not isinstance(bound, bool) and bound >= 1
    ):
        raise ValueError(f"max_silence_us must be None or an integer >= 1, got {bound!r}")


class _Flow:
    def __init__(self, topology: Topology, config: FlowConfig):
        _check(config)
        self.topology = topology
        self.config = config
        self.points = {p.name for p in topology.points}
        # The identifier each join is made on: a hop's or a crossing's key.
        self.join_key: dict[Where, str] = {
            f"{h.src} -> {h.dst}": h.key for h in topology.hops if h.key
        } | {c.point: c.key for c in topology.crossings if c.key}
        self.join_keys = frozenset(self.join_key.values())
        # The second signal of each hop that has one: its connection identifiers.
        self.observed: dict[Where, tuple[str, ...]] = {
            f"{h.src} -> {h.dst}": h.observed for h in topology.hops if h.observed
        }
        self.topology_id = topology_id(topology)
        self.config_id = config.config_id()
        self.engine_version = _engine_version()
        self.progress: dict[str, int | None] = dict.fromkeys(config.sources)
        self.first_at: int | None = None
        self.seen: dict[Identity, Seen] = {}
        self.open: dict[Identity, Seen] = {}
        self.positions: dict[Identity, int] = {}
        self.keys_of: dict[Identity, set[BusinessKey]] = {}
        # Who holds each join value, and open observations by event time: a group
        # is found from its members, never by going over all that is open.
        self.holders: dict[tuple[str, str], set[Identity]] = {}
        self.pending: list[tuple[int, Identity]] = []
        self.closed_values: set[tuple[str, str]] = set()  # (join key, value) of closed groups
        self.current: dict[BusinessKey, FlowDecision] = {}
        self.decisions: list[FlowDecision] = []
        self.dossiers: list[Dossier] = []
        self.duplicates = 0
        self.position = -1

    # -- journal ---------------------------------------------------------------

    def feed(self, entry: Entry) -> None:
        if isinstance(entry, Seen):
            self._seen(entry)
        elif isinstance(entry, Advance):
            self._declared(entry.source)
            self._time(entry.until, "until")
            self.position += 1
            self._advance(entry.source, entry.until)
            self._settle()
        else:
            raise ValueError(f"unknown journal entry {entry!r}")

    def _declared(self, source: str) -> None:
        if source not in self.progress:
            raise ValueError(f"undeclared source {source!r}")

    @staticmethod
    def _time(value: object, name: str) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{name} must be an integer of microseconds, got {value!r}")

    def _advance(self, source: str, until: int) -> None:
        previous = self.progress[source]
        self.progress[source] = until if previous is None else max(previous, until)
        self.first_at = until if self.first_at is None else min(self.first_at, until)

    def _seen(self, entry: Seen) -> None:
        obs = entry.observation
        _validate(obs, self.points)
        self._declared(obs.source)
        self._time(entry.at, "at")
        identity = (obs.source, obs.seq)
        if identity in self.seen:
            before = self.seen[identity]
            if (before.observation, before.at) != (obs, entry.at):
                raise ValueError(f"observation {identity!r} delivered with different content")
            self.duplicates += 1
            return
        self.seen[identity] = entry
        self.position += 1
        self.positions[identity] = self.position
        late = self.first_at is not None and entry.at < self._watermark()[0]
        self._advance(obs.source, entry.at)
        if late:
            self._emit(("", identity), _Proposal(Kind.LATE, None, None, ()), Status.FINAL)
            return
        reused = sorted(
            (key, obs.keys[key])
            for key in self.join_keys.intersection(obs.keys)
            if (key, obs.keys[key]) in self.closed_values
        )
        if reused:
            # On time, but past the window of a group already closed on this
            # value: in a batch it would collide, so it is told, never linked.
            key, value = reused[0]
            proposal = _Proposal(Kind.VALUE_AFTER_FINAL, key, value, ())
            self._emit(("", identity), proposal, Status.FINAL)
            return
        self.open[identity] = entry
        self.keys_of[identity] = set()
        for value in self._values(obs):
            self.holders.setdefault(value, set()).add(identity)
        heapq.heappush(self.pending, (entry.at, identity))
        self._evaluate(self._group_of(identity))
        self._settle()

    # -- watermark and groups --------------------------------------------------

    def _watermark(self) -> tuple[int, bool]:
        """The slowest holding source's progress minus the lateness, and whether a
        silent source was left out. A source that never reported holds the
        journal's first event time until it counts as silent. Called once an
        entry has set ``first_at``."""
        effective = {s: (p if p is not None else self.first_at) for s, p in self.progress.items()}
        fastest = max(effective.values())
        bound = self.config.max_silence_us
        holding = [p for p in effective.values() if bound is None or fastest - p <= bound]
        return min(holding) - self.config.lateness_us, len(holding) < len(effective)

    def _group_of(self, identity: Identity) -> list[Identity]:
        """The open observations reached from ``identity`` through shared join
        identifier values, sorted."""
        group = {identity}
        todo = [identity]
        while todo:
            for value in self._values(self.open[todo.pop()].observation):
                others = self.holders[value] - group
                group |= others
                todo += others
        return sorted(group)

    def _values(self, obs: Observation) -> list[tuple[str, str]]:
        """The (key, value) pairs a group is united by: authored and observed."""
        values = [(key, obs.keys[key]) for key in sorted(self.join_keys.intersection(obs.keys))]
        for identifiers in sorted(set(self.observed.values())):
            composite = _composite_of(obs, identifiers)
            if composite is not None:
                values.append((observed_key(identifiers), composite))
        return values

    def _ripe_groups(self, watermark: int) -> list[list[Identity]]:
        """The groups whose latest event the watermark has passed, sorted. Each is
        found when its latest member comes out of the queue: until then, a member
        coming out has a later one holding its group open."""
        groups = {}
        while self.pending and self.pending[0][0] + self.config.window_us < watermark:
            group = self._group_of(heapq.heappop(self.pending)[1])
            if max(self.open[i].at for i in group) + self.config.window_us < watermark:
                groups[group[0]] = group
        return sorted(groups.values())

    def _settle(self) -> None:
        watermark, forced = self._watermark()
        for group in self._ripe_groups(watermark):
            self._close(group, forced)

    def _close(self, group: list[Identity], forced: bool) -> None:
        finals = []
        for identity in group:
            for business_key in sorted(self.keys_of[identity]):
                proposal = _Proposal.of(self.current[business_key])
                finals.append(self._emit(business_key, proposal, Status.FINAL, forced).decision_id)
            for value in self._values(self.open[identity].observation):
                self.holders[value].discard(identity)
                if not self.holders[value]:
                    del self.holders[value]
            keys = self.open.pop(identity).observation.keys
            self.closed_values.update((k, keys[k]) for k in self.join_keys.intersection(keys))
        decisions = tuple(sorted(finals))
        observations = tuple(group)  # groups are sorted
        dossier_id = _digest(
            "dossier",
            self.topology_id,
            self.config_id,
            *(f"{s}{_SEP}{q}" for s, q in observations),
            *decisions,
        )
        self.dossiers.append(
            Dossier(
                dossier_id=dossier_id,
                observations=observations,
                decisions=decisions,
                forced=forced,
                topology_id=self.topology_id,
                config_id=self.config_id,
                engine_version=self.engine_version,
                positions=tuple(self.positions[i] for i in observations),
                closed_at=self.position,
            )
        )

    # -- evaluation --------------------------------------------------------------

    def _evaluate(self, group: list[Identity]) -> None:
        observations = [self.open[i].observation for i in group]
        authored = _outcomes(exact_links(self.topology, observations), self._authored_value)
        observed = _outcomes(observed_links(self.topology, observations), self._observed_value)
        proposals: dict[BusinessKey, _Proposal] = {}
        for business_key in sorted(authored.keys() | observed.keys()):
            where = business_key[0]
            if where in self.observed and where in self.join_key:
                proposals[business_key] = self._combined(
                    business_key, authored.get(business_key), observed.get(business_key)
                )
            else:
                outcome = authored.get(business_key) or observed[business_key]
                evidence = Evidence.AUTHORED if where in self.join_key else Evidence.OBSERVED
                proposals[business_key] = _single(outcome, evidence)
        for business_key in sorted(proposals):
            self.keys_of[business_key[1]].add(business_key)
            current = self.current.get(business_key)
            if current is None or _fingerprint(current) != proposals[business_key].fingerprint:
                self._emit(business_key, proposals[business_key], Status.PROVISIONAL)

    def _combined(
        self, business_key: BusinessKey, authored: _Outcome | None, observed: _Outcome | None
    ) -> _Proposal:
        """A hop with two signals: corroborated, traced when one is missing, no
        link when they disagree."""
        where, identity = business_key
        for outcome in (authored, observed):
            if outcome is not None and outcome[0] is Kind.COLLIDED:
                return _Proposal(*outcome)
        a_linked = authored is not None and authored[0] is Kind.LINKED
        o_linked = observed is not None and observed[0] is Kind.LINKED
        if a_linked and o_linked:
            if authored[3] == observed[3]:
                return _Proposal(*authored, Evidence.AUTHORED, Corroboration.CORROBORATED)
            partners = tuple(sorted({*authored[3], *observed[3]}))
            return _Proposal(
                Kind.CONFLICTED, *authored[1:3], partners, None, Corroboration.SIGNAL_OTHER_PARTNER
            )
        if a_linked or o_linked:
            linked, evidence = (
                (authored, Evidence.AUTHORED) if a_linked else (observed, Evidence.OBSERVED)
            )
            pair = (identity, *linked[3])
            other = self._has_observed if a_linked else self._has_authored
            missing = tuple(sorted(i for i in pair if not other(where, i)))
            if missing:
                return _Proposal(*linked, evidence, Corroboration.SIGNAL_ABSENT, missing)
            return _Proposal(Kind.CONFLICTED, *linked[1:], None, Corroboration.SIGNAL_UNMATCHED)
        return _Proposal(*(authored or observed))

    def _authored_value(self, where: Where, identity: Identity) -> tuple[str, str]:
        key = self.join_key[where]
        return key, self.open[identity].observation.keys[key]

    def _observed_value(self, where: Where, identity: Identity) -> tuple[str, str]:
        identifiers = self.observed[where]
        return observed_key(identifiers), _composite_of(
            self.open[identity].observation, identifiers
        )

    def _has_authored(self, where: Where, identity: Identity) -> bool:
        return self.join_key[where] in self.open[identity].observation.keys

    def _has_observed(self, where: Where, identity: Identity) -> bool:
        return _composite_of(self.open[identity].observation, self.observed[where]) is not None

    def _emit(
        self, key: BusinessKey, proposal: _Proposal, status: Status, forced: bool = False
    ) -> FlowDecision:
        previous = self.current.get(key)
        chain_version = 1 if previous is None else previous.chain_version + 1
        where, (source, seq) = key
        if status is Status.FINAL:
            # Content only: the chain's length depends on the arrival order.
            finality = "FORCED" if forced else Status.FINAL.value
            decision_id = _digest(where, source, str(seq), proposal.fingerprint, finality)
        else:
            decision_id = _digest(where, source, str(seq), str(chain_version), proposal.fingerprint)
        decision = FlowDecision(
            decision_id=decision_id,
            business_key=key,
            chain_version=chain_version,
            status=status,
            kind=proposal.kind,
            key=proposal.key,
            value=proposal.value,
            partners=proposal.partners,
            decided_at=self.position,
            final_at=self.position if status is Status.FINAL else None,
            forced=forced,
            supersedes=None if previous is None else previous.decision_id,
            evidence=proposal.evidence,
            corroboration=proposal.corroboration,
            missing_on=proposal.missing_on,
        )
        self.current[key] = decision
        self.decisions.append(decision)
        return decision


def _fingerprint(decision: FlowDecision) -> str:
    return _Proposal.of(decision).fingerprint


_Outcome = tuple[Kind, str, str, tuple[Identity, ...]]  # kind, key, value, partners


def _single(outcome: _Outcome, evidence: Evidence) -> _Proposal:
    """A join with one declared signal: step A's decision."""
    kind, key, value, partners = outcome
    if kind is Kind.LINKED:
        return _Proposal(kind, key, value, partners, evidence)
    return _Proposal(kind, key, value, partners)


def _outcomes(result, value_of) -> dict[BusinessKey, _Outcome]:
    """Each observation's outcome at each join of one signal's result."""
    out: dict[BusinessKey, _Outcome] = {}
    for link in result.links:
        out[(link.where, link.upstream)] = (Kind.LINKED, link.key, link.value, (link.downstream,))
        out[(link.where, link.downstream)] = (Kind.LINKED, link.key, link.value, (link.upstream,))
    for c in result.collisions:
        for identity in c.upstream:
            out[(c.where, identity)] = (Kind.COLLIDED, c.key, c.value, c.downstream)
        for identity in c.downstream:
            out[(c.where, identity)] = (Kind.COLLIDED, c.key, c.value, c.upstream)
    for where, identity in result.unlinked:
        out[(where, identity)] = (Kind.UNLINKED, *value_of(where, identity), ())
    return out


def replay_flow(topology: Topology, journal: Iterable[Entry], config: FlowConfig) -> FlowResult:
    """Replay the correlation over a journal; return every decision and every proof."""
    state = _Flow(topology, config)
    for entry in journal:
        state.feed(entry)
    return FlowResult(
        decisions=tuple(state.decisions),
        dossiers=tuple(state.dossiers),
        duplicates=state.duplicates,
    )
