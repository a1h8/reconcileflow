"""Correlation as a stream (step A: authored links), with a replayable proof.

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
- SUPERSEDED is derived, never written; the same journal yields the same bytes.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum
from fractions import Fraction

from ..stream import _engine_version
from .exact import Identity, Observation, _validate, exact_links
from .topology import Topology

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


@dataclass(frozen=True, slots=True)
class _Proposal:
    kind: Kind
    key: str | None
    value: str | None
    partners: tuple[Identity, ...]

    @property
    def fingerprint(self) -> str:
        partners = [f"{source}{_SEP}{seq}" for source, seq in self.partners]
        return _digest(self.kind.value, self.key or "", self.value or "", *partners)


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
        self.topology_id = topology_id(topology)
        self.config_id = config.config_id()
        self.engine_version = _engine_version()
        self.progress: dict[str, int | None] = dict.fromkeys(config.sources)
        self.first_at: int | None = None
        self.seen: dict[Identity, Seen] = {}
        self.open: dict[Identity, Seen] = {}
        self.positions: dict[Identity, int] = {}
        self.keys_of: dict[Identity, set[BusinessKey]] = {}
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
        group = next(g for g in self._groups() if identity in g)
        self._evaluate(group)
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

    def _groups(self) -> list[list[Identity]]:
        """Open observations linked by a shared join identifier value."""
        parent = {identity: identity for identity in self.open}

        def root(identity: Identity) -> Identity:
            return identity if parent[identity] == identity else root(parent[identity])

        holder: dict[tuple[str, str], Identity] = {}
        for identity in sorted(self.open):
            keys = self.open[identity].observation.keys
            for key in sorted(self.join_keys.intersection(keys)):
                other = holder.setdefault((key, keys[key]), identity)
                parent[root(identity)] = root(other)
        groups: dict[Identity, list[Identity]] = {}
        for identity in sorted(parent):
            groups.setdefault(root(identity), []).append(identity)
        return sorted(groups.values())

    def _settle(self) -> None:
        watermark, forced = self._watermark()
        for group in self._groups():
            if max(self.open[i].at for i in group) + self.config.window_us < watermark:
                self._close(group, forced)

    def _close(self, group: list[Identity], forced: bool) -> None:
        finals = []
        for identity in group:
            for business_key in sorted(self.keys_of[identity]):
                d = self.current[business_key]
                proposal = _Proposal(d.kind, d.key, d.value, d.partners)
                finals.append(self._emit(business_key, proposal, Status.FINAL, forced).decision_id)
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
        result = exact_links(self.topology, observations)
        proposals: dict[BusinessKey, _Proposal] = {}
        for link in result.links:
            proposals[(link.where, link.upstream)] = _Proposal(
                Kind.LINKED, link.key, link.value, (link.downstream,)
            )
            proposals[(link.where, link.downstream)] = _Proposal(
                Kind.LINKED, link.key, link.value, (link.upstream,)
            )
        for c in result.collisions:
            for identity in c.upstream:
                proposals[(c.where, identity)] = _Proposal(
                    Kind.COLLIDED, c.key, c.value, c.downstream
                )
            for identity in c.downstream:
                proposals[(c.where, identity)] = _Proposal(
                    Kind.COLLIDED, c.key, c.value, c.upstream
                )
        for where, identity in result.unlinked:
            key = self.join_key[where]
            value = self.open[identity].observation.keys[key]
            proposals[(where, identity)] = _Proposal(Kind.UNLINKED, key, value, ())
        for business_key in sorted(proposals):
            self.keys_of[business_key[1]].add(business_key)
            current = self.current.get(business_key)
            if current is None or _fingerprint(current) != proposals[business_key].fingerprint:
                self._emit(business_key, proposals[business_key], Status.PROVISIONAL)

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
        )
        self.current[key] = decision
        self.decisions.append(decision)
        return decision


def _fingerprint(decision: FlowDecision) -> str:
    return _Proposal(decision.kind, decision.key, decision.value, decision.partners).fingerprint


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
