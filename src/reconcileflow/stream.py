"""Stream semantics (J3): provisional, final and superseded decisions.

The matching engine is stateless and order-independent; it knows nothing about
revisions. This module replays it as records arrive and turns its successive
outputs into an append-only history of decisions (README, "J3 — Streaming"):

- the input is a journal of arrivals and source progress signals, in order;
- each arrival re-evaluates its block with the engine; a decision whose
  content changes is superseded by a new one, never edited;
- decisions become FINAL by component, not one by one. Two open records of
  opposite sides are linked when they are within the date window, or carry the
  same reference; greedy resolution can carry a change along a chain of links,
  so a component is final only when none of its members can gain a neighbour:
  the watermark, the slower side's progress minus the tolerated lateness, has
  passed every member's horizon (value date + window, or + the reference
  horizon for a record carrying a reference);
- a component whose dates span more than a declared bound is cut instead: its
  decisions past their own horizon become final and are marked forced, since
  their content may then depend on the arrival order;
- a final decision withdraws its records. An arrival already under its block's
  watermark is a final LATE break; one whose reference is that of an already
  final record of the other side is a final REFERENCE_AFTER_FINAL break;
- a break's reasons count only pairs within the date window: at finality those
  are all known, so a reason does not depend on what else arrived first;
- SUPERSEDED is derived (another decision replaces this one), never written.

Every clock in a decision is replayable: the business horizon, and positions
among the journal's distinct entries for "decided" and "final". There is no
wall-clock time, so the same journal always yields the same decisions.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from importlib.metadata import PackageNotFoundError, version

from .engine import BlockKey, blocking_key, reconcile
from .models import Record, RejectReason, Tolerance

LEFT, RIGHT = "left", "right"
SIDES = (LEFT, RIGHT)
OTHER = {LEFT: RIGHT, RIGHT: LEFT}
BusinessKey = tuple[str, str]  # (side, record id)
LATE = "LATE"
REFERENCE_AFTER_FINAL = "REFERENCE_AFTER_FINAL"
AS_COUNTERPART = "AS_COUNTERPART"
_SEP = "\x1f"  # as in models._digest: no field can run into the next


def _digest(*parts: str) -> str:
    return hashlib.sha256(_SEP.join(parts).encode()).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class Arrival:
    """A record arriving on one side."""

    side: str  # LEFT | RIGHT
    record: Record


@dataclass(frozen=True, slots=True)
class Progress:
    """The source of one side is complete up to this value date.

    It moves the watermark when the source has nothing to send, and it is part
    of the journal, so a replay sees it at the same place.
    """

    side: str  # LEFT | RIGHT
    until: date


Entry = Arrival | Progress


class Status(Enum):
    PROVISIONAL = "PROVISIONAL"
    FINAL = "FINAL"
    SUPERSEDED = "SUPERSEDED"  # derived by StreamResult.status, never written


class DecisionType(Enum):
    MATCH = "MATCH"
    BREAK = "BREAK"


@dataclass(frozen=True, slots=True)
class StreamConfig:
    tolerance: Tolerance = field(default_factory=Tolerance)
    lateness_days: int = 0  # tolerated lateness, in value days
    # Declared assumption: two records with the same reference are at most this
    # many days apart. M0 ignores dates, so without it a reference has no
    # horizon. None: the date window.
    reference_horizon_days: int | None = None
    # A component spanning more days than this is cut (forced finality). None:
    # never cut, exact, but a dense stream may then never become final.
    max_component_days: int | None = None

    def ruleset_id(self) -> str:
        """The engine's ruleset fingerprint, extended with the stream settings:
        they change decisions, so two settings must not look the same."""
        return _digest(
            self.tolerance.ruleset_id(),
            "lateness",
            str(self.lateness_days),
            "reference_horizon",
            str(self.reference_horizon_days),
            "max_component",
            str(self.max_component_days),
        )


@dataclass(frozen=True, slots=True)
class Decision:
    decision_id: str
    business_key: BusinessKey
    chain_version: int  # rank in its business key's chain, from 1
    status: Status  # PROVISIONAL or FINAL
    decision_type: DecisionType
    input_ids: tuple[str, ...]  # left id first, then right ids
    # A RuleId name for a match; the distinct RejectReason names joined by "+",
    # LATE or REFERENCE_AFTER_FINAL, for a break. A right record's chain points
    # to the left key's match with AS_COUNTERPART: a match is counted once, on
    # its left key.
    reason: str
    ruleset_id: str
    engine_version: str
    window_end: date  # business clock: the horizon
    decided_at: int  # processing clock: position among distinct journal entries
    final_at: int | None  # finalisation clock: position where the horizon closed
    supersedes: str | None
    forced: bool  # final because its component was cut, not because it closed


@dataclass(frozen=True, slots=True)
class StreamResult:
    decisions: tuple[Decision, ...]  # in emission order
    duplicates: int  # arrivals seen again with the same content, ignored

    def status(self, decision: Decision) -> Status:
        """SUPERSEDED when another decision replaces it, else its own status."""
        if any(d.supersedes == decision.decision_id for d in self.decisions):
            return Status.SUPERSEDED
        return decision.status

    def effective(self, upto: int | None = None) -> Mapping[BusinessKey, Decision]:
        """Effective state after the entry at position ``upto`` (default: the end):
        for each business key, the last decision made by then."""
        state: dict[BusinessKey, Decision] = {}
        for d in self.decisions:
            if upto is None or d.decided_at <= upto:
                state[d.business_key] = d
        return state


@dataclass(frozen=True, slots=True)
class _Proposal:
    """What the engine says about one business key now."""

    decision_type: DecisionType
    input_ids: tuple[str, ...]
    reason: str
    window_end: date

    @property
    def fingerprint(self) -> str:
        return _digest(self.decision_type.value, ",".join(self.input_ids), self.reason)


@dataclass
class _Block:
    pool: dict[str, dict[str, Record]] = field(default_factory=lambda: {s: {} for s in SIDES})
    latest: dict[str, date | None] = field(default_factory=lambda: dict.fromkeys(SIDES))
    # References of the records each side has already withdrawn as final.
    final_references: dict[str, set[str]] = field(default_factory=lambda: {s: set() for s in SIDES})


def _check_days(name: str, value: object, minimum: int) -> None:
    if not (isinstance(value, int) and not isinstance(value, bool) and value >= minimum):
        raise ValueError(f"{name} must be an integer >= {minimum}, got {value!r}")


class _Replay:
    def __init__(self, config: StreamConfig):
        _check_days("lateness_days", config.lateness_days, 0)
        self.tol = config.tolerance
        reference_days = config.reference_horizon_days
        if reference_days is None:
            reference_days = self.tol.date_days
        _check_days("reference_horizon_days", reference_days, self.tol.date_days)
        if config.max_component_days is not None:
            _check_days("max_component_days", config.max_component_days, 0)
        self.max_component_days = config.max_component_days
        self.window = timedelta(days=self.tol.date_days)
        self.reference_window = timedelta(days=reference_days)
        self.lateness = timedelta(days=config.lateness_days)
        self.ruleset = config.ruleset_id()
        self.engine_version = _engine_version()
        self.seen: dict[BusinessKey, Record] = {}
        self.duplicates = 0
        self.progress: dict[str, date | None] = dict.fromkeys(SIDES)
        self.blocks: dict[BlockKey, _Block] = {}
        self.current: dict[BusinessKey, Decision] = {}
        self.decisions: list[Decision] = []
        self.position = -1

    def _horizon(self, record: Record) -> date:
        return record.value_date + (self.reference_window if record.reference else self.window)

    # -- journal ---------------------------------------------------------------

    def feed(self, entry: Entry) -> None:
        if isinstance(entry, Arrival):
            self._arrival(entry)
        elif isinstance(entry, Progress):
            _check_side(entry.side)
            self.position += 1
            previous = self.progress[entry.side]
            self.progress[entry.side] = (
                entry.until if previous is None else max(previous, entry.until)
            )
            for key in sorted(self.blocks):
                self._settle(self.blocks[key])
        else:
            raise ValueError(f"unknown journal entry {entry!r}")

    def _arrival(self, entry: Arrival) -> None:
        _check_side(entry.side)
        record = entry.record
        key = (entry.side, record.id)
        if key in self.seen:
            if self.seen[key] != record:
                raise ValueError(f"record {key!r} delivered with different content")
            self.duplicates += 1
            return
        self.seen[key] = record
        self.position += 1
        block = self.blocks.setdefault(blocking_key(record), _Block())
        watermark = self._watermark(block)
        if watermark is not None and record.value_date < watermark:
            reason = LATE
        elif record.reference in block.final_references[OTHER[entry.side]]:
            reason = REFERENCE_AFTER_FINAL
        else:
            reason = None
        if reason is not None:
            alone = _Proposal(DecisionType.BREAK, (record.id,), reason, self._horizon(record))
            self._emit(key, alone, Status.FINAL)
            return
        block.pool[entry.side][record.id] = record
        previous = block.latest[entry.side]
        block.latest[entry.side] = (
            record.value_date if previous is None else max(previous, record.value_date)
        )
        self._evaluate(block)
        self._settle(block)

    # -- watermark and finality ---------------------------------------------------

    def _watermark(self, block: _Block) -> date | None:
        """The slower side's progress minus the lateness; None while a side has none."""
        progress = []
        for side in SIDES:
            known = [d for d in (block.latest[side], self.progress[side]) if d is not None]
            if not known:
                return None
            progress.append(max(known))
        return min(progress) - self.lateness

    def _settle(self, block: _Block) -> None:
        """Finalise every component whose horizon has closed, or that is cut,
        re-evaluate what is left, and repeat until nothing more closes."""
        while closing := self._closing(block):
            for decision, forced in closing:
                proposal = _Proposal(
                    decision.decision_type, decision.input_ids, decision.reason, decision.window_end
                )
                self._emit(decision.business_key, proposal, Status.FINAL, forced)
                side, record_id = decision.business_key
                record = block.pool[side].pop(record_id)
                if record.reference:
                    block.final_references[side].add(record.reference)
            if any(block.pool[side] for side in SIDES):
                self._evaluate(block)

    def _closing(self, block: _Block) -> list[tuple[Decision, bool]]:
        """The decisions to finalise now, each with whether it is forced."""
        watermark = self._watermark(block)
        if watermark is None:
            return []
        closing = []
        for component in self._components(block):
            records = [block.pool[side][record_id] for side, record_id in component]
            decisions = [self.current[key] for key in component]
            if max(self._horizon(r) for r in records) < watermark:
                closing += [(d, False) for d in decisions]
            elif self.max_component_days is not None and (
                max(r.value_date for r in records) - min(r.value_date for r in records)
            ) > timedelta(days=self.max_component_days):
                closing += [(d, True) for d in decisions if d.window_end < watermark]
        return closing

    def _components(self, block: _Block) -> list[list[BusinessKey]]:
        """Open records linked by the date window or a shared reference, grouped
        into connected components, each sorted, in a deterministic order."""
        parent: dict[BusinessKey, BusinessKey] = {
            (side, record_id): (side, record_id) for side in SIDES for record_id in block.pool[side]
        }

        def root(key: BusinessKey) -> BusinessKey:
            return key if parent[key] == key else root(parent[key])

        for left_id, lrec in block.pool[LEFT].items():
            for right_id, rrec in block.pool[RIGHT].items():
                near = abs((lrec.value_date - rrec.value_date).days) <= self.tol.date_days
                if near or (lrec.reference and lrec.reference == rrec.reference):
                    parent[root((RIGHT, right_id))] = root((LEFT, left_id))
        groups: dict[BusinessKey, list[BusinessKey]] = {}
        for key in sorted(parent):
            groups.setdefault(root(key), []).append(key)
        return sorted(groups.values())

    # -- evaluation ------------------------------------------------------------------

    def _evaluate(self, block: _Block) -> None:
        """Run the engine on the block's open records; supersede what changed."""
        lefts, rights = block.pool[LEFT], block.pool[RIGHT]
        result = reconcile(list(lefts.values()), list(rights.values()), self.tol)
        proposals: dict[BusinessKey, _Proposal] = {}
        for match in result.matches:
            inputs = (match.left_id, *match.right_ids)
            window_end = max(
                self._horizon(lefts[match.left_id]),
                *(self._horizon(rights[r]) for r in match.right_ids),
            )
            proposals[(LEFT, match.left_id)] = _Proposal(
                DecisionType.MATCH, inputs, match.rule.name, window_end
            )
            for right_id in match.right_ids:
                proposals[(RIGHT, right_id)] = _Proposal(
                    DecisionType.MATCH, inputs, AS_COUNTERPART, window_end
                )
        reasons: dict[BusinessKey, set[RejectReason]] = {}
        for reject in result.rejects:
            if reject.right_id is None:
                reasons.setdefault((LEFT, reject.left_id), set()).add(reject.reason)
            elif (
                abs((lefts[reject.left_id].value_date - rights[reject.right_id].value_date).days)
                <= self.tol.date_days
            ):
                reasons.setdefault((LEFT, reject.left_id), set()).add(reject.reason)
                reasons.setdefault((RIGHT, reject.right_id), set()).add(reject.reason)
        for side, pool in ((LEFT, lefts), (RIGHT, rights)):
            for record_id, record in pool.items():
                key = (side, record_id)
                if key not in proposals:
                    found = sorted(reasons.get(key, {RejectReason.NO_CANDIDATE}))
                    proposals[key] = _Proposal(
                        DecisionType.BREAK,
                        (record_id,),
                        "+".join(r.name for r in found),
                        self._horizon(record),
                    )
        for key in sorted(proposals):
            current = self.current.get(key)
            if current is None or _fingerprint(current) != proposals[key].fingerprint:
                self._emit(key, proposals[key], Status.PROVISIONAL)

    def _emit(
        self, key: BusinessKey, proposal: _Proposal, status: Status, forced: bool = False
    ) -> None:
        previous = self.current.get(key)
        chain_version = 1 if previous is None else previous.chain_version + 1
        if status is Status.FINAL:
            # Content only: the chain's length depends on the arrival order.
            finality = "FORCED" if forced else Status.FINAL.value
            decision_id = _digest(*key, proposal.fingerprint, finality)
        else:
            decision_id = _digest(*key, str(chain_version), proposal.fingerprint)
        decision = Decision(
            decision_id=decision_id,
            business_key=key,
            chain_version=chain_version,
            status=status,
            decision_type=proposal.decision_type,
            input_ids=proposal.input_ids,
            reason=proposal.reason,
            ruleset_id=self.ruleset,
            engine_version=self.engine_version,
            window_end=proposal.window_end,
            decided_at=self.position,
            final_at=self.position if status is Status.FINAL else None,
            supersedes=None if previous is None else previous.decision_id,
            forced=forced,
        )
        self.current[key] = decision
        self.decisions.append(decision)


def replay(journal: Iterable[Entry], config: StreamConfig) -> StreamResult:
    """Replay the engine over a journal and return the history of decisions."""
    state = _Replay(config)
    for entry in journal:
        state.feed(entry)
    return StreamResult(decisions=tuple(state.decisions), duplicates=state.duplicates)


def _fingerprint(decision: Decision) -> str:
    return _digest(decision.decision_type.value, ",".join(decision.input_ids), decision.reason)


def _check_side(side: str) -> None:
    if side not in SIDES:
        raise ValueError(f"side must be {LEFT!r} or {RIGHT!r}, got {side!r}")


def _engine_version() -> str:
    try:
        return version("reconcileflow")
    except PackageNotFoundError:  # running from a source tree that is not installed
        return "unknown"
