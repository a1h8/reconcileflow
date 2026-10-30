"""Stream semantics (J3): provisional, final and superseded decisions.

The matching engine is stateless and order-independent; it knows nothing about
revisions. This module replays it as records arrive and turns its successive
outputs into an append-only history of decisions (README, "J3 — Streaming"):

- the input is a journal of arrivals and source progress signals, in order;
- each arrival re-evaluates its block with the engine; a decision whose
  content changes is superseded by a new one, never edited;
- a decision becomes FINAL when its horizon closes: the watermark, the slower
  side's progress minus the tolerated lateness, passes value date + window. A
  match's horizon is its latest input's: until then a stricter rule could still
  take one of its right records;
- a final decision withdraws its records: a later arrival cannot contend for
  them. The final state can therefore differ from matching the whole journal at
  once, when a late arrival would have won; that is the price of finality;
- an arrival already under its block's watermark is a final LATE break;
- SUPERSEDED is derived (another decision replaces this one), never written.

Every clock in a decision is replayable: the business horizon, and positions
among the journal's distinct entries for "decided" and "final". There is no
wall-clock time, so the same journal always yields the same bytes.
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
BusinessKey = tuple[str, str]  # (side, record id)
LATE = "LATE"
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

    def ruleset_id(self) -> str:
        """The engine's ruleset fingerprint, extended with the lateness: it
        changes decisions, so two lateness settings must not look the same."""
        return _digest(self.tolerance.ruleset_id(), "lateness", str(self.lateness_days))


@dataclass(frozen=True, slots=True)
class Decision:
    decision_id: str
    business_key: BusinessKey
    chain_version: int  # rank in its business key's chain, from 1
    status: Status  # PROVISIONAL or FINAL
    decision_type: DecisionType
    input_ids: tuple[str, ...]  # left id first, then right ids
    # A RuleId name for a match; the distinct RejectReason names joined by "+",
    # or LATE, for a break. A right record's chain points to the left key's
    # match with AS_COUNTERPART: a match is counted once, on its left key.
    reason: str
    ruleset_id: str
    engine_version: str
    window_end: date  # business clock: the horizon
    decided_at: int  # processing clock: position among distinct journal entries
    final_at: int | None  # finalisation clock: position where the horizon closed
    supersedes: str | None


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


class _Replay:
    def __init__(self, config: StreamConfig):
        if not (
            isinstance(config.lateness_days, int)
            and not isinstance(config.lateness_days, bool)
            and config.lateness_days >= 0
        ):
            raise ValueError(f"lateness_days must be an integer >= 0, got {config.lateness_days!r}")
        self.tol = config.tolerance
        self.window = timedelta(days=self.tol.date_days)
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
            late = _Proposal(
                DecisionType.BREAK, (record.id,), LATE, record.value_date + self.window
            )
            self._emit(key, late, Status.FINAL)
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
        """Finalise every decision whose horizon has closed, re-evaluate what is
        left, and repeat until nothing more closes."""
        while closing := self._closing(block):
            for decision in closing:
                proposal = _Proposal(
                    decision.decision_type, decision.input_ids, decision.reason, decision.window_end
                )
                self._emit(decision.business_key, proposal, Status.FINAL)
                side, record_id = decision.business_key
                del block.pool[side][record_id]
            if any(block.pool[side] for side in SIDES):
                self._evaluate(block)

    def _closing(self, block: _Block) -> list[Decision]:
        """The open decisions whose horizon the watermark has passed."""
        watermark = self._watermark(block)
        if watermark is None:
            return []
        closing = []
        for side in SIDES:
            for record_id in sorted(block.pool[side]):
                decision = self.current[(side, record_id)]
                if decision.window_end < watermark:
                    closing.append(decision)
        return closing

    # -- evaluation ------------------------------------------------------------------

    def _evaluate(self, block: _Block) -> None:
        """Run the engine on the block's open records; supersede what changed."""
        lefts, rights = block.pool[LEFT], block.pool[RIGHT]
        result = reconcile(list(lefts.values()), list(rights.values()), self.tol)
        proposals: dict[BusinessKey, _Proposal] = {}
        for match in result.matches:
            inputs = (match.left_id, *match.right_ids)
            horizon = max(
                lefts[match.left_id].value_date, *(rights[r].value_date for r in match.right_ids)
            )
            window_end = horizon + self.window
            proposals[(LEFT, match.left_id)] = _Proposal(
                DecisionType.MATCH, inputs, match.rule.name, window_end
            )
            for right_id in match.right_ids:
                proposals[(RIGHT, right_id)] = _Proposal(
                    DecisionType.MATCH, inputs, AS_COUNTERPART, window_end
                )
        reasons: dict[BusinessKey, set[RejectReason]] = {}
        for reject in result.rejects:
            reasons.setdefault((LEFT, reject.left_id), set()).add(reject.reason)
            if reject.right_id is not None:
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
                        record.value_date + self.window,
                    )
        for key in sorted(proposals):
            current = self.current.get(key)
            if current is None or _fingerprint(current) != proposals[key].fingerprint:
                self._emit(key, proposals[key], Status.PROVISIONAL)

    def _emit(self, key: BusinessKey, proposal: _Proposal, status: Status) -> None:
        previous = self.current.get(key)
        chain_version = 1 if previous is None else previous.chain_version + 1
        if status is Status.FINAL:
            # Content only: the chain's length depends on the arrival order.
            decision_id = _digest(*key, proposal.fingerprint, Status.FINAL.value)
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
