"""Stream semantics (J3): provisional, final and superseded decisions.

The matching engine is stateless and order-independent; it knows nothing about
revisions. This module replays it as records arrive and turns its successive
outputs into an append-only history of decisions (README, "J3 — Streaming"):

- the input is a journal of arrivals and source progress signals, in order;
- each arrival re-evaluates its block with the engine; a decision whose
  fingerprint changes is superseded by a new one, never edited;
- a decision becomes FINAL when its horizon closes: the watermark, the slower
  side's progress minus the tolerated lateness, passes value date + window;
- SUPERSEDED is derived (another decision replaces this one), never written.

Interface only: ``replay`` is not implemented yet. The tests in
tests/test_stream.py are written first and must fail against it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import Enum

from .models import Record, Tolerance

LEFT, RIGHT = "left", "right"
BusinessKey = tuple[str, str]  # (side, record id)


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
        """The engine's ruleset fingerprint, extended with the lateness."""
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Decision:
    decision_id: str
    business_key: BusinessKey
    chain_version: int  # rank in its business key's chain, from 1
    status: Status  # PROVISIONAL or FINAL
    decision_type: DecisionType
    input_ids: tuple[str, ...]  # left id first, then right ids
    # A RuleId name for a match; a RejectReason name, or LATE, for a break. A right
    # record's chain points to the left key's match with AS_COUNTERPART: a match is
    # counted once, on its left key.
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
        raise NotImplementedError

    def effective(self, upto: int | None = None) -> Mapping[BusinessKey, Decision]:
        """Effective state after the entry at position ``upto`` (default: the end)."""
        raise NotImplementedError


def replay(journal: Iterable[Entry], config: StreamConfig) -> StreamResult:
    """Replay the engine over a journal and return the history of decisions."""
    raise NotImplementedError
