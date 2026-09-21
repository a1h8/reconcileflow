"""Core reconciliation types.

Two invariants are carried by these types rather than by documentation:

1. Amounts are ``Decimal``, never ``float``. Binary rounding produces
   cent-level discrepancies — precisely the kind of break the engine is meant
   to detect, manufactured by the engine itself.
2. Every justification is a **return value**. ``Reject`` records and counters
   leave with the result; the engine neither logs nor exports them. The shape
   of the audit trail is fixed by this interface, so replacing the
   implementation never changes it. See "Design notes" in the README.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import IntEnum

# Field separator for content hashes. Without one, ("ab", "c") and ("a", "bc")
# would hash identically — which would make the platform miss a transition or
# emit a phantom one. U+001F cannot appear in an identifier.
_SEP = "\x1f"


def _digest(*parts: str) -> str:
    return hashlib.sha256(_SEP.join(parts).encode()).hexdigest()[:16]


class RuleId(IntEnum):
    """Rules, ordered from strictest to most permissive.

    The enum order *is* the application order — the engine iterates over it.
    Inserting a rule between two existing ones changes decisions, and therefore
    the ruleset identifier (see ``Tolerance.ruleset_id``).
    """

    M0_REFERENCE = 0
    M1_EXACT = 1
    M2_TOLERANT = 2
    M3_AGGREGATE = 3


class RejectReason(IntEnum):
    """Rejection reasons — enumerated, never free text.

    Free-text reasons cannot be joined in SQL, cannot be translated, and are
    not stable across versions.

    The values are meant to be mutually exclusive: a rejected pair carries
    exactly one reason, so that aggregating by reason yields a root-cause
    breakdown rather than double counting.
    """

    NO_CANDIDATE = 1
    DATE_OUT_OF_WINDOW = 2
    AMOUNT_OUT_OF_TOLERANCE = 3
    LOST_TO_BETTER_CANDIDATE = 4
    AGGREGATE_NOT_FOUND = 5
    BLOCK_TOO_LARGE = 6


@dataclass(frozen=True, slots=True)
class Record:
    """A normalised record, ready to be matched.

    Normalisation (currency, sign, timezone, reference casing) has already
    happened: two records that correspond to each other carry **equal**
    amounts here, not opposite ones.
    """

    id: str
    account: str
    amount: Decimal
    value_date: date
    reference: str | None = None


@dataclass(frozen=True, slots=True)
class Tolerance:
    """Ruleset parameters.

    ``max_aggregate_size`` and ``max_block_candidates`` bound M3, which is a
    subset-sum problem: without bounds it is exponential. Past the bound the
    engine does not guess — it emits ``BLOCK_TOO_LARGE`` and a human decides.
    """

    amount_abs: Decimal = Decimal("0.00")
    amount_rel: Decimal = Decimal("0")
    date_days: int = 0
    max_aggregate_size: int = 3
    max_block_candidates: int = 32

    def ruleset_id(self) -> str:
        """Stable fingerprint of the ruleset, attached to every decision.

        One of the three elements that make a decision replayable: ruleset,
        code version, input offsets.
        """
        payload = "|".join(
            [
                *(r.name for r in RuleId),
                str(self.amount_abs),
                str(self.amount_rel),
                str(self.date_days),
                str(self.max_aggregate_size),
                str(self.max_block_candidates),
            ]
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class Match:
    """A match.

    ``right_ids`` is a tuple, not a single identifier: M3 matches one against
    many, and the interface must carry that from the start or be rewritten
    later.
    """

    left_id: str
    right_ids: tuple[str, ...]
    rule: RuleId
    score: Decimal
    amount_residual: Decimal
    date_gap_days: int

    @property
    def business_key(self) -> str:
        """Stable identity of what was decided, across revisions."""
        return self.left_id

    @property
    def fingerprint(self) -> str:
        """Content hash of the decision.

        The engine does not know about revisions, supersession or previously
        published decisions — that is the platform's job. It only has to make
        "did this decision change?" a cheap comparison: same fingerprint means
        no transition, different fingerprint means one.

        The score is deliberately excluded: it ranks candidates during
        resolution and is not part of the decision's meaning. Including it
        would emit transitions whenever scoring is retuned without any match
        actually changing.
        """
        return _digest(
            "MATCH",
            self.left_id,
            ",".join(self.right_ids),
            self.rule.name,
            str(self.amount_residual),
        )


@dataclass(frozen=True, slots=True)
class Reject:
    """A discarded candidate and its reason. This is the audit trail."""

    left_id: str
    right_id: str | None
    reason: RejectReason

    @property
    def business_key(self) -> str:
        return self.left_id

    @property
    def fingerprint(self) -> str:
        """Content hash of a break.

        A reason that changes *is* a transition — "no counterpart" becoming
        "amount out of tolerance" is a different finding, and an operator must
        see it.
        """
        return _digest("BREAK", self.left_id, self.right_id or "", self.reason.name)


@dataclass(frozen=True, slots=True)
class MatchResult:
    """Full engine output: decisions, justifications, counters.

    Nothing is emitted as a side effect. Counters feed OpenTelemetry on the
    caller's side; the engine knows nothing about the network.
    """

    matches: tuple[Match, ...]
    rejects: tuple[Reject, ...]
    counters: Mapping[str, int]
    ruleset_id: str

    @property
    def result_hash(self) -> str:
        """Content hash of the whole outcome, ruleset included.

        Lets a caller answer "did anything change since the last run?" without
        walking every decision. Fingerprints are sorted before hashing so the
        value depends on the *set* of decisions, not on the order the engine
        happened to emit them — the engine is already deterministic, but this
        keeps the hash meaningful if that ever stops being true.
        """
        parts = sorted(d.fingerprint for d in (*self.matches, *self.rejects))
        return _digest(self.ruleset_id, *parts)
