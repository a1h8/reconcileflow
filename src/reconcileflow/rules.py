"""Candidate generation, rule by rule.

Each rule produces *candidate* matches — final selection happens in the engine
(``engine._reconcile_block``), not here. A rule consumes nothing and holds no
state: it proposes.

Common signature: ``(lefts, rights, tol) -> list[Match]``. M0 through M2 produce
candidates with a single ``right_id``, M3 produces several. Same return type in
both cases — that is the whole point.
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Sequence
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal

from .models import Match, Record, RuleId

_ZERO = Decimal("0")
_QUANTUM = Decimal("0.000001")

# Base score per rule: a stricter rule always beats a more permissive one,
# whatever penalties are applied afterwards (those are bounded to 0.11 total).
_BASE_SCORE: dict[RuleId, Decimal] = {
    RuleId.M0_REFERENCE: Decimal("1.0"),
    RuleId.M1_EXACT: Decimal("0.9"),
    RuleId.M2_TOLERANT: Decimal("0.8"),
    RuleId.M3_AGGREGATE: Decimal("0.6"),
}

_AMOUNT_PENALTY = Decimal("0.05")
_DATE_PENALTY = Decimal("0.05")
_SIZE_PENALTY = Decimal("0.01")


def amount_tolerance(amount: Decimal, tol) -> Decimal:
    """Tolerance applicable to an amount: absolute part plus relative part."""
    return tol.amount_abs + (abs(amount) * tol.amount_rel)


def _date_gap(a: date, b: date) -> int:
    return abs((a - b).days)


def _score(
    rule: RuleId,
    residual: Decimal,
    limit: Decimal,
    date_gap: int,
    tol,
    size: int = 1,
) -> Decimal:
    """Deterministic, bounded, auditable score.

    Decreases with amount discrepancy, date gap and aggregate size. Quantised
    so that two runs produce the same bytes.
    """
    penalty = _ZERO
    if limit > _ZERO:
        penalty += (abs(residual) / limit) * _AMOUNT_PENALTY
    if tol.date_days > 0:
        penalty += (Decimal(date_gap) / Decimal(tol.date_days)) * _DATE_PENALTY
    penalty += Decimal(size - 1) * _SIZE_PENALTY
    return (_BASE_SCORE[rule] - penalty).quantize(_QUANTUM, rounding=ROUND_HALF_EVEN)


def m0_reference(lefts: Sequence[Record], rights: Sequence[Record], tol) -> list[Match]:
    """Identical transaction reference.

    Deliberately indifferent to the amount: in reconciliation, two lines
    carrying the same reference *are* the same operation. An amount
    discrepancy is then not a non-match — it is an "amount discrepancy" break,
    and you must match first in order to qualify it. The residual is therefore
    reported, not used to reject.
    """
    by_reference: dict[str, list[Record]] = defaultdict(list)
    for right in rights:
        if right.reference:
            by_reference[right.reference].append(right)

    out: list[Match] = []
    for left in lefts:
        if not left.reference:
            continue
        for right in by_reference.get(left.reference, ()):
            out.append(
                Match(
                    left_id=left.id,
                    right_ids=(right.id,),
                    rule=RuleId.M0_REFERENCE,
                    score=_BASE_SCORE[RuleId.M0_REFERENCE],
                    amount_residual=left.amount - right.amount,
                    date_gap_days=_date_gap(left.value_date, right.value_date),
                )
            )
    return out


def m1_exact(lefts: Sequence[Record], rights: Sequence[Record], tol) -> list[Match]:
    """Exactly equal amount, date within the window."""
    out: list[Match] = []
    for left in lefts:
        for right in rights:
            if left.amount != right.amount:
                continue
            gap = _date_gap(left.value_date, right.value_date)
            if gap > tol.date_days:
                continue
            out.append(
                Match(
                    left_id=left.id,
                    right_ids=(right.id,),
                    rule=RuleId.M1_EXACT,
                    score=_score(RuleId.M1_EXACT, _ZERO, _ZERO, gap, tol),
                    amount_residual=_ZERO,
                    date_gap_days=gap,
                )
            )
    return out


def m2_tolerant(lefts: Sequence[Record], rights: Sequence[Record], tol) -> list[Match]:
    """Amount within tolerance, date within the window."""
    out: list[Match] = []
    for left in lefts:
        limit = amount_tolerance(left.amount, tol)
        for right in rights:
            residual = left.amount - right.amount
            if abs(residual) > limit:
                continue
            gap = _date_gap(left.value_date, right.value_date)
            if gap > tol.date_days:
                continue
            out.append(
                Match(
                    left_id=left.id,
                    right_ids=(right.id,),
                    rule=RuleId.M2_TOLERANT,
                    score=_score(RuleId.M2_TOLERANT, residual, limit, gap, tol),
                    amount_residual=residual,
                    date_gap_days=gap,
                )
            )
    return out


def m3_aggregate(lefts: Sequence[Record], rights: Sequence[Record], tol) -> list[Match]:
    """One record on the left against *n* on the right (bundled entry).

    A subset-sum problem with tolerance: NP-hard in the general case. Made
    tractable by two bounds — subset size (``max_aggregate_size``) and block
    size (checked by the caller). Past those the engine does not guess: it
    emits ``BLOCK_TOO_LARGE`` and a human decides.

    A false match costs vastly more than a reported break.
    """
    if tol.max_aggregate_size < 2:
        return []

    out: list[Match] = []
    for left in lefts:
        in_window = [
            right
            for right in rights
            if _date_gap(left.value_date, right.value_date) <= tol.date_days
        ]
        if len(in_window) < 2:
            continue

        limit = amount_tolerance(left.amount, tol)
        for size in range(2, tol.max_aggregate_size + 1):
            for combo in itertools.combinations(in_window, size):
                residual = left.amount - sum((r.amount for r in combo), _ZERO)
                if abs(residual) > limit:
                    continue
                gap = max(_date_gap(left.value_date, r.value_date) for r in combo)
                out.append(
                    Match(
                        left_id=left.id,
                        right_ids=tuple(sorted(r.id for r in combo)),
                        rule=RuleId.M3_AGGREGATE,
                        score=_score(RuleId.M3_AGGREGATE, residual, limit, gap, tol, size),
                        amount_residual=residual,
                        date_gap_days=gap,
                    )
                )
    return out


GENERATORS = {
    RuleId.M0_REFERENCE: m0_reference,
    RuleId.M1_EXACT: m1_exact,
    RuleId.M2_TOLERANT: m2_tolerant,
    RuleId.M3_AGGREGATE: m3_aggregate,
}
