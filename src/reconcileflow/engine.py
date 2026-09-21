"""Engine: blocking, ordered rule application, resolution, audit.

Central property: **the result does not depend on input order.** Two runs over
the same records presented in a different order produce the same bytes. That is
what makes a decision defensible — and it is the property the tests check first.

It is obtained through three explicit sorts: blocks by key, records by
identifier, candidates by a total order.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence

from .models import Match, MatchResult, Record, Reject, RejectReason, RuleId, Tolerance
from .rules import GENERATORS, amount_tolerance

BlockKey = tuple[str, ...]


def blocking_key(record: Record) -> BlockKey:
    """Partition key, bounding the comparison space.

    Deliberately coarse: blocking must be **more permissive than any rule**,
    otherwise it discards true matches before the rules ever get to speak —
    and those false breaks are invisible, since nothing reports a comparison
    that never happened.

    Adding the date to the key is tempting and wrong as long as a tolerance
    window exists: a ±3 day window straddles month boundaries. Finer blocking
    (by amount bucket, say) is a subject in itself — see Splink's blocking.
    """
    return (record.account,)


def _candidate_order(match: Match) -> tuple:
    """Total order over candidates: descending score, then identifiers.

    Tie-breaking by identifier is not cosmetic: without it, two candidates
    with equal scores would be separated by iteration order, hence by input
    order.
    """
    return (-match.score, match.left_id, match.right_ids)


def _pair_reject_reason(
    left: Record, right: Record, tol: Tolerance, right_taken: bool
) -> RejectReason | None:
    """Why ``right`` was not matched to ``left``, or None.

    Reasons are checked in a strict priority order, so a rejected pair always
    carries exactly one reason — the invariant that makes aggregation by
    reason a valid root-cause breakdown.

    None means no reason applies: the pair should have matched. That case
    should not occur; emitting nothing is preferable to inventing a reason.
    """
    if right_taken:
        return RejectReason.LOST_TO_BETTER_CANDIDATE
    if abs((left.value_date - right.value_date).days) > tol.date_days:
        return RejectReason.DATE_OUT_OF_WINDOW
    if abs(left.amount - right.amount) > amount_tolerance(left.amount, tol):
        return RejectReason.AMOUNT_OUT_OF_TOLERANCE
    return None


def _reconcile_block(
    lefts: list[Record],
    rights: list[Record],
    tol: Tolerance,
    matches: list[Match],
    rejects: list[Reject],
    counters: Counter,
) -> None:
    lefts = sorted(lefts, key=lambda r: r.id)
    rights = sorted(rights, key=lambda r: r.id)

    taken_left: set[str] = set()
    taken_right: set[str] = set()

    # Past the bound, M3's subset enumeration explodes. We do not degrade
    # silently: it is stated in the audit trail.
    oversized = len(rights) > tol.max_block_candidates

    for rule in RuleId:
        if rule is RuleId.M3_AGGREGATE and oversized:
            continue

        available_left = [r for r in lefts if r.id not in taken_left]
        available_right = [r for r in rights if r.id not in taken_right]
        if not available_left or not available_right:
            break

        candidates = GENERATORS[rule](available_left, available_right, tol)
        counters["candidates_evaluated"] += len(candidates)

        for candidate in sorted(candidates, key=_candidate_order):
            if candidate.left_id in taken_left:
                continue
            if any(right_id in taken_right for right_id in candidate.right_ids):
                continue
            taken_left.add(candidate.left_id)
            taken_right.update(candidate.right_ids)
            matches.append(candidate)
            counters[f"matched_{rule.name}"] += 1

    _audit_unmatched(lefts, rights, tol, taken_left, taken_right, oversized, rejects, counters)


def _audit_unmatched(
    lefts: list[Record],
    rights: list[Record],
    tol: Tolerance,
    taken_left: set[str],
    taken_right: set[str],
    oversized: bool,
    rejects: list[Reject],
    counters: Counter,
) -> None:
    """Produce the justification for unmatched records.

    This is the engine's primary deliverable, not a by-product: a break with
    no reason has no operational value.

    Per-pair detail is emitted in full here. In a streaming setting it costs
    O(candidates) and will have to be aggregated by reason code, with full
    detail reserved for flagged blocks — the decision itself is never sampled.
    """
    for left in lefts:
        if left.id in taken_left:
            continue
        counters["unmatched_left"] += 1

        if not rights:
            rejects.append(Reject(left.id, None, RejectReason.NO_CANDIDATE))
            continue

        if oversized:
            rejects.append(Reject(left.id, None, RejectReason.BLOCK_TOO_LARGE))
            counters["manual_review"] += 1
            continue

        for right in rights:
            reason = _pair_reject_reason(left, right, tol, right.id in taken_right)
            if reason is not None:
                rejects.append(Reject(left.id, right.id, reason))

        # If a combination was conceivable and none was found, say so
        # explicitly: "no single counterpart" and "no combination sums to the
        # right amount" are two different breaks.
        if tol.max_aggregate_size >= 2:
            in_window_free = [
                r
                for r in rights
                if r.id not in taken_right
                and abs((left.value_date - r.value_date).days) <= tol.date_days
            ]
            if len(in_window_free) >= 2:
                rejects.append(Reject(left.id, None, RejectReason.AGGREGATE_NOT_FOUND))

    for right in rights:
        if right.id not in taken_right:
            counters["unmatched_right"] += 1


def reconcile(
    left: Sequence[Record],
    right: Sequence[Record],
    tol: Tolerance | None = None,
) -> MatchResult:
    """Match two collections of normalised records.

    Returns matches, rejection justifications and counters. Writes nothing,
    logs nothing, knows nothing about the network: the caller decides what to
    do with the audit trail and the metrics.
    """
    tol = tol or Tolerance()

    counters: Counter = Counter()
    counters["records_left"] = len(left)
    counters["records_right"] = len(right)

    blocks: dict[BlockKey, tuple[list[Record], list[Record]]] = defaultdict(
        lambda: ([], [])
    )
    for record in left:
        blocks[blocking_key(record)][0].append(record)
    for record in right:
        blocks[blocking_key(record)][1].append(record)

    counters["blocks"] = len(blocks)

    matches: list[Match] = []
    rejects: list[Reject] = []
    for key in sorted(blocks):
        block_left, block_right = blocks[key]
        _reconcile_block(block_left, block_right, tol, matches, rejects, counters)

    return MatchResult(
        matches=tuple(matches),
        rejects=tuple(rejects),
        counters=dict(counters),
        ruleset_id=tol.ruleset_id(),
    )
