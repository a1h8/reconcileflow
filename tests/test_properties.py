"""Property-based tests.

Example cases check what someone thought to check. These properties hold over
*every* input, including the ones nobody thought of — and they are what make a
decision defensible.

The most important one is permutation invariance: if the result depends on the
order in which records arrive, no decision can be defended and replaying proves
nothing.
"""

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from reconcileflow import Record, RuleId, Tolerance, reconcile
from reconcileflow.engine import blocking_key
from reconcileflow.rules import amount_tolerance

amounts = st.decimals(
    min_value=Decimal("-10000"),
    max_value=Decimal("10000"),
    places=2,
    allow_nan=False,
    allow_infinity=False,
)
value_dates = st.dates(min_value=date(2024, 1, 1), max_value=date(2024, 2, 29))
accounts = st.sampled_from(["A", "B"])
references = st.one_of(st.none(), st.sampled_from(["TX-1", "TX-2"]))

tolerances = st.builds(
    Tolerance,
    amount_abs=st.sampled_from([Decimal("0.00"), Decimal("0.50"), Decimal("5.00")]),
    amount_rel=st.sampled_from([Decimal("0"), Decimal("0.01")]),
    date_days=st.integers(min_value=0, max_value=5),
    max_aggregate_size=st.integers(min_value=1, max_value=3),
    max_block_candidates=st.just(32),
)


@st.composite
def records(draw, prefix: str, max_size: int = 6):
    size = draw(st.integers(min_value=0, max_value=max_size))
    return [
        Record(
            id=f"{prefix}{i}",
            account=draw(accounts),
            amount=draw(amounts),
            value_date=draw(value_dates),
            reference=draw(references),
        )
        for i in range(size)
    ]


@st.composite
def related_rights(draw, lefts: list[Record], tol: Tolerance):
    """Right-hand records partly derived from the left-hand ones.

    Independent random amounts almost never land within tolerance of each
    other, let alone sum to one another: with ``records("R")`` alone, M2 fired
    in ~1% of examples and M3 in none, so every property below held vacuously
    for the rules where greedy resolution is most fragile. Each left record
    here may get an exact copy, a near-match straddling the amount and date
    bounds, or a split into 2-3 parts -- on top of unrelated noise.
    """
    rights = list(draw(records("R")))
    for i, left in enumerate(lefts):
        kind = draw(st.sampled_from(["none", "exact", "near", "split"]))
        if kind == "none":
            continue
        limit = amount_tolerance(left.amount, tol).quantize(Decimal("0.01"))
        if kind == "split":
            parts = draw(st.integers(min_value=2, max_value=3))
            split_amounts = [draw(amounts) for _ in range(parts - 1)]
            split_amounts.append(left.amount - sum(split_amounts, Decimal("0")))
        elif kind == "near":
            delta = draw(st.decimals(min_value=-limit - 1, max_value=limit + 1, places=2))
            split_amounts = [left.amount + delta]
        else:
            split_amounts = [left.amount]
        for j, amount in enumerate(split_amounts):
            shift = draw(st.integers(min_value=-tol.date_days - 1, max_value=tol.date_days + 1))
            # replace(), not Record(): a derived record inherits every field
            # that decides its block (account, and currency once records carry
            # one), or it lands in a block its source never sees.
            rights.append(
                replace(
                    left,
                    id=f"D{i}.{j}",
                    amount=amount,
                    value_date=left.value_date + timedelta(days=shift),
                    reference=draw(references),
                )
            )
    return rights


# Shared so the derived rights see the very lefts and tolerance the test gets.
LEFTS = st.shared(records("L"), key="lefts")
TOLERANCES = st.shared(tolerances, key="tolerance")
RIGHTS = st.tuples(LEFTS, TOLERANCES).flatmap(lambda pair: related_rights(*pair))


SETTINGS = settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_permutation_invariance(lefts, rights, tol):
    """Input presentation order does not change the result.

    This is the property that underpins replay: without it, re-running the
    engine over the same data proves nothing.
    """
    assert reconcile(lefts, rights, tol) == reconcile(
        list(reversed(lefts)), list(reversed(rights)), tol
    )


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_exclusivity(lefts, rights, tol):
    """No record is matched twice, on either side.

    A record counted twice is an accounting error, not an imprecision.
    """
    result = reconcile(lefts, rights, tol)

    left_ids = [m.left_id for m in result.matches]
    right_ids = [rid for m in result.matches for rid in m.right_ids]

    assert len(left_ids) == len(set(left_ids))
    assert len(right_ids) == len(set(right_ids))


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_conservation(lefts, rights, tol):
    """Every left-hand record is either matched or justified.

    Nothing disappears silently: that is the difference between a
    reconciliation engine and a join.
    """
    result = reconcile(lefts, rights, tol)

    matched = {m.left_id for m in result.matches}
    justified = {r.left_id for r in result.rejects}

    for record in lefts:
        assert (record.id in matched) ^ (record.id in justified)


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_matches_respect_the_declared_bounds(lefts, rights, tol):
    """No match falls outside the announced date window.

    Amount tolerance is deliberately excluded: M0 matches on the reference
    alone and reports the discrepancy, by documented business choice.
    """
    result = reconcile(lefts, rights, tol)

    for match in result.matches:
        if match.rule is not RuleId.M0_REFERENCE:
            assert match.date_gap_days <= tol.date_days


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_aggregate_residual_stays_within_tolerance(lefts, rights, tol):
    by_id = {r.id: r for r in lefts}
    result = reconcile(lefts, rights, tol)

    for match in result.matches:
        if match.rule is RuleId.M3_AGGREGATE:
            limit = amount_tolerance(by_id[match.left_id].amount, tol)
            assert abs(match.amount_residual) <= limit
            assert 2 <= len(match.right_ids) <= tol.max_aggregate_size


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_idempotence(lefts, rights, tol):
    """Two identical runs produce the same bytes."""
    assert reconcile(lefts, rights, tol) == reconcile(lefts, rights, tol)


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_ruleset_id_is_constant_for_a_given_ruleset(lefts, rights, tol):
    """The fingerprint depends on the rules, never on the data."""
    assert reconcile(lefts, rights, tol).ruleset_id == tol.ruleset_id()


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_a_rejected_pair_carries_exactly_one_reason(lefts, rights, tol):
    """Reason codes are mutually exclusive per pair.

    Aggregating rejections by reason is the root-cause breakdown. If a pair
    could carry two reasons, that breakdown would double-count and mislead.
    """
    result = reconcile(lefts, rights, tol)

    pairs = [(r.left_id, r.right_id) for r in result.rejects if r.right_id is not None]
    assert len(pairs) == len(set(pairs))


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_every_pair_of_an_unmatched_record_carries_a_reason(lefts, rights, tol):
    """The converse of exactly-one-reason: at least one.

    The engine emits nothing for a pair no reason applies to, on the
    argument that such a pair would have been matched by M2. If that
    argument ever broke, a candidate would vanish from the audit trail
    silently -- so it is checked, not assumed.
    """
    result = reconcile(lefts, rights, tol)
    matched = {m.left_id for m in result.matches}
    explained = {(r.left_id, r.right_id) for r in result.rejects if r.right_id is not None}

    for left in lefts:
        if left.id in matched:
            continue
        # The engine's own blocking key, not a re-derivation of it: pairs in
        # different blocks are never compared, and are not expected to carry
        # a pair-level reason (the left one gets NO_CANDIDATE instead).
        block = [r for r in rights if blocking_key(r) == blocking_key(left)]
        assert len(block) <= tol.max_block_candidates  # pair detail only for bounded blocks
        for right in block:
            assert (left.id, right.id) in explained


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_fingerprints_are_stable_across_runs(lefts, rights, tol):
    """A fingerprint depends on content, never on run conditions.

    The platform compares fingerprints to decide whether a decision changed.
    A fingerprint that drifts between runs would emit phantom transitions;
    one that collides would hide real ones.
    """
    a = reconcile(lefts, rights, tol)
    b = reconcile(list(reversed(lefts)), list(reversed(rights)), tol)

    assert a.result_hash == b.result_hash
    assert [m.fingerprint for m in a.matches] == [m.fingerprint for m in b.matches]


@SETTINGS
@given(LEFTS, RIGHTS, TOLERANCES)
def test_distinct_decisions_have_distinct_fingerprints(lefts, rights, tol):
    """No collision between decisions of one run.

    Two different decisions sharing a fingerprint would make the platform
    treat a real change as "unchanged".
    """
    result = reconcile(lefts, rights, tol)
    decisions = (*result.matches, *result.rejects)

    by_fingerprint = {d.fingerprint: d for d in decisions}
    assert len(by_fingerprint) == len(set(decisions))
