"""Property-based tests.

Example cases check what someone thought to check. These properties hold over
*every* input, including the ones nobody thought of — and they are what make a
decision defensible.

The most important one is permutation invariance: if the result depends on the
order in which records arrive, no decision can be defended and replaying proves
nothing.
"""

from datetime import date
from decimal import Decimal

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from reconcileflow import Record, RuleId, Tolerance, reconcile
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


SETTINGS = settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


@SETTINGS
@given(records("L"), records("R"), tolerances)
def test_permutation_invariance(lefts, rights, tol):
    """Input presentation order does not change the result.

    This is the property that underpins replay: without it, re-running the
    engine over the same data proves nothing.
    """
    assert reconcile(lefts, rights, tol) == reconcile(
        list(reversed(lefts)), list(reversed(rights)), tol
    )


@SETTINGS
@given(records("L"), records("R"), tolerances)
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
@given(records("L"), records("R"), tolerances)
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
@given(records("L"), records("R"), tolerances)
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
@given(records("L"), records("R"), tolerances)
def test_aggregate_residual_stays_within_tolerance(lefts, rights, tol):
    by_id = {r.id: r for r in lefts}
    result = reconcile(lefts, rights, tol)

    for match in result.matches:
        if match.rule is RuleId.M3_AGGREGATE:
            limit = amount_tolerance(by_id[match.left_id].amount, tol)
            assert abs(match.amount_residual) <= limit
            assert 2 <= len(match.right_ids) <= tol.max_aggregate_size


@SETTINGS
@given(records("L"), records("R"), tolerances)
def test_idempotence(lefts, rights, tol):
    """Two identical runs produce the same bytes."""
    assert reconcile(lefts, rights, tol) == reconcile(lefts, rights, tol)


@SETTINGS
@given(records("L"), records("R"), tolerances)
def test_ruleset_id_is_constant_for_a_given_ruleset(lefts, rights, tol):
    """The fingerprint depends on the rules, never on the data."""
    assert reconcile(lefts, rights, tol).ruleset_id == tol.ruleset_id()


@SETTINGS
@given(records("L"), records("R"), tolerances)
def test_a_rejected_pair_carries_exactly_one_reason(lefts, rights, tol):
    """Reason codes are mutually exclusive per pair.

    Aggregating rejections by reason is the root-cause breakdown. If a pair
    could carry two reasons, that breakdown would double-count and mislead.
    """
    result = reconcile(lefts, rights, tol)

    pairs = [(r.left_id, r.right_id) for r in result.rejects if r.right_id is not None]
    assert len(pairs) == len(set(pairs))


@SETTINGS
@given(records("L"), records("R"), tolerances)
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
@given(records("L"), records("R"), tolerances)
def test_distinct_decisions_have_distinct_fingerprints(lefts, rights, tol):
    """No collision between decisions of one run.

    Two different decisions sharing a fingerprint would make the platform
    treat a real change as "unchanged".
    """
    result = reconcile(lefts, rights, tol)
    decisions = (*result.matches, *result.rejects)

    by_fingerprint = {d.fingerprint: d for d in decisions}
    assert len(by_fingerprint) == len(set(decisions))
