"""Example cases: one business intent per test."""

from datetime import date
from decimal import Decimal

import pytest

from reconcileflow import Record, RejectReason, RuleId, Tolerance, reconcile

D = Decimal


def rec(id_, amount, day, account="ACC", reference=None):
    return Record(
        id=id_,
        account=account,
        amount=D(amount),
        value_date=date(2024, 3, day),
        reference=reference,
    )


def test_identical_reference_matches_despite_amount_discrepancy():
    """M0 matches on the reference alone.

    An amount discrepancy is not a reason to leave records unmatched: it is an
    "amount discrepancy" break, and you must match in order to qualify it.
    """
    result = reconcile(
        [rec("L1", "100.00", 1, reference="TX-9")],
        [rec("R1", "99.00", 1, reference="TX-9")],
        Tolerance(),
    )

    assert len(result.matches) == 1
    match = result.matches[0]
    assert match.rule is RuleId.M0_REFERENCE
    assert match.right_ids == ("R1",)
    assert match.amount_residual == D("1.00")


def test_exact_amount_within_date_window():
    result = reconcile(
        [rec("L1", "100.00", 1)],
        [rec("R1", "100.00", 3)],
        Tolerance(date_days=3),
    )

    assert len(result.matches) == 1
    assert result.matches[0].rule is RuleId.M1_EXACT
    assert result.matches[0].date_gap_days == 2


def test_date_outside_window_yields_a_typed_break():
    result = reconcile(
        [rec("L1", "100.00", 1)],
        [rec("R1", "100.00", 20)],
        Tolerance(date_days=3),
    )

    assert result.matches == ()
    assert (result.rejects[0].left_id, result.rejects[0].reason) == (
        "L1",
        RejectReason.DATE_OUT_OF_WINDOW,
    )


def test_amount_tolerance():
    result = reconcile(
        [rec("L1", "100.00", 1)],
        [rec("R1", "100.02", 1)],
        Tolerance(amount_abs=D("0.05")),
    )

    assert len(result.matches) == 1
    assert result.matches[0].rule is RuleId.M2_TOLERANT
    assert result.matches[0].amount_residual == D("-0.02")


def test_amount_outside_tolerance_yields_a_typed_break():
    result = reconcile(
        [rec("L1", "100.00", 1)],
        [rec("R1", "150.00", 1)],
        Tolerance(amount_abs=D("0.05"), max_aggregate_size=1),
    )

    assert result.matches == ()
    assert result.rejects[0].reason is RejectReason.AMOUNT_OUT_OF_TOLERANCE


def test_one_to_many_aggregation():
    """The bundled entry: M3 matches one on the left against n on the right."""
    result = reconcile(
        [rec("L1", "100.00", 1)],
        [rec("R1", "60.00", 1), rec("R2", "40.00", 2)],
        Tolerance(date_days=3, max_aggregate_size=3),
    )

    assert len(result.matches) == 1
    match = result.matches[0]
    assert match.rule is RuleId.M3_AGGREGATE
    assert match.right_ids == ("R1", "R2")
    assert match.amount_residual == D("0.00")


def test_no_combination_found_is_a_distinct_break():
    """"No counterpart" and "no combination sums correctly" are two breaks."""
    result = reconcile(
        [rec("L1", "999.00", 1)],
        [rec("R1", "10.00", 1), rec("R2", "20.00", 1)],
        Tolerance(date_days=3, max_aggregate_size=3),
    )

    assert result.matches == ()
    reasons = {r.reason for r in result.rejects}
    assert RejectReason.AGGREGATE_NOT_FOUND in reasons


def test_no_candidate_in_block():
    result = reconcile([rec("L1", "100.00", 1)], [], Tolerance())

    assert result.rejects == (
        type(result.rejects[0])("L1", None, RejectReason.NO_CANDIDATE),
    )


def test_oversized_block_goes_to_manual_review():
    """Past the bound the engine does not guess: it defers to a human."""
    rights = [rec(f"R{i}", "1.00", 1) for i in range(6)]
    result = reconcile(
        [rec("L1", "999.00", 1)],
        rights,
        Tolerance(date_days=3, max_aggregate_size=3, max_block_candidates=5),
    )

    assert result.matches == ()
    assert result.rejects[0].reason is RejectReason.BLOCK_TOO_LARGE
    assert result.counters["manual_review"] == 1


def test_rule_priority():
    """A strict rule always wins against a permissive one."""
    result = reconcile(
        [rec("L1", "100.00", 1, reference="TX-9")],
        [rec("R1", "100.00", 1), rec("R2", "500.00", 1, reference="TX-9")],
        Tolerance(date_days=3, amount_abs=D("1.00")),
    )

    by_left = {m.left_id: m for m in result.matches}
    assert by_left["L1"].rule is RuleId.M0_REFERENCE
    assert by_left["L1"].right_ids == ("R2",)


def test_disjoint_blocks_do_not_mix():
    result = reconcile(
        [rec("L1", "100.00", 1, account="A")],
        [rec("R1", "100.00", 1, account="B")],
        Tolerance(date_days=3),
    )

    assert result.matches == ()
    assert result.rejects[0].reason is RejectReason.NO_CANDIDATE


def test_ruleset_id_changes_with_parameters():
    """The ruleset fingerprint must move when the rules move."""
    a = reconcile([], [], Tolerance(date_days=3)).ruleset_id
    b = reconcile([], [], Tolerance(date_days=4)).ruleset_id

    assert a != b
    assert a == reconcile([], [], Tolerance(date_days=3)).ruleset_id


@pytest.mark.parametrize("count", [0, 1, 5])
def test_counters_are_consistent(count):
    lefts = [rec(f"L{i}", "10.00", 1) for i in range(count)]
    result = reconcile(lefts, [], Tolerance())

    assert result.counters["records_left"] == count
    assert result.counters.get("unmatched_left", 0) == count


def only_match(result):
    assert len(result.matches) == 1, result.matches
    return result.matches[0]


def test_closest_candidate_wins_within_a_rule():
    """Score orders candidates of one rule: the smaller discrepancy wins.

    R1 sorts first, so a broken score that ties or inverts would pick it.
    """
    result = reconcile(
        [rec("L1", "100.00", 1)],
        [rec("R1", "100.04", 1), rec("R2", "100.01", 1)],
        Tolerance(amount_abs=D("0.05")),
    )

    assert only_match(result).right_ids == ("R2",)


# Expected scores are computed by hand from rules.py's constants: base score
# per rule, minus 0.05 * residual/limit, 0.05 * gap/date_days and 0.01 per
# extra aggregated record, quantised to 6 places. They enter result_hash.


def test_tolerant_match_reports_residual_gap_and_score():
    match = only_match(
        reconcile(
            [rec("L1", "100.00", 1)],
            [rec("R1", "100.01", 2)],
            Tolerance(amount_abs=D("0.05"), date_days=3),
        )
    )

    assert match.rule is RuleId.M2_TOLERANT
    assert match.amount_residual == D("-0.01")
    assert match.date_gap_days == 1
    assert match.score == D("0.773333")  # 0.8 - 0.01 - 0.016667


def test_one_day_window_still_penalises_the_date_gap():
    match = only_match(
        reconcile([rec("L1", "100.00", 1)], [rec("R1", "100.00", 2)], Tolerance(date_days=1))
    )

    assert match.rule is RuleId.M1_EXACT
    assert match.amount_residual == D("0")
    assert match.date_gap_days == 1
    assert match.score == D("0.850000")  # 0.9 - 0.05


def test_aggregate_match_reports_residual_gap_and_score():
    match = only_match(
        reconcile(
            [rec("L1", "100.00", 1)],
            [rec("R1", "60.00", 1), rec("R2", "39.98", 3)],
            Tolerance(amount_abs=D("0.05"), date_days=3),
        )
    )

    assert match.rule is RuleId.M3_AGGREGATE
    assert match.right_ids == ("R1", "R2")
    assert match.amount_residual == D("0.02")
    assert match.date_gap_days == 2
    assert match.score == D("0.536667")  # 0.6 - 0.02 - 0.033333 - 0.01


def test_reference_match_reports_its_date_gap():
    match = only_match(
        reconcile(
            [rec("L1", "100.00", 1, reference="TX-9")],
            [rec("R1", "100.00", 3, reference="TX-9")],
            Tolerance(),
        )
    )

    assert match.rule is RuleId.M0_REFERENCE
    assert match.date_gap_days == 2


@pytest.mark.parametrize(
    "lefts,rights,tol,rule",
    [
        # Date gap exactly at the window: M1, not left to M2.
        (
            [rec("L1", "100.00", 1)],
            [rec("R1", "100.00", 4)],
            Tolerance(date_days=3),
            RuleId.M1_EXACT,
        ),
        # Residual exactly at the tolerance.
        (
            [rec("L1", "100.00", 1)],
            [rec("R1", "100.05", 1)],
            Tolerance(amount_abs=D("0.05")),
            RuleId.M2_TOLERANT,
        ),
        # Aggregated record exactly at the window.
        (
            [rec("L1", "100.00", 1)],
            [rec("R1", "60.00", 4), rec("R2", "40.00", 1)],
            Tolerance(date_days=3),
            RuleId.M3_AGGREGATE,
        ),
    ],
    ids=["date-window", "amount-tolerance", "aggregate-window"],
)
def test_declared_bounds_are_inclusive(lefts, rights, tol, rule):
    assert only_match(reconcile(lefts, rights, tol)).rule is rule


def test_aggregate_size_two_is_allowed():
    match = only_match(
        reconcile(
            [rec("L1", "100.00", 1)],
            [rec("R1", "60.00", 1), rec("R2", "40.00", 1)],
            Tolerance(max_aggregate_size=2),
        )
    )

    assert match.rule is RuleId.M3_AGGREGATE


@pytest.mark.parametrize(
    "lefts,rights,tol,expected",
    [
        # A left without reference does not stop M0 for the next one.
        (
            [rec("L1", "5.00", 1), rec("L2", "100.00", 1, reference="TX-9")],
            [rec("R1", "99.00", 1, reference="TX-9")],
            Tolerance(),
            {"L2": (RuleId.M0_REFERENCE, ("R1",))},
        ),
        # A right with another amount does not stop M1 for the next right.
        (
            [rec("L1", "100.00", 1)],
            [rec("R1", "7.00", 1), rec("R2", "100.00", 1)],
            Tolerance(),
            {"L1": (RuleId.M1_EXACT, ("R2",))},
        ),
        # Nor does a right outside the date window.
        (
            [rec("L1", "100.00", 1)],
            [rec("R1", "100.00", 9), rec("R2", "100.00", 1)],
            Tolerance(date_days=3),
            {"L1": (RuleId.M1_EXACT, ("R2",))},
        ),
        # A left with too few records in its window does not stop M3.
        (
            [rec("L1", "50.00", 20), rec("L2", "100.00", 1)],
            [rec("R1", "60.00", 1), rec("R2", "40.00", 1), rec("R3", "7.00", 20)],
            Tolerance(date_days=3),
            {"L2": (RuleId.M3_AGGREGATE, ("R1", "R2"))},
        ),
        # A combination off by too much does not stop M3 for the next one.
        (
            [rec("L1", "100.00", 1)],
            [rec("R1", "10.00", 1), rec("R2", "60.00", 1), rec("R3", "40.00", 1)],
            Tolerance(),
            {"L1": (RuleId.M3_AGGREGATE, ("R2", "R3"))},
        ),
    ],
    ids=["m0-left", "m1-amount", "m1-date", "m3-window", "m3-combination"],
)
def test_a_rejected_candidate_does_not_end_the_search(lefts, rights, tol, expected):
    result = reconcile(lefts, rights, tol)

    assert {m.left_id: (m.rule, m.right_ids) for m in result.matches} == expected


def test_relative_tolerance_widens_the_absolute_one():
    """Limit = amount_abs + |amount| * amount_rel: 0.05 + 1000 * 0.01."""
    tol = Tolerance(amount_abs=D("0.05"), amount_rel=D("0.01"))

    match = only_match(reconcile([rec("L1", "1000.00", 1)], [rec("R1", "1010.05", 1)], tol))
    assert match.rule is RuleId.M2_TOLERANT
    assert not reconcile([rec("L1", "1000.00", 1)], [rec("R1", "1010.06", 1)], tol).matches


@pytest.mark.parametrize(
    "first",
    [rec("R1", "200.00", 1), rec("R1", "100.01", 9)],
    ids=["amount", "date"],
)
def test_a_rejected_candidate_does_not_end_the_tolerant_search(first):
    result = reconcile(
        [rec("L1", "100.00", 1)],
        [first, rec("R2", "100.01", 1)],
        Tolerance(amount_abs=D("0.05"), date_days=3, max_aggregate_size=1),
    )

    assert {m.left_id: (m.rule, m.right_ids) for m in result.matches} == {
        "L1": (RuleId.M2_TOLERANT, ("R2",))
    }


def test_aggregate_never_exceeds_its_declared_size():
    rights = [rec("R1", "30.00", 1), rec("R2", "30.00", 1), rec("R3", "40.00", 1)]

    assert not reconcile([rec("L1", "100.00", 1)], rights, Tolerance(max_aggregate_size=2)).matches
    assert only_match(reconcile([rec("L1", "100.00", 1)], rights, Tolerance())).right_ids == (
        "R1",
        "R2",
        "R3",
    )


def test_zero_amount_is_never_matched_to_an_empty_combination():
    result = reconcile(
        [rec("L1", "0.00", 1)], [rec("R1", "60.00", 1), rec("R2", "40.00", 1)], Tolerance()
    )

    assert not result.matches
    assert result.counters["unmatched_left"] == 1
