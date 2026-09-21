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
    """The bundled payment: M3 matches one on the left against n on the right."""
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
