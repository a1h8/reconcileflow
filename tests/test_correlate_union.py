"""Union-find correlator: components from the candidate graph, exact resolution."""

import importlib.util
import random
import sys
from decimal import Decimal
from pathlib import Path

from reconcileflow.models import Tolerance

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
spec = importlib.util.spec_from_file_location("correlate_union", HARNESS / "correlate_union.py")
cu = importlib.util.module_from_spec(spec)
sys.modules["correlate_union"] = cu
spec.loader.exec_module(cu)

TOL = Tolerance(amount_abs=Decimal(4), date_days=0, max_aggregate_size=1)


def L(i, t, d):
    return cu.Event(f"L{i}", t, d)


def R(i, t, d):
    return cu.Event(f"R{i}", t, d)


def pairs(result):
    return {(m.left_id, m.right_ids[0]) for m in result.matches}


def test_pair_straddling_a_bucket_boundary_is_kept():
    # A bucket of 2 by rank would put L1 (rank 1) and R1 (rank 2) in different
    # buckets. A component has no boundary to straddle.
    lefts = [L(0, 0, 100), L(1, 1, 500)]
    rights = [R(0, 0, 100), R(9, 1, 900), R(1, 2, 500)]
    result = cu.correlate(lefts, rights, window=2, tol=TOL)
    assert pairs(result) == {("L0", "R0"), ("L1", "R1")}


def test_components_are_split_by_duration_not_only_by_time():
    # All events fall inside one time window; durations far apart keep them separate.
    lefts = [L(i, i, 1000 * i) for i in range(5)]
    rights = [R(i, i, 1000 * i + 1) for i in range(5)]
    result = cu.correlate(lefts, rights, window=100, tol=TOL)
    assert result.component_sizes == [2] * 5
    assert len(pairs(result)) == 5


def test_union_is_transitive():
    # L0-R0 and L1-R0 and L1-R1 chain into one component of 4 records.
    edges = cu.candidate_edges([L(0, 0, 100), L(1, 0, 104)], [R(0, 0, 102), R(1, 0, 107)], 1, TOL)
    assert [
        len({e.left_id for e in c} | {e.right_ids[0] for e in c}) for c in cu.components(edges)
    ] == [4]


def test_exact_assignment_beats_greedy():
    # Greedy takes the single best edge L1-R0 (|diff|=1) and strands L0.
    # The optimum matches both.
    lefts = [L(0, 0, 100), L(1, 0, 104)]
    rights = [R(0, 0, 103), R(1, 0, 107)]
    result = cu.correlate(lefts, rights, window=1, tol=TOL)
    assert pairs(result) == {("L0", "R0"), ("L1", "R1")}
    assert not result.ambiguous


def test_indistinguishable_swap_is_ambiguous_not_guessed():
    lefts = [L(0, 0, 100), L(1, 0, 100)]
    rights = [R(0, 0, 100), R(1, 0, 100)]
    result = cu.correlate(lefts, rights, window=1, tol=TOL)
    assert result.matches == []
    assert result.ambiguous == {"L0", "L1"}


def test_oversized_component_is_reported_not_resolved():
    lefts = [L(i, 0, 100) for i in range(4)]
    rights = [R(i, 0, 100) for i in range(4)]
    result = cu.correlate(lefts, rights, window=1, tol=TOL, max_component=3)
    assert result.matches == []
    assert result.oversized == {"L0", "L1", "L2", "L3"}


def test_result_does_not_depend_on_input_order():
    rng = random.Random(7)
    lefts = [L(i, i, rng.randrange(0, 200)) for i in range(40)]
    rights = [
        R(i, i + rng.randrange(-2, 3), lefts[i].duration_ns + rng.randrange(-3, 4))
        for i in range(40)
    ]
    reference = cu.correlate(lefts, rights, window=3, tol=TOL)
    for _ in range(5):
        rng.shuffle(lefts)
        rng.shuffle(rights)
        again = cu.correlate(lefts, rights, window=3, tol=TOL)
        assert pairs(again) == pairs(reference)
        assert again.ambiguous == reference.ambiguous


def test_evaluate_keeps_every_left_in_the_denominator():
    lefts = [L(0, 0, 100), L(1, 0, 100), L(2, 0, 5000)]  # L2 has no candidate at all
    rights = [R(0, 0, 100), R(1, 0, 100)]
    result = cu.correlate(lefts, rights, window=1, tol=TOL)
    m = cu.evaluate(result, {"L0": "R0", "L1": "R1", "L2": "R2"}, total_left=3)
    assert m["R"] == 2 / 3  # true right is a candidate for L0 and L1
    assert m["T"] == 0.0  # both ambiguous, none resolved
    assert (m["ambiguous"], m["no_candidate"], m["wrong"]) == (2, 1, 0)


def test_near_tie_is_ambiguous_only_with_a_margin():
    # L0-R0 + L1-R1 (diffs 0 and 1) beats the crossed assignment (diffs 2 and 1) by
    # 0.025 in score: without a margin it is picked as a fact, with 0.03 it is not.
    lefts = [L(0, 0, 100), L(1, 0, 101)]
    rights = [R(0, 0, 100), R(1, 0, 102)]
    strict = cu.correlate(lefts, rights, window=1, tol=TOL)
    assert pairs(strict) == {("L0", "R0"), ("L1", "R1")}
    tolerant = cu.correlate(lefts, rights, window=1, tol=TOL, margin=Decimal("0.03"))
    assert tolerant.matches == []
    assert tolerant.ambiguous == {"L0", "L1"}
