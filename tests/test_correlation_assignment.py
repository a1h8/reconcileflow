"""Assignment core: one decision per record, checked against an exhaustive oracle.

The acceptance criteria (C1-C6) and the seeded cases were fixed in the private
phase 0 spec before the code existed; they are not to be tuned to a result.
"""

import ast
import hashlib
import random
from pathlib import Path

import pytest

import oracle_assignment as oracle
from reconcileflow.correlation import MAX_WEIGHT, Edge, Status, assign

SEEDS = range(3000)
MARGINS = (0, 1, 3)
DENSITIES = (0.2, 0.5, 1.0)
WEIGHT_RANGES = ((1, 3), (1, 10**6))


def generated(seed):
    """Seeded case: 1-7 records per side, density and weight range set by the seed."""
    rng = random.Random(seed)
    lefts = [f"L{i}" for i in range(rng.randint(1, 7))]
    rights = [f"R{i}" for i in range(rng.randint(1, 7))]
    density = DENSITIES[seed % 3]
    low, high = WEIGHT_RANGES[(seed // 3) % 2]
    edges = {
        (left, right): rng.randint(low, high)
        for left in lefts
        for right in rights
        if rng.random() < density
    }
    return lefts, rights, edges


def as_edges(edges):
    return [Edge(left, right, weight) for (left, right), weight in edges.items()]


def view(decisions):
    return {d.id: (d.status.name, d.partner) for d in decisions}


def fingerprint(result):
    return hashlib.sha256(repr(result).encode()).hexdigest()


def test_matches_the_exhaustive_oracle():
    """C1 best value, C2 every status on both sides, C5 totality and reciprocity.

    Asserts per case, so a defect stops the loop at its first seed (mutation
    runs stay short).
    """
    for seed in SEEDS:
        lefts, rights, edges = generated(seed)
        best, expected = oracle.statuses(lefts, rights, edges, MARGINS)
        for margin in MARGINS:
            case = (seed, margin)
            result = assign(lefts, rights, as_edges(edges), margin=margin)
            assert sum(c.best for c in result.components) == best, case
            assert (view(result.left), view(result.right)) == expected[margin], case
            assert [d.id for d in result.left] == sorted(lefts), case
            assert [d.id for d in result.right] == sorted(rights), case
            partner_of_right = {d.id: d.partner for d in result.right}
            for d in result.left:
                if d.status is Status.MATCHED:
                    assert partner_of_right[d.partner] == d.id, case


def test_same_bytes_under_any_input_order():
    """C3: five shuffles of lefts, rights and edges per seeded case."""
    for seed in SEEDS:
        lefts, rights, edges = generated(seed)
        edge_list = as_edges(edges)
        reference = fingerprint(assign(lefts, rights, edge_list, margin=1))
        rng = random.Random(10_000 + seed)
        for _ in range(5):
            lefts, rights, edge_list = lefts[:], rights[:], edge_list[:]
            rng.shuffle(lefts)
            rng.shuffle(rights)
            rng.shuffle(edge_list)
            assert fingerprint(assign(lefts, rights, edge_list, margin=1)) == reference, seed


def test_oracle_imports_nothing_from_the_code_under_test():
    """C4: a shared helper would move the oracle along with a defect."""
    tree = ast.parse((Path(__file__).parent / "oracle_assignment.py").read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported <= {"__future__", "collections", "itertools"}


def test_a_lone_record_with_an_equally_good_partner_is_ambiguous():
    # The prototype tested only the pairs it kept: L0 came out neither matched
    # nor ambiguous, and fell out of every count.
    result = assign(["L0", "L1"], ["R0"], [Edge("L0", "R0", 5), Edge("L1", "R0", 5)])
    assert view(result.left) == {"L0": ("AMBIGUOUS", None), "L1": ("AMBIGUOUS", None)}
    assert view(result.right) == {"R0": ("AMBIGUOUS", None)}


def test_a_lone_record_whose_only_partner_is_better_taken_is_unmatched():
    result = assign(["L0", "L1"], ["R0"], [Edge("L0", "R0", 9), Edge("L1", "R0", 5)])
    assert view(result.left) == {"L0": ("MATCHED", "R0"), "L1": ("UNMATCHED", None)}
    # Within a margin of 4 the other choice is as good: the data do not decide.
    tolerant = assign(["L0", "L1"], ["R0"], [Edge("L0", "R0", 9), Edge("L1", "R0", 5)], margin=4)
    assert {s for s, _ in view(tolerant.left).values()} == {"AMBIGUOUS"}


def test_a_lone_right_is_decided_on_its_own():
    # L0 is better with R0 (weight 9 against 5 with R1), so R1 stays alone in
    # every near-optimal assignment up to a margin of 3, ambiguous from 4.
    edges = [Edge("L0", "R0", 9), Edge("L0", "R1", 5)]
    assert view(assign(["L0"], ["R0", "R1"], edges, margin=3).right) == {
        "R0": ("MATCHED", "L0"),
        "R1": ("UNMATCHED", None),
    }
    assert view(assign(["L0"], ["R0", "R1"], edges, margin=4).right) == {
        "R0": ("AMBIGUOUS", None),
        "R1": ("AMBIGUOUS", None),
    }


def test_records_without_an_edge_have_no_candidate_and_no_component():
    result = assign(["L0", "L1"], ["R0", "R1"], [Edge("L0", "R0", 1)])
    assert view(result.left)["L1"] == ("NO_CANDIDATE", None)
    assert view(result.right)["R1"] == ("NO_CANDIDATE", None)
    assert [d.component for d in result.left] == [0, None]
    assert len(result.components) == 1


def test_an_oversized_component_is_reported_whole_not_resolved():
    lefts = [f"L{i}" for i in range(4)]
    edges = [Edge(left, "R0", 1) for left in lefts] + [Edge("X", "R9", 2)]
    result = assign([*lefts, "X"], ["R0", "R9"], edges, max_component=3)
    assert {d.id: d.status for d in result.left} == {
        **{left: Status.OVERSIZED for left in lefts},
        "X": Status.MATCHED,
    }
    assert view(result.right)["R0"] == ("OVERSIZED", None)
    assert [c.best for c in result.components] == [None, 2]


def test_component_size_bound_is_inclusive_and_per_side():
    lefts = [f"L{i}" for i in range(3)]
    edges = [Edge(left, "R0", 1) for left in lefts]
    statuses = {d.status for d in assign(lefts, ["R0"], edges, max_component=3).left}
    assert Status.OVERSIZED not in statuses


def test_components_are_ordered_and_labelled_by_their_smallest_record():
    edges = [Edge("L5", "R5", 1), Edge("L1", "R9", 1), Edge("L3", "R0", 1)]
    result = assign(["L1", "L3", "L5"], ["R0", "R5", "R9"], edges)
    assert [c.left for c in result.components] == [("L1",), ("L3",), ("L5",)]
    assert [c.right for c in result.components] == [("R9",), ("R0",), ("R5",)]
    assert {d.id: d.component for d in result.right} == {"R9": 0, "R0": 1, "R5": 2}


def test_largest_weights_stay_exact():
    # Weights at the cap on a complete 7 x 7 graph: a non-edge must never look
    # as good as a real edge, whatever the sum of real weights.
    rng = random.Random(0)
    lefts = [f"L{i}" for i in range(7)]
    rights = [f"R{i}" for i in range(7)]
    edges = {(left, right): MAX_WEIGHT - rng.randint(0, 2) for left in lefts for right in rights}
    best, expected = oracle.statuses(lefts, rights, edges, (0, 2))
    for margin in (0, 2):
        result = assign(lefts, rights, as_edges(edges), margin=margin)
        assert sum(c.best for c in result.components) == best
        assert (view(result.left), view(result.right)) == expected[margin]


@pytest.mark.parametrize(
    ("lefts", "rights", "edges", "options", "reason"),
    [
        (["L0", "L0"], ["R0"], [], {}, "duplicate left id"),
        (["L0"], ["R0", "R0"], [], {}, "duplicate right id"),
        (["L0"], ["R0"], [Edge("L9", "R0", 1)], {}, "unknown left id"),
        (["L0"], ["R0"], [Edge("L0", "R9", 1)], {}, "unknown right id"),
        (["L0"], ["R0"], [Edge("L0", "R0", 1), Edge("L0", "R0", 2)], {}, "duplicate edge"),
        (["L0"], ["R0"], [Edge("L0", "R0", 0)], {}, "weight must be"),
        (["L0"], ["R0"], [Edge("L0", "R0", MAX_WEIGHT + 1)], {}, "weight must be"),
        (["L0"], ["R0"], [Edge("L0", "R0", 1.0)], {}, "weight must be"),
        (["L0"], ["R0"], [Edge("L0", "R0", True)], {}, "weight must be"),
        (["L0"], ["R0"], [], {"margin": -1}, "margin must be"),
        (["L0"], ["R0"], [], {"margin": 0.5}, "margin must be"),
        (["L0"], ["R0"], [], {"max_component": 0}, "max_component must be"),
    ],
)
def test_invalid_input_is_refused(lefts, rights, edges, options, reason):
    """C6: one case per refusal, the reason checked."""
    with pytest.raises(ValueError, match=reason):
        assign(lefts, rights, edges, **options)


def test_weight_cap_is_inclusive():
    result = assign(["L0"], ["R0"], [Edge("L0", "R0", MAX_WEIGHT)])
    assert result.components[0].best == MAX_WEIGHT


def test_a_component_of_one_record_per_side_is_solved_at_the_smallest_bound():
    result = assign(["L0"], ["R0"], [Edge("L0", "R0", 1)], max_component=1)
    assert view(result.left) == {"L0": ("MATCHED", "R0")}


def test_oversized_records_point_to_their_component():
    edges = [Edge("L0", "R0", 1), Edge("L1", "R0", 1), Edge("X", "R9", 1)]
    result = assign(["L0", "L1", "X"], ["R0", "R9"], edges, max_component=1)
    assert {d.id: d.component for d in result.left} == {"L0": 0, "L1": 0, "X": 1}
    assert {d.id: d.component for d in result.right} == {"R0": 0, "R9": 1}


def test_default_margin_decides_a_near_tie():
    # Optimum 10 against 9 for the swap: with the default margin (0) only exact
    # ties are ambiguous.
    edges = [Edge("L0", "R0", 5), Edge("L1", "R1", 5), Edge("L0", "R1", 4), Edge("L1", "R0", 5)]
    result = assign(["L0", "L1"], ["R0", "R1"], edges)
    assert view(result.left) == {"L0": ("MATCHED", "R0"), "L1": ("MATCHED", "R1")}


def test_default_component_bound_is_32_per_side():
    def star(n):
        lefts = [f"L{i:02}" for i in range(n)]
        return assign(lefts, ["R0"], [Edge(left, "R0", 1) for left in lefts])

    assert star(32).left[0].status is Status.AMBIGUOUS
    assert star(33).left[0].status is Status.OVERSIZED
