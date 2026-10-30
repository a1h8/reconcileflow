"""Stream semantics (J3): one business intent per test, T1-T11 of the J3 spec.

Each test pins a clause of the done criterion (README, "J3 — Streaming", as
made exact in the spec): replay is byte-identical, permutations within the
tolerated lateness end in the same final decisions, a late counterpart
supersedes a provisional break without deleting it, and beyond the lateness an
arrival is a typed LATE break that never touches a final decision.
"""

import random
from datetime import date, timedelta
from decimal import Decimal

import pytest

from reconcileflow import Record, Tolerance, reconcile
from reconcileflow.stream import (
    LEFT,
    RIGHT,
    Arrival,
    DecisionType,
    Progress,
    Status,
    StreamConfig,
    replay,
)

D = Decimal
MATCH, BREAK = DecisionType.MATCH, DecisionType.BREAK
PROVISIONAL, FINAL, SUPERSEDED = Status.PROVISIONAL, Status.FINAL, Status.SUPERSEDED
CFG = StreamConfig(Tolerance(date_days=3))


def day(n):
    return date(2024, 3, 1) + timedelta(days=n - 1)


def rec(id_, amount, n, reference=None):
    return Record(id_, "ACC", D(amount), day(n), reference)


def left(id_, amount, n, reference=None):
    return Arrival(LEFT, rec(id_, amount, n, reference))


def right(id_, amount, n, reference=None):
    return Arrival(RIGHT, rec(id_, amount, n, reference))


def done(n):
    """Both sources complete up to day n: every horizon before it closes."""
    return [Progress(LEFT, day(n)), Progress(RIGHT, day(n))]


def chain(result, side, id_):
    return [d for d in result.decisions if d.business_key == (side, id_)]


def state(result, upto=None):
    """Effective state, by content: what was decided, not when."""
    return {
        key: (d.decision_type, d.input_ids, d.reason, d.status, d.decision_id)
        for key, d in result.effective(upto).items()
    }


def assert_one_match_per_right_record(result, upto):
    """Spec §3.6: a right record is in at most one effective match, counted on
    its left key; its own chain points to that same match."""
    effective = result.effective(upto)
    owners = {}
    for (side, _), d in effective.items():
        if side == LEFT and d.decision_type is MATCH:
            for right_id in d.input_ids[1:]:
                assert right_id not in owners, f"{right_id} matched twice at {upto}"
                owners[right_id] = d.input_ids
    for right_id, input_ids in owners.items():
        mine = effective[(RIGHT, right_id)]
        assert (mine.decision_type, mine.reason, mine.input_ids) == (
            MATCH,
            "AS_COUNTERPART",
            input_ids,
        )


# -- T1, T2: a replay is byte-identical, duplicates included ------------------


def dispute_journal():
    return [
        left("A1", "100.00", 11),
        right("R1", "100.00", 11, reference="TX-7"),
        left("A2", "100.00", 12, reference="TX-7"),
        right("R2", "250.00", 12),
        *done(30),
    ]


def test_t1_the_same_journal_replays_to_the_same_decisions():
    first, second = replay(dispute_journal(), CFG), replay(dispute_journal(), CFG)
    assert first.decisions == second.decisions
    assert first.decisions  # not vacuous


@pytest.mark.parametrize("seed", range(20))
def test_t2_duplicates_anywhere_after_the_original_change_nothing(seed):
    journal = dispute_journal()
    base = replay(journal, CFG)
    rng = random.Random(seed)
    with_duplicates = list(journal)
    copies = 0
    for i, entry in enumerate(journal):
        if isinstance(entry, Arrival) and rng.random() < 0.7:
            position = with_duplicates.index(entry) + 1 + rng.randint(0, len(journal) - i)
            with_duplicates.insert(min(position, len(with_duplicates)), entry)
            copies += 1
    result = replay(with_duplicates, CFG)
    assert result.decisions == base.decisions
    assert result.duplicates == copies


# -- T3: a different order within the lateness ends in the same final state ---


def random_arrivals(rng):
    amounts = ["100.00", "200.00", "300.00"]
    entries = []
    for i in range(6):
        ref = f"TX-{i}" if rng.random() < 0.3 else None
        entries.append(left(f"A{i}", rng.choice(amounts), rng.randint(1, 8), ref))
        entries.append(right(f"R{i}", rng.choice(amounts), rng.randint(1, 8), ref))
    return entries


def test_t3_permutations_within_the_lateness_end_in_the_same_final_decisions():
    # A lateness of 30 days over value dates spread on 8: no arrival is ever
    # late, so any order is within the bound; the progress signals stay last.
    config = StreamConfig(Tolerance(date_days=3), lateness_days=30)
    rng = random.Random(0)
    for _ in range(200):
        arrivals = random_arrivals(rng)
        reference = replay([*arrivals, *done(60)], config)
        expected = state(reference)
        assert {s for (_, _, _, s, _) in expected.values()} == {FINAL}
        shuffled = list(arrivals)
        rng.shuffle(shuffled)
        assert state(replay([*shuffled, *done(60)], config)) == expected


# -- T4: a late counterpart supersedes a provisional break, deletes nothing ---


def test_t4_a_counterpart_within_the_horizon_supersedes_the_break():
    result = replay([left("A1", "100.00", 11), right("R1", "100.00", 12), *done(20)], CFG)
    brk, match, final = chain(result, LEFT, "A1")
    assert (brk.decision_type, brk.status, brk.decided_at) == (BREAK, PROVISIONAL, 0)
    assert (match.decision_type, match.status, match.decided_at) == (MATCH, PROVISIONAL, 1)
    assert (final.decision_type, final.status) == (MATCH, FINAL)
    assert match.supersedes == brk.decision_id and final.supersedes == match.decision_id
    assert match.reason == "M1_EXACT" and match.input_ids == ("A1", "R1")
    # Nothing deleted: the break is still in the history, superseded by derivation.
    assert brk in result.decisions
    assert result.status(brk) is SUPERSEDED and result.status(final) is FINAL
    # The three clocks: business horizon, decision position, finalisation position.
    # A match's horizon is its latest input's: R1 (day 12) + 3 days. Until then a
    # stricter rule could still claim R1 (T6), so the match cannot be final before.
    assert final.window_end == day(15)
    # The left signal (position 2) leaves the right side at day 12; the right one
    # (position 3) moves the watermark to day 20 and closes the horizon.
    assert final.final_at == 3


# -- T5: M0 is provisional until its horizon (Q3) ----------------------------


def test_t5_a_reference_match_is_provisional_until_its_horizon():
    journal = [left("A1", "100.00", 11, "TX-9"), right("R1", "99.00", 12, "TX-9")]
    before = replay(journal, CFG)
    after = replay([*journal, *done(20)], CFG)
    assert state(before)[(LEFT, "A1")][:4] == (MATCH, ("A1", "R1"), "M0_REFERENCE", PROVISIONAL)
    assert state(after)[(LEFT, "A1")][:4] == (MATCH, ("A1", "R1"), "M0_REFERENCE", FINAL)


# -- T6: a dispute moves a right record from one key to another ---------------


def test_t6_a_stricter_rule_takes_a_right_record_from_another_key():
    result = replay(dispute_journal(), CFG)
    assert state(result, upto=1)[(LEFT, "A1")][:3] == (MATCH, ("A1", "R1"), "M1_EXACT")
    after = state(result, upto=2)
    assert after[(LEFT, "A2")][:3] == (MATCH, ("A2", "R1"), "M0_REFERENCE")
    assert after[(LEFT, "A1")][0] is BREAK
    for position in range(len(dispute_journal())):
        assert_one_match_per_right_record(result, position)


# -- T7: beyond the lateness, a typed LATE break; finals untouched ------------


def test_t7_an_arrival_beyond_the_lateness_is_late_and_final_decisions_stay():
    journal = [
        left("A1", "100.00", 11),
        right("R9", "50.00", 20),
        Progress(LEFT, day(20)),  # both sides now at day 20: A1's horizon (14) closes
        right("R1", "100.00", 12),  # A1's counterpart, under the watermark
    ]
    result = replay(journal, CFG)
    a1 = chain(result, LEFT, "A1")
    assert (a1[-1].decision_type, a1[-1].status, a1[-1].final_at) == (BREAK, FINAL, 2)
    assert all(d.decided_at <= 2 for d in a1)  # nothing after the late arrival
    (late,) = chain(result, RIGHT, "R1")
    assert (late.decision_type, late.reason, late.status) == (BREAK, "LATE", FINAL)
    # The price of finality, shown: matching everything at once would have paired them.
    batch = reconcile(
        [rec("A1", "100.00", 11)], [rec("R9", "50.00", 20), rec("R1", "100.00", 12)], CFG.tolerance
    )
    assert [(m.left_id, m.right_ids) for m in batch.matches] == [("A1", ("R1",))]


# -- T8: one record, two contents --------------------------------------------


def test_t8_the_same_record_with_another_content_is_refused():
    with pytest.raises(ValueError, match="different content"):
        replay([left("A1", "100.00", 11), left("A1", "200.00", 11)], CFG)


# -- T9: the slower source holds the watermark --------------------------------


def test_t9_a_fast_source_does_not_close_a_slow_source_s_horizons():
    journal = [
        right("R9", "50.00", 20),  # the right source is already at day 20
        left("A1", "100.00", 11),  # the left source is at day 11: not late
        right("R1", "100.00", 12),  # A1's counterpart, inside its horizon
    ]
    result = replay(journal, CFG)
    assert state(result)[(LEFT, "A1")][:2] == (MATCH, ("A1", "R1"))
    assert all(d.reason != "LATE" for d in result.decisions)


# -- T10: a silent source, and its progress signal (Q6) -----------------------


def test_t10_a_silent_side_holds_horizons_open_until_it_signals_progress():
    journal = [left("A1", "100.00", 11)]
    assert state(replay(journal, CFG))[(LEFT, "A1")][3] is PROVISIONAL
    one_side = [*journal, Progress(RIGHT, day(20))]
    assert state(replay(one_side, CFG))[(LEFT, "A1")][3] is PROVISIONAL  # left still at 11
    both = [*journal, *done(20)]
    first = replay(both, CFG)
    assert state(first)[(LEFT, "A1")][:4] == (BREAK, ("A1",), "NO_CANDIDATE", FINAL)
    assert replay(both, CFG).decisions == first.decisions


# -- T11: the right side has its own decisions (Q4) ---------------------------


def test_t11_a_right_record_alone_has_a_break_final_at_its_horizon():
    result = replay([right("R5", "100.00", 11), *done(20)], CFG)
    brk, final = chain(result, RIGHT, "R5")
    assert (brk.decision_type, brk.status) == (BREAK, PROVISIONAL)
    assert (final.decision_type, final.status, final.window_end) == (BREAK, FINAL, day(14))


def test_t11_a_right_break_is_superseded_when_its_counterpart_arrives():
    result = replay([right("R5", "100.00", 11), left("A5", "100.00", 12)], CFG)
    brk, counterpart = chain(result, RIGHT, "R5")
    assert brk.decision_type is BREAK
    assert (counterpart.decision_type, counterpart.reason, counterpart.input_ids) == (
        MATCH,
        "AS_COUNTERPART",
        ("A5", "R5"),
    )
    assert result.status(brk) is SUPERSEDED
    assert_one_match_per_right_record(result, 1)


# -- the lateness is part of the ruleset (Q5) ---------------------------------


def test_the_lateness_changes_the_ruleset_id_carried_by_every_decision():
    strict, lenient = (
        StreamConfig(Tolerance(date_days=3)),
        StreamConfig(Tolerance(date_days=3), lateness_days=1),
    )
    assert strict.ruleset_id() != lenient.ruleset_id()
    result = replay([left("A1", "100.00", 11)], strict)
    assert {d.ruleset_id for d in result.decisions} == {strict.ruleset_id()}


# -- T12: the watermark never recedes ------------------------------------------


def test_t12_an_older_arrival_within_the_lateness_does_not_move_the_watermark_back():
    # Lateness 5: both sides at day 15 put the watermark at day 10. An older left
    # record (day 12) is within the lateness and joins; the watermark stays at 10,
    # so a right record of day 8 is late. Taking the last arrival's date instead of
    # the latest would move the watermark back to day 7 and let it in.
    config = StreamConfig(Tolerance(date_days=3), lateness_days=5)
    journal = [
        left("A1", "100.00", 15),
        right("R1", "100.00", 15),
        left("A2", "200.00", 12),
        right("R2", "300.00", 8),
    ]
    result = replay(journal, config)
    assert [d.reason for d in chain(result, LEFT, "A2")] != ["LATE"]
    (late,) = chain(result, RIGHT, "R2")
    assert (late.reason, late.status) == ("LATE", FINAL)


# -- mutation testing: what T1-T12 left unpinned ------------------------------


@pytest.mark.parametrize("lateness", [-1, True, 1.5, "2"])
def test_a_lateness_that_is_not_an_integer_ge_0_is_refused(lateness):
    with pytest.raises(ValueError, match="lateness_days must be an integer >= 0"):
        replay([], StreamConfig(lateness_days=lateness))


def test_a_side_other_than_left_or_right_is_refused():
    record = rec("A1", "100.00", 11)
    with pytest.raises(ValueError, match="side must be 'left' or 'right', got 'middle'"):
        replay([Arrival("middle", record)], CFG)
    with pytest.raises(ValueError, match="side must be"):
        replay([Progress("middle", day(11))], CFG)


def test_an_entry_that_is_neither_an_arrival_nor_a_progress_is_refused():
    with pytest.raises(ValueError, match="unknown journal entry"):
        replay([rec("A1", "100.00", 11)], CFG)


def test_the_ruleset_id_depends_on_the_tolerance_too():
    assert StreamConfig(Tolerance(date_days=3)).ruleset_id() != (
        StreamConfig(Tolerance(date_days=4)).ruleset_id()
    )


def test_identifiers_are_the_same_bytes_in_every_process():
    # T1 replays twice in one process; this pins the bytes, so a change to the
    # fingerprints shows up as a change to every stored decision.
    result = replay([left("A1", "100.00", 11), right("R1", "100.00", 12), *done(20)], CFG)
    assert CFG.ruleset_id() == "a4a217fc0021c77c"
    assert [d.decision_id for d in result.decisions] == [
        "02fa57c650f8b328",
        "24de4ccf54a08eca",
        "4246a8b827201f51",
        "4f2d65c41bbe8113",
        "083171247a62fdba",
    ]


def test_every_decision_carries_the_installed_engine_version():
    from importlib.metadata import version

    result = replay([left("A1", "100.00", 11)], CFG)
    assert {d.engine_version for d in result.decisions} == {version("reconcileflow")}


def test_the_engine_version_is_unknown_outside_an_installed_package(monkeypatch):
    from importlib.metadata import PackageNotFoundError

    from reconcileflow import stream

    def missing(name):
        raise PackageNotFoundError(name)

    monkeypatch.setattr(stream, "version", missing)
    assert stream._engine_version() == "unknown"


def test_chain_versions_count_from_one_and_only_finals_have_a_final_clock():
    result = replay([left("A1", "100.00", 11), right("R1", "100.00", 12), *done(20)], CFG)
    a1 = chain(result, LEFT, "A1")
    assert [d.chain_version for d in a1] == [1, 2, 3]
    assert [d.final_at for d in a1] == [None, None, 3]


def test_decision_ids_are_unique_even_when_a_content_comes_back():
    # A1 has no candidate, then R1 out of tolerance; R1 closes at day 15 and
    # leaves the block, so A1 has no candidate again: versions 1 and 3 share
    # their content. A left and a right record, alone in their blocks, share X.
    journal = [
        left("A1", "100.00", 13),
        right("R1", "700.00", 11),
        Arrival(LEFT, Record("X", "P", D("1.00"), day(11))),
        Arrival(RIGHT, Record("X", "Q", D("1.00"), day(11))),
        *done(15),
    ]
    result = replay(journal, CFG)
    reasons = [d.reason for d in chain(result, LEFT, "A1")]
    assert reasons == ["NO_CANDIDATE", "AMOUNT_OUT_OF_TOLERANCE", "NO_CANDIDATE"]
    assert state(result)[(LEFT, "X")][2] == state(result)[(RIGHT, "X")][2] == "NO_CANDIDATE"
    ids = [d.decision_id for d in result.decisions]
    assert len(ids) == len(set(ids))


def test_an_unrelated_arrival_does_not_reissue_unchanged_decisions():
    result = replay([left("A1", "100.00", 11), right("R9", "700.00", 11)], CFG)
    assert len(chain(result, LEFT, "A1")) == 2  # NO_CANDIDATE, then the amount reason
    result = replay(
        [left("A1", "100.00", 11), right("R9", "700.00", 11), left("A2", "900.00", 11)], CFG
    )
    assert len(chain(result, LEFT, "A1")) == 2
    assert len(chain(result, RIGHT, "R9")) == 1
    # A match has two inputs or more: its fingerprint joins them.
    result = replay(
        [left("A1", "100.00", 11), right("R1", "100.00", 11), left("A2", "900.00", 11)], CFG
    )
    assert [d.decision_type for d in chain(result, LEFT, "A1")] == [BREAK, MATCH]
    assert len(chain(result, RIGHT, "R1")) == 1


def test_a_break_carries_every_distinct_reason_on_each_side():
    result = replay(
        [left("A1", "100.00", 11), right("R1", "250.00", 11), right("R2", "100.00", 20)], CFG
    )
    now = state(result)
    assert now[(LEFT, "A1")][2] == "DATE_OUT_OF_WINDOW+AMOUNT_OUT_OF_TOLERANCE"
    assert now[(RIGHT, "R1")][2] == "AMOUNT_OUT_OF_TOLERANCE"
    assert now[(RIGHT, "R2")][2] == "DATE_OUT_OF_WINDOW"


def test_a_late_break_has_the_late_record_s_horizon():
    journal = [left("A1", "100.00", 11), right("R9", "50.00", 20), Progress(LEFT, day(20))]
    (late,) = chain(replay([*journal, right("R1", "100.00", 12)], CFG), RIGHT, "R1")
    assert late.window_end == day(15)


def test_an_arrival_exactly_at_the_watermark_is_not_late():
    # Both sides at day 20, lateness 0: the watermark is day 20.
    journal = [left("A9", "50.00", 20), right("R9", "60.00", 20), right("R1", "100.00", 20)]
    assert chain(replay(journal, CFG), RIGHT, "R1")[0].reason != "LATE"


def test_a_horizon_exactly_at_the_watermark_is_still_open():
    # A1's horizon is day 14: a watermark of day 14 does not close it, day 15 does.
    journal = [left("A1", "100.00", 11)]
    assert chain(replay([*journal, *done(14)], CFG), LEFT, "A1")[-1].status is PROVISIONAL
    assert chain(replay([*journal, *done(15)], CFG), LEFT, "A1")[-1].status is FINAL


def test_each_block_has_its_own_watermark():
    # Account B is at day 20 on both sides; account ACC has not moved.
    other = [
        Arrival(LEFT, Record("B1", "B", D("50.00"), day(20))),
        Arrival(RIGHT, Record("B2", "B", D("60.00"), day(20))),
    ]
    result = replay([*other, left("A1", "100.00", 11), right("R1", "100.00", 12)], CFG)
    assert state(result)[(LEFT, "A1")][:2] == (MATCH, ("A1", "R1"))


def test_an_older_progress_signal_does_not_move_a_side_back():
    journal = [left("A1", "100.00", 11), *done(20), Progress(LEFT, day(5))]
    result = replay([*journal, right("R1", "100.00", 12)], CFG)
    (late,) = chain(result, RIGHT, "R1")
    assert late.reason == "LATE"
