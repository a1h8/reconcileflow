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
    assert CFG.ruleset_id() == "82a31d4469f76094"
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
    # A1 has no candidate, then matches R1 on their reference, nine days apart;
    # A0 carries the same reference and wins the tie by identifier, so A1 has
    # no candidate again (the pair is outside the window): versions 1 and 3
    # share their content. A left and a right record, alone in their blocks,
    # share the id X.
    config = StreamConfig(Tolerance(date_days=3), reference_horizon_days=15)
    journal = [
        left("A1", "100.00", 1, "TX-X"),
        right("R1", "100.00", 10, "TX-X"),
        left("A0", "100.00", 1, "TX-X"),
        Arrival(LEFT, Record("X", "P", D("1.00"), day(11))),
        Arrival(RIGHT, Record("X", "Q", D("1.00"), day(11))),
        *done(40),
    ]
    result = replay(journal, config)
    reasons = [d.reason for d in chain(result, LEFT, "A1")]
    assert reasons[:3] == ["NO_CANDIDATE", "M0_REFERENCE", "NO_CANDIDATE"]
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
    # A0 and A1 both match R2 exactly; A0 wins the tie by identifier. R3 is
    # outside every window: it gives no reason to anyone (J3 spec §8.3, B).
    journal = [
        left("A0", "100.00", 11),
        left("A1", "100.00", 11),
        right("R1", "250.00", 11),
        right("R2", "100.00", 11),
        right("R3", "100.00", 20),
    ]
    now = state(replay(journal, CFG))
    assert now[(LEFT, "A1")][2] == "AMOUNT_OUT_OF_TOLERANCE+LOST_TO_BETTER_CANDIDATE"
    assert now[(RIGHT, "R1")][2] == "AMOUNT_OUT_OF_TOLERANCE"
    assert now[(RIGHT, "R3")][2] == "NO_CANDIDATE"


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


# -- amendment (J3 spec §8, 2026-10-30): the order must not change finals -----
#
# T3 never closed a horizon before the last arrival. These close horizons in
# the middle of the journal, with every arrival still on time.


def finals(result):
    """Final effective state by content, without identifiers or clocks."""
    return {
        key: (d.decision_type, d.input_ids, d.reason)
        for key, d in result.effective().items()
        if d.status is FINAL
    }


def assert_same_finals_without_late(first, second, config):
    a, b = replay([*first, *done(60)], config), replay([*second, *done(60)], config)
    assert all(d.reason != "LATE" for d in (*a.decisions, *b.decisions))
    assert finals(a) == finals(b)


def test_t13_a_reference_counterpart_on_time_is_found_whatever_the_order():
    config = StreamConfig(Tolerance(date_days=3), reference_horizon_days=10)
    a1, a2 = left("A1", "100.00", 1, "TX-X"), left("A2", "500.00", 10)
    r9, r1 = right("R9", "700.00", 10), right("R1", "100.00", 10, "TX-X")
    assert_same_finals_without_late([a1, r1, a2, r9], [a1, a2, r9, r1], config)


def test_t14_a_far_record_does_not_change_the_reason_of_a_final_break():
    x, y = left("X", "100.00", 1), right("Y", "999.00", 10)
    assert_same_finals_without_late(
        [x, y, Progress(LEFT, day(5)), Progress(RIGHT, day(5))],
        [x, Progress(LEFT, day(5)), Progress(RIGHT, day(5)), y],
        CFG,
    )


def test_t18_a_later_arrival_cannot_cascade_into_a_final_match():
    # L0 takes Z from L1 (M1), which then wants R; R must not be final already.
    config = StreamConfig(Tolerance(amount_abs=D("0.50"), date_days=3))
    l2, r = left("L2", "100.45", 1), right("R", "100.20", 3)
    l1, z, l0 = left("L1", "100.00", 3), right("Z", "100.00", 5), left("L0", "100.00", 7)
    seven = [Progress(LEFT, day(7)), Progress(RIGHT, day(7))]
    assert_same_finals_without_late([l2, r, l1, z, l0, *seven], [l2, r, l1, z, *seven, l0], config)


LATENESS = 3


def random_records(rng):
    amounts = ["100.00", "100.20", "100.45", "200.00", "300.00"]
    entries = []
    for i in range(6):
        ref = f"TX-{i}" if rng.random() < 0.3 else None
        entries.append(left(f"A{i}", rng.choice(amounts), rng.randint(1, 15), ref))
        entries.append(right(f"R{i}", rng.choice(amounts), rng.randint(1, 15), ref))
    return entries


def interleaved_journal(records, rng):
    """One arrival order, out of order by less than the lateness, with progress
    signals at the latest date seen: no arrival is late, and horizons more than
    two windows behind close in the middle of the journal."""
    jitter = {(e.side, e.record.id): rng.random() * LATENESS for e in records}

    def arrival(e):
        return e.record.value_date.toordinal() + jitter[(e.side, e.record.id)]

    journal, latest = [], None
    for entry in sorted(records, key=arrival):
        journal.append(entry)
        latest = max(latest or entry.record.value_date, entry.record.value_date)
        if rng.random() < 0.4:
            journal += [Progress(LEFT, latest), Progress(RIGHT, latest)]
    return journal


def test_t15_permutations_with_horizons_closing_mid_journal_end_in_the_same_finals():
    config = StreamConfig(
        Tolerance(amount_abs=D("0.50"), date_days=3),
        lateness_days=LATENESS,
        reference_horizon_days=15,
    )
    rng = random.Random(0)
    compared = 0
    for _ in range(300):
        records = random_records(rng)
        a, b = (replay([*interleaved_journal(records, rng), *done(60)], config) for _ in range(2))
        if any(d.reason == "LATE" for d in (*a.decisions, *b.decisions)):
            continue
        compared += 1
        assert finals(a) == finals(b)
    assert compared >= 100  # not vacuous


def test_t16_a_reference_arriving_after_its_counterpart_is_final_is_a_typed_break():
    # A1 alone closes at day 4 + 0 (H = window); R1 shares its reference, is on
    # time (day 10), but A1 is gone: it is told, not left to search in silence.
    journal = [left("A1", "100.00", 1, "TX-X"), *done(10), right("R1", "100.00", 10, "TX-X")]
    (r1,) = chain(replay(journal, CFG), RIGHT, "R1")
    assert (r1.decision_type, r1.reason, r1.status) == (BREAK, "REFERENCE_AFTER_FINAL", FINAL)
    assert r1.window_end == day(13)
    # A reference of the same side, or another reference, is not concerned.
    journal = [left("A1", "100.00", 1, "TX-X"), *done(10), left("A2", "100.00", 10, "TX-X")]
    assert chain(replay(journal, CFG), LEFT, "A2")[0].reason == "NO_CANDIDATE"
    journal = [left("A1", "100.00", 1, "TX-X"), *done(10), right("R1", "100.00", 10, "TX-Y")]
    assert chain(replay(journal, CFG), RIGHT, "R1")[0].reason == "NO_CANDIDATE"


@pytest.mark.parametrize("days", [2, -1, True, 3.5])
def test_t17_a_reference_horizon_shorter_than_the_window_is_refused(days):
    with pytest.raises(ValueError, match="reference_horizon_days must be an integer >= 3"):
        replay([], StreamConfig(Tolerance(date_days=3), reference_horizon_days=days))


@pytest.mark.parametrize("days", [-1, True, 2.5])
def test_a_component_bound_that_is_not_an_integer_ge_0_is_refused(days):
    with pytest.raises(ValueError, match="max_component_days must be an integer >= 0"):
        replay([], StreamConfig(max_component_days=days))


def test_t17_the_stream_settings_change_the_ruleset_id():
    ids = {
        StreamConfig(Tolerance(date_days=3)).ruleset_id(),
        StreamConfig(Tolerance(date_days=3), reference_horizon_days=5).ruleset_id(),
        StreamConfig(Tolerance(date_days=3), max_component_days=5).ruleset_id(),
    }
    assert len(ids) == 3


def test_a_reference_waits_for_its_own_horizon():
    config = StreamConfig(Tolerance(date_days=3), reference_horizon_days=10)
    journal = [left("A1", "100.00", 1, "TX-X")]
    assert chain(replay([*journal, *done(11)], config), LEFT, "A1")[-1].status is PROVISIONAL
    final = chain(replay([*journal, *done(12)], config), LEFT, "A1")[-1]
    assert (final.status, final.window_end) == (FINAL, day(11))


def dense_chain(n):
    """One record a day on each side, never matching: a single component."""
    return [
        entry
        for i in range(1, n + 1)
        for entry in (left(f"A{i:02}", "100.00", i), right(f"R{i:02}", "500.00", i))
    ]


def test_a_dense_component_is_never_final_without_a_bound():
    result = replay([*dense_chain(20), Progress(LEFT, day(20)), Progress(RIGHT, day(20))], CFG)
    assert all(d.status is PROVISIONAL for d in result.effective().values())


def test_a_component_past_its_bound_is_cut_and_its_old_decisions_are_forced():
    config = StreamConfig(Tolerance(date_days=3), max_component_days=10)
    result = replay([*dense_chain(20), Progress(LEFT, day(20)), Progress(RIGHT, day(20))], config)
    now = result.effective()
    a01, a19 = now[(LEFT, "A01")], now[(LEFT, "A19")]
    assert (a01.status, a01.forced) == (FINAL, True)
    assert (a19.status, a19.forced) == (PROVISIONAL, False)
    # A forced final is not the same decision as a closed one with the same content.
    closed = replay([left("A01", "100.00", 1), *done(20)], CFG).effective()[(LEFT, "A01")]
    assert closed.forced is False and closed.decision_id != a01.decision_id


def test_a_closed_component_is_not_forced_even_when_a_bound_is_set():
    config = StreamConfig(Tolerance(date_days=3), max_component_days=10)
    a1 = replay([left("A1", "100.00", 1), *done(20)], config).effective()[(LEFT, "A1")]
    assert (a1.status, a1.forced) == (FINAL, False)


# -- mutation testing of the component rule ------------------------------------


def dense_span(first, last):
    """One record a day on each side, from day first to day last, never matching."""
    return [
        entry
        for i in range(first, last + 1)
        for entry in (left(f"A{i:02}", "100.00", i), right(f"R{i:02}", "500.00", i))
    ]


def test_a_cut_forces_only_decisions_whose_own_horizon_has_passed():
    # A lateness of 20 holds the watermark back while the records arrive, so the
    # cut happens once, at day 40 - 20: A16's horizon is day 19, A17's is day 20.
    config = StreamConfig(Tolerance(date_days=3), lateness_days=20, max_component_days=10)
    now = replay([*dense_span(1, 20), *done(40)], config).effective()
    assert (now[(LEFT, "A16")].status, now[(LEFT, "A16")].forced) == (FINAL, True)
    assert now[(LEFT, "A17")].status is PROVISIONAL


def test_a_component_spanning_exactly_its_bound_is_not_cut():
    journal = [*dense_span(1, 11), *done(11)]  # dates span 10 days
    at_bound = replay(journal, StreamConfig(Tolerance(date_days=3), max_component_days=10))
    assert all(d.status is PROVISIONAL for d in at_bound.effective().values())
    past = replay(journal, StreamConfig(Tolerance(date_days=3), max_component_days=9))
    assert past.effective()[(LEFT, "A01")].forced is True


def test_a_forced_final_has_its_own_identifier():
    config = StreamConfig(Tolerance(date_days=3), max_component_days=10)
    forced = replay([*dense_span(1, 20), *done(20)], config).effective()[(LEFT, "A01")]
    # The same content, final because its component closed.
    alone = [left("A01", "100.00", 1), right("R01", "500.00", 1), right("R02", "500.00", 2)]
    closed = replay([*alone, *done(20)], CFG).effective()[(LEFT, "A01")]
    assert (closed.reason, closed.input_ids) == (forced.reason, forced.input_ids)
    assert closed.forced is False and closed.decision_id != forced.decision_id
    assert forced.decision_id == "e0b7b136bfd23e74"


def test_components_closing_together_are_finalised_in_component_order():
    # Two components that close on the same step: a lateness of 3 keeps both
    # open until the right signal moves the watermark to day 17. The first one
    # first.
    journal = [
        left("A1", "100.00", 1),
        right("R1", "100.00", 1),
        left("A2", "100.00", 5),
        right("R2", "100.00", 5),
        *done(20),
    ]
    result = replay(journal, StreamConfig(Tolerance(date_days=3), lateness_days=3))
    finals_in_order = [d.business_key for d in result.decisions if d.status is FINAL]
    assert finals_in_order == [(LEFT, "A1"), (RIGHT, "R1"), (LEFT, "A2"), (RIGHT, "R2")]
    assert len({d.final_at for d in result.decisions if d.status is FINAL}) == 1
    # A component that closes and one that is cut on the same step. A lateness
    # of 41 holds the watermark at day 9 until the left signal moves it to 19:
    # past A00's horizon (9), and past A10-A15's in the A10-A50 component, which
    # stays open (horizon 53) but spans 40 days.
    config = StreamConfig(Tolerance(date_days=3), lateness_days=41, max_component_days=10)
    journal = [
        left("A00", "100.00", 6),
        *dense_span(10, 50),
        Progress(RIGHT, day(60)),
        Progress(LEFT, day(60)),
    ]
    result = replay(journal, config)
    last = len(journal) - 1
    on_last_step = [d for d in result.decisions if d.status is FINAL and d.final_at == last]
    assert on_last_step[0].business_key == (LEFT, "A00") and not on_last_step[0].forced
    assert all(d.forced for d in on_last_step[1:]) and len(on_last_step) > 1


def test_different_references_do_not_link_records():
    config = StreamConfig(Tolerance(date_days=3), reference_horizon_days=5)
    journal = [left("A1", "100.00", 1, "TX-X"), right("R1", "100.00", 20, "TX-Y"), *done(10)]
    assert chain(replay(journal, config), LEFT, "A1")[-1].status is FINAL


def test_a_record_without_reference_keeps_the_date_window_as_horizon():
    config = StreamConfig(Tolerance(date_days=3), reference_horizon_days=10)
    a1 = chain(replay([left("A1", "100.00", 1), *done(5)], config), LEFT, "A1")[-1]
    assert (a1.status, a1.window_end) == (FINAL, day(4))


def test_the_counterpart_side_of_a_match_has_the_match_s_horizon():
    result = replay([left("A1", "100.00", 11), right("R1", "100.00", 12)], CFG)
    assert chain(result, RIGHT, "R1")[-1].window_end == day(15)


def test_a_pair_exactly_at_the_window_still_gives_its_reason():
    result = replay([left("A1", "100.00", 1), right("R1", "250.00", 4)], CFG)
    assert state(result)[(LEFT, "A1")][2] == "AMOUNT_OUT_OF_TOLERANCE"


def test_a_reason_without_a_pair_is_kept():
    journal = [left("A1", "300.00", 1), right("R1", "100.00", 1), right("R2", "150.00", 1)]
    reason = state(replay(journal, CFG))[(LEFT, "A1")][2]
    assert reason == "AMOUNT_OUT_OF_TOLERANCE+AGGREGATE_NOT_FOUND"
