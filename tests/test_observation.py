"""Observation (J4): one business intent per test, the done criterion of the J4
spec (docs/target, private) made exact: replay gives the same series and
alerts; a drop of one source's M0 rate is detected and the others are not;
stable sources raise no drop; a thin day is INSUFFICIENT, never NORMAL; the
evidence itself is watched (silent source, capture failures, undecided rise).
"""

import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from reconcileflow import Record, Tolerance
from reconcileflow.observation import (
    AlertKind,
    CaptureFailure,
    Count,
    DriftRule,
    EvidenceRule,
    Verdict,
    match_rate,
    observe,
    series,
    wilson,
)
from reconcileflow.stream import LEFT, RIGHT, Arrival, Progress, StreamConfig, replay

D = Decimal
CFG = StreamConfig(Tolerance(date_days=3))


def day(n):
    return date(2026, 1, 1) + timedelta(days=n - 1)


def at(n, hour=12):
    return datetime(2026, 1, 1, hour, tzinfo=UTC) + timedelta(days=n - 1)


def pair(source, n, i, with_reference):
    """A left record from ``source`` and its counterpart from the book, alone in
    their block; matched by M0 when they share a reference, else by M1."""
    account = f"{source}-{n}-{i}"
    ref = f"TX-{account}" if with_reference else None
    lrec = Record(f"L-{account}", account, D("100.00"), day(n), ref)
    rrec = Record(f"R-{account}", account, D("100.00"), day(n), f"TX-{account}")
    return [
        Arrival(LEFT, lrec, source=source, captured_at=at(n)),
        Arrival(RIGHT, rrec, source="BOOK", captured_at=at(n)),
    ]


def done(n):
    return [Progress(LEFT, day(n)), Progress(RIGHT, day(n))]


def daily(source, days, per_day, m0_share):
    entries = []
    for n in days:
        with_reference = round(per_day * m0_share(n))
        for i in range(per_day):
            entries += pair(source, n, i, i < with_reference)
    return entries


# End to end, the default rule's 200 decisions a day would mean 80,000 arrivals
# per replay, replayed again for every mutant. The same rule with a lower floor
# keeps the journal at 25 pairs a day; the default floor is pinned on counts.
SMALL = DriftRule(min_decisions=20)


def drop_journal():
    """60 days, 3 sources, 25 pairs a day each; P2's M0 share falls from 88 %
    to 60 % on day 45 (its references stop arriving)."""
    days = range(1, 61)
    return [
        *daily("P1", days, 25, lambda n: 0.9),
        *daily("P2", days, 25, lambda n: 0.9 if n < 45 else 0.6),
        *daily("P3", days, 25, lambda n: 0.9),
        *done(70),
    ]


@pytest.fixture(scope="module")
def dropped():
    journal = drop_journal()
    return journal, observe(journal, replay(journal, CFG), drift=SMALL)


# -- 1. replay: the same journal, the same series, verdicts and alerts ---------


def test_1_the_same_journal_observes_to_the_same_bytes(dropped):
    journal, first = dropped
    again = observe(journal, replay(journal, CFG), drift=SMALL)
    assert again == first
    assert repr(again.alerts) == repr(first.alerts) and first.alerts  # not vacuous


# -- 2. detection: one source drops, it alone is flagged ------------------------


def test_2_a_drop_of_one_source_is_detected_and_the_others_are_not(dropped):
    _, observed = dropped
    drops = [a for a in observed.alerts if a.kind is AlertKind.MATCH_RATE_DROP and a.side == LEFT]
    assert {a.source for a in drops} == {"P2"}
    assert min(a.day for a in drops) == day(45)
    assert all(a.rule_id == SMALL.rule_id() for a in drops)


def test_2_the_verdict_before_the_drop_is_normal(dropped):
    _, observed = dropped
    p2 = {v.day: v for v in observed.verdicts if v.source == "P2" and v.side == LEFT}
    assert p2[day(44)].verdict is Verdict.NORMAL
    assert p2[day(45)].verdict is Verdict.DROP
    assert (p2[day(45)].matched, p2[day(45)].decided) == (15, 25)
    assert (p2[day(45)].reference_matched, p2[day(45)].reference_decided) == (14 * 22, 14 * 25)


# -- 3. no false drop on stable sources ----------------------------------------


def test_3_stable_sources_raise_no_drop():
    rng = random.Random(0)
    counts = []
    for source in ("P1", "P2", "P3"):
        for n in range(1, 1001):
            matched = sum(rng.random() < 0.9 for _ in range(300))
            counts += [
                Count(LEFT, source, day(n), "M0_REFERENCE", False, matched),
                Count(LEFT, source, day(n), "M1_EXACT", False, 300 - matched),
            ]
    verdicts = [
        v
        for s in ("P1", "P2", "P3")
        for v in match_rate(counts, LEFT, "M0_REFERENCE", DriftRule())
        if v.source == s
    ]
    judged = [v for v in verdicts if v.verdict in (Verdict.NORMAL, Verdict.DROP)]
    assert len(judged) >= 2900
    assert sum(v.verdict is Verdict.DROP for v in judged) == 0


# -- 4. a thin day is insufficient, never normal --------------------------------


def test_4_a_day_with_too_few_decisions_is_insufficient():
    counts = [Count(LEFT, "P1", day(n), "M0_REFERENCE", False, 198) for n in range(1, 15)]
    counts += [Count(LEFT, "P1", day(n), "M1_EXACT", False, 22) for n in range(1, 15)]
    counts += [Count(LEFT, "P1", day(15), "M0_REFERENCE", False, 10)]  # 10 decisions: thin
    (last,) = [v for v in match_rate(counts, LEFT, "M0_REFERENCE", DriftRule()) if v.day == day(15)]
    assert last.verdict is Verdict.INSUFFICIENT and last.upper is None


def test_4_a_day_without_enough_reference_has_no_reference():
    counts = [Count(LEFT, "P1", day(1), "M0_REFERENCE", False, 250)]
    (first,) = match_rate(counts, LEFT, "M0_REFERENCE", DriftRule())
    assert first.verdict is Verdict.NO_REFERENCE


def test_4_only_final_decisions_are_counted():
    journal = [*pair("P1", 1, 0, True)]  # never final: no progress
    assert series(journal, replay(journal, CFG)) == ()
    journal += done(10)
    (left, right) = sorted(series(journal, replay(journal, CFG)), key=lambda c: c.side)
    assert (left.side, left.source, left.day, left.outcome, left.count) == (
        LEFT,
        "P1",
        day(1),
        "M0_REFERENCE",
        1,
    )
    # A counterpart is counted under its match's rule, on its own source.
    assert (right.side, right.source, right.outcome) == (RIGHT, "BOOK", "M0_REFERENCE")


# -- 5. the evidence itself is watched ------------------------------------------


def test_5_a_source_that_stops_is_silent():
    journal = [
        *daily("P1", range(1, 21), 5, lambda n: 1.0),
        *daily("P3", range(1, 16), 5, lambda n: 1.0),  # P3 stops after day 15
        *done(30),
    ]
    alerts = observe(journal, replay(journal, CFG)).alerts
    silent = [a for a in alerts if a.kind is AlertKind.SOURCE_SILENT]
    assert [(a.side, a.source) for a in silent] == [(LEFT, "P3")]
    assert silent[0].day == day(20)


def test_5_capture_failures_above_their_bound_raise_an_alert():
    journal = [*daily("P1", [1], 220, lambda n: 1.0), *done(10)]
    many = [CaptureFailure(LEFT, "P1", at(1)) for _ in range(10)]
    alerts = observe(journal, replay(journal, CFG), failures=many).alerts
    assert [a.kind for a in alerts] == [AlertKind.CAPTURE_FAILURE]
    one = [CaptureFailure(LEFT, "P1", at(1))]
    assert observe(journal, replay(journal, CFG), failures=one).alerts == ()


def test_5_a_rise_of_late_records_raises_an_alert():
    journal = [*daily("P1", range(1, 21), 25, lambda n: 1.0), *done(25)]
    # Captured on day 21, but dated day 10: under the watermark, so LATE.
    for i in range(25):
        lrec = Record(f"L-late-{i}", f"late-{i}", D("100.00"), day(10))
        journal.append(Arrival(LEFT, lrec, source="P1", captured_at=at(21)))
    alerts = observe(journal, replay(journal, CFG), drift=SMALL).alerts
    jumps = [a for a in alerts if a.kind is AlertKind.UNDECIDED_JUMP]
    assert [(a.side, a.source, a.day) for a in jumps] == [(LEFT, "P1", day(21))]


# -- the rule and its inputs -----------------------------------------------------


def test_wilson_bounds_a_known_case():
    low, high = wilson(90, 100, D("1.96"))
    assert (round(low, 4), round(high, 4)) == (D("0.8256"), D("0.9448"))
    assert wilson(0, 10, D("1.96"))[0] == 0


def test_the_drift_rule_id_changes_with_every_setting():
    rules = [
        DriftRule(),
        DriftRule(reference_days=7),
        DriftRule(min_decisions=100),
        DriftRule(margin=D("0.10")),
        DriftRule(z=D("2.58")),
    ]
    assert len({r.rule_id() for r in rules}) == len(rules)


@pytest.mark.parametrize(
    "rule",
    [
        DriftRule(reference_days=0),
        DriftRule(min_decisions=0),
        DriftRule(margin=D("-0.01")),
        DriftRule(z=D("0")),
    ],
)
def test_a_drift_rule_out_of_range_is_refused(rule):
    with pytest.raises(ValueError):
        match_rate([], LEFT, "M0_REFERENCE", rule)


def test_an_arrival_without_a_capture_time_cannot_be_observed():
    lrec = Record("L1", "A", D("1.00"), day(1))
    journal = [Arrival(LEFT, lrec, source="P1"), *done(10)]
    with pytest.raises(ValueError, match="captured_at"):
        observe(journal, replay(journal, CFG))


def test_a_capture_time_without_a_timezone_is_refused():
    lrec = Record("L1", "A", D("1.00"), day(1))
    journal = [Arrival(LEFT, lrec, source="P1", captured_at=datetime(2026, 1, 1)), *done(10)]
    with pytest.raises(ValueError, match="timezone"):
        observe(journal, replay(journal, CFG))


def test_the_evidence_rule_is_part_of_the_alerts():
    journal = [*daily("P1", [1], 220, lambda n: 1.0), *done(10)]
    failures = [CaptureFailure(LEFT, "P1", at(1)) for _ in range(10)]
    strict = observe(journal, replay(journal, CFG), failures=failures)
    lenient = observe(
        journal,
        replay(journal, CFG),
        failures=failures,
        evidence=EvidenceRule(max_capture_failure=D("0.10")),
    )
    assert strict.alerts and not lenient.alerts


@pytest.mark.parametrize(
    "rule",
    [
        EvidenceRule(silent_days=0),
        EvidenceRule(silent_days=True),
        EvidenceRule(max_capture_failure=D("-0.01")),
        EvidenceRule(max_capture_failure=D("1.01")),
    ],
)
def test_an_evidence_rule_out_of_range_is_refused(rule):
    with pytest.raises(ValueError, match=r"silent_days|max_capture_failure"):
        observe([], replay([], CFG), evidence=rule)


@pytest.mark.parametrize("successes, trials", [(-1, 10), (11, 10), (0, 0)])
def test_wilson_refuses_an_impossible_count(successes, trials):
    with pytest.raises(ValueError, match="successes <= trials"):
        wilson(successes, trials, D("1.96"))


def test_a_duplicate_arrival_keeps_its_first_capture():
    journal = pair("P1", 1, 0, True)
    late_copy = Arrival(LEFT, journal[0].record, source="P1", captured_at=at(5))
    counts = series([*journal, late_copy, *done(10)], replay([*journal, late_copy, *done(10)], CFG))
    assert {c.day for c in counts} == {day(1)}


def test_an_empty_journal_observes_to_nothing():
    assert observe([], replay([], CFG)) == observe([], replay([], CFG))
    assert observe([], replay([], CFG)).alerts == ()


def test_capture_failures_on_a_thin_day_are_not_judged():
    journal = [*daily("P1", [1], 5, lambda n: 1.0), *done(10)]
    failures = [CaptureFailure(LEFT, "P1", at(1)) for _ in range(5)]  # half failed, 10 in all
    assert observe(journal, replay(journal, CFG), failures=failures).alerts == ()


# -- mutation testing: what the tests above left unpinned ----------------------

from reconcileflow import observation  # noqa: E402
from reconcileflow.observation import Alert, DayVerdict  # noqa: E402


def test_alerts_are_pinned_whole():
    # Computed here, not in the shared fixture: mutation testing runs, for each
    # function, only the tests it saw call it, and a module fixture is called once.
    journal = drop_journal()
    observed = observe(journal, replay(journal, CFG), drift=SMALL)
    first = next(a for a in observed.alerts if a.side == LEFT and a.source == "P2")
    assert first == Alert(
        AlertKind.MATCH_RATE_DROP,
        LEFT,
        "P2",
        day(45),
        "M0_REFERENCE 15/25, upper bound 0.7660 < reference lower 0.8418 - margin 0.05",
        "7596760383b62ee8",
    )
    assert observed.counts == series(journal, replay(journal, CFG))
    assert observed.verdicts[0].side == LEFT and len(observed.verdicts) > 60
    p2 = next(v for v in observed.verdicts if (v.side, v.source, v.day) == (LEFT, "P2", day(45)))
    assert p2 == DayVerdict(
        LEFT,
        "P2",
        "M0_REFERENCE",
        day(45),
        Verdict.DROP,
        15,
        25,
        14 * 22,
        14 * 25,
        wilson(15, 25, D("1.96"))[1],
        wilson(14 * 22, 14 * 25, D("1.96"))[0],
    )


def test_rule_fingerprints_are_pinned():
    assert DriftRule().rule_id() == "63df2949b68d7ac9"
    assert EvidenceRule().rule_id() == "2c90e3f12ebc7676"
    assert (
        len(
            {
                EvidenceRule().rule_id(),
                EvidenceRule(silent_days=3).rule_id(),
                EvidenceRule(max_capture_failure=D("0.02")).rule_id(),
            }
        )
        == 3
    )


def test_evidence_alerts_are_pinned_whole():
    journal = [*daily("P1", [1], 220, lambda n: 1.0), *done(10)]
    failures = [CaptureFailure(LEFT, "P1", at(1)) for _ in range(10)]
    assert observe(journal, replay(journal, CFG), failures=failures).alerts == (
        Alert(
            AlertKind.CAPTURE_FAILURE,
            LEFT,
            "P1",
            day(1),
            "10/230 failed, lower bound 0.0238 > 0.01",
            "2c90e3f12ebc7676",
        ),
    )
    journal = [
        *daily("P1", range(1, 21), 5, lambda n: 1.0),
        *daily("P3", range(1, 16), 5, lambda n: 1.0),
        *done(30),
    ]
    silent = [
        a
        for a in observe(journal, replay(journal, CFG)).alerts
        if a.kind is AlertKind.SOURCE_SILENT
    ]
    assert silent == [
        Alert(
            AlertKind.SOURCE_SILENT,
            LEFT,
            "P3",
            day(20),
            "last capture 2026-01-15, 5 days before 2026-01-20",
            "2c90e3f12ebc7676",
        )
    ]


def test_the_undecided_alert_is_pinned_whole():
    journal = [*daily("P1", range(1, 21), 25, lambda n: 1.0), *done(25)]
    for i in range(25):
        lrec = Record(f"L-late-{i}", f"late-{i}", D("100.00"), day(10))
        journal.append(Arrival(LEFT, lrec, source="P1", captured_at=at(21)))
    jumps = [
        a
        for a in observe(journal, replay(journal, CFG), drift=SMALL).alerts
        if a.kind is AlertKind.UNDECIDED_JUMP
    ]
    assert jumps == [
        Alert(
            AlertKind.UNDECIDED_JUMP,
            LEFT,
            "P1",
            day(21),
            "25/25 undecided, lower bound 0.8668 > reference upper 0.0109 + margin 0.05",
            "7596760383b62ee8",
        )
    ]


def test_a_reference_counts_only_the_days_it_has():
    counts = [
        Count(LEFT, "P1", day(1), "M0_REFERENCE", False, 250),
        Count(LEFT, "P1", day(10), "M0_REFERENCE", False, 250),
    ]
    last = match_rate(counts, LEFT, "M0_REFERENCE", DriftRule())[-1]
    assert (last.reference_matched, last.reference_decided) == (250, 250)
    # Day 15's reference is days 1-14 (day 1 in); day 16's is days 2-15 (day 1 out,
    # day 15 in).
    counts = [
        Count(LEFT, "P1", day(1), "M0_REFERENCE", False, 250),
        Count(LEFT, "P1", day(15), "M0_REFERENCE", False, 250),
        Count(LEFT, "P1", day(16), "M0_REFERENCE", False, 250),
    ]
    v15, v16 = match_rate(counts, LEFT, "M0_REFERENCE", DriftRule())[1:]
    assert (v15.reference_decided, v16.reference_decided) == (250, 250)


def two_days(today_matched, today, ref_matched, ref_total):
    return [
        Count(LEFT, "P1", day(1), "M0_REFERENCE", False, ref_matched),
        Count(LEFT, "P1", day(1), "M1_EXACT", False, ref_total - ref_matched),
        Count(LEFT, "P1", day(2), "M0_REFERENCE", False, today_matched),
        Count(LEFT, "P1", day(2), "M1_EXACT", False, today - today_matched),
    ]


def test_exactly_the_minimum_of_decisions_is_judged():
    rule = DriftRule(min_decisions=200)
    v = match_rate(two_days(180, 200, 180, 200), LEFT, "M0_REFERENCE", rule)[-1]
    assert v.verdict is Verdict.NORMAL


def test_a_drop_exactly_at_the_margin_is_not_a_drop():
    upper = wilson(120, 200, D("1.96"))[1]
    reference_lower = wilson(180, 200, D("1.96"))[0]
    rule = DriftRule(min_decisions=200, margin=reference_lower - upper)
    v = match_rate(two_days(120, 200, 180, 200), LEFT, "M0_REFERENCE", rule)[-1]
    assert v.verdict is Verdict.NORMAL
    assert (
        match_rate(two_days(119, 200, 180, 200), LEFT, "M0_REFERENCE", rule)[-1].verdict
        is Verdict.DROP
    )


def undecided_days(today, total, ref, ref_total, outcome="LATE"):
    return [
        Count(LEFT, "P1", day(1), outcome, False, ref),
        Count(LEFT, "P1", day(1), "M0_REFERENCE", False, ref_total - ref),
        Count(LEFT, "P1", day(2), outcome, False, today),
        Count(LEFT, "P1", day(2), "M0_REFERENCE", False, total - today),
    ]


def test_undecided_boundaries():
    rule = DriftRule(min_decisions=200)
    # Exactly the minimum on both days is judged.
    assert observation._undecided(undecided_days(100, 200, 0, 200), rule)
    # A joined reason counts when one of its parts is undecided.
    joined = undecided_days(100, 200, 0, 200, "AMOUNT_OUT_OF_TOLERANCE+BLOCK_TOO_LARGE")
    assert observation._undecided(joined, rule)
    # Forced decisions count as undecided.
    forced = [
        Count(LEFT, "P1", day(1), "M0_REFERENCE", False, 200),
        Count(LEFT, "P1", day(2), "M0_REFERENCE", True, 200),
    ]
    assert observation._undecided(forced, rule)
    # Exactly at the margin is not a jump.
    lower = wilson(100, 200, D("1.96"))[0]
    reference_upper = wilson(0, 200, D("1.96"))[1]
    at_margin = DriftRule(min_decisions=200, margin=lower - reference_upper)
    assert observation._undecided(undecided_days(100, 200, 0, 200), at_margin) == []


def test_capture_failure_boundaries():
    journal = [
        *daily("P1", [1], 190, lambda n: 1.0),
        *daily("P1", [2], 220, lambda n: 1.0),
        *done(10),
    ]
    result = replay(journal, CFG)
    # Day 1 is thin (200 in all with 10 failures: exactly the minimum, judged);
    # day 2 is judged too: a thin day must not stop the others.
    failures = [CaptureFailure(LEFT, "P1", at(1)) for _ in range(10)]
    failures += [CaptureFailure(LEFT, "P1", at(2)) for _ in range(10)]
    alerts = observe(journal, result, failures=failures).alerts
    assert [(a.day, a.detail.split(" ")[0]) for a in alerts] == [
        (day(1), "10/200"),
        (day(2), "10/230"),
    ]
    # Exactly at the bound is not above it.
    lower = wilson(10, 230, D("1.96"))[0]
    at_bound = EvidenceRule(max_capture_failure=lower)
    day2 = [f for f in failures if f.captured_at == at(2)]
    assert observe(journal, result, failures=day2, evidence=at_bound).alerts == ()


def test_a_thin_day_does_not_stop_the_judgement_of_later_days():
    journal = [
        *daily("P1", [1], 5, lambda n: 1.0),
        *daily("P1", [2], 220, lambda n: 1.0),
        *done(10),
    ]
    failures = [CaptureFailure(LEFT, "P1", at(1)) for _ in range(5)]
    failures += [CaptureFailure(LEFT, "P1", at(2)) for _ in range(10)]
    alerts = observe(journal, replay(journal, CFG), failures=failures).alerts
    assert [a.day for a in alerts] == [day(2)]


def test_a_silence_exactly_at_the_bound_is_not_an_alert():
    journal = [
        *daily("P1", range(1, 6), 2, lambda n: 1.0),
        *daily("P3", range(1, 4), 2, lambda n: 1.0),
        *done(30),
    ]
    assert observe(journal, replay(journal, CFG)).alerts == ()  # P3: 2 days, bound 2


def test_series_counts_finals_after_a_provisional_one():
    # B1 (day 20) opens a block that never closes; A's pair closes at day 10.
    journal = [
        Arrival(LEFT, Record("B1", "B", D("1.00"), day(20)), source="P1", captured_at=at(1)),
        *pair("P1", 1, 0, True),
        *done(10),
    ]
    counts = series(journal, replay(journal, CFG))
    assert sum(c.count for c in counts) == 2


def test_wilson_bounds_stay_in_the_unit_interval():
    assert wilson(12, 12, D("1.96"))[1] == 1  # unclamped, the bound is above 1
    assert wilson(0, 22, D("1.96"))[0] == 0  # unclamped, it is below 0
    assert wilson(1, 1, D("1.96"))[1] == 1  # a single trial is a valid count


def test_wilson_does_not_depend_on_the_caller_s_decimal_context():
    from decimal import ROUND_DOWN, localcontext

    expected = wilson(90, 100, D("1.96"))
    with localcontext() as ctx:
        ctx.prec, ctx.rounding = 6, ROUND_DOWN
        assert wilson(90, 100, D("1.96")) == expected


def test_capture_days_are_utc_dates_whatever_the_machine_s_zone(monkeypatch):
    import time
    from datetime import timezone

    monkeypatch.setenv("TZ", "Pacific/Kiritimati")  # UTC+14
    time.tzset()
    try:
        noon_utc = datetime(2026, 1, 1, 13, 0, tzinfo=timezone(timedelta(hours=1)))
        lrec = Record("L1", "A", D("1.00"), day(1))
        journal = [Arrival(LEFT, lrec, source="P1", captured_at=noon_utc), *done(10)]
        (count,) = series(journal, replay(journal, CFG))
        assert count.day == date(2026, 1, 1)  # 12:00 UTC; 02:00 on the 2nd in Kiritimati
    finally:
        monkeypatch.undo()
        time.tzset()


def test_error_messages_name_what_cannot_be_placed():
    failure = CaptureFailure(LEFT, "P9", datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="capture failure of 'P9' captured_at has no timezone"):
        observe([], replay([], CFG), failures=[failure])
    lrec = Record("L1", "A", D("1.00"), day(1))
    with pytest.raises(ValueError, match=r"arrival \('left', 'L1'\) has no captured_at"):
        series([Arrival(LEFT, lrec)], replay([], CFG))


@pytest.mark.parametrize(
    "rule, message",
    [
        (DriftRule(reference_days=0), "reference_days must be an integer >= 1"),
        (DriftRule(min_decisions=0), "min_decisions must be an integer >= 1"),
        (DriftRule(margin=D("-0.01")), "margin must be a Decimal >= 0"),
        (DriftRule(z=D("0")), "z must be a Decimal > 0"),
    ],
)
def test_a_refused_drift_rule_says_why(rule, message):
    with pytest.raises(ValueError, match=message):
        match_rate([], LEFT, "M0_REFERENCE", rule)


def test_the_smallest_valid_settings_are_accepted():
    match_rate(
        [],
        LEFT,
        "M0_REFERENCE",
        DriftRule(reference_days=1, min_decisions=1, margin=D("0"), z=D("0.5")),
    )
    observe([], replay([], CFG), evidence=EvidenceRule(silent_days=1, max_capture_failure=D("0")))
    observe([], replay([], CFG), evidence=EvidenceRule(max_capture_failure=D("1")))
