"""M0 decision rules: one protocol clause per test."""

from decimal import Decimal

import pytest

from reconcileflow.m0.attribution import (
    MIN_RESIDUAL_SAMPLE_SIZE,
    MIN_SAMPLE_SIZE,
    RUN_001,
    Attribution,
    Metrics,
    RunStatus,
    Thresholds,
    decide,
)
from reconcileflow.m0.latency import LatencyVerdict, Signal, gate, signal_availability
from reconcileflow.m0.stability import Repetition, Stability, assess

D = Decimal
TH = RUN_001
_UNSET = object()  # distinct from an explicit None, which PI-16's tests need to pass through


def metrics(d_cov="1", d_acc="1", r="1", t="1", coverage="1", capture="0", n=_UNSET, n_residual=_UNSET):
    r_value = D(r) if r is not None else None
    t_value = D(t) if t is not None else None
    if n is _UNSET:
        n = MIN_SAMPLE_SIZE
    if n_residual is _UNSET:
        n_residual = MIN_RESIDUAL_SAMPLE_SIZE if r_value is not None else None
    return Metrics(
        D(d_cov),
        D(d_acc),
        r_value,
        t_value,
        D(coverage),
        D(capture),
        n,
        n_residual,
    )


def verdict(**kw):
    return decide(metrics(**kw), TH).attribution


def test_capture_failure_above_one_percent_is_null():
    result = decide(metrics(capture="0.011", coverage="0.9"), TH)
    assert result.run is RunStatus.NULL
    assert result.attribution is None


def test_low_truth_coverage_is_inconclusive_not_null():
    result = decide(metrics(coverage="0.94"), TH)
    assert result.run is RunStatus.VALID_BUT_INCONCLUSIVE


def test_obi_carries_attribution_is_a():
    assert verdict() is Attribution.A


def test_weak_residual_behind_strong_obi():
    """A genuine small residual (D = 99.5%, not the F = 0 default) is required
    here: this test is about a weak residual, which cannot exist when the
    residual itself is empty (PI-6)."""
    assert verdict(d_acc="0.995", r="0.5", t="0.3") is Attribution.A_WITH_WEAK_RESIDUAL


def test_candidate_absent_needs_different_mechanism():
    assert verdict(d_cov="0.5", r="0.5", t="0.4") is Attribution.NEEDS_DIFFERENT_MECHANISM


def test_reliability_gap_with_enough_coverage_needs_different_mechanism():
    """D = 97.5% but D_acc = 97.5%: the hole in the original tree, now amended."""
    assert verdict(d_cov="1", d_acc="0.975") is Attribution.NEEDS_DIFFERENT_MECHANISM


def test_candidate_present_but_misranked_is_c():
    assert verdict(d_cov="0.5", r="0.9", t="0.5") is Attribution.C_SCORING_REQUIRED


def test_hybrid_when_obi_plus_correlator_are_good_enough():
    assert verdict(d_cov="0.7", r="0.95", t="0.9") is Attribution.B_HYBRID


def test_q_is_not_used_below_r_threshold():
    """R = 5%, T = 4% would give Q = 80%: it must not read as a good ranker.

    D = 99.5% (a genuine small residual), not the F = 0 default (PI-6).
    """
    assert verdict(d_acc="0.995", r="0.05", t="0.04") is Attribution.A_WITH_WEAK_RESIDUAL


def test_just_under_threshold_is_borderline_never_rounded_up():
    result = decide(metrics(d_cov="0.949"), TH)
    assert result.attribution is Attribution.BORDERLINE
    assert result.nominal is not Attribution.A


def test_irrelevant_metric_near_its_threshold_is_not_borderline():
    """E sits at 90% but D clears 95% with margin, so E cannot change the verdict.

    D = 99.5% (a genuine small residual), not the F = 0 default (PI-6).
    """
    result = decide(metrics(d_acc="0.995", r="0.5", t="0.3"), TH)
    assert result.attribution is Attribution.A_WITH_WEAK_RESIDUAL


def test_metrics_reject_non_fractions():
    with pytest.raises(ValueError):
        metrics(d_acc="99")


def test_f_and_e_follow_the_protocol_definitions():
    """F = 1 - D and E = D + (1 - D) * T (protocol section 3); E is undefined
    without a correlator measurement rather than defaulted."""
    m = metrics(d_cov="0.9", d_acc="0.9", r="0.8", t="0.5")
    assert m.d == D("0.81")
    assert m.f == D("0.19")
    assert m.e == D("0.81") + D("0.19") * D("0.5")
    assert metrics(r=None, t=None).e is None


def test_metrics_reject_non_fraction_r():
    """r/t have their own range check (they may be None), separate from the
    always-measured fields above."""
    with pytest.raises(ValueError, match="r must be a fraction"):
        metrics(r="1.5")


def test_capture_failure_cannot_exceed_the_unresolved_zone():
    """Capture failures are a subset of the unresolved attempts: 2% of them
    cannot fit inside a 1% unresolved zone."""
    with pytest.raises(ValueError, match="cannot exceed the unresolved zone"):
        metrics(coverage="0.99", capture="0.02")


# --- PI-12: Thresholds has no validation, unlike Metrics -------------------


def test_thresholds_reject_out_of_range_fraction():
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=D("-1"))
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=D("0.90"), d_min=D("-1"))


def test_thresholds_reject_zero_or_negative_band():
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=D("0.90"), borderline_band=D("0"))
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=D("0.90"), borderline_band=D("-0.02"))


def test_run_001_itself_still_constructs_cleanly():
    Thresholds(q_acceptable_min=D("0.90"))


def test_bad_q_acceptable_min_can_no_longer_launder_a_catastrophic_ranking():
    """Before PI-12: q_acceptable_min=-1 turned a Q=1.2% ranking into a
    clean A. Now construction itself is rejected."""
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=D("-1"))


# --- PI-20: r_min == 0 makes _q divide by zero ------------------------------


def test_thresholds_reject_zero_r_min():
    """r_min=0 is individually a legal fraction in [0, 1] (PI-12's original
    check), but _q() divides by r whenever r >= r_min -- only safe if r_min
    itself is > 0."""
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=D("0.90"), r_min=D("0"))


def test_run_001_r_min_still_constructs_cleanly():
    Thresholds(q_acceptable_min=D("0.90"), r_min=D("0.80"))


# --- PI-16: Metrics has no way to express sample size ----------------------


def test_metrics_rejects_n_below_minimum():
    with pytest.raises(ValueError):
        metrics(n=MIN_SAMPLE_SIZE - 1)


def test_metrics_rejects_n_residual_below_minimum():
    with pytest.raises(ValueError):
        metrics(r="0.9", t="0.8", n_residual=MIN_RESIDUAL_SAMPLE_SIZE - 1)


def test_metrics_rejects_n_residual_set_without_correlator():
    with pytest.raises(ValueError):
        metrics(r=None, t=None, n_residual=MIN_RESIDUAL_SAMPLE_SIZE)


def test_metrics_rejects_missing_n_residual_when_correlator_measured():
    with pytest.raises(ValueError):
        metrics(r="0.9", t="0.8", n_residual=None)


def test_single_lucky_request_can_no_longer_fire_the_strongest_verdict():
    """Before PI-16: d_cov=1, d_acc=1 from a single request (n=1) produced
    Attribution.A, the protocol's strongest verdict, with zero statistical
    basis. Now construction itself is rejected."""
    with pytest.raises(ValueError):
        metrics(n=1)


# --- PI-17: ratios must be achievable as an integer count of their own n --


def test_metrics_rejects_an_unachievable_d_cov():
    """0.12345 * 10000 = 1234.5 "correct" requests -- not an integer, so
    this ratio could not have come from any real run."""
    with pytest.raises(ValueError):
        metrics(d_cov="0.12345", n=MIN_SAMPLE_SIZE)


def test_metrics_accepts_a_legitimately_rounded_ratio():
    """1234 / 10001 = 0.12338766..., rounded to 4 places as 0.1234 -- a
    real, honest measurement, not a typo. d_acc=1 makes it its own
    D_cov-implied count."""
    metrics(d_cov="0.1234", d_acc="1", n=10001, r=None, t=None)


def test_d_cov_zero_leaves_d_acc_unconstrained():
    """D_cov = 0 (OBI never supplied a trace_id) has no sub-population to
    check D_acc against -- vacuously accepted regardless of D_acc's value,
    the same reasoning PI-6 applies when the residual itself is empty."""
    metrics(d_cov="0", d_acc="0.5", r=None, t=None)


def test_metrics_rejects_d_acc_unachievable_against_its_d_cov_count():
    """D_acc's denominator is the D_cov-implied count (5000 here), not n:
    0.3333 * 5000 = 1666.5 correct attempts, which no real run produces."""
    with pytest.raises(ValueError, match="D_cov-implied"):
        metrics(d_cov="0.5", d_acc="0.3333", n=MIN_SAMPLE_SIZE)


def test_metrics_rejects_an_unachievable_r():
    with pytest.raises(ValueError):
        metrics(r="0.33333", t="0.1", n_residual=MIN_RESIDUAL_SAMPLE_SIZE)


# --- PI-18: r and t are not coupled to each other's presence --------------


def test_metrics_rejects_r_set_with_t_none():
    with pytest.raises(ValueError):
        metrics(r="0.9", t=None, n_residual=MIN_RESIDUAL_SAMPLE_SIZE)


def test_metrics_rejects_t_set_with_r_none():
    with pytest.raises(ValueError):
        metrics(r=None, t="0.9", n_residual=MIN_RESIDUAL_SAMPLE_SIZE)


def test_metrics_accepts_both_none_or_both_measured():
    metrics(r=None, t=None, n_residual=None)
    metrics(r="0.9", t="0.8", n_residual=MIN_RESIDUAL_SAMPLE_SIZE)


# --- PI-7: T <= R invariant ------------------------------------------------


def test_t_greater_than_r_is_rejected():
    """A correct top1 pick is, by construction, inside the candidate set R
    counts -- T > R can only come from a measurement bug."""
    with pytest.raises(ValueError):
        metrics(r="0.85", t="0.95")


def test_t_equal_to_r_is_accepted():
    assert verdict(d_acc="0.995", r="0.5", t="0.5") is Attribution.A_WITH_WEAK_RESIDUAL


# --- PI-6: r/t have no way to express "not measured" ---------------------


def test_zero_residual_is_a_without_any_correlator_measurement():
    """F = 0 (D = 1 exactly): no residual exists, so r/t are never consulted."""
    assert verdict(r=None, t=None) is Attribution.A


def test_zero_residual_nominal_is_a_regardless_of_a_bad_placeholder():
    """Same F = 0 measurement; r/t are real but deliberately bad (0, 0). The
    NOMINAL verdict is still A -- computed without ever reading r/t, exactly
    as it would be for r=t=1 or any other value.

    The overall `.attribution` can still legitimately become BORDERLINE here
    (not asserted): perturbing `d` alone away from the exact F = 0 point
    re-enters the ordinary tree, where these particular (bad, but real and
    measured) r/t values would produce A_WITH_WEAK_RESIDUAL instead. That is
    a correct, mechanical reading of a real measurement, not the PI-6 bug --
    PI-6 was about r/t being *unmeasured* (None), covered by the tests below.
    """
    assert decide(metrics(r="0", t="0"), TH).nominal is Attribution.A


def test_correlator_not_measured_beats_a_with_weak_residual_guess():
    """D >= 95%/D_acc >= 99%/unresolved <= 3% but a real (nonzero) residual,
    and the correlator was never run: must not silently become
    A_WITH_WEAK_RESIDUAL."""
    assert verdict(d_acc="0.995", r=None, t=None) is Attribution.CORRELATOR_NOT_MEASURED


def test_correlator_not_measured_beats_hybrid_guess():
    """This is the dangerous PI-6 case: D < 95% with no correlator run must
    not silently become B_HYBRID just because a placeholder r/t was
    convenient."""
    assert verdict(d_cov="0.7", r=None, t=None) is Attribution.CORRELATOR_NOT_MEASURED


def test_needs_different_mechanism_unaffected_by_missing_correlator():
    """PI-1's routing depends only on d/d_acc/unresolved: unaffected by PI-6."""
    assert (
        verdict(d_cov="1", d_acc="0.975", r=None, t=None)
        is Attribution.NEEDS_DIFFERENT_MECHANISM
    )


def test_borderline_checked_false_when_correlator_not_measured():
    result = decide(metrics(r=None, t=None), TH)
    assert result.attribution is Attribution.A
    assert result.borderline_checked is False


def test_borderline_checked_false_for_correlator_not_measured_outcome():
    result = decide(metrics(d_cov="0.7", r=None, t=None), TH)
    assert result.attribution is Attribution.CORRELATOR_NOT_MEASURED
    assert result.borderline_checked is False


def test_borderline_checked_true_when_correlator_is_measured():
    result = decide(metrics(d_acc="0.995", r="0.5", t="0.3"), TH)
    assert result.attribution is Attribution.A_WITH_WEAK_RESIDUAL
    assert result.borderline_checked is True


def test_gate0_failure_is_not_borderline_checked():
    result = decide(metrics(capture="0.011", coverage="0.9"), TH)
    assert result.borderline_checked is False


# --- PI-10: F==0 short-circuit must not fire on an impossible perturbed point


def test_needs_different_mechanism_is_not_borderline_via_impossible_point():
    """d_cov=1, d_acc=0.98: a confident NEEDS_DIFFERENT_MECHANISM, nowhere
    near any real threshold. Before PI-10, perturbing the derived `d` axis
    alone landed on d=1.00 while d_acc stayed at its real 0.98 in that same
    point -- (d=1.00, d_acc=0.98) is impossible for any real measurement
    (implies d_cov=1.0204), but `if d == 1` alone treated it as a genuine
    zero-residual state and flipped the verdict, reporting BORDERLINE."""
    result = decide(metrics(d_acc="0.98"), TH)
    assert result.nominal is Attribution.NEEDS_DIFFERENT_MECHANISM
    assert result.attribution is Attribution.NEEDS_DIFFERENT_MECHANISM


# --- PI-15: the perturbation loop can't synthesize impossible points -----


def test_a_is_not_borderline_via_impossible_d_acc_perturbation():
    """d_cov=0.999, d_acc=0.991 (d=0.990009): close enough to the 0.99
    boundary that perturbing d_acc alone by its small band (0.005) crosses
    it, while `d` (derived, not perturbed in that same point) stays at its
    real 0.990009 -- above the perturbed d_acc=0.986, which is impossible
    (d <= d_acc always for any real d_cov <= 1). Isolated so only this one
    perturbation would flip (verified: every other axis leaves the verdict
    at A) -- before PI-15, this alone produced BORDERLINE."""
    result = decide(metrics(d_cov="0.999", d_acc="0.991", r="0.95", t="0.90"), TH)
    assert result.nominal is Attribution.A
    assert result.attribution is Attribution.A


def test_a_is_not_borderline_via_impossible_r_perturbation():
    """r=0.81, t=0.80 (nominal A: Q=80/81=98.8% >= 90%). Before PI-15,
    perturbing r alone to 0.79 (t staying at its real 0.80) synthesized the
    impossible (r=0.79, t=0.80) -- t <= r always (PI-7) -- and judged it as
    a genuine sensitivity instead of an impossible point."""
    result = decide(metrics(d_acc="0.995", r="0.81", t="0.80"), TH)
    assert result.nominal is Attribution.A
    assert result.attribution is Attribution.A



# --- thresholds are inclusive, read on `nominal` --------------------------
# At a threshold `attribution` is BORDERLINE whichever way the comparison
# goes; only `nominal` shows which side the point estimate falls on.


def test_capture_failure_exactly_at_its_maximum_is_still_valid():
    assert decide(metrics(capture="0.01", coverage="0.99"), TH).run is RunStatus.VALID


def test_truth_coverage_exactly_at_its_minimum_is_valid():
    assert decide(metrics(coverage="0.95"), TH).run is RunStatus.VALID


@pytest.mark.parametrize(
    "kw,expected",
    [
        # D_acc exactly at 99%: still the A branch.
        ({"d_acc": "0.99"}, Attribution.A),
        # Q = T / R exactly at q_acceptable_min (90%).
        ({"d_acc": "0.995", "r": "1", "t": "0.9"}, Attribution.A),
        # R exactly at r_min: Q is defined (Q = 1 here).
        ({"d_acc": "0.995", "r": "0.8", "t": "0.8"}, Attribution.A),
        # R exactly at r_min in the F > 5% branch: not NEEDS_DIFFERENT_MECHANISM.
        ({"d_cov": "0.9", "r": "0.8", "t": "0.5"}, Attribution.B_HYBRID),
        # E = 0.5 + 0.5 * 0.8 exactly at e_min.
        ({"d_cov": "0.5", "r": "1", "t": "0.8"}, Attribution.B_HYBRID),
    ],
    ids=["d_acc", "q", "q-defined-at-r_min", "r", "e"],
)
def test_decision_thresholds_are_inclusive(kw, expected):
    assert decide(metrics(**kw), TH).nominal is expected


def test_thresholds_accept_both_ends_of_their_range():
    """[0, 1] for plain fractions, (0, 1] for r_min and the bands."""
    Thresholds(q_acceptable_min=D("0"), d_min=D("1"), r_min=D("1"), borderline_band=D("1"))


def test_skipping_an_impossible_perturbation_still_checks_the_other_side():
    """r=0.79, t=0.78: moving r down by 2 points gives t > r, impossible
    (PI-15), so it is skipped. Moving r up to 0.81 is possible and turns
    NEEDS_DIFFERENT_MECHANISM into B_HYBRID (E = 0.6 + 0.4 * 0.78 = 0.912):
    that alone must make the run BORDERLINE."""
    result = decide(metrics(d_cov="0.6", r="0.79", t="0.78"), TH)
    assert result.nominal is Attribution.NEEDS_DIFFERENT_MECHANISM
    assert result.attribution is Attribution.BORDERLINE

def rep(value, ops=10_000):
    return Repetition({"d": D(value)}, ops)


STEADY = [rep("0.960"), rep("0.961"), rep("0.959"), rep("0.960"), rep("0.960")]
NOISY = [rep("0.90"), rep("0.95"), rep("0.99"), rep("0.92"), rep("0.97")]


def test_stable_when_both_seed_policies_are_steady():
    assert assess(STEADY, STEADY) is Stability.STABLE


def test_load_sensitive_is_not_unstable():
    assert assess(STEADY, NOISY) is Stability.STABLE_LOAD_SENSITIVE


def test_fixed_seed_noise_extends_then_becomes_unstable():
    assert assess(NOISY, STEADY) is Stability.EXTEND_TO_10
    assert assess(NOISY * 2, STEADY) is Stability.UNSTABLE


def test_too_few_reps_or_ops_is_insufficient():
    assert assess(STEADY[:4], STEADY) is Stability.INSUFFICIENT
    assert assess([rep("0.96", ops=9_999)] * 5, STEADY) is Stability.INSUFFICIENT


# --- PI-13: Repetition has no validation, same shape as PI-12 -------------


def test_repetition_rejects_out_of_range_metric():
    """A systematic 150% (unit-conversion typo, not a fraction) repeated
    identically across reps would give SD = 0 -- STABLE -- invisible to a
    consistency check precisely because it IS consistent. Reject at
    construction instead."""
    with pytest.raises(ValueError):
        Repetition({"d": D("150")}, 10_000)


def test_repetition_rejects_negative_gt_ops():
    with pytest.raises(ValueError):
        Repetition({"d": D("0.96")}, -5)


def test_bad_repeated_metric_can_no_longer_launder_a_fake_stable():
    """Before PI-13: five reps agreeing on d=150 reported STABLE. Now
    construction itself is rejected."""
    with pytest.raises(ValueError):
        [Repetition({"d": D("150")}, 10_000) for _ in range(5)]


# --- PI-19: Repetition's ratios must be achievable against their own ops --


def test_repetition_rejects_an_unachievable_metric():
    """0.33333 * 10_000 = 3333.3 -- not an integer, so this ratio could not
    have come from any real repetition with 10,000 deterministic GT ops."""
    with pytest.raises(ValueError):
        Repetition({"d": D("0.33333")}, 10_000)


def test_repetition_accepts_an_achievable_metric():
    Repetition({"d": D("0.9601")}, 10_000)


# --- PI-9: stability gate must not trust reps[0] for which metrics exist --


def test_metric_missing_from_first_rep_is_rejected_not_silently_stable():
    """r swings 0.10 -> 0.95 across reps (wildly unstable), but reps[0] lacks
    it. Before PI-9 this returned STABLE, never examining r at all."""
    reps = [
        Repetition({"d": D("0.96")}, 10_000),
        Repetition({"d": D("0.96"), "r": D("0.10")}, 10_000),
        Repetition({"d": D("0.96"), "r": D("0.90")}, 10_000),
        Repetition({"d": D("0.96"), "r": D("0.20")}, 10_000),
        Repetition({"d": D("0.96"), "r": D("0.95")}, 10_000),
    ]
    with pytest.raises(ValueError):
        assess(reps, reps)


def test_extra_metric_in_first_rep_is_rejected_clearly():
    reps = [Repetition({"d": D("0.96"), "r": D("0.50")}, 10_000)] + [
        Repetition({"d": D("0.96")}, 10_000) for _ in range(4)
    ]
    with pytest.raises(ValueError):
        assess(reps, reps)


def test_all_empty_metrics_is_rejected_clearly():
    reps = [Repetition({}, 10_000) for _ in range(5)]
    with pytest.raises(ValueError):
        assess(reps, reps)


def test_fixed_and_variable_groups_must_track_the_same_metrics():
    fixed = STEADY
    variable = [Repetition({"d": D(v), "r": D("0.5")}, 10_000) for v in ("0.96",) * 5]
    with pytest.raises(ValueError):
        assess(fixed, variable)


def test_latency_is_skew_corrected_before_the_gate():
    # Raw L1 = 5.0s, but the node clock runs 4.5s behind: real latency is 0.5s.
    signals = [Signal("n1", source_event_time=100.0, correlator_ingest_time=105.0)] * 100
    assert gate(signal_availability(signals, {"n1": -4.5})) is LatencyVerdict.PASS
    assert gate(signal_availability(signals, {"n1": 0.0})) is LatencyVerdict.FAIL


def test_missing_skew_is_an_error_not_a_silent_passthrough():
    with pytest.raises(KeyError):
        signal_availability([Signal("n2", 1.0, 2.0)], {"n1": 0.0})


def test_latency_tail_alone_fails_the_gate():
    l1 = [0.1] * 97 + [3.0] * 3
    assert gate(l1) is LatencyVerdict.FAIL


# --- PI-8: latency gate crashes below a minimum signal count --------------


def test_gate_on_empty_signals_raises_not_crashes():
    with pytest.raises(ValueError):
        gate([])


def test_gate_below_min_signals_raises():
    with pytest.raises(ValueError):
        gate([0.1] * 99)


def test_gate_at_min_signals_is_unaffected():
    assert gate([0.1] * 100) is LatencyVerdict.PASS


# --- PI-11: gate() rejects physically impossible negative latencies -------


def test_gate_rejects_a_single_negative_signal():
    with pytest.raises(ValueError):
        gate([0.1] * 99 + [-0.001])


def test_gate_rejects_all_negative_signals():
    with pytest.raises(ValueError):
        gate([-0.5] * 100)


def test_gate_accepts_zero_as_the_fastest_legal_latency():
    assert gate([0.0] * 100) is LatencyVerdict.PASS


# --- PI-14: gate() rejects non-finite (NaN/inf) latencies ------------------


def test_gate_rejects_nan():
    with pytest.raises(ValueError):
        gate([0.1] * 99 + [float("nan")])


def test_gate_rejects_inf():
    with pytest.raises(ValueError):
        gate([0.1] * 99 + [float("inf")])


def test_nan_breaks_permutation_invariance_before_the_fix_this_closes_it():
    """Regression sentinel: the exact multiset that demonstrated the bug --
    90 values at 0.1, 9 at 5.0, 1 NaN -- must now raise for every
    permutation, not silently return a different p95 depending on order."""
    import random

    base = [0.1] * 90 + [5.0] * 9 + [float("nan")]
    rng = random.Random(42)
    for _ in range(20):
        shuffled = base[:]
        rng.shuffle(shuffled)
        with pytest.raises(ValueError):
            gate(shuffled)
