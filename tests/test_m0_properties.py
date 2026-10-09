"""Property-based tests for the M0 decision rules (attribution, stability, latency).

Example cases in test_m0.py check what someone thought to check. These
properties hold over *every* input in the valid space, including the
combinations nobody picked by hand -- which is exactly how PI-6 through
PI-9 were found: a hand-picked example always happens to avoid the gap.

A pure uniform-over-[0,1] strategy for d_cov/d_acc/truth_coverage almost
never lands in the narrow bands the protocol actually cares about (>= 95%,
>= 99%, <= 3%) -- verified empirically before trusting this file: out of
300 uniform draws, zero exercised the "D >= 95% but D_acc < 99%" branch
(PI-1) and zero exercised "a genuine small residual" (0 < F <= 5%). Both
would have passed vacuously, the same silent-gap shape as PI-9. Fixed with
a biased mixture plus `assume()` on the branches narrow enough that even
biasing isn't enough to guarantee a hit within a reasonable example budget.
"""

import math
from decimal import ROUND_CEILING, ROUND_HALF_EVEN, Decimal

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

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
from reconcileflow.m0.latency import MIN_SIGNALS, LatencyVerdict, gate
from reconcileflow.m0.stability import Repetition, _achievable, assess

SETTINGS = settings(max_examples=500, deadline=None)


def _snap(value: Decimal, denominator: int) -> Decimal:
    """Round `value` to the nearest achievable k/denominator (PI-17):
    independently-drawn Decimals essentially never satisfy
    stability._achievable on their own (confirmed: every hand-picked
    strategy below failed immediately once PI-17 landed). Snapping after
    the fact, rather than redesigning every strategy to draw integer
    counts from scratch, keeps each strategy's own targeting of narrow
    protocol branches intact -- a snap moves a value by at most
    1/(2*denominator), negligible next to the bands this file targets."""
    if denominator == 0:
        return value
    k = (value * denominator).to_integral_value(rounding=ROUND_HALF_EVEN)
    k = max(Decimal(0), min(Decimal(denominator), k))
    return k / Decimal(denominator)


def _make_metrics(d_cov, d_acc, r, t, truth_coverage, capture_failure):
    """n/n_residual (PI-16) fixed comfortably above the minimum -- these
    tests are about the decision logic, not about re-testing PI-16's own
    sample-size gate (which has its own dedicated tests below). Every ratio
    is snapped to the nearest value achievable at that sample size (PI-17)."""
    n = MIN_SAMPLE_SIZE
    d_cov = _snap(d_cov, n)
    d_cov_k = int((d_cov * n).to_integral_value(rounding=ROUND_HALF_EVEN))
    # D_acc's real denominator is the D_cov-implied count, not n (PI-17);
    # vacuously unconstrained (left unsnapped) when that count is 0.
    d_acc = _snap(d_acc, d_cov_k) if d_cov_k else d_acc
    truth_coverage = _snap(truth_coverage, n)
    unresolved = Decimal(1) - truth_coverage
    # Clip a full increment below `unresolved` *before* snapping: snapping
    # a value already at the boundary can round it up past the boundary
    # by up to half an increment (verified: 0 violations over 2000 random
    # draws with this margin, vs. real violations without it).
    safe_capture_max = max(Decimal(0), unresolved - Decimal(1) / n)
    capture_failure = _snap(min(capture_failure, safe_capture_max), n)
    n_residual = None
    if r is not None:
        n_residual = MIN_RESIDUAL_SAMPLE_SIZE
        r = _snap(r, n_residual)
        if t is not None:
            # Not clamped to min(t, r): test_t_greater_than_r_always_rejected
            # deliberately passes t > r to confirm PI-7 rejects it -- clamping
            # here would silently "fix" the violation before Metrics ever
            # saw it. correlator_pair() (used by every other caller that
            # wants a *valid* pair) already draws both at n_residual's own
            # 4-place precision, where snapping is a no-op, so T <= R
            # survives unclamped in every case that needs it to.
            t = _snap(t, n_residual)
    return Metrics(d_cov, d_acc, r, t, truth_coverage, capture_failure, n, n_residual)


_fractions = st.decimals(
    min_value=Decimal("0"), max_value=Decimal("1"), places=4, allow_nan=False, allow_infinity=False
)
_near_one = st.decimals(
    min_value=Decimal("0.90"), max_value=Decimal("1"), places=4, allow_nan=False, allow_infinity=False
)
# Biased toward the thresholds the protocol actually tests against (0.95,
# 0.99, 0.97 for unresolved's complement): a plain uniform draw essentially
# never lands above 0.95, let alone 0.99.
fractions_biased_high = st.one_of(_fractions, _near_one)
maybe_fraction = st.one_of(st.none(), _fractions)


@st.composite
def correlator_pair(draw):
    """r, t with t <= r and both-or-neither None enforced (PI-7, PI-18) --
    the only relationship Metrics permits."""
    r = draw(maybe_fraction)
    t = None if r is None else draw(st.decimals(min_value=Decimal("0"), max_value=r, places=4))
    return r, t


@st.composite
def metrics(draw):
    d_cov = draw(fractions_biased_high)
    d_acc = draw(fractions_biased_high)
    r, t = draw(correlator_pair())
    truth_coverage = draw(fractions_biased_high)
    # oracle_capture_failure must not exceed unresolved (= 1 - truth_coverage)
    capture_failure = draw(
        st.decimals(
            min_value=Decimal("0"),
            max_value=Decimal("1") - truth_coverage,
            places=4,
        )
    )
    return _make_metrics(d_cov, d_acc, r, t, truth_coverage, capture_failure)


@SETTINGS
@given(metrics())
def test_decide_never_crashes(m):
    """Every valid Metrics combination produces a Decision -- no branch of
    _classify silently falls through or raises for a reason other than the
    documented, deliberate ones (which construction already rejected)."""
    decide(m, RUN_001)


@st.composite
def zero_residual_metrics(draw):
    """d_cov == d_acc == 1 exactly, r/t free -- forced, not hoped for.
    A one_of(just(1), fractions) mixture for d_cov/d_acc still produced
    zero (0/500) exact-1.0-on-both draws when checked directly: two
    independent 50%-ish draws each needing to land on a single point among
    10,000 (4 decimal places) is far rarer than it looks. Same lesson as
    the other two narrow branches, found the same way -- by checking the
    actual hit count instead of trusting a green test."""
    truth_coverage = draw(st.decimals(min_value=Decimal("0.97"), max_value=Decimal("1"), places=4))
    r, t = draw(correlator_pair())
    return _make_metrics(Decimal("1"), Decimal("1"), r, t, truth_coverage, Decimal("0"))


@SETTINGS
@given(zero_residual_metrics())
def test_zero_residual_is_always_a_regardless_of_correlator(m):
    """F == 0 (D == 1 exactly) must give A as the nominal verdict, whatever
    r/t happen to be -- the PI-6 short-circuit, fuzzed across the whole
    r/t space instead of the two examples picked by hand."""
    result = decide(m, RUN_001)
    assert result.run is RunStatus.VALID
    assert result.nominal is Attribution.A


@st.composite
def small_real_residual_metrics(draw, correlator_measured):
    """Directly targets D >= 95%, D_acc >= 99%, unresolved <= 3%, D != 1 --
    too narrow an intersection for assume()-filtering the general strategy
    (confirmed: hypothesis's own FailedHealthCheck refused to proceed on 0/50
    successful draws rather than silently testing nothing, which is the
    right call)."""
    th = RUN_001
    # +0.001 margin: _make_metrics re-snaps d_acc against the D_cov-implied
    # count (PI-17), not against a power of ten like n -- drawing exactly
    # at d_acc_min left zero room, and the re-snap pushed it back below the
    # threshold it was meant to clear (found by this very test, failing
    # immediately once the margin-free version ran).
    d_acc = draw(
        st.decimals(min_value=th.d_acc_min + Decimal("0.001"), max_value=Decimal("0.9999"), places=4)
    )
    # Round UP: rounding down could let d_cov * d_acc fall just under d_min
    # after both are truncated to 4 places (found by the property test
    # itself, failing on exactly this off-by-one-ULP case).
    d_cov_min = (th.d_min / d_acc).quantize(Decimal("0.0001"), rounding=ROUND_CEILING)
    d_cov = draw(st.decimals(min_value=min(d_cov_min, Decimal("0.9999")), max_value=Decimal("1"), places=4))
    truth_coverage = draw(st.decimals(min_value=Decimal("0.97"), max_value=Decimal("1"), places=4))
    assume(d_cov * d_acc != 1)
    assume(d_cov * d_acc >= th.d_min)  # defensive: rounding has bitten this once already
    if correlator_measured:
        r = draw(st.decimals(min_value=Decimal("0"), max_value=Decimal("1"), places=4))
        t = draw(st.decimals(min_value=Decimal("0"), max_value=r, places=4))
    else:
        r, t = None, None
    return _make_metrics(d_cov, d_acc, r, t, truth_coverage, Decimal("0"))


@SETTINGS
@given(small_real_residual_metrics(correlator_measured=False))
def test_small_real_residual_with_unmeasured_correlator_is_not_a_guess(m):
    """The narrowest branch: D >= 95%, D_acc >= 99%, unresolved <= 3%, but a
    genuine nonzero residual (D != 1) and the correlator unmeasured. Must be
    CORRELATOR_NOT_MEASURED, never a silently-guessed A_WITH_WEAK_RESIDUAL."""
    result = decide(m, RUN_001)
    assert result.run is RunStatus.VALID
    assert result.nominal is Attribution.CORRELATOR_NOT_MEASURED
    assert result.borderline_checked is False


@SETTINGS
@given(small_real_residual_metrics(correlator_measured=True))
def test_small_real_residual_with_measured_correlator_is_a_or_weak(m):
    """Same branch, correlator present this time: must land on A or
    A_WITH_WEAK_RESIDUAL -- never CORRELATOR_NOT_MEASURED, and always
    borderline_checked (robustness is computable once r/t are real)."""
    result = decide(m, RUN_001)
    assert result.run is RunStatus.VALID
    assert result.nominal in (Attribution.A, Attribution.A_WITH_WEAK_RESIDUAL)
    assert result.borderline_checked is True


@st.composite
def pi1_route_metrics(draw):
    """Directly targets D >= 95% with D_acc < 99% (PI-1's original hole:
    coverage compensating for a reliability shortfall) -- same reason as
    small_real_residual_metrics: too narrow for assume() on the general
    strategy, confirmed by hypothesis's own FailedHealthCheck."""
    th = RUN_001
    d_acc = draw(st.decimals(min_value=th.d_min, max_value=th.d_acc_min - Decimal("0.0001"), places=4))
    d_cov_min = (th.d_min / d_acc).quantize(Decimal("0.0001"), rounding=ROUND_CEILING)
    d_cov = draw(st.decimals(min_value=min(d_cov_min, Decimal("1")), max_value=Decimal("1"), places=4))
    truth_coverage = draw(st.decimals(min_value=Decimal("0.97"), max_value=Decimal("1"), places=4))
    assume(d_cov * d_acc >= th.d_min)
    r, t = draw(correlator_pair())
    return _make_metrics(d_cov, d_acc, r, t, truth_coverage, Decimal("0"))


@SETTINGS
@given(pi1_route_metrics())
def test_pi1_route_needs_no_correlator(m):
    """D >= 95% but D_acc < 99% (PI-1's amendment) must reach
    NEEDS_DIFFERENT_MECHANISM regardless of r/t -- that route never
    depended on the correlator, before or after PI-6."""
    th = RUN_001
    result = decide(m, th)
    assert result.run is RunStatus.VALID
    assert result.nominal is Attribution.NEEDS_DIFFERENT_MECHANISM


@SETTINGS
@given(metrics())
def test_unmeasured_correlator_never_reaches_a_correlator_dependent_outcome(m):
    """r or t missing must never silently produce A_WITH_WEAK_RESIDUAL,
    B_HYBRID or C_SCORING_REQUIRED -- those require an actual measurement
    on the residual. This is PI-6's core guarantee, stated as an invariant
    over every input rather than the handful of examples that motivated it."""
    result = decide(m, RUN_001)
    if result.run is not RunStatus.VALID or (m.r is not None and m.t is not None):
        return
    assert result.nominal not in (
        Attribution.A_WITH_WEAK_RESIDUAL,
        Attribution.B_HYBRID,
        Attribution.C_SCORING_REQUIRED,
    )


@SETTINGS
@given(metrics())
def test_borderline_checked_iff_correlator_measured(m):
    """borderline_checked is true exactly when both r and t are real
    numbers and the run is VALID -- never a guess either way (PI-6)."""
    result = decide(m, RUN_001)
    expected = result.run is RunStatus.VALID and m.r is not None and m.t is not None
    assert result.borderline_checked is expected


@SETTINGS
@given(_fractions, _fractions)
def test_t_greater_than_r_always_rejected(r, t):
    """T > R is impossible by the metrics' own definitions (PI-7): a correct
    top1 pick is, by construction, inside the candidate set R counts."""
    if t <= r:
        return
    with pytest.raises(ValueError):
        _make_metrics(Decimal("1"), Decimal("1"), r, t, Decimal("1"), Decimal("0"))


# --- latency.py -------------------------------------------------------------

small_latencies = st.lists(
    st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False),
    min_size=0,
    max_size=MIN_SIGNALS - 1,
)
enough_latencies = st.lists(
    st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False),
    min_size=MIN_SIGNALS,
    max_size=MIN_SIGNALS * 3,
)


@SETTINGS
@given(small_latencies)
def test_gate_below_min_signals_always_raises(l1):
    """PI-8: never silently compute a verdict on too few signals to make
    p95/p99 meaningful -- fuzzed across every count below the threshold,
    not just n=0 and n=99."""
    with pytest.raises(ValueError):
        gate(l1)


@SETTINGS
@given(enough_latencies)
def test_gate_at_or_above_min_signals_never_raises(l1):
    assert gate(l1) in (LatencyVerdict.PASS, LatencyVerdict.FAIL)


maybe_negative_latencies = st.lists(
    st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False),
    min_size=MIN_SIGNALS,
    max_size=MIN_SIGNALS * 3,
)


@SETTINGS
@given(maybe_negative_latencies)
def test_gate_rejects_any_mix_containing_a_negative_signal(l1):
    """PI-11: a single negative L1 anywhere in an otherwise-plausible batch
    must still raise -- fuzzed across every count and position, not just
    the all-negative and single-negative examples picked by hand."""
    assume(any(v < 0 for v in l1))
    with pytest.raises(ValueError):
        gate(l1)


# --- stability.py ------------------------------------------------------------


@SETTINGS
@given(st.sets(st.sampled_from(["d", "r", "t", "e"]), min_size=1, max_size=4))
def test_assess_never_raises_when_all_reps_share_the_same_metrics(names):
    """PI-9: a consistent metric set across both groups must never be
    rejected -- only a mismatch should raise."""
    fixed = [Repetition({n: Decimal("0.9") for n in names}, 10_000) for _ in range(5)]
    variable = [Repetition({n: Decimal("0.9") for n in names}, 10_000) for _ in range(5)]
    assess(fixed, variable)  # must not raise


@SETTINGS
@given(
    st.sets(st.sampled_from(["d", "r", "t", "e"]), min_size=1, max_size=3),
    st.sampled_from(["x", "y", "z"]),
)
def test_assess_raises_when_one_repetition_disagrees(names, extra_only_in_one_rep):
    """PI-9: a single repetition tracking a different metric set -- even
    just one extra key -- must raise, not silently defer to reps[0]."""
    assume(extra_only_in_one_rep not in names)
    base = [Repetition({n: Decimal("0.9") for n in names}, 10_000) for _ in range(4)]
    odd_one_out = Repetition({**{n: Decimal("0.9") for n in names}, extra_only_in_one_rep: Decimal("0.1")}, 10_000)
    reps = [odd_one_out, *base]
    with pytest.raises(ValueError):
        assess(reps, reps)


# --- attribution.py: Thresholds validation (PI-12) --------------------------

out_of_range = st.one_of(
    st.decimals(min_value=Decimal("-10"), max_value=Decimal("-0.0001"), places=4),
    st.decimals(min_value=Decimal("1.0001"), max_value=Decimal("10"), places=4),
)


@SETTINGS
@given(out_of_range)
def test_thresholds_always_rejects_out_of_range_q_acceptable_min(bad):
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=bad)


@SETTINGS
@given(
    st.sampled_from(("r_min", "borderline_band", "d_acc_band")),
    st.one_of(st.just(Decimal("0")), out_of_range),
)
def test_thresholds_always_rejects_zero_or_out_of_range_strictly_positive_field(name, bad):
    """r_min, borderline_band and d_acc_band share the strictly-positive
    (0, 1] group (PI-12, PI-20) -- fuzzed across all three, not just
    borderline_band, so a regression narrowing the check to one field
    would be caught regardless of which one it hits."""
    with pytest.raises(ValueError):
        Thresholds(q_acceptable_min=Decimal("0.90"), **{name: bad})


# --- stability.py: Repetition validation (PI-13) ---------------------------


@SETTINGS
@given(out_of_range)
def test_repetition_always_rejects_out_of_range_metric(bad):
    with pytest.raises(ValueError):
        Repetition({"d": bad}, 10_000)


@SETTINGS
@given(st.integers(max_value=-1))
def test_repetition_always_rejects_negative_gt_ops(bad_ops):
    with pytest.raises(ValueError):
        Repetition({"d": Decimal("0.9")}, bad_ops)


maybe_non_finite_latencies = st.lists(
    st.one_of(
        st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False),
        st.just(float("nan")),
        st.just(float("inf")),
        st.just(float("-inf")),
    ),
    min_size=MIN_SIGNALS,
    max_size=MIN_SIGNALS * 3,
)


@SETTINGS
@given(maybe_non_finite_latencies)
def test_gate_rejects_any_mix_containing_a_non_finite_signal(l1):
    """PI-14: a single NaN or inf anywhere in an otherwise-plausible batch
    must still raise -- fuzzed across every count and position."""
    assume(any(not math.isfinite(v) for v in l1))
    with pytest.raises(ValueError):
        gate(l1)


# --- attribution.py: perturbation loop must skip impossible synthetic
#     points (PI-15) --------------------------------------------------------


def _independent_borderline(m, th):
    """Re-implementation of decide()'s perturbation loop, written separately
    from attribution.py, so a regression in attribution.py's own loop (not
    just in the two specific examples PI-15 was found through) still gets
    caught by comparing against this."""
    from reconcileflow.m0.attribution import _classify

    point = {"d": m.d_cov * m.d_acc, "d_acc": m.d_acc, "unresolved": m.unresolved, "r": m.r, "t": m.t}
    nominal = _classify(**point, th=th)
    for name, value in point.items():
        for sign in (-1, 1):
            band = th.d_acc_band if name == "d_acc" else th.borderline_band
            moved = {**point, name: value + sign * band}
            if moved["d"] > moved["d_acc"] or moved["t"] > moved["r"]:
                continue  # impossible synthetic point (PI-15): must not count
            if _classify(**moved, th=th) is not nominal:
                return Attribution.BORDERLINE
    return nominal


@SETTINGS
@given(small_real_residual_metrics(correlator_measured=True))
def test_borderline_matches_an_independent_reimplementation_of_the_guard(m):
    """decide()'s own attribution must agree with a from-scratch
    reimplementation of the perturbation loop (including the PI-15 skip),
    across the narrowest region this bug was found in."""
    th = RUN_001
    assert decide(m, th).attribution is _independent_borderline(m, th)


# --- attribution.py: Metrics sample-size validation (PI-16) ----------------


@SETTINGS
@given(st.integers(max_value=MIN_SAMPLE_SIZE - 1))
def test_metrics_always_rejects_n_below_minimum(bad_n):
    with pytest.raises(ValueError):
        Metrics(Decimal("1"), Decimal("1"), None, None, Decimal("1"), Decimal("0"), bad_n, None)


@SETTINGS
@given(st.integers(max_value=MIN_RESIDUAL_SAMPLE_SIZE - 1))
def test_metrics_always_rejects_n_residual_below_minimum(bad_n_residual):
    with pytest.raises(ValueError):
        Metrics(
            Decimal("1"),
            Decimal("0.995"),
            Decimal("0.9"),
            Decimal("0.8"),
            Decimal("1"),
            Decimal("0"),
            MIN_SAMPLE_SIZE,
            bad_n_residual,
        )


# --- stability.py: _achievable (PI-17, moved from attribution.py by PI-19) -


@st.composite
def k_and_denominator(draw):
    denominator = draw(st.integers(min_value=1, max_value=1_000_000))
    k = draw(st.integers(min_value=0, max_value=denominator))
    return k, denominator


@SETTINGS
@given(k_and_denominator())
def test_every_true_ratio_is_achievable_against_its_own_denominator(pair):
    """For any integer k in [0, denominator], k/denominator must be
    achievable against that denominator -- this is the construction
    _snap/_make_metrics relies on throughout this file; if it ever stopped
    holding, every other property test here would be fuzzing against
    inputs that silently fail to construct, not against the decision
    logic they claim to test."""
    k, denominator = pair
    value = Decimal(k) / Decimal(denominator)
    assert _achievable(value, denominator)


@SETTINGS
@given(st.integers(min_value=1, max_value=100_000))
def test_achievable_is_vacuous_at_denominator_zero(value_places):
    """A population of zero has nothing to check a ratio against --
    verified directly (not assumed) that any value at all is accepted,
    the same reasoning PI-6 applies when a residual is empty."""
    value = Decimal(value_places % 10001) / Decimal(10000)
    assert _achievable(value, 0)


@SETTINGS
@given(st.integers(min_value=1, max_value=1_000_000), st.integers(min_value=1, max_value=100))
def test_ratio_outside_unit_interval_is_never_achievable(denominator, excess):
    """No count k in [0, denominator] yields a ratio above 1 or below 0.
    Metrics and Repetition range-check before calling _achievable, so this
    branch is only reachable by calling the helper directly -- tested here
    so a future caller that skips the range check is still covered."""
    over = Decimal(1) + Decimal(excess) / Decimal(100)
    assert not _achievable(over, denominator)
    assert not _achievable(-over, denominator)
