"""M0 decision rules: one protocol clause per test."""

from decimal import Decimal

import pytest

from reconcileflow.m0.attribution import RUN_001, Attribution, Metrics, RunStatus, decide
from reconcileflow.m0.latency import LatencyVerdict, Signal, gate, signal_availability
from reconcileflow.m0.stability import Repetition, Stability, assess

D = Decimal
TH = RUN_001


def metrics(d_cov="1", d_acc="1", r="1", t="1", coverage="1", capture="0"):
    return Metrics(D(d_cov), D(d_acc), D(r), D(t), D(coverage), D(capture))


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
    assert verdict(r="0.5", t="0.3") is Attribution.A_WITH_WEAK_RESIDUAL


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
    """R = 5%, T = 4% would give Q = 80%: it must not read as a good ranker."""
    assert verdict(r="0.05", t="0.04") is Attribution.A_WITH_WEAK_RESIDUAL


def test_just_under_threshold_is_borderline_never_rounded_up():
    result = decide(metrics(d_cov="0.949"), TH)
    assert result.attribution is Attribution.BORDERLINE
    assert result.nominal is not Attribution.A


def test_irrelevant_metric_near_its_threshold_is_not_borderline():
    """E sits at 90% but D clears 95% with margin, so E cannot change the verdict."""
    assert decide(metrics(r="0.5", t="0.3"), TH).attribution is Attribution.A_WITH_WEAK_RESIDUAL


def test_metrics_reject_non_fractions():
    with pytest.raises(ValueError):
        metrics(d_acc="99")


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
