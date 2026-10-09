"""M0 stability gate (protocol §3, "Gate stabilité") — stability is a result."""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from enum import Enum

from reconcileflow.numeric import ENGINE_CONTEXT

MIN_REPS = 5
EXTENDED_REPS = 10
MIN_GT_OPS_PER_REP = 10_000
MAX_SD = Decimal("0.015")  # 1.5 points
_ZERO = Decimal(0)
_ONE = Decimal(1)


def _achievable(value: Decimal, denominator: int) -> bool:
    """Is ``value`` exactly ``k / denominator`` for some integer ``k``, at
    ``value``'s own expressed decimal precision (PI-17)?

    Not a tolerance in the usual sense: no epsilon is invented. A value
    honestly rounded from a real count will reproduce itself exactly when
    the nearest candidate count is divided back out and re-rounded to the
    same number of places; one that could never have come from any integer
    count (e.g. ``0.12345`` at ``n=10_000``, implying ``1234.5`` events)
    will not.

    Lives here, not in ``attribution.py``, so both ``attribution.Metrics``
    (PI-17) and ``Repetition`` (PI-19) can use it without a circular import:
    ``attribution.py`` already imports constants from this module, not the
    other way around.
    """
    if denominator == 0:
        return True  # nothing to check against -- a population of zero
    exponent = value.as_tuple().exponent
    places = -exponent if isinstance(exponent, int) and exponent < 0 else 0
    quantum = Decimal(1).scaleb(-places)
    k = (value * denominator).to_integral_value(rounding=ROUND_HALF_EVEN)
    if not (_ZERO <= k <= denominator):
        return False
    return (k / denominator).quantize(quantum) == value


class Stability(Enum):
    STABLE = "STABLE"
    STABLE_LOAD_SENSITIVE = "STABLE_LOAD_SENSITIVE"  # steady SUT, pattern-sensitive
    UNSTABLE = "UNSTABLE"
    EXTEND_TO_10 = "EXTEND_TO_10"  # unstable after 5: run 5 more
    INSUFFICIENT = "INSUFFICIENT"  # too few reps or too few GT ops to conclude


@dataclass(frozen=True, slots=True)
class Repetition:
    metrics: Mapping[str, Decimal]  # decision metrics, as fractions
    deterministic_gt_ops: int

    def __post_init__(self) -> None:
        with localcontext(ENGINE_CONTEXT):
            self._validate()

    def _validate(self) -> None:
        # Same scrutiny Metrics and Thresholds already apply (PI-12, PI-13):
        # a systematic error -- the same wrong value every repetition --
        # gives SD = 0 and a confident STABLE verdict, invisible to a check
        # whose purpose is measuring consistency precisely because it IS
        # consistent.
        for name, value in self.metrics.items():
            if not _ZERO <= value <= _ONE:
                raise ValueError(f"metric {name!r} must be a fraction in [0, 1], got {value}")
        if self.deterministic_gt_ops < 0:
            raise ValueError("deterministic_gt_ops cannot be negative")
        # PI-19: a ratio must be achievable as an integer count of its own
        # repetition's sample size, the same relationship PI-17 enforces on
        # attribution.Metrics against n/n_residual.
        for name, value in self.metrics.items():
            if not _achievable(value, self.deterministic_gt_ops):
                raise ValueError(
                    f"metric {name!r} ({value}) is not achievable as "
                    f"k/deterministic_gt_ops={self.deterministic_gt_ops}"
                )


def _metric_names(reps: Sequence[Repetition]) -> frozenset[str]:
    """The metric set every repetition in the group must share (PI-9).

    ``reps[0]`` is not treated as ground truth: a metric missing from it but
    present (and possibly unstable) later must be rejected, not silently
    dropped from the worst-SD computation.
    """
    names = frozenset(reps[0].metrics)
    if not names:
        raise ValueError("a repetition's metrics must be nonempty")
    for r in reps:
        if frozenset(r.metrics) != names:
            raise ValueError(
                f"all repetitions in a group must track the same metrics, "
                f"got {sorted(names)} vs {sorted(r.metrics)}"
            )
    return names


def _worst_sd(reps: Sequence[Repetition]) -> Decimal:
    return max(
        Decimal(str(statistics.stdev(float(r.metrics[name]) for r in reps)))
        for name in _metric_names(reps)
    )


def assess(fixed_seed: Sequence[Repetition], variable_seed: Sequence[Repetition]) -> Stability:
    """Between-run SD, decomposed by seed policy.

    Fixed seed replays the exact same request sequence, so its SD is the SUT's
    intrinsic noise. Variable seed adds sensitivity to the load profile. A large
    variable-seed SD over a small fixed-seed SD is a different architectural
    conclusion, not instability.
    """
    with localcontext(ENGINE_CONTEXT):
        return _assess(fixed_seed, variable_seed)


def _assess(fixed_seed: Sequence[Repetition], variable_seed: Sequence[Repetition]) -> Stability:
    groups = (fixed_seed, variable_seed)
    if any(len(g) < MIN_REPS for g in groups) or any(
        r.deterministic_gt_ops < MIN_GT_OPS_PER_REP for g in groups for r in g
    ):
        return Stability.INSUFFICIENT

    fixed_names, variable_names = _metric_names(fixed_seed), _metric_names(variable_seed)
    if fixed_names != variable_names:
        raise ValueError(
            f"fixed and variable seed groups must track the same metrics, "
            f"got {sorted(fixed_names)} vs {sorted(variable_names)}"
        )

    if _worst_sd(fixed_seed) > MAX_SD:
        return Stability.UNSTABLE if len(fixed_seed) >= EXTENDED_REPS else Stability.EXTEND_TO_10
    if _worst_sd(variable_seed) > MAX_SD:
        return Stability.STABLE_LOAD_SENSITIVE
    return Stability.STABLE
