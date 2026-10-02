"""M0 stability gate (protocol §3, "Gate stabilité") — stability is a result."""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

MIN_REPS = 5
EXTENDED_REPS = 10
MIN_GT_OPS_PER_REP = 10_000
MAX_SD = Decimal("0.015")  # 1.5 points


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
