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


def _worst_sd(reps: Sequence[Repetition]) -> Decimal:
    return max(
        Decimal(str(statistics.stdev(float(r.metrics[name]) for r in reps)))
        for name in reps[0].metrics
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

    if _worst_sd(fixed_seed) > MAX_SD:
        return Stability.UNSTABLE if len(fixed_seed) >= EXTENDED_REPS else Stability.EXTEND_TO_10
    if _worst_sd(variable_seed) > MAX_SD:
        return Stability.STABLE_LOAD_SENSITIVE
    return Stability.STABLE
