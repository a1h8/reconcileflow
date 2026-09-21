"""M0 latency gate (protocol §4) — separate from the attribution verdict.

Only L1 (signal availability) is gated here. It crosses two clocks, so it is
corrected for per-node skew *before* PASS/FAIL; an uncorrected L1 mixes real
transport delay with clock desync.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum

P95_MAX_S = 1.0
P99_MAX_S = 2.0


class LatencyVerdict(Enum):
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True, slots=True)
class Signal:
    node: str
    source_event_time: float  # seconds, on the node's clock
    correlator_ingest_time: float  # seconds, on the correlator's clock


def signal_availability(signals: Sequence[Signal], skew_s: Mapping[str, float]) -> list[float]:
    """L1 per signal, corrected. ``skew_s[node] = node_clock - correlator_clock``.

    A node with no measured skew raises rather than being passed through
    uncorrected.
    """
    return [s.correlator_ingest_time - (s.source_event_time - skew_s[s.node]) for s in signals]


def percentile(values: Sequence[float], p: int) -> float:
    """Nearest-rank percentile: deterministic, no interpolation."""
    ordered = sorted(values)
    return ordered[max(math.ceil(p / 100 * len(ordered)), 1) - 1]


def gate(l1_corrected: Sequence[float]) -> LatencyVerdict:
    ok = percentile(l1_corrected, 95) <= P95_MAX_S and percentile(l1_corrected, 99) <= P99_MAX_S
    return LatencyVerdict.PASS if ok else LatencyVerdict.FAIL
