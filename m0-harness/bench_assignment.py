#!/usr/bin/env python3
"""Seeded synthetic benchmark of the union-find correlator.

The same request is seen at two points of one hop (client and server side):
equal true end time and duration, each side with its own Gaussian noise.
Arrivals are Poisson at ``rate``. Per cell: window = 5 x timestamp jitter,
duration noise = jitter / 4, duration tolerance = 4 x duration noise.

What it reports, every denominator = all requests:
- wrong: matched to the wrong partner and not flagged (a silent error);
- ambiguous, oversized: flagged, not decided;
- max component, and single-process speed (events/s, both sides counted).

A regression baseline, not an acceptance threshold: thresholds are fixed
before the real runs that judge them. Invented noise, independent and
Gaussian: magnitudes are synthetic, only directions carry over.

    python m0-harness/bench_assignment.py            # the full grid
    python m0-harness/bench_assignment.py --quick    # one small cell
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from decimal import Decimal

from correlate_union import Event, correlate

from reconcileflow.models import Tolerance

# Durations from the harness's own workload (fake-upstream latency profile C):
# log-normal, median 40 ms, sigma 1.225, plus a 0.5% stall of 1-5 s.
DURATION_MEDIAN_NS = 40_000_000
DURATION_SIGMA = 1.225
STALL_SHARE = 0.005

# (rate per s, timestamp jitter in ns, margin in score units)
GRID = [
    (1_000, 200_000, "0"),
    (10_000, 200_000, "0"),
    (10_000, 200_000, "0.02"),
    (100_000, 200_000, "0"),
    (100_000, 200_000, "0.02"),
    (100_000, 200_000, "0.05"),
    (100_000, 50_000, "0"),
    (100_000, 50_000, "0.02"),
    (100_000, 10_000, "0"),
    (100_000, 10_000, "0.02"),
    (100_000, 1_000, "0.02"),
]


def generate(n: int, rate: int, jitter_ns: int, seed: int):
    """Both sides of n requests, and the truth (left id -> right id)."""
    rng = random.Random(seed)
    noise = jitter_ns / 4
    lefts, rights, truth = [], [], {}
    t = 0.0
    for i in range(n):
        t += rng.expovariate(rate) * 1e9
        duration = rng.lognormvariate(0, DURATION_SIGMA) * DURATION_MEDIAN_NS
        if rng.random() < STALL_SHARE:
            duration += rng.uniform(1e9, 5e9)
        end = t + duration
        lefts.append(
            Event(
                f"L{i}", round(end + rng.gauss(0, jitter_ns)), round(duration + rng.gauss(0, noise))
            )
        )
        rights.append(
            Event(
                f"R{i}", round(end + rng.gauss(0, jitter_ns)), round(duration + rng.gauss(0, noise))
            )
        )
        truth[f"L{i}"] = f"R{i}"
    return lefts, rights, truth


def run_cell(rate: int, jitter_ns: int, margin: str, n: int, seed: int) -> dict:
    lefts, rights, truth = generate(n, rate, jitter_ns, seed)
    tol = Tolerance(amount_abs=Decimal(jitter_ns), date_days=0, max_aggregate_size=1)
    start = time.perf_counter()
    result = correlate(lefts, rights, window=5 * jitter_ns, tol=tol, margin=Decimal(margin))
    elapsed = time.perf_counter() - start
    wrong = sum(1 for m in result.matches if truth[m.left_id] != m.right_ids[0])
    return {
        "rate": rate,
        "jitter_us": jitter_ns // 1000,
        "margin": margin,
        "n": n,
        "wrong": wrong / n,
        "ambiguous": len(result.ambiguous) / n,
        "oversized": len(result.oversized) / n,
        "max_component": max(result.component_sizes, default=0),
        "events_per_s": round(2 * n / elapsed),
    }


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--n", type=int, default=5000, help="requests per cell")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--quick", action="store_true", help="one small cell")
    args = parser.parse_args(argv)
    grid = [(10_000, 200_000, "0.02")] if args.quick else GRID
    n = 500 if args.quick else args.n
    print("rate jitter_us margin n wrong ambiguous oversized max_component events_per_s")
    for rate, jitter_ns, margin in grid:
        row = run_cell(rate, jitter_ns, margin, n, args.seed)
        print(
            f"{row['rate']} {row['jitter_us']} {row['margin']} {row['n']} "
            f"{row['wrong']:.2%} {row['ambiguous']:.2%} {row['oversized']:.2%} "
            f"{row['max_component']} {row['events_per_s']}"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
