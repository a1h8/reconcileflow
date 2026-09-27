#!/usr/bin/env python3
"""Validate reconcileflow's engine, unmodified, as the M0 duration correlator.

docs/target/reconcileflow-traceability-spec.md §4 proposes reusing reconcileflow's
existing Record/Tolerance/engine.reconcile() for M0's temporal/duration correlation
problem -- treating "amount" as duration and "account" as connection -- rather than
building a separate component, but flags this as "not yet built or tested against
the M0 spike's own data." This script is that test.

Ground truth is trace_id (a `Record.reference`-like field kept OUTSIDE the Record
passed to the engine, since the whole point is testing whether duration alone,
without trace_id, recovers the correct pairing). It uses Traefik's own inbound
HTTP span duration (OBI-observed) against load-gen's own measured round-trip
duration -- not fake-upstream's compromised header read -- because Traefik's
side is independently confirmed 0% self-authored throughout the concurrency
sweep (see README's "Correction" and "Dose-response" sections): a real
mismatch here can only be a correlator error, never inherited noise from
OBI's known-buggy self-authored reads.

First pass (single block, account="1" for every record) tested only the
tolerance/scoring half of the proposal -- this dataset runs on one pooled
connection, so connection-scoped blocking alone has nothing to partition on.
That gave T=15-21%, far below the M0 harness's own duration-ranking
correlator's ~90% (README, "A better ranking signal, tried and it works:
duration"). The gap turned out to be methodological, not a scoring failure:
that correlator's candidate generation narrows to a *temporal* window first
(load-calibrated ~830ms, giving 8-21 candidates) and only ranks by duration
*within* that narrow pool -- our first pass skipped that narrowing entirely
and let duration-tolerance alone sort all 300 candidates globally, a much
harder and unrepresentative problem.

Second pass (`--bucket N`) repurposes `blocking_key`/`Record.account` for
this temporal narrowing instead of leaving it at "1": sort each side by its
own best time proxy (load-gen's `timestamp_ns`; OBI's log emission order,
since its own per-event timestamp is batched/imprecise -- see README, "OBI's
... leading timestamp is batched, not per-event") and bucket every N
consecutive records together. This is a harder, non-overlapping partition
than the original sliding ε-window (a true match straddling a bucket
boundary is missed outright, not just deprioritized) -- an honest
approximation, not a reimplementation of the sliding window.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date
from decimal import Decimal

from reconcileflow.engine import reconcile
from reconcileflow.models import Record, Tolerance

_PLACEHOLDER_DATE = date(2000, 1, 1)

OBI_LINE = re.compile(
    r"^\S+ \S+ \((?P<dur>[0-9.]+)(?P<unit>ms|s)\[.*?\) (?P<event>\w+)\(subType=\d+\) "
    r"(?P<status>\d+) \S+ \S+\(\S*\) \[.*?\]->\[.*?\] .*?svc=\[(?P<svc>\S+) \S+\] "
    r"traceparent=\[\d{2}-(?P<tid>[0-9a-f]{32})-"
)


def _duration_ns(value: str, unit: str) -> int:
    factor = 1_000_000 if unit == "ms" else 1_000_000_000
    return int(round(float(value) * factor))


def load_traefik_inbound(obi_log: str, start: int, end: int) -> list[tuple[int, str]]:
    """Returns (duration_ns, true_trace_id) for each Traefik inbound HTTP event."""
    out = []
    with open(obi_log) as f:
        lines = f.readlines()[start:end]
    for line in lines:
        m = OBI_LINE.match(line)
        if not m or m.group("event") != "HTTP" or m.group("svc") != "traefik":
            continue
        out.append((_duration_ns(m.group("dur"), m.group("unit")), m.group("tid")))
    return out


def load_gen_rows(path: str) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f]


def _bucket_account(index: int, bucket: int) -> str:
    return "1" if bucket <= 0 else str(index // bucket)


def run(obi_log: str, load_gen_jsonl: str, start: int, end: int, tol: Tolerance, bucket: int) -> None:
    # Sort by *completion* time (sent + duration), not send time: OBI emits
    # events in completion order (each hop's span ends when its response
    # returns), and under concurrency with variable latency, send order and
    # completion order diverge substantially -- sorting by send time was
    # tried first and made bucketing actively worse (T dropped to 4-13%,
    # below even the unbucketed baseline), because it scattered genuinely
    # simultaneous completions across unrelated buckets.
    rows = sorted(load_gen_rows(load_gen_jsonl), key=lambda r: r["timestamp_ns"] + r["duration_ns"])
    lefts = [
        Record(
            id=row["trace_id"],
            account=_bucket_account(i, bucket),
            amount=Decimal(row["duration_ns"]),
            value_date=_PLACEHOLDER_DATE,
        )
        for i, row in enumerate(rows)
    ]

    observed = load_traefik_inbound(obi_log, start, end)  # already in log-emission order
    rights = [
        Record(
            id=f"obi-{i}",
            account=_bucket_account(i, bucket),
            amount=Decimal(dur),
            value_date=_PLACEHOLDER_DATE,
        )
        for i, (dur, _tid) in enumerate(observed)
    ]
    true_tid_by_right_id = {f"obi-{i}": tid for i, (_dur, tid) in enumerate(observed)}

    result = reconcile(lefts, rights, tol)

    correct = sum(
        1
        for m in result.matches
        if len(m.right_ids) == 1 and true_tid_by_right_id[m.right_ids[0]] == m.left_id
    )
    total_left = len(lefts)
    print(
        f"bucket={bucket or 'none'} tol amount_abs={tol.amount_abs}ns: "
        f"matched={len(result.matches)}/{total_left} "
        f"correct={correct}/{len(result.matches) if result.matches else 0} "
        f"(T={100 * correct / total_left:.1f}% of all left records)"
    )


if __name__ == "__main__":
    obi_log, load_gen_jsonl, start, end = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    bucket = int(sys.argv[5]) if len(sys.argv) > 5 else 0
    for abs_ns in (500_000, 1_000_000, 2_000_000, 5_000_000):
        # max_aggregate_size=1 disables M3: summing two durations together has
        # no meaning for this domain, unlike summing two amounts for a bundled
        # payment entry.
        tol = Tolerance(amount_abs=Decimal(abs_ns), date_days=0, max_aggregate_size=1)
        run(obi_log, load_gen_jsonl, start, end, tol, bucket)
