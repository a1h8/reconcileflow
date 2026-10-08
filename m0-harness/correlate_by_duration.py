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
approximation, not a reimplementation of the sliding window. Got to T=65-70%,
short of the M0 correlator's ~90%, plausibly (not confirmed) because of
exactly that boundary-loss effect.

Third pass (`--window K`, `run_sliding`) tests that plausibility directly:
a genuine overlapping window (each left record's candidates are the *2K+1*
right records nearest its own rank position, not one shared disjoint bucket).
`engine.reconcile()`'s block dict structure cannot express this -- a record
belongs to exactly one block by construction, and reworking that invariant
is a bigger, riskier change to a shipped module than this validation
warrants. Calls `rules.m2_tolerant()` (the same scoring formula) and
`engine._candidate_order` (the same tie-break) directly instead, doing the
greedy resolution globally across all windows' pooled candidates in this
script -- reuses the engine's real scoring/ordering primitives without
touching `engine.py` itself.

The T figures above were measured before this script read sub-millisecond or
composite Go durations ("850µs", "1m2.5s"); such events were skipped without
trace. Their raw logs were not kept and their observed-event counts were not
recorded, so the figures cannot be re-run or checked. Under profile C, at most
~0.13% of requests last under 1ms, so a 300-request window has at most a ~32%
chance of containing one. Because pairing is by rank, one such skip in bucket
mode costs about one pair per later bucket. Treat the figures as historical,
not as results of the current script.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date
from decimal import Decimal

from reconcileflow.engine import _candidate_order, reconcile
from reconcileflow.models import Record, Tolerance
from reconcileflow.rules import m2_tolerant

_PLACEHOLDER_DATE = date(2000, 1, 1)

OBI_LINE = re.compile(
    r"^\S+ \S+ \((?P<dur>(?:[0-9.]+(?:ns|µs|us|ms|s|m|h))+)\[.*?\) (?P<event>\w+)\(subType=\d+\) "
    r"(?P<status>\d+) \S+ \S+\(\S*\) \[.*?\]->\[.*?\] .*?svc=\[(?P<svc>\S+) \S+\] "
    r"traceparent=\[\d{2}-(?P<tid>[0-9a-f]{32})-"
)


# OBI prints Go's time.Duration.String(): "850µs", "41.5ms", "1m2.5s".
_UNIT_NS = {"ns": 1, "µs": 1_000, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}
_UNIT_NS |= {"m": 60 * _UNIT_NS["s"], "h": 3600 * _UNIT_NS["s"]}
_DURATION_PART = re.compile(r"([0-9.]+)(ns|µs|us|ms|s|m|h)")


def _duration_ns(text: str) -> int:
    return int(sum(Decimal(v) * _UNIT_NS[u] for v, u in _DURATION_PART.findall(text)))


def count_unparsed(lines: list[str]) -> int:
    """HTTP events OBI_LINE cannot read. Pairing is by rank, so each one lost
    shifts every later observation against its true counterpart."""
    return sum(1 for line in lines if "HTTP(subType=" in line and not OBI_LINE.match(line))


def load_traefik_inbound(obi_log: str, start: int, end: int) -> list[tuple[int, str]]:
    """Returns (duration_ns, true_trace_id) for each Traefik inbound HTTP event."""
    out = []
    with open(obi_log) as f:
        lines = f.readlines()[start:end]
    for line in lines:
        m = OBI_LINE.match(line)
        if not m or m.group("event") != "HTTP" or m.group("svc") != "traefik":
            continue
        out.append((_duration_ns(m.group("dur")), m.group("tid")))
    return out


def load_gen_rows(path: str) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f]


def _bucket_account(index: int, bucket: int) -> str:
    return "1" if bucket <= 0 else str(index // bucket)


def _sorted_sides(obi_log: str, load_gen_jsonl: str, start: int, end: int):
    # Sort by *completion* time (sent + duration), not send time: OBI emits
    # events in completion order (each hop's span ends when its response
    # returns), and under concurrency with variable latency, send order and
    # completion order diverge substantially -- sorting by send time was
    # tried first and made bucketing actively worse (T dropped to 4-13%,
    # below even the unbucketed baseline), because it scattered genuinely
    # simultaneous completions across unrelated buckets.
    rows = sorted(load_gen_rows(load_gen_jsonl), key=lambda r: r["timestamp_ns"] + r["duration_ns"])
    observed = load_traefik_inbound(obi_log, start, end)  # already in log-emission order
    return rows, observed


def run(
    obi_log: str, load_gen_jsonl: str, start: int, end: int, tol: Tolerance, bucket: int
) -> tuple[int, int]:
    rows, observed = _sorted_sides(obi_log, load_gen_jsonl, start, end)
    lefts = [
        Record(
            id=row["trace_id"],
            account=_bucket_account(i, bucket),
            amount=Decimal(row["duration_ns"]),
            value_date=_PLACEHOLDER_DATE,
        )
        for i, row in enumerate(rows)
    ]
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
    return len(result.matches), correct


def run_sliding(
    obi_log: str, load_gen_jsonl: str, start: int, end: int, tol: Tolerance, window: int
) -> tuple[int, int]:
    """Genuine overlapping ±window candidate generation, resolved globally.

    Each left record's candidates are the ``2*window+1`` right records nearest
    its own rank position -- unlike `run()`'s buckets, adjacent left records'
    windows overlap, so a true match sitting near a bucket boundary is no
    longer lost outright. Conflict resolution (best score wins, mark both
    sides taken, move on) is done globally across every window's pooled
    candidates in one pass -- the same greedy rule `engine._reconcile_block`
    applies per-block, just not scoped to any block here.
    """
    rows, observed = _sorted_sides(obi_log, load_gen_jsonl, start, end)
    lefts = [
        Record(id=row["trace_id"], account="1", amount=Decimal(row["duration_ns"]), value_date=_PLACEHOLDER_DATE)
        for row in rows
    ]
    rights = [
        Record(id=f"obi-{i}", account="1", amount=Decimal(dur), value_date=_PLACEHOLDER_DATE)
        for i, (dur, _tid) in enumerate(observed)
    ]
    true_tid_by_right_id = {f"obi-{i}": tid for i, (_dur, tid) in enumerate(observed)}

    n = len(rights)
    all_candidates = []
    for i, left in enumerate(lefts):
        window_rights = rights[max(0, i - window) : min(n, i + window + 1)]
        all_candidates.extend(m2_tolerant([left], window_rights, tol))

    taken_left: set[str] = set()
    taken_right: set[str] = set()
    matched = 0
    correct = 0
    for candidate in sorted(all_candidates, key=_candidate_order):
        if candidate.left_id in taken_left or candidate.right_ids[0] in taken_right:
            continue
        taken_left.add(candidate.left_id)
        taken_right.add(candidate.right_ids[0])
        matched += 1
        if true_tid_by_right_id[candidate.right_ids[0]] == candidate.left_id:
            correct += 1

    total_left = len(lefts)
    print(
        f"window=±{window} tol amount_abs={tol.amount_abs}ns: "
        f"matched={matched}/{total_left} correct={correct}/{matched if matched else 0} "
        f"(T={100 * correct / total_left:.1f}% of all left records)"
    )
    return matched, correct


def main(argv: list[str]) -> int:
    obi_log, load_gen_jsonl, start, end = argv[0], argv[1], int(argv[2]), int(argv[3])
    mode = argv[4] if len(argv) > 4 else "bucket"
    param = int(argv[5]) if len(argv) > 5 else 0
    for abs_ns in (500_000, 1_000_000, 2_000_000, 5_000_000):
        # max_aggregate_size=1 disables M3: summing two durations together has
        # no meaning for this domain, unlike summing two amounts for a bundled
        # payment entry.
        tol = Tolerance(amount_abs=Decimal(abs_ns), date_days=0, max_aggregate_size=1)
        if mode == "window":
            run_sliding(obi_log, load_gen_jsonl, start, end, tol, param)
        else:
            run(obi_log, load_gen_jsonl, start, end, tol, param)
    with open(obi_log) as f:
        unparsed = count_unparsed(f.readlines()[start:end])
    if unparsed:
        print(f"INVALID: {unparsed} HTTP lines not parsed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
