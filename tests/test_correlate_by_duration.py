"""correlate_by_duration.py on synthetic runs whose true pairing is known.

Durations are spaced 10ms apart against a 0.5ms tolerance, so any pairing loss
below comes from candidate generation (buckets, windows, rank alignment), never
from durations being ambiguous. This shows the mechanisms exist; it does not
measure how much of the gap on real runs they explain.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path

import pytest

from reconcileflow.models import Tolerance

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
spec = importlib.util.spec_from_file_location(
    "correlate_by_duration", HARNESS / "correlate_by_duration.py"
)
cbd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cbd)

TOL = Tolerance(amount_abs=Decimal(500_000), date_days=0, max_aggregate_size=1)


def tid(i):
    return f"{i:032x}"


def obi_line(duration, trace_id, route="(/*)"):
    return (
        f"2026-09-29 10:00:00.12345 ({duration}[{duration}]) HTTP(subType=0) 200 GET "
        f"/x{route} [peer as client:1234]->[host as traefik:7080] contentLen:0B "
        f"responseLen:0B svc=[traefik go] traceparent=[00-{trace_id}-{'1' * 16}[{'2' * 16}]-01]"
    )


def write_run(tmp_path, n, emission_order=None, extra_lines=()):
    """n requests sent together, request i lasting 40 + 10*i ms. OBI sees each
    0.1ms shorter (its span is inside the client's round trip) and emits them
    in emission_order, by default completion order."""
    rows = [
        {"trace_id": tid(i), "timestamp_ns": 0, "duration_ns": (40 + 10 * i) * 1_000_000}
        for i in range(n)
    ]
    order = range(n) if emission_order is None else emission_order
    lines = [obi_line(f"{40 + 10 * i - 0.1:.1f}ms", tid(i)) for i in order]
    log = tmp_path / "obi.log"
    log.write_text("".join(f"{line}\n" for line in [*lines, *extra_lines]))
    gt = tmp_path / "load-gen.jsonl"
    gt.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return str(log), str(gt), len(lines) + len(extra_lines)


@pytest.mark.parametrize(
    "text,ns",
    [
        ("120ns", 120),
        ("850µs", 850_000),
        ("1.5µs", 1_500),
        ("41.5ms", 41_500_000),
        ("1.2s", 1_200_000_000),
        ("1m2.5s", 62_500_000_000),
        ("0s", 0),
    ],
)
def test_reads_every_go_duration_format(text, ns):
    line = obi_line(text, tid(0))
    assert cbd.OBI_LINE.match(line), line
    assert cbd._duration_ns(cbd.OBI_LINE.match(line)["dur"]) == ns


def test_sub_millisecond_event_is_kept(tmp_path):
    log, _, end = write_run(tmp_path, 0, extra_lines=[obi_line("850µs", tid(0))])
    assert cbd.load_traefik_inbound(log, 0, end) == [(850_000, tid(0))]


@pytest.mark.parametrize(
    "runner,param",
    [(cbd.run, 0), (cbd.run, 5), (cbd.run_sliding, 1)],
    ids=["single-block", "bucket-5", "window-1"],
)
def test_unambiguous_durations_pair_fully_in_every_mode(tmp_path, runner, param):
    log, gt, end = write_run(tmp_path, 20)
    assert runner(log, gt, 0, end, TOL, param) == (20, 20)


def test_pair_straddling_a_bucket_boundary_is_lost_by_buckets_not_by_windows(tmp_path):
    # Requests 4 and 5 complete in one order on load-gen and the other in OBI's log.
    order = [0, 1, 2, 3, 5, 4, 6, 7, 8, 9]
    log, gt, end = write_run(tmp_path, 10, emission_order=order)
    assert cbd.run(log, gt, 0, end, TOL, 5) == (8, 8)
    assert cbd.run_sliding(log, gt, 0, end, TOL, 1) == (10, 10)


def test_a_missing_observation_shifts_every_later_bucket(tmp_path):
    # OBI misses request 2: every later observation moves up one rank, so the
    # first request of each later bucket finds its counterpart in the previous one.
    log, gt, end = write_run(tmp_path, 20, emission_order=[i for i in range(20) if i != 2])
    assert cbd.run(log, gt, 0, end, TOL, 5) == (16, 16)
    assert cbd.run_sliding(log, gt, 0, end, TOL, 1) == (19, 19)
    # A window absorbs as many drops as its half-width, no more.
    order = [i for i in range(20) if i not in (2, 3)]
    log, gt, end = write_run(tmp_path, 20, emission_order=order)
    matched, correct = cbd.run_sliding(log, gt, 0, end, TOL, 1)
    assert correct == matched < 18


def test_main_fails_on_an_unreadable_http_line(tmp_path, capsys):
    log, gt, end = write_run(tmp_path, 5)
    assert cbd.main([log, gt, "0", str(end), "window", "1"]) == 0
    assert capsys.readouterr().out.count("correct=5/5") == 4
    log, gt, end = write_run(tmp_path, 5, extra_lines=[obi_line("1ms", tid(9), route="")])
    assert cbd.main([log, gt, "0", str(end)]) == 1
    assert "INVALID: 1 HTTP lines" in capsys.readouterr().err
