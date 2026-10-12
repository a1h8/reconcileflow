"""A1 timestamp-precision analysis: pairing without time, residuals, verdict."""

import importlib.util
import json
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
spec = importlib.util.spec_from_file_location("h1_timestamps", HARNESS / "h1_timestamps.py")
h1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h1)


def row(tid, ts_ns, dur_ns=1_000_000):
    return {"side": "load-gen", "trace_id": tid, "timestamp_ns": ts_ns, "duration_ns": dur_ns}


def span(tid, start_us, end_us, port=8443, kind="SPAN_KIND_SERVER"):
    return {
        "traceID": tid,
        "kind": kind,
        "hostPort": str(port),
        "start": str(start_us),
        "handlerStart": str(start_us),
        "end": str(end_us),
    }


def test_constant_offset_gives_zero_residual_and_holds():
    rows = [row(f"{i:032x}", i * 5_000_000) for i in range(200)]
    spans = [span(f"{i:032x}", i * 5000 + 80, i * 5000 + 80 + 1000) for i in range(200)]
    result = h1.score_obi(rows, spans)
    assert result["paired"] == 200
    assert result["start"]["p99_abs_us"] == 0
    assert result["start"]["verdict"] == "HOLDS"


def test_second_granularity_timestamps_fail_on_distinct_ratio():
    # The text-printer defect: every event of one second gets the same value.
    rows = [row(f"{i:032x}", i * 5_000_000) for i in range(400)]
    spans = [span(f"{i:032x}", (i * 5000 // 1_000_000) * 1_000_000, 0) for i in range(400)]
    result = h1.score_obi(rows, spans)
    assert result["start"]["distinct_ratio"] < 0.99
    assert result["start"]["verdict"] == "FAILS"


def test_pairing_is_by_trace_id_not_by_time():
    # Spans listed in reverse order and shifted: pairing must still use identity.
    rows = [row("a" * 32, 0), row("b" * 32, 1_000_000)]
    spans = [span("b" * 32, 1000 + 50, 2000), span("a" * 32, 0 + 50, 1000)]
    result = h1.score_obi(rows, spans)
    assert result["paired"] == 2
    assert result["start"]["max_abs_us"] == 0


def test_duplicate_trace_id_is_not_scored_and_other_spans_are_ignored():
    rows = [row("a" * 32, 0), row("b" * 32, 1_000_000)]
    spans = [
        span("a" * 32, 50, 1000),
        span("a" * 32, 60, 1000),  # same trace id twice: ambiguous, dropped
        span("b" * 32, 1050, 2000),
        span("b" * 32, 1050, 2000, port=9081),  # other hop
        span("b" * 32, 1050, 2000, kind="SPAN_KIND_CLIENT"),
    ]
    filtered = [s for s in spans if s["kind"] == "SPAN_KIND_SERVER" and int(s["hostPort"]) == 8443]
    result = h1.score_obi(rows, filtered)
    assert result["paired"] == 1


def test_obi_parser_skips_log_lines_and_filters_kind_and_port():
    lines = [
        "time=... level=INFO msg=starting\n",
        json.dumps([span("a" * 32, 1, 2), span("b" * 32, 1, 2, port=9081)]) + "\n",
        json.dumps([span("c" * 32, 1, 2, kind="SPAN_KIND_CLIENT")]) + "\n",
    ]
    assert [s["traceID"] for s in h1.obi_server_spans(lines, 8443)] == ["a" * 32]


def test_probe_not_scored_when_counts_differ():
    rows = [row("a" * 32, 0), row("b" * 32, 1_000_000)]
    result = h1.score_probe(rows, [{"pid": 1, "timestamp_ns": 5}])
    assert result["entry"]["verdict"] == "NOT_SCORED"


def test_probe_paired_by_order_with_jitter():
    rows = [row(f"{i:032x}", i * 10_000_000) for i in range(100)]
    jitter = [(-1) ** i * 30_000 for i in range(100)]  # +/-30 us
    events = [
        {"pid": 1, "timestamp_ns": 7_000_000_000 + i * 10_000_000 + jitter[i]} for i in range(100)
    ]
    result = h1.score_probe(rows, events)
    assert abs(result["entry"]["p99_abs_us"] - 30) < 1e-6
    assert result["entry"]["verdict"] == "HOLDS"


def test_verdict_bands():
    base = {"n": 10, "distinct_ratio": 1.0}
    assert h1.verdict({**base, "p99_abs_us": 100}) == "HOLDS"
    assert h1.verdict({**base, "p99_abs_us": 101}) == "PARTIAL"
    assert h1.verdict({**base, "p99_abs_us": 1000}) == "PARTIAL"
    assert h1.verdict({**base, "p99_abs_us": 1001}) == "FAILS"
    assert h1.verdict({"n": 0}) == "NOT_SCORED"


def test_kernel_pair_compares_obi_and_probe_on_the_same_request():
    rows = [row(f"{i:032x}", i * 10_000_000) for i in range(50)]
    # OBI handlerStart and probe entry agree within +/-4 us; load-gen is far off (irrelevant).
    spans = [span(f"{i:032x}", i * 10_000 + 500 + (4 if i % 2 else -4), 0) for i in range(50)]
    events = [{"pid": 1, "timestamp_ns": 9_000_000_000 + i * 10_000_000} for i in range(50)]
    result = h1.score_kernel_pair(rows, spans, events)
    assert result["n"] == 50
    assert abs(result["p99_abs_us"] - 4) < 1e-6
    assert result["verdict"] == "HOLDS"


def test_id_consistency_reports_missing_positions_and_duplicates():
    rows = [row(f"{i:032x}", i) for i in range(5)]
    spans = [span(f"{i:032x}", 0, 0) for i in (0, 1, 1, 2)] + [span("f" * 32, 0, 0)]
    result = h1.id_consistency(rows, spans)
    assert result["missing_positions"] == [3, 4]
    assert result["duplicated"] == 1
    assert result["spans_with_unknown_id"] == 1
