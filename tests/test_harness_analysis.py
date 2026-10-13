"""Regression cases for the path-labelled, single-hop lab evaluator."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"


def load_module(name):
    spec = importlib.util.spec_from_file_location(name, HARNESS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sys.modules["analyze_multihop"] = load_module("analyze_multihop")
evaluate = load_module("analyze_run").evaluate
A, B = "a" * 32, "b" * 32


def row(tid, error=""):
    return {
        "schema_version": 2,
        "side": "load-gen",
        "trace_id": tid,
        "protocol": "HTTP/2.0",
        "error": error,
    }


def event(truth, reported, destination="traefik:7080"):
    return (
        f"2026-09-29 10:00:00 (1ms[1ms]) HTTP(subType=0) 200 GET "
        f"/truth-{truth}(/*) [client:1234]->[{destination}] "
        f"svc=[traefik go] traceparent=[00-{reported}-{'1' * 16}[{'2' * 16}]-01]"
    )


def run(rows, lines):
    return evaluate(rows, lines, "traefik", "HTTP", "traefik:7080")


def test_swaps_are_errors_even_when_trace_id_sets_are_equal():
    result = run([row(A), row(B)], [event(A, B), event(B, A)])
    assert result["status"] == "VALID"
    assert result["wrong_known_trace"] == 2
    assert result["correct_per_attempt"] == 0


def test_other_hop_does_not_contaminate_selected_hop():
    result = run([row(A)], [event(A, A), event(A, B, "traefik:9081")])
    assert result["selected_events"] == result["correct"] == 1
    assert result["correct_per_attempt"] == 1


def test_failures_and_missing_observations_stay_in_denominator():
    result = run([row(A), row(B, "timeout")], [event(A, A)])
    assert result["attempted"] == 2
    assert result["request_failures"] == result["missing_requests"] == 1
    assert result["correct_per_attempt"] == 0.5


@pytest.mark.parametrize(
    "lines",
    [
        [],
        [event(A, A), event(A, A)],
        [event(B, B)],
        [event(A, A).replace(f"/truth-{A}", "/compliant")],
        [event(A, A), "broken HTTP(subType=0)"],
    ],
)
def test_invalid_capture_never_publishes_accuracy(lines):
    result = run([row(A)], lines)
    assert result["status"] == "INVALID"
    assert result["correct_per_attempt"] is None


@pytest.mark.parametrize("rows", [[], [row(A), row(A)], [{"trace_id": A}]])
def test_ground_truth_must_be_explicit_and_unique(rows):
    with pytest.raises(ValueError):
        run(rows, [])


evaluate_json = load_module("analyze_run").evaluate_json


def json_line(*spans):
    return json.dumps(list(spans)) + "\n"


def jspan(truth, reported, port=8443, kind="SPAN_KIND_SERVER"):
    return {
        "kind": kind,
        "hostPort": str(port),
        "traceID": reported,
        "attributes": {"url": f"/truth-{truth}"},
    }


def test_json_swaps_are_errors_even_when_trace_id_sets_are_equal():
    lines = ["level=INFO msg=starting\n", json_line(jspan(A, B), jspan(B, A))]
    result = evaluate_json([row(A), row(B)], lines, 8443)
    assert result["status"] == "VALID"
    assert result["wrong_known_trace"] == 2
    assert result["correct"] == 0


def test_json_selects_kind_and_port_only():
    lines = [
        json_line(
            jspan(A, A),
            jspan(A, B, port=9081),
            jspan(A, B, kind="SPAN_KIND_CLIENT"),
        )
    ]
    result = evaluate_json([row(A)], lines, 8443)
    assert result["selected_events"] == result["correct"] == 1


def test_json_self_authored_id_is_unknown_and_corrupt_line_invalidates():
    result = evaluate_json([row(A)], [json_line(jspan(A, "c" * 32))], 8443)
    assert result["unknown_trace"] == 1
    corrupt = evaluate_json([row(A)], [json_line(jspan(A, A)), "[{broken\n"], 8443)
    assert corrupt["status"] == "INVALID"
    assert corrupt["unparsed_http_lines"] == 1


def test_excluded_known_traffic_does_not_invalidate_but_unknown_extra_does():
    W, T, X = "d" * 32, "e" * 32, "f" * 32
    lines = [
        json_line(
            jspan(A, A),
            {
                "kind": "SPAN_KIND_SERVER",
                "hostPort": "8443",
                "traceID": W,
                "attributes": {"url": "/"},
            },
            jspan(T, T),
        )
    ]
    result = evaluate_json([row(A)], lines, 8443, excluded=frozenset({W, T}))
    assert result["status"] == "VALID"
    assert result["excluded_outside_run"] == 2
    assert result["selected_events"] == result["correct"] == 1
    extra = evaluate_json(
        [row(A)], [*lines, json_line(jspan(X, X))], 8443, excluded=frozenset({W, T})
    )
    assert extra["status"] == "INVALID"
    assert extra["unexpected_requests"] == 1


def test_json_points_told_apart_by_service_name_not_port():
    # Two hops reporting the same (wrong) port: only the service name separates them.
    def hop_span(service, truth, reported, kind="SPAN_KIND_SERVER"):
        key = "serverAddr" if kind == "SPAN_KIND_SERVER" else "clientAddr"
        span = jspan(truth, reported, port=48736, kind=kind)
        span["attributes"][key] = service
        return span

    lines = [
        json_line(
            hop_span("hop-1", A, A),
            hop_span("hop-2", A, B),
            hop_span("hop-1", A, A, kind="SPAN_KIND_CLIENT"),
        )
    ]
    rows = [row(A), row(B)]
    first = evaluate_json(rows, lines, attrs={"serverAddr": "hop-1"})
    second = evaluate_json(rows, lines, attrs={"serverAddr": "hop-2"})
    out = evaluate_json(rows, lines, kind="SPAN_KIND_CLIENT", attrs={"clientAddr": "hop-1"})
    assert (first["correct"], second["wrong_known_trace"], out["correct"]) == (1, 1, 1)
    with pytest.raises(ValueError):
        evaluate_json(rows, lines)
