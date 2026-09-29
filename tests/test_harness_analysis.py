"""Regression cases for the path-labelled, single-hop lab evaluator."""

import importlib.util
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
