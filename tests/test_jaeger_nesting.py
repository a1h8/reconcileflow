"""Level A tool: our timing path on published Jaeger traces (phase 4 spec 8.10)."""

import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))


def _module(name):
    spec = importlib.util.spec_from_file_location(name, HARNESS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


jn = _module("jaeger_nesting")

T0 = 1_700_000_000_000_000  # µs


def span(trace, sid, parent, service, kind, op, start, duration, pod=None, **tags):
    tag_list = [{"key": "span.kind", "type": "string", "value": kind}]
    tag_list += [{"key": k, "type": "string", "value": v} for k, v in tags.items()]
    return {
        "traceID": trace,
        "spanID": sid,
        "operationName": op,
        "references": (
            [{"refType": "CHILD_OF", "traceID": trace, "spanID": parent}] if parent else []
        ),
        "startTime": T0 + start,
        "duration": duration,
        "tags": tag_list,
        "processID": f"{service}@{pod or service + '-0'}",
    }


def trace_file(tmp_path, name, traces):
    """One Jaeger export: {"data": [trace, ...]}; processes from the spans' ids."""
    data = []
    for spans in traces:
        processes = {}
        for s in spans:
            service, pod = s["processID"].split("@")
            processes[s["processID"]] = {
                "serviceName": service,
                "tags": [{"key": "hostname", "type": "string", "value": pod}],
            }
        data.append({"traceID": spans[0]["traceID"], "spans": spans, "processes": processes})
    (tmp_path / name).write_text(json.dumps({"data": data}))


def request(k, start, call_at=100, call_len=300, total=1000, two_calls=False):
    """One request into 'front' (server span) that calls 'back' once (client span)."""
    t = f"t{k:04d}"
    spans = [
        span(t, f"s{k}", None, "front", "server", "GET /", start, total),
        span(t, f"c{k}", f"s{k}", "front", "client", "/back.Call", start + call_at, call_len),
    ]
    if two_calls:
        spans.append(
            span(t, f"d{k}", f"s{k}", "front", "client", "/back.Call", start + 600, 200)
        )
    return spans


def test_spans_carry_no_truth_to_the_linker(tmp_path):
    t = request(1, 0)
    t[0]["tags"].append({"key": "requestId", "type": "string", "value": "r1"})
    trace_file(tmp_path, "a.json", [t])
    spans = jn.load(tmp_path)
    seen = jn.observable(spans[0])
    assert set(seen) == {"key", "type", "pod", "start", "end"}
    assert "t0001" not in json.dumps(seen) and "r1" not in json.dumps(seen)


def test_joins_come_from_the_type_graph(tmp_path):
    trace_file(tmp_path, "a.json", [request(k, k * 10_000) for k in range(3)])
    joins = jn.joins(jn.load(tmp_path))
    assert joins == [(("front", "server", "GET /"), ("front", "client", "/back.Call"))]


def test_isolated_pairs_exclude_a_parent_with_two_children(tmp_path):
    traces = [request(k, k * 10_000) for k in range(4)] + [request(9, 90_000, two_calls=True)]
    trace_file(tmp_path, "a.json", traces)
    spans = jn.load(tmp_path)
    (join,) = jn.joins(spans)
    isolated = jn.isolated_pairs(spans, join)
    assert len(isolated) == 4
    calls = Counter(s["parent"] for s in spans if s["type"] == join[1])
    (twice,) = [k for k, n in calls.items() if n == 2]
    assert all(a["key"] != twice for a, _ in isolated)


def test_separated_requests_are_never_linked_wrong(tmp_path):
    # Calibration needs spread: call offsets vary a little from request to request.
    traces = [request(k, k * 10_000, call_at=100 + k % 7) for k in range(200)]
    trace_file(tmp_path, "a.json", traces)
    report = jn.analyze(tmp_path, calibration="isolated")
    (j,) = report["joins"].values()
    assert j["wrong"] == 0
    assert j["judged_parents"] == 100  # second half only
    # Known limit of the frozen rule (margin = p99, window = p99.9 of the gaps):
    # with evenly spread gaps the margin nears the window, and a lone candidate
    # whose weight is within the margin is refused AMBIGUOUS, never guessed.
    assert j["correct"] > 0
    assert j["correct"] + j["refused"] == 100
    assert set(j["refused_by_status"]) <= {"AMBIGUOUS"}


def test_identical_overlapping_requests_are_refused_not_guessed(tmp_path):
    calm = [request(k, k * 10_000, call_at=100 + k % 7) for k in range(200)]
    # Two requests in the second half with the very same timings, overlapping.
    twins = [request(500, 1_995_000, call_at=103), request(501, 1_995_000, call_at=103)]
    trace_file(tmp_path, "a.json", calm + twins)
    report = jn.analyze(tmp_path, calibration="isolated")
    (j,) = report["joins"].values()
    assert j["wrong"] == 0
    assert j["refused"] >= 2
    # Their way: a refusal counts as wrong.
    assert report["accuracy_by_service"]["front"] < 1.0


def test_multiple_calls_are_counted_apart_and_judged_only_on_the_second_half(tmp_path):
    traces = [request(k, k * 10_000, call_at=100 + k % 7) for k in range(200)]
    traces.append(request(700, 1_999_000, two_calls=True))
    trace_file(tmp_path, "a.json", traces)
    report = jn.analyze(tmp_path, calibration="isolated")
    (j,) = report["joins"].values()
    assert j["multiple_calls"] == 1
    assert j["judged_parents"] == 100  # the multiple-call parent is apart


def test_truth_calibration_uses_the_first_half_only(tmp_path):
    first = [request(k, k * 10_000, call_at=100 + k % 7) for k in range(100)]
    # Second half: calls much later in the request; windows from the first half miss them.
    second = [request(k, k * 10_000, call_at=500 + k % 7) for k in range(100, 200)]
    trace_file(tmp_path, "a.json", first + second)
    report = jn.analyze(tmp_path, calibration="truth")
    (j,) = report["joins"].values()
    assert j["correct"] == 0 and j["wrong"] == 0
    assert j["refused"] == 100


def test_a_parent_calling_two_services_is_judged_per_join(tmp_path):
    # Each request calls 'back' then 'side': two joins from the same parent span.
    traces = []
    for k in range(200):
        t = request(k, k * 10_000, call_at=100 + k % 7)
        t.append(span(f"t{k:04d}", f"e{k}", f"s{k}", "front", "client", "/side.Call",
                      k * 10_000 + 500 + k % 5, 200))
        traces.append(t)
    trace_file(tmp_path, "a.json", traces)
    report = jn.analyze(tmp_path, calibration="isolated")
    assert len(report["joins"]) == 2
    for j in report["joins"].values():
        assert j["wrong"] == 0
