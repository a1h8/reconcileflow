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
        spans.append(span(t, f"d{k}", f"s{k}", "front", "client", "/back.Call", start + 600, 200))
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
        t.append(
            span(
                f"t{k:04d}",
                f"e{k}",
                f"s{k}",
                "front",
                "client",
                "/side.Call",
                k * 10_000 + 500 + k % 5,
                200,
            )
        )
        traces.append(t)
    trace_file(tmp_path, "a.json", traces)
    report = jn.analyze(tmp_path, calibration="isolated")
    assert len(report["joins"]) == 2
    for j in report["joins"].values():
        assert j["wrong"] == 0


# Aligned comparison (phase 4 spec 8.10.4).


def their_output(traces, method="TW"):
    """Their converted e2e JSON; traces: {trace: (true [(t, s)], pred [(t, s) | None])}."""
    return {
        "traces": {
            method: {
                t: {"true": [list(x) for x in true], "pred": [x and list(x) for x in pred]}
                for t, (true, pred) in traces.items()
            }
        }
    }


def calm_with_back_server(n=200):
    traces = []
    for k in range(n):
        t = request(k, k * 10_000, call_at=100 + k % 7)
        t.append(
            span(
                f"t{k:04d}",
                f"b{k}",
                f"c{k}",
                "back",
                "server",
                "Call",
                k * 10_000 + 110 + k % 7,
                200,
            )
        )
        traces.append(t)
    return traces


def test_aligned_counts_another_traces_child_wrong_and_a_missing_one_refused(tmp_path):
    trace_file(tmp_path, "a.json", calm_with_back_server())
    theirs = {}
    for k in range(200):
        t = f"t{k:04d}"
        pred = [(t, f"c{k}")]
        if k == 150:
            pred = [("t0151", "c151")]
        if k == 160:
            pred = [None]
        theirs[t] = ([(t, f"c{k}")], pred)
    report = jn.aligned(tmp_path, their_output(theirs), calibration="isolated")
    tw = report["methods"]["TW"]["slots"]
    assert tw["wrong"] == 1 and tw["refused"] == 1 and tw["right"] == 98


def test_aligned_a_trace_with_a_refusal_and_no_wrong_is_without_wrong_but_not_right(tmp_path):
    trace_file(tmp_path, "a.json", calm_with_back_server())
    theirs = {
        f"t{k:04d}": ([(f"t{k:04d}", f"c{k}")], [None if k == 170 else (f"t{k:04d}", f"c{k}")])
        for k in range(200)
    }
    e2e = jn.aligned(tmp_path, their_output(theirs), calibration="isolated")["methods"]["TW"][
        "traces"
    ]
    assert e2e["judged"] == 100
    assert e2e["right"] == 99 and e2e["without_wrong"] == 100 and e2e["with_refusal"] == 1


def test_aligned_counts_only_the_second_half(tmp_path):
    trace_file(tmp_path, "a.json", calm_with_back_server())
    # Wrong everywhere in the first half: must not count.
    theirs = {
        f"t{k:04d}": (
            [(f"t{k:04d}", f"c{k}")],
            [(f"t{(k + 1) % 100:04d}", f"c{(k + 1) % 100}") if k < 100 else (f"t{k:04d}", f"c{k}")],
        )
        for k in range(200)
    }
    tw = jn.aligned(tmp_path, their_output(theirs), calibration="isolated")["methods"]["TW"]
    assert tw["slots"]["wrong"] == 0 and tw["slots"]["right"] == 100


def test_aligned_leaves_a_client_to_server_join_out(tmp_path):
    trace_file(tmp_path, "a.json", calm_with_back_server())
    theirs = {f"t{k:04d}": ([(f"t{k:04d}", f"c{k}")], [(f"t{k:04d}", f"c{k}")]) for k in range(200)}
    report = jn.aligned(tmp_path, their_output(theirs), calibration="isolated")
    assert report["compared"] == ["front/server/GET / -> front/client//back.Call"]
    ours = report["methods"]["ours"]["slots"]
    assert ours["right"] + ours["wrong"] + ours["refused"] == 100
    assert ours["wrong"] == 0


def test_aligned_a_parent_calling_two_services_has_one_slot_each(tmp_path):
    traces, theirs = [], {}
    for k in range(200):
        t = f"t{k:04d}"
        spans = request(k, k * 10_000, call_at=100 + k % 7)
        spans.append(
            span(
                t, f"e{k}", f"s{k}", "front", "client", "/side.Call", k * 10_000 + 500 + k % 5, 200
            )
        )
        traces.append(spans)
        theirs[t] = ([(t, f"c{k}"), (t, f"e{k}")], [(t, f"c{k}"), (t, f"e{k}")])
    trace_file(tmp_path, "a.json", traces)
    report = jn.aligned(tmp_path, their_output(theirs), calibration="isolated")
    assert report["apart_multiple_calls"] == 0
    assert report["methods"]["TW"]["slots"]["right"] == 200


def server_chain(k, start):
    """Node.js-like: only server spans, a root client span, no call spans."""
    t = f"t{k:04d}"
    return [
        span(t, f"r{k}", None, "init", "client", "init-span", start, 1000),
        span(t, f"a{k}", f"r{k}", "one", "server", "span-one", start + 10, 900),
        span(t, f"b{k}", f"a{k}", "two", "server", "span-two", start + 100 + k % 7, 300),
    ]


def test_aligned_maps_their_made_up_client_copies_to_our_server_spans(tmp_path):
    trace_file(tmp_path, "a.json", [server_chain(k, k * 10_000) for k in range(200)])
    theirs = {}
    for k in range(200):
        t = f"t{k:04d}"
        both = [(t, f"a{k}_client"), (t, f"b{k}_client")]
        theirs[t] = (both, both)
    report = jn.aligned(tmp_path, their_output(theirs), calibration="isolated")
    assert report["compared"] == [
        "init/client/init-span -> one/server/span-one",
        "one/server/span-one -> two/server/span-two",
    ]
    assert report["truth_mismatch"] == 0
    tw = report["methods"]["TW"]
    assert tw["unmatched"] == 0 and tw["slots"]["right"] == 200


def test_aligned_reports_their_truth_when_it_differs_from_ours(tmp_path):
    trace_file(tmp_path, "a.json", calm_with_back_server())
    theirs = {
        f"t{k:04d}": ([(f"t{k:04d}", f"c{k}")] if k != 180 else [], [(f"t{k:04d}", f"c{k}")])
        for k in range(200)
    }
    report = jn.aligned(tmp_path, their_output(theirs), calibration="isolated")
    assert report["truth_mismatch"] == 1
