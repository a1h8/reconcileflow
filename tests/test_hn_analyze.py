"""n-hop propagation analysis: observation points in chain order, first failure."""

import importlib.util
import json
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
spec = importlib.util.spec_from_file_location("hn_analyze", HARNESS / "hn_analyze.py")
hn = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hn)

A, B = "a" * 32, "b" * 32
FU_PORT = 9443


def lg_row(tid):
    return {"schema_version": 2, "side": "load-gen", "trace_id": tid}


def span(kind, service, truth, reported, port=48736):
    key = "serverAddr" if kind == hn.SERVER else "clientAddr"
    return {
        "kind": kind,
        "hostPort": str(port),
        "traceID": reported,
        "attributes": {"url": f"/truth-{truth}", key: service},
    }


def write_cell(tmp_path, spans, fu_ids, hops=2):
    cell = tmp_path / "n2"
    cell.mkdir()
    (cell / "cell.json").write_text(
        json.dumps({"hops": hops, "strip_hop": 0, "fake_upstream_port": FU_PORT, "hop_ports": []})
    )
    (cell / "lg.jsonl").write_text("".join(json.dumps(lg_row(t)) + "\n" for t in (A, B)))
    (cell / "tail.jsonl").write_text("")
    (cell / "agent.out").write_text("level=INFO msg=start\n" + json.dumps(spans) + "\n")
    (cell / "fu.jsonl").write_text("".join(json.dumps({"trace_id": t}) + "\n" for t in fu_ids))
    return cell


def chain_spans(fu_reported):
    out = []
    for tid in (A, B):
        for k in (1, 2):
            out.append(span(hn.SERVER, f"hop-{k}", tid, tid))
            out.append(span(hn.CLIENT, f"hop-{k}", tid, tid))
        out.append(span(hn.SERVER, "fake-upstream", tid, fu_reported[tid], port=FU_PORT))
    return out


def test_points_follow_the_request_along_the_chain():
    labels = [label for label, _ in hn.points(2, FU_PORT)]
    assert labels == ["hop-1 in", "hop-1 out", "hop-2 in", "hop-2 out", "fake-upstream in"]


def test_clean_chain_has_no_failure_and_wire_agrees(tmp_path):
    cell = write_cell(tmp_path, chain_spans({A: A, B: B}), fu_ids=[A, B])
    result = hn.analyze_cell(cell)
    assert all(r["correct"] == 2 for _, r in result["points"])
    assert hn.first_failure(result["points"]) is None
    assert result["wire"] == {"received_once": 2, "measured": 2}


def test_swap_at_the_last_hop_is_reported_there_first(tmp_path):
    cell = write_cell(tmp_path, chain_spans({A: B, B: A}), fu_ids=[B, A])
    result = hn.analyze_cell(cell)
    assert hn.first_failure(result["points"]) == "fake-upstream in"
    last = dict(result["points"])["fake-upstream in"]
    assert last["wrong_known_trace"] == 2


def test_stripped_header_shows_as_self_authored_downstream(tmp_path):
    cell = write_cell(tmp_path, chain_spans({A: "c" * 32, B: "d" * 32}), fu_ids=["", ""])
    result = hn.analyze_cell(cell)
    assert dict(result["points"])["fake-upstream in"]["unknown_trace"] == 2
    assert result["wire"]["received_once"] == 0


def test_wire_joins_by_path_and_checks_baggage_without_relying_on_trace_id():
    rows = [{**lg_row(A), "baggage": "exchange_id=1"}, {**lg_row(B), "baggage": "exchange_id=2"}]
    fu = [
        {"path": f"/truth-{A}", "trace_id": "", "baggage": "exchange_id=1"},  # traceparent lost
        {"path": f"/truth-{B}", "trace_id": B, "baggage": ""},  # baggage lost
    ]
    w = hn.wire(rows, fu)
    assert w == {
        "measured": 2,
        "reached_once": 2,
        "received_once": 1,
        "traceparent_absent": 1,
        "baggage_exact": 1,
        "baggage_absent": 1,
    }


def test_cell_without_agent_has_wire_check_only(tmp_path):
    cell = write_cell(tmp_path, [], fu_ids=[A, B])
    (cell / "agent.out").unlink()
    result = hn.analyze_cell(cell)
    assert result["points"] == []
    assert result["wire"]["received_once"] == 2
