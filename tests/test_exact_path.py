"""Harness adapter for the exact path: observations from a run, scoring by truth."""

import importlib.util
import json
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
spec = importlib.util.spec_from_file_location("exact_path", HARNESS / "exact_path.py")
ep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ep)

FU_PORT = 4000


def span(kind, trace, truth, **attrs):
    return {
        "kind": kind,
        "traceID": trace,
        "hostPort": str(attrs.pop("port", 1)),
        "attributes": {"url": f"/truth-{truth}", **attrs},
    }


def chain_spans(truth, trace, hops):
    out = []
    for k in range(1, hops + 1):
        out.append(span(ep.SERVER, trace, truth, serverAddr=f"hop-{k}"))
        out.append(span(ep.CLIENT, trace, truth, clientAddr=f"hop-{k}"))
    out.append(span(ep.SERVER, trace, truth, port=FU_PORT))
    return out


def write_cell(tmp_path, ids, spans, hops=2, masked=0, warmup=(), kinds=None):
    cell = tmp_path / "n2"
    cell.mkdir()
    meta = {"hops": hops, "fake_upstream_port": FU_PORT, "lg_mask": masked, "agent": "obi"}
    meta |= {"context_propagation": "headers", "hop_kinds": kinds or ["traefik"] * hops}
    (cell / "cell.json").write_text(json.dumps(meta))
    (cell / "lg.jsonl").write_text("".join(json.dumps({"trace_id": t}) + "\n" for t in ids))
    (cell / "warmup-1.jsonl").write_text(
        "".join(json.dumps({"trace_id": t}) + "\n" for t in warmup)
    )
    (cell / "agent.out").write_text("time=... level=INFO msg=start\n" + json.dumps(spans) + "\n")
    return cell


def test_a_clean_chain_links_every_request_on_every_join(tmp_path):
    ids = ["a" * 32, "b" * 32]
    cell = write_cell(tmp_path, ids, [s for t in ids for s in chain_spans(t, t, 2)])
    report = ep.analyze_cell(cell)
    assert set(report["joins"]) == {
        "load-gen -> hop-1",
        "hop-1",
        "hop-1 -> hop-2",
        "hop-2",
        "hop-2 -> fake-upstream",
    }
    for r in report["joins"].values():
        assert (r["links"], r["correct"], r["wrong"], r["lost"], r["keyless"]) == (2, 2, 0, 0, 0)
        assert (r["collisions"], r["self_authored"], r["both_seen"]) == (0, 0, 2)


def test_swapped_ids_are_scored_as_wrong_links(tmp_path):
    a, b = "a" * 32, "b" * 32
    spans = chain_spans(a, a, 2) + chain_spans(b, b, 2)
    # hop-2's inbound spans report each other's trace id: a swapping agent.
    for s in spans:
        if s["attributes"].get("serverAddr") == "hop-2":
            s["traceID"] = b if s["traceID"] == a else a
    r = ep.analyze_cell(write_cell(tmp_path, [a, b], spans))["joins"]
    assert (r["hop-1 -> hop-2"]["wrong"], r["hop-1 -> hop-2"]["correct"]) == (2, 0)
    assert r["hop-1 -> hop-2"]["lost"] == 2
    assert (r["hop-2"]["wrong"], r["load-gen -> hop-1"]["wrong"]) == (2, 0)


def test_masked_cell_has_no_client_key_and_agent_ids_are_self_authored(tmp_path):
    ids = ["a" * 32]
    cell = write_cell(tmp_path, ids, chain_spans(ids[0], "f" * 32, 2), masked=1)
    r = ep.analyze_cell(cell)["joins"]
    assert (r["load-gen -> hop-1"]["links"], r["load-gen -> hop-1"]["keyless"]) == (0, 1)
    assert (r["hop-1 -> hop-2"]["correct"], r["hop-1 -> hop-2"]["self_authored"]) == (1, 1)


def test_an_all_zero_trace_id_is_no_key(tmp_path):
    ids = ["a" * 32]
    spans = chain_spans(ids[0], ids[0], 2)
    spans[-1]["traceID"] = "0" * 32  # fake-upstream reported no trace
    r = ep.analyze_cell(write_cell(tmp_path, ids, spans))["joins"]["hop-2 -> fake-upstream"]
    assert (r["links"], r["keyless"], r["lost"]) == (0, 1, 0)


def test_warm_up_and_unknown_traffic_never_reach_the_module(tmp_path):
    ids, warm = ["a" * 32], ["w" * 32]
    spans = chain_spans(ids[0], ids[0], 2) + chain_spans(warm[0], warm[0], 2)
    spans.append(span(ep.SERVER, "c" * 32, "c" * 32, serverAddr="hop-1"))  # not measured
    spans.append(span(ep.SERVER, "d" * 32, "", serverAddr="hop-1"))  # no truth path
    spans.append(span("SPAN_KIND_INTERNAL", "e" * 32, ids[0]))  # not a point
    cell = write_cell(tmp_path, ids, spans, warmup=warm)
    obs, truth, measured = ep.observations(cell, ep.cell_meta(cell))
    assert set(truth.values()) == measured == set(ids)
    assert len(obs) == 1 + 5


def test_observations_carry_no_truth(tmp_path):
    ids = ["a" * 32]
    cell = write_cell(tmp_path, ids, chain_spans(ids[0], "f" * 32, 2), masked=1)
    obs, _truth, _ = ep.observations(cell, ep.cell_meta(cell))
    for o in obs:
        assert set(o.keys) <= {"traceparent"}
        assert "truth" not in repr(o)
        assert ids[0] not in repr(o)  # masked: the client's id appears nowhere


def test_relay_hops_copy_only_through_an_agent_in_headers_mode():
    def crossings(kinds, propagation):
        return {c.point: c.key for c in ep.topology_for(2, kinds, propagation).crossings}

    assert crossings(["relay", "relay"], "agent-default") == {"hop-1": None, "hop-2": None}
    assert crossings(["relay", "relay"], "headers") == {
        "hop-1": "traceparent",
        "hop-2": "traceparent",
    }
    assert crossings(["traefik", "relay"], "agent-default") == {
        "hop-1": "traceparent",
        "hop-2": None,
    }


def test_cell_meta_of_an_older_n_hop_cell_defaults_kinds_and_mask(tmp_path):
    cell = tmp_path / "n1"
    cell.mkdir()
    (cell / "cell.json").write_text(json.dumps({"hops": 1, "fake_upstream_port": 9}))
    assert ep.cell_meta(cell) == {
        "hops": 1,
        "kinds": ["traefik"],
        "port": 9,
        "propagation": "agent-default",
        "masked": False,
    }


def test_cell_meta_of_a_one_hop_run_comes_from_cells_txt(tmp_path):
    (tmp_path / "cells.txt").write_text(
        "positive-c100: port=32783 requests=2000 c=100 flags=[-pool=false] propagation=headers\n"
        "negative-c100: port=44231 requests=2000 c=100 flags=[-mask] propagation=headers\n"
        "old-c1: port=1 requests=10 c=1 flags=[]\n"
    )
    for name, port in (("positive-c100", 32783), ("negative-c100", 44231), ("old-c1", 1)):
        (tmp_path / name).mkdir()
        (tmp_path / name / "port").write_text(f"{port}\n")
    assert ep.cell_meta(tmp_path / "positive-c100") == {
        "hops": 0,
        "kinds": [],
        "port": 32783,
        "propagation": "headers",
        "masked": False,
    }
    assert ep.cell_meta(tmp_path / "negative-c100")["masked"] is True
    assert ep.cell_meta(tmp_path / "old-c1")["propagation"] == "agent-default"


def test_one_hop_cell_joins_load_gen_to_fake_upstream(tmp_path):
    (tmp_path / "cells.txt").write_text("positive-c100: port=4000 flags=[-pool=false]\n")
    cell = tmp_path / "positive-c100"
    cell.mkdir()
    (cell / "port").write_text("4000")
    t = "a" * 32
    (cell / "lg.jsonl").write_text(json.dumps({"trace_id": t}) + "\n")
    (cell / "agent.out").write_text(json.dumps([span(ep.SERVER, t, t, port=FU_PORT)]) + "\n")
    report = ep.analyze_cell(cell)
    assert list(report["joins"]) == ["load-gen -> fake-upstream"]
    assert report["joins"]["load-gen -> fake-upstream"]["correct"] == 1


def test_a_cell_without_agent_output_has_only_load_gen(tmp_path):
    ids = ["a" * 32]
    cell = write_cell(tmp_path, ids, [])
    (cell / "agent.out").unlink()
    r = ep.analyze_cell(cell)["joins"]["load-gen -> hop-1"]
    assert (r["links"], r["both_seen"]) == (0, 0)


def test_main_prints_every_join_and_the_wrong_link_total(tmp_path, capsys):
    ids = ["a" * 32]
    write_cell(tmp_path, ids, chain_spans(ids[0], ids[0], 2))
    ep.main(["exact_path.py", str(tmp_path)])
    out = capsys.readouterr().out
    assert "== n2: hops=2 kinds=traefik,traefik propagation=headers masked=0 measured=1" in out
    assert "hop-2 -> fake-upstream" in out
    assert out.rstrip().endswith("wrong links in the run: 0")
    ep.main(["exact_path.py", str(tmp_path), "--json"])
    report = json.loads(capsys.readouterr().out)
    assert report["n2"]["joins"]["hop-1"]["correct"] == 1
