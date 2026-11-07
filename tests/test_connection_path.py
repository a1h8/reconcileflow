"""Harness tool for the connection path (phase 4, 4a): reading, scoring, controls."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))


def _module(name):
    spec = importlib.util.spec_from_file_location(name, HARNESS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tp = _module("timing_path")
cp = _module("connection_path")

FU_PORT = 4000
HOP_PORT = 5000
T0 = 1_792_000_000_000_000  # µs since the epoch
J1 = "hop-1 -> fake-upstream"
J3 = "load-gen -> hop-1"


def truth(k):
    return f"{k:032x}"


def span(kind, k, start, end, peer_port, host_port, **attrs):
    return {
        "kind": kind,
        "traceID": "f" * 32,
        "peer": attrs.pop("peer", "127.0.0.1"),
        "peerPort": str(peer_port),
        "host": "127.0.0.1",
        "hostPort": str(host_port),
        "start": str(start),
        "handlerStart": str(start),
        "end": str(end),
        "attributes": {"url": f"/truth-{truth(k)}", **attrs},
    }


def write_cell(tmp_path, n, conns, spacing=10_000, concurrency=100, protocol="http/1.1", empty=()):
    """A one-hop cell: request k sent at T0 + k*spacing µs on connection k % conns,
    on both links. Requests in ``empty`` have an empty four-tuple at fake-upstream."""
    tmp_path.mkdir(exist_ok=True)
    cell = tmp_path / "n1"
    cell.mkdir()
    meta = {
        "hops": 1,
        "fake_upstream_port": FU_PORT,
        "hop_ports": [HOP_PORT],
        "agent": "obi",
        "lg_http1": 1,
        "hop_upstream_proto": "h1",
        "hop_max_idle": concurrency,
    }
    (cell / "cell.json").write_text(json.dumps(meta))
    (tmp_path / "cells.txt").write_text(
        f"n1: hops=1 agent=obi requests={n} c={concurrency} load=0\n"
    )
    lg, fu, spans = [], [], []
    for k in range(n):
        t = T0 + k * spacing
        c = k % conns
        lg_port, up_port = 30_000 + c, 40_000 + c
        lg.append(
            {
                "trace_id": truth(k),
                "timestamp_ns": t * 1000,
                "duration_ns": 5_000_000,
                "negotiated_protocol": protocol,
                "conn_key": f"127.0.0.1:{lg_port}-127.0.0.1:{HOP_PORT}|0",
            }
        )
        fu.append({"trace_id": truth(k), "protocol": protocol.upper(), "conn_key": f"{c}"})
        spans.append(span(tp.SERVER, k, t + 100, t + 4_900, lg_port, HOP_PORT, serverAddr="hop-1"))
        spans.append(span(tp.CLIENT, k, t + 200, t + 4_800, up_port, FU_PORT, clientAddr="hop-1"))
        if k in empty:
            spans.append(span(tp.SERVER, k, t + 300, t + 4_700, 0, FU_PORT, peer=""))
        else:
            spans.append(span(tp.SERVER, k, t + 300, t + 4_700, up_port, FU_PORT))
    (cell / "lg.jsonl").write_text("".join(json.dumps(r) + "\n" for r in lg))
    (cell / "fu.jsonl").write_text("".join(json.dumps(r) + "\n" for r in fu))
    (cell / "agent.out").write_text("level=INFO msg=start\n" + json.dumps(spans) + "\n")
    return cell


def params_file(tmp_path, concurrency=100):
    body = ""
    for where in (J1, J3):
        body += f"""[obi.c{concurrency}."{where}"]
receiver_start = "start"
margin_us = 0
local_span_us = 20000
signals = [{{ name = "end", offset_us = 100, window_us = 50 }}]

"""
    path = tmp_path / "params.toml"
    path.write_text(body)
    return path


def j1(report):
    return report["joins"][J1]


# -- reading ---------------------------------------------------------------------


def test_each_observation_carries_the_four_tuple_of_its_connection(tmp_path):
    cell = write_cell(tmp_path, 4, conns=2)
    rows, _ = cp.records(cell, tp.cell_meta(cell))
    by = {(r["point"], r["side"], r["truth"]): r["conn"] for r in rows}
    up = by["hop-1", "outbound", truth(1)]
    assert (
        up
        == by["fake-upstream", "inbound", truth(1)]
        == ("127.0.0.1", 40_001, "127.0.0.1", FU_PORT)
    )
    assert by["load-gen", "outbound", truth(0)] == ("127.0.0.1", 30_000, "127.0.0.1", HOP_PORT)
    assert by["hop-1", "inbound", truth(0)] == by["load-gen", "outbound", truth(0)]


# -- scoring ---------------------------------------------------------------------


def test_simultaneous_requests_on_their_own_connections_are_linked_where_time_alone_refuses(
    tmp_path,
):
    cell = write_cell(tmp_path, 6, conns=6, spacing=0)
    report = cp.analyze_cell(cell, params_file(tmp_path))
    assert (j1(report)["links"], j1(report)["correct"], j1(report)["wrong"]) == (6, 6, 0)
    assert j1(report)["coverage"] == 1.0 and j1(report)["connections"] == 6
    alone = tp.analyze_cell(cell, params_file(tmp_path))["joins"][J1]
    assert (alone["links"], alone["ambiguous"]) == (0, 6)


def test_simultaneous_requests_sharing_a_connection_stay_ambiguous(tmp_path):
    cell = write_cell(tmp_path, 6, conns=3, spacing=0)
    j = j1(cp.analyze_cell(cell, params_file(tmp_path)))
    assert (j["links"], j["wrong"], j["ambiguous"]) == (0, 0, 6)


def test_an_empty_four_tuple_is_no_key_and_never_linked_by_time(tmp_path):
    cell = write_cell(tmp_path, 10, conns=10, empty={3})
    j = j1(cp.analyze_cell(cell, params_file(tmp_path)))
    assert (j["links"], j["wrong"], j["no_key"]) == (9, 0, 1)
    assert j["no_candidate"] == 1  # its partner, alone on its connection
    assert j["coverage"] == pytest.approx(0.9)


def test_the_other_join_is_reported_too(tmp_path):
    cell = write_cell(tmp_path, 6, conns=6, spacing=0)
    report = cp.analyze_cell(cell, params_file(tmp_path))
    assert report["joins"][J3]["correct"] == 6
    assert report["judged"] == J1


# -- validity --------------------------------------------------------------------


def test_a_cell_not_in_http1_is_invalid_not_failed(tmp_path):
    cell = write_cell(tmp_path, 4, conns=2, protocol="h2")
    report = cp.analyze_cell(cell, params_file(tmp_path))
    assert "HTTP/1.1" in report["invalid"] and report["joins"] == {}


def test_a_cell_with_too_many_connections_on_the_judged_link_is_invalid(tmp_path):
    cell = write_cell(tmp_path, 30, conns=30, concurrency=10)
    report = cp.analyze_cell(cell, params_file(tmp_path, concurrency=10))
    assert "connections" in report["invalid"] and report["joins"] == {}


# -- controls --------------------------------------------------------------------


def test_k1_permuted_four_tuples_are_shown_by_wrong_links_or_refusals(tmp_path):
    cell = write_cell(tmp_path, 40, conns=40, spacing=0)
    j = j1(cp.analyze_cell(cell, params_file(tmp_path), permute=1))
    assert j["wrong"] > 0 or j["correct"] < 40 // 2
    assert cp.analyze_cell(cell, params_file(tmp_path), permute=1)["control"] == "K1 seed=1"


def test_k2_dropped_observations_lower_coverage_and_link_nothing_wrong(tmp_path):
    cell = write_cell(tmp_path, 1000, conns=20)
    report = cp.analyze_cell(cell, params_file(tmp_path), drop=10, seed=7)
    j = j1(report)
    assert j["wrong"] == 0
    assert 0.97 <= j["coverage"] <= 0.999
    assert report["control"] == "K2 drop=10 permille seed=7"


def test_k4_without_connections_gives_the_timing_path_figures(tmp_path):
    cell = write_cell(tmp_path, 30, conns=30, spacing=40)
    mine = j1(cp.analyze_cell(cell, params_file(tmp_path), no_connection=True))
    theirs = tp.analyze_cell(cell, params_file(tmp_path))["joins"][J1]
    for field in ("links", "correct", "wrong", "both_seen", "ambiguous", "no_candidate", "dense"):
        assert mine[field] == theirs[field], field


def test_k4_runs_on_any_cell_since_it_judges_nothing_of_the_connection(tmp_path):
    cell = write_cell(tmp_path, 10, conns=1, protocol="h2")
    report = cp.analyze_cell(cell, params_file(tmp_path), no_connection=True)
    assert "invalid" not in report and j1(report)["correct"] == 10


def test_controls_exclude_each_other(tmp_path):
    cell = write_cell(tmp_path, 4, conns=4)
    with pytest.raises(ValueError, match="one control"):
        cp.analyze_cell(cell, params_file(tmp_path), permute=1, no_connection=True)


# -- verdict ---------------------------------------------------------------------


def test_main_prints_the_coverage_against_the_threshold_and_the_wrong_links(tmp_path, capsys):
    write_cell(tmp_path, 6, conns=6, spacing=0)
    cp.main(["connection_path.py", str(tmp_path), str(params_file(tmp_path))])
    out = capsys.readouterr().out
    assert "hop-1 -> fake-upstream (judged): links 6 correct 6 wrong 0" in out
    assert "coverage 100.00% (S-4a >= 99%: pass)" in out
    assert "wrong links on the judged join in the run: 0" in out


def test_stress_judges_wrong_links_only(tmp_path, capsys):
    write_cell(tmp_path, 6, conns=6, spacing=0, concurrency=200)
    cp.main(["connection_path.py", str(tmp_path), str(params_file(tmp_path)), "--stress"])
    out = capsys.readouterr().out
    assert "params from c100" in out and "S-4a" not in out


def test_the_reading_without_the_guard_is_labelled_and_never_a_verdict(tmp_path, capsys):
    # Requests back to back on one connection: the guard counts them as neighbours.
    cell = write_cell(tmp_path, 40, conns=1, spacing=6_000)
    dense = tmp_path / "dense.toml"
    dense.write_text(
        params_file(tmp_path)
        .read_text()
        .replace("local_span_us = 20000", "local_span_us = 20000\nlocal_limit_permille = 1")
    )
    guarded = j1(cp.analyze_cell(cell, dense))
    free = j1(cp.analyze_cell(cell, dense, reading_no_guard=True))
    assert guarded["dense"] > 0 and free["dense"] == 0 and free["wrong"] == 0
    cp.main(["connection_path.py", str(tmp_path), str(dense), "--reading-no-guard"])
    out = capsys.readouterr().out
    assert "READING, rule not frozen: local density guard off inside each connection" in out
    assert "S-4a" not in out
