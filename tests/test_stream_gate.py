"""Gate of the patched agent (phase 4, 4b): G1-G4 on synthetic cells."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
_spec = importlib.util.spec_from_file_location("stream_gate", HARNESS / "stream_gate.py")
gate_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate_mod)

FU_PORT = 4000
T0 = 1_792_000_000_000_000
TUPLE = ("127.0.0.1", 40000, "127.0.0.1", FU_PORT)
SERVER_PROBE = "obi_uprobe_ServeHTTPReturns"
CLIENT_PROBE = "obi_uprobe_roundTripReturn"


def truth(k):
    return f"{k:032x}"


def span(kind, k, start, end, tup, stream, **attrs):
    a = {"url": f"/truth-{truth(k)}", **attrs}
    if stream:
        a["http2StreamId"] = str(stream)
    peer, peer_port, host, host_port = tup
    return {
        "kind": kind,
        "peer": peer,
        "peerPort": str(peer_port),
        "host": host,
        "hostPort": str(host_port),
        "start": str(start),
        "end": str(end),
        "attributes": a,
    }


def write_cell(
    root,
    n=20,
    up_stream=lambda k: 2 * k + 1,
    down_stream=None,
    down_tuple=None,
    warm=(),
    agent_cpu=None,
    probes=None,
):
    """n requests on one HTTP/2 connection from hop-1 to fake-upstream; stream
    numbers per side given by functions of k (0: not reported). agent_cpu: the
    agent's %CPU of one core in each of 10 pidstat samples. probes: name ->
    (run time ns, run count) during the measured requests."""
    down_stream = down_stream or up_stream
    cell = root / "n1"
    cell.mkdir(parents=True)
    (cell / "cell.json").write_text(json.dumps({"hops": 1, "fake_upstream_port": FU_PORT}))
    (root / "cells.txt").write_text(f"n1: hops=1 requests={n} c=20 load=0\n")
    lg, spans = [], []
    for k in [*range(n), *warm]:
        t = T0 + k * 10_000
        if k < n:
            lg.append({"trace_id": truth(k)})
        spans.append(
            span("SPAN_KIND_CLIENT", k, t + 200, t + 4_800, TUPLE, up_stream(k), clientAddr="hop-1")
        )
        dt = down_tuple(k) if down_tuple else TUPLE
        spans.append(span("SPAN_KIND_SERVER", k, t + 300, t + 4_700, dt, down_stream(k)))
    (cell / "lg.jsonl").write_text("".join(json.dumps(r) + "\n" for r in lg))
    (cell / "warmup-1.jsonl").write_text(
        "".join(json.dumps({"trace_id": truth(k)}) + "\n" for k in warm)
    )
    (cell / "agent.out").write_text("level=INFO msg=start\n" + json.dumps(spans) + "\n")
    if agent_cpu is not None:
        lines = ["# Time UID PID %usr %system %guest %wait %CPU CPU Command"]
        for s in range(10):
            lines.append(f"10:00:{s:02d} 0 99 0 0 0 0 {agent_cpu:.2f} 1 agent")
            lines.append(f"10:00:{s:02d} 1000 98 0 0 0 0 5.00 2 other")
        (cell / "cpu-procs.txt").write_text("\n".join(lines) + "\n")
    if probes is not None:
        unrelated = {"name": "obi_kprobe_other", "run_time_ns": 9_999, "run_cnt": 1}
        before = [{"name": p, "run_time_ns": 1_000, "run_cnt": 10} for p in probes] + [unrelated]
        after = [
            {"name": p, "run_time_ns": 1_000 + t, "run_cnt": 10 + c} for p, (t, c) in probes.items()
        ] + [unrelated]
        (cell / "bpf-before.json").write_text(json.dumps(before))
        (cell / "bpf-after.json").write_text(json.dumps(after))
    return cell


def runs(root, label, k, **kw):
    return [write_cell(root / f"{label}{i}", **kw) for i in range(k)]


# -- G1-G3: the stream number on one run ----------------------------------------


def test_equal_stream_numbers_on_both_sides_pass_g1_g2_g3(tmp_path):
    result = gate_mod.stream_checks(write_cell(tmp_path, warm=(90, 91)))
    assert (result["both_seen"], result["stream_equal"]) == (20, 20)  # warm-up left out
    assert result["G1"] and result["G2"] and result["G3_four_tuple"]


def test_the_shipped_agent_fails_g1(tmp_path):
    assert not gate_mod.stream_checks(write_cell(tmp_path, up_stream=lambda k: 0))["G1"]


def test_one_missing_or_different_number_fails_g1(tmp_path):
    def one_off(value):
        return lambda k: value if k == 7 else 2 * k + 1

    missing = gate_mod.stream_checks(write_cell(tmp_path / "a", down_stream=one_off(0)))
    shifted = gate_mod.stream_checks(write_cell(tmp_path / "b", down_stream=one_off(99)))
    assert not missing["G1"] and missing["stream_equal"] == 19
    assert not shifted["G1"] and shifted["stream_equal"] == 19


def test_one_key_for_two_requests_fails_g2(tmp_path):
    def same_for_3_and_4(k):
        return 5 if k in (3, 4) else 2 * k + 1

    result = gate_mod.stream_checks(
        write_cell(tmp_path, up_stream=same_for_3_and_4, down_stream=same_for_3_and_4)
    )
    assert result["G1"]  # equal on both sides...
    assert (
        not result["G2"] and result["keys_reused_by_two_requests"] == 2
    )  # ...one key, two requests


def test_a_different_four_tuple_fails_g3(tmp_path):
    result = gate_mod.stream_checks(
        write_cell(
            tmp_path,
            down_tuple=lambda k: ("127.0.0.1", 65304, "127.0.0.1", FU_PORT) if k == 2 else TUPLE,
        )
    )
    assert not result["G3_four_tuple"] and result["same_four_tuple"] == 19


def test_every_patched_run_must_pass(tmp_path):
    good = runs(tmp_path, "p", 2)
    bad = write_cell(tmp_path / "p9", down_stream=lambda k: 0)
    assert not gate_mod.gate([*good, bad], runs(tmp_path, "s", 3))["G1"]


# -- G4a: cost of each patched probe, kernel statistics -------------------------


def test_g4a_holds_up_to_ten_percent_more_per_run(tmp_path):
    shipped = runs(tmp_path, "s", 3, probes={SERVER_PROBE: (1_000_000, 1_000)})
    within = runs(tmp_path, "p", 3, probes={SERVER_PROBE: (1_100_000, 1_000)})  # +10 %
    beyond = runs(tmp_path, "q", 3, probes={SERVER_PROBE: (1_101_000, 1_000)})
    assert gate_mod.gate(within, shipped)["G4a"] is True
    result = gate_mod.gate(beyond, shipped)
    assert result["G4a"] is False
    assert result["probes"][SERVER_PROBE] == {
        "patched_ns": 1101.0,
        "shipped_ns": 1000.0,
        "ok": False,
    }


def test_g4a_is_the_cost_per_run_not_the_total(tmp_path):
    # Twice the runs at the same cost per run: not slower.
    shipped = runs(tmp_path, "s", 3, probes={CLIENT_PROBE: (500_000, 1_000)})
    patched = runs(tmp_path, "p", 3, probes={CLIENT_PROBE: (1_000_000, 2_000)})
    assert gate_mod.gate(patched, shipped)["G4a"] is True


def test_g4a_judges_every_probe_run_on_both_sides(tmp_path):
    shipped = runs(tmp_path, "s", 3, probes={SERVER_PROBE: (1_000, 10), CLIENT_PROBE: (1_000, 10)})
    one_slow = runs(tmp_path, "p", 3, probes={SERVER_PROBE: (1_000, 10), CLIENT_PROBE: (2_000, 10)})
    result = gate_mod.gate(one_slow, shipped)
    assert set(result["probes"]) == {SERVER_PROBE, CLIENT_PROBE}
    assert result["G4a"] is False


def test_g4a_fails_without_statistics_or_without_a_probe_run(tmp_path):
    shipped = runs(tmp_path, "s", 3, probes={SERVER_PROBE: (1_000, 10)})
    no_stats = runs(tmp_path, "p", 3)
    never_run = runs(tmp_path, "q", 3, probes={SERVER_PROBE: (0, 0)})
    assert gate_mod.gate(no_stats, shipped)["G4a"] is False
    assert gate_mod.gate(never_run, shipped)["G4a"] is False


# -- G4b: agent CPU per 1,000 requests, against the measured noise ---------------


def test_g4b_holds_inside_the_range_of_the_shipped_runs(tmp_path):
    shipped = [
        write_cell(tmp_path / f"s{i}", agent_cpu=cpu) for i, cpu in enumerate((2.0, 3.0, 4.0))
    ]
    inside = runs(tmp_path, "p", 3, agent_cpu=3.9)
    above = runs(tmp_path, "q", 3, agent_cpu=4.1)
    assert gate_mod.gate(inside, shipped)["G4b"] is True
    assert gate_mod.gate(above, shipped)["G4b"] is False
    # 10 samples x 3.9 % of a core = 0.39 CPU s for 20 requests: 19.5 s per 1,000
    # (a float sum: exact only to rounding, which differs before Python 3.12)
    assert gate_mod.gate(inside, shipped)["cpu_per_1k_patched"][0] == pytest.approx(19.5)


def test_g4b_needs_three_runs_on_each_side(tmp_path):
    shipped = runs(tmp_path, "s", 2, agent_cpu=3.0)
    patched = runs(tmp_path, "p", 3, agent_cpu=3.0)
    assert gate_mod.gate(patched, shipped)["G4b"] is False
    assert gate_mod.gate(patched, [*shipped, write_cell(tmp_path / "s9", agent_cpu=3.0)])["G4b"]
    assert gate_mod.gate(patched[:2], runs(tmp_path, "t", 3, agent_cpu=3.0))["G4b"] is False


def test_g4b_fails_without_cpu_samples(tmp_path):
    assert gate_mod.gate(runs(tmp_path, "p", 3), runs(tmp_path, "s", 3))["G4b"] is False
