"""Harness tools for the timing path: calibration rule, adapter, scoring."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from reconcileflow.correlation.topology import LinkKind

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))


def _module(name):
    spec = importlib.util.spec_from_file_location(name, HARNESS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tp = _module("timing_path")
cal = _module("calibrate_timing")

FU_PORT = 4000
T0 = 1_792_000_000_000_000  # µs since the epoch


def truth(k):
    return f"{k:032x}"


def server(k, start, end, handler=None, **attrs):
    return {
        "kind": tp.SERVER,
        "traceID": "f" * 32,
        "hostPort": str(attrs.pop("port", 1)),
        "start": str(start),
        "handlerStart": str(handler if handler is not None else start),
        "end": str(end),
        "attributes": {"url": f"/truth-{truth(k)}", **attrs},
    }


def client(k, start, end, **attrs):
    return {
        "kind": tp.CLIENT,
        "traceID": "f" * 32,
        "hostPort": "1",
        "start": str(start),
        "end": str(end),
        "attributes": {"url": f"/truth-{truth(k)}", **attrs},
    }


def write_cell(tmp_path, n, spacing=10_000, agent="obi", concurrency=100, warm=()):
    """A one-hop cell: request k sent at T0 + k*spacing µs, 5 ms round trip."""
    cell = tmp_path / "n1"
    cell.mkdir()
    meta = {"hops": 1, "fake_upstream_port": FU_PORT, "agent": agent}
    (cell / "cell.json").write_text(json.dumps(meta))
    (tmp_path / "cells.txt").write_text(f"n1: hops=1 agent={agent} requests={n} c={concurrency}\n")
    lg, spans = [], []
    for k in range(n):
        t = T0 + k * spacing
        lg.append({"trace_id": truth(k), "timestamp_ns": t * 1000, "duration_ns": 5_000_000})
        spans.append(server(k, t + 100, t + 4_900, serverAddr="hop-1"))
        spans.append(client(k, t + 200, t + 4_800, clientAddr="hop-1"))
        spans.append(server(k, t + 300, t + 4_700, handler=t + 310, port=FU_PORT))
    for k in warm:
        t = T0 + k * spacing
        spans.append(server(k, t + 300, t + 4_700, port=FU_PORT))
    (cell / "lg.jsonl").write_text("".join(json.dumps(r) + "\n" for r in lg))
    (cell / "warmup-1.jsonl").write_text(
        "".join(json.dumps({"trace_id": truth(k)}) + "\n" for k in warm)
    )
    (cell / "agent.out").write_text("level=INFO msg=start\n" + json.dumps(spans) + "\n")
    return cell


# -- adapter -------------------------------------------------------------------


def test_the_declared_chain_has_only_timing_derived_hops():
    topology = tp.topology_for(2)
    assert [(h.src, h.dst) for h in topology.hops] == [
        ("hop-1", "hop-2"),
        ("hop-2", "fake-upstream"),
        ("load-gen", "hop-1"),
    ]
    assert {h.link for h in topology.hops} == {LinkKind.NESTED_TIMING}


def test_records_carry_times_and_truth_only_for_scoring(tmp_path):
    cell = write_cell(tmp_path, 3, warm=[100])
    rows, measured = tp.records(cell, tp.cell_meta(cell))
    assert len(measured) == 3
    by_place = {(r["point"], r["side"]) for r in rows}
    assert by_place == {
        ("load-gen", "outbound"),
        ("hop-1", "inbound"),
        ("hop-1", "outbound"),
        ("fake-upstream", "inbound"),
    }
    assert len(rows) == 12  # the warm-up span is dropped
    fu = next(r for r in rows if r["point"] == "fake-upstream")
    assert fu["handler"] == fu["start"] + 10


def params_file(tmp_path, body):
    path = tmp_path / "params.toml"
    path.write_text(body)
    return path


J1 = """[obi.c100."hop-1 -> fake-upstream"]
receiver_start = "start"
margin_us = 0
signals = [{ name = "end", offset_us = 100, window_us = 50 }]
"""


def test_well_separated_requests_are_linked_and_scored_by_truth(tmp_path):
    cell = write_cell(tmp_path, 50)
    report = tp.analyze_cell(cell, params_file(tmp_path, J1))
    j = report["joins"]["hop-1 -> fake-upstream"]
    assert (j["links"], j["correct"], j["wrong"], j["both_seen"]) == (50, 50, 0, 50)
    assert j["ambiguous"] == j["no_candidate"] == 0
    assert j["feasible"] and j["component_max"] == 2
    assert report["rate_per_s"] == pytest.approx(50 / ((49 * 10_000 + 5_000) / 1e6), rel=1e-3)


def test_requests_closer_than_the_noise_are_ambiguous_not_wrong(tmp_path):
    # Six requests at the same instant: nothing in time tells them apart.
    cell = write_cell(tmp_path, 6, spacing=0)
    j = tp.analyze_cell(cell, params_file(tmp_path, J1))["joins"]["hop-1 -> fake-upstream"]
    assert (j["links"], j["wrong"], j["ambiguous"]) == (0, 0, 6)
    assert j["component_max"] == 12


def test_the_handler_start_is_used_only_when_calibration_chose_it(tmp_path):
    cell = write_cell(tmp_path, 20)
    start_based = """[obi.c100."hop-1 -> fake-upstream"]
receiver_start = "{receiver}"
margin_us = 0
signals = [{{ name = "start", offset_us = {offset}, window_us = 5 }}]
"""
    for receiver, offset in (("start", 100), ("handlerStart", 110)):
        body = start_based.format(receiver=receiver, offset=offset)
        j = tp.analyze_cell(cell, params_file(tmp_path, body))["joins"]["hop-1 -> fake-upstream"]
        assert j["correct"] == 20, receiver


def test_a_cell_without_parameters_for_its_agent_and_concurrency_is_reported(tmp_path, capsys):
    write_cell(tmp_path, 5, agent="beyla")
    tp.main(["timing_path.py", str(tmp_path), str(params_file(tmp_path, J1))])
    out = capsys.readouterr().out
    assert "no calibrated join" in out and "wrong links in the run: 0" in out


def test_main_prints_every_join(tmp_path, capsys):
    write_cell(tmp_path, 5)
    tp.main(["timing_path.py", str(tmp_path), str(params_file(tmp_path, J1))])
    out = capsys.readouterr().out
    assert "hop-1 -> fake-upstream: links 5 correct 5 wrong 0" in out
    tp.main(["timing_path.py", str(tmp_path), str(params_file(tmp_path, J1)), "--json"])
    assert json.loads(capsys.readouterr().out)["n1"]["measured"] == 5


# -- calibration rule ------------------------------------------------------------


def pair(start_gap, end_gap, b_duration=1_000, handler_gap=None):
    """A true pair with the given start and end gaps (µs)."""
    a = {"start": 0, "end": start_gap + b_duration + end_gap}
    b_start = start_gap
    b = {
        "start": b_start,
        "handler": handler_gap if handler_gap is not None else b_start,
        "end": b_start + b_duration,
    }
    return a, b


def test_the_quietest_signal_is_kept_and_a_second_only_within_three_times():
    # end residual up to 10, start up to 25 (<= 30: kept), duration ~35 (> 30).
    pairs = [pair(100 + (k % 26), 50 + (k % 11), b_duration=1_000 + (k % 71)) for k in range(1000)]
    chosen = cal.choose(pairs)
    names = [s["name"] for s in chosen["signals"]]
    assert names[0] == "end"
    p999 = {n: chosen["stats"][n]["p999"] for n in cal.SIGNALS}
    assert ("start" in names) == (p999["start"] <= 3 * p999["end"])
    assert "duration" not in names or p999["duration"] <= 3 * p999["end"]
    assert len(names) <= 2
    end = chosen["signals"][0]
    assert end["offset_us"] == chosen["stats"]["end"]["offset_us"]
    assert end["window_us"] == chosen["stats"]["end"]["p999"]
    assert chosen["margin_us"] == chosen["stats"]["end"]["p99"]


def test_a_noisy_second_signal_is_dropped():
    pairs = [pair(100 + (k % 500), 50 + (k % 5)) for k in range(1000)]
    assert [s["name"] for s in cal.choose(pairs)["signals"]] == ["end"]


def test_the_handler_start_is_chosen_when_it_gives_the_quieter_best_signal():
    # The span start wanders by up to 400 µs; the handler start does not.
    pairs = [pair(100 + (k % 400), 5_000, handler_gap=100 + (k % 3)) for k in range(1000)]
    for a, b in pairs:
        a["end"] = b["end"] + 5_000 + (pairs.index((a, b)) % 900)  # a noisy end
    chosen = cal.choose(pairs)
    assert chosen["receiver_start"] == "handlerStart"
    assert chosen["signals"][0]["name"] == "start"


def test_duration_is_never_the_first_signal():
    # Duration is the quietest (constant), but it does not place a request in time.
    pairs = [pair(100 + (k % 50), 50 + (k % 40)) for k in range(1000)]
    for k, (a, b) in enumerate(pairs):
        shift = 400_000 if k % 100 == 0 else 0  # a clock jump on both ends
        b["start"] += shift
        b["handler"] += shift
        b["end"] += shift
        a["end"] = a["start"] + (b["end"] - b["start"]) + 300
    chosen = cal.choose(pairs)
    assert chosen["stats"]["duration"]["p999"] < chosen["stats"]["start"]["p999"]
    assert chosen["signals"][0]["name"] in cal.LOCATING
    assert [s["name"] for s in chosen["signals"]][1:] in ([], ["duration"])


def test_a_window_is_never_zero():
    pairs = [pair(100, 50) for _ in range(10)]
    assert all(s["window_us"] == 1 for s in cal.choose(pairs)["signals"])


def test_calibration_writes_parameters_the_adapter_reads_back(tmp_path, capsys):
    write_cell(tmp_path, 40)
    out = tmp_path / "params.toml"
    cal.main(["calibrate_timing.py", str(tmp_path), "--write", str(out)])
    printed = capsys.readouterr().out
    assert "hop-1 -> fake-upstream: 40 pairs" in printed and "crossing hop-1" in printed
    joins = tp.params_for(out, {"agent": "obi", "concurrency": 100})
    assert set(joins) == {"hop-1 -> fake-upstream", "load-gen -> hop-1"}
    params, receiver_start = joins["hop-1 -> fake-upstream"]
    assert receiver_start == "start" and params.signals[0].window_us >= 1
    j = tp.analyze_cell(tmp_path / "n1", out)["joins"]["hop-1 -> fake-upstream"]
    assert j["correct"] == 40 and j["wrong"] == 0


def test_containment_is_the_share_of_downstream_intervals_inside_the_upstream_one():
    inside, outside = pair(10, 10), pair(-5, 10)
    assert cal.contained([inside, outside]) == 0.5
    assert cal.contained([]) == 0.0
