"""stream_path.py --attach (phase 4, 4c (b)): agent spans linked through the probe.

A synthetic one-hop cell: the agent's spans (wall clock, µs), stream-probe's
events (monotonic clock, ns, with clock records to convert), fake-upstream's
truth. Request k leaves at T0 + k * spacing on connection k % conns.
"""

import json
import sys
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))

import stream_path as sp  # noqa: E402

FU_PORT, HOP_PORT = 4000, 5000
T0 = 1_792_000_000_000_000  # µs since the epoch
OFF = 1_790_000_000_000_000_000  # ns: wall - monotonic
HELLO, FINISHED, REQUEST = 300, 64, 392
CLIENT_GAP, SERVER_GAP = 5, 3  # µs: the agent dates a message just after the probe


def truth(k):
    return f"{k:032x}"


def span(kind, k, start, peer_port, host_port, handler=None, **attrs):
    return {
        "kind": kind,
        "traceID": "f" * 32,
        "peer": "127.0.0.1",
        "peerPort": str(peer_port),
        "host": "127.0.0.1",
        "hostPort": str(host_port),
        "start": str(start),
        "handlerStart": str(start if handler is None else handler),
        "end": str(start + 4_000),
        "attributes": {"url": f"/truth-{truth(k)}", **attrs},
    }


def write_cell(
    tmp_path,
    n,
    conns,
    spacing=10_000,
    server_delay=None,
    extra_send=(),
    no_send=(),
    clocks="one",
    handler_gap=0,
    duplicate_span=(),
):
    """``server_delay``: {k: µs} the server side of request k is late by (a
    freeze: span and read alike). ``extra_send``: requests with a second send
    2 µs before theirs. ``no_send``: requests whose send the probe missed.
    ``clocks``: "one", "none", or "step" (the wall clock jumps 1 s forward
    halfway, the agent's later spans with it, and a clock record says so).
    ``duplicate_span``: requests whose client span the agent reports twice."""
    server_delay = server_delay or {}
    cell = tmp_path / "n1"
    cell.mkdir(parents=True)
    (cell / "cell.json").write_text(
        json.dumps(
            {"hops": 1, "fake_upstream_port": FU_PORT, "hop_ports": [HOP_PORT], "agent": "obi"}
        )
    )
    (tmp_path / "cells.txt").write_text(f"n1: hops=1 agent=obi requests={n} c={conns} load=0\n")
    half = T0 + (n // 2) * spacing  # µs: where a clock step happens

    def mono(t_us):  # the kernel's monotonic clock never jumps
        return t_us * 1000 - OFF

    def wall(t_us):  # the wall clock the agent reads: 1 s ahead after a step
        return t_us + (1_000_000 if clocks == "step" and t_us >= half else 0)

    probe, lg, fu, spans = [], [], [], []

    def ev(kind, port, seq, length, wall_us):
        probe.append(
            {
                "kind": kind,
                "client_addr": "127.0.0.1",
                "client_port": port,
                "server_addr": "127.0.0.1",
                "server_port": FU_PORT,
                "seq": seq % 2**32,
                "len": length,
                "timestamp_ns": mono(wall_us),
            }
        )

    for c in range(conns):
        ks = [k for k in range(n) if k % conns == c]
        port, base = 40_000 + c, 10_000 + 7_919 * c
        t = T0 + ks[0] * spacing - 1_000 if ks else T0
        ev("connect", port, base, 0, t - 500)
        ev("accept", port, base, 0, t - 490)
        ev("send", port, base, HELLO, t - 400)
        ev("recv", port, base, HELLO, t - 390)
        ev("send", port, base + HELLO, FINISHED, t - 300)
        ev("recv", port, base + HELLO, FINISHED, t - 290)
        for i, k in enumerate(ks):
            w = T0 + k * spacing
            off = HELLO + FINISHED + i * REQUEST
            delay = server_delay.get(k, 0)
            lg.append(
                {
                    "trace_id": truth(k),
                    "timestamp_ns": w * 1000,
                    "duration_ns": 5_000_000,
                    "negotiated_protocol": "http/1.1",
                    "conn_key": f"127.0.0.1:{30_000 + c}-127.0.0.1:{HOP_PORT}|0",
                }
            )
            client_start = w + 200
            server_start = w + 300 + delay
            spans.append(
                span("SPAN_KIND_CLIENT", k, wall(client_start), port, FU_PORT, clientAddr="hop-1")
            )
            if k in duplicate_span:
                spans.append(
                    span(
                        "SPAN_KIND_CLIENT",
                        k,
                        wall(client_start) + 1,
                        port,
                        FU_PORT,
                        clientAddr="hop-1",
                    )
                )
            spans.append(
                span(
                    "SPAN_KIND_SERVER",
                    k,
                    wall(server_start),
                    port,
                    FU_PORT,
                    handler=wall(server_start) + handler_gap,
                )
            )
            if k not in no_send:
                ev("send", port, base + off, REQUEST, client_start - CLIENT_GAP)
            if k in extra_send:
                ev("send", port, base + off + 1, 1, client_start - CLIENT_GAP - 2)
            ev("recv", port, base + off, REQUEST, server_start - SERVER_GAP)
            fu.append(
                {
                    "trace_id": truth(k),
                    "protocol": "HTTP/1.1",
                    "conn_key": f"127.0.0.1:{port}-127.0.0.1:{FU_PORT}|0",
                    "stream_offset": off,
                }
            )
    if clocks != "none":
        for w in (T0 - 10_000, half - 1, half, T0 + n * spacing + 10_000):
            if clocks == "one" and w in (half - 1, half):
                continue
            probe.append(
                {
                    "kind": "clock",
                    "timestamp_ns": mono(w),
                    "realtime_ns": wall(w) * 1000,
                    "spread_ns": 200,
                }
            )
    probe.sort(key=lambda r: r["timestamp_ns"])
    (cell / "probe.jsonl").write_text("".join(json.dumps(r) + "\n" for r in probe))
    (cell / "lg.jsonl").write_text("".join(json.dumps(r) + "\n" for r in lg))
    (cell / "fu.jsonl").write_text("".join(json.dumps(r) + "\n" for r in fu))
    (cell / "agent.out").write_text("level=INFO msg=start\n" + json.dumps(spans) + "\n")
    return cell


def attach(cell, **kw):
    return sp.attach_cell(
        cell,
        window_us=kw.pop("window_us", 1_000_000),
        attach_window_us=kw.pop("attach_window_us", 20),
        **kw,
    )


def test_spans_are_linked_through_the_probe_and_judged_by_their_own_truth(tmp_path):
    r = attach(write_cell(tmp_path, 10, conns=5))
    assert (r["links"], r["correct"], r["wrong"], r["both_seen"]) == (10, 10, 0, 10)
    assert r["coverage"] == 1.0
    assert r["attached_client"] == r["attached_server"] == 10
    assert r["gap_client_us"]["max"] == CLIENT_GAP and r["gap_server_us"]["max"] == SERVER_GAP


def test_a_freeze_on_the_server_side_delays_span_and_read_alike(tmp_path):
    # 32 to 41 ms late on the server (requests on one connection are 50 ms apart):
    # far outside any window between the two processes,
    # but the span and the probe's read of the same message are late together.
    r = attach(write_cell(tmp_path, 10, conns=5, server_delay={3: 41_000, 7: 32_000}))
    assert (r["correct"], r["wrong"]) == (10, 0)
    assert r["gap_server_us"]["max"] == SERVER_GAP


def test_two_probe_events_in_the_window_leave_the_span_unattached(tmp_path):
    r = attach(write_cell(tmp_path, 10, conns=5, extra_send={4}))
    assert (r["correct"], r["wrong"]) == (9, 0)
    assert r["ambiguous_client"] == 1


def test_a_send_the_probe_missed_leaves_the_span_without_candidate(tmp_path):
    r = attach(write_cell(tmp_path, 10, conns=5, no_send={2}))
    assert (r["correct"], r["wrong"], r["no_candidate_client"]) == (9, 0, 1)
    assert r["coverage"] == pytest.approx(0.9)


def test_two_spans_claiming_one_probe_event_are_both_left_unattached(tmp_path):
    r = attach(write_cell(tmp_path, 10, conns=5, duplicate_span={6}))
    assert (r["correct"], r["wrong"], r["ambiguous_client"]) == (9, 0, 2)


def test_k2b_independent_losses_on_spans_and_probe_events_link_nothing_wrong(tmp_path):
    r = attach(write_cell(tmp_path, 1000, conns=10, spacing=1_000), drop=50, seed=3)
    assert r["wrong"] == 0
    assert 0.7 <= r["coverage"] <= 0.9  # four independent 5 % losses on each request's path
    assert r["control"] == "K2b drop=50 permille each side seed=3"


def test_k5_reads_shifted_by_one_message_give_wrong_span_links(tmp_path):
    r = attach(write_cell(tmp_path, 40, conns=4), shift=True)
    assert r["wrong"] > 0  # a probe off by one message on one side is not caught
    assert r["control"] == "K5 reads shifted by one message"


def test_without_a_clock_record_nothing_is_attached(tmp_path):
    r = attach(write_cell(tmp_path, 4, conns=2, clocks="none"))
    assert r["error"] == "no clock record in probe.jsonl: cannot convert to the agents' clock"


def test_the_nearest_clock_record_is_used_across_a_wall_clock_step(tmp_path):
    r = attach(write_cell(tmp_path, 10, conns=2, clocks="step"))
    assert (r["correct"], r["wrong"]) == (10, 0)
    assert r["clock_records"] == 4


def test_the_server_start_and_each_sides_offset_are_settings(tmp_path):
    cell = write_cell(tmp_path, 6, conns=3, handler_gap=40)
    assert attach(cell, server_start="handlerStart")["attached_server"] == 0
    r = attach(cell, server_start="handlerStart", server_offset_us=40 + SERVER_GAP)
    assert (r["attached_server"], r["correct"]) == (6, 6)
    with pytest.raises(ValueError, match="server_start"):
        attach(cell, server_start="end")


def test_main_with_attach_prints_the_span_links(tmp_path, capsys):
    write_cell(tmp_path, 6, conns=3)
    sp.main(["stream_path.py", str(tmp_path), "--attach", "--attach-window-us", "20"])
    out = capsys.readouterr().out
    assert "n1 spans: links 6 correct 6 wrong 0 both_seen 6 coverage 100.00%" in out
    assert "attached client 6 server 6" in out
    assert "wrong span links in the run: 0" in out
