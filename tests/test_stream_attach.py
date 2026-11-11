"""stream_path.py --attach (phase 4, 4c (b)): agent spans linked through the probe.

A synthetic one-hop cell: the agent's spans (wall clock, µs), stream-probe's
events (monotonic clock, ns, with clock records to convert), fake-upstream's
truth. Request k leaves at T0 + k * spacing on connection k % conns.
"""

import json
import sys
import tomllib
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
    load=0,
    agent="obi",
    client_shift=None,
    client_end=None,
    extra_message=(),
    drops=(),
    send_delay=None,
):
    """``server_delay``: {k: µs} the server side of request k is late by (a
    freeze: span and read alike). ``extra_send``: requests with a second send
    2 µs before theirs. ``no_send``: requests whose send the probe missed.
    ``clocks``: "one", "none", or "step" (the wall clock jumps 1 s forward
    halfway, the agent's later spans with it, and a clock record says so).
    ``duplicate_span``: requests whose client span the agent reports twice.
    ``client_shift``: {k: µs} the client span of request k starts that much
    earlier or later (waiting for its connection). ``client_end``: {k: µs
    after T0} where the client span of request k ends. ``extra_message``:
    requests preceded, 2 µs before, by a whole message seen at both ends that
    no span reports. ``drops``: (µs after T0, count) records of the probe's
    declared losses. ``send_delay``: {k: µs} hop-1 frozen: request k's send,
    and its read and server span after it, that much later; its client span
    starts on time (it ends 4 ms after the send)."""
    server_delay = server_delay or {}
    client_shift = client_shift or {}
    client_end = client_end or {}
    send_delay = send_delay or {}
    cell = tmp_path / "n1"
    cell.mkdir(parents=True)
    (cell / "cell.json").write_text(
        json.dumps(
            {"hops": 1, "fake_upstream_port": FU_PORT, "hop_ports": [HOP_PORT], "agent": "obi"}
        )
    )
    (tmp_path / "cells.txt").write_text(
        f"n1: hops=1 agent={agent} requests={n} c={conns} load={load}\n"
    )
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
            frozen = send_delay.get(k, 0)
            delay = server_delay.get(k, 0) + frozen
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
            client_span = span(
                "SPAN_KIND_CLIENT",
                k,
                wall(client_start + client_shift.get(k, 0)),
                port,
                FU_PORT,
                clientAddr="hop-1",
            )
            if k in client_end:
                client_span["end"] = str(wall(T0 + client_end[k]))
            elif frozen:
                client_span["end"] = str(wall(client_start + frozen + 4_000))
            spans.append(client_span)
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
                ev("send", port, base + off, REQUEST, client_start + frozen - CLIENT_GAP)
            if k in extra_send:  # a piece of a write: no read starts there
                ev("send", port, base + off + 1, 1, client_start - CLIENT_GAP - 2)
            if k in extra_message:  # a whole message, seen at both ends
                ev("send", port, base + 3_000_000 + off, 10, client_start - CLIENT_GAP - 2)
                ev("recv", port, base + 3_000_000 + off, 10, server_start - SERVER_GAP - 2)
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
    for at_us, count in drops:
        probe.append({"kind": "drops", "timestamp_ns": mono(T0 + at_us), "drops": count})
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


def test_two_messages_in_the_window_leave_the_span_unattached(tmp_path):
    r = attach(write_cell(tmp_path, 10, conns=5, extra_message={4}))
    assert (r["correct"], r["wrong"]) == (9, 0)
    assert r["ambiguous_client"] == 1


def test_a_piece_of_a_write_is_never_a_candidate(tmp_path):
    # no read starts at its number: it cannot lead to a link, so it does not compete
    r = attach(write_cell(tmp_path, 10, conns=5, extra_send={4}))
    assert (r["correct"], r["wrong"], r["ambiguous_client"]) == (10, 0, 0)


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


# -- with 4a' ---------------------------------------------------------------------


def params_file(tmp_path, concurrency, receiver_start="start", offset_us=100):
    """4a' parameters: server start offset_us after the client's, 50 µs either way."""
    path = tmp_path / f"params-{receiver_start}-{offset_us}.toml"
    path.write_text(
        f"""[obi.c{concurrency}."hop-1 -> fake-upstream"]
receiver_start = "{receiver_start}"
margin_us = 0
local_span_us = 20000
signals = [{{ name = "start", offset_us = {offset_us}, window_us = 50 }}]
"""
    )
    return path


def test_compared_with_4a_prime_the_probe_links_what_a_freeze_pushed_out(tmp_path):
    cell = write_cell(tmp_path, 10, conns=5, server_delay={3: 41_000, 7: 32_000})
    r = attach(cell, compare=params_file(tmp_path, 5))["compare"]
    assert r["4a"] == {"links": 8, "correct": 8, "wrong": 0}
    assert (r["both"], r["only_4a"], r["only_4c"], r["conflicts"]) == (8, 0, 2, 0)
    assert r["combined"] == {"links": 10, "correct": 10, "wrong": 0, "coverage": 1.0}


def test_links_the_two_paths_disagree_on_are_all_refused(tmp_path):
    cell = write_cell(tmp_path, 40, conns=4)
    r = attach(cell, compare=params_file(tmp_path, 4), shift=True)
    assert r["wrong"] > 0  # K5 on the probe path alone
    c = r["compare"]
    assert c["conflicts"] > 0
    assert c["combined"]["wrong"] == 0  # every wrong probe link contradicts a 4a' link
    assert (
        c["combined"]["correct"] < 40
    )  # and takes the right one down with it: refused, not chosen


def test_main_prints_the_comparison(tmp_path, capsys):
    write_cell(tmp_path, 6, conns=3)
    params = params_file(tmp_path, 3)
    sp.main(
        [
            "stream_path.py",
            str(tmp_path),
            "--attach",
            "--attach-window-us",
            "20",
            "--compare",
            str(params),
        ]
    )
    out = capsys.readouterr().out
    assert "with 4a': both 6 only 4a' 0 only probe 0 conflicts 0" in out
    assert "combined: links 6 correct 6 wrong 0 coverage 100.00%" in out


def test_4a_prime_uses_the_receiver_start_it_was_calibrated_on(tmp_path):
    cell = write_cell(tmp_path, 6, conns=3, handler_gap=200)
    on_start = attach(cell, compare=params_file(tmp_path, 3, "start", 300))["compare"]["4a"]
    on_handler = attach(cell, compare=params_file(tmp_path, 3, "handlerStart", 300))["compare"][
        "4a"
    ]
    assert (on_start["correct"], on_handler["correct"]) == (0, 6)


@pytest.mark.parametrize(
    "a, b, kept",
    [
        ({("c1", "s1")}, {("c1", "s1")}, {("c1", "s1")}),  # confirmed
        ({("c1", "s1")}, set(), {("c1", "s1")}),  # one path only: kept
        ({("c1", "s1")}, {("c1", "s2")}, set()),  # one client, two servers
        ({("c1", "s1")}, {("c2", "s1")}, set()),  # two clients, one server
        ({("c1", "s1"), ("c3", "s3")}, {("c2", "s1")}, {("c3", "s3")}),  # others untouched
    ],
)
def test_the_combination_refuses_every_link_of_a_span_given_two_partners(a, b, kept):
    assert sp._combine(a, b) == kept


def test_gaps_are_reported_on_both_tails():
    # the client side reads leftwards (span before the send): its low tail matters
    g = sp._gaps(list(range(-100, 0)))
    assert g == {"min": -100, "p1": -99, "p5": -95, "median": -50, "p95": -5, "p99": -1, "max": -1}
    assert sp._gaps([]) == dict.fromkeys(("min", "p1", "p5", "median", "p95", "p99", "max"))


# -- calibration of the attach windows ---------------------------------------------


def test_each_bound_is_widened_by_a_fifth_of_its_own_distance_to_the_median():
    gaps = list(range(-2_000, 0))  # 2,000 values, -2000 .. -1
    got = sp.attach_interval(gaps)
    # p0.1 = gaps[2] = -1998, median = gaps[1000] = -1000, p99.9 = gaps[1998] = -2
    assert (got["p0_1"], got["median"], got["p99_9"]) == (-1998, -1000, -2)
    assert (got["low_us"], got["high_us"]) == (-2198, 198)  # 998 / 5 each way, rounded outwards
    assert (got["offset_us"], got["window_us"]) == (-1000, 1198)
    assert got["samples"] == 2000


def test_a_tight_side_stays_tight():
    # client side: the span always starts before the send, a long tail to the left only
    gaps = [-60] * 900 + [-100] * 90 + [-30_000] * 10
    got = sp.attach_interval(gaps)
    assert got["high_us"] == -60  # p99.9 = median = -60: nothing added on the tight side
    assert got["low_us"] == -35_988  # -30000 - (30000 - 60) / 5


def test_calibration_writes_windows_by_agent_regime_and_side(tmp_path):
    calm = write_cell(tmp_path / "calm", 50, conns=5)
    loaded = write_cell(tmp_path / "loaded", 50, conns=5, load=4, server_delay={3: 900})
    out = tmp_path / "attach-params.toml"
    sp.calibrate_attach([calm, loaded], out, wide_us=50_000)
    params = tomllib.loads(out.read_text())
    assert set(params["obi"]) == {"calm", "load"}
    calm_client = params["obi"]["calm"]["client"]
    assert (
        calm_client["samples"] == 50 and calm_client["p0_1"] == calm_client["p99_9"] == CLIENT_GAP
    )
    assert (
        params["obi"]["load"]["server"]["p99_9"] == SERVER_GAP
    )  # the delayed span is late with its read
    assert "each bound widened by 20 % of its distance to the median" in params["rule"]


def test_attach_reads_its_windows_per_cell_from_the_calibration(tmp_path):
    cell = write_cell(tmp_path, 10, conns=5)
    params = tmp_path / "attach-params.toml"
    params.write_text(
        """rule = "test"
[obi.calm.client]
passes = [[4, 6]]
[obi.calm.server]
passes = [[99, 101]]
"""
    )
    r = sp.attach_cell(cell, window_us=1_000_000, attach_params=params)
    assert (r["attached_client"], r["attached_server"], r["correct"]) == (10, 0, 0)
    assert r["windows"] == {"client": [(4, 6)], "server": [(99, 101)]}


def test_attach_params_without_this_agent_and_regime_are_refused(tmp_path):
    cell = write_cell(tmp_path, 4, conns=2, load=4)
    params = tmp_path / "attach-params.toml"
    params.write_text('rule = "test"\n[obi.calm.client]\npasses = [[0, 1]]\n')
    with pytest.raises(ValueError, match=r"obi\.load"):
        sp.attach_cell(cell, window_us=1_000_000, attach_params=params)


def test_compare_under_load_uses_4a_prime_in_stress_mode(tmp_path):
    cell = write_cell(tmp_path, 10, conns=5, load=4)
    with pytest.raises(ValueError, match="load"):
        attach(cell, compare=params_file(tmp_path, 5))
    r = attach(cell, compare=params_file(tmp_path, 5), stress=True)["compare"]
    assert r["4a"]["correct"] == 10 and r["stress"] is True


# -- successive passes, order between anchors, declared losses ----------------------

NARROW = (0, 10)  # gap span - probe, µs: the fixture's agent dates just after the probe


def passes(*client, server=(NARROW,)):
    return {"client": list(client), "server": list(server)}


def test_a_later_wider_pass_attaches_what_the_narrow_one_left(tmp_path):
    cell = write_cell(tmp_path, 10, conns=5, client_shift={9: -3_000})
    # dynamic=False: the subject is the calibrated passes (spec 8.8.4 apart)
    assert sp.attach_cell(cell, 1_000_000, passes=passes(NARROW), dynamic=False)["correct"] == 9
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, (-4_000, 10)), dynamic=False)
    assert (r["correct"], r["wrong"]) == (10, 0)


def test_events_claimed_in_a_narrow_pass_no_longer_compete(tmp_path):
    # request 7 (second on connection 2) waits 45 ms; in one wide window it sees
    # its own send and the next one on its connection, claimed narrowly first
    cell = write_cell(tmp_path, 20, conns=5, client_shift={7: -45_000})
    wide = (-100_000, 10)
    alone = sp.attach_cell(cell, 1_000_000, passes=passes(wide))
    stepped = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, wide))
    assert alone["correct"] < 20 and alone["wrong"] == 0
    assert (stepped["correct"], stepped["wrong"]) == (20, 0)


def waiting(tmp_path, **kw):
    """One connection, requests 10 ms apart; the spans of requests 1 and 2
    start long before their sends (both waiting for the connection)."""
    return write_cell(tmp_path, 4, conns=1, client_shift={1: -9_000, 2: -18_000}, **kw)


WIDE = (-20_000, 10)


def test_between_two_anchors_the_leftovers_are_paired_in_order(tmp_path):
    cell = waiting(tmp_path)
    assert sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE))["ambiguous_client"] == 2
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE), order=True)
    assert (r["correct"], r["wrong"], r["ordered_client"]) == (4, 0, 2)


def test_no_order_when_the_spans_start_and_end_in_different_orders(tmp_path):
    # request 2's span ends first: the connection may have been handed out of order
    cell = waiting(tmp_path, client_end={1: 30_000, 2: 25_000})
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE), order=True)
    assert (r["correct"], r["wrong"], r["ordered_client"]) == (2, 0, 0)


def test_no_order_when_a_span_is_missing_from_the_segment(tmp_path):
    # a whole message nobody reports: as many events, one span fewer
    cell = waiting(tmp_path, extra_message={2})
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE), order=True)
    assert (r["wrong"], r["ordered_client"]) == (0, 0)


def test_where_the_probe_declares_a_loss_only_the_narrow_pass_applies(tmp_path):
    # a loss declared in the second around requests 1 and 2 (drops 0 -> 3)
    cell = waiting(tmp_path, drops=[(-1_000_000, 0), (1_000_000, 3), (2_000_000, 3)])
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE), order=True)
    assert (r["correct"], r["wrong"], r["ordered_client"]) == (2, 0, 0)
    assert r["lossy_intervals"] == 1


def test_k2b_declares_the_probes_losses_like_the_probe(tmp_path):
    cell = write_cell(tmp_path, 1000, conns=10, spacing=1_000)
    r = sp.attach_cell(
        cell, 1_000_000, passes=passes(NARROW, (-5_000, 10)), order=True, drop=50, seed=3
    )
    assert r["wrong"] == 0 and r["lossy_intervals"] > 0


def test_calibration_writes_one_pass_per_level(tmp_path):
    cell = write_cell(tmp_path / "calm", 50, conns=5)
    out = tmp_path / "attach-params.toml"
    sp.calibrate_attach([cell], out, wide_us=50_000)
    params = tomllib.loads(out.read_text())
    client = params["obi"]["calm"]["client"]
    assert len(client["passes"]) == len(sp.LEVELS) == 4
    assert params["levels"] == ["p25-p75", "p5-p95", "p1-p99", "p0.1-p99.9"]


def test_a_declared_loss_stops_the_wider_passes_alone(tmp_path):
    cell = write_cell(tmp_path, 10, conns=5, client_shift={9: -3_000}, drops=[(0, 0), (100_000, 1)])
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, (-4_000, 10)))
    assert (r["correct"], r["wrong"]) == (9, 0)


def test_a_declared_loss_inside_the_segment_stops_the_order(tmp_path):
    # the loss is between the two sends (12 to 18 ms), far from both spans' windows
    cell = waiting(tmp_path, drops=[(12_000, 0), (18_000, 3)])
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW), order=True)
    assert (r["wrong"], r["ordered_client"]) == (0, 0)
    clean = sp.attach_cell(
        waiting(tmp_path / "clean"), 1_000_000, passes=passes(NARROW), order=True
    )
    assert (clean["correct"], clean["ordered_client"]) == (4, 2)


def test_a_span_out_of_every_window_does_not_lend_its_send_to_the_next_one(tmp_path):
    # Spec 8.8.2-8.8.3. One connection, sends 30 ms apart (at 30 k ms + 0.195).
    # Request 1's span, 5.2 to 31 ms, starts 25 ms before its send: out of
    # every window, unattached. Request 2's span starts at 29 ms, waiting for
    # the connection: its wide window holds request 1's send, not its own;
    # alone there, it would be taken. Request 1 could have sent it: refused.
    cell = write_cell(
        tmp_path,
        4,
        conns=1,
        spacing=30_000,
        client_shift={1: -25_000, 2: -31_200},
        client_end={1: 31_000, 2: 61_000},
    )
    for order in (False, True):
        r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE), order=order)
        assert r["wrong"] == 0
        assert r["correct"] == 2  # requests 0 and 3, from the narrow pass


def test_a_span_out_of_every_window_refuses_nothing_outside_its_life(tmp_path):
    # Request 1's span is out of every window, open 5.2 to 35 ms. Request 2's
    # span starts 9 ms early and takes its own send (60.2 ms) in the wide pass:
    # request 1 could not have sent it, kept.
    cell = write_cell(
        tmp_path,
        4,
        conns=1,
        spacing=30_000,
        client_shift={1: -25_000, 2: -9_000},
        client_end={1: 35_000, 2: 61_000},
    )
    # dynamic=False: the subject is the guard (spec 8.8.4 would attach request 1)
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE), dynamic=False)
    assert (r["correct"], r["wrong"]) == (3, 0)


def test_the_guard_runs_until_nothing_changes(tmp_path):
    # A chain: request 1 out of every window; request 2 takes request 1's send,
    # request 3 takes request 2's. Refusing request 2 makes it a possible owner
    # of what request 3 took: refused too.
    cell = write_cell(
        tmp_path,
        5,
        conns=1,
        spacing=30_000,
        client_shift={1: -25_000, 2: -31_200, 3: -31_200},
        client_end={1: 31_000, 2: 61_000, 3: 91_000},
    )
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE))
    assert (r["correct"], r["wrong"]) == (2, 0)


def test_a_free_message_elsewhere_does_not_hide_a_taken_send(tmp_path):
    # The TLS opening messages of a connection are seen at both ends and no span
    # claims them: free events before the first request. Request 0's span,
    # -24.8 to 1 ms, is out of every window. Request 1's span starts at -1 ms,
    # waiting for the connection: in the wide pass it takes request 0's send
    # (0.195 ms). "Is a free event left before request 1's?" says yes (the
    # opening); but request 0, open over that send, could have sent it: refused.
    cell = write_cell(
        tmp_path,
        4,
        conns=1,
        spacing=30_000,
        client_shift={0: -25_000, 1: -31_200},
        client_end={0: 1_000, 1: 31_000},
    )
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, WIDE))
    assert (r["correct"], r["wrong"]) == (2, 0)


# -- dynamic zone under freeze (spec 8.8.4) ----------------------------------------

DYN = (-4_000, 10)  # the widest calibrated pass in these tests


def dense(tmp_path, **kw):
    """200 requests 0.5 ms apart over 40 connections: a send every 0.5 ms, so
    no hole of more than 1 ms in the send stream unless a freeze makes one."""
    return write_cell(tmp_path, 200, conns=40, spacing=500, **kw)


def test_a_span_waiting_for_its_connection_is_placed_from_its_release(tmp_path):
    # Request 100 (connection 20) starts 15 ms early; request 60 holds the
    # connection until 50 ms; request 100 sends at 50.195 ms. From the span
    # start: out of every window; from the release: 195 µs.
    cell = dense(tmp_path, client_shift={100: -15_000}, client_end={60: 50_000, 100: 55_000})
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, DYN))
    assert (r["correct"], r["wrong"]) == (200, 0)


def test_a_span_whose_send_a_freeze_delayed_is_placed_net_of_the_frozen_time(tmp_path):
    # hop-1 frozen from 60 to 68 ms: requests 120-135 send after it, in order;
    # their spans started on time, up to 8 ms before their sends.
    delays = {k: 68_000 - k * 500 + (k - 120) * 20 for k in range(120, 136)}
    cell = dense(tmp_path, send_delay=delays)
    before = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, DYN), dynamic=False)
    assert before["correct"] < 200 and before["wrong"] == 0
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, DYN))
    assert (r["correct"], r["wrong"]) == (200, 0)


def test_two_candidates_in_the_dynamic_zone_attach_nothing(tmp_path):
    # As the first test, with a whole message nobody reports 2 µs before
    # request 100's send: two candidates.
    cell = dense(
        tmp_path,
        client_shift={100: -15_000},
        client_end={60: 50_000, 100: 55_000},
        extra_message={100},
    )
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, DYN))
    assert (r["correct"], r["wrong"]) == (199, 0)


def test_the_dynamic_zone_leaves_an_event_another_unattached_span_could_have_sent(tmp_path):
    # As on the bench: a swap. Connection 20: request 20 holds it until 27 ms;
    # request 100's span (22 to 55 ms) is placed from 27 ms and its only
    # candidate is request 60's send (30.195 ms); request 60's span (24 to
    # 31 ms) is out of every window but open over that send. Taken, the order
    # would then give request 60 the send of request 100: two wrong links.
    cell = dense(
        tmp_path,
        client_shift={100: -28_200, 60: -6_200},
        client_end={20: 27_000, 100: 55_000, 60: 31_000},
    )
    r = sp.attach_cell(cell, 1_000_000, passes=passes(NARROW, DYN), order=True)
    assert (r["correct"], r["wrong"]) == (198, 0)
