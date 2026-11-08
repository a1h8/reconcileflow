"""Harness tool for the TCP stream path (phase 4, 4c (a)): probe events, truth, controls.

A synthetic cell: stream-probe's probe.jsonl (already decoded: connect and
accept carry the stream's base) and fake-upstream's fu.jsonl with
stream_offset, the truth measured apart from the probe.
"""

import json
import sys
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))

import stream_path as sp  # noqa: E402

FU_PORT = 4000
HELLO, FINISHED, REQUEST = 300, 64, 392  # bytes the client writes
T0 = 1_000_000_000  # ns, monotonic


def truth(k):
    return f"{k:032x}"


def write_cell(
    tmp_path,
    n,
    conns,
    base=1_000,
    spacing_ns=1_000_000,
    gap_ns=50_000,
    coalesce=(),
    late=(),
    reuse=False,
    duplicate=(),
    drops=0,
):
    """n requests spread over ``conns`` connections, request k on connection
    k % conns. Each connection: ClientHello, Finished, then its requests,
    each written by the client and read by the server ``gap_ns`` later.
    ``coalesce``: connections whose server reads Finished and the first
    request at once. ``late``: requests read 10 s after being sent.
    ``reuse``: connection 1 reuses connection 0's four-tuple, later.
    ``duplicate``: requests whose read is reported twice."""
    cell = tmp_path / "n1"
    cell.mkdir(parents=True)
    (cell / "cell.json").write_text(json.dumps({"hops": 1, "fake_upstream_port": FU_PORT}))
    probe, fu = [], []

    def ev(kind, port, seq, length, t):
        probe.append(
            {
                "kind": kind,
                "client_addr": "127.0.0.1",
                "client_port": port,
                "server_addr": "127.0.0.1",
                "server_port": FU_PORT,
                "seq": seq % 2**32,
                "len": length,
                "timestamp_ns": t,
            }
        )

    per_conn = {c: [k for k in range(n) if k % conns == c] for c in range(conns)}
    for c, ks in per_conn.items():
        port = 30_000 if reuse and c == 1 else 30_000 + c
        b = base + 7_919 * c
        t = T0 + c * 100_000 + (10**9 if reuse and c == 1 else 0)
        ev("connect", port, b, 0, t)
        ev("accept", port, b, 0, t + 10_000)
        ev("send", port, b, HELLO, t + 20_000)
        ev("recv", port, b, HELLO, t + 30_000)
        ev("send", port, b + HELLO, FINISHED, t + 40_000)
        if c not in coalesce:
            ev("recv", port, b + HELLO, FINISHED, t + 50_000)
        gen = 1 if reuse and c == 1 else 0
        for i, k in enumerate(ks):
            off = HELLO + FINISHED + i * REQUEST
            ts = t + 100_000 + i * spacing_ns
            ev("send", port, b + off, REQUEST, ts)
            read_at = ts + (10**10 if k in late else gap_ns)
            if c in coalesce and i == 0:
                ev("recv", port, b + HELLO, FINISHED + REQUEST, read_at)
            else:
                ev("recv", port, b + off, REQUEST, read_at)
                if k in duplicate:
                    ev("recv", port, b + off, REQUEST, read_at + 1)
            fu.append(
                {
                    "trace_id": truth(k),
                    "path": f"/truth-{truth(k)}",
                    "conn_key": f"127.0.0.1:{port}-127.0.0.1:{FU_PORT}|{gen}",
                    "stream_offset": off,
                }
            )
    probe.sort(key=lambda r: r["timestamp_ns"])
    probe.append({"kind": "drops", "seq": 0, "len": 0, "timestamp_ns": T0 + 10**11, "drops": drops})
    (cell / "probe.jsonl").write_text("".join(json.dumps(r) + "\n" for r in probe))
    (cell / "fu.jsonl").write_text("".join(json.dumps(r) + "\n" for r in fu))
    return cell


def analyze(cell, **kw):
    return sp.analyze_cell(cell, window_us=kw.pop("window_us", 1_000_000), **kw)


# -- reading ---------------------------------------------------------------------


def test_offsets_count_from_each_connections_base_at_both_ends(tmp_path):
    cell = write_cell(tmp_path, 4, conns=2)
    events = sp.probe_events(cell / "probe.jsonl")
    sends = sorted(e.offset for e in events if e.kind == "send" and e.port == 30_001)
    recvs = sorted(e.offset for e in events if e.kind == "recv" and e.port == 30_001)
    assert sends == recvs == [0, HELLO, HELLO + FINISHED, HELLO + FINISHED + REQUEST]


def test_offsets_survive_the_sequence_number_wrapping(tmp_path):
    cell = write_cell(tmp_path, 3, conns=1, base=2**32 - 500)
    report = analyze(cell)
    assert (report["correct"], report["wrong"], report["requests"]) == (3, 0, 3)


def test_an_event_before_its_connections_base_has_no_offset(tmp_path):
    cell = write_cell(tmp_path, 2, conns=1)
    lines = (cell / "probe.jsonl").read_text().splitlines()
    stray = json.loads(lines[2]) | {"client_port": 39_999, "timestamp_ns": T0 - 1}
    (cell / "probe.jsonl").write_text("\n".join([json.dumps(stray), *lines]) + "\n")
    assert analyze(cell)["no_base"] == 1


# -- linking and scoring ----------------------------------------------------------


def test_every_request_is_linked_to_its_own_read_and_the_server_agrees(tmp_path):
    report = analyze(write_cell(tmp_path, 30, conns=3))
    assert (report["links"], report["correct"], report["wrong"]) == (30, 30, 0)
    assert report["coverage"] == 1.0
    assert report["server_offset_in_send"] == report["server_offset_in_recv"] == 30
    assert report["handshake_links"] == 6  # ClientHello and Finished, per connection


def test_a_request_read_with_the_handshake_end_is_a_fragment_never_linked(tmp_path):
    report = analyze(write_cell(tmp_path, 9, conns=3, coalesce={1}))
    assert (report["correct"], report["wrong"]) == (8, 0)
    assert report["server_offset_in_recv"] == 8
    assert report["fragments"] == 1  # the read that starts at Finished
    assert report["coverage"] == pytest.approx(8 / 9)


def test_a_read_reported_twice_counts_once(tmp_path):
    report = analyze(write_cell(tmp_path, 6, conns=2, duplicate={3}))
    assert (report["correct"], report["wrong"], report["recv_duplicates"]) == (6, 0, 1)


def test_a_reused_four_tuple_keeps_its_connections_apart(tmp_path):
    report = analyze(write_cell(tmp_path, 10, conns=2, reuse=True, base=5_000))
    assert (report["correct"], report["wrong"]) == (10, 0)
    assert report["server_offset_in_send"] == report["server_offset_in_recv"] == 10


def test_the_absolute_number_not_the_offset_keeps_reused_four_tuples_apart(tmp_path):
    # Both connections carry the same offsets; with a window spanning both, only
    # the absolute number (a fresh initial number per connection) tells them apart.
    report = analyze(write_cell(tmp_path, 10, conns=2, reuse=True), window_us=5_000_000)
    assert (report["correct"], report["wrong"], report["ambiguous"]) == (10, 0, 0)


def test_events_out_of_time_order_in_the_file_are_read_in_time_order(tmp_path):
    cell = write_cell(tmp_path, 6, conns=2)
    lines = (cell / "probe.jsonl").read_text().splitlines()
    (cell / "probe.jsonl").write_text("\n".join(reversed(lines)) + "\n")
    report = analyze(cell)
    assert (report["correct"], report["no_base"]) == (6, 0)


def test_time_still_has_to_fit(tmp_path):
    report = analyze(write_cell(tmp_path, 6, conns=2, late={4}))
    assert (report["correct"], report["wrong"]) == (5, 0)
    assert report["key_time_conflict"] == 1


def test_the_probes_declared_losses_are_reported(tmp_path):
    assert analyze(write_cell(tmp_path, 2, conns=1, drops=3))["probe_drops"] == 3


# -- controls --------------------------------------------------------------------


def test_k2b_independent_losses_on_both_sides_link_nothing_wrong(tmp_path):
    cell = write_cell(tmp_path, 1000, conns=10)
    report = analyze(cell, drop=50, seed=3)
    assert report["wrong"] == 0
    assert 0.85 <= report["coverage"] <= 0.95  # about 0.95 x 0.95 of the requests
    assert report["control"] == "K2b drop=50 permille each side seed=3"


def test_k5_reads_shifted_by_one_message_give_wrong_links(tmp_path):
    report = analyze(write_cell(tmp_path, 40, conns=4), shift=True)
    assert report["wrong"] > 0  # the written limit: a wrong offset inside the window is not caught
    assert report["control"] == "K5 reads shifted by one message"


def test_controls_exclude_each_other(tmp_path):
    with pytest.raises(ValueError, match="one control"):
        analyze(write_cell(tmp_path, 4, conns=1), drop=10, shift=True)


# -- output ----------------------------------------------------------------------


def test_main_prints_the_cell_summary(tmp_path, capsys):
    write_cell(tmp_path, 6, conns=2)
    sp.main(["stream_path.py", str(tmp_path), "--window-us", "1000000"])
    out = capsys.readouterr().out
    assert "n1: requests 6 links 6 correct 6 wrong 0 coverage 100.00%" in out
    assert "server offset in probe send 6 recv 6" in out
    assert "wrong links in the run: 0" in out
