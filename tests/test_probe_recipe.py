"""The recipe on the probe's events (phase 4 spec 8.13): inside the proxy,
request lives and call lives from kernel events only, then recipe_links;
judged with load-gen's and fake-upstream's own logs."""

import json
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))

import probe_recipe as pr  # noqa: E402

HOP, FU = 7000, 4000
HELLO, FLIGHT, REQ, RESP = 300, 2000, 392, 900
T0 = 1_000_000_000


class Cell:
    """A synthetic cell: one proxy (pid 2) between load-gen (pid 1) and
    fake-upstream (pid 3), TLS on both links (a handshake opens each one)."""

    def __init__(self):
        self.probe, self.lg, self.fu = [], [], []
        self.conns = {}  # (client port, server port) -> state

    def _ev(self, kind, cport, sport, seq, length, t, pid, s2c=False):
        r = {
            "kind": kind,
            "client_addr": "127.0.0.1",
            "client_port": cport,
            "server_addr": "127.0.0.1",
            "server_port": sport,
            "seq": seq,
            "len": length,
            "pid": pid,
            "socket": cport * 10 + (sport == FU),
            "timestamp_ns": t,
        }
        if s2c:
            r["dir"] = "s2c"
        self.probe.append(r)

    def open(self, cport, sport, t, base=1000):
        """A connection, or a new one on the same four-tuple (base: its first
        byte's number, a new one for each)."""
        client, server = (1, 2) if sport == HOP else (2, 3)
        st = {"c2s": base, "s2c": base + 4000, "n": 0, "base": base}
        self.conns[cport, sport] = st
        self._ev("connect", cport, sport, base, 0, t, client)
        self._ev("accept", cport, sport, base, 0, t + 1, server)
        self.exchange(cport, sport, t + 2, HELLO, FLIGHT, handshake=True)

    def finish(self, cport, sport, t):
        """The client's TLS Finished, sent right after the handshake: the server
        answers nothing, so it joins the next request's asks."""
        st = self.conns[cport, sport]
        self._ev("send", cport, sport, st["c2s"], 64, t, 1 if sport == HOP else 2)
        self._ev("recv", cport, sport, st["c2s"], 64, t + 1, 2 if sport == HOP else 3)
        st["c2s"] += 64

    def exchange(self, cport, sport, t, ask, answer, handshake=False, answer_at=50, pieces=1):
        """The client writes ask bytes at t; the server answers at t + answer_at,
        in `pieces` writes 2 ns apart."""
        client, server = (1, 2) if sport == HOP else (2, 3)
        st = self.conns[cport, sport]
        self._ev("send", cport, sport, st["c2s"], ask, t, client)
        self._ev("recv", cport, sport, st["c2s"], ask, t + 1, server)
        offset = st["c2s"] - st["base"]
        st["c2s"] += ask
        cut = [answer // pieces] * (pieces - 1)
        for k, part in enumerate([*cut, answer - sum(cut)]):
            at = t + answer_at + 2 * k
            self._ev("send", cport, sport, st["s2c"], part, at, server, s2c=True)
            self._ev("recv", cport, sport, st["s2c"], part, at + 1, client, s2c=True)
            st["s2c"] += part
        if not handshake:
            st["n"] += 1
        return st["n"], offset

    def request(
        self,
        k,
        in_port,
        up_port,
        t,
        call_at=10,
        call_len=30,
        after=10,
        size=RESP,
        extra=0,
        pieces=1,
    ):
        """Request k: read by the proxy at t+1, its call sent at t+1+call_at,
        answered call_len later with `size` bytes, the proxy answers `after`
        later with size + extra bytes."""
        rank, _ = self.exchange(
            in_port, HOP, t, REQ, size + extra, answer_at=1 + call_at + call_len + 1 + after
        )
        _, offset = self.exchange(
            up_port, FU, t + 1 + call_at, REQ, size, answer_at=call_len, pieces=pieces
        )
        tid = f"{k:032x}"
        self.lg.append(
            {
                "conn_key": f"127.0.0.1:{in_port}-127.0.0.1:{HOP}|0",
                "stream_id": rank,
                "trace_id": tid,
            }
        )
        self.fu.append(
            {
                "conn_key": f"127.0.0.1:{up_port}-127.0.0.1:{FU}|0",
                "stream_offset": offset,
                "trace_id": tid,
            }
        )

    def write(self, tmp_path, drops=()):
        cell = tmp_path / "n1"
        cell.mkdir(parents=True)
        rows = sorted(self.probe, key=lambda r: r["timestamp_ns"])
        rows += [
            {"kind": "drops", "seq": 0, "len": 0, "timestamp_ns": t, "drops": n} for t, n in drops
        ]
        (cell / "probe.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        (cell / "lg.jsonl").write_text("".join(json.dumps(r) + "\n" for r in self.lg))
        (cell / "fu.jsonl").write_text("".join(json.dumps(r) + "\n" for r in self.fu))
        (cell / "cell.json").write_text(json.dumps({"hop_ports": [HOP], "fake_upstream_port": FU}))
        return cell


def busy(n=200, conns=4, spacing=100, **kw):
    """n requests over `conns` inbound and upstream connections, overlapping."""
    c = Cell()
    for i in range(conns):
        c.open(50_000 + i, HOP, T0 + i)
        c.open(60_000 + i, FU, T0 + 10 + i)
    t = T0 + 10_000
    for k in range(n):
        i = k % conns
        c.request(k, 50_000 + i, 60_000 + i, t + k * spacing, **kw)
    return c


def test_lives_skip_the_handshake_and_bound_each_exchange(tmp_path):
    c = Cell()
    c.open(50_000, HOP, T0)
    c.open(60_000, FU, T0 + 10)
    c.request(0, 50_000, 60_000, T0 + 1000)
    requests, calls = pr.lives(c.write(tmp_path) / "probe.jsonl", HOP, FU)
    (r,) = requests
    (x,) = calls
    # read at t+1; answered at t + 1 + call_at + call_len + 1 + after
    assert (r.rank, r.start, r.end) == (1, T0 + 1001, T0 + 1000 + 1 + 10 + 30 + 1 + 10)
    assert (x.rank, x.start, x.end) == (1, T0 + 1011, T0 + 1011 + 30 + 1)
    assert x.offsets == (HELLO,)  # the call's bytes start after the hello
    assert (r.answer, x.answer) == (RESP, RESP)  # answer bytes; the handshake's left out


def test_overlapping_requests_are_linked_right(tmp_path):
    # Each request lasts longer than the spacing: four in flight at once.
    report = pr.judge(busy(spacing=20).write(tmp_path))
    assert report["wrong"] == 0
    assert report["causality_violations"] == 0
    assert report["right"] == report["judged"] > 0


def test_a_declared_loss_refuses_its_group(tmp_path):
    cell = busy(spacing=20).write(tmp_path, drops=[(T0, 0), (T0 + 10_000 + 150 * 20, 3)])
    report = pr.judge(cell)
    assert report["wrong"] == 0
    assert report["refused_by_status"].get("INCOMPLETE", 0) > 0


def test_a_call_sent_before_its_request_is_read_is_a_causality_violation(tmp_path):
    c = busy(n=60, spacing=500)
    # Request 59's call: move its send before the proxy read the request.
    up = [
        r
        for r in c.probe
        if r["server_port"] == FU and r["kind"] == "send" and "dir" not in r and r["pid"] == 2
    ]
    up[-1]["timestamp_ns"] -= 200
    report = pr.judge(c.write(tmp_path))
    assert report["causality_violations"] == 1


def test_a_finished_sent_long_before_the_first_call_does_not_start_it(tmp_path):
    # Seen on the bench (c200): the upstream connection opens, its TLS Finished
    # leaves at once, the first call only seconds later.
    c = busy(n=40, spacing=500)
    for i in range(4):
        c.finish(60_000 + i, FU, T0 + 100 + i)
    _, calls = pr.lives(c.write(tmp_path) / "probe.jsonl", HOP, FU)
    first = [x for x in calls if x.rank == 1]
    assert all(x.start > T0 + 10_000 for x in first)
    report = pr.judge(tmp_path / "n1")
    assert report["causality_violations"] == 0
    assert report["wrong"] == 0 and report["right"] == report["judged"] > 0


# -- evaluation options and controls (phase 4 spec 8.14) ----------------------


def ticking(n):
    """Drop records once every 1,000 ns, all zero: the probe lost nothing."""
    return [(T0 + k * 1_000, 0) for k in range(n)]


def test_a_declared_recipe_needs_no_truth(tmp_path):
    report = pr.judge(busy(spacing=20).write(tmp_path), recipe="declared")
    assert report["recipe_source"] == "declared"
    assert report["wrong"] == 0 and report["right"] == report["judged"] > 0


def test_k2b_events_dropped_and_declared_never_give_a_wrong_link(tmp_path):
    cell = busy(n=400, spacing=20).write(tmp_path, drops=ticking(40))
    for seed in (1, 2, 3):
        report = pr.judge(cell, recipe="declared", drop=50, seed=seed)
        assert report["control"] == f"K2b drop=50 seed={seed}"
        assert report["wrong"] == 0
        assert report["refused_by_status"].get("INCOMPLETE", 0) > 0


def test_k5_a_shifted_truth_shows_up_as_wrong_links(tmp_path):
    report = pr.judge(busy(spacing=20).write(tmp_path), recipe="declared", shift=True)
    assert report["control"] == "K5 call truth shifted by one rank"
    linked = report["right"] + report["wrong"]
    assert linked > 0 and report["wrong"] >= 0.99 * linked


def test_ranks_that_do_not_match_load_gen_make_the_cell_invalid(tmp_path):
    c = busy(n=40, spacing=500)
    assert pr.judge(c.write(tmp_path / "a"))["ranks_ok"]
    c.lg[-1]["stream_id"] += 5  # load-gen counted requests the probe never saw
    assert not pr.judge(c.write(tmp_path / "b"))["ranks_ok"]


# -- the evaluation verdict (spec 8.14) -----------------------------------------


def cell(
    name="c20-load0",
    wrong=0,
    coverage=0.995,
    lossy=0,
    violations=0,
    ranks=True,
    k2b_wrong=0,
    k5=1.0,
    k6_wrong=0,
):
    main = {
        "wrong": wrong,
        "coverage": coverage,
        "lossy_intervals": lossy,
        "causality_violations": violations,
        "ranks_ok": ranks,
        "right": 100,
        "judged": 100,
    }
    k2b = [{"wrong": k2b_wrong, "coverage": 0.0} for _ in range(3)]
    linked = 100
    k5r = {"wrong": round(k5 * linked), "right": linked - round(k5 * linked)}
    return {"name": name, "main": main, "k2b": k2b, "k5": k5r, "k6": {"wrong": k6_wrong}}


def test_the_verdict_holds_when_every_valid_cell_meets_every_criterion():
    held, lines = pr.verdict([cell(), cell("c800-load4", coverage=0.9941)])
    assert held is True and any("tenu" in x for x in lines)


def test_one_wrong_link_anywhere_fails_the_verdict():
    held, _ = pr.verdict([cell(), cell("c400-load0", wrong=1)])
    assert held is False


def test_low_coverage_fails_only_without_declared_losses():
    assert pr.verdict([cell(coverage=0.98)])[0] is False
    assert pr.verdict([cell(coverage=0.40, lossy=3)])[0] is True


def test_a_causality_violation_or_a_k2b_wrong_link_fails():
    assert pr.verdict([cell(violations=1)])[0] is False
    assert pr.verdict([cell(k2b_wrong=1)])[0] is False


def test_a_blind_judge_gives_no_verdict():
    held, lines = pr.verdict([cell(k5=0.5)])
    assert held is None and any("K5" in x for x in lines)


def test_a_cell_with_unmatched_ranks_is_invalid_not_failed():
    held, lines = pr.verdict([cell(), cell("c100-load4", wrong=7, ranks=False)])
    assert held is True and any("invalid" in x for x in lines)


# -- answer sizes the proxy forwards (spec 8.17) ---------------------------------


def with_twins(seed=5, solo=300, pairs=40):
    """Requests the timings alone link (one at a time), then pairs that start,
    call and answer at the same instants on two connections: only the answer
    sizes, forwarded by the proxy with 22 bytes more, tell them apart; the
    last pair's sizes are 30 bytes apart, inside the window."""
    import random

    rng = random.Random(seed)
    c = Cell()
    for i in range(2):
        c.open(50_000 + i, HOP, T0 + i)
        c.open(60_000 + i, FU, T0 + 10 + i)
    t, k = T0 + 10_000, 0
    for _ in range(solo):
        i = k % 2
        c.request(k, 50_000 + i, 60_000 + i, t, size=rng.randint(50, 5000), extra=22)
        t, k = t + 200, k + 1
    for p in range(pairs + 1):
        a = rng.randint(50, 2000)
        # sizes far apart, but the last pair's within the window: still open
        for i, size in enumerate((a, a + 30 if p == pairs else a + rng.randint(500, 3000))):
            c.request(k, 50_000 + i, 60_000 + i, t, size=size, extra=22)
            k += 1
        t += 200
    return c


def test_learned_sizes_settle_what_the_timings_leave_open(tmp_path):
    cell = with_twins().write(tmp_path)
    plain = pr.judge(cell, recipe="declared")
    assert plain["wrong"] == 0 and plain["refused_by_status"].get("AMBIGUOUS", 0) > 0
    sized = pr.judge(cell, recipe="declared", sizes="learned")
    assert sized["size_window"] == [22 - 32, 22 + 32]
    assert sized["wrong"] == 0
    assert sized["right"] > plain["right"]
    # only the last pair, 30 bytes apart, is left open: refused, not guessed
    assert sized["refused_by_status"] == {"AMBIGUOUS": 2}
    assert sized["right"] == sized["judged"] - 2


def test_k6_a_wrong_window_is_refused_not_linked(tmp_path):
    cell = with_twins().write(tmp_path)
    report = pr.judge(cell, recipe="declared", sizes="learned", size_shift=1000)
    assert report["control"] == "K6 window shifted by 1000 bytes"
    assert report["wrong"] == 0
    assert report["refused_by_status"].get("SIZE_MISMATCH", 0) > 0


def test_one_wrong_link_under_k6_fails_the_verdict():
    assert pr.verdict([cell()])[0] is True
    assert pr.verdict([cell(k6_wrong=1)])[0] is False


# -- losses located by the byte positions (spec 8.18) ----------------------------


def proxy_reads_on(port, sport=HOP):
    return lambda r: (
        r["kind"] == "recv"
        and r["server_port"] == sport
        and r["client_port"] == port
        and "dir" not in r
    )


def test_a_lost_event_leaves_a_hole_between_its_neighbours():
    c = busy(n=40, spacing=500)
    reads = [r for r in c.probe if proxy_reads_on(50_001)(r)]
    lost = reads[5]
    c.probe.remove(lost)
    holes = pr.seq_holes(c.probe)
    assert [(h.conn, h.start < lost["timestamp_ns"] < h.end) for h in holes] == [
        ((50_001, HOP), True)
    ]


def test_the_one_byte_close_artifact_is_not_a_hole():
    c = busy(n=40, spacing=500)
    last = [r for r in c.probe if proxy_reads_on(50_002)(r)][-1]
    last["seq"] += 1  # recorded before bbfa835: the read that took the FIN
    assert pr.seq_holes(c.probe) == []


def test_a_lost_last_event_is_seen_from_the_other_side():
    c = busy(n=40, spacing=500)
    answers = [
        r
        for r in c.probe
        if r["server_port"] == HOP
        and r["client_port"] == 50_003
        and r.get("dir") == "s2c"
        and r["kind"] == "send"
    ]
    c.probe.remove(answers[-1])  # the proxy's last answer on that connection
    (hole,) = pr.seq_holes(c.probe)
    assert hole.conn == (50_003, HOP)
    assert hole.start <= answers[-2]["timestamp_ns"] < hole.end
    assert hole.end >= answers[-1]["timestamp_ns"]


def test_located_losses_leave_links_far_from_them(tmp_path):
    # Requests one after the other: each its own group, so what a loss
    # refuses is its neighbourhood, not a group that holds everything. Drop
    # records far apart, as on the bench (one a second, over a thousand
    # requests at c800): each covers a hundred requests here.
    marks = [(T0 + k * 50_000, 0) for k in range(6)]
    cell = busy(n=400, spacing=500).write(tmp_path, drops=marks)
    far = pr.judge(cell, recipe="declared", drop=5, seed=1, locate=True)
    wide = pr.judge(cell, recipe="declared", drop=5, seed=1, locate=False)
    assert far["wrong"] == 0 and wide["wrong"] == 0
    assert wide["right"] == 0
    assert far["right"] > 0.5 * far["judged"]
    assert far["unlocated_losses"] == 0


def test_a_lost_piece_of_an_answer_removes_the_call_size(tmp_path):
    c = Cell()
    for i in range(2):
        c.open(50_000 + i, HOP, T0 + i)
        c.open(60_000 + i, FU, T0 + 10 + i)
    for k in range(300):
        t = T0 + 10_000 + 200 * k
        c.request(k, 50_000 + k % 2, 60_000 + k % 2, t, size=1000 + k, extra=22, pieces=2)
    pieces = [
        r
        for r in c.probe
        if r["server_port"] == FU
        and r["client_port"] == 60_001
        and r.get("dir") == "s2c"
        and r["kind"] == "recv"
    ]
    lost = pieces[-20]  # the second piece of a late call's answer
    c.probe.remove(lost)
    requests, calls = pr.lives(c.probe, HOP, FU)
    _, _, damaged_calls, unlocated = pr.locate_losses(
        c.probe, HOP, FU, requests, calls, [(T0, 2**62, 1)]
    )
    (x,) = [j for j, x in enumerate(calls) if x.span[0] <= lost["timestamp_ns"] <= x.span[1]]
    assert x in damaged_calls and not unlocated
    cell = c.write(tmp_path, drops=[(T0, 0), (2**62, 1)])
    report = pr.judge(cell, recipe="declared", sizes="learned", locate=True)
    assert report["wrong"] == 0 and report["right"] > 0


def test_a_declared_loss_without_any_hole_keeps_its_whole_interval(tmp_path):
    marks = [(T0, 0), (T0 + 10_000 + 150 * 20, 3)]
    cell = busy(spacing=20).write(tmp_path, drops=marks)
    report = pr.judge(cell, locate=True)
    assert report["unlocated_losses"] == 1
    assert report["wrong"] == 0 and report["refused_by_status"].get("INCOMPLETE", 0) > 0


def test_more_holes_than_declared_losses_make_the_cell_invalid():
    c = cell("c200-load0", wrong=3)
    c["main"]["holes_exceed_drops"] = True
    held, lines = pr.verdict([cell(), c])
    assert held is True and any("more holes than declared losses" in x for x in lines)


def test_bytes_sent_after_the_reader_closed_are_no_hole():
    # At a close, one side sends its TLS close alert after the other closed:
    # never read, not lost (seen on every closed connection of the bench).
    c = busy(n=40, spacing=500)
    st = c.conns[50_001, HOP]
    t = T0 + 100_000
    c._ev("close", 50_001, HOP, 0, 0, t, 1)  # load-gen closes first
    c._ev("send", 50_001, HOP, st["s2c"], 24, t + 10, 2, s2c=True)  # the proxy's alert
    assert pr.seq_holes(c.probe) == []


def test_bytes_left_unread_before_the_reader_closed_are_a_hole_up_to_its_close():
    c = busy(n=40, spacing=500)
    st = c.conns[50_001, HOP]
    t = T0 + 100_000
    c._ev("send", 50_001, HOP, st["s2c"], 24, t, 2, s2c=True)  # unread, or its read lost
    c._ev("close", 50_001, HOP, 0, 0, t + 10, 1)
    (hole,) = pr.seq_holes(c.probe)
    assert hole.conn == (50_001, HOP) and hole.end == t + 10


def test_a_hole_where_the_probe_declared_no_loss_is_not_one():
    # The probe counts each lost event when it happens: a hole outside every
    # interval where its count grew is the protocol (a close), not a loss.
    c = busy(n=40, spacing=500)
    st = c.conns[60_001, FU]
    t = T0 + 100_000
    c._ev("send", 60_001, FU, st["s2c"], 24, t, 3, s2c=True)  # left unread by the proxy
    c._ev("close", 60_001, FU, 0, 0, t + 10, 2)
    rows = c.probe
    requests, calls = pr.lives(rows, HOP, FU)
    assert len(pr.seq_holes(rows)) == 1
    zones, dr, dc, unlocated = pr.locate_losses(rows, HOP, FU, requests, calls, [])
    assert (zones, dr, dc, unlocated) == ([], set(), set(), [])
    zones, *_ = pr.locate_losses(rows, HOP, FU, requests, calls, [(t - 5, t + 5, 1)])
    assert len(zones) == 1


def upstream(c, port, way, kind, pid):
    return [
        r
        for r in c.probe
        if r["server_port"] == FU
        and r["client_port"] == port
        and (r.get("dir") or "c2s") == way
        and r["kind"] == kind
        and r["pid"] == pid
    ]


def test_a_hole_in_fake_upstreams_events_alone_changes_nothing():
    # lives only uses the proxy's own events: fake-upstream's lost read of a
    # call makes a hole, but no zone and no size dropped.
    c = busy(n=40, spacing=500)
    c.probe.remove(upstream(c, 60_001, "c2s", "recv", 3)[5])
    requests, calls = pr.lives(c.probe, HOP, FU)
    assert len(pr.seq_holes(c.probe)) == 1
    found = pr.locate_losses(c.probe, HOP, FU, requests, calls, [(T0, 2**62, 1)])
    assert found == ([], set(), set(), [])


def test_a_lost_read_of_the_proxy_starts_its_zone_at_the_other_sides_send():
    c = busy(n=40, spacing=1_000_000)  # an upstream connection idle 4 ms between calls
    reads = upstream(c, 60_002, "s2c", "recv", 2)
    lost = reads[5]
    c.probe.remove(lost)
    sent = next(x for x in upstream(c, 60_002, "s2c", "send", 3) if x["seq"] == lost["seq"])
    requests, calls = pr.lives(c.probe, HOP, FU)
    zones, *_ = pr.locate_losses(c.probe, HOP, FU, requests, calls, [(T0, 2**62, 1)])
    assert zones == [(sent["timestamp_ns"], reads[6]["timestamp_ns"])]


def test_a_lost_send_of_the_proxy_ends_its_zone_at_the_other_sides_read():
    c = busy(n=40, spacing=1_000_000)
    sends = upstream(c, 60_002, "c2s", "send", 2)
    lost = sends[5]
    c.probe.remove(lost)
    read = next(x for x in upstream(c, 60_002, "c2s", "recv", 3) if x["seq"] == lost["seq"])
    requests, calls = pr.lives(c.probe, HOP, FU)
    zones, *_ = pr.locate_losses(c.probe, HOP, FU, requests, calls, [(T0, 2**62, 1)])
    assert zones == [(sends[4]["timestamp_ns"], read["timestamp_ns"])]


def test_a_call_merged_with_the_next_is_judged_by_its_last_request():
    # The proxy's only read of one answer is lost: that call's asks join the
    # next call's on the connection; the merged exchange is the next call (it
    # starts at its last send), so its truth is the last request's.
    merged = pr.Exchange((60_001, FU), 3, 0, 10, (392, 784))
    truth = {(60_001, 0, 392): "x", (60_001, 0, 784): "y"}  # (port, incarnation, offset)
    assert pr._call_tid(merged, truth) == "y"
    assert pr._call_tid(pr.Exchange((60_001, FU), 1, 0, 10, (0,)), truth) is None


def test_the_window_is_learnt_even_where_losses_refuse_the_timings(tmp_path):
    # 600 requests: enough links left (200) once the damaged sizes are out.
    marks = [(T0 + k * 50_000, 0) for k in range(5)]
    cell = with_twins(solo=600).write(tmp_path, drops=marks)
    report = pr.judge(cell, recipe="declared", drop=50, seed=1, sizes="learned", locate=True)
    assert report["size_window"] is not None
    assert report["wrong"] == 0 and report["right"] > 0


def test_a_window_given_is_used_not_learnt(tmp_path):
    # The window is the relay's: learnt where nothing was lost, then kept.
    cell = with_twins().write(tmp_path)
    report = pr.judge(cell, recipe="declared", sizes="learned", window=(10, 40))
    assert report["size_window"] == [10, 40]
    assert report["wrong"] == 0 and report["right"] > 0


def test_a_wrong_link_under_k2c_fails_the_verdict():
    c = cell()
    c["k2c"] = [{"wrong": 0, "coverage": 0.5}, {"wrong": 1, "coverage": 0.5}]
    held, lines = pr.verdict([c])
    assert held is False and any("K2c 1 wrong" in x for x in lines)


def test_the_series_runs_k2c_with_the_main_window(tmp_path, monkeypatch):
    calls = []

    def fake(cell, recipe="learned", **kw):
        calls.append(kw)
        return {"size_window": [1, 2], "wrong": 0, "right": 1}

    monkeypatch.setattr(pr, "judge", fake)
    monkeypatch.setattr(pr, "verdict", lambda cells: (True, [sorted(cells[0])]))
    listing = tmp_path / "runs.txt"
    listing.write_text(f"{tmp_path / 'run-a'} none 20 0\n")
    _, (keys,) = pr.judge_series(listing)
    assert keys == ["k2b", "k2c", "k5", "k6", "main", "name"]
    k2c = [kw for kw in calls if kw.get("drop") == 1]
    assert [kw["seed"] for kw in k2c] == [1, 2, 3]
    assert all(kw["window"] == (1, 2) and kw["locate"] for kw in k2c)


# -- truth on the wire, connections on a reused four-tuple (spec 8.20) ----------


def test_a_reused_four_tuple_is_a_new_connection():
    # The tail phase's load-gen reopened a port the measured one had used.
    c = busy(n=20, spacing=500)
    c.open(50_001, HOP, T0 + 200_000, base=900_000_000)
    c.open(60_001, FU, T0 + 200_010, base=700_000_000)
    for k in range(3):
        c.request(100 + k, 50_001, 60_001, T0 + 300_000 + 500 * k)
    assert pr.seq_holes(c.probe) == []
    requests, _ = pr.lives(c.probe, HOP, FU)
    later = [r for r in requests if r.conn == (50_001, HOP) and r.start > T0 + 200_000]
    assert [r.rank for r in later] == [1, 2, 3]
    assert len({r.incarnation for r in requests if r.conn == (50_001, HOP)}) == 2


def swap_two_ranks(c, port=50_001):
    """load-gen numbered two requests of a connection at their end, in the
    other order (spec 8.20): stream_id swapped, wire_rank as on the wire."""
    mine = [r for r in c.lg if r["conn_key"].startswith(f"127.0.0.1:{port}-")]
    for r in mine:
        r["wire_rank"] = r["stream_id"]
    a, b = mine[4], mine[5]
    a["stream_id"], b["stream_id"] = b["stream_id"], a["stream_id"]


def test_the_truth_follows_the_wire_rank_not_the_completion_ordinal(tmp_path):
    c = busy(n=40, spacing=500)
    swap_two_ranks(c)
    report = pr.judge(c.write(tmp_path / "a"), recipe="declared")
    assert report["wrong"] == 0 and report["causality_violations"] == 0
    for r in c.lg:
        r.pop("wire_rank", None)  # a run recorded before wire_rank: the old truth, wrong here
    old = pr.judge(c.write(tmp_path / "b"), recipe="declared")
    assert old["wrong"] + old["causality_violations"] > 0


def test_the_truth_of_a_reused_port_goes_to_the_connection_open_at_its_time(tmp_path):
    # load-gen's records carry wall-clock times; the probe's clock records
    # give the offset to its own clock.
    OFFSET = 10**18
    c = busy(n=20, spacing=500)
    for r in c.lg:
        r["timestamp_ns"] = OFFSET + T0 + 10_000
    c.open(50_001, HOP, T0 + 200_000, base=900_000_000)  # the tail's, no truth of its own
    c.open(60_001, FU, T0 + 200_010, base=700_000_000)
    for k in range(3):
        c.request(100 + k, 50_001, 60_001, T0 + 300_000 + 500 * k)
    tail = [r for r in c.lg if int(r["trace_id"], 16) >= 100]
    for r in tail:  # the tail's own load-gen: its own file, its own times
        r["timestamp_ns"] = OFFSET + T0 + 300_000
    c.lg = [r for r in c.lg if int(r["trace_id"], 16) < 100]
    c.fu = [r for r in c.fu if int(r["trace_id"], 16) < 100]
    c.probe += [
        {"kind": "clock", "seq": 0, "len": 0, "timestamp_ns": t, "realtime_ns": OFFSET + t}
        for t in (T0, T0 + 400_000)
    ]
    cell = c.write(tmp_path)
    (cell / "tail.jsonl").write_text("".join(json.dumps(r) + "\n" for r in tail))
    report = pr.judge(cell, recipe="declared")
    assert report["ranks_ok"]
    assert report["wrong"] == 0 and report["causality_violations"] == 0
    assert report["right"] == report["judged"] > 0
