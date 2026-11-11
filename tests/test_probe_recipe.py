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

    def open(self, cport, sport, t):
        client, server = (1, 2) if sport == HOP else (2, 3)
        st = {"c2s": 1000, "s2c": 5000, "n": 0}
        self.conns[cport, sport] = st
        self._ev("connect", cport, sport, 1000, 0, t, client)
        self._ev("accept", cport, sport, 1000, 0, t + 1, server)
        self.exchange(cport, sport, t + 2, HELLO, FLIGHT, handshake=True)

    def finish(self, cport, sport, t):
        """The client's TLS Finished, sent right after the handshake: the server
        answers nothing, so it joins the next request's asks."""
        st = self.conns[cport, sport]
        self._ev("send", cport, sport, st["c2s"], 64, t, 1 if sport == HOP else 2)
        self._ev("recv", cport, sport, st["c2s"], 64, t + 1, 2 if sport == HOP else 3)
        st["c2s"] += 64

    def exchange(self, cport, sport, t, ask, answer, handshake=False, answer_at=50):
        """The client writes ask bytes at t; the server answers at t + answer_at."""
        client, server = (1, 2) if sport == HOP else (2, 3)
        st = self.conns[cport, sport]
        self._ev("send", cport, sport, st["c2s"], ask, t, client)
        self._ev("recv", cport, sport, st["c2s"], ask, t + 1, server)
        offset = st["c2s"] - 1000
        st["c2s"] += ask
        self._ev("send", cport, sport, st["s2c"], answer, t + answer_at, server, s2c=True)
        self._ev("recv", cport, sport, st["s2c"], answer, t + answer_at + 1, client, s2c=True)
        st["s2c"] += answer
        if not handshake:
            st["n"] += 1
        return st["n"], offset

    def request(self, k, in_port, up_port, t, call_at=10, call_len=30, after=10):
        """Request k: read by the proxy at t+1, its call sent at t+1+call_at,
        answered call_len later, the proxy answers `after` later."""
        rank, _ = self.exchange(
            in_port, HOP, t, REQ, RESP, answer_at=1 + call_at + call_len + 1 + after
        )
        _, offset = self.exchange(up_port, FU, t + 1 + call_at, REQ, RESP, answer_at=call_len)
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
    return {"name": name, "main": main, "k2b": k2b, "k5": k5r}


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
