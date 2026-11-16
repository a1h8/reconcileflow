"""Other methods on the same probe events (phase 4 spec 8.27): the same
lives, truth and judged half as the recipe; only the linking differs."""

import json
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import probe_compare as pc  # noqa: E402
import probe_recipe as pr  # noqa: E402

from test_probe_recipe import FU, HOP, T0, Cell, busy  # noqa: E402


def crossed(pairs=60, spacing=1_000):
    """Pairs of requests A, B on two connections: B is read after A but its
    call leaves first; both calls lie inside both requests."""
    c = Cell()
    for i in range(2):
        c.open(50_000 + i, HOP, T0 + i)
        c.open(60_000 + i, FU, T0 + 10 + i)
    t = T0 + 10_000
    for k in range(pairs):
        base = t + k * spacing
        c.request(2 * k, 50_000, 60_000, base, call_at=100, call_len=30, after=200)
        c.request(2 * k + 1, 50_001, 60_001, base + 20, call_at=5, call_len=30, after=200)
    return c


def test_first_come_first_served_links_requests_in_order(tmp_path):
    report = pr.judge(busy(n=60, spacing=500).write(tmp_path), "declared", linker=pc.fcfs)
    assert report["method"] == "fcfs"
    assert report["wrong"] == 0 and report["right"] == report["judged"] > 0


def test_first_come_first_served_is_wrong_when_calls_cross(tmp_path):
    report = pr.judge(crossed().write(tmp_path), "declared", linker=pc.fcfs)
    assert report["wrong"] == report["judged"] > 0


def test_greedy_nesting_gives_a_call_to_the_oldest_free_request_holding_it(tmp_path):
    report = pr.judge(crossed().write(tmp_path), "declared", linker=pc.greedy_nesting)
    assert report["method"] == "greedy_nesting"
    assert report["wrong"] == report["judged"] > 0


def test_the_recipe_refuses_what_greedy_nesting_gets_wrong(tmp_path):
    report = pr.judge(crossed().write(tmp_path), "declared")
    assert report["method"] == "recipe"
    assert report["wrong"] == 0
    assert report["refused"] == report["judged"] > 0


def test_greedy_nesting_skips_a_call_no_free_request_holds(tmp_path):
    # Sequential requests: every call lies in its own request only.
    report = pr.judge(busy(n=60, spacing=500).write(tmp_path), "declared", linker=pc.greedy_nesting)
    assert report["wrong"] == 0 and report["right"] == report["judged"] > 0


def test_a_method_that_gives_nothing_counts_refusals(tmp_path):
    report = pr.judge(busy(n=60, spacing=500).write(tmp_path), "declared", linker=pc.nothing)
    assert report["refused"] == report["judged"] > 0
    assert report["refused_by_status"] == {"NOT_GIVEN": report["judged"]}


# -- methods run elsewhere (TraceWeaver in its own environment, spec 8.27) ----------


def test_the_lives_are_exported_as_the_judge_sees_them_in_microseconds(tmp_path):
    cell = busy(n=60, spacing=500).write(tmp_path)
    out = tmp_path / "lives.json"
    pc.export_lives(cell, out)
    requests, calls = pr.lives(cell / "probe.jsonl", HOP, FU)
    got = json.loads(out.read_text())
    assert got["requests"] == [[r.start / 1000, r.end / 1000] for r in requests]
    assert got["calls"] == [[x.start / 1000, x.end / 1000] for x in calls]


def test_links_read_from_a_file_are_judged_like_any_method(tmp_path):
    cell = busy(n=60, spacing=500).write(tmp_path)
    lives = tmp_path / "lives.json"
    pc.export_lives(cell, lives)
    got = json.loads(lives.read_text())
    # Sequential requests: the k-th call by start belongs to the k-th request
    # that holds a call; the last one is left as a skip.
    holding = [
        i
        for i, (s, e) in enumerate(got["requests"])
        if any(s <= cs and ce <= e for cs, ce in got["calls"])
    ]
    links = {str(i): j for j, i in enumerate(holding[:-1])}
    path = tmp_path / "links.json"
    path.write_text(json.dumps({"links": links, "skips": [holding[-1]]}))
    report = pr.judge(cell, "declared", linker=pc.from_file(path, "traceweaver"))
    assert report["method"] == "traceweaver"
    assert report["wrong"] == 0 and report["right"] == report["judged"] - 1
    assert report["refused_by_status"] == {"SKIP": 1}


def test_first_come_first_served_is_not_shifted_by_warm_up_requests(tmp_path):
    # Warm-up requests go through the proxy before the measure: the probe sees
    # them, load-gen's truth does not. Helped means helped on both sides.
    c = Cell()
    for i in range(4):
        c.open(50_000 + i, HOP, T0 + i)
        c.open(60_000 + i, FU, T0 + 10 + i)
    for k in range(5):
        c.request(1000 + k, 50_000, 60_000, T0 + 2_000 + 1_000 * k)
    for k in range(60):
        c.request(k, 50_000 + k % 4, 60_000 + k % 4, T0 + 10_000 + 500 * k)
    c.lg, c.fu = c.lg[5:], c.fu[5:]  # the warm-up's truth is not kept
    report = pr.judge(c.write(tmp_path), "declared", linker=pc.fcfs)
    assert report["wrong"] == 0 and report["right"] == report["judged"] > 0
