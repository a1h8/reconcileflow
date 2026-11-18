"""The judge's own numbers (phase 4 spec 8.14, 8.19, 8.23), each test written
for a mutant that survived the mutation audit of 2026-11-17: the coverage
judge() computes, its 99 % limit, K5's share of wrong links and its limit,
the controls a series runs, and a causality check at exact equality."""

import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import probe_compare as pc  # noqa: E402
import probe_recipe as pr  # noqa: E402

from test_probe_recipe import FU, HOP, T0, Cell, busy, cell  # noqa: E402

# -- T5: the coverage judge() reports, and the verdict's limit ------------------------


def test_the_coverage_is_the_share_of_right_links_among_the_judged(tmp_path):
    def all_but_five(requests, calls, helped):
        return dict(list(pc.greedy_nesting(requests, calls, helped).items())[:-5])

    all_but_five.name = "all_but_five"
    report = pr.judge(busy(n=60, spacing=500).write(tmp_path), "declared", linker=all_but_five)
    assert report["wrong"] == 0 and report["refused"] > 0
    assert report["coverage"] == report["right"] / report["judged"]


def test_a_coverage_of_exactly_99_percent_meets_the_criterion():
    assert pr.verdict([cell(coverage=0.99)])[0] is True


# -- T6: K5, and the controls a series runs ----------------------------------------------


def test_k5_seeing_98_percent_of_wrong_links_is_a_blind_judge():
    c = cell()
    c["k5"] = {"wrong": 98, "right": 2}
    assert pr.verdict([c])[0] is None


def test_k5_seeing_exactly_99_percent_of_wrong_links_is_enough():
    c = cell()
    c["k5"] = {"wrong": 99, "right": 1}
    assert pr.verdict([c])[0] is True


def test_a_series_runs_k5_with_the_shifted_truth(tmp_path):
    # Run without its shift, K5 would see no wrong link: no verdict.
    run = tmp_path / "run"
    busy(n=300, spacing=20).write(run)
    listing = tmp_path / "runs.txt"
    listing.write_text(f"{run} none 20 0\n")
    held, lines = pr.judge_series(listing)
    assert held is True, lines


# -- T7: a call that fills its request's life exactly is no violation ---------------------


def test_a_call_sent_at_the_read_and_answered_at_the_reply_is_inside_its_request(tmp_path):
    c = Cell()
    c.open(50_000, HOP, T0)
    c.open(60_000, FU, T0 + 10)
    for k in range(20):  # sent on the read, answered on the reply: equal bounds
        c.request(k, 50_000, 60_000, T0 + 10_000 + 1_000 * k, call_at=0, after=0)
    requests, calls = pr.lives(c.write(tmp_path) / "probe.jsonl", HOP, FU)
    assert all(r.start == x.start and r.end == x.end for r, x in zip(requests, calls, strict=True))
    report = pr.judge(tmp_path / "n1", "declared")
    assert report["causality_violations"] == 0
    assert report["wrong"] == 0 and report["right"] == report["judged"] > 0
