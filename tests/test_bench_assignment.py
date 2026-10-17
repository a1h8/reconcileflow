"""The synthetic benchmark is a regression baseline: it must replay exactly."""

import importlib.util
import sys
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
spec = importlib.util.spec_from_file_location("bench_assignment", HARNESS / "bench_assignment.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def test_same_seed_same_events_other_seed_other_events():
    assert bench.generate(200, 10_000, 50_000, seed=3) == bench.generate(
        200, 10_000, 50_000, seed=3
    )
    assert bench.generate(200, 10_000, 50_000, seed=3) != bench.generate(
        200, 10_000, 50_000, seed=4
    )


def test_every_request_has_its_truth_on_both_sides():
    lefts, rights, truth = bench.generate(50, 1_000, 1_000, seed=0)
    assert len(lefts) == len(rights) == len(truth) == 50
    assert set(truth.values()) == {e.id for e in rights}


def test_sparse_precise_cell_decides_everything_correctly():
    row = bench.run_cell(1_000, 1_000, "0", n=300, seed=0)
    assert (row["wrong"], row["ambiguous"], row["oversized"]) == (0, 0, 0)
    assert row["max_component"] == 2


def test_cell_figures_replay_except_speed():
    a = bench.run_cell(100_000, 200_000, "0.02", n=400, seed=1)
    b = bench.run_cell(100_000, 200_000, "0.02", n=400, seed=1)
    a.pop("events_per_s"), b.pop("events_per_s")
    assert a == b
    assert a["ambiguous"] > 0  # dense enough that the margin has work to do


def test_quick_run_prints_one_cell(capsys):
    bench.main(["--quick"])
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("rate jitter_us margin")
    assert lines[1].startswith("10000 200 0.02 500 ")
