"""Freeze detection on the agent-free references: load-gen and fake-upstream."""

import importlib.util
import json
from pathlib import Path

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
spec = importlib.util.spec_from_file_location("freezes", HARNESS / "freezes.py")
fz = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fz)

MS = 1_000_000
T0 = 1_792_000_000_000_000_000  # ns since the epoch


def request(truth, sent, arrival, answer, received):
    """One measured request: load-gen row and fake-upstream row (times in ns)."""
    lg = {"trace_id": truth, "timestamp_ns": sent, "duration_ns": received - sent}
    fu = {"path": f"/truth-{truth}", "arrival_ns": arrival, "timestamp_ns": answer}
    return lg, fu


def write_cell(tmp_path, rows, extra_fu=(), name="n1"):
    cell = tmp_path / name
    cell.mkdir()
    (cell / "lg.jsonl").write_text("".join(json.dumps(lg) + "\n" for lg, _ in rows))
    fus = [fu for _, fu in rows] + list(extra_fu)
    (cell / "fu.jsonl").write_text("".join(json.dumps(fu) + "\n" for fu in fus))
    return cell


def calm(truth, at):
    """Arrives 1 ms after the send, answered 40 ms later, received 1 ms after that."""
    return request(truth, at, at + MS, at + 41 * MS, at + 42 * MS)


def held(truth, at, release):
    """Sent at `at`, held until `release` before reaching fake-upstream."""
    return request(truth, at, release, release + 40 * MS, release + 41 * MS)


def test_delays_come_from_load_gen_and_fake_upstream_only(tmp_path):
    cell = write_cell(tmp_path, [calm("a1", T0)])
    d = fz.delays(cell)
    assert d["arrival"] == [(1000, T0 + MS)]
    assert d["answer"] == [(1000, T0 + 42 * MS)]


def test_requests_outside_the_measured_run_or_without_arrival_are_skipped(tmp_path):
    other = {"path": "/truth-warmup", "arrival_ns": T0, "timestamp_ns": T0 + MS}
    old = {"path": "/truth-b2", "timestamp_ns": T0 + MS}  # fake-upstream before arrival_ns
    lg_b2, _ = calm("b2", T0)
    cell = write_cell(tmp_path, [calm("a1", T0), (lg_b2, old)], extra_fu=[other])
    assert [d for d, _ in fz.delays(cell)["arrival"]] == [1000]


def test_ten_held_requests_released_in_one_millisecond_are_a_freeze():
    release = T0 + 500 * MS
    samples = [((release - (T0 + k * MS)) // 1000, release + k * 50_000) for k in range(10)]
    assert fz.freezes(samples) == {release // MS: 10}


def test_nine_requests_or_short_delays_are_not_a_freeze():
    release = T0 + 500 * MS
    nine = [(30_000, release) for _ in range(9)]
    assert fz.freezes(nine) == {}
    at_the_limit = [(fz.FREEZE_DELAY_US, release) for _ in range(20)]
    assert fz.freezes(at_the_limit) == {}


def test_released_in_different_milliseconds_is_not_one_freeze():
    spread = [(30_000, T0 + k * MS) for k in range(20)]
    assert fz.freezes(spread) == {}


def test_a_cell_report_counts_freezes_and_quantiles(tmp_path):
    rows = [calm(f"c{k}", T0 + k * 10 * MS) for k in range(30)]
    rows += [held(f"b{k}", T0 + k * MS, T0 + 200 * MS) for k in range(12)]
    report = fz.analyze_cell(write_cell(tmp_path, rows))
    arrival = report["arrival"]
    assert arrival["n"] == 42
    assert arrival["freezes"] == 1 and arrival["largest_freeze"] == 12
    assert arrival["p50_us"] == 1000 and arrival["max_us"] == 200_000
    assert report["answer"]["freezes"] == 0
    assert report["cpu"] is None


MPSTAT = """Linux 7.0.0-38-generic (lab) \t2026-10-17 \t_x86_64_\t(2 CPU)

{t1}     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
{t1}     all   50.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   50.00
{t1}       0  100.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00
{t1}       1    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00  100.00

{t2}     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
{t2}     all   20.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   80.00
{t2}       0   30.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   70.00
{t2}       1   10.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00   90.00
"""

PIDSTAT = """Linux 7.0.0-38-generic (lab) \t10/17/26 \t_x86_64_\t(2 CPU)

# Time        UID       PID    %usr %system  %guest   %wait    %CPU   CPU  Command
{t1}        0       100   90.00    5.00    0.00    0.00   95.00     0  agent
{t1}     1000       200   10.00    0.00    0.00    0.00   10.00     1  hop-1

# Time        UID       PID    %usr %system  %guest   %wait    %CPU   CPU  Command
{t2}        0       100   20.00    0.00    0.00    0.00   20.00     0  agent
"""


def test_cpu_summary_ties_saturated_seconds_to_freezes(tmp_path):
    release = T0 + 500 * MS
    t1 = fz._clock(release)
    t2 = fz._clock(release + 1000 * MS)
    rows = [held(f"b{k}", T0 + k * MS, release) for k in range(10)]
    cell = write_cell(tmp_path, rows)
    (cell / "cpu-cores.txt").write_text(MPSTAT.format(t1=t1, t2=t2))
    (cell / "cpu-procs.txt").write_text(PIDSTAT.format(t1=t1, t2=t2))
    c = fz.analyze_cell(cell)["cpu"]
    assert c["seconds"] == 2
    assert c["seconds_with_a_saturated_core"] == 1
    assert c["freeze_seconds"] == 1 and c["freeze_seconds_with_a_saturated_core"] == 1
    assert c["top_processes_mean_cpu"] == {"agent": 57.5, "hop-1": 5.0}
    assert c["foreign_cores"] == 0.0


def test_foreign_load_is_everything_outside_the_bench_summed(tmp_path):
    t1, t2 = fz._clock(T0), fz._clock(T0 + 1000 * MS)
    cell = write_cell(tmp_path, [calm("b1", T0)])
    (cell / "cpu-cores.txt").write_text(MPSTAT.format(t1=t1, t2=t2))
    foreign = (
        f"{t1}     1000       300   60.00    0.00    0.00    0.00   60.00     1  python\n"
        f"{t1}     1000       301   50.00    0.00    0.00    0.00   50.00     1  python\n"
        f"{t2}     1000       302   40.00    0.00    0.00    0.00   40.00     1  goland\n"
        f"{t2}        0       400    1.00    0.00    0.00    0.00    1.00     1  pidstat\n"
        f"{t1}        0       500  200.00    0.00    0.00    0.00  200.00     1  cpu-load\n"
        f"{t2}        0       500  198.00    0.00    0.00    0.00  198.00     1  cpu-load\n"
    )
    (cell / "cpu-procs.txt").write_text(PIDSTAT.format(t1=t1, t2=t2) + foreign)
    c = fz.analyze_cell(cell)["cpu"]
    # (60 + 50 + 40) / 2 s: two python processes at 0.55 core together, plus goland.
    assert c["foreign_cores"] == 0.75
    assert c["foreign_by_process"] == {"python": 0.55, "goland": 0.2}
    # The declared load (cpu-load, LOAD_CORES) is reported apart, never as foreign.
    assert c["declared_load_cores"] == 1.99


def test_main_prints_each_cell(tmp_path, capsys):
    write_cell(tmp_path, [calm("a1", T0)])
    fz.main(["freezes.py", str(tmp_path)])
    out = capsys.readouterr().out
    assert "== n1" in out and "arrival delay µs: p50 1000" in out and "not sampled" in out
    fz.main(["freezes.py", str(tmp_path), "--json"])
    assert json.loads(capsys.readouterr().out)["n1"]["answer"]["max_us"] == 1000
