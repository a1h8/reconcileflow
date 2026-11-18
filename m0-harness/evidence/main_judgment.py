"""The main judgment of every cell of a series, in parallel: the method under
evaluation (declared recipe, size signature, losses located), without the
controls (``probe_recipe.py --series`` runs those). Each cell's report, plus
where it comes from.

usage: main_judgment.py <procs> <runs-list>... > out.json
Run from m0-harness with PYTHONPATH=../src:.
"""

import json
import sys
from multiprocessing import Pool
from pathlib import Path

import probe_recipe as pr

KEEP = (
    "ranks_ok",
    "size_window",
    "size_signature",
    "judged",
    "right",
    "wrong",
    "refused",
    "unjudgeable",
    "refused_by_status",
    "coverage",
    "lossy_intervals",
    "causality_violations",
)


def one(line):
    run, agent, c, load = line.split()[:4]
    r = pr.judge(Path(run) / "n1", "declared", sizes="lattice", locate=True)
    keep = {k: r.get(k) for k in KEEP}
    return {
        "day": Path(run).name[6:8],
        "agent": agent,
        "c": int(c),
        "load": int(load),
        "run": Path(run).name,
        **keep,
    }


if __name__ == "__main__":
    lines = [ln for f in sys.argv[2:] for ln in Path(f).read_text().splitlines() if ln.strip()]
    with Pool(int(sys.argv[1])) as p:
        json.dump(p.map(one, lines, chunksize=1), sys.stdout, indent=1)
