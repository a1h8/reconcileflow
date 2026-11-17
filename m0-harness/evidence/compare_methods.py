"""Other methods on the same lives, truth and judged half as the recipe
(``probe_compare``), the ones that run in this environment: the recipe
without sizes, first come first served (helped by the truth), greedy nesting.
ONLY=<name,...> keeps some of them.

usage: compare_methods.py <procs> <runs-list>... > out.json
Run from m0-harness with PYTHONPATH=../src:.
"""

import json
import os
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import probe_compare as pc
import probe_recipe as pr

ONLY = [m for m in os.environ.get("ONLY", "").split(",") if m]
KEEP = (
    "method",
    "judged",
    "right",
    "wrong",
    "refused",
    "unjudgeable",
    "refused_by_status",
    "coverage",
    "causality_violations",
)
METHODS = [
    ("recipe_no_size", {"sizes": "none", "locate": True}),
    ("fcfs", {"linker": pc.fcfs}),
    ("greedy_nesting", {"linker": pc.greedy_nesting}),
]


def one(line):
    run, agent, c, load = line.split()[:4]
    cell = Path(run) / "n1"
    out = {"run": Path(run).name, "agent": agent, "c": int(c), "load": int(load), "methods": {}}
    for name, kw in [m for m in METHODS if not ONLY or m[0] in ONLY]:
        t = time.time()
        r = pr.judge(cell, "declared", **kw)
        out["methods"][name] = {**{k: r.get(k) for k in KEEP}, "seconds": round(time.time() - t, 1)}
    return out


if __name__ == "__main__":
    lines = [ln for f in sys.argv[2:] for ln in Path(f).read_text().splitlines() if ln.strip()]
    with Pool(int(sys.argv[1])) as p:
        json.dump(p.map(one, lines, chunksize=1), sys.stdout, indent=1)
