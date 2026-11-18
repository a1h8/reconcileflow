"""Judge links computed in another environment (run_elsewhere.sh), files
<links-dir>/<run>.<method>.json, with the same judge as every method.

usage: judge_from_files.py <links-dir> <method> <procs> <runs-list>... > out.json
Run from m0-harness with PYTHONPATH=../src:.
"""

import json
import sys
from multiprocessing import Pool
from pathlib import Path

import probe_compare as pc
import probe_recipe as pr

KEEP = (
    "method",
    "judged",
    "right",
    "wrong",
    "refused",
    "unjudgeable",
    "refused_by_status",
    "coverage",
)
LINKS, METHOD = Path(sys.argv[1]), sys.argv[2]


def one(line):
    run, agent, c, load = line.split()[:4]
    f = LINKS / f"{Path(run).name}.{METHOD}.json"
    out = {"run": Path(run).name, "agent": agent, "c": int(c), "load": int(load)}
    if not f.exists():
        return {**out, "missing": True}
    r = pr.judge(Path(run) / "n1", "declared", linker=pc.from_file(f, METHOD))
    return {**out, **{k: r.get(k) for k in KEEP}}


if __name__ == "__main__":
    lines = [ln for x in sys.argv[4:] for ln in Path(x).read_text().splitlines() if ln.strip()]
    with Pool(int(sys.argv[3])) as p:
        json.dump(p.map(one, lines, chunksize=1), sys.stdout, indent=1)
