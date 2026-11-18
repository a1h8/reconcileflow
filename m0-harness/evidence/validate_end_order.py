"""May the agent's client spans be ranked by their end on a connection?
Validation only, it uses the clock: V1 on ports without overlap, the end
order is the start order; V2 on the others, the k-th span by its end is the
nearest to the end of the probe's k-th call (agent clock put on the probe's
by its clock records); V3, the counts match (else the port is set aside).

usage: validate_end_order.py <procs> <runs-list>... > out.json
Run from m0-harness with PYTHONPATH=../src:.:evidence (agent cells only).
"""

import json
import sys
from bisect import bisect_left
from collections import Counter, defaultdict
from itertools import pairwise
from multiprocessing import Pool
from pathlib import Path

import probe_recipe as pr
from second_judge import agent_spans


def one(line):
    run = line.split()[0]
    cell = Path(run) / "n1"
    meta = json.loads((cell / "cell.json").read_text())
    rows = list(pr._jsonl(cell / "probe.jsonl"))
    offset = pr._Openings(rows).offset
    _, calls = pr.lives(rows, meta["hop_ports"][0], meta["fake_upstream_port"])
    _, client = agent_spans(cell / "agent.out", meta["hop_ports"][0], meta["fake_upstream_port"])
    by_port = defaultdict(list)
    for x in calls:
        by_port[x.conn[0]].append(x)
    out = Counter()
    worst = []
    for port, xs in by_port.items():
        if len({x.incarnation for x in xs}) > 1:
            out["ports several incarnations"] += 1
            continue
        spans = client.get(port, [])
        if not spans:
            continue
        overlap = any(a[1] > b[0] for a, b in pairwise(spans))
        by_end = sorted(spans, key=lambda s: (s[1], s[0]))
        if not overlap:  # V1
            out["V1 ports"] += 1
            out["V1 same order"] += by_end == spans
            continue
        out["V2 ports"] += 1  # V2
        xs.sort(key=lambda x: x.rank)
        ends = [x.end for x in xs]
        used = len(xs) - len(spans) in (0, 1)  # V3: only these ports are used
        out["V3 ports count ok"] += used
        for k, s in enumerate(by_end):
            e = s[1] * 1000 - offset
            i = bisect_left(ends, e)
            near = min(
                (j for j in (i - 1, i) if 0 <= j < len(ends)), key=lambda j: abs(ends[j] - e)
            )
            tag = "V2" if used else "V2 (port dropped by V3)"
            out[tag + " spans"] += 1
            if near == k:
                out[tag + " nearest is k"] += 1
            elif used and len(worst) < 10:
                worst.append(
                    {
                        "port": port,
                        "k": k,
                        "nearest": near,
                        "gap_us": (e - ends[min(k, len(ends) - 1)]) / 1000,
                    }
                )
    return {"run": Path(run).name, **out, "examples": worst}


if __name__ == "__main__":
    lines = [
        ln
        for f in sys.argv[2:]
        for ln in Path(f).read_text().splitlines()
        if ln.strip() and ln.split()[1] != "none"
    ]
    with Pool(int(sys.argv[1])) as p:
        json.dump(p.map(one, lines, chunksize=1), sys.stdout, indent=1)
