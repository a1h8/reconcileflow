"""Two counts the proof of the recipe needs, per cell.
H5: true links (one call per request by the truth) whose size gap falls outside the
rule the main judgment learnt (signature, else window): 0 expected.
Truth by the clock: load-gen / fake-upstream records attached to a connection by
the clock (four-tuple opened more than once), not by the only connection seen.

usage: conditions.py <main-judgment.json> <procs> <runs-list>... > out.json
Run from m0-harness with PYTHONPATH=../src:.
"""

import json
import sys
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path

import probe_recipe as pr

MAIN: dict = {}


def one(line):
    run = line.split()[0]
    cell = Path(run) / "n1"
    meta = json.loads((cell / "cell.json").read_text())
    rows = list(pr._jsonl(cell / "probe.jsonl"))
    requests, calls = pr.lives(rows, meta["hop_ports"][0], meta["fake_upstream_port"])
    openings = pr._Openings(rows)
    seen = {(x.conn, x.incarnation) for x in [*requests, *calls]}
    per_conn = defaultdict(set)
    for c, k in seen:
        per_conn[c].add(k)
    by_clock = {"lg": 0, "fu": 0}
    lg, fu = {}, {}
    for side, path in (("lg", "lg.jsonl"), ("fu", "fu.jsonl")):
        for r in pr._jsonl(cell / path):
            at = pr._record_connection(r, openings, seen)
            if at is None:
                continue
            if len(per_conn[at[0]]) > 1:
                by_clock[side] += 1
            if side == "lg" and (r.get("wire_rank") or r.get("stream_id")):
                lg[at[0][0], at[1], r.get("wire_rank") or r.get("stream_id")] = r["trace_id"]
            if side == "fu" and r.get("stream_offset") is not None:
                fu[at[0][0], at[1], r["stream_offset"]] = r["trace_id"]
    req_tid = [lg.get((r.conn[0], r.incarnation, r.rank)) for r in requests]
    call_of = defaultdict(list)
    for j, x in enumerate(calls):
        t = pr._call_tid(x, fu)
        if t:
            call_of[t].append(j)
    m = MAIN[Path(run).name]
    sig, win = m.get("size_signature"), m.get("size_window")
    rule = pr.Lattice(*sig) if sig else tuple(win) if win else None
    starts = sorted(r.start for r in requests)
    cut = starts[len(starts) // 2]
    out = {
        "run": Path(run).name,
        "rule": sig or win,
        "rule_kind": "signature" if sig else "window" if win else None,
    }
    for half in ("all", "second"):
        n = outside = 0
        for i, t in enumerate(req_tid):
            if (
                not t
                or len(call_of.get(t, [])) != 1
                or (half == "second" and requests[i].start < cut)
            ):
                continue
            gap = requests[i].answer - calls[call_of[t][0]].answer
            n += 1
            inside = rule is None or (
                gap in rule if isinstance(rule, pr.Lattice) else rule[0] <= gap <= rule[1]
            )
            outside += not inside
        out[f"true_links_{half}"] = n
        out[f"outside_rule_{half}"] = outside
    out["records_attached_by_clock"] = by_clock
    return out


if __name__ == "__main__":
    MAIN.update({c["run"]: c for c in json.loads(Path(sys.argv[1]).read_text())})
    lines = [ln for f in sys.argv[3:] for ln in Path(f).read_text().splitlines() if ln.strip()]
    with Pool(int(sys.argv[2])) as p:
        json.dump(p.map(one, lines, chunksize=1), sys.stdout, indent=1)
