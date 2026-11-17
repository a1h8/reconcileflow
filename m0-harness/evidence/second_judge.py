"""Second judge: check the links the main judgment holds against the agent's
spans, attached by port and order only, never by comparing the agent's clock
with the probe's. Server spans of the proxy carry the trace id read from the
traceparent header; client spans carry the id written in the request path
(/truth-<id>), another carrier. Client spans are ranked by their end (the
answers of an HTTP/1.1 connection come back in order; validated by
validate_end_order.py); with --first-pass, by their start, ports whose spans
overlap set aside (the first pass, kept to rerun it).

usage: second_judge.py [--first-pass] <procs> <runs-list>... > out.json
Run from m0-harness with PYTHONPATH=../src:.:evidence (agent cells only).
"""

import json
import os
import re
import sys
from collections import Counter, defaultdict
from itertools import pairwise
from multiprocessing import Pool
from pathlib import Path

import probe_recipe as pr

# Read at import, so that worker processes see it however they are started.
BY_END = os.environ.get("SECOND_JUDGE_FIRST_PASS") != "1"

TRUTH = re.compile(r"/truth-([0-9a-f]{32})")


def agent_spans(path: Path, hop_port: int, fu_port: int):
    """(server spans by client port, client spans by relay port): lists of
    (start, end, truth), sorted by start."""
    server, client = defaultdict(list), defaultdict(list)
    with path.open() as f:
        for line in f:
            if not line.startswith("["):
                continue
            for s in json.loads(line):
                if (
                    s.get("type") == "HTTP"
                    and s["kind"] == "SPAN_KIND_SERVER"
                    and s["hostName"] == "hop-1"
                ):
                    server[int(s["peerPort"])].append(
                        (int(s["start"]), int(s["end"]), s["traceID"])
                    )
                elif (
                    s.get("type") == "HTTPClient"
                    and s["kind"] == "SPAN_KIND_CLIENT"
                    and s["peerName"] == "hop-1"
                    and int(s["hostPort"]) == fu_port
                ):
                    m = TRUTH.search(s["attributes"].get("url", ""))
                    client[int(s["peerPort"])].append(
                        (int(s["start"]), int(s["end"]), m and m.group(1))
                    )
    for d in (server, client):
        for v in d.values():
            v.sort()
    return server, client


def attach(exchanges, spans_by_port, reasons, by_end=False):
    """Exchange index -> agent truth, by rank on the port; ports that cannot
    be attached are counted in reasons."""
    by_port = defaultdict(list)
    for i, x in enumerate(exchanges):
        by_port[x.conn[0]].append(i)
    out = {}
    for port, idx in by_port.items():
        if len({exchanges[i].incarnation for i in idx}) > 1:
            reasons["several incarnations"] += 1
            continue
        spans = spans_by_port.get(port, [])
        idx.sort(key=lambda i: exchanges[i].rank)
        if by_end:  # validated by validate_end_order.py (V1-V3)
            spans = sorted(spans, key=lambda s: (s[1], s[0]))
        elif any(a[1] > b[0] for a, b in pairwise(spans)):
            reasons["overlapping spans"] += 1
            continue
        if len(idx) - len(spans) not in (0, 1):  # the probe may see the closing exchange too
            reasons["count differs"] += 1
            continue
        for k, i in enumerate(idx[: len(spans)]):
            out[i] = spans[k][2]
    return out


def one(line):
    run, agent, c, load = line.split()[:4]
    cell = Path(run) / "n1"
    captured = {}
    real_lives, real_links = pr.lives, pr.recipe_links

    def lives(*a, **k):
        captured["lives"] = real_lives(*a, **k)
        return captured["lives"]

    def recipe_links(*a, **k):
        captured["result"] = real_links(*a, **k)
        return captured["result"]

    pr.lives, pr.recipe_links = lives, recipe_links
    try:
        main = pr.judge(cell, "declared", sizes="lattice", locate=True)
    finally:
        pr.lives, pr.recipe_links = real_lives, real_links
    requests, calls = captured["lives"]
    meta = json.loads((cell / "cell.json").read_text())
    server, client = agent_spans(
        cell / "agent.out", meta["hop_ports"][0], meta["fake_upstream_port"]
    )
    reasons_in, reasons_out = Counter(), Counter()
    t_req = attach(requests, server, reasons_in)
    t_call = attach(calls, client, reasons_out, by_end=BY_END)

    # The main judge's truths, by its own functions (as in probe_recipe.judge).
    rows = list(pr._jsonl(cell / "probe.jsonl"))
    openings = pr._Openings(rows)
    seen = {(x.conn, x.incarnation) for x in [*requests, *calls]}
    lg, fu = {}, {}
    for r in pr._jsonl(cell / "lg.jsonl"):
        at, rank = (
            pr._record_connection(r, openings, seen),
            r.get("wire_rank") or r.get("stream_id"),
        )
        if at is not None and rank:
            lg[at[0][0], at[1], rank] = r["trace_id"]
    for r in pr._jsonl(cell / "fu.jsonl"):
        at = pr._record_connection(r, openings, seen)
        if at is not None and r.get("stream_offset") is not None:
            fu[at[0][0], at[1], r["stream_offset"]] = r["trace_id"]
    m_req = {i: lg.get((r.conn[0], r.incarnation, r.rank)) for i, r in enumerate(requests)}
    m_call = {j: pr._call_tid(x, fu) for j, x in enumerate(calls)}
    both_req = [i for i in t_req if t_req[i] and m_req.get(i)]
    both_call = [j for j in t_call if t_call[j] and m_call.get(j)]
    truth_agreement = {
        "requests_both": len(both_req),
        "requests_differ": sum(t_req[i] != m_req[i] for i in both_req),
        "calls_both": len(both_call),
        "calls_differ": sum(t_call[j] != m_call[j] for j in both_call),
    }

    starts = sorted(r.start for r in requests)
    cut = starts[len(starts) // 2]
    links = [(x.upstream[1], x.downstream[1]) for x in captured["result"].links]
    held = [(i, j) for i, j in links if requests[i].start >= cut]
    checked = [(i, j) for i, j in held if t_req.get(i) and t_call.get(j)]
    disagree = [(i, j) for i, j in checked if t_req[i] != t_call[j]]
    examples = [
        {
            "request": [requests[i].conn, requests[i].incarnation, requests[i].rank, t_req[i]],
            "call": [calls[j].conn, calls[j].incarnation, calls[j].rank, t_call[j]],
        }
        for i, j in disagree[:20]
    ]
    return {
        "run": Path(run).name,
        "agent": agent,
        "c": int(c),
        "load": int(load),
        "main_right": main["right"],
        "main_wrong": main["wrong"],
        "held_second_half": len(held),
        "checked": len(checked),
        "checked_share": len(checked) / len(held) if held else 0.0,
        "disagreements": len(disagree),
        "examples": examples,
        "truth_agreement": truth_agreement,
        "inbound_ports_dropped": dict(reasons_in),
        "upstream_ports_dropped": dict(reasons_out),
        "requests_attached": len(t_req),
        "calls_attached": len(t_call),
        "requests": len(requests),
        "calls": len(calls),
    }


if __name__ == "__main__":
    if sys.argv[1] == "--first-pass":
        os.environ["SECOND_JUDGE_FIRST_PASS"] = "1"
        BY_END = False
        del sys.argv[1]
    lines = [
        ln
        for f in sys.argv[2:]
        for ln in Path(f).read_text().splitlines()
        if ln.strip() and ln.split()[1] != "none"
    ]
    with Pool(int(sys.argv[1])) as p:
        out = p.map(one, lines, chunksize=1)
    json.dump(out, sys.stdout, indent=1)
