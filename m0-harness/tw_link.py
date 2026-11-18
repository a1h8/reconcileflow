"""TraceWeaver on the probe's events (phase 4 spec 8.27): runs in
TraceWeaver's own environment (their pinned versions, offline), never in ours.

  <their venv>/bin/python tw_link.py <their python dir> <lives.json> <links.json>

<their python dir> is src/trace_reconstructor/ports/python of their
repository; <lives.json> comes from ``probe_compare.export_lives``. Their
method ``MaxScoreBatchSubsetWithSkips`` (TraceWeaverV3, the one of their
paper) links each request to one call, or to none ("Skip"). Written out:
{"links": {request index: call index}, "skips": [request index, ...]}.

What they are given, and nothing else: one service (the proxy), one callee
(the upstream), each span with its own trace id (no id shared by a request
and its call), times in microseconds (floats: the probe's nanoseconds are
kept). Their code reads the truth only to print counts on this path
(read on 2026-11-16, ``FindAssignments``, ``ComputeEpPairDistParams5``):
it gets a placeholder truth, every request "NA". Their invocation graph,
which they learn from the truth, is the trivial one of a single callee.

One fix of theirs, declared (spec 8.27.2): ``ScoreAssignmentAsPerInvocationGraph``
returns a bare 0 for an all-skip assignment where its callers unpack
(score, sub_scores); with one callee and requests that made no call (warm-up,
TLS close), every skip candidate takes that branch and their code stops
(TypeError). Wrapped here to return (0, sub_scores), the value its twin
``...Graph2`` returns; their files are not changed.
"""

import json
import sys
from pathlib import Path


def run(their_dir: str, lives: dict) -> dict:
    sys.path.insert(0, their_dir)
    import networkx as nx
    from algorithms.traceweaver_v1 import TraceWeaverV1
    from algorithms.traceweaver_v3 import TraceWeaverV3
    from spans import Span

    score = TraceWeaverV1.ScoreAssignmentAsPerInvocationGraph

    def fixed(self, assignment, invocation_graph, out_eps, sub_scores, normalized=False):
        got = score(self, assignment, invocation_graph, out_eps, sub_scores, normalized)
        return (got, sub_scores) if isinstance(got, (int, float)) else got

    TraceWeaverV1.ScoreAssignmentAsPerInvocationGraph = fixed

    all_spans, all_processes = {}, {}

    def span(tid, sid, start, end, kind, process, refs=()):
        s = Span(tid, sid, start, end - start, "GET", list(refs), process, kind, {})
        all_spans[s.GetId()] = s
        all_processes.setdefault(tid, {})[process] = process
        return s

    ins = []
    for i, (start, end) in enumerate(lives["requests"]):
        ins.append(span(f"r{i}", f"r{i}", start, end, "server", "proxy"))
    outs = []
    for j, (start, end) in enumerate(lives["calls"]):
        child = span(f"c{j}", f"u{j}", start, end, "server", "upstream", [f"c{j}"])
        out = span(f"c{j}", f"c{j}", start, end, "client", "proxy")
        out.AddChild(child.GetId())
        outs.append(out)
    in_ep = ins[0].GetParentProcess(all_processes, all_spans)
    in_parts = {in_ep: sorted(ins, key=lambda x: (x.start_mus, x.start_mus + x.duration_mus))}
    out_parts = {
        "upstream": sorted(outs, key=lambda x: (x.start_mus, x.start_mus + x.duration_mus))
    }
    placeholder = {"upstream": {s.GetId(): ("NA", "NA") for s in ins}}
    graph = nx.DiGraph()
    graph.add_node("upstream")

    tw = TraceWeaverV3(all_spans, all_processes)
    pred, _, _, _, _, _ = tw.FindAssignments(
        "MaxScoreBatchSubsetWithSkips", "proxy", in_parts, out_parts, False, [], placeholder, graph
    )
    links, skips = {}, []
    for (tid, _), got in pred["upstream"].items():
        i = int(tid[1:])
        if got == ("Skip", "Skip"):
            skips.append(i)
        elif got != ("NA", "NA"):
            links[i] = int(got[0][1:])
    return {"links": links, "skips": sorted(skips)}


if __name__ == "__main__":
    their_dir, lives_path, out_path = sys.argv[1:4]
    result = run(their_dir, json.loads(Path(lives_path).read_text()))
    Path(out_path).write_text(json.dumps(result))
