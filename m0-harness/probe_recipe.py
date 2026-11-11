"""The recipe on the probe's events, inside the proxy (phase 4 spec 8.13).

The probe, run on the proxy's inbound port and on fake-upstream's port with
-both-directions, sees from the kernel, for each connection, the proxy read a
request and send its response (inbound), send a call and read its answer
(upstream). On one HTTP/1.1 connection without pipelining, asks and answers
alternate: an exchange starts at the first ask after an answer and ends at the
first answer after it. The first exchange of a TLS connection is the
handshake, left out.

- request life = [the proxy's first read of the asks, the proxy sends its response];
- call life    = [the proxy's last send of the asks, the proxy reads its answer].

The asks of an exchange may hold more than one message: a TLS client sends
its Finished right after the handshake and the server answers nothing, so an
idle connection's Finished joins its first call (seen at c200: seconds
before). Hence these bounds: a request's starts at its first read (its life
can only widen), a call's at its last send (its life can only narrow). Either
way a call stays nested in its own request.

Both bounds are system calls of one process on one clock: a call cannot start
before its request is read, nor end after the proxy answered. Then
recipe_links (reconcileflow.correlation.recipe) links requests to calls, with
the probe's declared losses as lossy intervals.

Truth, never given to the linker: load-gen's log gives each request's rank on
its connection and its trace id; fake-upstream's (-stream-offsets) gives each
call's byte position on its connection and its trace id. Judged on the
requests of the second half in time; a recipe learnt on the first half.

Usage: probe_recipe.py <cell-dir> [--json]
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from reconcileflow.correlation.recipe import learn_recipe, recipe_links
from reconcileflow.correlation.timing import Timed

HOP_NAME = "proxy -> upstream"


@dataclass(frozen=True, slots=True)
class Exchange:
    conn: tuple[int, int]  # (client port, server port)
    rank: int  # 1 for the first exchange after the handshake
    start: int  # ns, monotonic
    end: int
    offsets: tuple[int, ...]  # byte positions of the asks (upstream sends only)


def _jsonl(path: Path):
    with path.open() as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def lives(probe: Path, hop_port: int, fu_port: int, handshake: bool = True):
    """(requests, calls): the exchanges at the proxy's end of each connection."""
    rows = sorted(
        ((r["timestamp_ns"], i, r) for i, r in enumerate(_jsonl(probe))),
        key=lambda x: (x[0], x[1]),
    )
    base: dict[tuple[int, int], int] = {}
    seen: dict[tuple[int, int], list] = defaultdict(list)  # conn -> [(ts, ask?, offset)]
    for ts, _, r in rows:
        kind = r.get("kind")
        conn = (r.get("client_port"), r.get("server_port"))
        if conn[1] not in (hop_port, fu_port):
            continue
        if kind == "connect":
            base.setdefault(conn, r["seq"])
            continue
        s2c = r.get("dir") == "s2c"
        if conn[1] == hop_port:  # the proxy is the server: it reads asks, sends answers
            if kind == "recv" and not s2c:
                seen[conn].append((ts, True, None))
            elif kind == "send" and s2c:
                seen[conn].append((ts, False, None))
        else:  # the proxy is the client: it sends asks, reads answers
            if kind == "send" and not s2c:
                offset = (r["seq"] - base[conn]) % 2**32 if conn in base else None
                seen[conn].append((ts, True, offset))
            elif kind == "recv" and s2c:
                seen[conn].append((ts, False, None))
    requests, calls = [], []
    for conn, events in seen.items():
        upstream = conn[1] != hop_port
        exchanges, open_ = [], None
        for ts, ask, offset in events:
            if ask:
                if open_ is None:
                    open_ = [ts, [], ts]
                open_[2] = ts
                if offset is not None:
                    open_[1].append(offset)
            elif open_ is not None:
                start = open_[2] if upstream else open_[0]
                exchanges.append((start, ts, tuple(open_[1])))
                open_ = None
        if handshake:
            exchanges = exchanges[1:]
        out = requests if conn[1] == hop_port else calls
        out += [Exchange(conn, k + 1, s, e, o) for k, (s, e, o) in enumerate(exchanges)]
    return requests, calls


def _lossy(probe: Path) -> list[tuple[int, int]]:
    """Intervals over which the probe declared a loss: between two drop
    records whose count grew (from the start when the first is not 0)."""
    marks = sorted(
        (r["timestamp_ns"], r["drops"]) for r in _jsonl(probe) if r.get("kind") == "drops"
    )
    out, previous_ts, previous = [], None, 0
    for ts, count in marks:
        if count > previous:
            out.append((-(2**63) if previous_ts is None else previous_ts, ts))
        previous_ts, previous = ts, count
    return out


def _port_rank(conn_key: str) -> tuple[int, int] | None:
    four, _, incarnation = conn_key.partition("|")
    if incarnation not in ("", "0"):
        return None  # a reused four-tuple: left out
    client, _, server = four.partition("-")
    return int(client.rsplit(":", 1)[1]), int(server.rsplit(":", 1)[1])


def judge(cell: Path) -> dict:
    meta = json.loads((cell / "cell.json").read_text())
    hop, fu = meta["hop_ports"][0], meta["fake_upstream_port"]
    probe = cell / "probe.jsonl"
    requests, calls = lives(probe, hop, fu)

    lg = {}
    for r in _jsonl(cell / "lg.jsonl"):
        conn = _port_rank(r.get("conn_key", ""))
        if conn is not None:
            lg[conn[0], r["stream_id"]] = r["trace_id"]
    fu_truth = {}
    for r in _jsonl(cell / "fu.jsonl"):
        conn = _port_rank(r.get("conn_key", ""))
        if conn is not None and r.get("stream_offset") is not None:
            fu_truth[conn[0], r["stream_offset"]] = r["trace_id"]
    req_tid = [lg.get((r.conn[0], r.rank)) for r in requests]
    call_tid = [
        next((fu_truth[x.conn[0], o] for o in x.offsets if (x.conn[0], o) in fu_truth), None)
        for x in calls
    ]

    req = [Timed("req", i, "proxy", "inbound", r.start, r.end) for i, r in enumerate(requests)]
    call = [Timed("call", j, "proxy", "outbound", x.start, x.end) for j, x in enumerate(calls)]
    calls_of = defaultdict(list)
    for j, tid in enumerate(call_tid):
        if tid is not None:
            calls_of[tid].append(j)

    violations = 0
    for i, tid in enumerate(req_tid):
        own = calls_of.get(tid, []) if tid else []
        if len(own) == 1:
            x, r = calls[own[0]], requests[i]
            violations += not (r.start <= x.start and x.end <= r.end)

    starts = sorted(r.start for r in requests)
    cut = starts[len(starts) // 2] if starts else 0
    examples = [
        (req[i], {HOP_NAME: [call[calls_of[tid][0]]]})
        for i, tid in enumerate(req_tid)
        if requests[i].start < cut and tid and len(calls_of.get(tid, [])) == 1
    ]
    recipe = learn_recipe("proxy", examples, source="first half, truth")
    report = {
        "requests": len(requests),
        "calls": len(calls),
        "requests_without_truth": req_tid.count(None),
        "calls_without_truth": call_tid.count(None),
        "causality_violations": violations,
        "lossy_intervals": len(_lossy(probe)),
        "calibration": len(examples),
        "recipe": None if recipe is None else list(recipe.calls),
    }
    counts, refused = Counter(), Counter()
    if recipe is not None and recipe.calls == (HOP_NAME,):
        result = recipe_links(req, {HOP_NAME: call}, recipe, _lossy(probe))
        linked = {x.upstream[1]: x.downstream[1] for x in result.links}
        status = {u.observation[1]: u.status.name for u in result.undecided if u.side == "inbound"}
        for i, tid in enumerate(req_tid):
            if requests[i].start < cut or not tid or len(calls_of.get(tid, [])) != 1:
                continue
            counts["judged"] += 1
            got = linked.get(i)
            if got is None:
                counts["refused"] += 1
                refused[status.get(i, "?")] += 1
            elif call_tid[got] is None:
                counts["unjudgeable"] += 1
            else:
                counts["right" if call_tid[got] == tid else "wrong"] += 1
    judged = counts["judged"]
    report |= {
        "judged": judged,
        "right": counts["right"],
        "wrong": counts["wrong"],
        "refused": counts["refused"],
        "unjudgeable": counts["unjudgeable"],
        "refused_by_status": dict(refused),
        "coverage": counts["right"] / judged if judged else 0.0,
    }
    return report


def main(argv: list[str]) -> None:
    report = judge(Path(argv[1]))
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    print(f"{Path(argv[1]).parent.name}: " + " ".join(f"{k} {v}" for k, v in report.items()))


if __name__ == "__main__":
    main(sys.argv)
