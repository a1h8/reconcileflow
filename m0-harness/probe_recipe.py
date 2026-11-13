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

Evaluation (spec 8.14): recipe="declared" (each request makes one call
upstream, as an operator knows its chain; no truth at all); controls K2b
(drop permille of the probe's send and read events, each declared lost like
the probe would) and K5 (the calls' truth shifted by one rank on each
upstream connection: the judge must see the wrong links); ranks_ok checks
that each inbound connection holds load-gen's requests (one closing exchange
allowed).

Answer sizes (spec 8.17): sizes="learned" adds the window the proxy's
answers keep from their calls' answers, learnt from the links the timings
alone hold (no truth); control K6 shifts it by size_shift bytes: a wrong
window must refuse (SIZE_MISMATCH), never link wrong.

Losses located (spec 8.18, locate=True). The probe counts what it loses,
not where; refusing every group that meets the interval between two drop
records (one a second) refuses nearly all under load. But each send and
read carries its byte positions: a lost one leaves a hole on its own
connection, between two events seen (seq_holes), and locate_losses widens
each hole to the lives of the exchanges whose events reach into it (their
answer sizes are not used). Those zones replace the intervals; a declared
loss with no hole in its interval stays whole.
Why no wrong link is added. Let X be an exchange whose observation differs
from the truth because of a loss. (1) A lost send or read lies between two
events a, b of its stream, or at an edge the other side sees: a hole;
unless it and its counterpart on the other side are both lost at the same
edge (open risk). (2) On a stream, byte order is time order: the lost
event's instant is in [t(a), t(b)]. (3) If the loss changes X's life, the
lost event is a bound of it or changes how exchanges are cut: X's events
reach into [t(a), t(b)], X is damaged and its zone holds its observed life;
if it changes only X's size (a piece of its answer), X is damaged and its
size is dropped. (4) Let R be the true request of a call X: R's life holds
X's true life. If X's life changed, X's true life meets [t(a), t(b)], so
does R's, and any group holding R meets a zone: refused. If only X's size
changed, it is dropped and R keeps X among its options. A request damaged
likewise meets its zone. A call lost whole lies between the events around
it on its connection: its request meets the zone, refused, never linked
to another call. (5) Outside refused groups no observation is wrong and no
wrong size is used: the reasoning of recipe_links holds as it is.

Usage: probe_recipe.py <cell-dir> [--json] [--declared] [--drop N --seed S] [--shift]
                       [--sizes [--size-shift B]] [--locate]
       probe_recipe.py --series <runs-list>   (the spec 8.14 verdict)
"""

from __future__ import annotations

import json
import random
import sys
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path

from reconcileflow.correlation.recipe import (
    Recipe,
    Sizes,
    learn_recipe,
    learn_window,
    recipe_links,
)
from reconcileflow.correlation.timing import Timed

HOP_NAME = "proxy -> upstream"


@dataclass(frozen=True, slots=True)
class Exchange:
    conn: tuple[int, int]  # (client port, server port)
    rank: int  # 1 for the first exchange after the handshake
    start: int  # ns, monotonic
    end: int
    offsets: tuple[int, ...]  # byte positions of the asks (upstream sends only)
    answer: int = 0  # bytes of the answer, at the proxy's end, until the next ask
    span: tuple[int, int] = (0, 0)  # first and last event of the exchange, answer pieces included


def _jsonl(path: Path):
    with path.open() as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def lives(probe, hop_port: int, fu_port: int, handshake: bool = True):
    """(requests, calls): the exchanges at the proxy's end of each connection.
    probe: probe.jsonl, or its rows."""
    records = _jsonl(probe) if isinstance(probe, Path) else probe
    rows = sorted(
        ((r["timestamp_ns"], i, r) for i, r in enumerate(records)),
        key=lambda x: (x[0], x[1]),
    )
    base: dict[tuple[int, int], int] = {}
    seen: dict[tuple[int, int], list] = defaultdict(list)  # conn -> [(ts, ask?, offset, bytes)]
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
                seen[conn].append((ts, True, None, r["len"]))
            elif kind == "send" and s2c:
                seen[conn].append((ts, False, None, r["len"]))
        else:  # the proxy is the client: it sends asks, reads answers
            if kind == "send" and not s2c:
                offset = (r["seq"] - base[conn]) % 2**32 if conn in base else None
                seen[conn].append((ts, True, offset, r["len"]))
            elif kind == "recv" and s2c:
                seen[conn].append((ts, False, None, r["len"]))
    requests, calls = [], []
    for conn, events in seen.items():
        upstream = conn[1] != hop_port
        exchanges, open_ = [], None
        for ts, ask, offset, length in events:
            if ask:
                if open_ is None:
                    open_ = [ts, [], ts]
                open_[2] = ts
                if offset is not None:
                    open_[1].append(offset)
            elif open_ is not None:
                start = open_[2] if upstream else open_[0]
                exchanges.append([start, ts, tuple(open_[1]), length, open_[0], ts])
                open_ = None
            elif exchanges:  # more of the last answer
                exchanges[-1][3] += length
                exchanges[-1][5] = ts
        if handshake:
            exchanges = exchanges[1:]
        out = requests if conn[1] == hop_port else calls
        out += [
            Exchange(conn, k + 1, s, e, o, a, (f, z))
            for k, (s, e, o, a, f, z) in enumerate(exchanges)
        ]
    return requests, calls


@dataclass(frozen=True, slots=True, order=True)
class Hole:
    """Events lost on a connection, between two instants (spec 8.18)."""

    conn: tuple[int, int]
    start: int
    end: int
    edge: bool = False  # at a stream's first or last byte: may be the protocol's close


def _signed(x: int) -> int:
    return (x + 2**31) % 2**32 - 2**31


def seq_holes(rows) -> list[Hole]:
    """Where the probe lost a send or a read (phase 4 spec 8.18.6). On one
    connection and direction, the sender's and the reader's events each
    follow on in byte positions; a gap between two of them is a lost event
    between their instants. The first and last bytes of a direction are seen
    by both sides: a side that starts later or ends earlier lost its first or
    last event (an edge, open from the connection's first event or to its
    last). Bytes sent after the reader closed could never be read: not a
    loss (each side's TLS close alert, at a close). Bytes left unread before
    the reader closed may be unread or a lost read: a hole up to that close,
    where the read would have been. Runs recorded before the probe took the
    peer's close into account
    (bbfa835) date the read that took it one byte late: a one-byte gap before
    the last read of a stream is that, not a loss."""
    streams: dict = defaultdict(list)
    first: dict = {}
    last: dict = {}
    closed: dict = {}  # (conn, pid) -> first close
    for i, r in enumerate(rows):
        if "client_port" not in r:
            continue
        conn = (r["client_port"], r["server_port"])
        ts = r["timestamp_ns"]
        first[conn] = min(first.get(conn, ts), ts)
        last[conn] = max(last.get(conn, ts), ts)
        if r.get("kind") in ("send", "recv"):
            streams[conn, r.get("dir") or "c2s", r["kind"]].append(
                [ts, i, r["seq"], r["len"], r.get("pid")]
            )
        elif r.get("kind") == "close":
            closed[conn, r.get("pid")] = min(closed.get((conn, r.get("pid")), ts), ts)
    for key, ev in streams.items():
        ev.sort()
        if key[2] == "recv" and len(ev) >= 2 and _signed(ev[-1][2] - ev[-2][2] - ev[-2][3]) == 1:
            ev[-1][2] -= 1  # the read that took the peer's close, before bbfa835
    holes = []
    for (conn, _, _), ev in streams.items():
        for a, b in pairwise(ev):
            if (a[2] + a[3] - b[2]) % 2**32:
                holes.append(Hole(conn, a[0], b[0]))
    for conn, way in {(c, w) for c, w, _ in streams}:
        sent, read = streams.get((conn, way, "send"), []), streams.get((conn, way, "recv"), [])
        if not sent or not read:
            holes.append(Hole(conn, first[conn], last[conn], True))
            continue
        lead = _signed(sent[0][2] - read[0][2])
        if lead:  # the side that starts later lost its first event
            later = sent if lead > 0 else read
            holes.append(Hole(conn, first[conn], later[0][0], True))
        end = read[-1][2] + read[-1][3]
        tail = _signed(sent[-1][2] + sent[-1][3] - end)
        if tail < 0:  # read beyond what was sent: the sender lost its last send
            holes.append(Hole(conn, sent[-1][0], last[conn], True))
        elif tail > 0:  # sent beyond what was read
            close = closed.get((conn, read[-1][4]))
            unread = [e for e in sent if _signed(e[2] + e[3] - end) > 0]
            if close is None or any(e[0] <= close for e in unread):
                holes.append(Hole(conn, read[-1][0], last[conn] if close is None else close, True))
    return sorted(holes)


def locate_losses(rows, hop_port, fu_port, requests, calls, declared, holes=None):
    """(zones, damaged requests, damaged calls, unlocated): spec 8.18.6.
    The probe counts each lost event when it happens, so a loss lies in an
    interval where its count grew (lo, hi, count): a hole that meets none is
    the protocol, not a loss, and is left out. A zone is a hole's interval
    widened to the lives of the exchanges of its connection whose events
    reach into it (damaged: their sizes are not to be used). A declared loss
    with no hole in its interval stays whole: unlocated."""
    lossy = _Union((lo, hi) for lo, hi, n in declared if n > 0)
    holes = [
        h for h in (seq_holes(rows) if holes is None else holes) if lossy.meets(h.start, h.end)
    ]
    by_conn: dict = defaultdict(list)
    for inbound, exchanges in ((True, requests), (False, calls)):
        for i, x in enumerate(exchanges):
            by_conn[x.conn].append((x, i, inbound))
    zones, damaged_requests, damaged_calls = [], set(), set()
    for h in holes:
        lo, hi = h.start, h.end
        for x, i, inbound in by_conn.get(h.conn, ()):
            if x.span[0] <= h.end and h.start <= x.span[1]:
                (damaged_requests if inbound else damaged_calls).add(i)
                lo, hi = min(lo, x.start), max(hi, x.end)
        zones.append((lo, hi))
    spans = _Union((h.start, h.end) for h in holes)
    unlocated = [(lo, hi) for lo, hi, n in declared if n > 0 and not spans.meets(lo, hi)]
    return zones, damaged_requests, damaged_calls, unlocated


class _Union:
    """Closed intervals merged where they meet; meets() in log time."""

    def __init__(self, intervals):
        merged: list[list[int]] = []
        for a, b in sorted(intervals):
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        self.starts = [a for a, _ in merged]
        self.ends = [b for _, b in merged]

    def meets(self, lo: int, hi: int) -> bool:
        k = bisect_left(self.ends, lo)  # the first interval that ends at or after lo
        return k < len(self.ends) and self.starts[k] <= hi


def _declared(rows) -> list[tuple[int, int, int]]:
    """The probe's declared losses: (from, to, count) between two drop
    records whose count grew (from the start when the first is not 0)."""
    marks = sorted((r["timestamp_ns"], r["drops"]) for r in rows if r.get("kind") == "drops")
    out, previous_ts, previous = [], None, 0
    for ts, count in marks:
        if count > previous:
            out.append((-(2**63) if previous_ts is None else previous_ts, ts, count - previous))
        previous_ts, previous = ts, count
    return out


def _lossy(rows) -> list[tuple[int, int]]:
    """Intervals over which the probe declared a loss: between two drop
    records whose count grew (from the start when the first is not 0)."""
    marks = sorted((r["timestamp_ns"], r["drops"]) for r in rows if r.get("kind") == "drops")
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


def _dropped(rows, drop: int, seed: int):
    """K2b: drop permille of the send and read events at random, and declare
    each lost over the interval between the drop records around it."""
    events = [i for i, r in enumerate(rows) if r.get("kind") in ("send", "recv")]
    gone = set(random.Random(seed).sample(events, round(len(events) * drop / 1000)))
    marks = sorted(r["timestamp_ns"] for r in rows if r.get("kind") == "drops")
    declared = []
    for i in gone:
        ts = rows[i]["timestamp_ns"]
        before = [m for m in marks if m <= ts]
        after = [m for m in marks if m >= ts]
        declared.append((before[-1] if before else -(2**63), after[0] if after else 2**63))
    return [r for i, r in enumerate(rows) if i not in gone], declared


def _max_stream_ids(cell: Path, hop: int) -> dict[int, int]:
    """Per inbound client port, load-gen's highest request number (warm-up,
    measured and tail requests alike)."""
    out: dict[int, int] = {}
    for name in sorted(p.name for p in cell.glob("*.jsonl")):
        if not (name.startswith(("lg", "warmup", "tail"))):
            continue
        for r in _jsonl(cell / name):
            conn = _port_rank(r.get("conn_key", ""))
            if conn is not None and conn[1] == hop and r.get("stream_id") is not None:
                out[conn[0]] = max(out.get(conn[0], 0), r["stream_id"])
    return out


def judge(
    cell: Path,
    recipe: str = "learned",
    drop: int | None = None,
    seed: int = 0,
    shift: bool = False,
    sizes: str = "none",
    size_shift: int = 0,
    locate: bool = False,
) -> dict:
    meta = json.loads((cell / "cell.json").read_text())
    hop, fu = meta["hop_ports"][0], meta["fake_upstream_port"]
    rows = list(_jsonl(cell / "probe.jsonl"))

    # Truth, from every event the probe wrote: an exchange is known by its
    # connection and its end, which survive the K2b control.
    full_requests, full_calls = lives(rows, hop, fu)
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
    req_truth = {(r.conn, r.end): lg.get((r.conn[0], r.rank)) for r in full_requests}
    call_truth = {
        (x.conn, x.end): next(
            (fu_truth[x.conn[0], o] for o in x.offsets if (x.conn[0], o) in fu_truth), None
        )
        for x in full_calls
    }
    control = None
    if shift:  # K5: each call takes the truth of the next call on its connection
        control = "K5 call truth shifted by one rank"
        by_conn = defaultdict(list)
        for x in full_calls:
            by_conn[x.conn].append(x)
        for xs in by_conn.values():
            xs.sort(key=lambda x: x.rank)
            for x, y in zip(xs, [*xs[1:], None], strict=True):
                call_truth[x.conn, x.end] = None if y is None else call_truth.get((y.conn, y.end))
    most = _max_stream_ids(cell, hop)
    per_conn = Counter(r.conn[0] for r in full_requests)
    mismatched = sum(1 for p, n in per_conn.items() if n - most.get(p, 0) not in (0, 1))

    declared = _declared(rows)
    if drop is not None:
        control = f"K2b drop={drop} seed={seed}"
        rows, simulated = _dropped(rows, drop, seed)
        declared += [(lo, hi, 1) for lo, hi in simulated]
    requests, calls = lives(rows, hop, fu)
    damaged_requests, damaged_calls, located = set(), set(), {}
    if locate:  # spec 8.18: each loss narrowed to where its bytes are missing
        holes = seq_holes(rows)
        zones, damaged_requests, damaged_calls, unlocated = locate_losses(
            rows, hop, fu, requests, calls, declared, holes
        )
        lossy = sorted(zones + unlocated)
        certain = sum(1 for h in holes if not h.edge)  # a gap inside a stream is a loss
        located = {
            "loss_zones": len(zones),
            "unlocated_losses": len(unlocated),
            "damaged": len(damaged_requests) + len(damaged_calls),
            "holes_exceed_drops": certain > sum(n for _, _, n in declared),
        }
    else:
        lossy = sorted((lo, hi) for lo, hi, _ in declared)
    req_tid = [req_truth.get((r.conn, r.end)) for r in requests]
    call_tid = [call_truth.get((x.conn, x.end)) for x in calls]

    req = [Timed("req", i, "proxy", "inbound", r.start, r.end) for i, r in enumerate(requests)]
    call = [Timed("call", j, "proxy", "outbound", x.start, x.end) for j, x in enumerate(calls)]
    calls_of = defaultdict(list)
    for j, tid in enumerate(call_tid):
        if tid is not None:
            calls_of[tid].append(j)

    violations = 0
    for i, tid in enumerate(req_tid):
        own = calls_of.get(tid, []) if tid else []
        if len(own) == 1 and not shift:
            x, r = calls[own[0]], requests[i]
            violations += not (r.start <= x.start and x.end <= r.end)

    starts = sorted(r.start for r in requests)
    cut = starts[len(starts) // 2] if starts else 0
    examples = [
        (req[i], {HOP_NAME: [call[calls_of[tid][0]]]})
        for i, tid in enumerate(req_tid)
        if requests[i].start < cut and tid and len(calls_of.get(tid, [])) == 1
    ]
    if recipe == "declared":
        chosen = Recipe("proxy", (HOP_NAME,), (), 0, "declared")
    else:
        chosen = learn_recipe("proxy", examples, source="first half, truth")
    report = {
        "control": control,
        "recipe_source": None if chosen is None else chosen.source,
        "ranks_ok": mismatched == 0,
        "rank_mismatched_connections": mismatched,
        "requests": len(requests),
        "calls": len(calls),
        "requests_without_truth": req_tid.count(None),
        "calls_without_truth": call_tid.count(None),
        "causality_violations": violations,
        "lossy_intervals": len(lossy),
        "calibration": len(examples) if recipe != "declared" else 0,
        "recipe": None if chosen is None else list(chosen.calls),
    }
    counts, refused = Counter(), Counter()
    window = None
    if chosen is not None and chosen.calls == (HOP_NAME,):
        result = recipe_links(req, {HOP_NAME: call}, chosen, lossy)
        if sizes == "learned":  # spec 8.17: from the links timings alone hold, no truth
            answers_in = {
                ("req", i): r.answer for i, r in enumerate(requests) if i not in damaged_requests
            }
            answers_out = {
                ("call", j): x.answer for j, x in enumerate(calls) if j not in damaged_calls
            }
            window = learn_window(
                [(x.upstream, x.downstream) for x in result.links], answers_in, answers_out
            )
            if window is not None:
                if size_shift:
                    control = f"K6 window shifted by {size_shift} bytes"
                    window = (window[0] + size_shift, window[1] + size_shift)
                result = recipe_links(
                    req,
                    {HOP_NAME: call},
                    chosen,
                    lossy,
                    Sizes(answers_in, {HOP_NAME: answers_out}, {HOP_NAME: window}),
                )
        linked = {x.upstream[1]: x.downstream[1] for x in result.links}
        status = {u.observation[1]: u.status.name for u in result.undecided if u.side == "inbound"}
        for i, tid in enumerate(req_tid):
            if requests[i].start < cut or not tid:
                continue
            if not shift and len(calls_of.get(tid, [])) != 1:
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
    report["control"] = control
    report["size_window"] = None if window is None else list(window)
    report |= located
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


COVERAGE_MIN = 0.99  # spec 8.14, criterion 3, without declared losses
K5_MIN = 0.99  # spec 8.14: share of wrong links the judge must see under K5


def verdict(cells: list[dict]) -> tuple[bool | None, list[str]]:
    """Spec 8.14 on judged cells {"name", "main", "k2b": [...], "k5", "k6"}: True
    (held), False (not held), None (a control failed: no verdict). A cell
    whose ranks do not match load-gen's is invalid: reported, not judged."""
    lines, failed, blind = [], False, False
    for c in cells:
        m = c["main"]
        if not m["ranks_ok"]:
            lines.append(f"{c['name']}: invalid (ranks do not match load-gen)")
            continue
        if m.get("holes_exceed_drops"):  # spec 8.18: more holes than losses: the tool is wrong
            lines.append(f"{c['name']}: invalid (more holes than declared losses)")
            continue
        faults = []
        if m["wrong"]:
            faults.append(f"{m['wrong']} wrong")
        if m["causality_violations"]:
            faults.append(f"{m['causality_violations']} calls outside their request")
        if not m["lossy_intervals"] and m["coverage"] < COVERAGE_MIN:
            faults.append(f"coverage {100 * m['coverage']:.2f}% < {100 * COVERAGE_MIN:.0f}%")
        k2b_wrong = sum(k["wrong"] for k in c["k2b"])
        if k2b_wrong:
            faults.append(f"K2b {k2b_wrong} wrong")
        if c.get("k6", {}).get("wrong"):  # spec 8.17: a wrong window links nothing
            faults.append(f"K6 {c['k6']['wrong']} wrong")
        linked = c["k5"]["wrong"] + c["k5"]["right"]
        seen = c["k5"]["wrong"] / linked if linked else 0.0
        if seen < K5_MIN:
            blind = True
            lines.append(f"{c['name']}: K5 control failed (judge sees {100 * seen:.1f}% wrong)")
        failed |= bool(faults)
        lossy = f", {m['lossy_intervals']} declared losses" if m["lossy_intervals"] else ""
        state = "; ".join(faults) if faults else "ok"
        lines.append(f"{c['name']}: coverage {100 * m['coverage']:.2f}%{lossy}: {state}")
    held = None if blind else not failed
    word = {True: "tenu", False: "non tenu", None: "pas de verdict (contrôle K5)"}[held]
    lines.append(f"verdict: {word}")
    return held, lines


def judge_series(listing: Path) -> tuple[bool | None, list[str]]:
    """Every run of a series list ("<run-dir> <agent> <c> <load>" lines):
    declared recipe with the window learnt from it (spec 8.17), losses
    located (8.18), K2b seeds 1-3, K5, K6; then the verdict."""
    cells = []
    for line in listing.read_text().splitlines():
        if not line.strip():
            continue
        run, agent, c, load = line.split()[:4]
        cell = Path(run) / "n1"
        cells.append(
            {
                "name": f"{agent}-c{c}-load{load} ({Path(run).name})",
                "main": judge(cell, "declared", sizes="learned", locate=True),
                "k2b": [
                    judge(cell, "declared", drop=50, seed=s, sizes="learned", locate=True)
                    for s in (1, 2, 3)
                ],
                "k5": judge(cell, "declared", shift=True, sizes="learned", locate=True),
                "k6": judge(cell, "declared", sizes="learned", size_shift=1000, locate=True),
            }
        )
    return verdict(cells)


def main(argv: list[str]) -> None:
    if argv[1] == "--series":
        held, lines = judge_series(Path(argv[2]))
        print("\n".join(lines))
        sys.exit(0 if held else 1)
    drop = int(argv[argv.index("--drop") + 1]) if "--drop" in argv else None
    seed = int(argv[argv.index("--seed") + 1]) if "--seed" in argv else 0
    recipe = "declared" if "--declared" in argv else "learned"
    sizes = "learned" if "--sizes" in argv else "none"
    size_shift = int(argv[argv.index("--size-shift") + 1]) if "--size-shift" in argv else 0
    report = judge(
        Path(argv[1]), recipe, drop, seed, "--shift" in argv, sizes, size_shift, "--locate" in argv
    )
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    print(f"{Path(argv[1]).parent.name}: " + " ".join(f"{k} {v}" for k, v in report.items()))


if __name__ == "__main__":
    main(sys.argv)
