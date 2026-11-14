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
connection between two events a, b seen (seq_holes), or at an edge the
other side sees. Only holes in the proxy's own events count (lives uses no
other), and only where the probe declared a loss. A hole's zone is
[t(a), t(b)], narrowed by the other side, which saw the same bytes on the
same clock: a lost read comes after the send of its first byte, a lost
send before the read of its last. The exchanges whose events reach into
[t(a), t(b)] are damaged: their answer sizes are not used. A declared loss
with no hole stays whole.
Why no wrong link is added (spec 8.18.9). Among complete assignments that
fit the options seen, recipe_links holds only the links they all share;
none is wrong while the true assignment fits. It stops fitting only when
a true option is missing (request R's true call X is not among its
options, the life seen of R or of X having changed) or an element is
missing or extra; so the groups of R and of X must be refused, and the
zone must meet the life seen of each. An extra option changes nothing; a
wrong size is dropped. Case by case, t_l the instant lost:
- the proxy's first read of R lost (R reads more): R starts at t(b), in
  the zone; X starts in [t_l, t(b)] when R lost it, in the zone too;
- its only read of R lost: R is gone, its answer goes to the request
  before (size dropped); X lies in [t_l, t(b)];
- a piece of R's answer: size dropped; its only piece: R merges with the
  next request, whose life seen holds t_l;
- the proxy's last send of X lost (X sends more): X starts earlier;
  t_l is in X's true life, so in R's and in X's seen;
- its only send of X: X is gone, its answer goes to the call before
  (size dropped); t_l is X's true start, in R;
- the first piece of X's answer: X ends at t(b): t_l in R, t(b) in X
  seen; its only piece: X merges with the next call; t_l in R.
Every zone holds t_l, and t(b) for reads. Outside refused groups no
observation is wrong and no wrong size is used. Open risks: an event and
its counterpart both lost at the same edge; one anomaly making up for
another in a group's count.
The window of sizes is the relay's: learnt where nothing was lost and
kept (judge(window=...)); the size trip of recipe_links leaves out what a
loss touches (8.18.10).

Usage: probe_recipe.py <cell-dir> [--json] [--declared] [--drop N --seed S] [--shift]
                       [--sizes [--size-shift B]] [--locate]
       probe_recipe.py --series <runs-list>   (the spec 8.14 verdict)
"""

from __future__ import annotations

import json
import random
import sys
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path

from reconcileflow.correlation.recipe import (
    Lattice,
    Recipe,
    Sizes,
    learn_lattice,
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
    incarnation: int = 0  # which connection on its four-tuple (spec 8.20), in opening order


class _Openings:
    """The connections of each four-tuple, by their openings (spec 8.20): a
    new connect (or accept, where no connect was seen) on a four-tuple
    already used starts a new connection; events go to the one open at their
    instant. Wall-clock times (load-gen's, fake-upstream's) are put on the
    probe's clock by its clock records."""

    def __init__(self, rows):
        opened: dict = defaultdict(lambda: {"connect": [], "accept": []})
        offsets = []
        for r in rows:
            kind = r.get("kind")
            if kind in ("connect", "accept") and "client_port" in r:
                opened[r["client_port"], r["server_port"]][kind].append(r["timestamp_ns"])
            elif kind == "clock" and "realtime_ns" in r:
                offsets.append(r["realtime_ns"] - r["timestamp_ns"])
        self.opens = {c: sorted(k["connect"] or k["accept"]) for c, k in opened.items()}
        self.offset = sorted(offsets)[len(offsets) // 2] if offsets else None

    def of(self, conn, ts: int) -> int:
        return bisect_right(self.opens.get(conn, []), ts)

    def of_record(self, conn, wall_ns: int | None, seen: set) -> int | None:
        """The connection a load-gen or fake-upstream record belongs to: the
        only one seen on its four-tuple, else the one open at its time; None
        when that cannot be told (no guess)."""
        mine = {k for c, k in seen if c == conn}
        if len(mine) == 1:
            return next(iter(mine))
        if self.offset is None or not wall_ns:
            return None
        return self.of(conn, wall_ns - self.offset)


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
    openings = _Openings([r for _, _, r in rows])
    base: dict = {}
    seen: dict = defaultdict(list)  # (conn, incarnation) -> [(ts, ask?, offset, bytes)]
    for ts, _, r in rows:
        kind = r.get("kind")
        four = (r.get("client_port"), r.get("server_port"))
        if four[1] not in (hop_port, fu_port):
            continue
        conn = (four, openings.of(four, ts))
        if kind == "connect":
            base.setdefault(conn, r["seq"])
            continue
        s2c = r.get("dir") == "s2c"
        if four[1] == hop_port:  # the proxy is the server: it reads asks, sends answers
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
    for (conn, incarnation), events in seen.items():
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
            Exchange(conn, k + 1, s, e, o, a, (f, z), incarnation)
            for k, (s, e, o, a, f, z) in enumerate(exchanges)
        ]
    return requests, calls


@dataclass(frozen=True, slots=True, order=True)
class Hole:
    """Events lost on a connection, between two instants (spec 8.18)."""

    conn: tuple[int, int]
    start: int  # the events seen around it, on its stream
    end: int
    edge: bool = False  # at a stream's first or last byte: may be the protocol's close
    way: str = "c2s"  # its stream: direction and kind of the lost events
    kind: str = "send"
    lo: int = 0  # [lo, hi]: where the lost events lie, narrowed by the other side
    hi: int = 0
    incarnation: int = 0  # which connection on its four-tuple (spec 8.20)


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
    openings = _Openings(rows)
    streams: dict = defaultdict(list)  # here a connection is (four-tuple, incarnation)
    first: dict = {}
    last: dict = {}
    closed: dict = {}  # (conn, pid) -> first close
    for i, r in enumerate(rows):
        if "client_port" not in r:
            continue
        four = (r["client_port"], r["server_port"])
        ts = r["timestamp_ns"]
        conn = (four, openings.of(four, ts))
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

    covering: dict = {}  # (conn, way, kind) -> (starts, [(from, to, ts)], base), byte offsets

    def other_side(conn, way, kind, seq):
        """When the other side's event holding byte seq happened, if seen."""
        key = (conn, way, "recv" if kind == "send" else "send")
        if key not in covering:
            base = (streams.get((conn, way, "send")) or streams[conn, way, "recv"])[0][2]
            ev = sorted(
                (_signed(x[2] - base), _signed(x[2] - base) + x[3], x[0])
                for x in streams.get(key, [])
            )
            covering[key] = ([e[0] for e in ev], ev, base)
        starts, ev, base = covering[key]
        at = _signed(seq - base)
        k = bisect_right(starts, at) - 1
        return ev[k][2] if k >= 0 and ev[k][0] <= at < ev[k][1] else None

    holes = []
    for (conn, way, kind), ev in streams.items():
        for a, b in pairwise(ev):
            if not (a[2] + a[3] - b[2]) % 2**32:
                continue
            lo, hi = a[0], b[0]
            if kind == "recv":  # a lost read comes after the send of its first byte
                t = other_side(conn, way, kind, a[2] + a[3])
                lo = lo if t is None else min(max(lo, t), hi)
            else:  # a lost send comes before the read of its last byte
                t = other_side(conn, way, kind, b[2] - 1)
                hi = hi if t is None else max(min(hi, t), lo)
            holes.append(Hole(conn[0], a[0], b[0], False, way, kind, lo, hi, conn[1]))

    def edge(conn, way, kind, lo, hi):
        holes.append(Hole(conn[0], lo, hi, True, way, kind, lo, hi, conn[1]))

    for conn, way in {(c, w) for c, w, _ in streams}:
        sent, read = streams.get((conn, way, "send"), []), streams.get((conn, way, "recv"), [])
        if not sent or not read:  # one side saw nothing of this direction
            edge(conn, way, "recv" if sent else "send", first[conn], last[conn])
            continue
        lead = _signed(sent[0][2] - read[0][2])
        if lead:  # the side that starts later lost its first event
            later, kind = (sent, "send") if lead > 0 else (read, "recv")
            edge(conn, way, kind, first[conn], later[0][0])
        end = read[-1][2] + read[-1][3]
        tail = _signed(sent[-1][2] + sent[-1][3] - end)
        if tail < 0:  # read beyond what was sent: the sender lost its last send
            edge(conn, way, "send", sent[-1][0], last[conn])
        elif tail > 0:  # sent beyond what was read
            close = closed.get((conn, read[-1][4]))
            unread = [e for e in sent if _signed(e[2] + e[3] - end) > 0]
            if close is None or any(e[0] <= close for e in unread):
                edge(conn, way, "recv", read[-1][0], last[conn] if close is None else close)
    return sorted(holes)


def _at_the_proxy(h: Hole, hop_port: int, fu_port: int) -> bool:
    """Whether the hole is in the proxy's own events, the only ones lives uses:
    its reads of requests and sends of answers, its sends of calls and reads
    of their answers."""
    if h.conn[1] == hop_port:
        return (h.way, h.kind) in (("c2s", "recv"), ("s2c", "send"))
    if h.conn[1] == fu_port:
        return (h.way, h.kind) in (("c2s", "send"), ("s2c", "recv"))
    return False


def locate_losses(rows, hop_port, fu_port, requests, calls, declared, holes=None):
    """(zones, damaged requests, damaged calls, unlocated): spec 8.18.9.
    The probe counts each lost event when it happens, so a loss lies in an
    interval where its count grew (lo, hi, count): a hole that meets none is
    the protocol, not a loss, and is left out; so is a hole in load-gen's or
    fake-upstream's events, which no observation uses. A zone is a hole's
    [lo, hi], narrowed by the other side's event; the exchanges of its
    connection whose events reach into [start, end] are damaged: their sizes
    are not to be used. A declared loss with no hole in its interval stays
    whole: unlocated."""
    lossy = _Union((lo, hi) for lo, hi, n in declared if n > 0)
    holes = [
        h for h in (seq_holes(rows) if holes is None else holes) if lossy.meets(h.start, h.end)
    ]
    spans = _Union((h.start, h.end) for h in holes)  # located, the proxy's or not
    holes = [h for h in holes if _at_the_proxy(h, hop_port, fu_port)]
    by_conn: dict = defaultdict(list)
    for inbound, exchanges in ((True, requests), (False, calls)):
        for i, x in enumerate(exchanges):
            by_conn[x.conn, x.incarnation].append((x, i, inbound))
    zones, damaged_requests, damaged_calls = [], set(), set()
    for h in holes:
        for x, i, inbound in by_conn.get((h.conn, h.incarnation), ()):
            if x.span[0] <= h.end and h.start <= x.span[1]:
                (damaged_requests if inbound else damaged_calls).add(i)
        zones.append((h.lo, h.hi))
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


def _record_connection(r: dict, openings: _Openings, seen: set):
    """((client port, server port), incarnation) of a load-gen or
    fake-upstream record (spec 8.20), or None when it cannot be told."""
    four, _, generation = r.get("conn_key", "").partition("|")
    client, _, server = four.partition("-")
    try:
        conn = (int(client.rsplit(":", 1)[1]), int(server.rsplit(":", 1)[1]))
    except (IndexError, ValueError):
        return None
    if generation not in ("", "0"):  # the recorder's own reuse: only its time tells
        seen = set()
    incarnation = openings.of_record(conn, r.get("timestamp_ns"), seen)
    return None if incarnation is None else (conn, incarnation)


def _max_ranks(cell: Path, hop: int, openings: _Openings, seen: set) -> dict:
    """Per inbound connection (client port, incarnation), load-gen's highest
    request rank on the wire (warm-up, measured and tail requests alike)."""
    out: dict = {}
    for name in sorted(p.name for p in cell.glob("*.jsonl")):
        if not (name.startswith(("lg", "warmup", "tail"))):
            continue
        for r in _jsonl(cell / name):
            at, rank = (
                _record_connection(r, openings, seen),
                r.get("wire_rank") or r.get("stream_id"),
            )
            if at is not None and at[0][1] == hop and rank is not None:
                key = (at[0][0], at[1])
                out[key] = max(out.get(key, 0), rank)
    return out


def _call_tid(x: Exchange, fu_truth: dict) -> str | None:
    """A call's truth: that of the last request among its asks. A call starts
    at its last send; one merged with the next after a lost answer (spec
    8.18.9, D3) is the next call. Without loss a call holds one request."""
    keys = ((x.conn[0], x.incarnation, o) for o in reversed(x.offsets))
    return next((fu_truth[k] for k in keys if k in fu_truth), None)


def judge(
    cell: Path,
    recipe: str = "learned",
    drop: int | None = None,
    seed: int = 0,
    shift: bool = False,
    sizes: str = "none",
    size_shift: int = 0,
    locate: bool = False,
    window: tuple[int, int] | None = None,
    signature: tuple[int, int, int] | None = None,
    lattice_shift: bool = False,
) -> dict:
    meta = json.loads((cell / "cell.json").read_text())
    hop, fu = meta["hop_ports"][0], meta["fake_upstream_port"]
    rows = list(_jsonl(cell / "probe.jsonl"))

    # Truth, from every event the probe wrote: an exchange is known by its
    # connection and its end, which survive the K2b control.
    full_requests, full_calls = lives(rows, hop, fu)
    # Spec 8.20: a load-gen request is known by its rank on the wire
    # (wire_rank; stream_id, numbered at completion, in runs recorded
    # before), on the connection open at its time.
    openings = _Openings(rows)
    seen = {(x.conn, x.incarnation) for x in [*full_requests, *full_calls]}

    def where(r):
        return _record_connection(r, openings, seen)

    lg = {}
    for r in _jsonl(cell / "lg.jsonl"):
        at, rank = where(r), r.get("wire_rank") or r.get("stream_id")
        if at is not None and rank:
            lg[at[0][0], at[1], rank] = r["trace_id"]
    fu_truth = {}
    for r in _jsonl(cell / "fu.jsonl"):
        at = where(r)
        if at is not None and r.get("stream_offset") is not None:
            fu_truth[at[0][0], at[1], r["stream_offset"]] = r["trace_id"]
    req_truth = {(r.conn, r.end): lg.get((r.conn[0], r.incarnation, r.rank)) for r in full_requests}
    call_truth = {(x.conn, x.end): _call_tid(x, fu_truth) for x in full_calls}
    control = None
    if shift:  # K5: each call takes the truth of the next call on its connection
        control = "K5 call truth shifted by one rank"
        by_conn = defaultdict(list)
        for x in full_calls:
            by_conn[x.conn, x.incarnation].append(x)
        for xs in by_conn.values():
            xs.sort(key=lambda x: x.rank)
            for x, y in zip(xs, [*xs[1:], None], strict=True):
                call_truth[x.conn, x.end] = None if y is None else call_truth.get((y.conn, y.end))
    most = _max_ranks(cell, hop, openings, seen)
    per_conn = Counter((r.conn[0], r.incarnation) for r in full_requests)
    mismatched = sum(1 for k, n in per_conn.items() if n - most.get(k, 0) not in (0, 1))

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
    given, window, lattice = window, None, None
    if chosen is not None and chosen.calls == (HOP_NAME,):
        result = recipe_links(req, {HOP_NAME: call}, chosen, lossy)
        if sizes in ("learned", "lattice"):  # spec 8.17, 8.22: from the links timings alone hold
            answers_in = {
                ("req", i): r.answer for i, r in enumerate(requests) if i not in damaged_requests
            }
            answers_out = {
                ("call", j): x.answer for j, x in enumerate(calls) if j not in damaged_calls
            }
            if given is not None:  # the relay's, learnt where nothing was lost (8.18.10)
                window = given
                if sizes == "lattice" and signature is not None:
                    lattice = Lattice(*signature)
            else:
                # Learnt on the timings alone without the losses (8.18.8): a wrong
                # link among these can only widen the window or remove it.
                free = result if not lossy else recipe_links(req, {HOP_NAME: call}, chosen)
                pairs = [(x.upstream, x.downstream) for x in free.links]
                window = learn_window(pairs, answers_in, answers_out)
                if sizes == "lattice":  # spec 8.22: the window kept to the step
                    lattice = learn_lattice(pairs, answers_in, answers_out)
            if window is not None:
                if size_shift:
                    control = f"K6 window shifted by {size_shift} bytes"
                    window = (window[0] + size_shift, window[1] + size_shift)
                    if lattice is not None:
                        lattice = Lattice(
                            lattice.low + size_shift, lattice.high + size_shift, lattice.step
                        )
                if lattice is not None and lattice_shift:
                    control = "K7 signature shifted by half a step"
                    half = lattice.step // 2
                    lattice = Lattice(lattice.low + half, lattice.high + half, lattice.step)
                rule = window if lattice is None else lattice
                result = recipe_links(
                    req,
                    {HOP_NAME: call},
                    chosen,
                    lossy,
                    Sizes(answers_in, {HOP_NAME: answers_out}, {HOP_NAME: rule}),
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
    if sizes == "lattice":
        report["size_signature"] = (
            None if lattice is None else [lattice.low, lattice.high, lattice.step]
        )
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
    """Spec 8.14 (8.19) on judged cells {"name", "main", "k2b", "k2c", "k5", "k6"}: True
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
        k2c_wrong = sum(k["wrong"] for k in c.get("k2c", []))  # spec 8.19: rare losses
        if k2c_wrong:
            faults.append(f"K2c {k2c_wrong} wrong")
        if c.get("k6", {}).get("wrong"):  # spec 8.17: a wrong window links nothing
            faults.append(f"K6 {c['k6']['wrong']} wrong")
        if (c.get("k7") or {}).get("wrong"):  # spec 8.22: a wrong signature links nothing
            faults.append(f"K7 {c['k7']['wrong']} wrong")
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


def judge_series(listing: Path, sizes: str = "learned") -> tuple[bool | None, list[str]]:
    """Every run of a series list ("<run-dir> <agent> <c> <load>" lines):
    declared recipe with the window learnt from it (spec 8.17), losses
    located (8.18), K2b (50 permille) and K2c (1 permille) seeds 1-3, K5,
    K6 with the main judgment's window (8.18.10); then the verdict of the
    protocol as amended (spec 8.19)."""
    cells = []
    for line in listing.read_text().splitlines():
        if not line.strip():
            continue
        run, agent, c, load = line.split()[:4]
        cell = Path(run) / "n1"
        main = judge(cell, "declared", sizes=sizes, locate=True)
        # The controls keep the window learnt where nothing was lost (8.18.10),
        # and the signature (8.22).
        window = None if main["size_window"] is None else tuple(main["size_window"])
        kept = {"recipe": "declared", "sizes": sizes, "locate": True, "window": window}
        if sizes == "lattice":
            sig = main.get("size_signature")
            kept["signature"] = None if sig is None else tuple(sig)
        entry = {
            "name": f"{agent}-c{c}-load{load} ({Path(run).name})",
            "main": main,
            "k2b": [judge(cell, drop=50, seed=s, **kept) for s in (1, 2, 3)],
            "k2c": [judge(cell, drop=1, seed=s, **kept) for s in (1, 2, 3)],
            "k5": judge(cell, shift=True, **kept),
            "k6": judge(cell, size_shift=1000, **kept),
        }
        if sizes == "lattice":  # without a signature there is nothing to shift
            entry["k7"] = judge(cell, lattice_shift=True, **kept) if kept["signature"] else None
        cells.append(entry)
    return verdict(cells)


def main(argv: list[str]) -> None:
    if argv[1] == "--series":
        held, lines = judge_series(Path(argv[2]), "lattice" if "--lattice" in argv else "learned")
        print("\n".join(lines))
        sys.exit(0 if held else 1)
    drop = int(argv[argv.index("--drop") + 1]) if "--drop" in argv else None
    seed = int(argv[argv.index("--seed") + 1]) if "--seed" in argv else 0
    recipe = "declared" if "--declared" in argv else "learned"
    sizes = "lattice" if "--lattice" in argv else "learned" if "--sizes" in argv else "none"
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
