"""Run reconcileflow's TCP stream path (phase 4, 4c (a)) on stream-probe's events, and score it.

stream-probe (probe.jsonl) writes, for each message on fake-upstream's port,
the four-tuple and the TCP sequence number of its first byte: at the client
when hop-1 sends it, at the server when fake-upstream reads it. Each end's
base comes from its connect (client) or accept (server) event; the offset of
a message is its number minus that base, the same at both ends for the same
byte.

reconcileflow.correlation.connection links sends to reads with
key="tcp-seq". The key given to the engine is the absolute sequence number,
not the offset: the initial number differs from one connection to the next,
so a four-tuple reused later keeps its connections apart. Time only checks
(one window around a zero gap, --window-us): a key on both sides with no pair
inside it is KEY_TIME_CONFLICT.

Truth, measured apart from the probe: fake-upstream -stream-offsets logs each
request's offset in the client's raw stream (fu.jsonl stream_offset), with its
truth id and connection (four-tuple and generation). Each probe event gets the
truth of the request starting at its own offset, before any control. Then:
- correct / wrong: links whose two ends are the same request / are not;
- handshake_links: links between two messages that are not requests (TLS
  opening), reported apart, never scored;
- coverage: correct links / requests fake-upstream logged with an offset;
- server_offset_in_send / _in_recv: those requests whose offset the probe saw
  in a send / a read; a difference with the request count is a probe fault
  or a fragment;
- fragments: reads that do not match one send exactly, same start and
  length (a read joining two messages, or cut in one); a request whose
  start no read shares stays unlinked;
- recv_duplicates: reads reported twice at one number, kept once;
- no_base: events seen before their connection's base, without an offset;
- probe_drops: the probe's last declared count of lost events.

With this truth, 0 wrong links is expected by construction while the probe's
offsets are right: what this tool measures is the probe (offsets agreeing with
the server, fragments, losses) and whether time refuses true pairs. Linking
the agents' spans through these events is a further step.

Controls (one at a time, seed recorded):
- --drop PERMILLE --seed SEED (K2b): that share of the sends and, drawn
  apart, of the reads removed;
- --shift (K5): on each connection, every read takes the number of the next
  read: a probe off by one message on one side.

--attach (4c (b), correlation-phase4-spec.md section 8.6.2): the agents'
spans linked through the probe. Each span is attached to the one probe event
of its connection whose time, on the agents' clock (the probe's clock
records), is within --attach-window-us of it, after the side's offset
(--client-offset-us, --server-offset-us), and only when no other span claims
that event: otherwise it stays unattached, never guessed. A client span and a
server span are linked when their send and read are; the links are judged by
the spans' own truth ids, which owe nothing to the probe. --server-start picks
start or handlerStart for server spans. K2b also drops spans, each side drawn
apart.

Usage: stream_path.py <run-dir> [--window-us US] [--json]
       [--drop PERMILLE --seed SEED | --shift]
       [--attach [--attach-window-us US] [--client-offset-us US]
        [--server-offset-us US] [--server-start start|handlerStart]
        [--compare <connection-params.toml> [--stress]]
        [--attach-params <attach-params.toml>]]
       stream_path.py <run-dir>... --calibrate-attach <attach-params.toml> [--wide-us US]

--calibrate-attach measures the span - probe gaps on whole chains (client
span, send, linked read, server span with the same truth id: calibration may
read the truth, linking never does), pools them per agent, regime (calm or
under load) and side over every cell of the run directories given, and
writes the accepted windows: p0.1 to p99.9, widened by 20 % of the width each
way. --attach-params applies them, per cell.

--compare runs 4a' (connection_path's rule) on the same spans and reports
what each path links that the other does not, and their combination: a link
found by either is kept, by both confirmed, and a span the two link to
different partners has all its links refused.
"""

from __future__ import annotations

import json
import math
import random
import sys
import tomllib
from bisect import bisect_left
from collections import Counter
from dataclasses import dataclass, replace
from itertools import pairwise
from pathlib import Path

from connection_path import _conn_key
from connection_path import records as connection_records
from timing_path import _jsonl, cell_meta, params_for, topology_for

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.connection import Conn, Connected, connection_links
from reconcileflow.correlation.timing import JoinParams, Signal, Timed

WHERE = "hop-1 -> fake-upstream"
WRAP = 2**32
END = {"connect": "client", "send": "client", "accept": "server", "recv": "server"}


@dataclass(frozen=True, slots=True)
class Event:
    line: int  # rank in probe.jsonl: its identity
    kind: str  # "send" | "recv"
    four: tuple  # (client addr, client port, server addr, server port)
    seq: int  # absolute TCP sequence number of the first byte
    offset: int | None  # seq - base of its connection at this end; None: no base seen
    incarnation: int  # connections opened on this four-tuple before this one, at this end
    length: int
    ts_ns: int

    @property
    def port(self) -> int:
        return self.four[1]


def probe_events(path: Path) -> list[Event]:
    """Sends and reads, in time order, with their offset from their connection's base."""
    rows = [(i, r) for i, r in enumerate(_jsonl(path)) if r.get("kind") in END]
    rows.sort(key=lambda x: (x[1]["timestamp_ns"], x[0]))
    bases: dict[tuple, tuple[int, int]] = {}  # (four, end) -> (base, incarnation)
    events = []
    for line, r in rows:
        four = (r["client_addr"], r["client_port"], r["server_addr"], r["server_port"])
        end = END[r["kind"]]
        if r["kind"] in ("connect", "accept"):
            previous = bases.get((four, end))
            bases[four, end] = (r["seq"], 0 if previous is None else previous[1] + 1)
            continue
        base = bases.get((four, end))
        offset = None if base is None else (r["seq"] - base[0]) % WRAP
        incarnation = -1 if base is None else base[1]
        events.append(
            Event(line, r["kind"], four, r["seq"], offset, incarnation, r["len"], r["timestamp_ns"])
        )
    return events


def _truth(cell_dir: Path) -> dict[tuple, str]:
    """(four-tuple, generation, offset) -> truth id, from fake-upstream's log."""
    out = {}
    for r in _jsonl(cell_dir / "fu.jsonl"):
        four = _conn_key(r.get("conn_key", ""))
        if four is None or r.get("stream_offset") is None:
            continue
        out[four, int(r["conn_key"].rsplit("|", 1)[1]), r["stream_offset"]] = r["trace_id"]
    return out


def _probe_drops(path: Path) -> int | None:
    drops = [r["drops"] for r in _jsonl(path) if r.get("kind") == "drops"]
    return drops[-1] if drops else None


def _control(events, drop, seed, shift) -> tuple[list[Event], str | None]:
    if drop is not None and shift:
        raise ValueError("one control at a time: --drop or --shift")
    if drop is not None:
        rng = random.Random(seed)
        gone = set()
        for kind in ("send", "recv"):
            side = [e for e in events if e.kind == kind]
            gone |= {e.line for e in rng.sample(side, round(len(side) * drop / 1000))}
        kept = [e for e in events if e.line not in gone]
        return kept, f"K2b drop={drop} permille each side seed={seed}"
    if shift:
        by_conn: dict[tuple, list[Event]] = {}
        for e in events:
            if e.kind == "recv":
                by_conn.setdefault((e.four, e.incarnation), []).append(e)
        moved = {}
        for reads in by_conn.values():
            reads.sort(key=lambda e: e.offset)
            for a, b in pairwise(reads):
                moved[a.line] = b.seq
            moved[reads[-1].line] = (reads[-1].seq + reads[-1].length) % WRAP
        return [replace(e, seq=moved.get(e.line, e.seq)) for e in events], (
            "K5 reads shifted by one message"
        )
    return events, None


def _distinct(events: list[Event]) -> tuple[list[Event], int]:
    """Events with an offset, a read reported twice at one number kept once."""
    distinct, seen, duplicates = [], set(), 0
    for e in events:
        if e.offset is None:
            continue
        k = (e.kind, e.four, e.incarnation, e.seq)
        if e.kind == "recv" and k in seen:
            duplicates += 1
            continue
        seen.add(k)
        distinct.append(e)
    return distinct, duplicates


def _link_events(events: list[Event], window_us: int):
    """Sends linked to reads by the engine, key tcp-seq (absolute number)."""
    obs = []
    for e in events:
        point, side = ("hop-1", "outbound") if e.kind == "send" else ("fake-upstream", "inbound")
        t = e.ts_ns // 1000
        obs.append(
            Connected(Timed("probe.jsonl", e.line, point, side, t, t), Conn(*e.four), offset=e.seq)
        )
    params = JoinParams(signals=(Signal("start", 0, window_us),), margin_us=0)
    return connection_links(topology_for(1), obs, {WHERE: params}, key="tcp-seq")


def analyze_cell(
    cell_dir: Path,
    window_us: int,
    drop: int | None = None,
    seed: int = 0,
    shift: bool = False,
) -> dict:
    probe = cell_dir / "probe.jsonl"
    events = probe_events(probe)
    no_base = sum(1 for e in events if e.offset is None)
    distinct, duplicates = _distinct(events)
    truth = _truth(cell_dir)

    def place(e):
        return e.four, e.incarnation, e.offset

    of = {e.line: truth.get(place(e)) for e in distinct}  # before any control
    sends = {place(e) for e in distinct if e.kind == "send"}
    recvs = {place(e) for e in distinct if e.kind == "recv"}
    whole_sends = {(place(e), e.length) for e in distinct if e.kind == "send"}
    fragments = sum(
        1 for e in distinct if e.kind == "recv" and (place(e), e.length) not in whole_sends
    )
    controlled, control = _control(distinct, drop, seed, shift)
    result = _link_events(controlled, window_us)

    correct = wrong = handshake = 0
    for x in result.links:
        up, down = of[x.upstream[1]], of[x.downstream[1]]
        if up is None and down is None:
            handshake += 1
        elif up == down:
            correct += 1
        else:
            wrong += 1
    upstream = Counter(u.status for u in result.undecided if u.side == "outbound")
    requests = len(truth)
    return {
        "requests": requests,
        "links": correct + wrong,
        "correct": correct,
        "wrong": wrong,
        "coverage": round(correct / requests, 6) if requests else 0.0,
        "handshake_links": handshake,
        "server_offset_in_send": sum(1 for p in truth if p in sends),
        "server_offset_in_recv": sum(1 for p in truth if p in recvs),
        "fragments": fragments,
        "recv_duplicates": duplicates,
        "no_base": no_base,
        "key_time_conflict": upstream[Status.KEY_TIME_CONFLICT],
        "ambiguous": upstream[Status.AMBIGUOUS],
        "probe_drops": _probe_drops(probe),
        "window_us": window_us,
        "control": control,
    }


# -- 4c (b): agent spans linked through the probe ---------------------------------

SERVER_STARTS = ("start", "handlerStart")
SPAN_SIDES = {("hop-1", "outbound"): "send", ("fake-upstream", "inbound"): "recv"}


def _clocks(path: Path) -> list[tuple[int, int]]:
    """(monotonic ns, wall ns) pairs from the probe's clock records, in order."""
    return sorted(
        (r["timestamp_ns"], r["realtime_ns"])
        for r in _jsonl(path)
        if r.get("kind") == "clock" and r.get("realtime_ns") is not None
    )


def _wall_us(ts_ns: int, clocks: list[tuple[int, int]]) -> int:
    """A monotonic time on the wall clock, with the nearest clock record."""
    i = bisect_left(clocks, (ts_ns,))
    near = min(clocks[max(i - 1, 0) : i + 1], key=lambda c: abs(c[0] - ts_ns))
    return (near[1] + ts_ns - near[0]) // 1000


GAP_POINTS = (("min", 0), ("p1", 1), ("p5", 5), ("median", 50), ("p95", 95), ("p99", 99))


def _gaps(values: list[int]) -> dict:
    """Both tails: on the client side the span starts before the send, so the
    low tail is the one that leaves spans out."""
    if not values:
        return dict.fromkeys([name for name, _ in GAP_POINTS] + ["max"])
    v = sorted(values)
    out = {name: v[min(len(v) - 1, len(v) * q // 100)] for name, q in GAP_POINTS}
    out["max"] = v[-1]
    return out


def _attach(spans, events, offset_us, window_us):
    """Each span to the one probe event of its connection inside the window,
    when that event has no other span inside it either; else no attachment.

    spans: (identity, four, time µs); events: (line, four, time µs).
    Returns {span identity: event line}, the gaps of the attached pairs, and
    the counts of spans with no candidate and with several."""
    by_four: dict[tuple, list] = {}
    for e in events:
        by_four.setdefault(e[1], []).append(e)

    def fits(span_t, event_t):
        return abs(span_t - event_t - offset_us) <= window_us

    candidates = {s[0]: [e for e in by_four.get(s[1], []) if fits(s[2], e[2])] for s in spans}
    claims = Counter(e[0] for found in candidates.values() for e in found)
    attached, gaps, none, several = {}, [], 0, 0
    for s in spans:
        found = candidates[s[0]]
        if not found:
            none += 1
        elif len(found) > 1 or claims[found[0][0]] > 1:
            several += 1
        else:
            attached[s[0]] = found[0][0]
            gaps.append(s[2] - found[0][2])
    return attached, gaps, none, several


def regime(meta: dict) -> str:
    return "calm" if meta["load"] == 0 else "load"


def _windows(attach_params: Path, meta: dict) -> dict[str, tuple[int, int]]:
    """(offset, window) per side for this cell's agent and regime, from the calibration."""
    data = tomllib.loads(attach_params.read_text())
    table = data.get(meta["agent"], {}).get(regime(meta))
    if not table or not {"client", "server"} <= set(table):
        raise ValueError(f"{attach_params}: no windows for {meta['agent']}.{regime(meta)}")
    return {
        side: (table[side]["offset_us"], table[side]["window_us"]) for side in ("client", "server")
    }


def attach_cell(
    cell_dir: Path,
    window_us: int,
    attach_window_us: int | None = None,
    client_offset_us: int = 0,
    server_offset_us: int = 0,
    server_start: str = "start",
    drop: int | None = None,
    seed: int = 0,
    shift: bool = False,
    compare: Path | None = None,
    attach_params: Path | None = None,
    stress: bool = False,
) -> dict:
    """Client span -> probe send -> (same TCP number) -> probe read -> server span,
    judged by the spans' own truth ids, which owe nothing to the probe.

    The attach windows come from the calibration (attach_params, per agent,
    regime and side) or, for readings, from attach_window_us and the offsets."""
    if server_start not in SERVER_STARTS:
        raise ValueError(f"server_start must be one of {SERVER_STARTS}, got {server_start!r}")
    probe = cell_dir / "probe.jsonl"
    clocks = _clocks(probe)
    if not clocks:
        return {"error": "no clock record in probe.jsonl: cannot convert to the agents' clock"}
    meta = cell_meta(cell_dir)
    if attach_params is not None:
        windows = _windows(attach_params, meta)
    elif attach_window_us is not None:
        windows = {
            "client": (client_offset_us, attach_window_us),
            "server": (server_offset_us, attach_window_us),
        }
    else:
        raise ValueError("attach windows: give attach_params or attach_window_us")
    rows, _ = connection_records(cell_dir, meta)
    rows = [r for r in rows if (r["point"], r["side"]) in SPAN_SIDES and r["conn"] is not None]
    events, _ = _distinct(probe_events(probe))
    events, control = _control(events, drop, seed, shift)
    if drop is not None:  # K2b: the spans lose their share too, each side drawn apart
        rng = random.Random(seed + 1)
        gone = set()
        for kind in ("send", "recv"):
            side = [r for r in rows if SPAN_SIDES[r["point"], r["side"]] == kind]
            gone |= {id(r) for r in rng.sample(side, round(len(side) * drop / 1000))}
        kept = [r for r in rows if id(r) not in gone]
    else:
        kept = rows
    links = {x.upstream[1]: x.downstream[1] for x in _link_events(events, window_us).links}

    out: dict = {"clock_records": len(clocks), "control": control, "windows": windows}
    attached = {}
    for kind, side in (("send", "client"), ("recv", "server")):
        offset, side_window = windows[side]
        spans = [
            (
                (r["source"], r["seq"]),
                tuple(r["conn"]),
                r["handler" if kind == "recv" and server_start == "handlerStart" else "start"],
            )
            for r in kept
            if SPAN_SIDES[r["point"], r["side"]] == kind
        ]
        evs = [(e.line, e.four, _wall_us(e.ts_ns, clocks)) for e in events if e.kind == kind]
        attached[kind], gaps, none, several = _attach(spans, evs, offset, side_window)
        out[f"{side}_spans"] = len(spans)
        out[f"attached_{side}"] = len(attached[kind])
        out[f"no_candidate_{side}"] = none
        out[f"ambiguous_{side}"] = several
        out[f"gap_{side}_us"] = _gaps(gaps)

    truth = {(r["source"], r["seq"]): r["truth"] for r in rows}
    server_of_read = {line: span for span, line in attached["recv"].items()}
    probe_links = set()
    for client, send in attached["send"].items():
        server = server_of_read.get(links.get(send))
        if server is not None:
            probe_links.add((client, server))
    correct, wrong = _judge(probe_links, truth)
    ups = {r["truth"] for r in rows if SPAN_SIDES[r["point"], r["side"]] == "send"}
    both = len(ups & {r["truth"] for r in rows if SPAN_SIDES[r["point"], r["side"]] == "recv"})
    out.update(
        {
            "links": correct + wrong,
            "correct": correct,
            "wrong": wrong,
            "both_seen": both,
            "coverage": round(correct / both, 6) if both else 0.0,
            "server_start": server_start,
        }
    )
    if compare is not None:
        out["compare"] = _compare(probe_links, kept, compare, meta, truth, both, stress)
    return out


def _judge(pairs, truth) -> tuple[int, int]:
    correct = sum(1 for c, s in pairs if truth[c] == truth[s])
    return correct, len(pairs) - correct


def _links_4a(rows, params_file: Path, meta: dict, stress: bool = False) -> set:
    """4a': the same spans linked inside their connection by time alone (with
    stress: the parameters of the highest calibrated concurrency, whatever the load)."""
    params, receiver_start = params_for(params_file, meta, stress=stress)[WHERE]
    obs = []
    for r in rows:
        handler = receiver_start == "handlerStart" and r["point"] == "fake-upstream"
        start = r["handler"] if handler else r["start"]
        timed = Timed(r["source"], r["seq"], r["point"], r["side"], start, r["end"])
        obs.append(Connected(timed, Conn(*r["conn"])))
    result = connection_links(topology_for(meta["hops"]), obs, {WHERE: params}, key="connection")
    return {(x.upstream, x.downstream) for x in result.links}


def _combine(a: set, b: set) -> set:
    """Links of either path, less every link of a span given two partners."""
    union = a | b
    partners = Counter()
    for c, s in union:
        partners["client", c] += 1
        partners["server", s] += 1
    return {(c, s) for c, s in union if partners["client", c] == partners["server", s] == 1}


def _compare(
    probe_links: set, rows, params_file: Path, meta: dict, truth, both: int, stress: bool = False
) -> dict:
    """Both paths run apart. A link found by one is kept; found by both,
    confirmed; a span the two link to different partners has all its links
    refused: never a choice between the paths."""
    links_4a = _links_4a(rows, params_file, meta, stress)
    union = links_4a | probe_links
    combined = _combine(links_4a, probe_links)
    a_correct, a_wrong = _judge(links_4a, truth)
    c_correct, c_wrong = _judge(combined, truth)
    return {
        "stress": stress,
        "4a": {"links": len(links_4a), "correct": a_correct, "wrong": a_wrong},
        "both": len(links_4a & probe_links),
        "only_4a": len(links_4a - probe_links),
        "only_4c": len(probe_links - links_4a),
        "conflicts": len(union - combined),
        "combined": {
            "links": len(combined),
            "correct": c_correct,
            "wrong": c_wrong,
            "coverage": round(c_correct / both, 6) if both else 0.0,
        },
    }


# -- calibration of the attach windows (correlation-phase4-spec.md 8.6.2) ---------

WIDEN = 0.2  # each bound, as a share of its distance to the median (decided 2026-11-09)
CALIBRATION_RULE = "p0.1 to p99.9, each bound widened by 20 % of its distance to the median"


def attach_interval(gaps: list[int]) -> dict:
    """The accepted interval: p0.1 to p99.9 of the gaps, each bound widened by a
    fifth of its own distance to the median, rounded outwards; as an offset
    and a half-width. A tight side stays tight: on the client side the span
    always starts before the send, and a send before the span is refused."""
    v = sorted(gaps)
    lo, hi = v[len(v) // 1000], v[min(len(v) - 1, len(v) * 999 // 1000)]
    med = v[len(v) // 2]
    low = math.floor(lo - (med - lo) * WIDEN)
    high = math.ceil(hi + (hi - med) * WIDEN)
    return {
        "p0_1": lo,
        "median": med,
        "p99_9": hi,
        "low_us": low,
        "high_us": high,
        "offset_us": (low + high) // 2,
        "window_us": math.ceil((high - low) / 2),
        "samples": len(v),
    }


def calibration_gaps(cell_dir: Path, window_us: int, wide_us: int) -> dict:
    """Span - probe gaps on both sides, measured with the truth (calibration may
    read it; linking never does): for each truth id, the send nearest in time
    to its client span and the read nearest to its server span, on their
    connection and within wide_us, kept only when the probe links that send to
    that read. A gap so large that a neighbour is nearer is left out, not
    misread: the cases the one-candidate rule would refuse anyway. Returns the
    gaps and how many truth ids were left out."""
    probe = cell_dir / "probe.jsonl"
    clocks = _clocks(probe)
    meta = cell_meta(cell_dir)
    rows, _ = connection_records(cell_dir, meta)
    rows = [r for r in rows if (r["point"], r["side"]) in SPAN_SIDES and r["conn"] is not None]
    events, _ = _distinct(probe_events(probe))
    links = {x.upstream[1]: x.downstream[1] for x in _link_events(events, window_us).links}
    times: dict[tuple, list[tuple[int, int]]] = {}
    for e in events:
        times.setdefault((e.kind, e.four), []).append((_wall_us(e.ts_ns, clocks), e.line))
    for v in times.values():
        v.sort()

    def nearest(kind, four, t):
        v = times.get((kind, four), [])
        i = bisect_left(v, (t,))
        near = [v[j] for j in (i - 1, i) if 0 <= j < len(v) and abs(v[j][0] - t) <= wide_us]
        return min(near, key=lambda x: abs(x[0] - t)) if near else None

    spans: dict[str, dict[str, dict]] = {}
    for r in rows:
        spans.setdefault(r["truth"], {})[SPAN_SIDES[r["point"], r["side"]]] = r
    client_gaps, server_gaps, left_out = [], [], 0
    for pair in spans.values():
        if set(pair) != {"send", "recv"}:
            continue
        c, s = pair["send"], pair["recv"]
        send = nearest("send", tuple(c["conn"]), c["start"])
        read = nearest("recv", tuple(s["conn"]), s["start"])
        if send is None or read is None or links.get(send[1]) != read[1]:
            left_out += 1
            continue
        client_gaps.append(c["start"] - send[0])
        server_gaps.append(s["start"] - read[0])
    return {"client": client_gaps, "server": server_gaps, "left_out": left_out}


def calibrate_attach(
    cells: list[Path], out: Path, wide_us: int, window_us: int = 1_000_000
) -> dict:
    """Windows per agent, regime and side, pooled over the cells, written to out."""
    pooled: dict[tuple[str, str], tuple[list[int], list[int]]] = {}
    for cell in cells:
        meta = cell_meta(cell)
        gaps = calibration_gaps(cell, window_us, wide_us)
        acc = pooled.setdefault((meta["agent"], regime(meta)), ([], [], [0]))
        acc[0].extend(gaps["client"])
        acc[1].extend(gaps["server"])
        acc[2][0] += gaps["left_out"]
    result = {}
    for key, (c, s, left) in sorted(pooled.items()):
        if c and s:
            result[key] = {"client": attach_interval(c), "server": attach_interval(s)}
            for side in ("client", "server"):
                result[key][side]["left_out"] = left[0]
    lines = [
        f'rule = "{CALIBRATION_RULE}"',
        f"wide_us = {wide_us}",
        f"cells = [{', '.join(json.dumps(str(c)) for c in cells)}]",
        "",
    ]
    for (agent, reg), sides in result.items():
        for side, values in sides.items():
            lines.append(f"[{agent}.{reg}.{side}]")
            lines += [f"{k} = {v}" for k, v in values.items()]
            lines.append("")
    out.write_text("\n".join(lines))
    return result


def _arg(argv: list[str], name: str) -> int | None:
    return int(argv[argv.index(name) + 1]) if name in argv else None


def main(argv: list[str]) -> None:
    run_dir = Path(argv[1])
    options = {
        "window_us": _arg(argv, "--window-us") or 1_000_000,
        "drop": _arg(argv, "--drop"),
        "seed": _arg(argv, "--seed") or 0,
        "shift": "--shift" in argv,
    }
    cells = sorted(p.parent for p in run_dir.glob("*/probe.jsonl"))
    if "--calibrate-attach" in argv:
        # every run directory given before the options is pooled
        dirs = [Path(a) for a in argv[1:] if not a.startswith("--") and Path(a).is_dir()]
        cells = sorted(p.parent for d in dirs for p in d.glob("*/probe.jsonl"))
        out = Path(argv[argv.index("--calibrate-attach") + 1])
        result = calibrate_attach(cells, out, wide_us=_arg(argv, "--wide-us") or 50_000)
        for (agent, reg), sides in result.items():
            for side, v in sides.items():
                print(
                    f"{agent} {reg} {side}: p0.1 {v['p0_1']} p99.9 {v['p99_9']} -> accepted"
                    f" [{v['low_us']}, {v['high_us']}] µs ({v['samples']} chains)"
                )
        print(f"written to {out}")
        return
    if "--attach" in argv:
        _main_attach(argv, cells, options["window_us"])
        return
    report = {cell.name: analyze_cell(cell, **options) for cell in cells}
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    wrong = 0
    for name, c in report.items():
        head = (
            f"{name}: requests {c['requests']} links {c['links']} correct {c['correct']}"
            f" wrong {c['wrong']} coverage {100 * c['coverage']:.2f}%"
        )
        print(head + (f" control {c['control']}" if c["control"] else ""))
        print(
            f"   server offset in probe send {c['server_offset_in_send']}"
            f" recv {c['server_offset_in_recv']} fragments {c['fragments']}"
            f" handshake links {c['handshake_links']} no base {c['no_base']}"
            f" key/time conflicts {c['key_time_conflict']} ambiguous {c['ambiguous']}"
            f" duplicate reads {c['recv_duplicates']} probe drops {c['probe_drops']}"
        )
        wrong += c["wrong"]
    print(f"wrong links in the run: {wrong}")


def _main_attach(argv: list[str], cells: list[Path], window_us: int) -> None:
    server_start = argv[argv.index("--server-start") + 1] if "--server-start" in argv else "start"
    report = {
        cell.name: attach_cell(
            cell,
            window_us=window_us,
            attach_window_us=_arg(argv, "--attach-window-us"),
            client_offset_us=_arg(argv, "--client-offset-us") or 0,
            server_offset_us=_arg(argv, "--server-offset-us") or 0,
            server_start=server_start,
            drop=_arg(argv, "--drop"),
            seed=_arg(argv, "--seed") or 0,
            shift="--shift" in argv,
            compare=Path(argv[argv.index("--compare") + 1]) if "--compare" in argv else None,
            attach_params=(
                Path(argv[argv.index("--attach-params") + 1]) if "--attach-params" in argv else None
            ),
            stress="--stress" in argv,
        )
        for cell in cells
    }
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    wrong = 0
    for name, c in report.items():
        if "error" in c:
            print(f"{name}: {c['error']}")
            continue
        print(
            f"{name} spans: links {c['links']} correct {c['correct']} wrong {c['wrong']}"
            f" both_seen {c['both_seen']} coverage {100 * c['coverage']:.2f}%"
            + (f" control {c['control']}" if c["control"] else "")
        )
        print(
            f"   attached client {c['attached_client']} server {c['attached_server']}"
            f" (no candidate {c['no_candidate_client']}/{c['no_candidate_server']},"
            f" ambiguous {c['ambiguous_client']}/{c['ambiguous_server']});"
            f" gap µs client {c['gap_client_us']} server {c['gap_server_us']}"
        )
        if "compare" in c:
            k = c["compare"]
            print(
                f"   with 4a': both {k['both']} only 4a' {k['only_4a']} only probe {k['only_4c']}"
                f" conflicts {k['conflicts']}; 4a' alone: correct {k['4a']['correct']}"
                f" wrong {k['4a']['wrong']}"
            )
            m = k["combined"]
            print(
                f"   combined: links {m['links']} correct {m['correct']} wrong {m['wrong']}"
                f" coverage {100 * m['coverage']:.2f}%"
            )
        wrong += c["wrong"]
    print(f"wrong span links in the run: {wrong}")


if __name__ == "__main__":
    main(sys.argv)
