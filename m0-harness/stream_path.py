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

Usage: stream_path.py <run-dir> [--window-us US] [--json]
       [--drop PERMILLE --seed SEED | --shift]
"""

from __future__ import annotations

import json
import random
import sys
from collections import Counter
from dataclasses import dataclass, replace
from itertools import pairwise
from pathlib import Path

from connection_path import _conn_key
from timing_path import _jsonl, topology_for

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
    events = [e for e in events if e.offset is not None]
    distinct, seen, duplicates = [], set(), 0
    for e in events:
        k = (e.kind, e.four, e.incarnation, e.seq)
        if e.kind == "recv" and k in seen:
            duplicates += 1
            continue
        seen.add(k)
        distinct.append(e)
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

    obs = []
    for e in controlled:
        point, side = ("hop-1", "outbound") if e.kind == "send" else ("fake-upstream", "inbound")
        t = e.ts_ns // 1000
        obs.append(
            Connected(Timed("probe.jsonl", e.line, point, side, t, t), Conn(*e.four), offset=e.seq)
        )
    params = JoinParams(signals=(Signal("start", 0, window_us),), margin_us=0)
    result = connection_links(topology_for(1), obs, {WHERE: params}, key="tcp-seq")

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


if __name__ == "__main__":
    main(sys.argv)
