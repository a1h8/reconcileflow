"""Run reconcileflow's connection path (phase 4, 4a) on a harness run, and score it.

The timing path (timing_path.py) with one more fact per observation: the
four-tuple of the TCP connection it travelled on, as each end reported it
(agent spans: peer, peerPort, host, hostPort; load-gen: its own conn_key).
reconcileflow.correlation.connection links inside each four-tuple with the
calibrated windows (connection-params.toml). An empty or impossible four-tuple
is NO_KEY and stays unlinked: there is no fallback to time alone. Only then is
the truth id in each request's path read, to score the links.

J1 (hop-1 -> fake-upstream) is judged; J3 (load-gen -> hop-1) is reported
(correlation-phase4-spec.md, section 2, C3).

Per join, as in timing_path.py, plus:
- coverage: correct links / measured requests seen on both sides, counted
  before any control removes or changes anything;
- no_key: observations of either side without a usable four-tuple (the other
  statuses are counted on the upstream side, as in timing_path.py);
- connections: distinct four-tuples on the downstream side.

A cell is invalid, not failed, when a link did not speak HTTP/1.1 or when the
judged link used more than 2 x c connections (section 3: connections closed
and reopened).

Controls (one at a time, seed recorded):
- --permute SEED (K1): fake-upstream's four-tuples shuffled across connections;
- --drop PERMILLE --seed SEED (K2): that share of fake-upstream's observations removed;
- --no-connection (K4): one key for everything, i.e. the timing path less its
  density guard; the validity check is skipped, so that it runs on round 6's
  HTTP/2 cells.

--stress: judged on wrong links only, with the parameters of the highest
calibrated concurrency at most the cell's (c=200 -> c100).

The density guard is not applied inside a four-tuple (4a', in the engine);
round 4a's reading without it (--reading-no-guard, commit 59c80cb) is gone.

Usage: connection_path.py <run-dir> <connection-params.toml> [--json] [--stress]
       [--permute SEED | --drop PERMILLE --seed SEED | --no-connection]
"""

from __future__ import annotations

import json
import random
import sys
import tomllib
from collections import Counter
from pathlib import Path

from timing_path import (
    _jsonl,
    calibrated_concurrency,
    cell_meta,
    params_for,
    rate,
    topology_for,
)
from timing_path import records as timed_records

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.connection import Conn, Connected, connection_links
from reconcileflow.correlation.timing import Timed

JUDGED = "hop-1 -> fake-upstream"
S_4A = 0.99  # section 6, fixed before any run
S_4A_UP_TO = 100  # concurrencies the threshold applies to
ONE_KEY = ("one", 1, "one", 1)  # K4: a single partition, the timing path itself


def _conn_key(text: str) -> tuple | None:
    """load-gen's "addr:port-addr:port|n" as (client addr, port, server addr, port)."""
    try:
        client, server = text.split("|")[0].split("-")
        (ca, cp), (sa, sp) = client.rsplit(":", 1), server.rsplit(":", 1)
        return ca, int(cp), sa, int(sp)
    except ValueError:
        return None


def _span_conn(span: dict) -> tuple | None:
    try:
        return span["peer"], int(span["peerPort"]), span["host"], int(span["hostPort"])
    except (KeyError, ValueError):
        return None


def records(cell_dir: Path, meta: dict) -> tuple[list[dict], set[str]]:
    """timing_path's records, each with the four-tuple its source reported."""
    rows, measured = timed_records(cell_dir, meta)
    conns: dict[tuple[str, int], tuple | None] = {}
    for seq, r in enumerate(_jsonl(cell_dir / "lg.jsonl")):
        conns["lg.jsonl", seq] = _conn_key(r.get("conn_key", ""))
    agent_out = cell_dir / "agent.out"
    seq = 0
    for line in agent_out.read_text().splitlines() if agent_out.exists() else []:
        if line.startswith("["):
            for span in json.loads(line):
                seq += 1
                conns["agent.out", seq] = _span_conn(span)
    for r in rows:
        r["conn"] = conns.get((r["source"], r["seq"]))
    return rows, measured


def _invalid(cell_dir: Path, rows: list[dict], meta: dict) -> str | None:
    lg = {r.get("negotiated_protocol") for r in _jsonl(cell_dir / "lg.jsonl")}
    fu = {r.get("protocol") for r in _jsonl(cell_dir / "fu.jsonl")}
    if lg != {"http/1.1"} or fu != {"HTTP/1.1"}:
        return (
            f"not HTTP/1.1 on both links: load-gen {sorted(map(str, lg))},"
            f" fake-upstream {sorted(map(str, fu))}"
        )
    connections = len({r["conn"] for r in rows if r["point"] == "fake-upstream"})
    limit = 2 * (meta["concurrency"] or 0)
    if connections > limit:
        return f"{connections} connections on the judged link, more than 2 x c = {limit}"
    return None


def _control(rows, permute, drop, seed, no_connection) -> tuple[list[dict], str | None]:
    if (permute is not None) + (drop is not None) + bool(no_connection) > 1:
        raise ValueError("one control at a time: --permute, --drop or --no-connection")
    if no_connection:
        return [{**r, "conn": ONE_KEY} for r in rows], "K4 no connection"
    down = [r for r in rows if r["point"] == "fake-upstream"]
    if permute is not None:
        keys = sorted({r["conn"] for r in down if r["conn"] is not None})
        shuffled = list(keys)
        random.Random(permute).shuffle(shuffled)
        moved = dict(zip(keys, shuffled, strict=True))
        out = [
            {**r, "conn": moved.get(r["conn"])} if r["point"] == "fake-upstream" else r
            for r in rows
        ]
        return out, f"K1 seed={permute}"
    if drop is not None:
        gone = {id(r) for r in random.Random(seed).sample(down, round(len(down) * drop / 1000))}
        return [r for r in rows if id(r) not in gone], f"K2 drop={drop} permille seed={seed}"
    return rows, None


def analyze_cell(
    cell_dir: Path,
    params_file: Path,
    stress: bool = False,
    permute: int | None = None,
    drop: int | None = None,
    seed: int = 0,
    no_connection: bool = False,
) -> dict:
    meta = cell_meta(cell_dir)
    meta["hop_max_idle"] = json.loads((cell_dir / "cell.json").read_text()).get("hop_max_idle")
    rows, measured = records(cell_dir, meta)
    report = {"meta": meta, "measured": len(measured), "judged": JUDGED, "joins": {}}
    # K4 judges nothing of the connection: it must give the timing path's figures
    # on any cell, round 6's HTTP/2 ones included.
    invalid = None if no_connection else _invalid(cell_dir, rows, meta)
    if invalid:
        report["invalid"] = invalid
        return report
    controlled, report["control"] = _control(rows, permute, drop, seed, no_connection)
    joins = params_for(params_file, meta, check_load=not stress, stress=stress)
    report["params_from"] = calibrated_concurrency(
        tomllib.loads(params_file.read_text()), meta, stress
    )
    report["rate_per_s"] = round(rate(rows), 1)
    topology = topology_for(meta["hops"])
    for where, (params, receiver_start) in sorted(joins.items()):
        src, dst = where.split(" -> ")
        report["joins"][where] = score_join(
            topology, rows, controlled, where, src, dst, params, receiver_start
        )
    return report


def score_join(topology, rows, controlled, where, src, dst, params, receiver_start) -> dict:
    def on_join(rs):
        return [r for r in rs if (r["point"], r["side"]) in {(src, "outbound"), (dst, "inbound")}]

    before = on_join(rows)
    ups = {r["truth"] for r in before if r["point"] == src}
    both = len(ups & {r["truth"] for r in before if r["point"] == dst})
    side_rows = on_join(controlled)
    truth = {(r["source"], r["seq"]): r["truth"] for r in side_rows}
    obs = []
    for r in side_rows:
        handler = receiver_start == "handlerStart" and r["point"] == dst
        start = r["handler"] if handler else r["start"]
        timed = Timed(r["source"], r["seq"], r["point"], r["side"], start, r["end"])
        obs.append(Connected(timed, Conn(*r["conn"]) if r["conn"] is not None else None))
    result = connection_links(topology, obs, {where: params}, key="connection")
    correct = sum(1 for x in result.links if truth[x.upstream] == truth[x.downstream])
    upstream = Counter(u.status.name.lower() for u in result.undecided if u.side == "outbound")
    statuses = {s.name.lower(): upstream.get(s.name.lower(), 0) for s in Status}
    del statuses["matched"]
    return {
        "links": len(result.links),
        "correct": correct,
        "wrong": len(result.links) - correct,
        "both_seen": both,
        "coverage": round(correct / both, 6) if both else 0.0,
        **statuses,
        "no_key": sum(1 for u in result.undecided if u.status is Status.NO_KEY),
        "connections": len({r["conn"] for r in side_rows if r["point"] == dst}),
    }


def _arg(argv: list[str], name: str) -> int | None:
    return int(argv[argv.index(name) + 1]) if name in argv else None


def main(argv: list[str]) -> None:
    run_dir, params_file = Path(argv[1]), Path(argv[2])
    stress = "--stress" in argv
    options = {
        "stress": stress,
        "permute": _arg(argv, "--permute"),
        "drop": _arg(argv, "--drop"),
        "seed": _arg(argv, "--seed") or 0,
        "no_connection": "--no-connection" in argv,
    }
    cells = sorted(p.parent for p in run_dir.glob("*/lg.jsonl"))
    report = {cell.name: analyze_cell(cell, params_file, **options) for cell in cells}
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    if stress:
        print("STRESS: judged on wrong links only, with the quiet calibration")
    wrong = 0
    for name, cell in report.items():
        meta = cell["meta"]
        head = (
            f"== {name}: agent={meta['agent']} c={meta['concurrency']} measured {cell['measured']}"
        )
        if stress and cell.get("params_from") is not None:
            head += f" params from c{cell['params_from']}"
        if cell.get("control"):
            head += f" control {cell['control']}"
        print(head)
        if "invalid" in cell:
            print(f"   INVALID (not a failure): {cell['invalid']}")
            continue
        if not cell["joins"]:
            print("   no calibrated join for this agent and concurrency")
        for where, j in cell["joins"].items():
            judged = where == JUDGED
            print(
                f"   {where}{' (judged)' if judged else ''}: links {j['links']}"
                f" correct {j['correct']} wrong {j['wrong']} both_seen {j['both_seen']}"
                f" no_key {j['no_key']} no_candidate {j['no_candidate']}"
                f" ambiguous {j['ambiguous']} dense {j['dense']} connections {j['connections']}"
            )
            if judged:
                wrong += j["wrong"]
                applies = not stress and (meta["concurrency"] or 0) <= S_4A_UP_TO
                verdict = ""
                if applies:
                    verdict = f" (S-4a >= 99%: {'pass' if j['coverage'] >= S_4A else 'FAIL'})"
                print(f"      coverage {100 * j['coverage']:.2f}%{verdict}")
    print(f"wrong links on the judged join in the run: {wrong}")


if __name__ == "__main__":
    main(sys.argv)
