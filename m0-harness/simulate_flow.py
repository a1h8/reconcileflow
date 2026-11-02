#!/usr/bin/env python3
"""Simulate N exchanges through the lab chain, traceparent passed hop to hop,
and score reconcileflow's stream correlation on them.

Out of the bench: no agent, no sudo, a fixed seed. The chain is the bench's
topology (exact_path.topology_for): load-gen -> hop-1 ... hop-n ->
fake-upstream, joined on the trace id at every hop and crossing, one source
per point. Faults are injected and written down in a truth that never reaches
the engine: reuse (an exchange carries a neighbour's trace id from some hop
on, the neighbour keeping it: a collision), swap (two exchanges trade their
ids), loss (an observation is never emitted), disorder (delivered out of
order by less than the lateness), duplicates (delivered twice), late
(delivered under the watermark).

Equality of an identifier cannot see a value that is still unique per side but
carried by the wrong exchange: a swap, or a reuse whose donor lost its own
observation at that join. Those wrong links are counted and explained, never
hidden; any other wrong link is a defect.

With ``--connection`` (step B), every end also records its connection and
HTTP/2 stream, which a swap or a reuse does not touch: on a hop the two
signals disagree and nothing is linked. A swap inside a point (swap_inside)
is still a wrong link at its crossing, which has one signal; an observation
without its connection key (keyless) leaves the link standing, traced.

Usage: simulate_flow.py [--exchanges N] [--hops H] [--seed S] [--reuse F] [--swap F]
       [--swap-inside F] [--loss F] [--keyless F] [--duplicates F] [--late F]
       [--disorder-us D] [--connection] [--json]
"""

from __future__ import annotations

import argparse
import json
import random
from collections.abc import Sequence
from dataclasses import dataclass, field

from exact_path import topology_for

from reconcileflow.correlation.exact import Identity, Observation
from reconcileflow.correlation.flow import (
    Advance,
    Corroboration,
    FlowConfig,
    FlowResult,
    Kind,
    Seen,
    Status,
)
from reconcileflow.correlation.topology import Topology


@dataclass(frozen=True, slots=True)
class Faults:
    reuse: float = 0.0  # share of exchanges carrying a neighbour's trace id (it keeps it)
    swap: float = 0.0  # share of exchange pairs swapping their trace ids from a hop on
    swap_inside: float = 0.0  # the same, from inside a proxy on (between its two sides)
    loss: float = 0.0  # share of observations never emitted
    duplicates: float = 0.0  # share of observations delivered twice
    late: float = 0.0  # share of observations delivered under the watermark
    disorder_us: int = 0  # delivery shifted by less than this (and the lateness)
    keyless: float = 0.0  # share of observations without their connection key


@dataclass(frozen=True, slots=True)
class Plan:
    exchanges: int
    hops: int = 1
    gap_us: int = 1_000  # between two exchanges
    hop_delay_us: int = 100  # from one point to the next
    window_us: int = 2_000
    faults: Faults = field(default_factory=Faults)
    seed: int = 0
    connection: bool = False  # every end records its connection and stream (step B)
    connections: int = 4  # pooled connections per hop


def _points(hops: int) -> list[str]:
    return ["load-gen", *(f"hop-{k}" for k in range(1, hops + 1)), "fake-upstream"]


def _source(point: str) -> str:
    return "load-gen-log" if point == "load-gen" else f"agent-{point}"


def _sides(hops: int) -> list[tuple[str, str]]:
    """The (point, side) an exchange is seen at, in the order it crosses them."""
    out = [("load-gen", "outbound")]
    for k in range(1, hops + 1):
        out += [(f"hop-{k}", "inbound"), (f"hop-{k}", "outbound")]
    return [*out, ("fake-upstream", "inbound")]


def _joins(sides: list[tuple[str, str]]) -> list[tuple[str, int, int]]:
    """(where, upstream index, downstream index) of every join, by position in
    ``sides``: a hop joins an outbound to the next inbound, a crossing an
    inbound to the outbound of the same point."""
    joins = []
    for j in range(len(sides) - 1):
        (a, side_a), (b, _) = sides[j], sides[j + 1]
        joins.append((f"{a} -> {b}" if side_a == "outbound" else a, j, j + 1))
    return joins


@dataclass(frozen=True, slots=True)
class Simulation:
    plan: Plan
    topology: Topology
    config: FlowConfig
    journal: tuple
    truth: dict[Identity, int]  # observation -> exchange; never given to the engine
    placed: dict[tuple[int, int], Identity]  # (exchange, side index) -> observation
    reused: frozenset[Identity]
    swapped: dict[int, tuple[int, int]]  # exchange -> (the other, side index it starts at)
    reusers: frozenset[int]  # exchanges carrying i-1's id from some side on
    lost: frozenset[tuple[int, int]]  # (exchange, side index) never emitted
    late: frozenset[Identity]
    duplicates: int
    keyless: frozenset[Identity] = frozenset()


def simulate(plan: Plan) -> Simulation:
    """Build the journal of ``plan.exchanges`` exchanges, faults injected."""
    if not 1 <= plan.hops <= 5:
        raise ValueError(f"hops must be between 1 and 5, got {plan.hops}")
    faults = plan.faults
    draw = random.Random(plan.seed)  # ids and fault choices
    deliver = random.Random(f"{plan.seed}-delivery")  # order only
    topology = topology_for(plan.hops, ["relay"] * plan.hops, "headers", plan.connection)
    sides = _sides(plan.hops)
    trace = [f"{draw.getrandbits(128):032x}" for _ in range(plan.exchanges)]
    n = plan.exchanges
    # Reuse: exchange i carries i-1's id, from a drawn inbound side on. Never two
    # in a row: i-1 keeps its id (it is used twice: a collision), and i's own id
    # is carried by nobody downstream. A chain of reuses would be a swap.
    reuse_from: dict[int, int] = {}
    for i in range(n):
        start = draw.choice(range(1, len(sides), 2))
        if draw.random() < faults.reuse and i > 0 and i - 1 not in reuse_from:
            reuse_from[i] = start
    # Swap: exchanges i and i+1 exchange their ids from a drawn inbound side on.
    # Each id is still seen once per side: equality alone cannot tell, and the
    # join where the swap starts gets a wrong link. Kept apart from reuses.
    swap_from: dict[int, tuple[int, int]] = {}
    busy = set(reuse_from) | {i - 1 for i in reuse_from}
    for i in range(n - 1):
        start = draw.choice(range(1, len(sides), 2))
        if draw.random() < faults.swap and not busy & {i, i + 1}:
            swap_from[i], swap_from[i + 1] = (i + 1, start), (i, start)
            busy |= {i, i + 1}
    # Swap inside a proxy: from a drawn outbound side of a proxy on. A fault
    # model, not a measured one. Drawn only when asked, so the other faults
    # keep their draws.
    if faults.swap_inside:
        for i in range(n - 1):
            start = draw.choice(range(2, len(sides) - 1, 2))
            if draw.random() < faults.swap_inside and not busy & {i, i + 1}:
                swap_from[i], swap_from[i + 1] = (i + 1, start), (i, start)
                busy |= {i, i + 1}
    seq = dict.fromkeys((_source(p) for p in _points(plan.hops)), 0)
    truth, placed, reused, lost, late, keyless = {}, {}, set(), set(), set(), set()
    emitted: list[Seen] = []
    for i in range(plan.exchanges):
        for j, (point, side) in enumerate(sides):
            gone, is_late = draw.random() < faults.loss, draw.random() < faults.late
            if gone:
                lost.add((i, j))
                continue
            value = trace[i]
            if i in reuse_from and j >= reuse_from[i]:
                value = trace[i - 1]
            elif i in swap_from and j >= swap_from[i][1]:
                value = trace[swap_from[i][0]]
            source = _source(point)
            identity = (source, seq[source])
            seq[source] += 1
            keys = {"traceparent": value}
            if plan.connection:
                # The hop this side is an end of: the one it sends on, or receives on.
                hop = j // 2
                c = i % plan.connections
                if not (faults.keyless and draw.random() < faults.keyless):
                    keys["tuple5"] = f"10.0.{hop}.1:{40000 + c}>10.0.{hop}.2:443"
                    keys["h2_stream_id"] = str(2 * (i // plan.connections) + 1)
                else:
                    keyless.add(identity)
            obs = Observation(source, identity[1], point, side, keys)
            truth[identity], placed[(i, j)] = i, identity
            if i in reuse_from and value != trace[i]:
                reused.add(identity)
            if is_late:
                late.add(identity)
            emitted.append(Seen(obs, i * plan.gap_us + j * (plan.hop_delay_us // 2)))
    on_time = [e for e in emitted if (e.observation.source, e.observation.seq) not in late]
    jitter = {id(e): deliver.random() * faults.disorder_us for e in on_time}
    journal, latest = [], 0
    sources = sorted(seq)
    for entry in sorted(on_time, key=lambda e: e.at + jitter[id(e)]):
        journal.append(entry)
        latest = max(latest, entry.at)
        if deliver.random() < 0.2:
            journal += [Advance(s, latest) for s in sources]
    copies = [e for e in on_time if draw.random() < faults.duplicates]
    for entry in copies:  # delivered again, somewhere after the original
        journal.insert(deliver.randint(journal.index(entry) + 1, len(journal)), entry)
    end = max((e.at for e in emitted), default=0) + plan.window_us + faults.disorder_us + 1
    journal += [Advance(s, end) for s in sources]
    journal += [e for e in emitted if (e.observation.source, e.observation.seq) in late]
    config = FlowConfig(tuple(sources), plan.window_us, lateness_us=faults.disorder_us)
    return Simulation(
        plan,
        topology,
        config,
        tuple(journal),
        truth,
        placed,
        frozenset(reused),
        swap_from,
        frozenset(reuse_from),
        frozenset(lost),
        frozenset(late),
        len(copies),
        frozenset(keyless),
    )


def score(simulation: Simulation, result: FlowResult) -> dict:
    """Judge the engine's decisions against the truth it never saw."""
    truth = simulation.truth
    effective = result.effective()
    finals = {k: d for k, d in effective.items() if d.status is Status.FINAL}
    linked = [d for d in finals.values() if d.kind is Kind.LINKED]
    wrong = [d for d in linked if {truth[p] for p in d.partners} != {truth[d.business_key[1]]}]
    kinds: dict[str, int] = {}
    for d in finals.values():
        kinds[d.kind.value] = kinds.get(d.kind.value, 0) + 1
    sides = _sides(simulation.plan.hops)
    on_reused = [d for k, d in finals.items() if k[1] in simulation.reused]
    # A swap's wrong links sit at the join just before the side it starts at.
    swap_joins = {
        (where, i)
        for i, (_, start) in simulation.swapped.items()
        for where, _up, down in _joins(_sides(simulation.plan.hops))
        if down == start
    }
    at_swap = [d for d in wrong if (d.business_key[0], truth[d.business_key[1]]) in swap_joins]
    # A reuse whose donor lost an observation, or got it delivered late (LATE,
    # out of the group): the reused value is unique there.
    late_at = {k for k, identity in simulation.placed.items() if identity in simulation.late}
    absent = simulation.lost | late_at
    donors_lost = {
        i for i in simulation.reusers if any((i - 1, j) in absent for j in range(len(sides)))
    }
    pairs = {frozenset((i, i - 1)) for i in donors_lost}
    by_reuse_loss = [
        d
        for d in wrong
        if d not in at_swap
        and frozenset({truth[d.business_key[1]], *(truth[p] for p in d.partners)}) in pairs
    ]
    at_hops = [d for d in wrong if " -> " in d.business_key[0]]
    swap_hop_joins = {(w, i) for w, i in swap_joins if " -> " in w}
    conflicted_at_start = sum(
        d.kind is Kind.CONFLICTED
        for (where, identity), d in finals.items()
        if (where, truth[identity]) in swap_hop_joins
    )
    expected = found = 0
    sides = _sides(simulation.plan.hops)
    reused_exchanges = {truth[i] for i in simulation.reused}
    # A keyless end of a hop whose exchange is untouched by a reuse or a swap,
    # its partner there delivered on time: the link stands, traced absent.
    untouched = set(range(simulation.plan.exchanges)) - reused_exchanges
    untouched -= set(simulation.swapped) | {i - 1 for i in simulation.reusers}
    absent_expected = absent_found = 0
    for (i, j), identity in simulation.placed.items():
        if identity not in simulation.keyless or i not in untouched or identity in simulation.late:
            continue
        up, down = (j, j + 1) if j % 2 == 0 else (j - 1, j)
        partner = simulation.placed.get((i, down if j == up else up))
        if partner is None or partner in simulation.late:
            continue
        absent_expected += 1
        d = finals.get((f"{sides[up][0]} -> {sides[down][0]}", identity))
        absent_found += (
            d is not None
            and d.kind is Kind.LINKED
            and d.corroboration is Corroboration.SIGNAL_ABSENT
        )
    corroboration: dict[str, int] = {}
    for (_, c), count in result.signal_counts().items():
        corroboration[c.value] = corroboration.get(c.value, 0) + count
    for i, j in sorted(simulation.lost):
        if i in reused_exchanges:
            continue  # its partners' outcome depends on the reuse too
        if i in simulation.swapped or i + 1 in simulation.reusers:
            continue  # and on a swap, or a neighbour reusing its id (a collision)
        for where, up, down in _joins(sides):
            if j not in (up, down):
                continue
            partner = simulation.placed.get((i, down if j == up else up))
            if partner is None or partner in simulation.late:
                continue
            expected += 1
            d = finals.get((where, partner))
            found += d is not None and d.kind is Kind.UNLINKED
    return {
        "exchanges": simulation.plan.exchanges,
        "hops": simulation.plan.hops,
        "decisions": kinds,
        "linked": len(linked),
        "wrong": len(wrong),
        "wrong_at_hops": len(at_hops),
        "wrong_at_crossings": len(wrong) - len(at_hops),
        "wrong_at_hops_untraced": sum(
            d.corroboration is not Corroboration.SIGNAL_ABSENT for d in at_hops
        ),
        "corroboration": corroboration,
        "open": sum(d.status is not Status.FINAL for d in effective.values()),
        "dossiers": len(result.dossiers),
        "reuse": {
            "injected": len(reused_exchanges),
            "linked": sum(d in wrong for d in on_reused),
            "collided": sum(d.kind is Kind.COLLIDED for d in on_reused),
            "after_final": sum(d.kind is Kind.VALUE_AFTER_FINAL for d in on_reused),
        },
        "swap": {
            "injected": len(simulation.swapped) // 2,
            "wrong_at_start": len(at_swap),
            "conflicted_at_start": conflicted_at_start,
        },
        "wrong_explained": {"swap": len(at_swap), "reuse_with_donor_loss": len(by_reuse_loss)},
        "wrong_unexplained": len(wrong) - len(at_swap) - len(by_reuse_loss),
        "loss": {
            "injected": len(simulation.lost),
            "unlinked_expected": expected,
            "unlinked_found": found,
        },
        "late": {
            "injected": len(simulation.late),
            "found": sum(d.kind is Kind.LATE for d in finals.values()),
        },
        "duplicates": {"injected": simulation.duplicates, "counted": result.duplicates},
        "keyless": {
            "injected": len(simulation.keyless),
            "absent_expected": absent_expected,
            "absent_found": absent_found,
        },
        "finals": sorted(
            (repr(k), d.kind.value, d.value, repr(d.partners)) for k, d in finals.items()
        ),
    }


def main(argv: Sequence[str] | None = None) -> None:
    from reconcileflow.correlation.flow import replay_flow

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--exchanges", type=int, default=1000)
    parser.add_argument("--hops", type=int, default=1)
    parser.add_argument("--seed", type=int, default=0)
    for name in ("reuse", "swap", "swap-inside", "loss", "keyless", "duplicates", "late"):
        parser.add_argument(f"--{name}", type=float, default=0.0)
    parser.add_argument("--disorder-us", type=int, default=0)
    parser.add_argument("--connection", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    faults = Faults(
        args.reuse,
        args.swap,
        args.swap_inside,
        args.loss,
        args.duplicates,
        args.late,
        args.disorder_us,
        args.keyless,
    )
    plan = Plan(
        args.exchanges, args.hops, faults=faults, seed=args.seed, connection=args.connection
    )
    simulation = simulate(plan)
    report = score(
        simulation, replay_flow(simulation.topology, simulation.journal, simulation.config)
    )
    del report["finals"]
    if args.json:
        print(json.dumps(report, indent=2))
        return
    print(
        f"{report['exchanges']} exchanges, {report['hops']} hops: "
        f"{report['linked']} final links, {report['wrong']} wrong, {report['open']} still open, "
        f"{report['dossiers']} dossiers"
    )
    print(
        f"  wrong at hops: {report['wrong_at_hops']} "
        f"(untraced {report['wrong_at_hops_untraced']}), "
        f"at crossings: {report['wrong_at_crossings']}"
    )
    for name in ("reuse", "swap", "loss", "keyless", "late", "duplicates"):
        print(f"  {name}: {report[name]}")
    explained, unexplained = report["wrong_explained"], report["wrong_unexplained"]
    print(f"  wrong links explained: {explained}, unexplained: {unexplained}")
    print(f"  decisions: {report['decisions']}")
    print(f"  corroboration at hops: {report['corroboration']}")


if __name__ == "__main__":
    main()
