"""Run reconcileflow's timing path on published Jaeger traces, and score it
(phase 4 spec, sections 8.10 and 8.10.1: level A of the comparison).

Input: one directory of Jaeger JSON exports ({"data": [trace, ...]}), e.g. one
load level of a published dataset. Each span is typed (service, span.kind,
operation). A join is a (parent type, child type) pair found in the data: the
type graph is taken as known, as the compared work assumes, never a request's
own parent.

The linker sees, per span, only its type, its pod and its start and end
(observable): no trace id, no span id, no parent, no tag. Per join, the
parent spans are the upstream side and the child spans the downstream side of
one timing hop of reconcileflow.correlation.timing, with the start and end
signals of the hop and the rule of calibrate_timing.py (choose) for windows
and margin, and its local density guard. Nothing in the engine changes.

Calibration, one of:
- isolated (default, no truth): the pairs where the child is nested in only
  one parent of the parent type (same pod when both types are of one
  service) and that parent holds only that child of the child type;
- truth: the true pairs of the traces that start in the first half in time.

Judged on the traces that start in the second half in time (median of the
trace starts), for every method alike. Per join and parent span: correct,
wrong, refused (by status), and "multiple calls" apart (a parent with more
than one true child of the child type: never judged as linked or not). Their
accuracy, per service: the share of judged parent spans whose every outgoing
join gives the true child (or none when there is none); a refusal is wrong.

Usage: jaeger_nesting.py <level-dir> [--calibration isolated|truth] [--json]
       [--theirs <converted e2e json>]   (aligned comparison, spec 8.10.4)
"""

from __future__ import annotations

import bisect
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from calibrate_timing import LOCAL_LIMIT_PERMILLE, LOCAL_SPAN_US, choose

from reconcileflow.correlation.timing import JoinParams, Signal, Timed, timed_links
from reconcileflow.correlation.topology import load as load_topology

Type = tuple[str, str, str]  # (service, span.kind, operation)


def load(level_dir: Path) -> list[dict]:
    """Every span of every trace file, sorted by file name, with its truth apart.
    key is a number in load order: it carries no trace or span id."""
    spans, number = [], {}
    for path in sorted(level_dir.glob("*.json")):
        for trace in json.loads(path.read_text())["data"]:
            processes = trace["processes"]
            for s in trace["spans"]:
                number[trace["traceID"], s["spanID"]] = len(number)
            for s in trace["spans"]:
                tags = {t["key"]: t["value"] for t in s.get("tags", [])}
                process = processes[s["processID"]]
                ptags = {t["key"]: t["value"] for t in process.get("tags", [])}
                refs = [r for r in s.get("references", []) if r.get("refType") == "CHILD_OF"]
                spans.append(
                    {
                        "key": number[trace["traceID"], s["spanID"]],
                        "type": (
                            process["serviceName"],
                            tags.get("span.kind", ""),
                            s["operationName"],
                        ),
                        "pod": ptags.get("hostname", process["serviceName"]),
                        "start": s["startTime"],
                        "end": s["startTime"] + s["duration"],
                        "parent": number.get((trace["traceID"], refs[0]["spanID"]))
                        if refs
                        else None,
                        "trace": trace["traceID"],
                        "sid": s["spanID"],
                    }
                )
    return spans


def observable(span: dict) -> dict:
    """What the linker may see. key is an opaque identity, only used to tell
    spans apart and, after linking, to score."""
    return {k: span[k] for k in ("key", "type", "pod", "start", "end")}


def joins(spans: list[dict]) -> list[tuple[Type, Type]]:
    by_key = {s["key"]: s for s in spans}
    found = {(by_key[s["parent"]]["type"], s["type"]) for s in spans if s["parent"] in by_key}
    return sorted(found)


def _nested(a: dict, b: dict, same_service: bool) -> bool:
    return (
        a["start"] <= b["start"]
        and b["end"] <= a["end"]
        and (not same_service or a["pod"] == b["pod"])
    )


def _sides(spans, join):
    parents = sorted((s for s in spans if s["type"] == join[0]), key=lambda s: s["start"])
    children = [s for s in spans if s["type"] == join[1]]
    return parents, children


def isolated_pairs(spans: list[dict], join: tuple[Type, Type]) -> list[tuple[dict, dict]]:
    """Pairs found without truth: one nesting parent, one nested child."""
    parents, children = _sides(spans, join)
    same = join[0][0] == join[1][0]
    starts = [a["start"] for a in parents]
    longest = max((a["end"] - a["start"] for a in parents), default=0)
    holders: dict[tuple, list[dict]] = {}
    held = Counter()
    for b in children:
        # Only parents starting in [b.end - longest, b.start] can hold b.
        low = bisect.bisect_left(starts, b["end"] - longest)
        high = bisect.bisect_right(starts, b["start"])
        hs = [a for a in parents[low:high] if _nested(a, b, same)]
        holders[b["key"]] = hs
        for a in hs:
            held[a["key"]] += 1
    out = []
    for b in children:
        hs = holders[b["key"]]
        if len(hs) == 1 and held[hs[0]["key"]] == 1:
            out.append((observable(hs[0]), observable(b)))
    return out


def true_pairs(spans, join, traces):
    by_key = {s["key"]: s for s in spans}
    return [
        (observable(by_key[s["parent"]]), observable(s))
        for s in spans
        if s["type"] == join[1]
        and s["trace"] in traces
        and s["parent"] in by_key
        and by_key[s["parent"]]["type"] == join[0]
    ]


def params_from(pairs) -> JoinParams:
    rows = [
        (
            {"start": a["start"], "end": a["end"]},
            {"start": b["start"], "end": b["end"], "handler": b["start"]},
        )
        for a, b in pairs
    ]
    chosen = choose(rows)
    signals = tuple(Signal(s["name"], s["offset_us"], s["window_us"]) for s in chosen["signals"])
    return JoinParams(signals, chosen["margin_us"], 32, LOCAL_SPAN_US, LOCAL_LIMIT_PERMILLE)


def _topology(n: int):
    point = {
        "host": "data",
        "visibility": "kernel",
        "clock": "data",
        "timestamps": "kernel_monotonic",
        "inbound": [],
        "outbound": [],
    }
    points = {f"j{i}-{r}": dict(point) for i in range(n) for r in "ab"}
    hops = [{"from": f"j{i}-a", "to": f"j{i}-b", "pooling": True} for i in range(n)]
    return load_topology({"points": points, "hops": hops})


def halves(spans):
    starts = defaultdict(lambda: None)
    for s in spans:
        t = s["trace"]
        starts[t] = s["start"] if starts[t] is None else min(starts[t], s["start"])
    ordered = sorted(starts.values())
    cutoff = ordered[len(ordered) // 2]
    first = {t for t, v in starts.items() if v < cutoff}
    return first, set(starts) - first


def _link(spans, found, first, calibration):
    """Our links, per join: {(f"j{i}a", parent key): child key}, the refusal
    status of the unlinked parents, the params and the calibration pairs."""
    params, observations, index = {}, [], {}
    calib_pairs = {}
    for i, join in enumerate(found):
        pairs = (
            isolated_pairs(spans, join)
            if calibration == "isolated"
            else true_pairs(spans, join, first)
        )
        calib_pairs[i] = len(pairs)
        if len(pairs) < 2:
            continue  # no calibration possible: the join stays unlinked, reported
        params[f"j{i}-a -> j{i}-b"] = params_from(pairs)
        parents, children = _sides(spans, join)
        for role, side, group in (("a", "outbound", parents), ("b", "inbound", children)):
            for seq, s in enumerate(sorted(group, key=lambda s: s["key"])):
                o = observable(s)
                observations.append(
                    Timed(f"j{i}{role}", seq, f"j{i}-{role}", side, o["start"], o["end"])
                )
                index[(f"j{i}{role}", seq)] = s["key"]

    result = timed_links(_topology(len(found)), observations, params)
    # Per join: a parent span is on the upstream side of each of its joins.
    linked = {
        (link.upstream[0], index[link.upstream]): index[link.downstream] for link in result.links
    }
    status = {
        (u.observation[0], index[u.observation]): u.status.name
        for u in result.undecided
        if u.side == "outbound"
    }
    return linked, status, params, calib_pairs


def analyze(level_dir: Path, calibration: str = "isolated") -> dict:
    spans = load(level_dir)
    found = joins(spans)
    first, second = halves(spans)
    by_key = {s["key"]: s for s in spans}
    linked, status, params, calib_pairs = _link(spans, found, first, calibration)

    report, outcome = {"calibration": calibration, "joins": {}}, defaultdict(dict)
    for i, join in enumerate(found):
        parents, _ = _sides(spans, join)
        kids = defaultdict(list)
        for s in spans:
            if s["type"] == join[1] and s["parent"] is not None:
                kids[s["parent"]].append(s["key"])
        c = Counter()
        refused = Counter()
        for a in parents:
            if a["trace"] not in second:
                continue
            true = kids.get(a["key"], [])
            if len(true) > 1:
                c["multiple_calls"] += 1
                outcome[a["key"]][i] = None
                continue
            c["judged_parents"] += 1
            got = linked.get((f"j{i}a", a["key"]))
            if got is None:
                if true:
                    c["refused"] += 1
                    refused[status.get((f"j{i}a", a["key"]), "NOT_CALIBRATED")] += 1
                ok = not true
            else:
                ok = bool(true) and got == true[0]
                c["correct" if ok else "wrong"] += 1
            outcome[a["key"]][i] = ok
        report["joins"][f"{'/'.join(join[0])} -> {'/'.join(join[1])}"] = {
            "correct": c["correct"],
            "wrong": c["wrong"],
            "refused": c["refused"],
            "refused_by_status": dict(refused),
            "judged_parents": c["judged_parents"],
            "multiple_calls": c["multiple_calls"],
            "calibration_pairs": calib_pairs[i],
            "params": _params_dict(params.get(f"j{i}-a -> j{i}-b")),
        }

    # Their accuracy, per service: every outgoing join of a judged parent right.
    per_service = defaultdict(lambda: [0, 0])
    for key, by_join in outcome.items():
        if any(v is None for v in by_join.values()):
            continue
        service = by_key[key]["type"][0]
        per_service[service][1] += 1
        per_service[service][0] += all(by_join.values())
    report["accuracy_by_service"] = {s: ok / n for s, (ok, n) in sorted(per_service.items())}
    report["spans"] = len(spans)
    report["traces_judged"] = len(second)
    return report


def aligned(level_dir: Path, theirs: dict, calibration: str = "isolated") -> dict:
    """Aligned comparison (phase 4 spec 8.10.4): their problem only (server
    span of a service -> its client span toward each called service), on the
    traces of the second half, for every method alike.

    Unit (slot): a true child of child type C in trace t whose parent, a
    server span of the same service, has exactly one child of type C; a trace
    where a parent has several is apart for C. A method's predicted children of
    type C for trace t: in the slots, right; else wrong; the slots left,
    refused. End to end, a trace with at least one slot: right (all right),
    without wrong (none wrong), with refusal (at least one refused).
    theirs: {"traces": {method: {trace: {"true": [[t, s]], "pred": [[t, s] | None]}}}}."""
    spans = load(level_dir)
    found = joins(spans)
    first, second = halves(spans)
    by_key = {s["key"]: s for s in spans}
    key_of = {(s["trace"], s["sid"]): s["key"] for s in spans}
    with_parent = {s["type"] for s in spans if s["parent"] is not None}

    def their_key(ident):
        t, sid = ident
        k = key_of.get((t, sid))
        if k is None and sid.endswith("_client"):  # their copy of our server span
            k = key_of.get((t, sid[: -len("_client")]))
        return k

    compared = [
        i
        for i, (a, b) in enumerate(found)
        if (a[1] == "server" and b[1] == "client" and a[0] == b[0])
        or (b[1] == "server" and a[0] != b[0] and (a[1] == "server" or a not in with_parent))
    ]
    child_types = {found[i][1] for i in compared}
    if len(child_types) != len(compared):
        raise ValueError("one child type under two compared joins: slots would mix")

    pairs = {found[i] for i in compared}
    kids = defaultdict(list)  # (parent key, child type) -> child keys
    for s in spans:
        if s["parent"] in by_key and (by_key[s["parent"]]["type"], s["type"]) in pairs:
            kids[s["parent"], s["type"]].append(s["key"])
    slots, apart = defaultdict(set), set()  # (trace, child type) -> child keys
    for (parent, c), ks in kids.items():
        t = by_key[parent]["trace"]
        if t not in second:
            continue
        if len(ks) > 1:
            apart.add((t, c))
        else:
            slots[t, c].add(ks[0])
    expected = defaultdict(set)
    for (parent, c), ks in kids.items():
        if by_key[parent]["trace"] in second:
            expected[by_key[parent]["trace"], c].update(ks)
    for tc in apart:
        slots.pop(tc, None)

    predictions = {}  # method -> {(trace, child type): [child key | None]}
    linked, _, _, _ = _link(spans, found, first, calibration)
    ours = defaultdict(list)
    for (_, parent), child in linked.items():
        c = by_key[child]["type"]
        if c in child_types:
            ours[by_key[parent]["trace"], c].append(child)
    predictions["ours"] = ours
    unmatched, mismatch = Counter(), Counter()
    for method, traces in theirs["traces"].items():
        got = defaultdict(list)
        their_true = defaultdict(set)
        for t, lists in traces.items():
            if t not in second:
                continue
            for p in lists["true"]:
                k = their_key(p)
                if k is not None and by_key[k]["type"] in child_types:
                    their_true[t, by_key[k]["type"]].add(k)
            for p in lists["pred"]:
                if p is None:
                    continue
                k = their_key(p)
                if k is None:
                    unmatched[method] += 1
                elif by_key[k]["type"] in child_types:
                    got[t, by_key[k]["type"]].append(k)
        predictions[method] = got
        mismatch[method] = sum(
            their_true.get(tc, set()) != expected.get(tc, set())
            for tc in set(their_true) | set(expected)
        )

    report = {
        "calibration": calibration,
        "compared": [f"{'/'.join(found[i][0])} -> {'/'.join(found[i][1])}" for i in compared],
        "apart_multiple_calls": len(apart),
        "truth_mismatch": max(mismatch.values(), default=0),
        "methods": {},
    }
    for method, got in predictions.items():
        n, per_trace = Counter(), defaultdict(Counter)
        for (t, c), keys in slots.items():
            right = sum(k in keys for k in got.get((t, c), []))
            wrong = len(got.get((t, c), [])) - right
            refused = len(keys) - right - wrong
            if refused < 0:
                raise ValueError(f"{method}: more predictions than slots in {t}")
            for name, v in (("right", right), ("wrong", wrong), ("refused", refused)):
                n[name] += v
                per_trace[t][name] += v
        traces = Counter(judged=len(per_trace))
        for c in per_trace.values():
            traces["right"] += c["wrong"] == 0 and c["refused"] == 0
            traces["without_wrong"] += c["wrong"] == 0
            traces["with_refusal"] += c["refused"] > 0
        report["methods"][method] = {
            "slots": {k: n[k] for k in ("right", "wrong", "refused")},
            "traces": {k: traces[k] for k in ("judged", "right", "without_wrong", "with_refusal")},
            "unmatched": unmatched[method],
        }
    return report


def _params_dict(p: JoinParams | None):
    if p is None:
        return None
    return {
        "signals": [[s.name, s.offset_us, s.window_us] for s in p.signals],
        "margin_us": p.margin_us,
    }


def main(argv: list[str]) -> None:
    level = Path(argv[1])
    calibration = argv[argv.index("--calibration") + 1] if "--calibration" in argv else "isolated"
    if "--theirs" in argv:
        theirs = json.loads(Path(argv[argv.index("--theirs") + 1]).read_text())
        print(json.dumps(aligned(level, theirs, calibration), indent=2))
        return
    report = analyze(level, calibration)
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    print(
        f"== {level.name}: calibration {calibration}, spans {report['spans']},"
        f" traces judged {report['traces_judged']}"
    )
    for where, j in report["joins"].items():
        print(
            f"   {where}: correct {j['correct']} wrong {j['wrong']} refused {j['refused']}"
            f" {j['refused_by_status']} judged {j['judged_parents']}"
            f" multiple_calls {j['multiple_calls']} calibration_pairs {j['calibration_pairs']}"
        )
    for s, acc in report["accuracy_by_service"].items():
        print(f"   accuracy (their way) {s}: {100 * acc:.2f}%")


if __name__ == "__main__":
    main(sys.argv)
