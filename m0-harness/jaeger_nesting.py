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
                        "type": (process["serviceName"], tags.get("span.kind", ""), s["operationName"]),
                        "pod": ptags.get("hostname", process["serviceName"]),
                        "start": s["startTime"],
                        "end": s["startTime"] + s["duration"],
                        "parent": number.get((trace["traceID"], refs[0]["spanID"])) if refs else None,
                        "trace": trace["traceID"],
                    }
                )
    return spans


def observable(span: dict) -> dict:
    """What the linker may see. key is an opaque identity, only used to tell
    spans apart and, after linking, to score."""
    return {k: span[k] for k in ("key", "type", "pod", "start", "end")}


def joins(spans: list[dict]) -> list[tuple[Type, Type]]:
    by_key = {s["key"]: s for s in spans}
    found = {
        (by_key[s["parent"]]["type"], s["type"])
        for s in spans
        if s["parent"] in by_key
    }
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
    rows = [({"start": a["start"], "end": a["end"]},
             {"start": b["start"], "end": b["end"], "handler": b["start"]}) for a, b in pairs]
    chosen = choose(rows)
    signals = tuple(Signal(s["name"], s["offset_us"], s["window_us"]) for s in chosen["signals"])
    return JoinParams(signals, chosen["margin_us"], 32, LOCAL_SPAN_US, LOCAL_LIMIT_PERMILLE)


def _topology(n: int):
    point = {"host": "data", "visibility": "kernel", "clock": "data",
             "timestamps": "kernel_monotonic", "inbound": [], "outbound": []}
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


def analyze(level_dir: Path, calibration: str = "isolated") -> dict:
    spans = load(level_dir)
    found = joins(spans)
    first, second = halves(spans)
    by_key = {s["key"]: s for s in spans}

    params, observations, index = {}, [], {}
    calib_pairs = {}
    for i, join in enumerate(found):
        pairs = isolated_pairs(spans, join) if calibration == "isolated" else true_pairs(spans, join, first)
        calib_pairs[i] = len(pairs)
        if len(pairs) < 2:
            continue  # no calibration possible: the join stays unlinked, reported
        params[f"j{i}-a -> j{i}-b"] = params_from(pairs)
        parents, children = _sides(spans, join)
        for role, side, group in (("a", "outbound", parents), ("b", "inbound", children)):
            for seq, s in enumerate(sorted(group, key=lambda s: s["key"])):
                o = observable(s)
                observations.append(Timed(f"j{i}{role}", seq, f"j{i}-{role}", side, o["start"], o["end"]))
                index[(f"j{i}{role}", seq)] = s["key"]

    result = timed_links(_topology(len(found)), observations, params)
    # Per join: a parent span is on the upstream side of each of its joins.
    linked = {(l.upstream[0], index[l.upstream]): index[l.downstream] for l in result.links}
    status = {
        (u.observation[0], index[u.observation]): u.status.name
        for u in result.undecided
        if u.side == "outbound"
    }

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
            "correct": c["correct"], "wrong": c["wrong"], "refused": c["refused"],
            "refused_by_status": dict(refused), "judged_parents": c["judged_parents"],
            "multiple_calls": c["multiple_calls"], "calibration_pairs": calib_pairs[i],
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


def _params_dict(p: JoinParams | None):
    if p is None:
        return None
    return {"signals": [[s.name, s.offset_us, s.window_us] for s in p.signals], "margin_us": p.margin_us}


def main(argv: list[str]) -> None:
    level = Path(argv[1])
    calibration = argv[argv.index("--calibration") + 1] if "--calibration" in argv else "isolated"
    report = analyze(level, calibration)
    if "--json" in argv:
        print(json.dumps(report, indent=2))
        return
    print(f"== {level.name}: calibration {calibration}, spans {report['spans']},"
          f" traces judged {report['traces_judged']}")
    for where, j in report["joins"].items():
        print(f"   {where}: correct {j['correct']} wrong {j['wrong']} refused {j['refused']}"
              f" {j['refused_by_status']} judged {j['judged_parents']}"
              f" multiple_calls {j['multiple_calls']} calibration_pairs {j['calibration_pairs']}")
    for s, acc in report["accuracy_by_service"].items():
        print(f"   accuracy (their way) {s}: {100 * acc:.2f}%")


if __name__ == "__main__":
    main(sys.argv)
