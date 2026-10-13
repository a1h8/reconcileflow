#!/usr/bin/env python3
"""Per-request OBI evaluation for one explicitly selected hop.

The /truth-<trace_id> path is evaluation-only. Never give it to a predictor.
Unlike set membership, this evaluation detects swaps between known trace IDs.
This is a path-labelled lab evaluation, not the protocol-level GT2 oracle.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict

from analyze_multihop import OBI_LINE

PATH = re.compile(r"\sGET (?P<path>[^ (]+)\(")
TRUTH = re.compile(r"/truth-([0-9a-f]{32})(?:$|[/?])")


def _check_ground_truth(rows: list[dict]) -> set[str]:
    expected = {row["trace_id"] for row in rows}
    if not rows or len(expected) != len(rows):
        raise ValueError("ground truth must contain nonempty, unique request identities")
    if any(row.get("schema_version") != 2 for row in rows):
        raise ValueError("schema_version=2 required for explicit request accounting")
    if any(row.get("side") != "load-gen" for row in rows):
        raise ValueError("ground truth must contain load-gen records only")
    return expected


def _score(
    rows: list[dict],
    expected: set[str],
    observed: list[tuple[str | None, str]],
    parse_failures: int,
    hop: dict,
    excluded: frozenset[str] = frozenset(),
) -> dict:
    """Score selected observations, each (request path or None, reported trace id).

    ``excluded`` holds ids of known traffic outside the measured run (warm-up,
    tail): an event whose path id -- or, without a path id, reported id -- is in
    it is counted apart and does not invalidate the run. Any other extra event
    still does.
    """
    observations: dict[str, list[str]] = defaultdict(list)
    unlabelled = unexpected = outside = 0
    for path, reported_tid in observed:
        truth = TRUTH.search(path) if path else None
        if (truth[1] if truth else reported_tid) in excluded:
            outside += 1
            continue
        if not truth:
            unlabelled += 1
            continue
        request_id = truth[1]
        if request_id not in expected:
            unexpected += 1
            continue
        observations[request_id].append(reported_tid)

    correct = swapped = unknown = missing = duplicates = 0
    for request_id in expected:
        reported = observations[request_id]
        if not reported:
            missing += 1
        elif len(reported) != 1:
            duplicates += 1
        elif reported[0] == request_id:
            correct += 1
        elif reported[0] in expected:
            swapped += 1
        else:
            unknown += 1
    selected = len(observed) - outside
    valid = not (unlabelled or unexpected or duplicates or parse_failures) and selected > 0
    return {
        "evaluation": "path_labelled_lab_not_GT2",
        "hop": hop,
        "status": "VALID" if valid else "INVALID",
        "attempted": len(rows),
        "request_failures": sum(bool(row.get("error")) for row in rows),
        "client_protocols": dict(Counter(row.get("protocol", "unavailable") for row in rows)),
        "selected_events": selected,
        "correct": correct,
        "wrong_known_trace": swapped,
        "unknown_trace": unknown,
        "missing_requests": missing,
        "duplicate_requests": duplicates,
        "unlabelled_events": unlabelled,
        "unexpected_requests": unexpected,
        "unparsed_http_lines": parse_failures,
        "excluded_outside_run": outside,
        "correct_per_attempt": correct / len(rows) if valid else None,
    }


def evaluate(
    rows: list[dict],
    lines: list[str],
    service: str,
    event: str,
    destination: str,
    excluded: frozenset[str] = frozenset(),
) -> dict:
    """Score OBI's text-printer output for one hop."""
    expected = _check_ground_truth(rows)
    observed: list[tuple[str | None, str]] = []
    parse_failures = 0
    for line in lines:
        match = OBI_LINE.match(line)
        if not match:
            if "HTTP(subType=" in line or "HTTPClient(subType=" in line:
                parse_failures += 1
            continue
        if (match["svc"], match["event"], match["dest"]) != (service, event, destination):
            continue
        path = PATH.search(line)
        observed.append((path["path"] if path else None, match["tid"]))
    hop = {"service": service, "event": event, "destination": destination}
    return _score(rows, expected, observed, parse_failures, hop, excluded)


def evaluate_json(
    rows: list[dict],
    lines: list[str],
    port: int,
    kind: str = "SPAN_KIND_SERVER",
    excluded: frozenset[str] = frozenset(),
) -> dict:
    """Score OBI's JSON-printer output (OTEL_EBPF_TRACE_PRINTER=json) for one hop.

    Selects spans of ``kind`` whose ``hostPort`` is ``port``; the request path is
    ``attributes.url``. Same scoring as the text printer.
    """
    expected = _check_ground_truth(rows)
    observed: list[tuple[str | None, str]] = []
    parse_failures = 0
    for line in lines:
        line = line.strip()
        if not line.startswith("["):
            continue  # OBI's own log lines share stdout
        try:
            spans = json.loads(line)
        except json.JSONDecodeError:
            parse_failures += 1
            continue
        for span in spans:
            if span.get("kind") != kind or int(span.get("hostPort", 0)) != port:
                continue
            observed.append((span.get("attributes", {}).get("url"), span.get("traceID", "")))
    hop = {"kind": kind, "port": port}
    return _score(rows, expected, observed, parse_failures, hop, excluded)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("obi_log")
    parser.add_argument("load_gen_jsonl")
    parser.add_argument("--json-port", type=int, help="JSON printer output: hop's server port")
    parser.add_argument("--json-kind", default="SPAN_KIND_SERVER")
    parser.add_argument("--service", help="text printer output: printed service")
    parser.add_argument("--event", choices=("HTTP", "HTTPClient"))
    parser.add_argument("--destination", help="exact printed destination, e.g. traefik:7080")
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=[],
        help="load-gen JSONL files of known traffic outside the measured run (warm-up, tail)",
    )
    args = parser.parse_args()
    with open(args.load_gen_jsonl) as source:
        rows = [json.loads(line) for line in source if line.strip()]
    excluded: set[str] = set()
    for path in args.exclude:
        with open(path) as source:
            excluded |= {json.loads(line)["trace_id"] for line in source if line.strip()}
    if excluded & {row["trace_id"] for row in rows}:
        parser.error("--exclude files share ids with the measured run")
    with open(args.obi_log) as source:
        if args.json_port is not None:
            result = evaluate_json(
                rows, list(source), args.json_port, args.json_kind, frozenset(excluded)
            )
        elif args.service and args.event and args.destination:
            result = evaluate(
                rows,
                list(source),
                args.service,
                args.event,
                args.destination,
                frozenset(excluded),
            )
        else:
            parser.error("give --json-port, or --service, --event and --destination")
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] != "VALID":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
