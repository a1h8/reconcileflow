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


def evaluate(
    rows: list[dict], lines: list[str], service: str, event: str, destination: str
) -> dict:
    expected = {row["trace_id"] for row in rows}
    if not rows or len(expected) != len(rows):
        raise ValueError("ground truth must contain nonempty, unique request identities")
    if any(row.get("schema_version") != 2 for row in rows):
        raise ValueError("schema_version=2 required for explicit request accounting")
    if any(row.get("side") != "load-gen" for row in rows):
        raise ValueError("ground truth must contain load-gen records only")

    observations: dict[str, list[str]] = defaultdict(list)
    selected = unlabelled = unexpected = parse_failures = 0
    for line in lines:
        match = OBI_LINE.match(line)
        if not match:
            if "HTTP(subType=" in line or "HTTPClient(subType=" in line:
                parse_failures += 1
            continue
        if (match["svc"], match["event"], match["dest"]) != (service, event, destination):
            continue
        selected += 1
        path = PATH.search(line)
        truth = TRUTH.search(path["path"]) if path else None
        if not truth:
            unlabelled += 1
            continue
        request_id = truth[1]
        if request_id not in expected:
            unexpected += 1
            continue
        observations[request_id].append(match["tid"])

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
    valid = not (unlabelled or unexpected or duplicates or parse_failures) and selected > 0
    return {
        "evaluation": "path_labelled_lab_not_GT2",
        "hop": {"service": service, "event": event, "destination": destination},
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
        "correct_per_attempt": correct / len(rows) if valid else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("obi_log")
    parser.add_argument("load_gen_jsonl")
    parser.add_argument("--service", required=True)
    parser.add_argument("--event", choices=("HTTP", "HTTPClient"), required=True)
    parser.add_argument(
        "--destination", required=True, help="exact printed destination, e.g. traefik:7080"
    )
    args = parser.parse_args()
    with open(args.load_gen_jsonl) as source:
        rows = [json.loads(line) for line in source if line.strip()]
    with open(args.obi_log) as source:
        result = evaluate(rows, list(source), args.service, args.event, args.destination)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] != "VALID":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
