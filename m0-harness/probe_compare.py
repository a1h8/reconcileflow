"""Other methods on the probe's events (phase 4 spec 8.27).

Each method links the proxy's requests to its calls from the same lives the
recipe uses (``probe_recipe.lives``), and is judged by the same judge
(``probe_recipe.judge(..., linker=method)``): same truth, same second half.
A method is ``method(requests, calls, helped) -> {request index: call index}``;
a request it leaves out counts as refused (``NOT_GIVEN``). ``helped`` is
(requests, calls) the truth pairs one to one: a method that may use it is
helped by the truth, an advantage the recipe does not get (declared, spec
8.27). Without it, warm-up requests (seen by the probe, absent from the
truth) and closing exchanges shift any order-based method.

- ``fcfs``: first come, first served, helped on both sides: the k-th helped
  request by start takes the k-th helped call by start.
- ``greedy_nesting``: calls by start, each to the oldest free request that
  holds it (starts before, ends after); the nesting idea of Aguilera et al.
  2003 without its scores. Never refuses unless no free request holds a call.
- ``nothing``: links nothing (a check of the judge's refusal count).
- ``from_file``: links computed elsewhere (TraceWeaver in its own
  environment, ``tw_link.py``, from ``export_lives``).
"""

from __future__ import annotations

import json
from pathlib import Path

import probe_recipe as pr


def fcfs(requests, calls, helped):
    helped_req, helped_call = helped
    reqs = sorted(helped_req, key=lambda i: (requests[i].start_us, i))
    order = sorted(helped_call, key=lambda j: (calls[j].start_us, j))
    return dict(zip(reqs, order, strict=False))


fcfs.name = "fcfs"


def greedy_nesting(requests, calls, helped):
    by_start = sorted(range(len(requests)), key=lambda i: (requests[i].start_us, i))
    live = []  # free requests already started, oldest first
    nxt = 0
    out = {}
    for j in sorted(range(len(calls)), key=lambda j: (calls[j].start_us, j)):
        x = calls[j]
        while nxt < len(by_start) and requests[by_start[nxt]].start_us <= x.start_us:
            live.append(by_start[nxt])
            nxt += 1
        # A request ended before this call starts can hold no later call either.
        live = [i for i in live if requests[i].end_us >= x.start_us]
        for k, i in enumerate(live):
            if x.end_us <= requests[i].end_us:
                out[i] = j
                del live[k]
                break
    return out


greedy_nesting.name = "greedy_nesting"


def nothing(requests, calls, helped):
    return {}


nothing.name = "nothing"


def export_lives(cell, out) -> None:
    """The lives the judge sees in ``cell`` (same order, same indices), in
    microseconds, for a method run in another environment (``tw_link.py``)."""
    meta = json.loads((cell / "cell.json").read_text())
    requests, calls = pr.lives(
        cell / "probe.jsonl", meta["hop_ports"][0], meta["fake_upstream_port"]
    )
    lives = {
        "requests": [[r.start / 1000, r.end / 1000] for r in requests],
        "calls": [[x.start / 1000, x.end / 1000] for x in calls],
    }
    Path(out).write_text(json.dumps(lives))


def from_file(path, name):
    """A method whose links were computed elsewhere: {"links": {request
    index: call index}, "skips": [request index]}. A skip (the method says
    the request made no call) is reported as such (``SKIP``), not linked."""
    got = json.loads(Path(path).read_text())
    links = {int(i): j for i, j in got["links"].items()}

    def method(requests, calls, helped):
        return links

    method.name = name
    method.skips = frozenset(got.get("skips", ()))
    return method
