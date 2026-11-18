# Evidence: how to rerun every check behind the recipe's evaluation

The verdict of the recipe on the probe's events (`docs/probe-recipe-evaluation.md`)
rests on more than the judgment itself. This directory holds every script used
for the checks around it, so that someone else can rerun them on the same run
data and get the same numbers. Results are **offline**: each cell was recorded
first, then judged as a whole; nothing here is a live (streaming) result.

Run data: the 60 cells of the evaluation days `evalr-M1` and `evalr-M2` (lists
`<runs>/evalr-M1-runs.txt`, `<runs>/evalr-M2-runs.txt`, one line per cell:
`<run-dir> <agent> <concurrency> <load>`). It stays on the evaluation machine
(about 760 MB for a c800 cell). Run everything from `m0-harness/`:

```sh
export PYTHONPATH=../src:.:evidence
L="<runs>/evalr-M1-runs.txt <runs>/evalr-M2-runs.txt"
```

## The checks

| script | question | result on M1 + M2 |
|---|---|---|
| `main_judgment.py 7 $L` | the method under evaluation, per cell, without the controls | 982,478 judged, 982,417 right, **0 wrong**, 61 refused (all `AMBIGUOUS`), 0 causality violations |
| `conditions.py <main.json> 6 $L` | (a) does the learnt size rule ever drop a true link? (b) how many truth records were attached to a connection by the clock (a client port opened twice)? | (a) **0 of 1,800,000** true links outside the rule; (b) 730 load-gen records, 5 agent cells |
| `second_judge.py 6 $L` | do the links held agree with a truth attached another way (the agent's spans, by port and order; header on one side, request path on the other)? | 646,237 links checked of 657,323 (98.3 %), **0 disagreements**; 0 truth differences over 1,194,865 requests and 1,306,839 calls |
| `second_judge.py --first-pass 6 $L` | the same, client spans ranked by start, overlapping ports set aside | 284,573 checked, 0 disagreements |
| `validate_end_order.py 6 $L` | may client spans be ranked by their end? (uses the clock, for this check only) | same order on 5,150 of 5,150 non-overlapping ports; 99.85 % nearest on the others |
| `compare_methods.py 5 $L` | other methods on the same lives, truth and judged half | see below |
| `run_elsewhere.sh export\|crosstrace\|traceweaver $L` then `judge_from_files.py <links> crosstrace 5 $L` | methods needing numpy, scipy, scikit-learn, run in another environment | see below |

Agent cells only for the second judge and its validation (the agent's spans
are the second source of truth); the other scripts take all 60 cells.

## Other methods on the same events

Same judge, same 982,478 judged requests for every method; right / wrong (%):

| method | c20 | c800 | wrong links | cells with a wrong link |
|---|---|---|---|---|
| recipe + size signature (the evaluated method) | 100.00 / 0 | 99.98 / 0 | 0 | 0 / 60 |
| recipe, timings only | 99.81 / 0 | 90.71 / 0 | 0 | 0 / 60 |
| in the manner of CrossTrace (our rewrite, choices declared in `crosstrace_like.py`) | 99.95 / 0.01 | 90.33 / 3.59 | 9,066 | 53 / 60 |
| greedy nesting | 98.58 / 0.90 | 53.74 / 39.01 | 168,635 | 60 / 60 |
| first come, first served (helped by the truth) | 97.86 / 2.14 | 34.44 / 65.56 | 276,879 | 60 / 60 |

TraceWeaver (its own code at `ee8dff3`, through `tw_link.py`) does not fit
this size of cell: 1,800 of 30,355 requests linked in about 22 hours on one
core, against a limit of 4 hours per cell set before the run. No figure of
TraceWeaver on these cells is reported.

## What these checks do not cover

- the second judge checks how the truth is attached to the probe's exchanges,
  not load-gen itself (both of its carriers come from load-gen); cells without
  an agent have no second judge;
- one bench: one hop, HTTP/1.1, one relay, response bodies that favour sizes;
- CrossTrace's code is not public: the rewrite makes choices the paper leaves
  open, so its figures are ours, not theirs.
