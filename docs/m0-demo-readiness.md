# M0 demonstration: readiness and next steps

Status: 2026-09-29. This is the execution order for a defensible lab demonstration.
The matching library, the M0 evaluation harness, and the target operational platform
have different completion criteria. Finishing this harness does not implement the
platform or establish PCI-DSS compliance.

## 1. OBI dependency: merged and measured — the per-stream fix holds under load

[OBI PR #3587](https://github.com/open-telemetry/opentelemetry-ebpf-instrumentation/pull/3587)
merged 2026-09-29T09:18:34Z, merge commit `335c1c9eac0d908193609aca66fb3205240fb0ac`
on `main`, parent `5016ad0019229860483c9e0e541ca7179c3bb6b6`. The correction
retracting the permanent-cliff claim was already
[published on #3571](https://github.com/open-telemetry/opentelemetry-ebpf-instrumentation/issues/3571#issuecomment-5843336129).

**Method.** Both commits built from source with the same toolchain (Go 1.27.1,
clang 21.1.8) — not a downloaded release: the last tagged release (`v0.13.0`) is
248 commits behind, and OBI's `main`/`nightly` image tags are rolling, already
overwritten by the merge, so no artifact for the pre-merge commit is recoverable
from any registry. Building both this way isolates exactly this diff and removes
build-toolchain as a confound. The 2-hop repro topology (`load-gen` → Traefik
v3.7.13, pooled backend connection → `fake-upstream`) that originally produced
the 999/2000 cliff and the concurrency-sweep table above was rebuilt from
scratch — none of it persisted on disk (gitignored) — plus a `/broken` route
(Traefik middleware stripping `traceparent`) as a negative control.

`analyze_run.py` scored each request at three hops independently — Traefik
inbound (`traefik:48736`), Traefik outbound on its pooled backend connection
(`127.0.0.1:8443:8443`), `fake-upstream` inbound (`fake-upstream:8443`) — as
correct / swapped to another real, known `trace_id` / self-authored, rather
than one end-to-end verdict. Events were attributed to their run by the
request's own `trace_id` (globally unique, verified collision-free), not by
log line-count markers — the line-marker approach used in earlier sections of
`m0-harness/README.md` is known to leak a few boundary events across a
flush-timing race; keying on identity instead eliminates that artifact class
entirely.

**Result — correct-attribution rate, by concurrency:**

| Concurrency | `old` (`5016ad00`) | `new` (`335c1c9e`) |
|---|---|---|
| c=20 (mean of 5 reps) | 96.2–96.3%, SD 0.008–0.011 | **100.000%**, SD 0.000 |
| c=50 | 90.9–95.1% | **100.000%** |
| c=100 | 80.1–84.0% | **100.000%** |
| c=200 / n=10000 (mean of 2 reps) | 83.7–89.0% | **100.000%** |

`old`'s error rate is dominated by `wrong_known_trace` — a request's response
correctly arrives, but OBI attributes it to a *different*, real, concurrently
in-flight request's `trace_id`, not just a missing one — and it worsens
monotonically with concurrency (445–728 swaps per 10000 at c=200, up from
7–28 per 1000 at c=20). `new` shows zero swaps and zero self-authored events
at every concurrency level tested, c=20 through c=200 — 103,000 hop
observations total, none wrong. The SD at c=20 is within this repo's own
`stability.assess()` threshold (`MAX_SD=0.015`) on both sides, so the c=20 gap
itself is `STABLE`, not noise; the c=200 point is 2 reps, not 5 — enough to
see the trend is not noise (same direction and order of magnitude as the
c=20–100 progression), not enough to claim a formal stability verdict at that
scale.

**Negative control held on both binaries.** The `/broken` route's
`fake-upstream` hop reads 0% correct / 100% self-authored on `old` *and* `new`
— the header is genuinely gone before it reaches the backend in both cases,
confirming the measurement did not lose sensitivity after the OBI upgrade.
Traefik's own two hops on that same route (upstream of where the header gets
stripped) track the clean-route rate on each binary, as expected.

This closes the "wait for merge, then measure" action item. Do not infer the
remaining need for a correlator solely from a bug that upstream has now fixed
and that this session independently confirmed fixed on our own topology.

## 2. Measurement foundation: first implementation slice

Implemented in this slice:

- `load-gen` explicitly enables HTTP/2 attempts despite its custom TLS configuration.
  It requires HTTP/2 by default and records `protocol` and TLS `negotiated_protocol`.
  `-require-http2=false` permits a separately labelled HTTP/1.1 diagnostic: it stops
  flagging a non-HTTP/2 response as a failure, but does not downgrade the client.
  Against `fake-upstream`, which always offers `h2`, it still negotiates HTTP/2
  (measured 2026-10-04); an HTTP/1.1 run needs an HTTP/1.1-only server.
- Every logical request attempt produces a schema-v2 record, including request-build,
  transport, body-read, protocol and HTTP status failures. Requests have a bounded
  `-timeout` (default 30s). Redirects are not followed. Failed requests and output
  write/close errors make the command exit nonzero.
- A connection reused from a pool retains its identity; a distinct connection
  recycling the same tuple increments the client-side generation. This corrects
  the previous permanently cached tuple key. Agreement between independent client
  and server observers is still not guaranteed.
- `seed-policy=fixed` is rejected until deterministic workload replay exists.
  Earlier fixed/variable labels did not establish a variance decomposition.
- The legacy `join` refuses schema-v2 HTTP/2 or failed-request input. Its local
  counters are not HTTP/2 stream IDs: the server increments on handler entry,
  while the client increments on completion. It remains a historical diagnostic.
- `analyze_run.py` evaluates exact per-request identity from an evaluation-only
  URL label. It requires an explicit service, event type and printed destination,
  so two Traefik instances are not silently combined. It detects known-ID swaps,
  unknown IDs, missing requests, duplicate observations and unlabelled events.

The old client's custom TLS transport did not explicitly enable HTTP/2. Historical
claims about direct HTTP/2 multiplexing must therefore be remeasured with protocol
assertions; a server supporting HTTP/2 is not evidence that a client negotiated it.
Enabling HTTP/2 changes the stimulus, so new numbers must not be presented as a
controlled comparison against old runs with an unverified protocol.

### Running the new path-labelled evaluation

Build the binaries using the harness README. Use fresh output files and a separate
OBI capture per cell; wait for its export to drain before evaluating. For example,
with the existing TLS gateway listening on port 7080:

```bash
./bin/load-gen -target https://localhost:7080 -requests 300 -concurrency 50 \
  -truth-in-path -out /tmp/cell-load-gen.jsonl
python3 analyze_run.py /tmp/cell-obi.log /tmp/cell-load-gen.jsonl \
  --service traefik --event HTTP --destination traefik:7080
```

Run these commands from `m0-harness/`. The capture and gateway must already be
running. Select the exact destination actually printed by OBI; an unexpectedly
labelled hop requires investigation rather than a guessed mapping.

The label must stay outside candidate generation and ranking. This is explicitly
`path_labelled_lab_not_GT2`: it measures an individual logical request, not a
protocol network operation or retry. The headline denominator is every logged
attempt, including failures. Capture ambiguity yields `INVALID` and a null
accuracy; missing requests remain in the denominator. `VALID` only means this
analyzer's input checks passed, not that protocol Gate 0, stability or latency passed.
The parser currently targets OBI's existing text format and GET requests.

## 3. Remaining work, ordered by dependency

| Order | Deliverable | Completion evidence |
|---|---|---|
| 1 | Reproducible run command and manifest | Isolated processes/files per cell; exact binary hashes, configs, command lines, requested/logged counts and capture-drain status saved; bounded teardown |
| 2 | Deterministic workload replay | Fixed seed fixes per-request latency/body/profile assignments; variable seed changes them; scheduling variability remains measurable |
| 3 | Independent per-operation oracle | Real protocol stream identity or another independently validated operation key; detects swaps, retries, missing records, tuple reuse and capture loss |
| 4 | Reliable per-hop signals | Protocol observed at each boundary; structured event timestamps distinguished from export time; explicit service-instance identity |
| 5 | Fair correlator comparison | Same dataset, oracle and candidates for specialized and library rankers; measure recall, top-1, candidate sizes and unresolved cases separately; test elapsed-time windows only with valid timestamps |
| 6 | Full M0-A verdict | Freeze PI-2/PI-3 before confirmatory runs; evaluate Gate 0, attribution, latency and fixed/variable stability independently with required sample sizes |
| 7 | M0-B population demonstration | Controlled release/dependency/node/noise faults plus concurrent causes; compare estimated and injected blast radius; no LLM required |

OBI's post-merge comparison (§1 above) is done; it does not itself complete any
of steps 1–7 — those still stand on their own dependencies. M0-B does not
require solving per-request kernel attribution. No claim of a completed A/B/C
regime is made by this slice.

For step 5, the earlier roughly 90% specialized duration result and the library's
65–70% bucket result came from different conditions. They are not yet a controlled
algorithm comparison. Overlapping rank windows failed to improve the latter;
this does not settle elapsed-time windows or signal quality.

Also, `Match.score` is a deterministic ranking score, not calibrated probability.
`max_block_candidates` bounds M3 enumeration; it does not prevent M0–M2 from
matching a large block. A general ambiguity/review gate remains to be designed.

## 4. Publication boundary

Published instructions and conclusions live in `docs/` and the harness README.
`docs/target/` stays ignored and contains private working material. Historical
observations stay identifiable as historical; new measurements need their own
versioned method and artifacts. Raw run outputs are not committed by default.
