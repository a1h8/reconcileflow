# M0 demonstration: readiness and next steps

Status: 2026-09-29. This is the execution order for a defensible lab demonstration.
The matching library, the M0 evaluation harness, and the target operational platform
have different completion criteria. Finishing this harness does not implement the
platform or establish PCI-DSS compliance.

## 1. OBI dependency: wait for the merge, then measure

[OBI PR #3587](https://github.com/open-telemetry/opentelemetry-ebpf-instrumentation/pull/3587)
explicitly targets issue #3571. At review on 2026-09-29 it was open, unmerged,
and approved by two reviewers. The reviewed head was
`2bbaca32fd647dcef8b9ce7f0c50b18e8c3c2aa2`.

The patch replaces connection-scoped HTTP/2 traceparent handoff with per-stream
response-writer identity. Its integration test checks concurrent streams on one
connection, exact parent attribution, and streams without incoming trace context.
This provides a concrete upstream explanation consistent with our observations;
it does not replace a measurement on our topology.

The correction retracting the permanent-cliff claim was already
[published on #3571](https://github.com/open-telemetry/opentelemetry-ebpf-instrumentation/issues/3571#issuecomment-5843336129).
Older notes saying it remains to be sent are historical.

After merge, pin the actual merged commit/build and compare the old and corrected
OBI versions using the same recorded workload. Compare each hop separately and
include a deliberately stripped-header route. Do not infer the remaining need for
a correlator solely from a bug that upstream is fixing.

## 2. Measurement foundation: first implementation slice

Implemented in this slice:

- `load-gen` explicitly enables HTTP/2 attempts despite its custom TLS configuration.
  It requires HTTP/2 by default and records `protocol` and TLS `negotiated_protocol`.
  `-require-http2=false` permits a separately labelled HTTP/1.1 diagnostic.
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

OBI's post-merge comparison joins this sequence once the new binary is available;
steps 1–4 can progress before it. M0-B does not require solving per-request kernel
attribution. No claim of a completed A/B/C regime is made by this slice.

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
