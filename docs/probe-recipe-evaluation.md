# Recipe on the probe's events: evaluation verdict

Status: 2026-11-15. Verdict of the two dedicated evaluation days `evalr-M1` and
`evalr-M2`, judged under a protocol written before any evaluation data. The run
data stays on the evaluation machine (`runs/`, not in the repository).

## What was judged

Inside one relay, the stream probe sees the relay's inbound requests and
outbound calls. The recipe links each inbound request to the outbound call it
caused, using only what the probe sees:

- a **declared** recipe ("each request makes one call upstream"), which an
  operator knows about their chain; no ground truth is used to link;
- a **size signature** learnt without ground truth from the relay's own
  traffic: the response sizes a relay forwards differ from what it received
  only by a few fixed steps (here 22 bytes, one TLS 1.3 record). When no
  signature is learnt, a plain size window is used instead;
- probe losses, when there are any, are **located** and contained to their
  neighbourhood.

Ground truth (load-gen's ranks, fake-upstream's records) is used only to judge.

Code: commit `bea4bc6` on `correlation-phase4`, identical on both days; the
judging tool `m0-harness/probe_recipe.py` (sha256 `c6edde90…`) is the one from
that commit. Judged with `probe_recipe.py --series <runs-list> --lattice`.

## Cells

30 cells per day, two days (60 cells): no agent, Beyla, OBI × concurrency 20,
100, 200, 400, 800 × CPU load 0 and 4 extra busy cores. 30,000 requests per
cell through one Traefik hop over TLS 1.3 and HTTP/1.1; the second half of each
cell is judged. The agent binaries were checked identical on both days, and the
series script refused to start beside IDEs or containers.

## Criteria (per valid cell) and controls

1. **0 wrong links** (hard);
2. **0 causality violations** (a call outside its request's lifetime);
3. **coverage ≥ 99.0 %** on every cell without declared losses, agent cells
   included.

Controls on every cell, all required to show **0 wrong links**: K2b (50 ‰ of
probe events removed and declared lost, seeds 1-3), K2c (1 ‰, seeds 1-3), K6
(size window shifted by 1,000 bytes), K7 (signature shifted by half a step).
K5 is the judge's negative control: with the truth shifted by one rank, the
judge must see ≥ 99 % wrong links, or no verdict is given.

## Result: held

**60 cells valid, 60 `ok`; verdict "held" on each day and on both days.**

- 0 wrong links and 0 causality violations on all 60 cells; no declared probe
  loss; ranks matched everywhere.
- Lowest coverage: 99.89 % (OBI, c800, load 4, day 2), 99.93 % (Beyla, c800,
  load 4, day 2), 99.95 % (Beyla, c800, load 4, day 1). Every other cell:
  99.97-100 %.
- K2b, K2c, K6, K7: no wrong link. K5: the judge sees the injected errors.
- Refused requests: 61 in total, all `AMBIGUOUS` (two candidates left), none
  `SIZE_MISMATCH`; 51 of them in the c800 cells with an agent.
- Signatures learnt: step 22 bytes and upper bound 88 everywhere; two cells
  (no agent, day 2) learnt none and fell back on the size window.

For comparison, the method without the size signature was judged "not held"
on an earlier evaluation day: 96.9-97.7 % coverage on the c800, load 4 cells
with an agent.

## Scope

This holds for this bench only: random bodies forwarded unchanged, Go
services, Traefik, TLS 1.3, one hop, HTTP/1.1. It says nothing about a relay
that compresses or rewrites bodies. A bench with realistic response sizes
comes next, under its own protocol.
