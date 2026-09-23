# M0: Evaluation Run #001 (pre-registered decision protocol)

> English edition, including the amendments listed in [m0-protocol-issues.md](m0-protocol-issues.md)
> (marked **[amended]**). The rules are implemented in `src/reconcileflow/m0/`.

> First run of the **Correlation Evaluation Harness**. This is not an exploratory spike: it is a
> **pre-registered protocol**. Decision rules (thresholds, verdicts, null conditions) are fixed
> **before** a single measurement is taken. No threshold is chosen or adjusted after seeing the
> numbers.

> **Thesis.** The numbers do not decide. The pre-registered decision rules turn numbers into a
> verdict. Fixing the criterion after seeing the result is a post-hoc threshold, and it voids
> the rigor of the ground truth.

---

## 0. Blocking status

```yaml
target_stack:
  protocol_flavor:       generic HTTP/2   # RESOLVED 2026-09-23 — see justification below
  runtime:               Go
  client_library:        net/http (stdlib, native HTTP/2)
  obi_propagation_mode:  headers          # W3C traceparent

run_status: TARGET_STACK_RESOLVED — first vertical-slice results below (§0.2)
```

**Justification (2026-09-23), fixed before any measurement — not adjusted after the fact:**
- **Go**: consistent with the stream-processing choice for the operational-state component (Goka) and with the platform's own reasoning on language choice, which explicitly endorses Go for **a standalone service** — exactly the role of an `outbound-connector`.
- **Generic HTTP/2, not gRPC**: real external counterparty APIs are almost always REST/HTTPS, never gRPC exposed publicly — gRPC is an internal architecture choice. Consistent with the H2+TLS already modeled elsewhere in this evaluation plane, not a new assumption.
- **`net/http`**: follows mechanically from Go + HTTP/2, no third-party library to justify.
- **headers**: standard W3C `traceparent` propagation — the only option that needs no extra application instrumentation for OBI to see it.

The verdict is a property of the **pair OBI x application stack** and does not transfer. So:

- **No convenience stack "to make progress".** A run on Go/gRPC says nothing about a production
  connector in Java/generic-HTTP2.
- The four fields are recorded as **dataset dimensions**, not metadata: `direct_trace_coverage`
  co-varies with them.
- `target_stack` is resolved (above, 2026-09-23) on the declared real stack of the target
  `outbound-connector` — the run can proceed per the harness build order in §0.1.

### 0.1 First real result (2026-09-23) — fallback tier only

A minimal harness exists implementing this stack: a controlled-latency HTTP/2 server, a load
generator that originates `traceparent` (authoritative by construction), and an offline join
reporting `D_acc` — matched / **expected**, where a join miss counts as a failure rather than
being excluded from the denominator.

| Cell | Scale | D_acc | Join misses |
|---|---|---|---|
| Normal (pooling on) | n=500, c=100 | 1.0000 | 0 |
| Positive control (no pooling, isolated) | n=500, c=100 | 1.0000 | 0 |
| Negative control (shuffled trace_id) | n=500, c=100 | 0.0000 | 0 |
| **Normal, sustained load** | **n=10000, c=200** | **0.9631** | **369** |

**The scale result is the one that matters, not the small-n ones.** All 369 misses share one
signature: the client-side and server-side processes disagree on the reuse generation of the same
underlying 5-tuple. Both count 5-tuple recurrences correctly; under sustained concurrency the OS
recycles ephemeral ports fast enough that two independent processes can observe the recycling in
a different relative order — there is no atomicity between them. This is the fallback-tier oracle
(`5tuple, connection_start, generation`, §1 below) failing on its own terms at moderate scale, not
a harness defect to silence.

**What this does and does not show.** This measures the fallback tier only. The strong tier
(`boot_id, network_namespace, socket_cookie`) is kernel-assigned and does not suffer this
cross-process ordering skew. Read 96.3% here as concrete evidence *for* building that strong
tier before trusting the fallback one past a few hundred concurrent connections, not as grounds
to declare regime A or C — that call still waits on the three-way join below.

### 0.2 Strong tier — first real measurement (2026-09-23)

A single privileged observer (kprobe on `tcp_connect`, kretprobe on `inet_csk_accept`) was
built and loaded successfully after fixing a program-type/helper mismatch caught by the real
kernel verifier. Same scale as the fallback-tier degradation above:

| Scale | Connect events | Paired to an accept |
|---|---|---|
| n=1000, c=100 | 354 | 354/354 (100%) |
| **n=10000, c=200** | **3927** | **3927/3927 (100%)**, incl. 328 same-port collisions resolved by time ordering |

**The core hypothesis is confirmed empirically.** At the exact scale where the fallback tier
lost 369/10000, a single observer with one clock — no cross-process race — correctly pairs
100%, including the 328 cases where a client port was reused mid-run, resolved because one
observer's own timestamps put connect-before-accept in a provable order.

**Not yet shown**: this is kernel-level connect/accept pairing only, not yet cross-checked
against `load-gen`'s actual `trace_id` records — the three-way join that would confirm full
end-to-end attribution is the next step, not done here.

---

## 1. Run configuration (one discriminating cell)

One discriminating cell **before** the full matrix.

```yaml
load:
  concurrency: 100          # high concurrency stresses connection pooling
  seed_policy: [fixed, variable]   # see section 3, variance decomposition

latency:
  fake_upstream_profile: C      # heavy tail: p50 40 · p95 300 · p99 850 · max several seconds

protocol:
  from: target_stack        # the real stack, not an assumed generic H2

oracle:
  primary: GT2              # (connection_instance, stream_id) -> expected_trace_id
  connection_instance:
    strong:   [boot_id, network_namespace, socket_cookie]
    fallback: [5tuple, connection_start, generation]
```

The verdict of this cell steers what follows: clearly A, the full matrix will confirm; clearly
C/NEEDS, deploy the matrix to map the problem.

---

## 2. Metrics

**Two predictors, judged by the same independent oracle:**

```
P1 - OBI direct
   direct_trace_coverage      D_cov  = fraction where OBI supplies a trace_id
   direct_trace_accuracy      D_acc  = fraction of THOSE that are correct (deterministic zone)
   direct_correct_coverage    D      = D_cov x D_acc

P2 - Correlator (on the RESIDUAL, not the whole dataset)
   residual_fraction          F      = 1 - D
   residual_candidate_recall  R      = P(true_trace in candidate_set | residual)
   residual_top1_accuracy     T      = P(top1 correct | residual, deterministic zone)
   ranking_efficiency         Q      = T / R   [DEFINED ONLY IF R >= 80%, see 3.1]
   residual_MRR
   candidate_size_p50/p95/p99 (diagnostic)

AGGREGATE
   end_to_end_correct         E      = D + F x T

COVERAGE
   truth_coverage             = fraction GT1 + GT2 + GT3
   unresolved_truth_zone      = fraction GT-UNKNOWN  (= 1 - truth_coverage)
      - no_protocol_identity     (information limit)
      - oracle_capture_failure   (probe defect: must tend to 0)
```

---

## 3. PRE-REGISTERED decision rules

### Gate 0: experimental validity (before ANY look at A/B/C)

```
oracle_capture_failure > 1%
   -> RUN = NULL          (the probe failed, not the system. Repair the harness, rerun.
                           No other metric is interpretable.)

oracle_capture_failure <= 1%  BUT  truth_coverage < 95%
   -> RUN = VALID_BUT_INCONCLUSIVE
                          (valid measurements but unrepresentative traffic: no A/B/C verdict.
                           Distinct from a null run.)

oracle_capture_failure <= 1%  AND  truth_coverage >= 95%
   -> an attribution verdict may be pronounced.
```

### 3.1 Guard on `ranking_efficiency` (division trap)

`Q = T/R` is unstable and misleading for small R (`R=0.05, T=0.04 -> Q=0.80` looks like a good
ranker on a population that means nothing). Therefore:

```
Q is DEFINED and interpreted only for R >= 80%.
Below that, Q is not computed and the decision rests on R alone (NEEDS branch).
```

**[amended, PI-3]** "Q acceptable" means `Q >= 0.90` (provisional, to be frozen before the run).

### 3.2 The five outcomes: a mutually exclusive, exhaustive partition

```
                  D >= 95%  AND  D_acc >= 99%  AND  unresolved <= 3% ?
                    /                                        \
                 YES                                          NO
                  |                                            |
        residual quality good?                        D >= 95% ? (D_acc or
        (R >= 80% AND Q acceptable)                    unresolved failed)
          /            \                                 /          \
       YES              NO                            YES            NO   (F > 5%)
        |                |                             |              |
        v                v                             v          R < 80% ?
        A       A_WITH_WEAK_RESIDUAL          NEEDS_DIFFERENT      /        \
                (weak tail <= 5%,               _MECHANISM      YES          NO
                 made visible)                  [amended, PI-1]  |            |
                                                                 v        E >= 90% ?
                                                       NEEDS_DIFFERENT     /      \
                                                          _MECHANISM     YES       NO
                                                                          |         |
                                                                          v         v
                                                                          B         C
                                                                             SCORING_REQUIRED
```

**Positive definitions (no "otherwise" bucket):**

```
A
   D >= 95%  AND  D_acc >= 99%  AND  unresolved <= 3%  AND  (R >= 80% AND Q acceptable)
   -> OBI carries the attribution; the correlator is a marginal fallback.

A_WITH_WEAK_RESIDUAL
   D >= 95%  AND  D_acc >= 99%  AND  unresolved <= 3%  AND  weak residual quality  AND  F <= 5%
   -> OBI is enough; the fallback is weak BUT only covers <= 5% of traffic.
      The risk is made VISIBLE without over-building.

NEEDS_DIFFERENT_MECHANISM   [amended, PI-1]
   (F > 5%  AND  R < 80%)
   OR
   (D >= 95%  AND  (D_acc < 99%  OR  unresolved > 3%))
   -> The true candidate is MISSING too often (or OBI's direct IDs are not reliable enough).
      Better ranking will not save this. Look at: better context propagation, a
      different or reconfigured OBI, additional keys, instrumentation, another attribution
      mechanism.
      WARNING: do NOT fund a better ranker: the problem is candidate generation.

C: SCORING_REQUIRED
   F > 5%  AND  R >= 80%  AND  E < 90%  (insufficient ranking efficiency)
   -> The candidate is generally there but ranked badly. Investing in scoring has a real
      chance of paying off: temporal scoring, duration, connection metadata, topology, span
      semantics, priors.

B: HYBRID
   D < 95%  AND  E >= 90%  AND  R >= 80%
   -> OBI alone is insufficient, but OBI + correlator already give a correct global
      attribution. Co-necessary. A POSITIVE category, not a dumping ground.
   [amended, PI-4] The source clause "non-catastrophic ranking" is removed: `R >= 80%` and
   `E >= 90%` already bound it.
```

Resolution of the `E < 90%` case (no sixth hole):

```
E < 90%
  - R < 80%   -> NEEDS_DIFFERENT_MECHANISM
  - R >= 80%  -> C / SCORING_REQUIRED
```

`ranking_efficiency`: a strong annotation in B, an explicit justification for C.

### 3.3 Borderline zone (against post-hoc tinkering)

```
a decision input within the band of a threshold  ->  VERDICT = BORDERLINE
   -> run the complementary matrix BEFORE deciding.
   Forbidden: "94.9% is almost 95%, call it A."
```

**[amended, PI-2]** The band is +/-2 points, except for `D_acc` where it is +/-0.5 point. A
+/-2 point band on a 99% threshold would make even a perfect `D_acc` borderline (band 97%-101%),
so A could never be reached. Provisional, to be frozen before the run.

BORDERLINE means: moving any single input (`D`, `D_acc`, `unresolved`, `R`, `T`) by its band
could change the verdict. An input that cannot change the outcome does not trigger it.

Cross-dimension conflicts: the primary decision is **weighted by traffic fraction** (`E` already
aggregates). The **veto** looks explicitly at the residual (`R`, `Q`) so a bad tail is not hidden
behind an average. Poor performance on <= 5% of traffic gives `A_WITH_WEAK_RESIDUAL`, never an
automatic C.

### Stability gate: repeatability (a verdict only if stable)

```
minimum repetitions                = 5 (independent)
minimum deterministic GT ops / rep = 10,000
criterion: between-run SD <= 1.5 points on the decision metrics

unstable after 5    -> extend to 10
unstable after 10   -> VERDICT = UNSTABLE   (neither A, B nor C: instability IS a result)
```

**Variance decomposition (mandatory):**

```
SD at FIXED seed (the exact same request sequence)  -> intrinsic noise of the SUT
SD added at VARIABLE seed                           -> sensitivity to the load profile

SD_fixed > 1.5                       -> genuinely unstable system -> UNSTABLE justified
SD_fixed small, SD_variable large    -> stable BUT pattern-sensitive
                                        (a different architectural conclusion, NOT unstable)
```

**[amended, PI-5]** The criterion is the sample standard deviation of each decision metric across
repetitions; the worst metric decides. It is not a confidence-interval width: a CI narrows as
repetitions are added, so it could be satisfied by running more of them.

---

## 4. Latencies: three distinct latencies, skew-corrected

**Never confuse with an end-to-end application SLO** (client to business response). That is a
*different* SLO and is not reused here.

```
L1 - Signal availability
   = correlator_ingest_time - source_event_time
   WARNING: crosses two clocks (kernel/node vs correlator) -> SKEW CORRECTION mandatory
     (reuse the per-node skew measured in the foundation document) before any computation.
   by: source, node, load, stack   -> p50/p95/p99

L2 - Correlation latency
   = result_published_time - last_required_input_available_time
   measures the ALGORITHM, not the pipes.

L3 - Detection latency (M0-B)
   = incident_detection_time - fault_effective_time
   the latency the SRE actually feels.
```

**Cross-clock rule (pre-registered):** any latency crossing two clocks is first corrected for
per-node skew, and only THEN judged PASS/FAIL. An uncorrected L1 mixes real transport with clock
desync.

### Latency gate: SEPARATE from the attribution verdict

```
signal_availability_p95 <= 1 s        (pre-registered, skew-corrected)
signal_availability_p99 <= 2 s

-> LATENCY_VERDICT = PASS | FAIL, independent of A/B/C.

Example: REGIME = A  AND  LATENCY_VERDICT = FAIL
   (perfect attribution, but too slow to serve during an incident).
Never "A = all good".
```

`detection_latency` (L3): threshold pre-registered **separately**, once the operational delay of
the SRE platform is fixed. **Do not reuse an application SLO.**

---

## 5. M0-B: population-level blast radius (Tetragon), separate

Aggregation by **`window · service · release · destination · node`**, never by `exchange_id`.

**Baseline (relative AND absolute both mandatory):**
```
healthy 0.08%  ->  incident 0.64%   =  +700% relative / +0.56 pt absolute
```
(without the absolute figure, `0.001% -> 0.008%` is also +700% and may be insignificant.)

**Scenarios:**
```
S1  release regression     v4.27 only
S2  external dependency failure  v4.26 AND v4.27
S3  node failure           all pods of N7
S4  isolated noise         1 pod, a few resets
S5  S1 + S2 CONCURRENT     regression DURING an external-dependency incident   <- causal confusion
```

**Expected signatures (testing hypotheses H1/H2/H3):**
```
                    release   destination   node
H1 regression         ++          -           -
H2 dependency             -          ++           -
H3 infrastructure     -           -          ++
H4 noise              -           -           -
```

S5 is **mandatory**: it prevents implicitly building a single-label classifier. The goal is not
to file into a box but to produce competing hypotheses with a contribution per hypothesis.

**Metrics:** `detection_delay · FP_rate · TP_rate · affected_pods/total · affected_nodes/total ·
error_rate_by_release · error_rate_by_destination · baseline · incident · absolute_delta ·
relative_delta`, and **`blast_radius_estimate` vs the INJECTED blast radius** (breaking 3/12 and
estimating 11/12 means the engine cannot measure).

**Output = factual JSON, NO LLM:**
```json
{
  "signal": "tcp_reset_rate", "service": "outbound-connector", "release": "v4.27",
  "destination": "dependency.external", "baseline": 0.0008, "observed": 0.0064,
  "affected_pods": 3, "total_pods": 12, "detection_delay_ms": 840
}
```
First question: *do the data distinguish the hypotheses?* The AI narrates afterwards.

---

## 6. Overall logic

```
                 PRE-REGISTERED RULES  (this document, fixed BEFORE the run)
                         |
                         v
                    RUN EXPERIMENT  (target_stack resolved on the real stack)
                         |
             +-----------+---------------+
             v           v               v
        GATE 0        ATTRIBUTION     LATENCY
        VALIDITY      5 outcomes      L1/L2/L3 corrected
        NULL /        A - A_WEAK -    PASS / FAIL
        INCONCLUSIVE  NEEDS - B - C   (separate from A/B/C)
             |           |
             |           v
             |       STABILITY  (5 -> 10; fixed/variable seed variance)
             |           |      UNSTABLE if not reached
             v           v
                   FINAL VERDICT
              { attribution, latency, stability }
                         |
                         v
            correlation-engine  (scoring chapter sized BY the verdict)
```

---

## 7. Expected results: two tables

**Attribution (M0-A)**
```
Stack(flavor/runtime/client/OBI) | Conc | Profile | seed |
   D_cov | D_acc | D | F | R | T | Q | E | truth_coverage | unresolved(no_id/capture_fail) |
   attribution_verdict | latency_verdict | stability
```

**Population-level detection (M0-B)**
```
Scenario | detection_delay | actual_blast | estimated_blast | FP | FN
S1 - S2 - S3 - S4 - S5
```

---

## 8. Locked invariants

1. **Target stack is a blocking hole**; no convenience stack; the verdict is a property of the
   OBI x stack pair.
2. **Pre-registered decision rules**; no threshold adjusted after the numbers; borderline zone
   triggers the complementary matrix.
3. **Gate 0 validity first**: `oracle_capture_failure > 1% -> NULL`;
   `truth_coverage < 95% -> INCONCLUSIVE` (not null).
4. **Five outcomes, exhaustive partition**: A, A_WITH_WEAK_RESIDUAL, NEEDS_DIFFERENT_MECHANISM,
   C (SCORING_REQUIRED), B (HYBRID). No "otherwise" category.
5. **`ranking_efficiency Q = T/R` defined only for R >= 80%**; otherwise decide on R alone.
6. **NEEDS_DIFFERENT_MECHANISM** separates "candidate absent" from "badly ranked", so a ranker is
   not funded when the problem is generation.
7. **Decision weighted by traffic fraction + explicit residual veto**; a weak tail of <= 5% gives
   A_WITH_WEAK_RESIDUAL, never an automatic C.
8. **Stability is a verdict**: 5 -> 10 reps, SD <= 1.5 pt; unstable is UNSTABLE. Variance
   decomposed fixed-seed (SUT) / variable-seed (load).
9. **Three distinct latencies**, corrected for inter-clock skew BEFORE PASS/FAIL; **latency gate
   separate** from the attribution verdict; never reuse an application SLO.
10. **M0-B is population-level**: aggregation by dimensions, never `exchange_id`; relative and
    absolute baseline; S5 multi-cause mandatory; estimate vs injected; no LLM.

---

## 9. Boundary

M0 produces the verdict `{attribution in 5 outcomes, latency, stability}`. This verdict, and only
it, sizes the scoring chapter of the correlation engine design. No scoring decision is taken in
this document: it is delegated to the numbers, through the rules above.

Order: **resolve `target_stack` on the real stack** -> run #001 -> Gate 0 -> attribution +
latency -> stability -> final verdict -> correlation engine skeleton, stage 1 filled according
to the regime.
