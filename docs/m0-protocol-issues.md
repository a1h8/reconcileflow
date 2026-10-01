# M0: protocol issues register (Run #001)

> Defects found while implementing [m0-evaluation-protocol.md](m0-evaluation-protocol.md)
> (`src/reconcileflow/m0/`).
> Rule: a defect is **fixed by a dated amendment to the protocol, before the run**, never by
> adjusting a threshold after seeing numbers.
> Categories: **GAP** (uncovered case) · **UNDERSPEC** (missing value or term) ·
> **INCONSISTENT** (two incompatible rules) · **AMBIGUOUS** (several readings).

| ID | Category | Severity | Status |
|----|----------|----------|--------|
| PI-1 | GAP | high | RESOLVED (2026-09-19) |
| PI-2 | INCONSISTENT | blocking | PROVISIONAL (option a) |
| PI-3 | UNDERSPEC | blocking | PROVISIONAL (0.90) |
| PI-4 | UNDERSPEC | low | RESOLVED (2026-09-21) |
| PI-5 | AMBIGUOUS | medium | RESOLVED (2026-09-21) |
| PI-6 | GAP | high | RESOLVED (2026-09-30) |
| PI-7 | GAP | medium | RESOLVED (2026-10-01) |
| PI-8 | GAP / UNDERSPEC | medium | RESOLVED, MIN_SIGNALS PROVISIONAL (2026-10-01) |

---

## PI-1: hole in the section 3.2 tree (RESOLVED)

**Symptom.** If `D >= 95%` but `D_acc < 99%` or `unresolved > 3%`, none of the five outcomes
applies. Example: `D_cov = 97.5%`, `D_acc = 98.5%`, so `D = 96%`.

**Cause.** The NO branch of the conjunctive test was labelled "F > 5% (necessarily)". But
`F = 1 - D` depends only on the first condition. A failure of `D_acc` or `unresolved` leaves
`F <= 5%`. A/A_WEAK require all three conditions; NEEDS/C require `F > 5%`; B requires `D < 95%`.

**Decision (2026-09-19).** That case is routed to `NEEDS_DIFFERENT_MECHANISM`, whose definition
becomes: `(F > 5% AND R < 80%) OR (D >= 95% AND (D_acc < 99% OR unresolved > 3%))`.
Implemented in `attribution._classify`, test `test_reliability_gap_...`.

## PI-2: the borderline band makes A unreachable (PROVISIONAL)

**Symptom.** With `D_acc >= 99%` and a +/-2 point band (section 3.3), the borderline zone is
`[97%, 101%]`. A `D_acc` of 100% is borderline: perturbing it to 98% changes the verdict. No run
can produce A or A_WITH_WEAK_RESIDUAL without being BORDERLINE. The same holds for any metric
whose threshold is within 2 points of 100%.

**Cause.** The band is absolute (+/-2 points) while the `D_acc` threshold leaves an error budget
of only 1 point. The two rules are incompatible.

**Options.**
- (a) Band **relative to the error budget** for metrics near 100%: +/-2 points on `D`, `R`, `E`;
  `D_acc` judged on its error rate with a +/-0.5 point band.
- (b) Absolute band everywhere, **capped at 100%**: does not solve the case above (the drop to
  98% remains possible).
- (c) Lower the `D_acc` threshold to 97%: changes the business requirement, so a product decision.

**Provisional decision (2026-09-20).** Option (a): `Thresholds.d_acc_band = 0.5 pt`, +/-2 points
elsewhere. To be frozen in the protocol before the run.

## PI-3: "acceptable Q" has no value (PROVISIONAL)

**Symptom.** A vs A_WITH_WEAK_RESIDUAL depends on "acceptable Q" (section 3.2), never quantified.

**Provisional decision (2026-09-20).** `q_acceptable_min = 0.90`, aligned with the `E >= 90%`
requirement of category B. Carried by `attribution.RUN_001`; the parameter remains mandatory in
`Thresholds` (no hidden default). To be frozen in the protocol before the run.

## PI-4: "non-catastrophic ranking" (category B) (RESOLVED)

**Symptom.** B cites a condition with no value. `R >= 80%` and `E >= 90%` already bound it in
practice.

**Decision (2026-09-21).** The clause is removed from the protocol. The code never used it.

## PI-5: "SD <= 1.5 pt (95% CI)" (RESOLVED)

**Symptom.** Two readings: (i) the between-run standard deviation is at most 1.5 points;
(ii) the 95% CI of the mean is narrower than 1.5 points. They are not equivalent (the CI narrows
with n). The code applies (i), on the sample standard deviation, for each decision metric (max).

**Decision (2026-09-21).** Reading (i). A CI narrows as repetitions are added, so under (ii) a
system could be made to pass by running more repetitions, which reopens post-hoc adjustment.
The standard deviation measures repeatability independently of n.

## PI-6: `r`/`t` have no way to express "not measured" (RESOLVED)

**Symptom.** `Metrics.r`/`Metrics.t` (P2's residual-only metrics) have no way to express "not
measured." Two consequences, both reachable with real numbers from the 2026-09-30 OBI PR #3587
before/after measurement (`m0-demo-readiness.md` §1): (a) a perfect P1 result (`F = 1 - D = 0`,
the first time this project has measured `D` at exactly 100%) has its `A` vs
`A_WITH_WEAK_RESIDUAL` verdict decided by an arbitrary `r`/`t` placeholder instead of the
measurement, since there is no residual for either value to describe; (b) a P1-only run (the
correlator never executed) can silently manufacture `B_HYBRID` — crediting a correlator
contribution with zero supporting evidence.

**Cause.** The protocol's own draft (`m0-evaluation-run-001.md`, `docs/target/`) defines
`A_WITH_WEAK_RESIDUAL` with a redundant `F <= 5%` conjunct that gestures at "a small residual
exists" without ever specifying the `F = 0` boundary. Section 3.3's `BORDERLINE` definition lists
`R`/`T` as unconditionally perturbable inputs, which is equally undefined once they are not
measured.

**Decision (2026-09-30).** `r`/`t` become optional (`Decimal | None`). `F == 0` short-circuits to
`A` unconditionally — there is no residual, so the §3.3 veto has nothing to act on and cannot
fire. `F > 0` with `r`/`t` unset and needed by the branch (the `q` computation, or the `r < 80%`
check) returns a new `CORRELATOR_NOT_MEASURED` outcome rather than guessing; the
`NEEDS_DIFFERENT_MECHANISM` routes that depend only on `D`/`D_acc`/`unresolved` (PI-1) are
unaffected. `Decision` gains `borderline_checked: bool`, false whenever the full
`D`/`D_acc`/`unresolved`/`R`/`T` perturbation set (§3.3) could not be executed for lack of a
correlator measurement — silently reporting "not BORDERLINE" would otherwise claim a robustness
check that never ran. Implemented in `attribution.py`; tests in `tests/test_m0.py`. Full working
notes: `docs/target/m0-signal-confidence-gap.md`.

## PI-7: missing `T <= R` invariant (RESOLVED)

**Symptom.** `R` and `T` are definitionally related (`T <= R` always: a correct top1 pick is, by
construction, inside the candidate set counted by `R`), but nothing enforced it. `r=0.85, t=0.95`
(`Q = T/R = 1.118`, itself impossible) passed `Q >= 0.90` and returned a clean, confident `A` —
not even `BORDERLINE` — on a measurement that could only originate from a bug (`R`/`T` computed
with diverging denominators, the same class of defect `m0-harness/README.md`'s own "Lesson"
section already documents once, for a different pair of counters).

**Cause.** `Metrics.__post_init__` validated `r`/`t` independently as fractions in `[0, 1]` (or
`None`, per PI-6) but never validated the relationship between them. The protocol text
(`m0-evaluation-protocol.md` §2) defines `R` and `T` separately and never states the `T <= R`
invariant.

**Decision (2026-10-01).** `Metrics.__post_init__` rejects `t > r` when both are measured
(`ValueError`), the same way out-of-range values already are. A pure input-validation addition:
no new `Attribution` outcome, no change to `_classify`/`decide`/`Decision`. `t == r` remains
legal; `r` or `t == None` (PI-6) bypasses the check entirely. Implemented in `attribution.py`;
tests in `tests/test_m0.py`. Full working notes: `docs/target/m0-pi7-t-leq-r-invariant.md`.

## PI-8: latency gate crashes below a minimum signal count (RESOLVED, `MIN_SIGNALS` PROVISIONAL)

**Symptom.** `gate([])` raises `IndexError` (`percentile()` indexes an empty sorted list with no
check) — reachable trivially via the public API (`gate(signal_availability([], {}))`). Neither
`m0-evaluation-protocol.md` §4 nor `tests/test_m0.py` addressed the zero-signal case. The crash is
the sharp edge of a wider problem: nearest-rank percentiles are statistically hollow at any small
`n`, not just `n=0`.

**Cause.** `gate()` never checked its input had enough signals to make `p95`/`p99` meaningful,
let alone nonempty.

**Decision (2026-10-01).** A provisional `MIN_SIGNALS = 100` (same status as PI-3's
`q_acceptable_min` — to be frozen before a real run). `gate()` raises `ValueError` below it,
naming the count and threshold. No new `LatencyVerdict` member: `PASS`/`FAIL` stays binary, and
the module already has a "raise rather than guess" precedent (`signal_availability`'s per-node
skew check). Implemented in `latency.py`; tests in `tests/test_m0.py`. Full working notes:
`docs/target/m0-pi8-latency-min-signals.md`.
