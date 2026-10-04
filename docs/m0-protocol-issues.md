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
| PI-9 | GAP | high | RESOLVED (2026-10-02) |
| PI-10 | GAP | medium | RESOLVED (2026-10-02) |
| PI-11 | GAP | medium | RESOLVED (2026-10-02) |
| PI-12 | GAP | high | RESOLVED (2026-10-02) |
| PI-13 | GAP | medium | RESOLVED (2026-10-02) |
| PI-14 | GAP | high | RESOLVED (2026-10-02) |
| PI-15 | GAP | high | RESOLVED (2026-10-02) |
| PI-16 | GAP | high | RESOLVED, thresholds PROVISIONAL (2026-10-03) |
| PI-17 | GAP | medium | RESOLVED (2026-10-04) |

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

## PI-9: stability gate silently ignores metrics missing from `reps[0]` (RESOLVED)

**Symptom.** `_worst_sd` iterates only `reps[0].metrics`. A metric absent from `reps[0]` but
wildly unstable in later reps is never checked — `assess()` returns a confident `STABLE` verdict,
not a crash, the most severe failure mode found in this series (PI-6/7/8 all surface as a crash
or an explicit not-a-verdict outcome; this one lies silently). Separately, an extra key in
`reps[0]` crashes with an opaque `KeyError`, and all-empty metrics crashes with an opaque
`ValueError` from `max()` on an empty generator.

**Cause.** No check that every repetition in a group — or across the fixed/variable groups the
decomposition compares — tracks an identical, nonempty set of metric names. `reps[0]` was
treated as ground truth for "which metrics exist" with nothing to catch disagreement.

**Decision (2026-10-02).** New `_metric_names(reps)` helper: nonempty, and identical across
every repetition in the group, else `ValueError` naming the mismatch. `_worst_sd` uses it instead
of `reps[0].metrics` directly. `assess()` additionally cross-checks fixed-seed and variable-seed
track the same names, before any SD is computed. Implemented in `stability.py`; tests in
`tests/test_m0.py`. Full working notes: `docs/target/m0-pi9-stability-metric-consistency.md`.

## PI-10: the `F == 0` short-circuit fires on a mathematically impossible perturbed point (RESOLVED)

**Symptom.** `decide()` on `d_cov=1, d_acc=0.98` (a confident `NEEDS_DIFFERENT_MECHANISM`,
nowhere near a real threshold) reports `BORDERLINE`. Perturbing the derived `d` axis alone by its
registered band lands on `d=1.00` while `d_acc` stays at its real `0.98` in that same point —
`(d=1.00, d_acc=0.98)` is mathematically impossible for any real `Metrics` instance (implies
`d_cov=1.0204`), but PI-6's `if d == 1` short-circuit does not check `d_acc` and treats it as a
genuine zero-residual state anyway.

**Cause.** PI-6's short-circuit on `d == 1` is correct for every real measurement (a product of
two `<= 1` factors reaches `1` only if both do), but was never checked against the pre-existing
perturbation loop, which treats `D` and `D_acc` as independently movable and can synthesize this
impossible combination.

**Decision (2026-10-02).** Short-circuit requires `d == 1 AND d_acc == 1`, not `d` alone. A no-op
for every real measurement; only refuses to fire on the synthetic, impossible perturbed point.
Implemented in `attribution.py`; test in `tests/test_m0.py` reproducing this exact case. Full
working notes: `docs/target/m0-pi10-borderline-impossible-point.md`.

## PI-11: `gate()` accepts physically impossible negative latencies (RESOLVED)

**Symptom.** `gate([-0.5] * 100)` returns `PASS`. `L1` (`signal_availability`) is causally
impossible to be negative once skew is correctly applied — a badly broken skew correction (wrong
sign, stale value, wrong node) reads as an excellent, fast `PASS` instead of a detected fault.
Nothing checked this.

**Cause.** `gate()` never validated that its input is a physically valid set of corrected
latencies, only that there are enough of them (PI-8).

**Decision (2026-10-02).** Any negative value in `l1_corrected` is rejected (`ValueError`), no
tolerance band: skew is a fixed, pre-measured correction (not a live noisy estimate), so a
negative result after applying it means the skew is wrong, not that the true latency was merely
near zero. A tolerance band would be exactly PI-3/PI-8's shape of problem — a new provisional
business threshold nobody asked for — so none is introduced. Checked at the same `gate()`
chokepoint as PI-8's `MIN_SIGNALS`. No new `LatencyVerdict` member. Implemented in `latency.py`;
tests in `tests/test_m0.py`. Full working notes: `docs/target/m0-pi11-negative-latency.md`.

## PI-12: `Thresholds` has zero validation, unlike `Metrics` (RESOLVED)

**Symptom.** `Metrics.__post_init__` rejects out-of-range fractions, `None`-ambiguity (PI-6), and
the `T <= R` invariant (PI-7). `Thresholds` is a plain `@dataclass` with no `__post_init__`
whatsoever. `Thresholds(q_acceptable_min=Decimal("-1"))` turns a catastrophic `Q=1.2%` ranking
into a clean `A` instead of `A_WITH_WEAK_RESIDUAL`. `Thresholds(d_min=Decimal("-1"))` makes
`D >= 95%` vacuously true regardless of the real `D`, making `B`/`C`/`NEEDS`-via-`R` unreachable.
`Thresholds` is exactly the kind of "pre-registered, frozen before the run" object this protocol
is built around — a transcription mistake in it is at least as likely and consequential as a bad
measurement, and had zero protection.

**Cause.** `Metrics` validates its own fields; nobody applied the same scrutiny to `Thresholds`.

**Decision (2026-10-02).** `Thresholds.__post_init__` validates the eight fraction fields as
`[0, 1]` (same check `Metrics` already applies) and the two bands (`borderline_band`,
`d_acc_band`) as strictly positive, `(0, 1]`. `RUN_001`'s actual values are unaffected.
Implemented in `attribution.py`; tests in `tests/test_m0.py`. Full working notes:
`docs/target/m0-pi12-thresholds-unvalidated.md`.

## PI-13: `Repetition` has no validation, same shape as PI-12 (RESOLVED)

**Symptom.** `Repetition({"d": Decimal("150")}, 10_000)` ×5 reports `STABLE` — documented as
"fractions," never checked. Because all five repetitions agree on the same wrong value, `SD = 0`
exactly, which is `<= MAX_SD` by construction: systematic corruption is invisible to a check
whose whole purpose is measuring consistency, precisely because it *is* consistent. A genuinely
inconsistent bad value is already caught today (verified) as a large SD — only the agreeing one
slips through. Separately, `deterministic_gt_ops < 0` is only accidentally caught (any negative
number is `< MIN_GT_OPS_PER_REP`), not by any real check.

**Cause.** `Repetition` is a plain dataclass, the same gap `Thresholds` had before PI-12 —
nobody applied `Metrics`' scrutiny to this third dataclass.

**Decision (2026-10-02).** `Repetition.__post_init__` validates every `metrics` value as a
fraction in `[0, 1]` and `deterministic_gt_ops` as `>= 0`, explicitly rather than by accident.
Implemented in `stability.py`; tests in `tests/test_m0.py`. Full working notes:
`docs/target/m0-pi13-repetition-unvalidated.md`.

## PI-14: `NaN` in latency data breaks `percentile()`'s permutation invariance (RESOLVED)

**Symptom.** `NaN` is missed by PI-11's `v < 0` check (`NaN` comparisons are always `False`) and
breaks `sorted()`'s determinism: the identical multiset, permuted into 20 different input orders,
produced `p95 ∈ {0.1, 5.0}` for the same data — `percentile()` lost the permutation-invariance
property this project treats as foundational elsewhere (the core engine's own
`test_permutation_invariance`). `inf` is milder (sorts correctly, a single `inf` among `n=100`
legitimately reads `PASS` by design — percentiles tolerate a bounded fraction of arbitrary
values) but is still physically meaningless for a real latency.

**Cause.** `gate()` validated count (PI-8) and sign (PI-11) but never finiteness.

**Decision (2026-10-02).** `gate()` rejects any non-finite value (`NaN` or `inf`) in
`l1_corrected`, same chokepoint as PI-8/PI-11. Implemented in `latency.py`; tests in
`tests/test_m0.py`, including the permutation-based reproduction. Full working notes:
`docs/target/m0-pi14-nan-inf-latency.md`.

## PI-15: the perturbation loop can synthesize impossible points on either side of two invariants (RESOLVED)

**Symptom.** PI-10 fixed one direction of one pair (perturbing `D` onto an impossible
`D > D_acc` point). The opposite direction of the same pair (perturbing `D_acc` instead) reaches
the identical kind of impossible point and was never checked — confirmed, isolated so only this
one of the ten single-axis perturbations flips: `d_cov=0.999, d_acc=0.991, r=0.95, t=0.90`
(nominal `A`, `D = 0.990009 >= 95%`, `D_acc = 99.1% >= 99%`) reports `BORDERLINE` via perturbing
`D_acc` alone by its own band (`-0.005`, crossing the `99%` boundary to `98.6%` while `D` stays at
its real, unperturbed `0.990009` — above the perturbed `D_acc`, impossible). A second, independent
pair has the identical problem: `R`/`T`, the exact invariant PI-7 enforces on *measured* values,
is never checked *during perturbation* — confirmed: `r=0.81, t=0.80` (nominal `A`, `Q=98.8%`)
reports `BORDERLINE` via perturbing `r` alone onto the impossible `(r=0.79, t=0.80)`, `t > r`.

**Cause.** The perturbation loop treats `D`, `D_acc`, `unresolved`, `R`, `T` as five fully
independent axes, but two pairs are not independent: `D <= D_acc` (`D = D_cov * D_acc`,
`D_cov <= 1`) and `T <= R` (PI-7). PI-10 patched the short-circuit, which happened to fix one
direction of one pair, not the root cause.

**Decision (2026-10-02).** The perturbation loop itself skips any synthetic point where
`moved[d] > moved[d_acc]` or `moved[t] > moved[r]`, before asking `_classify` to judge it — both
always real `Decimal`s inside this loop, since it only runs when `borderline_checked` is already
true. Subsumes PI-10's fix for the borderline interaction without requiring the short-circuit's
specific wording; PI-10's tightening stays in place as defense-in-depth on the nominal
classification. Implemented in `attribution.py`; tests in `tests/test_m0.py` reproducing both
exact cases. Full working notes:
`docs/target/m0-pi15-perturbation-ignores-cross-metric-invariants.md`.

## PI-16: `Metrics` carries no sample size — the strongest verdict needs none to fire (RESOLVED, thresholds PROVISIONAL)

**Symptom.** `Metrics(d_cov=1, d_acc=1, ...)` from a single lucky request (`1/1`) produces
`Attribution.A` — the protocol's strongest, most committing verdict — with zero statistical
basis. Nothing in `Metrics` carries how many observations its ratios were computed over.
`stability.py` already has this concept for repetitions (`MIN_GT_OPS_PER_REP=10,000`);
`latency.py` has it for signals (`MIN_SIGNALS=100`, PI-8). `attribution.py` had nothing
analogous.

**Cause.** `d_cov`/`d_acc`/`r`/`t`/`truth_coverage`/`oracle_capture_failure` are reported as bare
ratios with no denominator anywhere in `Metrics`.

**Decision (2026-10-03).** Two new required fields, no defaults: `n` (total attempts behind
`d_cov`/`d_acc`/`truth_coverage`/`oracle_capture_failure`) and `n_residual` (residual attempts
behind `r`/`t` — `None` iff `r`/`t` are `None`, PI-6). `Metrics.__post_init__` rejects either
below its minimum. `n`'s minimum reuses `stability.MIN_GT_OPS_PER_REP=10,000` (the same decision
metrics, not a new number). `n_residual`'s minimum provisionally reuses the same `10,000`,
explicitly flagged more conservative than may be necessary for the structurally smaller residual
population — no independent justification exists yet for a smaller number. No new `RunStatus`: a
raised exception at construction, the same layer PI-7 already fails at. Full working notes:
`docs/target/m0-pi16-metrics-missing-sample-size.md`.

## PI-17: ratios can be mathematically impossible given their own sample size (RESOLVED)

**Symptom.** `Metrics(d_cov=0.12345, n=10000, ...)` is accepted, but `0.12345 * 10000 = 1234.5`
"correct" requests — not an integer, so this ratio could not have come from any real run. A
direct consequence of PI-16: this check was not expressible before `n`/`n_residual` existed.

**Cause.** Ratios and their sample sizes are independent fields with no consistency check
between them — the same "two things that should move together, tracked separately" shape as
PI-9 (repetitions and their metric names) and PI-15 (`D` and `D_acc` as independent perturbation
axes).

**Decision (2026-10-04).** A precision-derived achievability check (no invented epsilon): round
`value * denominator` to the nearest integer `k`, then confirm `k / denominator`, rounded back to
`value`'s own decimal precision, reproduces `value` exactly. Applied to
`d_cov`/`truth_coverage`/`oracle_capture_failure` against `n`; to `d_acc` against the
`D_cov`-implied count (`round(d_cov * n)`), vacuously true when that count is `0` (`D_cov=0`
genuinely has no sub-population to check `D_acc` against); to `r`/`t` against `n_residual` when
measured. Implemented in `attribution.py`; tests in `tests/test_m0.py`. Full working notes:
`docs/target/m0-pi17-ratio-sample-size-mismatch.md`.
