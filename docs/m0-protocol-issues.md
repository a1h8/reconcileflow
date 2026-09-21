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
| PI-4 | UNDERSPEC | low | OPEN |
| PI-5 | AMBIGUOUS | medium | OPEN |

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

## PI-4: "non-catastrophic ranking" (category B) (OPEN)

**Symptom.** B cites a condition with no value. `R >= 80%` and `E >= 90%` already bound it in
practice.

**Proposed decision.** Remove the clause from the protocol, or quantify it. The code ignores it.

## PI-5: "SD <= 1.5 pt (95% CI)" (OPEN)

**Symptom.** Two readings: (i) the between-run standard deviation is at most 1.5 points;
(ii) the 95% CI of the mean is narrower than 1.5 points. They are not equivalent (the CI narrows
with n). The code applies (i), on the sample standard deviation, for each decision metric (max).

**Decision required.** Choose the reading; for (ii), specify the CI method.
