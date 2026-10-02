"""M0 attribution verdict — the pre-registered decision rules, as code.

Source: ``docs/m0-evaluation-protocol.md`` sections 2-3. Every threshold below is
copied from that document; none may be adjusted after a measurement has been
seen. A different threshold is a new protocol version, not a parameter tweak.

All quantities are fractions in ``[0, 1]`` held as ``Decimal``: the ±2 point
borderline band is an inclusive comparison, and binary floats would misplace
``0.93 + 0.02`` against ``0.95``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

_ONE = Decimal(1)
_ZERO = Decimal(0)


class RunStatus(Enum):
    """Gate 0 — experimental validity, decided before any A/B/C question."""

    NULL = "NULL"  # probe failed, not the system: repair the harness and rerun
    VALID_BUT_INCONCLUSIVE = "VALID_BUT_INCONCLUSIVE"  # traffic not representative
    VALID = "VALID"


class Attribution(Enum):
    A = "A"
    A_WITH_WEAK_RESIDUAL = "A_WITH_WEAK_RESIDUAL"
    NEEDS_DIFFERENT_MECHANISM = "NEEDS_DIFFERENT_MECHANISM"
    C_SCORING_REQUIRED = "C_SCORING_REQUIRED"
    B_HYBRID = "B_HYBRID"
    # Not a verdict: an outcome that forbids pronouncing one.
    BORDERLINE = "BORDERLINE"  # a decision metric is within the band of a threshold
    # Not a verdict either: the branch needed r/t (protocol §3.2's residual
    # metrics) to decide, and the correlator was never run (PI-6).
    CORRELATOR_NOT_MEASURED = "CORRELATOR_NOT_MEASURED"


@dataclass(frozen=True, slots=True)
class Thresholds:
    """Pre-registered thresholds (protocol §3).

    ``q_acceptable_min`` has no default on purpose: the protocol says "Q
    acceptable" without giving a number. It must be pre-registered explicitly
    before the run rather than chosen here, or after the figures.
    """

    q_acceptable_min: Decimal
    d_min: Decimal = Decimal("0.95")
    d_acc_min: Decimal = Decimal("0.99")
    unresolved_max: Decimal = Decimal("0.03")
    r_min: Decimal = Decimal("0.80")
    e_min: Decimal = Decimal("0.90")
    capture_failure_max: Decimal = Decimal("0.01")
    truth_coverage_min: Decimal = Decimal("0.95")
    borderline_band: Decimal = Decimal("0.02")
    # D_acc's threshold leaves a 1-point error budget, so the ±2 point band would
    # make even a perfect D_acc borderline (protocol issue PI-2).
    d_acc_band: Decimal = Decimal("0.005")


# Run #001 thresholds. ``q_acceptable_min`` = 0.90 is provisional (issue PI-3): it
# mirrors the E >= 90% bar of category B and must be frozen before the run.
RUN_001 = Thresholds(q_acceptable_min=Decimal("0.90"))


@dataclass(frozen=True, slots=True)
class Metrics:
    """Measurements of one run (protocol §2).

    ``unresolved`` (GT-UNKNOWN) is ``1 - truth_coverage``; it is derived, not
    passed, so the two cannot disagree.
    """

    d_cov: Decimal  # OBI supplies a trace_id
    d_acc: Decimal  # of those, fraction correct
    r: Decimal | None  # residual_candidate_recall; None = correlator not measured (PI-6)
    t: Decimal | None  # residual_top1_accuracy; None = correlator not measured (PI-6)
    truth_coverage: Decimal  # GT1 + GT2 + GT3
    oracle_capture_failure: Decimal  # probe defect, share of all traffic

    def __post_init__(self) -> None:
        for name in ("d_cov", "d_acc", "truth_coverage", "oracle_capture_failure"):
            if not _ZERO <= getattr(self, name) <= _ONE:
                raise ValueError(f"{name} must be a fraction in [0, 1]")
        for name in ("r", "t"):
            value = getattr(self, name)
            if value is not None and not _ZERO <= value <= _ONE:
                raise ValueError(f"{name} must be a fraction in [0, 1], or None if not measured")
        if self.r is not None and self.t is not None and self.t > self.r:
            # A correct top1 pick is, by construction, inside the candidate
            # set R counts -- T > R can only come from a measurement bug
            # (diverging denominators), never a genuine result (PI-7).
            raise ValueError(f"t ({self.t}) cannot exceed r ({self.r}): T <= R always")
        if self.oracle_capture_failure > self.unresolved:
            raise ValueError("oracle_capture_failure cannot exceed the unresolved zone")

    @property
    def unresolved(self) -> Decimal:
        return _ONE - self.truth_coverage

    @property
    def d(self) -> Decimal:
        return self.d_cov * self.d_acc

    @property
    def f(self) -> Decimal:
        return _ONE - self.d

    @property
    def e(self) -> Decimal | None:
        return _e(self.d, self.t) if self.t is not None else None


def _e(d: Decimal, t: Decimal) -> Decimal:
    return d + (_ONE - d) * t


def _q(r: Decimal, t: Decimal, th: Thresholds) -> Decimal | None:
    """``T / R``, defined only for ``R >= 80%`` (protocol §3.1)."""
    return t / r if r >= th.r_min else None


def gate0(m: Metrics, th: Thresholds) -> RunStatus:
    if m.oracle_capture_failure > th.capture_failure_max:
        return RunStatus.NULL
    if m.truth_coverage < th.truth_coverage_min:
        return RunStatus.VALID_BUT_INCONCLUSIVE
    return RunStatus.VALID


def _classify(
    d: Decimal,
    d_acc: Decimal,
    unresolved: Decimal,
    r: Decimal | None,
    t: Decimal | None,
    th: Thresholds,
) -> Attribution:
    """The tree of §3.2, with no borderline handling.

    ``F == 0`` (``d == 1`` *and* ``d_acc == 1``) short-circuits to ``A``
    before ``r``/``t`` are ever read: an empty residual leaves nothing for
    the §3.3 veto to act on, so no measurement of the correlator could
    change this verdict (PI-6). Every other branch that needs ``r``/``t``
    returns ``CORRELATOR_NOT_MEASURED`` rather than guessing when they are
    ``None``.

    Checking ``d_acc`` too, not just ``d`` (PI-10): for a genuine
    measurement ``d == d_cov * d_acc == 1`` already forces ``d_acc == 1``
    (two factors `<= 1` multiply to `1` only if both do), so this is a
    no-op on real data. It matters for the §3.3 perturbation loop, which
    moves `d` and `d_acc` as independent axes and can otherwise synthesize
    the impossible point ``(d=1, d_acc=0.98)`` -- `d == 1` alone would
    treat that as a genuine zero-residual measurement.
    """
    if d == _ONE and d_acc == _ONE:
        return Attribution.A
    if d >= th.d_min and d_acc >= th.d_acc_min and unresolved <= th.unresolved_max:
        if r is None or t is None:
            return Attribution.CORRELATOR_NOT_MEASURED
        q = _q(r, t, th)
        if q is not None and q >= th.q_acceptable_min:
            return Attribution.A
        return Attribution.A_WITH_WEAK_RESIDUAL
    if d >= th.d_min:
        # Coverage is enough but OBI's IDs are not reliable enough (D_acc) or the
        # truth zone is too unresolved: F <= 5%, so not the F > 5% branches below.
        # Protocol amendment: routed to NEEDS_DIFFERENT_MECHANISM. Does not need
        # r/t, so unaffected by whether the correlator was measured (PI-1).
        return Attribution.NEEDS_DIFFERENT_MECHANISM
    if r is None or t is None:
        return Attribution.CORRELATOR_NOT_MEASURED
    if r < th.r_min:
        return Attribution.NEEDS_DIFFERENT_MECHANISM
    if _e(d, t) >= th.e_min:
        return Attribution.B_HYBRID
    return Attribution.C_SCORING_REQUIRED


@dataclass(frozen=True, slots=True)
class Decision:
    run: RunStatus
    attribution: Attribution | None  # None unless the run is VALID
    nominal: Attribution | None  # what the point estimate says, even if BORDERLINE
    # Whether §3.3's full perturbation set (D, D_acc, unresolved, R, T) could
    # actually be run. False whenever r/t are unmeasured (PI-6): R and T
    # cannot be perturbed if they were never measured, so a run without a
    # correlator can never be certified robust in the sense §3.3 defines —
    # reporting "not BORDERLINE" in that case would claim a check that never
    # happened.
    borderline_checked: bool


def decide(m: Metrics, th: Thresholds) -> Decision:
    """Gate 0, then the attribution verdict.

    BORDERLINE (§3.3) means: moving any single input metric by the band
    (±2 points, ±0.5 for D_acc) could change the verdict. It is computed by perturbation, not
    by comparing each metric to its own threshold, so a metric that cannot
    change the outcome (E when D already clears 95%) does not trigger it.
    """
    status = gate0(m, th)
    if status is not RunStatus.VALID:
        return Decision(status, None, None, False)

    point = {"d": m.d, "d_acc": m.d_acc, "unresolved": m.unresolved, "r": m.r, "t": m.t}
    nominal = _classify(**point, th=th)
    borderline_checked = m.r is not None and m.t is not None

    if not borderline_checked:
        # §3.3 perturbs all five of D, D_acc, unresolved, R, T. Without a
        # correlator measurement that set can never be run in full (PI-6):
        # e.g. perturbing `d` alone away from an exact F == 0 can re-enter a
        # branch that needs r/t, which would spuriously report BORDERLINE
        # for a reason that has nothing to do with anything measured. Report
        # the nominal outcome as-is; `borderline_checked = False` is the
        # honest signal that robustness was not verified.
        return Decision(status, nominal, nominal, False)

    for name, value in point.items():
        for sign in (-1, 1):
            band = th.d_acc_band if name == "d_acc" else th.borderline_band
            moved = {**point, name: value + sign * band}
            if _classify(**moved, th=th) is not nominal:
                return Decision(status, Attribution.BORDERLINE, nominal, True)
    return Decision(status, nominal, nominal, True)
