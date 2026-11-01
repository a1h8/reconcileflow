"""Observation (J4): the evidence, watched.

Pure functions over a replayed journal: no clock, no I/O, no state. The same
journal always yields the same series, verdicts and alerts. Real time enters
only as data, the capture time each arrival carries.

- series: final decisions counted by side, source, capture day and outcome
  (a rule name for a match, the typed reason for a break), forced ones apart.
  A provisional decision may still change, so it is not counted: a series
  lags by the time finality takes;
- drift: a declared rule turns a rule's daily match rate into verdicts, and a
  drop into a typed alert. A day with too few decisions is INSUFFICIENT, never
  NORMAL. The rule is one an auditor can redo by hand: Wilson bounds, a
  reference of the previous days, a margin;
- evidence: a silent source, capture failures above their bound, a rise of
  undecided outcomes, each a typed alert.
"""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from enum import Enum

from .numeric import ENGINE_CONTEXT
from .stream import (
    AS_COUNTERPART,
    LATE,
    LEFT,
    REFERENCE_AFTER_FINAL,
    SIDES,
    Arrival,
    Entry,
    Status,
    StreamResult,
)

_SEP = "\x1f"  # as in models._digest: no field can run into the next
_PLACES = Decimal("0.0001")  # bounds as written in an alert
# Outcomes that are not a decision on the merits: the evidence did not allow one.
UNDECIDED = frozenset({LATE, REFERENCE_AFTER_FINAL, "BLOCK_TOO_LARGE"})


def _digest(*parts: str) -> str:
    return hashlib.sha256(_SEP.join(parts).encode()).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class DriftRule:
    """Declared before any measurement (J4 spec, Q4)."""

    reference_days: int = 14
    min_decisions: int = 200
    margin: Decimal = Decimal("0.05")
    z: Decimal = Decimal("1.96")  # 95 %, two-sided

    def rule_id(self) -> str:
        return _digest(
            "drift",
            str(self.reference_days),
            str(self.min_decisions),
            str(self.margin),
            str(self.z),
        )


@dataclass(frozen=True, slots=True)
class EvidenceRule:
    silent_days: int = 2
    max_capture_failure: Decimal = Decimal("0.01")

    def rule_id(self) -> str:
        return _digest("evidence", str(self.silent_days), str(self.max_capture_failure))


DEFAULT_DRIFT = DriftRule()
DEFAULT_EVIDENCE = EvidenceRule()


@dataclass(frozen=True, slots=True)
class CaptureFailure:
    """A payload from a source that could not become a record."""

    side: str
    source: str
    captured_at: datetime


@dataclass(frozen=True, slots=True)
class Count:
    side: str
    source: str
    day: date  # UTC capture date
    outcome: str  # rule name for a match, reason for a break
    forced: bool
    count: int


class Verdict(Enum):
    NORMAL = "NORMAL"
    DROP = "DROP"
    INSUFFICIENT = "INSUFFICIENT"  # too few decisions that day
    NO_REFERENCE = "NO_REFERENCE"  # too few decisions in the reference days


@dataclass(frozen=True, slots=True)
class DayVerdict:
    side: str
    source: str
    rule: str
    day: date
    verdict: Verdict
    matched: int
    decided: int
    reference_matched: int
    reference_decided: int
    upper: Decimal | None  # Wilson upper bound of the day's rate
    reference_lower: Decimal | None  # Wilson lower bound of the reference rate


class AlertKind(Enum):
    MATCH_RATE_DROP = "MATCH_RATE_DROP"
    SOURCE_SILENT = "SOURCE_SILENT"
    CAPTURE_FAILURE = "CAPTURE_FAILURE"
    UNDECIDED_JUMP = "UNDECIDED_JUMP"


@dataclass(frozen=True, slots=True)
class Alert:
    kind: AlertKind
    side: str
    source: str
    day: date
    detail: str  # what crossed which bound, readable and stable
    rule_id: str


@dataclass(frozen=True, slots=True)
class Observation:
    counts: tuple[Count, ...]
    verdicts: tuple[DayVerdict, ...]
    alerts: tuple[Alert, ...]


# -- checks ----------------------------------------------------------------------


def _check_drift(drift: DriftRule) -> None:
    for name in ("reference_days", "min_decisions"):
        value = getattr(drift, name)
        if not (isinstance(value, int) and not isinstance(value, bool) and value >= 1):
            raise ValueError(f"{name} must be an integer >= 1, got {value!r}")
    if not (isinstance(drift.margin, Decimal) and drift.margin >= 0):
        raise ValueError(f"margin must be a Decimal >= 0, got {drift.margin!r}")
    if not (isinstance(drift.z, Decimal) and drift.z > 0):
        raise ValueError(f"z must be a Decimal > 0, got {drift.z!r}")


def _check_evidence(evidence: EvidenceRule) -> None:
    days = evidence.silent_days
    if not (isinstance(days, int) and not isinstance(days, bool) and days >= 1):
        raise ValueError(f"silent_days must be an integer >= 1, got {days!r}")
    bound = evidence.max_capture_failure
    if not (isinstance(bound, Decimal) and 0 <= bound <= 1):
        raise ValueError(f"max_capture_failure must be a Decimal in [0, 1], got {bound!r}")


def _capture_day(what: str, captured_at: datetime | None) -> date:
    if captured_at is None:
        raise ValueError(f"{what} has no captured_at: it cannot be placed in time")
    if captured_at.utcoffset() is None:
        raise ValueError(f"{what} captured_at has no timezone: {captured_at!r}")
    return captured_at.astimezone(UTC).date()


# -- Wilson ----------------------------------------------------------------------


def wilson(successes: int, trials: int, z: Decimal) -> tuple[Decimal, Decimal]:
    """Wilson score interval of successes / trials, under the engine context."""
    with localcontext(ENGINE_CONTEXT):
        return _wilson(successes, trials, z)


def _wilson(successes: int, trials: int, z: Decimal) -> tuple[Decimal, Decimal]:
    if not 0 <= successes <= trials or trials < 1:
        raise ValueError(f"need 0 <= successes <= trials and trials >= 1, got {successes}/{trials}")
    n = Decimal(trials)
    p = Decimal(successes) / n
    z2 = z * z
    centre = p + z2 / (2 * n)
    spread = z * (p * (1 - p) / n + z2 / (4 * n * n)).sqrt()
    denominator = 1 + z2 / n
    low = max(Decimal(0), (centre - spread) / denominator)
    high = min(Decimal(1), (centre + spread) / denominator)
    return low, high


def _bound(value: Decimal) -> str:
    return str(value.quantize(_PLACES))


# -- series ----------------------------------------------------------------------


def _origins(journal: Iterable[Entry]) -> dict[tuple[str, str], tuple[str, date]]:
    """The source and capture day of each record, from its first arrival."""
    origins: dict[tuple[str, str], tuple[str, date]] = {}
    for entry in journal:
        if isinstance(entry, Arrival):
            key = (entry.side, entry.record.id)
            if key not in origins:
                day = _capture_day(f"arrival {key!r}", entry.captured_at)
                origins[key] = (entry.source, day)
    return origins


def series(journal: Iterable[Entry], result: StreamResult) -> tuple[Count, ...]:
    """Final decisions counted by side, source, capture day, outcome and forced."""
    origins = _origins(journal)
    effective = result.effective()
    counter: Counter[tuple[str, str, date, str, bool]] = Counter()
    for key, decision in effective.items():
        if decision.status is not Status.FINAL:
            continue
        outcome = decision.reason
        if outcome == AS_COUNTERPART:
            # Counted under its match's rule, which its left key carries.
            outcome = effective[(LEFT, decision.input_ids[0])].reason
        source, day = origins[key]
        counter[(key[0], source, day, outcome, decision.forced)] += 1
    return tuple(Count(*fields, count) for fields, count in sorted(counter.items()))


# -- drift -----------------------------------------------------------------------


def _daily(counts: Iterable[Count], side: str, hit) -> dict[str, dict[date, tuple[int, int]]]:
    """Per source and day: (hits, total) over the counts of one side."""
    days: dict[str, dict[date, list[int]]] = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for c in counts:
        if c.side == side:
            cell = days[c.source][c.day]
            cell[0] += c.count if hit(c) else 0
            cell[1] += c.count
    return {s: {d: (h, t) for d, (h, t) in sorted(by_day.items())} for s, by_day in days.items()}


def _reference(by_day: dict[date, tuple[int, int]], day: date, span: int) -> tuple[int, int]:
    hits = total = 0
    for offset in range(1, span + 1):
        h, t = by_day.get(day - timedelta(days=offset), (0, 0))
        hits += h
        total += t
    return hits, total


def match_rate(
    counts: Iterable[Count], side: str, rule: str, drift: DriftRule
) -> tuple[DayVerdict, ...]:
    """A verdict per source and day on ``rule``'s share of the non-forced finals."""
    _check_drift(drift)
    unforced = [c for c in counts if not c.forced]
    daily = _daily(unforced, side, lambda c: c.outcome == rule)
    verdicts = []
    for source in sorted(daily):
        by_day = daily[source]
        for day, (matched, decided) in by_day.items():
            ref_matched, ref_decided = _reference(by_day, day, drift.reference_days)
            upper = reference_lower = None
            if decided < drift.min_decisions:
                verdict = Verdict.INSUFFICIENT
            elif ref_decided < drift.min_decisions:
                verdict = Verdict.NO_REFERENCE
            else:
                upper = wilson(matched, decided, drift.z)[1]
                reference_lower = wilson(ref_matched, ref_decided, drift.z)[0]
                dropped = upper < reference_lower - drift.margin
                verdict = Verdict.DROP if dropped else Verdict.NORMAL
            verdicts.append(
                DayVerdict(
                    side,
                    source,
                    rule,
                    day,
                    verdict,
                    matched,
                    decided,
                    ref_matched,
                    ref_decided,
                    upper,
                    reference_lower,
                )
            )
    return tuple(verdicts)


# -- evidence --------------------------------------------------------------------


def _silent(origins, evidence: EvidenceRule) -> list[Alert]:
    if not origins:
        return []
    last_seen: dict[tuple[str, str], date] = {}
    for (side, _), (source, day) in origins.items():
        last_seen[(side, source)] = max(day, last_seen.get((side, source), day))
    end = max(last_seen.values())
    return [
        Alert(
            AlertKind.SOURCE_SILENT,
            side,
            source,
            end,
            f"last capture {seen.isoformat()}, {(end - seen).days} days before {end.isoformat()}",
            evidence.rule_id(),
        )
        for (side, source), seen in sorted(last_seen.items())
        if (end - seen).days > evidence.silent_days
    ]


def _capture_failures(origins, failures, drift, evidence) -> list[Alert]:
    failed: Counter[tuple[str, str, date]] = Counter()
    for f in failures:
        failed[
            (f.side, f.source, _capture_day(f"capture failure of {f.source!r}", f.captured_at))
        ] += 1
    arrived: Counter[tuple[str, str, date]] = Counter(
        (side, source, day) for (side, _), (source, day) in origins.items()
    )
    alerts = []
    for (side, source, day), bad in sorted(failed.items()):
        total = bad + arrived[(side, source, day)]
        if total < drift.min_decisions:
            continue
        lower = wilson(bad, total, drift.z)[0]
        if lower > evidence.max_capture_failure:
            bound = evidence.max_capture_failure
            detail = f"{bad}/{total} failed, lower bound {_bound(lower)} > {bound}"
            alerts.append(
                Alert(AlertKind.CAPTURE_FAILURE, side, source, day, detail, evidence.rule_id())
            )
    return alerts


def _undecided(counts, drift: DriftRule) -> list[Alert]:
    def hit(c: Count) -> bool:
        return c.forced or bool(UNDECIDED.intersection(c.outcome.split("+")))

    alerts = []
    for side in SIDES:
        daily = _daily(counts, side, hit)
        for source in sorted(daily):
            by_day = daily[source]
            for day, (undecided, total) in by_day.items():
                ref_undecided, ref_total = _reference(by_day, day, drift.reference_days)
                if total < drift.min_decisions or ref_total < drift.min_decisions:
                    continue
                lower = wilson(undecided, total, drift.z)[0]
                reference_upper = wilson(ref_undecided, ref_total, drift.z)[1]
                if lower > reference_upper + drift.margin:
                    detail = (
                        f"{undecided}/{total} undecided, lower bound {_bound(lower)} > "
                        f"reference upper {_bound(reference_upper)} + margin {drift.margin}"
                    )
                    alerts.append(
                        Alert(AlertKind.UNDECIDED_JUMP, side, source, day, detail, drift.rule_id())
                    )
    return alerts


# -- all together ----------------------------------------------------------------


def observe(
    journal: Iterable[Entry],
    result: StreamResult,
    failures: Iterable[CaptureFailure] = (),
    drift: DriftRule = DEFAULT_DRIFT,
    evidence: EvidenceRule = DEFAULT_EVIDENCE,
    rules: tuple[str, ...] = ("M0_REFERENCE",),
) -> Observation:
    """Series, verdicts per rule and side, and every alert, in a stable order."""
    _check_drift(drift)
    _check_evidence(evidence)
    journal = list(journal)
    origins = _origins(journal)
    counts = series(journal, result)
    verdicts = tuple(
        v for side in SIDES for rule in rules for v in match_rate(counts, side, rule, drift)
    )
    alerts = [
        Alert(
            AlertKind.MATCH_RATE_DROP,
            v.side,
            v.source,
            v.day,
            f"{v.rule} {v.matched}/{v.decided}, upper bound {_bound(v.upper)} < reference lower "
            f"{_bound(v.reference_lower)} - margin {drift.margin}",
            drift.rule_id(),
        )
        for v in verdicts
        if v.verdict is Verdict.DROP
    ]
    alerts += _silent(origins, evidence)
    alerts += _capture_failures(origins, failures, drift, evidence)
    alerts += _undecided(counts, drift)
    alerts.sort(key=lambda a: (a.day, a.kind.value, a.side, a.source, a.detail))
    return Observation(counts, verdicts, tuple(alerts))
