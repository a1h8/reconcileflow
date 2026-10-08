"""Durable source provenance outside the side-effect-free matching engine.

SQLite commits raw bytes before an adapter runs. Canonical records and runs are
append-only through this API; stored content is checked when it is read.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from .engine import reconcile
from .models import MatchResult, Record, Tolerance


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(value: object) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _record_data(record: Record) -> dict:
    return {
        **asdict(record),
        "amount": str(record.amount),
        "value_date": record.value_date.isoformat(),
    }


def _result_data(result: MatchResult) -> dict:
    return {
        "ruleset_id": result.ruleset_id,
        "result_hash": result.result_hash,
        "matches": [
            {
                **asdict(m),
                "rule": m.rule.name,
                "score": str(m.score),
                "amount_residual": str(m.amount_residual),
            }
            for m in result.matches
        ],
        "rejects": [{**asdict(r), "reason": r.reason.name} for r in result.rejects],
        "counters": dict(result.counters),
    }


@dataclass(frozen=True, slots=True)
class RawEvent:
    raw_event_id: str
    source: str
    received_at: datetime
    payload_sha256: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class CanonicalRecord:
    record: Record
    raw_event_id: str
    source_locator: str
    adapter_version: str
    normalizer_version: str

    @classmethod
    def create(
        cls,
        raw_event_id: str,
        source_locator: str,
        adapter_version: str,
        normalizer_version: str,
        *,
        account: str,
        amount: Decimal,
        value_date: date,
        currency: str,
        reference: str | None = None,
    ) -> CanonicalRecord:
        if not all((raw_event_id, source_locator, adapter_version, normalizer_version, account)):
            raise ValueError("provenance fields and account must be nonempty")
        if type(value_date) is not date:
            raise ValueError("value_date must be a date without time")
        if not isinstance(amount, Decimal) or not amount.is_finite():
            raise ValueError("amount must be a finite Decimal")
        if (
            not isinstance(currency, str)
            or len(currency) != 3
            or not currency.isascii()
            or not currency.isupper()
            or not currency.isalpha()
        ):
            raise ValueError("currency must be three uppercase ASCII letters")
        record = Record("", account, amount, value_date, reference, currency)
        identity = _hash(
            [
                raw_event_id,
                source_locator,
                adapter_version,
                normalizer_version,
                _record_data(record),
            ]
        )
        return cls(
            Record(identity, account, amount, value_date, reference, currency),
            raw_event_id,
            source_locator,
            adapter_version,
            normalizer_version,
        )

    def to_dict(self) -> dict:
        return {**asdict(self), "record": _record_data(self.record)}


@dataclass(frozen=True, slots=True)
class ProvenancedResult:
    run_id: str
    result: MatchResult


class ProvenanceStore:
    """Local durable store. Own a store per thread; use as a context manager.

    Reingesting identical bytes from the same source returns the original event.
    This is payload deduplication, not business-transaction deduplication.
    """

    def __init__(self, path: str | Path):
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS raw_events (
                id TEXT PRIMARY KEY, source TEXT NOT NULL, received_at TEXT NOT NULL,
                sha256 TEXT NOT NULL, payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS canonical_records (
                id TEXT PRIMARY KEY, raw_id TEXT NOT NULL REFERENCES raw_events(id),
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS reconciliation_runs (
                id TEXT PRIMARY KEY, data TEXT NOT NULL
            );
        """)

    def __enter__(self) -> ProvenanceStore:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._db.close()

    def capture(self, payload: bytes, *, source: str) -> RawEvent:
        if not source or not isinstance(payload, bytes):
            raise ValueError("source must be nonempty and payload must be bytes")
        sha = hashlib.sha256(payload).hexdigest()
        identity = _hash([source, sha])
        received_at = datetime.now(UTC).isoformat()
        # Separate transaction: a subsequent parse/normalisation failure cannot
        # roll back evidence of what was received.
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO raw_events VALUES (?, ?, ?, ?, ?)",
                (identity, source, received_at, sha, payload),
            )
        return self.raw_event(identity)

    def raw_event(self, identity: str) -> RawEvent:
        row = self._db.execute(
            "SELECT source, received_at, sha256, payload FROM raw_events WHERE id=?", (identity,)
        ).fetchone()
        if row is None:
            raise KeyError(identity)
        source, received_at, sha, payload = row
        if hashlib.sha256(payload).hexdigest() != sha or _hash([source, sha]) != identity:
            raise ValueError("raw evidence integrity check failed")
        return RawEvent(identity, source, datetime.fromisoformat(received_at), sha, payload)

    def save_records(self, records: Sequence[CanonicalRecord]) -> None:
        """Persist a complete adapter result atomically; reject conflicting identities."""
        with self._db:
            for canonical in records:
                self.raw_event(canonical.raw_event_id)
                self._validate_canonical(canonical)
                data = _json(canonical.to_dict())
                self._db.execute(
                    "INSERT OR IGNORE INTO canonical_records VALUES (?, ?, ?)",
                    (canonical.record.id, canonical.raw_event_id, data),
                )
                stored = self._db.execute(
                    "SELECT data FROM canonical_records WHERE id=?", (canonical.record.id,)
                ).fetchone()[0]
                if stored != data:
                    raise ValueError("canonical identity collision")

    @staticmethod
    def _validate_canonical(canonical: CanonicalRecord) -> None:
        record = canonical.record
        expected = CanonicalRecord.create(
            canonical.raw_event_id,
            canonical.source_locator,
            canonical.adapter_version,
            canonical.normalizer_version,
            account=record.account,
            amount=record.amount,
            value_date=record.value_date,
            currency=record.currency,
            reference=record.reference,
        )
        if canonical != expected:
            raise ValueError("canonical record integrity check failed")

    def canonical_record(self, identity: str) -> CanonicalRecord:
        row = self._db.execute(
            "SELECT raw_id, data FROM canonical_records WHERE id=?", (identity,)
        ).fetchone()
        if row is None:
            raise KeyError(identity)
        data = json.loads(row[1])
        record = data.pop("record")
        record["amount"] = Decimal(record["amount"])
        record["value_date"] = date.fromisoformat(record["value_date"])
        canonical = CanonicalRecord(record=Record(**record), **data)
        if canonical.record.id != identity or canonical.raw_event_id != row[0]:
            raise ValueError("canonical record identity mismatch")
        self._validate_canonical(canonical)
        self.raw_event(canonical.raw_event_id)
        return canonical

    def reconcile(
        self,
        left_ids: Sequence[str],
        right_ids: Sequence[str],
        *,
        engine_version: str,
        tolerance: Tolerance | None = None,
    ) -> ProvenancedResult:
        """Match persisted inputs and retain the full input/configuration/result chain.

        engine_version must identify the actual release/build used by the caller.
        A label is recorded, not treated as proof that archived code is available.
        """
        if not engine_version:
            raise ValueError("engine_version is required")
        if len(set(left_ids)) != len(left_ids) or len(set(right_ids)) != len(right_ids):
            raise ValueError("duplicate input IDs on one side")
        if set(left_ids) & set(right_ids):
            raise ValueError("the same source record cannot be reconciled against itself")
        left = [self.canonical_record(key).record for key in sorted(left_ids)]
        right = [self.canonical_record(key).record for key in sorted(right_ids)]
        tol = tolerance or Tolerance()
        result = reconcile(left, right, tol)
        data = {
            "schema_version": 1,
            "engine_version": engine_version,
            "left_ids": sorted(left_ids),
            "right_ids": sorted(right_ids),
            "tolerance": {
                **asdict(tol),
                "amount_abs": str(tol.amount_abs),
                "amount_rel": str(tol.amount_rel),
            },
            **_result_data(result),
        }
        identity = _hash(data)
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO reconciliation_runs VALUES (?, ?)", (identity, _json(data))
            )
        self.run(identity)
        return ProvenancedResult(identity, result)

    def run(self, identity: str) -> dict:
        row = self._db.execute(
            "SELECT data FROM reconciliation_runs WHERE id=?", (identity,)
        ).fetchone()
        if row is None:
            raise KeyError(identity)
        data = json.loads(row[0])
        if _hash(data) != identity:
            raise ValueError("run integrity check failed")
        for key in data["left_ids"] + data["right_ids"]:
            self.canonical_record(key)
        return data

    def replay(self, run_id: str, *, engine_version: str) -> ProvenancedResult:
        """Recompute persisted inputs/configuration and verify the entire result.

        The caller must run the archived build named by engine_version. No adapter
        is rerun: the original normalised inputs and their versions are retained.
        """
        data = self.run(run_id)
        if engine_version != data["engine_version"]:
            raise ValueError("replay requires the recorded engine version")
        parameters = dict(data["tolerance"])
        parameters["amount_abs"] = Decimal(parameters["amount_abs"])
        parameters["amount_rel"] = Decimal(parameters["amount_rel"])
        result = reconcile(
            [self.canonical_record(key).record for key in data["left_ids"]],
            [self.canonical_record(key).record for key in data["right_ids"]],
            Tolerance(**parameters),
        )
        actual = _result_data(result)
        if _json(actual) != _json({key: data[key] for key in actual}):
            raise ValueError("replayed decisions differ from the recorded result")
        return ProvenancedResult(run_id, result)

    def evidence(self, run_id: str, record_id: str) -> tuple[CanonicalRecord, RawEvent]:
        run = self.run(run_id)
        if record_id not in run["left_ids"] + run["right_ids"]:
            raise ValueError("record is not an input of this run")
        canonical = self.canonical_record(record_id)
        return canonical, self.raw_event(canonical.raw_event_id)
