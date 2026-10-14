"""Source-to-decision invariants, exercised across real database reopenings."""

import sqlite3
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from reconcileflow import Record, RuleId, Tolerance, reconcile
from reconcileflow.adapters.camt053 import IngestionError, ingest
from reconcileflow.provenance import CanonicalRecord, ProvenanceStore

PAYLOAD = (Path(__file__).parent / "fixtures" / "camt053-booked.xml").read_bytes()


def test_source_to_decision_and_replay_survive_reopening(tmp_path):
    path = tmp_path / "provenance.sqlite"
    with ProvenanceStore(path) as store:
        left = ingest(store, PAYLOAD, source="bank")
        right = ingest(store, PAYLOAD.replace(b"100.00", b"100.02"), source="ledger-export")
        run = store.reconcile(
            [left[0].record.id],
            [right[0].record.id],
            engine_version="test-build",
            tolerance=Tolerance(amount_abs=Decimal("0.05")),
        )
        assert run.result.matches[0].rule == RuleId.M2_TOLERANT
        assert run.result.matches[0].amount_residual == Decimal("-0.02")
    with ProvenanceStore(path) as store:
        canonical, raw = store.evidence(run.run_id, run.result.matches[0].left_id)
        assert raw.payload == PAYLOAD
        assert canonical.source_locator == "/Document/BkToCstmrStmt/Stmt[1]/Ntry[1]"
        assert canonical.adapter_version and canonical.normalizer_version
        assert raw.received_at.tzinfo is not None
        assert store.replay(run.run_id, engine_version="test-build") == run
        with pytest.raises(ValueError, match="engine version"):
            store.replay(run.run_id, engine_version="another-build")


def test_failed_ingestion_preserves_bytes_but_no_partial_canonicals(tmp_path):
    path = tmp_path / "raw.sqlite"
    first_entry = PAYLOAD[
        PAYLOAD.index(b"      <Ntry>") : PAYLOAD.index(b"      </Ntry>") + len(b"      </Ntry>")
    ]
    payload = PAYLOAD.replace(
        b"    </Stmt>", first_entry.replace(b"BOOK", b"PDNG") + b"    </Stmt>"
    )
    with ProvenanceStore(path) as store:
        with pytest.raises(IngestionError) as failure:
            ingest(store, payload, source="bank")
        identity = failure.value.raw_event_id
    with ProvenanceStore(path) as store:
        assert store.raw_event(identity).payload == payload
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM canonical_records").fetchone()[0] == 0


def test_capture_is_committed_before_adapter_execution(tmp_path, monkeypatch):
    from reconcileflow.adapters import camt053

    path = tmp_path / "raw.sqlite"

    def fail_after_read(raw):
        with sqlite3.connect(path) as observer:
            assert (
                observer.execute(
                    "SELECT payload FROM raw_events WHERE id=?", (raw.raw_event_id,)
                ).fetchone()[0]
                == PAYLOAD
            )
        raise ValueError("adapter failed")

    monkeypatch.setattr(camt053, "normalize", fail_after_read)
    with ProvenanceStore(path) as store, pytest.raises(IngestionError, match="adapter failed"):
        ingest(store, PAYLOAD, source="bank")


def test_identical_delivery_is_idempotent_and_sources_remain_distinct(tmp_path):
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        a = ingest(store, PAYLOAD, source="A")
        again = ingest(store, PAYLOAD, source="A")
        b = ingest(store, PAYLOAD, source="B")
        assert a == again
        assert a[0].record.id != b[0].record.id
        assert store.raw_event(a[0].raw_event_id) == store.capture(PAYLOAD, source="A")
        new = CanonicalRecord.create(
            a[0].raw_event_id,
            a[0].source_locator,
            "adapter-v2",
            a[0].normalizer_version,
            account=a[0].record.account,
            amount=a[0].record.amount,
            value_date=a[0].record.value_date,
            currency="EUR",
        )
        store.save_records([new])
        assert new.record.id != a[0].record.id
        assert store.canonical_record(a[0].record.id) == a[0]


@pytest.mark.parametrize("currency", ["USD", None])
def test_currency_blocks_even_reference_matching(currency):
    left = Record("L", "ACCOUNT", Decimal("100"), date(2026, 9, 28), "REF", "EUR")
    right = replace(left, id="R", currency=currency)
    result = reconcile([left], [right])
    assert not result.matches
    assert result.counters["unmatched_left"] == result.counters["unmatched_right"] == 1


def test_reordered_inputs_produce_same_persisted_run(tmp_path):
    with ProvenanceStore(tmp_path / "run.sqlite") as store:
        left = ingest(store, PAYLOAD, source="L1") + ingest(store, PAYLOAD, source="L2")
        right = ingest(store, PAYLOAD, source="R1") + ingest(store, PAYLOAD, source="R2")
        lids, rids = [r.record.id for r in left], [r.record.id for r in right]
        a = store.reconcile(lids, rids, engine_version="test")
        b = store.reconcile(lids[::-1], rids[::-1], engine_version="test")
        assert a == b
        with pytest.raises(ValueError, match="duplicate"):
            store.reconcile(lids * 2, rids, engine_version="test")
        with pytest.raises(ValueError, match="itself"):
            store.reconcile(lids, lids, engine_version="test")
        unrelated = ingest(store, PAYLOAD, source="unrelated")[0]
        with pytest.raises(ValueError, match="not an input"):
            store.evidence(a.run_id, unrelated.record.id)


def test_changed_raw_bytes_are_detected(tmp_path):
    path = tmp_path / "raw.sqlite"
    with ProvenanceStore(path) as store:
        records = ingest(store, PAYLOAD, source="bank")
        run = store.reconcile([records[0].record.id], [], engine_version="test")
    with sqlite3.connect(path) as db:
        db.execute("UPDATE raw_events SET payload=?", (b"changed",))
    with ProvenanceStore(path) as store, pytest.raises(ValueError, match="integrity"):
        store.run(run.run_id)


def test_forged_canonical_is_rejected_atomically(tmp_path):
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        records = ingest(store, PAYLOAD, source="bank")
        forged = replace(records[0], record=replace(records[0].record, amount=Decimal("999")))
        with pytest.raises(ValueError, match="integrity"):
            store.save_records([forged])
        assert store.canonical_record(records[0].record.id) == records[0]


def test_aggregate_decision_retains_every_source_entry(tmp_path):
    first_entry = PAYLOAD[
        PAYLOAD.index(b"      <Ntry>") : PAYLOAD.index(b"      </Ntry>") + len(b"      </Ntry>")
    ]
    ledger = PAYLOAD.replace(
        first_entry,
        first_entry.replace(b"100.00", b"60.00") + first_entry.replace(b"100.00", b"40.00"),
    )
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        left = ingest(store, PAYLOAD, source="bank")
        right = ingest(store, ledger, source="ledger-example")
        run = store.reconcile(
            [left[0].record.id], [r.record.id for r in right], engine_version="test"
        )
        assert run.result.matches[0].rule == RuleId.M3_AGGREGATE
        assert len(run.result.matches[0].right_ids) == 2
        for key in run.result.matches[0].right_ids:
            canonical, raw = store.evidence(run.run_id, key)
            assert canonical.source_locator in {r.source_locator for r in right}
            assert raw.payload == ledger
        assert store.replay(run.run_id, engine_version="test") == run


def test_replay_detects_different_engine_behavior_without_rewriting_run(tmp_path, monkeypatch):
    from reconcileflow import provenance

    with ProvenanceStore(tmp_path / "run.sqlite") as store:
        left = ingest(store, PAYLOAD, source="bank")
        right = ingest(store, PAYLOAD, source="ledger")
        run = store.reconcile([left[0].record.id], [right[0].record.id], engine_version="test")
        saved = store.run(run.run_id)
        monkeypatch.setattr(provenance, "reconcile", lambda *_: replace(run.result, matches=()))
        with pytest.raises(ValueError, match="differ"):
            store.replay(run.run_id, engine_version="test")
        assert store.run(run.run_id) == saved


def test_changed_canonical_and_run_are_detected(tmp_path):
    import json

    path = tmp_path / "run.sqlite"
    with ProvenanceStore(path) as store:
        left = ingest(store, PAYLOAD, source="bank")
        run = store.reconcile([left[0].record.id], [], engine_version="test")
    with sqlite3.connect(path) as db:
        data = json.loads(db.execute("SELECT data FROM canonical_records").fetchone()[0])
        data["record"]["amount"] = "999"
        db.execute("UPDATE canonical_records SET data=?", (json.dumps(data),))
    with ProvenanceStore(path) as store, pytest.raises(ValueError, match="integrity"):
        store.canonical_record(left[0].record.id)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE reconciliation_runs SET data='{}'")
    with ProvenanceStore(path) as store, pytest.raises(ValueError, match="integrity"):
        store.run(run.run_id)


@pytest.mark.parametrize(
    "fields,reason",
    [
        ({"account": ""}, "nonempty"),
        ({"value_date": datetime(2026, 9, 28, tzinfo=UTC)}, "without time"),
        ({"amount": Decimal("NaN")}, "finite Decimal"),
        ({"amount": 100.0}, "finite Decimal"),
    ],
)
def test_canonical_creation_rejects_invalid_fields(fields, reason):
    valid = {
        "account": "ACCOUNT",
        "amount": Decimal("100"),
        "value_date": date(2026, 9, 28),
        "currency": "EUR",
    }
    with pytest.raises(ValueError, match=reason):
        CanonicalRecord.create("raw", "/locator", "adapter", "normalizer", **{**valid, **fields})


@pytest.mark.parametrize("payload,source", [(PAYLOAD, ""), (PAYLOAD.decode(), "bank")])
def test_capture_rejects_missing_source_or_non_bytes(tmp_path, payload, source):
    with ProvenanceStore(tmp_path / "raw.sqlite") as store, pytest.raises(ValueError):
        store.capture(payload, source=source)


def test_unknown_identities_raise_key_error_and_runs_need_an_engine_version(tmp_path):
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        for lookup in (store.raw_event, store.canonical_record, store.run):
            with pytest.raises(KeyError):
                lookup("unknown")
        with pytest.raises(ValueError, match="engine_version"):
            store.reconcile([], [], engine_version="")


def test_resaving_over_a_tampered_canonical_is_detected(tmp_path):
    path = tmp_path / "raw.sqlite"
    with ProvenanceStore(path) as store:
        records = ingest(store, PAYLOAD, source="bank")
    with sqlite3.connect(path) as db:
        db.execute("UPDATE canonical_records SET data='{}'")
    with ProvenanceStore(path) as store, pytest.raises(ValueError, match="collision"):
        store.save_records(records)


def test_canonical_moved_to_another_raw_event_is_detected(tmp_path):
    path = tmp_path / "raw.sqlite"
    with ProvenanceStore(path) as store:
        record = ingest(store, PAYLOAD, source="bank")[0]
        other = store.capture(b"other", source="bank")
    with sqlite3.connect(path) as db:
        db.execute("UPDATE canonical_records SET raw_id=?", (other.raw_event_id,))
    with ProvenanceStore(path) as store, pytest.raises(ValueError, match="identity mismatch"):
        store.canonical_record(record.record.id)
