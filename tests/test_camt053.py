"""Synthetic CAMT entry fixtures: signs, dates, scope and raw-first failures."""

from decimal import Decimal
from pathlib import Path

import pytest

from reconcileflow.adapters.camt053 import IngestionError, ingest
from reconcileflow.provenance import ProvenanceStore

PAYLOAD = (Path(__file__).parent / "fixtures" / "camt053-booked.xml").read_bytes()


def test_credit_and_reversal_debit_preserve_entry_direction(tmp_path):
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        credit = ingest(store, PAYLOAD, source="bank")[0]
        debit_xml = PAYLOAD.replace(
            b"<CdtDbtInd>CRDT</CdtDbtInd>", b"<CdtDbtInd>DBIT</CdtDbtInd><RvslInd>true</RvslInd>"
        )
        debit = ingest(store, debit_xml, source="bank")[0]
        assert credit.record.amount == Decimal("100.00")
        assert debit.record.amount == Decimal("-100.00")
        assert str(credit.record.value_date) == "2026-09-28"
        assert credit.record.currency == "EUR"
        assert credit.record.reference is None


@pytest.mark.parametrize(
    "before,after",
    [
        (b"camt.053.001.08", b"camt.053.001.02"),
        (b"<Cd>BOOK</Cd>", b"<Cd>PDNG</Cd>"),
        (b"100.00", b"NaN"),
        (b"100.00", b"-100"),
        (b"100.00", b"1E2"),
        (b'Ccy="EUR"', b'Ccy="eur"'),
        (b"<ValDt><Dt>2026-09-28</Dt></ValDt>", b""),
        (
            b"<ValDt><Dt>2026-09-28</Dt></ValDt>",
            b"<ValDt><DtTm>2026-09-28T00:00:00Z</DtTm></ValDt>",
        ),
        (b"2026-09-28", b"2026-02-30"),
        (b"<CdtDbtInd>CRDT</CdtDbtInd>", b"<CdtDbtInd>OTHER</CdtDbtInd>"),
        (b"<NtryRef>LAB-ENTRY</NtryRef>", b'<Amt Ccy="EUR">20</Amt>'),
    ],
)
def test_unsupported_or_invalid_input_has_explicit_error_and_retained_raw(tmp_path, before, after):
    payload = PAYLOAD.replace(before, after)
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        with pytest.raises(IngestionError) as error:
            ingest(store, payload, source="bank")
        assert store.raw_event(error.value.raw_event_id).payload == payload


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16"])
def test_dtd_rejected_even_with_non_utf8_encoding(tmp_path, encoding):
    xml = f'<?xml version="1.0" encoding="{encoding}"?><!DOCTYPE Document [<!ENTITY test "expanded">]><Document>&test;</Document>'
    with (
        ProvenanceStore(tmp_path / "raw.sqlite") as store,
        pytest.raises(IngestionError, match="DTD"),
    ):
        ingest(store, xml.encode(encoding), source="bank")


def test_malformed_xml_is_preserved(tmp_path):
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        with pytest.raises(IngestionError) as error:
            ingest(store, b"<Document", source="bank")
        assert store.raw_event(error.value.raw_event_id).payload == b"<Document"


def test_multiple_statements_and_entries_have_distinct_locators(tmp_path):
    statement = PAYLOAD[
        PAYLOAD.index(b"    <Stmt>") : PAYLOAD.index(b"    </Stmt>") + len(b"    </Stmt>")
    ]
    payload = PAYLOAD.replace(b"  </BkToCstmrStmt>", statement + b"  </BkToCstmrStmt>")
    with ProvenanceStore(tmp_path / "raw.sqlite") as store:
        records = ingest(store, payload, source="bank")
        assert len(records) == 2
        assert records[0].record.id != records[1].record.id
        assert records[0].source_locator == "/Document/BkToCstmrStmt/Stmt[1]/Ntry[1]"
        assert records[1].source_locator == "/Document/BkToCstmrStmt/Stmt[2]/Ntry[1]"
