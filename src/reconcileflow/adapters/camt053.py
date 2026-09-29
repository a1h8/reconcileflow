"""Narrow entry-level CAMT.053.001.08 adapter with raw-first ingestion.

Only booked entries with an explicit value date are normalised. The full XML
remains evidence; this is not an XSD validator or a transaction-detail expander.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import date
from decimal import Decimal

from ..provenance import CanonicalRecord, ProvenanceStore, RawEvent

NAMESPACE = "urn:iso:std:iso:20022:tech:xsd:camt.053.001.08"
ADAPTER_VERSION = "camt.053.001.08/entry-v1"
NORMALIZER_VERSION = "booked-value-date-signed-entry/no-reference-v1"
_NS = {"c": NAMESPACE}


class IngestionError(ValueError):
    """Parsing failed; raw_event_id points to already committed evidence."""

    def __init__(self, raw_event_id: str, reason: str):
        self.raw_event_id = raw_event_id
        super().__init__(f"{raw_event_id}: {reason}")


class _NoDTD(ET.TreeBuilder):
    def doctype(self, name, pubid, system):
        raise ValueError("DTD declarations are not supported")


def _find(node: ET.Element, path: str) -> list[ET.Element]:
    return node.findall("/".join("c:" + part for part in path.split("/")), _NS)


def _one(node: ET.Element, path: str) -> ET.Element:
    nodes = _find(node, path)
    if len(nodes) != 1:
        raise ValueError(f"expected exactly one {path}")
    return nodes[0]


def _text(node: ET.Element, path: str) -> str:
    value = (_one(node, path).text or "").strip()
    if not value:
        raise ValueError(f"empty {path}")
    return value


def normalize(raw: RawEvent) -> tuple[CanonicalRecord, ...]:
    root = ET.fromstring(raw.payload, parser=ET.XMLParser(target=_NoDTD()))
    if root.tag != f"{{{NAMESPACE}}}Document":
        raise ValueError("only CAMT.053.001.08 Document is supported")
    statements = _find(_one(root, "BkToCstmrStmt"), "Stmt")
    if not statements:
        raise ValueError("statement is missing")
    records = []
    for statement_index, statement in enumerate(statements, 1):
        _text(statement, "Id")
        account_id = _one(statement, "Acct/Id")
        accounts = _find(account_id, "IBAN") + _find(account_id, "Othr/Id")
        if len(accounts) != 1 or not (accounts[0].text or "").strip():
            raise ValueError("exactly one nonempty account identifier is required")
        account = accounts[0].text.strip()
        for entry_index, entry in enumerate(_find(statement, "Ntry"), 1):
            locator = f"/Document/BkToCstmrStmt/Stmt[{statement_index}]/Ntry[{entry_index}]"
            try:
                if _text(entry, "Sts/Cd") != "BOOK":
                    raise ValueError("only BOOK entries are supported")
                direction = _text(entry, "CdtDbtInd")
                if direction not in ("CRDT", "DBIT"):
                    raise ValueError("invalid CdtDbtInd")
                amount_node = _one(entry, "Amt")
                amount_text = (amount_node.text or "").strip()
                if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", amount_text):
                    raise ValueError("Amt must be an unsigned decimal without exponent")
                amount = Decimal(amount_text)
                if direction == "DBIT":
                    amount = amount.copy_negate()
                value_node = _one(entry, "ValDt")
                if len(value_node) != 1 or value_node[0].tag != f"{{{NAMESPACE}}}Dt":
                    raise ValueError(
                        "ValDt must contain only Dt; timestamps need a timezone policy"
                    )
                value_text = _text(value_node, "Dt")
                if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value_text):
                    raise ValueError("ValDt/Dt must be YYYY-MM-DD")
                value_date = date.fromisoformat(value_text)
                reversal = _find(entry, "RvslInd")
                if len(reversal) > 1 or (
                    reversal and (reversal[0].text or "").strip() not in ("true", "false", "1", "0")
                ):
                    raise ValueError("invalid RvslInd")
                # CdtDbtInd already describes this entry's direction, including
                # a reversal. Do not flip the amount a second time.
                records.append(
                    CanonicalRecord.create(
                        raw.raw_event_id,
                        locator,
                        ADAPTER_VERSION,
                        NORMALIZER_VERSION,
                        account=account,
                        amount=amount,
                        value_date=value_date,
                        currency=amount_node.get("Ccy", ""),
                        # Neither NtryRef nor a reference from one bundled transaction
                        # is assumed to identify the entire entry across both sources.
                        reference=None,
                    )
                )
            except ValueError as exc:
                raise ValueError(f"{locator}: {exc}") from exc
    return tuple(records)


def ingest(store: ProvenanceStore, payload: bytes, *, source: str) -> tuple[CanonicalRecord, ...]:
    raw = store.capture(payload, source=source)
    try:
        records = normalize(raw)
    except (ValueError, ET.ParseError) as exc:
        raise IngestionError(raw.raw_event_id, str(exc)) from exc
    store.save_records(records)
    return records
