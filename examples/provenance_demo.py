"""Synthetic J2 demonstration: a bundled entry, raw evidence, and replay.

Run from the repository root:
  PYTHONPATH=src python examples/provenance_demo.py --database /tmp/reconcile-demo.sqlite
"""

from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path

from reconcileflow import RuleId
from reconcileflow.adapters.camt053 import NAMESPACE, ingest
from reconcileflow.provenance import ProvenanceStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    args = parser.parse_args()
    bank = (Path(__file__).resolve().parents[1] / "tests/fixtures/camt053-booked.xml").read_bytes()
    # A second synthetic statement demonstrates a 100 = 60 + 40 comparison.
    root = ET.fromstring(bank)
    statement = root.find(f"{{{NAMESPACE}}}BkToCstmrStmt/{{{NAMESPACE}}}Stmt")
    original = statement.find(f"{{{NAMESPACE}}}Ntry")
    statement.remove(original)
    for amount in ("60.00", "40.00"):
        entry = ET.fromstring(ET.tostring(original))
        entry.find(f"{{{NAMESPACE}}}Amt").text = amount
        statement.append(entry)
    ledger_example = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    version = "j2-synthetic-demo-v1"
    with ProvenanceStore(args.database) as store:
        left = ingest(store, bank, source="synthetic-bank")
        right = ingest(store, ledger_example, source="synthetic-ledger-statement")
        run = store.reconcile(
            [r.record.id for r in left], [r.record.id for r in right], engine_version=version
        )
    with ProvenanceStore(args.database) as store:
        replay = store.replay(run.run_id, engine_version=version)
        if len(replay.result.matches) != 1 or replay.result.matches[0].rule != RuleId.M3_AGGREGATE:
            raise RuntimeError("expected the synthetic aggregate match")
        match = replay.result.matches[0]
        print(f"run_id={run.run_id}")
        print(f"rule={match.rule.name} residual={match.amount_residual} replay=verified")
        for key in (match.left_id, *match.right_ids):
            canonical, raw = store.evidence(run.run_id, key)
            print(
                f"{canonical.record.amount} {canonical.record.currency} <- {raw.source} "
                f"{canonical.source_locator} sha256={raw.payload_sha256}"
            )


if __name__ == "__main__":
    main()
