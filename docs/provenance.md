# J2: source provenance and local replay

The matching engine remains a pure function. `reconcileflow.provenance` is an
optional SQLite-backed boundary around it: capture bytes, normalise with an
explicit adapter, persist inputs, then reconcile and retain the result.
No additional runtime package is required.

## Run the synthetic demonstration

From the repository root, after installing the development environment:

```bash
PYTHONPATH=src .venv/bin/python examples/provenance_demo.py \
  --database /tmp/reconcile-demo.sqlite
```

The example reads a synthetic CAMT fixture and constructs another synthetic
statement with two entries. It demonstrates **100 EUR = 60 EUR + 40 EUR** via
`M3_AGGREGATE`, closes the database, reopens it, verifies replay, and prints the
source locator and payload hash for all three inputs. It does not introduce a
ledger adapter or claim to use real bank data. Running it again reuses the same
raw events, canonical records and run identity.

## Persistence contract

| Object | Identity and contents |
|---|---|
| `RawEvent` | SHA-256 identity of source plus payload hash; original bytes; source; first capture time in UTC |
| `CanonicalRecord` | Content identity covering raw event, locator, adapter version, normaliser version and all normalised fields |
| Reconciliation run | Content identity covering sorted input IDs, caller-declared engine build, complete tolerance settings, ruleset, matches, rejects and counters |

A raw event is committed **before parsing** in a separate transaction. An invalid
XML document or unsupported entry raises `IngestionError` with the durable
`raw_event_id`. No partial canonical batch is persisted if normalisation fails.

The API never updates or deletes stored evidence. Repeated identical payloads
from the same source return the first capture; identical bytes from another
source have a different identity. This deduplicates payload delivery, not business
transactions across different statements. Receipt-attempt history is not captured.

Reading evidence checks payload hashes and canonical/run content identities.
These checks detect content corruption; they are not signed evidence, external
anchoring, access-control enforcement or protection against someone rewriting the
entire database. SQLite is a local store, not the streaming state store of J3.

## CAMT adapter contract

The adapter is pinned to `camt.053.001.08`, whose message definition is
`BankToCustomerStatementV08`. The ISO definition distinguishes entry amount,
credit/debit direction and reversal indication; the direction already describes
the current entry, including a reversal. [ISO 20022 message definition report](https://www.iso20022.org/sites/default/files/documents/messages/mdr_part_2/ISO20022_MDRPart2_BankToCustomerCashManagement_2018_2019_v1_0.pdf)

The following are **our supported profile and normalisation choices**, not a
claim that other valid CAMT documents are invalid:

- One canonical record per `Stmt/Ntry`. Multiple statements and entries are
  supported. Transaction details within a bundled entry are retained in raw XML
  and are not expanded or counted again.
- Explicit `BOOK` status, unsigned decimal entry amount, three-letter uppercase
  currency, and `ValDt/Dt` are required. The adapter does not validate membership
  in the ISO currency register or per-currency minor units; it preserves the
  exact decimal amount without rounding or currency conversion.
- `CRDT` becomes positive and `DBIT` negative, from the account holder's
  perspective. A reversal does not invert that sign again. Both reconciliation
  sources must use this same convention.
- Value dates must be dates. Missing value dates and `DtTm` are rejected instead
  of silently substituting booking dates or selecting an implicit timezone.
- The account comes from `Acct/Id/IBAN` or `Acct/Id/Othr/Id`, with surrounding
  whitespace removed. The caller must arrange a shared account-identity convention
  across sources; the adapter does not infer account equivalence.
- `Record.reference` is left unset. Neither an entry reference local to a bank nor
  an arbitrary transaction inside a bundle is assumed to be a shared unique
  transaction reference. A future reference policy needs its own normaliser version.
- Other versions, non-booked entries, missing required profile fields, duplicate
  consumed scalar fields and DTD declarations fail explicitly. Unconsumed metadata
  remains in the raw payload. This is not full XSD validation, balance validation,
  bank-specific conformance or exhaustive support for all valid CAMT.053.001.08.

The source locator is an indexed entry path, such as
`/Document/BkToCstmrStmt/Stmt[1]/Ntry[2]`, interpreted in the adapter's declared XML
namespace. It identifies an element in the preserved document, not a reconstructed
XML fragment or a byte offset.

## Using the API

```python
from pathlib import Path
from decimal import Decimal
from reconcileflow import Tolerance
from reconcileflow.adapters.camt053 import ingest
from reconcileflow.provenance import ProvenanceStore

with ProvenanceStore("/tmp/reconciliation.sqlite") as store:
    bank = ingest(store, Path("bank.xml").read_bytes(), source="bank-feed")
    comparison = ingest(store, Path("comparison.xml").read_bytes(), source="comparison-feed")
    run = store.reconcile(
        [item.record.id for item in bank],
        [item.record.id for item in comparison],
        engine_version="<actual release or build revision>",
        tolerance=Tolerance(amount_abs=Decimal("0.05"), date_days=3),
    )
    match = run.result.matches[0]  # only if a match exists
    canonical, raw = store.evidence(run.run_id, match.left_id)
    original_bytes = raw.payload
```

`engine_version` is mandatory and must name the actual build the caller uses.
`replay(run_id, engine_version=...)` requires the same label, recomputes from the
persisted canonical inputs, and verifies the complete result. It does not retrieve
or install historical code: archiving and running the right build is the caller's
responsibility. Adapter replay is a separate operation; changed adapter/normaliser
versions create new canonical identities instead of rewriting existing inputs.

Input IDs must be unique per side. The store refuses to match a source record
against itself. Every record in a stored run can be traced back, including those
that remain unmatched. The existing engine's right-side unmatched outputs are
still counters, not a new symmetric break taxonomy.

## Currency compatibility

`Record` gains an optional trailing `currency` field. Existing positional calls
remain valid, and records without a currency retain the old account-only block.
Explicit-currency records use `(account, currency)` blocks. An unknown currency
does not match an explicit one, and different currencies cannot match even when
references agree. This changes blocking, not the M0–M3 rule order or scoring.
The engine version must distinguish this behavior from an older build; the
ruleset fingerprint alone is not a version of the engine implementation.

## Next reconciliation work

Before declaring a general ingestion service, extend the supported profiles only
against concrete examples and declared reference/timezone conventions. The next
engine work can cover duplicate identifiers, symmetric unmatched explanations,
ambiguity policy and complete decision justifications. J3 then adds event-time
horizons and append-only supersession; it does not use TTL to decide outcomes.
