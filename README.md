# reconcileflow

**A reconciliation engine: declared rules, typed breaks, replayable decisions.**

Match two sources of records that ought to agree — a provider feed and an
internal state, an export and the system it came from — then explain every
discrepancy and be able to replay every decision.

---

## Status

**J1 matching semantics implemented; J2 local provenance now available through a
first adapter, for a narrow CAMT.053.001.08 profile; J3 stream semantics
implemented in memory (`reconcileflow.stream`).** The engine stays in memory
and side-effect free;
the optional provenance layer stores source bytes, normalised inputs and results
in SQLite. No streaming runtime or real customer data. See the
[provenance contract and demo](docs/provenance.md) for supported input and limitations.

## What it is

- **Declared rules, ordered from strictest to most permissive.** They are
  readable, versioned, and defensible to an auditor — not weights learned by a
  model.
- **One-to-many matching from the interface up.** A bundled entry is not an
  edge case bolted on later: `Match.right_ids` has been a tuple since the first
  line of code.
- **Typed breaks.** An unmatched record does not vanish — it comes out with an
  enumerated reason. That is the primary deliverable, not a by-product.
- **Replayable decisions.** Every result carries the ruleset fingerprint, and
  the engine is invariant under permutation of its inputs — verified by a
  property test, not assumed.

## What it is not

Neither a streaming engine, nor a probabilistic entity-resolution tool, nor a
platform. The engine writes nothing, logs nothing, and knows nothing about the
network: it takes two collections and returns decisions, justifications and
counters. The caller decides the rest.

## Example

```python
from datetime import date
from decimal import Decimal
from reconcileflow import Record, Tolerance, reconcile

left = [
    Record("A1", "ACC-1", Decimal("1250.00"), date(2024, 3, 11)),
    Record("A2", "ACC-1", Decimal("89.90"), date(2024, 3, 12), reference="INV-4471"),
]
right = [
    Record("B1", "ACC-1", Decimal("800.00"), date(2024, 3, 11)),
    Record("B2", "ACC-1", Decimal("450.00"), date(2024, 3, 12)),
    Record("B3", "ACC-1", Decimal("91.40"), date(2024, 3, 10), reference="INV-4471"),
]

result = reconcile(left, right, Tolerance(amount_abs=Decimal("0.50"), date_days=3))
```

```
A2 -> B3        M0_REFERENCE     residual=-1.50
A1 -> B1,B2     M3_AGGREGATE     residual=0.00
ruleset_id = 514101369862cbf6
```

Two things to notice in that output:

- **`A1` is matched against `B1 + B2`** — 1250 = 800 + 450. That is `M3`,
  aggregation.
- **`A2` is matched to `B3` despite a 1.50 discrepancy.** The reference
  `INV-4471` is identical: these are the same operation. An amount discrepancy
  is not a reason to leave them unmatched — it is an **"amount discrepancy"
  break**, and you must match first in order to qualify it. The residual is
  reported, not hidden.

## The rules

| Rule | Criterion |
|---|---|
| `M0_REFERENCE` | identical transaction reference (indifferent to amount, reports the residual) |
| `M1_EXACT` | exactly equal amount, date within the window |
| `M2_TOLERANT` | amount within tolerance (absolute + relative), date within the window |
| `M3_AGGREGATE` | one on the left against *n* on the right, sum within tolerance |

A stricter rule always beats a more permissive one. When several candidates
compete, resolution is greedy by score with deterministic tie-breaking by
identifier — never by arrival order.

## Rejection reasons

`NO_CANDIDATE` · `DATE_OUT_OF_WINDOW` · `AMOUNT_OUT_OF_TOLERANCE` ·
`LOST_TO_BETTER_CANDIDATE` · `AGGREGATE_NOT_FOUND` · `BLOCK_TOO_LARGE`

Enumerated, never free text: a reason must be joinable in SQL, translatable,
and stable across versions. They are also mutually exclusive — a rejected pair
carries exactly one reason, which is what makes aggregating by reason a valid
root-cause breakdown rather than a double count. That invariant is enforced by
a property test.

`BLOCK_TOO_LARGE` deserves a note: past a bound, enumerating `M3`'s subsets
explodes. The engine does not guess — it defers to manual review. **A false
match costs vastly more than a reported break.**

## Install and test

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest      # engine, property, provenance and harness tests
.venv/bin/ruff check .
```

The property that matters: **the result does not depend on input order.**
Without it, replaying a decision proves nothing and no audit is defensible. It
is checked over 200 generated inputs by `hypothesis`.

## Positioning, and what already exists

This project does not invent record matching. The open-source ecosystem is real
and mature:

- [Splink](https://github.com/moj-analytical-services/splink) — probabilistic
  Fellegi-Sunter linkage, DuckDB/Spark backends, very solid
- [Zingg](https://github.com/zinggAI/zingg),
  [dedupe](https://github.com/dedupeio/dedupe) — entity resolution through
  learning

These answer "do these two records denote the same entity?", in batch, with
learned weights. Closer to this project, several tools reconcile two sets of
records with **declared** rules:

- [Blnk](https://github.com/blnkfinance/blnk) — an open-source ledger whose
  reconciliation engine takes declared matching rules with an allowed drift,
  one-to-one, one-to-many and many-to-one
- [reconcile-py](https://pypi.org/project/reconcile-py/) — deterministic,
  explainable matching with amount and date tolerances, one-to-many by subset
  search, near ties flagged for review
- [Reconcile.NET](https://www.nuget.org/packages/Reconcile.Net) — successive
  passes from exact reference to looser keys, one-to-many on a supplied group key
- [reconPy](https://pypi.org/project/reconPy/) — scored reconciliation between
  dataframes, one-to-one

So declared rules and one-to-many matching are not what sets this project apart.
What does, as far as these tools document it, stated narrowly: **an enumerated
reason for every unmatched record** (not only an explanation for the matched
ones); **stream semantics** — provisional, final and superseded decisions, a
replayable journal, finality decided per group of competing records; and
**provenance** back to the bytes received. Their documentation describes batch
runs only. A substantial part of the need is covered by one of them plus a
scheduled job — that is acknowledged, not hidden.

## Design notes

The decisions below are the load-bearing ones. Each is a constraint the code
enforces, not a preference.

**The audit trail is a return value, never a log.** `reconcile()` returns
justifications and counters; it never writes, logs or exports. Two consequences:
the engine is testable by asserting on the justification rather than on log
output, and the shape of the audit trail is fixed by the *interface*. If the
implementation is ever replaced — a compiled core, a different algorithm — the
trail does not change shape. An audit trail is a contract; contracts belong to
interfaces, not to implementations.

**Reason codes are enumerated and mutually exclusive.** Free text cannot be
joined in SQL, translated, or kept stable across versions. Exclusivity matters
more: a rejected pair carries exactly one reason, which is what makes
`GROUP BY reason` a root-cause breakdown instead of a double count. Aggregated
over time, the rejection taxonomy *is* the root-cause taxonomy — a query, not an
inference.

**Determinism is a correctness property, not an optimisation.** Blocks are
sorted by key, records by identifier, candidates by a total order that breaks
ties on identifiers. Without permutation invariance, re-running the engine over
the same data proves nothing, and no decision is defensible.

**Amounts are `Decimal`, never `float`.** Binary rounding manufactures
cent-level discrepancies — exactly the breaks the engine exists to detect.

**`M0` ignores the amount on purpose.** Two lines carrying the same transaction
reference *are* the same operation. The discrepancy is the finding, not a reason
to leave them unmatched — and you must match first in order to qualify it.

**Blocking is deliberately coarser than any rule.** A block finer than a rule
discards true matches before the rules ever run, and those false breaks are
invisible: nothing reports a comparison that never happened.

**`M3` refuses rather than guesses.** Subset-sum with tolerance is NP-hard;
bounds on subset size and block size make it tractable. Past those bounds the
engine emits `BLOCK_TOO_LARGE` and defers to a human. In reconciliation, "I do
not know, please look" is a legitimate and expected output.

## What you provide

The project provides the engine, the matching semantics and the audit trail.
**The rules are yours** — amount tolerances, date windows, reference
conventions, acceptable aggregate size.

This is deliberate: no two institutions share conventions, and an engine that
imposed its own would be unusable anywhere else. Rules are versioned parameters
(`Tolerance`, and the `ruleset_id` attached to every decision), not constants
buried in the code.

## Roadmap

Every milestone carries a verifiable completion criterion. Without one, nothing
is ever finished.

**J1 — Semantics** · *done*
`M0`–`M3` matching in memory, deterministic resolution, typed breaks.
*Done when:* permutation invariance of the inputs is verified by a property
test. ✔

**J2 — Provenance** · *local entry-level slice implemented*
`RawEvent` persisted **before** normalisation, payload intact;
`CanonicalRecord` carrying `adapter_version` and `normalizer_version`. A single
adapter so far: **CAMT.053**, chosen for its public samples and a format that does
not move every six months. The engine itself does not depend on it: an adapter
turns a source's bytes into `CanonicalRecord`s, each carrying the `Record` the
engine matches. The [adapter contract](docs/provenance.md) limits this first
implementation to booked entry-level CAMT.053.001.08 records with explicit value
dates; it does not claim general CAMT support or full XSD validation. The adapter
catalogue is not planned.
*Done when:* a matching decision can be traced back to the bytes received from
the provider, with no gap in the chain.

**J3 — Streaming** · *in-memory semantics implemented*
State is partitioned by reconciliation key. Event time and watermarks drive
business completeness; **TTL is a memory safety net and must never decide
an outcome** — otherwise a memory setting silently changes reconciliation
results. The watermark follows the **slower** of the two sources, minus a declared
tolerated lateness: a fast source must not close the horizons of a slow one. A
source with nothing to send declares its progress in the journal, not through a
wall-clock timer, so a replay sees it at the same place.

Decisions are append-only, versioned and deterministic. A break stays
`PROVISIONAL` until its horizon closes. A match becomes `FINAL` only when the
rule guarantees monotonicity, or when the horizon closes — and in practice only
`M0` is monotone, under an explicitly declared reference-uniqueness assumption.
J3 does not offer that declaration yet: every match waits for its horizon.
Greedy resolution means a later arrival can always contend for `M1` and `M2`,
and `M3` is never monotone since a new record may form a better combination. So
"final at horizon close" is the rule, not the exception.

**Finality is decided per component, not per record.** Greedy resolution carries
a change along a chain: a new arrival takes one record's counterpart, which then
takes another's. Two open records are linked when they are within the date
window or share a reference; a connected component becomes `FINAL` as a whole,
once the watermark has passed every member's horizon (value date + window, or +
a declared reference horizon for a record carrying a reference, since `M0`
ignores dates). A component spanning more days than a declared bound is cut
instead: its decisions past their own horizon become final, marked `forced`.
Without a bound the result is exact, but a dense stream may never become final.

A late arrival never rewrites an earlier decision: it emits a new one that
explicitly supersedes it. A final decision withdraws its records, so a later arrival
cannot contend for them: the final state can differ from matching the whole
journal at once. That is the price of finality, and a test shows it. An arrival
under the watermark is a typed `LATE` break; one carrying the reference of an
already final record is a typed `REFERENCE_AFTER_FINAL` break, rather than a
silent search that finds nothing. A break's reasons count only pairs within the
date window: at finality those are all known.

Minimum decision contract:

| | |
|---|---|
| `decision_id` · `chain_version` | identity and rank in the supersession chain |
| `status` | `PROVISIONAL` \| `FINAL` \| `SUPERSEDED` |
| `decision_type` | `MATCH` \| `BREAK` |
| `input_ids` · `reason` | what it decided, and why |
| `ruleset_id` · `engine_version` | replayability |
| `window_end` · `decided_at` · `final_at` | business horizon, decision time, finalisation |
| `supersedes` | the decision this one replaces |
| `forced` | final because its component was cut, not because its horizon closed |

Three distinct clocks, not two: when the business horizon closed, when the
system decided, when the decision became final. Collapsing them loses the ability
to answer "what did we know, and when?".

The distinction that carries the whole design: **an append-only history is not
the same thing as decisions that are never revised.** The history is immutable;
the *effective state* evolves by supersession and is a derived read model
(`status != SUPERSEDED`) — never an in-place update.

*Done when:* replaying the same journal, duplicates included, yields the same
decisions, identifiers included (pinned as bytes by a test); replaying it in a
different order, as long as no record arrives beyond the tolerated lateness,
yields the same final decisions and the same final effective state (what was
provisional depends on what was known when, by design) — **under the declared
assumption that two records with the same reference are at most the reference
horizon apart, for decisions that are not `forced`, and while no block exceeds
`max_block_candidates`**; a counterpart arriving within the lateness replaces a provisional
break with a match without deleting anything; and a record arriving beyond it is
a typed `LATE` break that never alters a final decision.

**J4 — Observation**
Counters exposed as time series, aggregated by `reason × provider × day`. The
rejection taxonomy *is* the root-cause taxonomy: a query, not an inference.
*Done when:* a drop in the `M0` match rate for one provider is detected without
anyone opening a ticket.

**J5 — Performance**
Profile first. Extract the hot path **if and only if** measurement points to
it — the hot spot may be matching, state, or deserialisation, and those are
three opposite refactorings.
*Done when:* a reproducible, versioned benchmark exists. Not before.

**Out of scope**, and staying there: multi-node distribution, end-to-end
exactly-once with two-phase commit, a SQL surface, live rescaling, an adapter
catalogue.

## Licence

[Apache-2.0](LICENSE) — see also [NOTICE](NOTICE).

Apache rather than MIT for the patent clause (§3): it explicitly grants
contributors' patent rights and terminates those rights for anyone bringing a
patent infringement action. In a domain where proprietary vendors hold patent
portfolios on reconciliation methods, that is not a theoretical precaution.
