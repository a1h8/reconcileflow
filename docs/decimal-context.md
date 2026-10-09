# Decimal context

**Status:** implemented in `reconcileflow.numeric`.

## Problem

Python's `decimal` arithmetic reads two settings from the *caller's* thread-local
context: the precision (`prec`) and, wherever no rounding mode is passed
explicitly, the rounding mode. Today reconcileflow inherits both. Its outputs
therefore depend on state it does not own:

- `stability._achievable` calls `quantize(quantum)` without a rounding mode.
  Under `localcontext(rounding=ROUND_DOWN)`, `Metrics(d_cov=0.25, n=10002, ...)`
  is rejected (`2500 / 10002 = 0.24995...` truncates to `0.24`), while the
  default context accepts it. A validation verdict flips on ambient state.
- `rules._score` and `Metrics.d_cov_count` pass `rounding=ROUND_HALF_EVEN`
  explicitly, which pins the final rounding mode but not the precision of the
  divisions before it. With `prec=3`, `reconcile()` raises `InvalidOperation`
  in `_score`.

Scores enter `result_hash` and decisions are meant to be replayable byte for
byte. A result that depends on the caller's context is not replayable: the
context is not part of the recorded inputs.

Passing `rounding=` at each call site was the first answer (mutation testing
showed those arguments were load-bearing, not redundant). It does not hold as a
rule: it covers neither precision nor the call sites someone forgets, and an
absent argument is invisible to mutation testing.

## Decision

Every public entry point computes under one fixed context, owned by the
library:

```python
ENGINE_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN)  # traps: Python defaults
```

`prec=28` and `ROUND_HALF_EVEN` are Python's own defaults, so results under the
default context do not change: this fixes behaviour under *other* contexts,
it does not move any published figure.

Entry points, entered with `localcontext(ENGINE_CONTEXT)`:

| Module | Entry point |
|---|---|
| `engine` | `reconcile` |
| `m0.attribution` | `Thresholds.__post_init__`, `Metrics.__post_init__`, `gate0`, `decide` |
| `m0.stability` | `Repetition.__post_init__`, `assess` |

Each entry point opens the context and delegates to an undecorated private
function (`reconcile` → `_reconcile`, `__post_init__` → `_validate`, ...).
Not a decorator: mutmut skips decorated functions entirely, so a decorator
would silently take every entry point out of mutation testing.

Private helpers (`_score`, `_achievable`, `_q`, ...) do not open their own
context: they run inside the one their entry point opened. Explicit `rounding=`
arguments already in the code stay; they are harmless and document intent.

`ENGINE_CONTEXT` lives in one module and is imported, never redefined.

## Out of scope

- Inputs with more than 28 significant digits. They are rounded by the first
  arithmetic operation under `prec=28`. Validating input precision is a
  separate decision.
- `m0.latency`, which works in `float`.
- Recording the context in `ruleset_id`. The context is fixed, not a
  parameter; if it ever becomes one, it must enter the fingerprint.

## Verification

A metamorphic property test: for generated inputs, each entry point returns the
same result (or raises the same error) under the default context and under
contexts with a different rounding mode (`ROUND_DOWN`, `ROUND_HALF_UP`,
`ROUND_CEILING`) and a low precision (`prec=3`, which today makes `reconcile`
raise as soon as a tolerant match is scored). For `reconcile`, "same result"
means the same `result_hash`.

Before the change this test must fail on `_achievable` (rounding) and on
`reconcile` (precision); after it, pass. The three point tests that pinned the
explicit `rounding=` arguments remain as named examples.
