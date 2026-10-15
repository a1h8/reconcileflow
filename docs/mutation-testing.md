# Mutation testing

**Status:** implemented: `[tool.mutmut]` in `pyproject.toml`, `mutation-baseline.toml`,
`tools/mutation_check.py`, `.github/workflows/mutation.yml`.

Run locally:

```sh
pip install -e ".[dev,mutation]"
mutmut run
python tools/mutation_check.py           # OK, or what to triage
python tools/mutation_check.py --draft   # entries to fill for new survivors
```

## Why

Line and branch coverage say a line ran, not that a test would notice it
changing. Mutation testing has already found, on this codebase, what coverage
could not:

- property tests that checked every match stayed within its bounds, while none
  checked the expected match was found (a matcher that matched less passed);
- a property-test oracle importing `amount_tolerance` from the code under test,
  so a wrong formula moved the oracle with it;
- validation tests passing because construction failed for another reason;
- "equivalent" rounding mutants that were only equivalent under the default
  decimal context (docs/decimal-context.md).

Until now runs were manual, in `/tmp`, with configurations that differed from
run to run and results that recorded neither the commit nor the branch. One
analysis was done on a survivor list that a later commit had already fixed.
This spec makes a run reproducible and its result checkable.

## Scope

`rules.py`, `engine.py`, `m0/attribution.py`, `m0/stability.py`: the code that
decides matches and verdicts. Adapters, provenance and `m0/latency.py` may be
added later; each addition starts with a full triage of its survivors.

## Reproducibility

A result is a function of the code, the tests, the mutmut version and the
Hypothesis seed. All four are pinned:

- `[tool.mutmut]` in `pyproject.toml` is the only configuration;
- mutmut is pinned to an exact version in a `mutation` optional dependency;
- tests run with `--hypothesis-seed=0`. A fixed seed means property tests kill
  only what that seed generates: boundaries must also be pinned by example
  tests, which is the existing practice;
- every report starts with the commit SHA, the mutmut version and the seed.

## Baseline of accepted survivors

`mutation-baseline.toml`, at the repository root, lists every survivor that is
accepted, one entry each:

```toml
[[survivor]]
function = "reconcileflow.rules._score"
removed = "penalty += (abs(residual) / limit) * _AMOUNT_PENALTY"
added   = "penalty = (abs(residual) / limit) * _AMOUNT_PENALTY"
reason  = "first assignment after penalty = _ZERO: = and += give the same value"
assumes = "nothing"
```

Entries are keyed by function and changed line, **not** by mutmut's numeric
id (`_score__mutmut_4`): ids are positional and shift whenever the function
changes, which would silently re-label survivors.

`assumes` is mandatory and must name the condition under which the mutant is
harmless (`"PI-18: r and t are both None or both set"`), or `"nothing"` for a
true equivalent. A mutant that is harmless only under an assumption the code
does not enforce is a finding, not a baseline entry.

## Check

`tools/mutation_check.py` reads mutmut's results and the baseline and fails
when:

1. a survivor matches no baseline entry (a new gap, or a changed mutant to
   re-triage);
2. a baseline entry matches no survivor (the mutant is now killed or gone: the
   entry is stale and must be removed, so the baseline never overstates).

It also fails on timeouts and on mutants mutmut could not run, rather than
counting them as killed.

## CI

A separate workflow, `mutation.yml`, on `workflow_dispatch` and on pull
requests that touch `src/` or `tests/`. It uploads the report as an artifact.

A local run over the four modules takes on the order of minutes. If the CI run
exceeds 20 minutes, it moves to a nightly schedule against `main` instead of
gating pull requests; that is a measured decision, not one taken in advance.
