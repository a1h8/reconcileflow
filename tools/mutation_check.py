"""Compare a mutmut run against the baseline of accepted survivors.

Run from the repository root after ``mutmut run`` (docs/mutation-testing.md).
Fails when a survivor is not in the baseline, when a baseline entry no longer
survives, or when a mutant ended in anything other than killed or survived.

    python tools/mutation_check.py           # check
    python tools/mutation_check.py --draft   # print baseline entries to triage
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from collections import defaultdict
from importlib.metadata import version
from pathlib import Path

BASELINE = Path("mutation-baseline.toml")
KILLED = {1, 3, 37}  # tests failed, pytest internal error, caught by type check
SURVIVED = 0


def function_of(mutant: str) -> str:
    """``pkg.mod.x__score__mutmut_4`` -> ``pkg.mod._score``;
    ``pkg.mod.xǁMetricsǁ_validate__mutmut_2`` -> ``pkg.mod.Metrics._validate``.

    Positional ids shift whenever a function changes, so the baseline is keyed
    by function and changed lines instead.
    """
    module, _, name = mutant.rpartition(".")
    name = re.sub(r"__mutmut_\d+$", "", name)
    if name.startswith("xǁ"):
        _, cls, method = name.split("ǁ")
        return f"{module}.{cls}.{method}"
    return f"{module}.{name.removeprefix('x_')}"


def changed_lines(mutant: str, source: Path) -> tuple[str, str]:
    # In-process rather than one `mutmut show` per survivor: mutmut is pinned,
    # so this internal helper is as stable as its CLI output.
    from mutmut.mutation.diff_apply import get_diff_for_mutant

    removed, added = [], []
    for line in get_diff_for_mutant(mutant, path=source).splitlines():
        if line.startswith(("---", "+++", "@@")):
            continue
        if line.startswith("-"):
            removed.append(line[1:].strip())
        elif line.startswith("+"):
            added.append(line[1:].strip())
    return "\n".join(removed), "\n".join(added)


def exit_codes(root: Path) -> dict[str, tuple[int | None, Path]]:
    """Exit code and source file of every mutant, from mutmut's .meta files."""
    codes: dict[str, tuple[int | None, Path]] = {}
    for meta in sorted(root.glob("mutants/**/*.py.meta")):
        source = meta.relative_to(root / "mutants").with_suffix("")
        for mutant, code in json.loads(meta.read_text())["exit_code_by_key"].items():
            codes[mutant] = (code, source)
    return codes


def header() -> str:
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    ).stdout.strip()
    pyproject = tomllib.loads(Path("pyproject.toml").read_text())
    args = pyproject["tool"]["mutmut"]["pytest_add_cli_args"]
    seed = next((a.split("=", 1)[1] for a in args if a.startswith("--hypothesis-seed=")), "unset")
    return f"commit {sha or 'unknown'} · mutmut {version('mutmut')} · hypothesis seed {seed}"


def toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)  # a JSON string is a valid TOML basic string


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--draft", action="store_true", help="print entries for unlisted survivors")
    draft = parser.parse_args().draft

    print(header())
    codes = exit_codes(Path("."))
    if not codes:
        print("no mutmut results under mutants/: run `mutmut run` first")
        return 1

    unrun = {m: c for m, (c, _) in codes.items() if c not in KILLED and c != SURVIVED}
    survivors = {
        m: (function_of(m), *changed_lines(m, source))
        for m, (c, source) in codes.items()
        if c == SURVIVED
    }
    killed = len(codes) - len(unrun) - len(survivors)
    print(f"{len(codes)} mutants: {killed} killed, {len(survivors)} survived, {len(unrun)} not run")

    entries = tomllib.loads(BASELINE.read_text()).get("survivor", []) if BASELINE.exists() else []
    matched: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for mutant, key in survivors.items():
        matched[key].append(mutant)

    failed = False
    for mutant, code in sorted(unrun.items()):
        failed = True
        print(f"NOT RUN   {mutant} (exit code {code})")

    for entry in entries:
        key = (entry["function"], entry["removed"], entry["added"])
        if not entry.get("reason") or not entry.get("assumes"):
            failed = True
            print(f"INCOMPLETE {entry['function']}: reason and assumes are both required")
        if key not in matched:
            failed = True
            print(f"STALE     {entry['function']}: -{entry['removed']!r} +{entry['added']!r}")

    listed = {(e["function"], e["removed"], e["added"]) for e in entries}
    for key, mutants in sorted(matched.items()):
        if key in listed:
            continue
        failed = True
        function, removed, added = key
        print(f"NEW       {', '.join(sorted(mutants))}")
        print(f"          -{removed!r}\n          +{added!r}")
        if draft:
            print(
                "\n[[survivor]]\n"
                f"function = {toml_string(function)}\n"
                f"removed = {toml_string(removed)}\n"
                f"added = {toml_string(added)}\n"
                'reason = ""\n'
                'assumes = ""\n'
            )

    print("FAIL" if failed else "OK: survivors and baseline entries match one to one")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
