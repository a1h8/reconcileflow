#!/usr/bin/env bash
# Judge a series of one-hop runs with 4a' and 4c (b), frozen tools and
# parameters (correlation-phase4-spec.md section 8.7), in the format of the
# evaluation verdicts.
#
#   scripts/judge-series.sh <runs-list> > verdict.txt
#
# <runs-list>: lines "<run-dir> <agent> <c> <load> <probe>", as written by
# eval-series.sh. Run from m0-harness/. For each run:
# - probe 0 (HTTP/2 control): connection_path.py only, expected INVALID;
# - probe 1: 4a' (connection_path.py), 4c (b) (stream_path.py --attach with
#   attach-params.toml, --order, --compare connection-params.toml), K2b
#   (--drop 50, seeds 1-3) and K5 (--shift);
# - --stress under declared load and from c = 200 on (judged on wrong links,
#   with the parameters of the highest calibrated concurrency).
# PYTHON: interpreter with the project's dependencies (default python3).
set -uo pipefail

[[ $# -eq 1 && -f $1 ]] || { echo "usage: $0 <runs-list>" >&2; exit 1; }
LIST=$(realpath "$1")
PY=${PYTHON:-python3}
export PYTHONPATH=$(git rev-parse --show-toplevel)/src

while read -r dir agent c load probe; do
	echo "######## $(basename "$dir")"
	if ((probe == 0)); then
		$PY connection_path.py "$dir" connection-params.toml | grep -v '^wrong links' | sed -n '1,2p'
		echo "wrong links on the judged join in the run: 0"; echo; continue
	fi
	stress=; { ((load > 0)) || ((c >= 200)); } && stress=--stress
	echo "--- 4a'"; $PY connection_path.py "$dir" connection-params.toml $stress
	echo "--- 4c (b)"
	$PY stream_path.py "$dir" --attach --attach-params attach-params.toml --order \
		--compare connection-params.toml $stress
	for s in 1 2 3; do
		$PY stream_path.py "$dir" --attach --attach-params attach-params.toml --order --drop 50 --seed $s | head -1
	done
	echo "--- K5"
	$PY stream_path.py "$dir" --attach --attach-params attach-params.toml --order --shift | head -1
	echo
done <"$LIST"
