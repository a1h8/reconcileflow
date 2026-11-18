#!/usr/bin/env bash
# Methods that run in another environment (numpy, scipy, scikit-learn; for
# TraceWeaver, its own pinned versions): export the lives of every cell with
# our Python, then run the method on each, offline, N at a time.
#
#   OUT=<dir> PY=<our python> TW=<traceweaver dir> evidence/run_elsewhere.sh \
#       export|crosstrace|traceweaver <runs-list>...       (from m0-harness)
#
# TW holds venv/ (their requirements, gurobipy 14.0.0) and src/ (their
# repository at ee8dff3). Lives go to OUT/lives, links to OUT/links.
set -euo pipefail
die() { echo "error: $*" >&2; exit 1; }
[[ -n ${OUT:-} && -n ${PY:-} && -n ${TW:-} ]] || die "OUT, PY and TW must be set"
step=$1; shift
mkdir -p "$OUT/lives" "$OUT/links"
case $step in
export)
	cat "$@" | awk '{print $1}' | while read -r run; do
		name=$(basename "$run")
		[[ -s $OUT/lives/$name.json ]] || PYTHONPATH=../src:. "$PY" -c "
import probe_compare as pc; from pathlib import Path
pc.export_lives(Path('$run/n1'), Path('$OUT/lives/$name.json'))"
	done ;;
crosstrace)
	ls "$OUT"/lives/*.json | xargs -P "${N:-3}" -I{} bash -c '
		n=$(basename {} .json); out="$0/links/$n.crosstrace.json"
		[[ -s $out ]] || /usr/bin/time -f "%e s %M kB" unshare -rn "$1/venv/bin/python" crosstrace_like.py {} "$out" \
			2>"$0/links/$n.crosstrace.time" >/dev/null' "$OUT" "$TW" ;;
traceweaver)
	ls "$OUT"/lives/*.json | xargs -P "${N:-3}" -I{} bash -c '
		n=$(basename {} .json); out="$0/links/$n.traceweaver.json"
		[[ -s $out ]] || timeout 4h /usr/bin/time -f "%e s %M kB" unshare -rn "$1/venv/bin/python" -u tw_link.py \
			"$1/src/src/trace_reconstructor/ports/python" {} "$out" >"$0/links/$n.traceweaver.log" \
			2>"$0/links/$n.traceweaver.time" || echo "$n: stopped (exit $?)" >>"$0/links/traceweaver-stopped.txt"' "$OUT" "$TW" ;;
*) die "step must be export, crosstrace or traceweaver" ;;
esac
