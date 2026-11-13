#!/usr/bin/env bash
# One day of the evaluation of the recipe on the probe's events
# (correlation-phase4-spec.md sections 8.14 and 8.19): 30 one-hop cells, without agent,
# with Beyla, with OBI; c20, c100, c200, c400, c800; load 0 and 4; 30,000
# requests; the probe on the hop's and fake-upstream's ports, both directions.
#
#   scripts/eval-recipe-series.sh <name-prefix> [<first-day-runs-list>]
#
# Run as your normal user, from m0-harness/, in a terminal (sudo asks once and
# is kept alive). Needs BEYLA_BIN, OBI_BIN, TRAEFIK_BIN and BPF_HEADERS. The
# repository must have no local change: the code judged is the commit's.
# With a first day's runs list, every binary's hash must match that day's
# (same agent), or the series stops. A cell already run under the prefix is
# skipped, so the script can be started again after a stop.
# The code judged is noted in <runs>/<prefix>-code.txt (commit, probe_recipe.py
# hash); with a first day's list, the commit must be that day's (spec 8.19).
# Appends "<run-dir> <agent> <c> <load>" to <runs>/<prefix>-runs.txt; then:
#   python probe_recipe.py --series <runs>/<prefix>-runs.txt
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }
[[ $(id -u) -ne 0 ]] || die "run as your normal user"
(($# >= 1 && $# <= 2)) || die "usage: $0 <name-prefix> [<first-day-runs-list>]"
PREFIX=$1 FIRST=${2:-}
[[ -f scripts/hn-propagation.sh ]] || die "run from m0-harness/"
[[ -z $FIRST || -f $FIRST ]] || die "$FIRST: no such runs list"
for v in BEYLA_BIN OBI_BIN TRAEFIK_BIN; do [[ -x ${!v:-} ]] || die "$v must point to an executable"; done
[[ -f ${BPF_HEADERS:-}/vmlinux.h && -d ${BPF_HEADERS:-}/vendor ]] || die "BPF_HEADERS needs vmlinux.h and vendor/"
TOP=$(git rev-parse --show-toplevel)
[[ -z $(git -C "$TOP" status --porcelain) ]] || die "local changes in the repository"

RUNS=$(dirname "$TOP")/runs
LIST=$RUNS/$PREFIX-runs.txt
MAX_LOAD=${MAX_LOAD:-1.5} SETTLE=${SETTLE:-120} REQUESTS=${REQUESTS:-30000}

hashes() { grep '  \./' "$1/manifest.txt" | sort; }

CODE=$RUNS/$PREFIX-code.txt
{
	echo "commit $(git -C "$TOP" rev-parse HEAD)"
	echo "probe_recipe.py $(sha256sum probe_recipe.py | cut -d' ' -f1)"
} >"$CODE.new"
if [[ -n $FIRST ]]; then
	first_code=${FIRST%-runs.txt}-code.txt
	[[ -f $first_code ]] || die "no $first_code beside the first day's list"
	cmp -s "$first_code" "$CODE.new" || die "the code differs from the first day's ($first_code)"
fi
[[ ! -f $CODE ]] || cmp -s "$CODE" "$CODE.new" || die "the code changed since this series started ($CODE)"
mv "$CODE.new" "$CODE"

sudo -v
(while sleep 60; do sudo -n -v || exit; done) &
KEEP=$!
trap 'kill $KEEP 2>/dev/null' EXIT

for agent in none beyla obi; do
	for load in 0 4; do
		for c in 20 100 200 400 800; do
			name=$PREFIX-$agent-c$c-load$load
			if grep -qs "/$name-" "$LIST"; then echo "skip $name (already run)"; continue; fi
			while awk -v m="$MAX_LOAD" '{ exit !($1 > m) }' /proc/loadavg; do
				echo "$(date +%T) load high, waiting"; sleep 30
			done
			dir=$RUNS/$name-$(date +%H%M%S)
			echo "$(date +%T) start $name -> $dir"
			agent_env=(NO_AGENT=1)
			[[ $agent == beyla ]] && agent_env=(AGENT_BIN="$BEYLA_BIN")
			[[ $agent == obi ]] && agent_env=(AGENT_BIN="$OBI_BIN")
			env "${agent_env[@]}" TRAEFIK_BIN="$TRAEFIK_BIN" RUN_DIR="$dir" \
				HOPS=1 REQUESTS="$REQUESTS" CONCURRENCY="$c" WARM_REQUESTS=$((10 * c)) TAIL=50 STRIP_N= \
				HOP_KIND=traefik CONTEXT_PROPAGATION=headers CPU_SAMPLE=1 LOAD_CORES="$load" \
				LG_HTTP1=1 LG_CONNS=1 HOP_UPSTREAM_PROTO=h1 HOP_MAX_IDLE="$c" \
				PROBE_TCP_SEQ=1 PROBE_BOTH=1 FU_STREAM_OFFSETS=1 BPF_HEADERS="$BPF_HEADERS" \
				scripts/hn-propagation.sh || die "$name failed: $dir (fix, then start the series again)"
			if [[ -n $FIRST ]]; then
				ref=$(awk -v a="$agent" '$2 == a {print $1; exit}' "$FIRST")
				[[ -n $ref ]] || die "no $agent run in $FIRST"
				diff <(hashes "$ref") <(hashes "$dir") || die "$name: binaries differ from $ref"
			fi
			echo "$dir $agent $c $load" >>"$LIST"
			sleep "$SETTLE"
		done
	done
done
echo "$(date +%T) series done; list: $LIST"
