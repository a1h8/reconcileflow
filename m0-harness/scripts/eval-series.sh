#!/usr/bin/env bash
# Run a series of one-hop cells with the same commit and binaries as a reference
# run (phase 4 evaluations, correlation-phase4-spec.md section 8.7).
#
#   scripts/eval-series.sh <reference-run-dir> <name-prefix> <cell>...
#   cell = c<concurrency>-load<cores>-<h1|h2>, e.g. c400-load0-h1
#
# Run as your normal user, from m0-harness/, in a terminal (sudo asks once and
# is kept alive for the whole series). For each cell:
# - waits until the 1-minute load average is <= MAX_LOAD (default 1.5);
# - runs hn-propagation.sh: 1 hop, REQUESTS requests (default 30000), HTTP/1.1
#   with the stream probe (h1) or the HTTP/2 control without it (h2);
# - checks that every binary it built has the hash recorded in the reference
#   run's manifest (probe binaries only for h1), and stops on any difference;
# - appends "<run-dir> <agent> <c> <load> <probe>" to <runs>/<prefix>-runs.txt.
# A cell already run successfully under the same prefix is skipped, so the
# script can be started again after a stop. It stops at the first failure.
#
# Environment: BPF_HEADERS (required for h1: vmlinux.h and vendor/), MAX_LOAD,
# SETTLE (seconds after each cell, default 120), REQUESTS.
# The commit may differ from the reference's only by these scripts and docs.
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }
[[ $(id -u) -ne 0 ]] || die "run as your normal user"
(($# >= 3)) || die "usage: $0 <reference-run-dir> <name-prefix> <cell>..."
REF=$(realpath "$1") PREFIX=$2; shift 2

RUNS=$(dirname "$(git rev-parse --show-toplevel)")/runs
LIST=$RUNS/$PREFIX-runs.txt
MAX_LOAD=${MAX_LOAD:-1.5} SETTLE=${SETTLE:-120} REQUESTS=${REQUESTS:-30000}
[[ -f $REF/manifest.txt && -x $REF/bin/agent && -x $REF/bin/traefik ]] || die "$REF: not a run directory"
[[ -f scripts/hn-propagation.sh ]] || die "run from m0-harness/"

COMMIT=$(awk '/^commit:/ {print $2}' "$REF/manifest.txt")
# Same code as the reference: nothing changed since its commit but these two
# scripts and docs, and no local change.
TOP=$(git rev-parse --show-toplevel)
git -C "$TOP" diff --quiet "$COMMIT" HEAD -- . ':(exclude)docs' \
	':(exclude)m0-harness/scripts/eval-series.sh' ':(exclude)m0-harness/scripts/judge-series.sh' ||
	die "code changed since the reference commit $COMMIT"
[[ -z $(git -C "$TOP" status --porcelain) ]] || die "local changes in the repository"
AGENT=$(awk '/^agent:/ {print $2}' "$REF/manifest.txt")

for cell in "$@"; do
	[[ $cell =~ ^c([0-9]+)-load([0-9]+)-(h1|h2)$ ]] || die "bad cell '$cell'"
	if [[ ${BASH_REMATCH[3]} == h1 ]]; then
		[[ -f ${BPF_HEADERS:-}/vmlinux.h && -d ${BPF_HEADERS:-}/vendor ]] ||
			die "h1 cells need BPF_HEADERS with vmlinux.h and vendor/"
	fi
done
export BPF_HEADERS=${BPF_HEADERS:-}

BIN=$(mktemp -d)
cp "$REF/bin/agent" "$BIN/agent"; cp "$REF/bin/traefik" "$BIN/traefik"

sudo -v
( while sleep 60; do sudo -n -v || exit; done ) &
KEEP=$!
trap 'kill $KEEP 2>/dev/null; rm -rf "$BIN"' EXIT

wait_free() {
	local l
	while :; do
		l=$(cut -d' ' -f1 /proc/loadavg)
		awk -v l="$l" -v m="$MAX_LOAD" 'BEGIN {exit !(l <= m)}' && return
		echo "$(date +%T) load $l > $MAX_LOAD, waiting"; sleep 30
	done
}

done_already() {
	local d
	for d in "$RUNS/$PREFIX-$AGENT-$1"-*/; do
		[[ -f $d/cells.txt ]] && grep -q 'load-gen exit=0' "$d/cells.txt" && return 0
	done
	return 1
}

hashes() {  # binary hashes of a manifest, without the probe's for h2
	grep '  \./' "$1" | { (($2)) && cat || grep -v probe; }
}

for cell in "$@"; do
	[[ $cell =~ ^c([0-9]+)-load([0-9]+)-(h1|h2)$ ]]
	c=${BASH_REMATCH[1]} load=${BASH_REMATCH[2]} proto=${BASH_REMATCH[3]}
	name=$AGENT-$cell
	if done_already "$cell"; then echo "skip $name (already run)"; continue; fi
	wait_free
	if [[ $proto == h1 ]]; then http1=1 probe=1; else http1=0 probe=0; fi
	dir=$RUNS/$PREFIX-$name-$(date +%H%M%S)
	echo "$(date +%T) start $name -> $dir"
	AGENT_BIN=$BIN/agent TRAEFIK_BIN=$BIN/traefik RUN_DIR=$dir \
	HOPS=1 REQUESTS=$REQUESTS CONCURRENCY=$c WARM_REQUESTS=$((10 * c)) STRIP_N= TAIL=50 \
	HOP_KIND=traefik CONTEXT_PROPAGATION=headers CPU_SAMPLE=1 LOAD_CORES=$load \
	LG_HTTP1=$http1 LG_CONNS=1 HOP_UPSTREAM_PROTO=$proto HOP_MAX_IDLE=$c \
	BPF_STATS=0 PROBE_TCP_SEQ=$probe FU_STREAM_OFFSETS=0 \
		scripts/hn-propagation.sh || die "$name failed: $dir (fix, then start the series again)"
	diff <(hashes "$REF/manifest.txt" $probe) <(hashes "$dir/manifest.txt" $probe) ||
		die "$name: binaries differ from $REF"
	echo "$dir $AGENT $c $load $probe" >>"$LIST"
	sleep "$SETTLE"
done
echo "$(date +%T) series done; list: $LIST"
