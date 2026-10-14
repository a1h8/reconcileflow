#!/usr/bin/env bash
# A2a: does OBI read the right traceparent for every request under pooling?
#
# One hop, load-gen -> fake-upstream, OBI's JSON printer. load-gen puts each
# request's own trace id in its path (-truth-in-path, evaluation only), so a
# swap between two real requests is detected even when the set of reported
# ids is unchanged. Scored by analyze_run.py --json-port, with the warm-up and
# tail files passed to --exclude (known traffic outside the measured run).
#
# Cells: pooled c=20/100/200, positive control (-pool=false), negative control
# (-mask: no traceparent sent, OBI must self-author every id). After each
# measured run, TAIL extra requests go to a separate, unscored file so OBI's
# last batch is flushed and cannot pass for a loss.
#
# Run as your normal user, from m0-harness/:
#   AGENT_BIN=/path/to/obi-or-beyla scripts/h2-propagation.sh
# (OBI_BIN is accepted as an alias.) The agent kind is detected from the
# binary: OBI reads OTEL_EBPF_* variables, Beyla reads BEYLA_*.
# Phase 1 (your user) builds into a fresh run directory and records hashes.
# Phase 2 re-executes this script under sudo (the agent needs root).
#
# Environment: RUN_BASE (default: ../runs next to the repository), AGENT_BIN (required; OBI_BIN alias), RUN_DIR, TAIL (50),
# CONTEXT_PROPAGATION (empty: the agent's default, disabled in OBI v0.14.0 and
# Beyla v3.38.0 -- reading only; headers, tcp, all: the agent writes context).
set -euo pipefail

# shellcheck source=agent.sh
source "$(dirname "$0")/agent.sh"

TAIL=${TAIL:-50}
CONTEXT_PROPAGATION=${CONTEXT_PROPAGATION:-}

die() { echo "error: $*" >&2; exit 1; }

build() {
	[[ $(id -u) -ne 0 ]] || die "run phase 1 as your normal user (it calls sudo itself)"
	[[ -f go.mod && -d cmd/load-gen ]] || die "run from m0-harness/"
	AGENT_BIN=${AGENT_BIN:-${OBI_BIN:-}}
	[[ -x $AGENT_BIN ]] || die "AGENT_BIN must point to an executable obi or beyla binary"
	AGENT_KIND=$(agent_kind "$AGENT_BIN") || die "$AGENT_BIN is neither OBI nor Beyla"
	# Runs go to persistent disk, next to the repository (RUN_BASE overrides):
	# /tmp may be a size-limited tmpfs, wiped at reboot, and runs are evidence.
	RUN_BASE=${RUN_BASE:-$(dirname "$(git rev-parse --show-toplevel)")/runs}
	RUN_DIR=${RUN_DIR:-$RUN_BASE/h2-propagation-$(date +%Y%m%dT%H%M%S)}
	mkdir -p "$RUN_DIR/bin"

	for cmd in fake-upstream load-gen; do
		go build -o "$RUN_DIR/bin/$cmd" "./cmd/$cmd"
	done
	cp "$AGENT_BIN" "$RUN_DIR/bin/agent"

	{
		echo "date: $(date -Iseconds)"
		echo "kernel: $(uname -r)"
		echo "go: $(go version)"
		echo "agent: $AGENT_KIND $(agent_identity "$RUN_DIR/bin/agent")"
		echo "commit: $(git rev-parse HEAD)$([[ -z $(git status --porcelain -- .) ]] || echo ' (dirty)')"
		echo "TAIL=$TAIL CONTEXT_PROPAGATION=${CONTEXT_PROPAGATION:-agent-default}"
		(cd "$RUN_DIR/bin" && sha256sum ./*)
	} >"$RUN_DIR/manifest.txt"

	echo "built into $RUN_DIR; phase 2 needs root"
	exec sudo AGENT_KIND="$AGENT_KIND" TAIL="$TAIL" CONTEXT_PROPAGATION="$CONTEXT_PROPAGATION" \
		"$0" --measure "$RUN_DIR"
}

free_port() {
	python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

wait_for() { # wait_for <seconds> <command...>
	local deadline=$((SECONDS + $1)); shift
	until "$@"; do
		((SECONDS < deadline)) || return 1
		sleep 0.2
	done
}

# cell <name> <requests> <concurrency> [load-gen flags...]
cell() {
	local name=$1 requests=$2 concurrency=$3; shift 3
	local dir=$RUN_DIR/$name bin=$RUN_DIR/bin
	mkdir -p "$dir"
	local port; port=$(free_port)
	echo "$port" >"$dir/port"

	"$bin/fake-upstream" -addr "127.0.0.1:$port" -out "$dir/fu.jsonl" 2>"$dir/fu.log" &
	local fu=$!
	wait_for 10 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null || die "$name: fake-upstream did not listen"

	local prefix; prefix=$(agent_env_prefix "$AGENT_KIND") || die "unknown AGENT_KIND '$AGENT_KIND'"
	local agent_env=("${prefix}_OPEN_PORT=$port" "${prefix}_TRACE_PRINTER=json" "${prefix}_LOG_LEVEL=info")
	[[ -z $CONTEXT_PROPAGATION ]] ||
		agent_env+=("${prefix}_BPF_CONTEXT_PROPAGATION=$CONTEXT_PROPAGATION")
	env "${agent_env[@]}" "$bin/agent" >"$dir/agent.out" 2>"$dir/agent.log" &
	local obi=$!

	# Warm-up until OBI prints a server span on this port; one file per attempt.
	local ready=0
	for i in $(seq 1 30); do
		"$bin/load-gen" -target "https://127.0.0.1:$port" -out "$dir/warmup-$i.jsonl" -truth-in-path \
			-requests 20 -concurrency 1 2>>"$dir/warmup.log" || true
		# One grep on the file, no pipe: under pipefail, "grep | grep -q" fails
		# once the output outgrows the pipe buffer (grep -q exits on the first
		# match and the writer gets SIGPIPE). Span fields never occur in log lines.
		if grep -q "\"hostPort\":\"$port\"" "$dir/agent.out"; then ready=1; break; fi
		sleep 1
	done
	((ready)) || { kill "$obi" "$fu" 2>/dev/null; die "$name: the agent printed no server span (see agent.log)"; }

	local lg_exit=0
	"$bin/load-gen" -target "https://127.0.0.1:$port" -out "$dir/lg.jsonl" -truth-in-path \
		-requests "$requests" -concurrency "$concurrency" "$@" 2>"$dir/lg.log" || lg_exit=$?
	# Tail traffic, unscored: flushes OBI's last batch of the measured run.
	"$bin/load-gen" -target "https://127.0.0.1:$port" -out "$dir/tail.jsonl" -truth-in-path \
		-requests "$TAIL" -concurrency 1 2>"$dir/tail.log" || true
	sleep 3

	kill -TERM "$obi"; wait "$obi" || true
	kill "$fu"; wait "$fu" 2>/dev/null || true
	echo "$name: port=$port requests=$requests c=$concurrency flags=[$*]" \
		"propagation=${CONTEXT_PROPAGATION:-agent-default} load-gen exit=$lg_exit" |
		tee -a "$RUN_DIR/cells.txt"
}

measure() {
	RUN_DIR=$1
	[[ $(id -u) -eq 0 ]] || die "phase 2 must run as root"
	cell pooled-c20 2000 20
	cell pooled-c100 10000 100
	cell pooled-c200 10000 200
	cell positive-c100 2000 100 -pool=false
	cell negative-c100 2000 100 -mask
	chown -R "${SUDO_UID:-0}:${SUDO_GID:-0}" "$RUN_DIR"
	echo
	echo "analyse (as your user, from the repo root), one cell at a time:"
	for d in "$RUN_DIR"/*/; do
		[[ -f $d/port ]] || continue
		echo "  .venv/bin/python m0-harness/analyze_run.py ${d}agent.out ${d}lg.jsonl" \
			"--json-port $(cat "${d}port") --exclude ${d}warmup-*.jsonl ${d}tail.jsonl"
	done
}

if [[ ${1:-} == --measure ]]; then
	measure "$2"
else
	build
fi
