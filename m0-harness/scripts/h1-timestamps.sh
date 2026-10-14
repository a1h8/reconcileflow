#!/usr/bin/env bash
# A1: are per-event timestamps precise enough for timing-based pairing?
#
# OBI's text printer formats span starts with the Go layout
# "2006-01-02 15:04:05.12345"; ".12345" is not a fractional-second layout
# (it prints month, day, hour, minute, second), so every event of one second
# prints the same instant. That, not batching, is the likely cause of the
# README's "~1 s granularity" finding. This script measures what OBI's JSON
# printer (start/handlerStart/end in microseconds, plus traceID) and the
# harness's own http-signal-probe (kernel timestamp at ServeHTTP entry)
# actually deliver, against load-gen's clock.
#
# Pairing never uses time: OBI by trace id, the probe by order (concurrency 1
# only). Analysis and decision rule: h1_timestamps.py.
#
# Run as your normal user, from m0-harness/:
#   AGENT_BIN=/path/to/obi-or-beyla scripts/h1-timestamps.sh
# (OBI_BIN is accepted as an alias.) The agent kind is detected from the
# binary: OBI reads OTEL_EBPF_* variables, Beyla reads BEYLA_*.
# Phase 1 (your user) builds everything into a fresh run directory and records
# hashes. Phase 2 re-executes this script under sudo (OBI and the probe need
# root).
#
# Runs: R1 = 2000 requests at concurrency 1, R2 = 10000 at concurrency 100.
# Environment: RUN_BASE (default: ../runs next to the repository), AGENT_BIN (required; OBI_BIN alias), RUN_DIR, R1_N, R2_N.
set -euo pipefail

# shellcheck source=agent.sh
source "$(dirname "$0")/agent.sh"

SERVE_HTTP_VALUE="net/http.serverHandler.ServeHTTP"
R1_N=${R1_N:-2000}
R2_N=${R2_N:-10000}

die() { echo "error: $*" >&2; exit 1; }

build() {
	[[ $(id -u) -ne 0 ]] || die "run phase 1 as your normal user (it calls sudo itself)"
	[[ -f go.mod && -d cmd/http-signal-probe ]] || die "run from m0-harness/"
	AGENT_BIN=${AGENT_BIN:-${OBI_BIN:-}}
	[[ -x $AGENT_BIN ]] || die "AGENT_BIN must point to an executable obi or beyla binary"
	AGENT_KIND=$(agent_kind "$AGENT_BIN") || die "$AGENT_BIN is neither OBI nor Beyla"
	# Runs go to persistent disk, next to the repository (RUN_BASE overrides):
	# /tmp may be a size-limited tmpfs, wiped at reboot, and runs are evidence.
	RUN_BASE=${RUN_BASE:-$(dirname "$(git rev-parse --show-toplevel)")/runs}
	RUN_DIR=${RUN_DIR:-$RUN_BASE/h1-timestamps-$(date +%Y%m%dT%H%M%S)}
	mkdir -p "$RUN_DIR/bin"

	for cmd in fake-upstream load-gen http-signal-probe; do
		go build -o "$RUN_DIR/bin/$cmd" "./cmd/$cmd"
	done
	cp "$AGENT_BIN" "$RUN_DIR/bin/agent"
	local bpf=cmd/http-signal-probe/bpf
	for f in "$bpf/vmlinux.h" "$bpf/vendor/bpf/bpf_helpers.h"; do
		[[ -f $f ]] || die "missing $f -- see README \"Building\" (same steps, http-signal-probe paths)"
	done
	clang -target bpf -D__TARGET_ARCH_x86 -I"$bpf" -I"$bpf/vendor" \
		-g -O2 -c "$bpf/probe.c" -o "$RUN_DIR/bin/probe.o"

	{
		echo "date: $(date -Iseconds)"
		echo "kernel: $(uname -r)"
		echo "go: $(go version)"
		echo "clang: $(clang --version | head -1)"
		echo "agent: $AGENT_KIND $(agent_identity "$RUN_DIR/bin/agent")"
		echo "commit: $(git rev-parse HEAD)$([[ -z $(git status --porcelain -- .) ]] || echo ' (dirty)')"
		echo "R1_N=$R1_N (c=1) R2_N=$R2_N (c=100) symbol=$SERVE_HTTP_VALUE"
		(cd "$RUN_DIR/bin" && sha256sum ./*)
	} >"$RUN_DIR/manifest.txt"

	echo "built into $RUN_DIR; phase 2 needs root"
	exec sudo AGENT_KIND="$AGENT_KIND" R1_N="$R1_N" R2_N="$R2_N" "$0" --measure "$RUN_DIR"
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

# run <dir> <requests> <concurrency>: fresh fake-upstream, OBI and probe.
run() {
	local dir=$1 requests=$2 concurrency=$3 bin=$RUN_DIR/bin
	mkdir -p "$dir"
	local port; port=$(free_port)

	"$bin/fake-upstream" -addr "127.0.0.1:$port" -out "$dir/fu.jsonl" 2>"$dir/fu.log" &
	local fu=$!
	wait_for 10 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null || die "$dir: fake-upstream did not listen"

	local prefix; prefix=$(agent_env_prefix "$AGENT_KIND") || die "unknown AGENT_KIND '$AGENT_KIND'"
	env "${prefix}_OPEN_PORT=$port" "${prefix}_TRACE_PRINTER=json" "${prefix}_LOG_LEVEL=info" \
		"$bin/agent" >"$dir/agent.out" 2>"$dir/agent.log" &
	local obi=$!

	# Warm-up until OBI prints spans, so the measured run starts instrumented.
	# Warm-up traffic goes to its own file; the probe is attached only after it.
	local ready=0
	for i in $(seq 1 30); do
		# One file per attempt: -out truncates, and an earlier attempt's ids
		# must stay attributable when OBI saw some of them.
		"$bin/load-gen" -target "https://127.0.0.1:$port" -out "$dir/warmup-$i.jsonl" -truth-in-path \
			-requests 20 -concurrency 1 2>>"$dir/warmup.log" || true
		# A server span on this port, not just any span (OBI also prints
		# internal ones, e.g. ProcessAlive, before it instruments anything).
		# One grep on the file, no pipe: under pipefail, "grep | grep -q" fails
		# once the output outgrows the pipe buffer (grep -q exits on the first
		# match and the writer gets SIGPIPE). Span fields never occur in log lines.
		if grep -q "\"hostPort\":\"$port\"" "$dir/agent.out"; then ready=1; break; fi
		sleep 1
	done
	((ready)) || { kill "$obi" "$fu" 2>/dev/null; die "$dir: the agent printed no span (see agent.log)"; }

	"$bin/http-signal-probe" -binary "$bin/fake-upstream" -obj "$bin/probe.o" \
		-symbol "$SERVE_HTTP_VALUE" -out "$dir/probe.jsonl" 2>"$dir/probe.log" &
	local probe=$!
	if ! wait_for 15 grep -q "http-signal-probe attached" "$dir/probe.log"; then
		kill "$obi" "$fu" 2>/dev/null; cat "$dir/probe.log" >&2; die "$dir: probe did not attach"
	fi

	local lg_exit=0
	"$bin/load-gen" -target "https://127.0.0.1:$port" -out "$dir/lg.jsonl" \
		-requests "$requests" -concurrency "$concurrency" 2>"$dir/lg.log" || lg_exit=$?
	sleep 3 # let OBI and the probe drain

	kill -TERM "$probe"; wait "$probe" || true
	kill -TERM "$obi"; wait "$obi" || true
	kill "$fu"; wait "$fu" 2>/dev/null || true

	# Keep only this fake-upstream's probe events (a uprobe fires for every
	# process running the binary).
	grep "\"pid\":$fu," "$dir/probe.jsonl" >"$dir/probe-fu.jsonl" || true
	echo "$port" >"$dir/port"
	echo "$(basename "$dir"): port=$port requests=$requests c=$concurrency load-gen exit=$lg_exit" \
		"probe events=$(wc -l <"$dir/probe-fu.jsonl")" | tee -a "$RUN_DIR/runs.txt"
}

measure() {
	RUN_DIR=$1
	[[ $(id -u) -eq 0 ]] || die "phase 2 must run as root"
	run "$RUN_DIR/r1-c1" "$R1_N" 1
	run "$RUN_DIR/r2-c100" "$R2_N" 100
	chown -R "${SUDO_UID:-0}:${SUDO_GID:-0}" "$RUN_DIR"
	echo
	echo "analyse (as your user, from the repo root):"
	for d in "$RUN_DIR"/r1-c1 "$RUN_DIR"/r2-c100; do
		echo "  .venv/bin/python m0-harness/h1_timestamps.py $d/lg.jsonl $d/agent.out $d/probe-fu.jsonl --port $(cat "$d/port")"
	done
}

if [[ ${1:-} == --measure ]]; then
	measure "$2"
else
	build
fi
