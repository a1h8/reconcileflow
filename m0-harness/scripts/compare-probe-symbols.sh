#!/usr/bin/env bash
# Which uprobe symbol fires on HTTP/2 traffic to fake-upstream?
#
# Settles the open question in README "http-signal-probe: default symbol...
# (2026-10-07)": the 2026-09-25 result was reported on
# net/http/internal/http2.(*serverConn).runHandler because
# net/http.(*serverHandler).ServeHTTP reportedly never fired on HTTP/2, but
# the exact command was not recorded. This script records it.
#
# Two ServeHTTP symbols exist. (*serverHandler).ServeHTTP is the
# compiler-generated pointer-receiver wrapper: go tool objdump finds no
# direct call to it, in HTTP/1 (conn.serve) or HTTP/2 (initALPNRequest).
# Both call the value method serverHandler.ServeHTTP, which OBI hooks.
# Measuring both separates "does not fire on HTTP/2" from "never called".
#
# Run as your normal user, from m0-harness/:
#   scripts/compare-probe-symbols.sh
# Phase 1 (your user) builds every binary and probe.o from the committed
# sources into a fresh run directory and records their hashes. Phase 2
# re-executes this script under sudo to load the probe (needs CAP_BPF).
#
# Every cell gets its own fake-upstream (README "Running one witness cell").
# Per symbol: a negative control (probe attached, no traffic: must record 0)
# and REPEATS independent traffic cells of N requests at CONCURRENCY.
# Events are counted only for the cell's own fake-upstream PID: a uprobe on
# a binary fires for every process running it.
#
# Environment: N (2000), CONCURRENCY (100), REPEATS (2), RUN_DIR.
set -euo pipefail

# The HTTP/2 dispatch symbol's name depends on the Go version (see
# http2DispatchSymbols in cmd/http-signal-probe); phase 1 looks it up in the
# fake-upstream it built and passes both names to phase 2.
SERVE_HTTP="net/http.(*serverHandler).ServeHTTP"
SERVE_HTTP_VALUE="net/http.serverHandler.ServeHTTP"
N=${N:-2000}
CONCURRENCY=${CONCURRENCY:-100}
REPEATS=${REPEATS:-2}

die() { echo "error: $*" >&2; exit 1; }

build() {
	[[ $(id -u) -ne 0 ]] || die "run phase 1 as your normal user (it calls sudo itself)"
	[[ -f go.mod && -d cmd/http-signal-probe ]] || die "run from m0-harness/"
	RUN_DIR=${RUN_DIR:-/tmp/probe-symbols-$(date +%Y%m%dT%H%M%S)}
	mkdir -p "$RUN_DIR/bin"

	for cmd in fake-upstream load-gen http-signal-probe; do
		go build -o "$RUN_DIR/bin/$cmd" "./cmd/$cmd"
	done
	local bpf=cmd/http-signal-probe/bpf
	for f in "$bpf/vmlinux.h" "$bpf/vendor/bpf/bpf_helpers.h"; do
		[[ -f $f ]] || die "missing $f -- see README \"Building\" (same steps, http-signal-probe paths)"
	done
	# awk reads all of nm's output: exiting early would SIGPIPE nm and fail
	# the pipeline under pipefail.
	RUN_HANDLER=$(go tool nm "$RUN_DIR/bin/fake-upstream" |
		awk '$2 == "T" && $3 ~ /serverConn\)\.runHandler$/ && !found { found = $3 } END { print found }')
	[[ -n $RUN_HANDLER ]] || die "no HTTP/2 runHandler symbol in the built fake-upstream"
	go tool nm "$RUN_DIR/bin/fake-upstream" |
		awk -v s="$SERVE_HTTP_VALUE" '$2 == "T" && $3 == s { found = 1 } END { exit !found }' ||
		die "no $SERVE_HTTP_VALUE in the built fake-upstream"

	clang -target bpf -D__TARGET_ARCH_x86 -I"$bpf" -I"$bpf/vendor" \
		-g -O2 -c "$bpf/probe.c" -o "$RUN_DIR/bin/probe.o"

	{
		echo "date: $(date -Iseconds)"
		echo "kernel: $(uname -r)"
		echo "go: $(go version)"
		echo "clang: $(clang --version | head -1)"
		echo "commit: $(git rev-parse HEAD)$([[ -z $(git status --porcelain -- .) ]] || echo ' (dirty)')"
		echo "N=$N CONCURRENCY=$CONCURRENCY REPEATS=$REPEATS"
		echo "symbols: $RUN_HANDLER | $SERVE_HTTP | $SERVE_HTTP_VALUE"
		(cd "$RUN_DIR/bin" && sha256sum ./*)
	} >"$RUN_DIR/manifest.txt"

	echo "built into $RUN_DIR; phase 2 needs root"
	exec sudo N="$N" CONCURRENCY="$CONCURRENCY" REPEATS="$REPEATS" RUN_HANDLER="$RUN_HANDLER" \
		"$0" --measure "$RUN_DIR"
}

free_port() {
	python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

wait_for() { # wait_for <seconds> <command...>
	local deadline=$((SECONDS + $1)); shift
	until "$@"; do
		((SECONDS < deadline)) || return 1
		sleep 0.1
	done
}

# cell <dir> <symbol> <requests>: one fake-upstream, one probe, optional traffic.
cell() {
	local dir=$1 symbol=$2 requests=$3 bin=$RUN_DIR/bin
	mkdir -p "$dir"
	local port; port=$(free_port)

	"$bin/fake-upstream" -addr "127.0.0.1:$port" -out "$dir/fu.jsonl" 2>"$dir/fu.log" &
	local fu=$!
	wait_for 10 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null || die "$dir: fake-upstream did not listen"

	"$bin/http-signal-probe" -binary "$bin/fake-upstream" -obj "$bin/probe.o" \
		-symbol "$symbol" -out "$dir/probe.jsonl" 2>"$dir/probe.log" &
	local probe=$!
	if ! wait_for 15 grep -q "http-signal-probe attached" "$dir/probe.log"; then
		kill "$fu" 2>/dev/null; cat "$dir/probe.log" >&2; die "$dir: probe did not attach"
	fi

	local lg_exit=0 succeeded=0
	if ((requests > 0)); then
		"$bin/load-gen" -target "https://127.0.0.1:$port" -out "$dir/lg.jsonl" \
			-requests "$requests" -concurrency "$CONCURRENCY" 2>"$dir/lg.log" || lg_exit=$?
		succeeded=$(grep -vc '"error"' "$dir/lg.jsonl" || true)
	fi
	sleep 1 # let the ring buffer drain before shutdown

	local probe_exit=0
	kill -TERM "$probe"; wait "$probe" || probe_exit=$? # SIGINT is ignored in background jobs of a non-interactive shell
	kill "$fu"; wait "$fu" 2>/dev/null || true

	local events drops protos
	events=$(grep -c "\"pid\":$fu," "$dir/probe.jsonl" || true)
	drops=$(grep -o 'never emitted): [0-9]*' "$dir/probe.log" | grep -o '[0-9]*$' || echo '?')
	protos=$( (grep -o '"protocol":"[^"]*"' "$dir/lg.jsonl" 2>/dev/null || true) | sort -u | cut -d'"' -f4 | paste -sd, -)
	printf '%s\t%s\t%d\t%d\t%s\t%d\t%s\t%d\t%d\n' "$(basename "$dir")" "$symbol" "$requests" \
		"$succeeded" "${protos:--}" "$events" "$drops" "$probe_exit" "$lg_exit" | tee -a "$RUN_DIR/results.tsv"
}

measure() {
	RUN_DIR=$1
	[[ $(id -u) -eq 0 ]] || die "phase 2 must run as root"
	printf 'cell\tsymbol\trequests\tsucceeded\tprotocol\tevents_for_fu_pid\tring_drops\tprobe_exit\tloadgen_exit\n' |
		tee "$RUN_DIR/results.tsv"
	[[ -n ${RUN_HANDLER:-} ]] || die "RUN_HANDLER unset: start from phase 1"
	local i=0
	for symbol in "$RUN_HANDLER" "$SERVE_HTTP" "$SERVE_HTTP_VALUE"; do
		i=$((i + 1))
		cell "$RUN_DIR/s$i-negative" "$symbol" 0
		for r in $(seq 1 "$REPEATS"); do
			cell "$RUN_DIR/s$i-run$r" "$symbol" "$N"
		done
	done
	chown -R "${SUDO_UID:-0}:${SUDO_GID:-0}" "$RUN_DIR"
	echo
	echo "results: $RUN_DIR/results.tsv (manifest: $RUN_DIR/manifest.txt)"
	echo "expect: negative cells 0 events; a symbol that fires per request ~= succeeded."
}

if [[ ${1:-} == --measure ]]; then
	measure "$2"
else
	build
fi
