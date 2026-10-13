#!/usr/bin/env bash
# A2b, generalised to n hops: does the trace context survive a chain of proxies?
#
#   load-gen -> hop-1 -> ... -> hop-n -> fake-upstream
#
# Each hop is a Traefik native binary with no tracing configured (it forwards
# headers as-is), HTTPS with HTTP/2 on every link, pooled connections. Each hop
# runs from its own copy of the binary named hop-k: two instances of one
# executable were seen with a wrong destination port in the agent's output, so
# observation points are told apart by service name, not by port.
#
# One agent (OBI or Beyla, see agent.sh) instruments every hop and
# fake-upstream. load-gen puts each request's id in its path (-truth-in-path,
# evaluation only). hn_analyze.py scores the 2n + 1 observation points of a
# cell: each hop's inbound server span and outbound client span, and
# fake-upstream's inbound span.
#
# Run as your normal user, from m0-harness/:
#   AGENT_BIN=/path/to/obi-or-beyla TRAEFIK_BIN=/path/to/traefik scripts/hn-propagation.sh
# Phase 1 (your user) builds into a fresh run directory and records hashes.
# Phase 2 re-executes this script under sudo (the agent needs root).
#
# Environment:
#   HOPS      hop counts to run, in order (default "1 2 3 5")
#   REQUESTS  requests per cell (default 5000), CONCURRENCY (default 100)
#   STRIP_N   if set, also run a negative-control cell with STRIP_N hops where
#             hop-2 strips traceparent (default 2; empty to skip)
#   TAIL      unscored tail requests per cell (default 50)
#   HOP_KIND  traefik (default: forwards headers itself) or relay (cmd/relay:
#             new outbound request, no header copied -- the agent alone can
#             carry context across it). STRIP_N applies to traefik only.
#   BAGGAGE   1: load-gen also sends a baggage entry (A2c)
#   NO_AGENT  1: run the chain without any agent (control cells); needs no
#             root, so phase 2 runs as your user and AGENT_BIN is not needed
#   RUN_DIR
set -euo pipefail

# shellcheck source=agent.sh
source "$(dirname "$0")/agent.sh"

HOPS=${HOPS:-1 2 3 5}
REQUESTS=${REQUESTS:-5000}
CONCURRENCY=${CONCURRENCY:-100}
STRIP_N=${STRIP_N-2}
TAIL=${TAIL:-50}
HOP_KIND=${HOP_KIND:-traefik}
BAGGAGE=${BAGGAGE:-0}
NO_AGENT=${NO_AGENT:-0}

die() { echo "error: $*" >&2; exit 1; }

build() {
	[[ $(id -u) -ne 0 ]] || die "run phase 1 as your normal user (it calls sudo itself)"
	[[ -f go.mod && -d cmd/load-gen ]] || die "run from m0-harness/"
	case $HOP_KIND in traefik | relay) ;; *) die "HOP_KIND must be traefik or relay" ;; esac
	AGENT_KIND=none
	if ((!NO_AGENT)); then
		AGENT_BIN=${AGENT_BIN:-${OBI_BIN:-}}
		[[ -x $AGENT_BIN ]] || die "AGENT_BIN must point to an executable obi or beyla binary"
		AGENT_KIND=$(agent_kind "$AGENT_BIN") || die "$AGENT_BIN is neither OBI nor Beyla"
	fi
	if [[ $HOP_KIND == traefik ]]; then
		[[ -x ${TRAEFIK_BIN:-} ]] || die "TRAEFIK_BIN must point to an executable traefik binary"
	fi
	RUN_DIR=${RUN_DIR:-/tmp/hn-propagation-$(date +%Y%m%dT%H%M%S)}
	mkdir -p "$RUN_DIR/bin"

	for cmd in fake-upstream load-gen relay; do
		go build -o "$RUN_DIR/bin/$cmd" "./cmd/$cmd"
	done
	((NO_AGENT)) || cp "$AGENT_BIN" "$RUN_DIR/bin/agent"
	[[ $HOP_KIND != traefik ]] || cp "$TRAEFIK_BIN" "$RUN_DIR/bin/traefik"

	{
		echo "date: $(date -Iseconds)"
		echo "kernel: $(uname -r)"
		echo "go: $(go version)"
		if ((NO_AGENT)); then echo "agent: none"; else
			echo "agent: $AGENT_KIND $(agent_identity "$RUN_DIR/bin/agent")"; fi
		[[ $HOP_KIND != traefik ]] ||
			echo "traefik: $("$RUN_DIR/bin/traefik" version | awk '/^Version:/ {print $2}')"
		echo "hop kind: $HOP_KIND"
		echo "commit: $(git rev-parse HEAD)$([[ -z $(git status --porcelain -- .) ]] || echo ' (dirty)')"
		echo "HOPS=$HOPS REQUESTS=$REQUESTS CONCURRENCY=$CONCURRENCY STRIP_N=$STRIP_N TAIL=$TAIL" \
			"HOP_KIND=$HOP_KIND BAGGAGE=$BAGGAGE NO_AGENT=$NO_AGENT"
		(cd "$RUN_DIR/bin" && sha256sum ./*)
	} >"$RUN_DIR/manifest.txt"

	local env_args=(AGENT_KIND="$AGENT_KIND" HOPS="$HOPS" REQUESTS="$REQUESTS"
		CONCURRENCY="$CONCURRENCY" STRIP_N="$STRIP_N" TAIL="$TAIL"
		HOP_KIND="$HOP_KIND" BAGGAGE="$BAGGAGE" NO_AGENT="$NO_AGENT")
	if ((NO_AGENT)); then
		echo "built into $RUN_DIR; no agent, phase 2 runs as your user"
		exec env "${env_args[@]}" "$0" --measure "$RUN_DIR"
	fi
	echo "built into $RUN_DIR; phase 2 needs root"
	exec sudo "${env_args[@]}" "$0" --measure "$RUN_DIR"
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

listening() { bash -c "exec 3<>/dev/tcp/127.0.0.1/$1" 2>/dev/null; }

# hop_config <file> <next-url> <strip: 0|1>
hop_config() {
	local file=$1 next=$2 strip=$3
	{
		echo "http:"
		echo "  routers:"
		echo "    all:"
		echo "      rule: PathPrefix(\`/\`)"
		echo "      entryPoints: [web]"
		echo "      service: next"
		echo "      tls: {}"
		((strip)) && echo "      middlewares: [strip]"
		if ((strip)); then
			echo "  middlewares:"
			echo "    strip:"
			echo "      headers:"
			echo "        customRequestHeaders:"
			echo "          traceparent: \"\""
		fi
		echo "  services:"
		echo "    next:"
		echo "      loadBalancer:"
		echo "        servers:"
		echo "          - url: $next"
	} >"$file"
}

# cell <name> <hops> <strip-hop: 0 for none>
cell() {
	local name=$1 n=$2 strip_hop=$3
	local dir=$RUN_DIR/$name bin=$RUN_DIR/bin
	mkdir -p "$dir"
	local pids=()

	local fu_port; fu_port=$(free_port)
	"$bin/fake-upstream" -addr "127.0.0.1:$fu_port" -out "$dir/fu.jsonl" 2>"$dir/fu.log" &
	pids+=($!)
	wait_for 10 listening "$fu_port" || die "$name: fake-upstream did not listen"

	# Build the chain from the far end: hop-n forwards to fake-upstream.
	local next="https://127.0.0.1:$fu_port" ports=("$fu_port") hop_ports=() k
	for ((k = n; k >= 1; k--)); do
		local port; port=$(free_port)
		if [[ $HOP_KIND == relay ]]; then
			cp "$bin/relay" "$dir/hop-$k"
			"$dir/hop-$k" -addr "127.0.0.1:$port" -next "$next" >"$dir/hop-$k.log" 2>&1 &
		else
			cp "$bin/traefik" "$dir/hop-$k"
			hop_config "$dir/hop-$k.yml" "$next" $((k == strip_hop))
			"$dir/hop-$k" --entrypoints.web.address="127.0.0.1:$port" --entrypoints.web.http.tls=true \
				--providers.file.filename="$dir/hop-$k.yml" --serversTransport.insecureSkipVerify=true \
				--log.level=ERROR >"$dir/hop-$k.log" 2>&1 &
		fi
		pids+=($!)
		wait_for 15 listening "$port" || die "$name: hop-$k did not listen"
		ports+=("$port")
		hop_ports=("$port" "${hop_ports[@]}")
		next="https://127.0.0.1:$port"
	done
	local entry=$next
	printf '{"hops": %d, "strip_hop": %d, "fake_upstream_port": %d, "hop_ports": [%s], "hop_kind": "%s", "baggage": %d, "agent": "%s"}\n' \
		"$n" "$strip_hop" "$fu_port" "$(IFS=,; echo "${hop_ports[*]}")" "$HOP_KIND" "$BAGGAGE" \
		"$AGENT_KIND" >"$dir/cell.json"

	local lg_flags=(-truth-in-path)
	((BAGGAGE)) && lg_flags+=(-baggage)

	local agent=""
	if ((!NO_AGENT)); then
		local prefix; prefix=$(agent_env_prefix "$AGENT_KIND") || die "unknown AGENT_KIND '$AGENT_KIND'"
		env "${prefix}_OPEN_PORT=$(IFS=,; echo "${ports[*]}")" "${prefix}_TRACE_PRINTER=json" \
			"${prefix}_LOG_LEVEL=info" "$bin/agent" >"$dir/agent.out" 2>"$dir/agent.log" &
		agent=$!
	fi

	# Warm-up through the whole chain until both ends print server spans.
	local ready=0 i
	for i in $(seq 1 40); do
		"$bin/load-gen" -target "$entry" -out "$dir/warmup-$i.jsonl" "${lg_flags[@]}" \
			-requests 20 -concurrency 1 2>>"$dir/warmup.log" || true
		if ((NO_AGENT)); then ready=1; break; fi
		# One grep on the file, no pipe: under pipefail, "grep | grep -q" fails
		# once the output outgrows the pipe buffer (grep -q exits on the first
		# match and the writer gets SIGPIPE). Span fields never occur in log lines.
		if grep -q '"serverAddr":"hop-1"' "$dir/agent.out" &&
			grep -q "\"hostPort\":\"$fu_port\"" "$dir/agent.out"; then
			ready=1; break
		fi
		sleep 1
	done
	((ready)) || { kill $agent "${pids[@]}" 2>/dev/null; die "$name: agent printed no span at both ends"; }

	local lg_exit=0
	"$bin/load-gen" -target "$entry" -out "$dir/lg.jsonl" "${lg_flags[@]}" \
		-requests "$REQUESTS" -concurrency "$CONCURRENCY" 2>"$dir/lg.log" || lg_exit=$?
	"$bin/load-gen" -target "$entry" -out "$dir/tail.jsonl" "${lg_flags[@]}" \
		-requests "$TAIL" -concurrency 1 2>"$dir/tail.log" || true
	sleep 3

	if [[ -n $agent ]]; then kill -TERM "$agent"; wait "$agent" || true; fi
	kill "${pids[@]}" 2>/dev/null; wait "${pids[@]}" 2>/dev/null || true
	echo "$name: hops=$n kind=$HOP_KIND strip_hop=$strip_hop baggage=$BAGGAGE agent=$AGENT_KIND" \
		"requests=$REQUESTS c=$CONCURRENCY load-gen exit=$lg_exit" |
		tee -a "$RUN_DIR/cells.txt"
}

measure() {
	RUN_DIR=$1
	((NO_AGENT)) || [[ $(id -u) -eq 0 ]] || die "phase 2 must run as root"
	local n
	for n in $HOPS; do
		cell "n$n" "$n" 0
		if [[ -n $STRIP_N && $n == "$STRIP_N" && $HOP_KIND == traefik ]]; then
			cell "n$n-strip" "$n" 2
		fi
	done
	((NO_AGENT)) || chown -R "${SUDO_UID:-0}:${SUDO_GID:-0}" "$RUN_DIR"
	echo
	echo "analyse (as your user, from the repo root):"
	echo "  .venv/bin/python m0-harness/hn_analyze.py $RUN_DIR"
}

if [[ ${1:-} == --measure ]]; then
	measure "$2"
else
	build
fi
