# Sourced by the measurement scripts: support both OBI and Beyla.
#
# Beyla is Grafana's distribution of OBI: same instrumentation code (built from
# a fork vendored as a submodule), same JSON trace printer, but its
# environment variables use the BEYLA_ prefix instead of OTEL_EBPF_. A verdict
# holds for one binary on one stack, so each is measured on its own; these
# helpers only make the same script drive either.

# agent_kind <binary>: prints "obi" or "beyla", from the buildinfo package the
# binary was linked with. Needs the go tool: call it in phase 1, not under sudo.
agent_kind() {
	local info
	info=$(go version -m "$1" 2>/dev/null) || return 1
	if grep -q 'github.com/grafana/beyla/.*buildinfo' <<<"$info"; then
		echo beyla
	elif grep -q 'go.opentelemetry.io/obi/.*buildinfo' <<<"$info"; then
		echo obi
	else
		return 1
	fi
}

# agent_identity <binary>: "Version=... Revision=..." from the link flags.
agent_identity() {
	go version -m "$1" | grep -o 'buildinfo\.\(Version\|Revision\)=[^ '"'"']*' |
		sed 's/^buildinfo\.//' | paste -sd' ' -
}

# agent_env_prefix <kind>: the environment variable prefix the agent reads.
agent_env_prefix() {
	case $1 in
	obi) echo OTEL_EBPF ;;
	beyla) echo BEYLA ;;
	*) return 1 ;;
	esac
}
