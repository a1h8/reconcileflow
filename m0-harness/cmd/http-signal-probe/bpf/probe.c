// http-signal-probe is the POC for docs/target/reconcileflow-traceability-spec.md
// §4's open hypothesis: does a stateless-in-kernel design avoid the class of
// failure filed as OBI#3571 (open-telemetry/opentelemetry-ebpf-instrumentation)?
//
// OBI's HTTP tracer keeps per-goroutine/per-connection state in bounded BPF
// LRU_HASH maps to correlate a request's entry with its later completion and
// with its propagated trace context (bpf/gotracer/maps/nethttp.h upstream).
// This probe deliberately does none of that: a single uprobe on Go's
// `net/http.(*serverHandler).ServeHTTP` entry, emitting {pid, timestamp_ns}
// straight to a ring buffer — the same architecture m0-harness's
// strong-tier-probe already validated at n=10000 with 0 drops (README
// iteration 6). All correlation (if any is needed downstream) happens in
// userspace, where there is no fixed-capacity structure to silently overflow.
//
// Deliberately narrow scope: this does NOT read the traceparent header (that
// needs Go-ABI-aware struct/map navigation — the hard, multi-month part of
// what OBI does) or attempt request/response pairing. It tests exactly one
// thing: does event throughput hold, with zero loss, under the same
// sustained-single-pooled-connection load that broke OBI's direct
// propagation at ~500 requests? Nothing more.
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

char LICENSE[] SEC("license") = "GPL";

struct http_event {
	__u64 pid_tgid;
	__u64 timestamp_ns;
};

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 20); // 1 MiB — same size as strong-tier-probe's, proven sufficient at n=10000
} events SEC(".maps");

// Same drop-visibility discipline as strong-tier-probe: if this fills under
// load, that's the honest failure mode to report, not silence.
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u64);
} drops SEC(".maps");

// uprobe on net/http.(*serverHandler).ServeHTTP entry (fake-upstream's own
// binary — this is the exact symbol OBI's gotracer also targets). No
// argument decoding: firing alone, with zero kernel-side state, is the
// entire test.
SEC("uprobe/ServeHTTP")
int on_serve_http(struct pt_regs *ctx)
{
	struct http_event *ev = bpf_ringbuf_reserve(&events, sizeof(*ev), 0);
	if (!ev) {
		__u32 key = 0;
		__u64 *cnt = bpf_map_lookup_elem(&drops, &key);
		if (cnt)
			__sync_fetch_and_add(cnt, 1);
		return 0; // dropped: never blocks the traced process either way
	}

	ev->pid_tgid = bpf_get_current_pid_tgid();
	ev->timestamp_ns = bpf_ktime_get_ns();

	bpf_ringbuf_submit(ev, 0);
	return 0;
}
