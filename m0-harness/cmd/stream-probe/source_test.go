package main

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The probe never reads a message's content: every kernel read in bpf/probe.c
// is of a field on this list (ports, addresses, family, TCP sequence
// numbers), every helper call is on the next one, and the message buffer
// passed to tcp_sendmsg is never touched. A change that reads anything else
// fails here first, and has to be argued in review.
var allowedReads = map[string]bool{
	"sk, __sk_common.skc_num":          true,
	"sk, __sk_common.skc_dport":        true,
	"sk, __sk_common.skc_v6_rcv_saddr": true,
	"sk, __sk_common.skc_v6_daddr":     true,
	"sk, __sk_common.skc_rcv_saddr":    true,
	"sk, __sk_common.skc_daddr":        true,
	"sk, __sk_common.skc_family":       true,
	"tp, write_seq":                    true,
	"tp, copied_seq":                   true,
}

var allowedHelpers = map[string]bool{
	"bpf_map_lookup_elem":      true,
	"bpf_ringbuf_reserve":      true,
	"bpf_ringbuf_submit":       true,
	"bpf_get_current_pid_tgid": true,
	"bpf_ktime_get_ns":         true,
	"bpf_ntohs":                true,
}

var (
	lineComment  = regexp.MustCompile(`//[^\n]*`)
	blockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	coreRead     = regexp.MustCompile(`BPF_CORE_READ(?:_INTO)?\(\s*(?:&ev->\w+,\s*)?([^)]*)\)`)
	helperCall   = regexp.MustCompile(`\b(bpf_\w+)\s*\(`)
	otherReads   = regexp.MustCompile(`\b(bpf_probe_read\w*|bpf_copy_from_user\w*|bpf_skb_\w+|bpf_dynptr_\w+|BPF_PROBE_READ\w*|BPF_CORE_READ_USER\w*|bpf_core_read\w*)\b`)
	msgUse       = regexp.MustCompile(`\bmsg\b`)
	include      = regexp.MustCompile(`(?m)^#include[^\n]*`)
)

func probeSource(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("bpf/probe.c")
	if err != nil {
		t.Fatal(err)
	}
	src := blockComment.ReplaceAllString(lineComment.ReplaceAllString(string(raw), ""), "")
	return include.ReplaceAllString(src, "")
}

func TestTheProbeNeverReadsMessageContent(t *testing.T) {
	src := probeSource(t)
	var reads []string
	for _, m := range coreRead.FindAllStringSubmatch(src, -1) {
		field := strings.TrimSpace(m[1])
		reads = append(reads, field)
		if !allowedReads[field] {
			t.Errorf("kernel read %q is not on the allowed list", field)
		}
	}
	if len(reads) == 0 {
		t.Fatal("no kernel read found: the pattern no longer matches the source")
	}
	var helpers []string
	for _, m := range helperCall.FindAllStringSubmatch(src, -1) {
		if allowedHelpers[m[1]] {
			helpers = append(helpers, m[1])
			continue
		}
		t.Errorf("helper %s is not on the allowed list", m[1])
	}
	for _, m := range otherReads.FindAllString(src, -1) {
		t.Errorf("memory read %s: only the allowed BPF_CORE_READ fields may be read", m)
	}
	// The message buffer appears once: in tcp_sendmsg's signature.
	if n := len(msgUse.FindAllString(src, -1)); n != 1 {
		t.Errorf("msg appears %d times, want once (the signature): its content must never be read", n)
	}
	sort.Strings(helpers)
	t.Logf("reads: %v; helpers: %v", reads, helpers)
}
