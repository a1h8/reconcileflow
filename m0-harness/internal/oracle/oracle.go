// Package oracle implements the GT2 ground-truth record shared by load-gen
// (authoritative: it originates trace_id) and fake-upstream (observed: it
// receives the connection/stream and can only infer identity from userspace).
//
// Protocol reference: docs/target/ground-truth-eval-plane-v3.md, oracle.primary = GT2,
// (connection_instance, stream_id) -> expected_trace_id.
//
// Limitation, stated plainly: this harness implements the FALLBACK connection-instance
// tier only (5tuple, connection_start, generation) — pure userspace Go has no access to
// boot_id/network_namespace/socket_cookie, which need an eBPF probe (CO-RE, root). The
// strong tier is not built here; do not report results from this harness as "OBI direct"
// evidence, only as the fallback-tier baseline the protocol explicitly allows.
package oracle

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// ConnectionInstance is the fallback-tier identity (protocol §1, oracle.connection_instance.fallback).
//
// ConnectionStartNS is metadata only, deliberately excluded from Key(): it is
// measured independently on each side (server accept time vs. client dial
// time, different clocks) and can never be bit-identical across processes.
// Only FiveTuple+Generation is safe to join on, because both sides observe
// 5-tuple recurrence in the same relative order.
type ConnectionInstance struct {
	FiveTuple         string `json:"five_tuple"` // "srcIP:srcPort-dstIP:dstPort"
	ConnectionStartNS int64  `json:"connection_start_ns"`
	Generation        int    `json:"generation"` // increments if the 5-tuple is reused (TIME_WAIT reuse)
}

func (c ConnectionInstance) Key() string {
	return ConnKey(c.FiveTuple, c.Generation)
}

// ConnKey is the single source of truth for the join key format — both
// fake-upstream and load-gen must call this, not build the string by hand,
// since the two sides can never agree on a format they each derive independently.
func ConnKey(fiveTuple string, generation int) string {
	return fmt.Sprintf("%s|%d", fiveTuple, generation)
}

// Record is one line of the ground-truth JSONL log. Emitted by both sides;
// joined offline on (connection_instance_key, stream_id).
type Record struct {
	Side        string `json:"side"` // "load-gen" | "fake-upstream"
	ConnKey     string `json:"conn_key"`
	StreamID    int    `json:"stream_id"` // per-connection request sequence number
	TraceID     string `json:"trace_id"`  // W3C traceparent trace-id field
	TimestampNS int64  `json:"timestamp_ns"`
	Control     string `json:"control,omitempty"` // "" | "positive" | "negative" — witness cell tag
	// DurationNS is load-gen's own client-measured round-trip time (send to
	// response received) — a correlation signal independent of temporal
	// proximity: injectLatencyProfileC gives each request a genuinely
	// different duration, and OBI reports its own server-side duration for
	// every event regardless of trace_id propagation, so the two can be
	// compared even when the header is lost. Zero/omitted on fake-upstream's
	// side, which has no round-trip of its own to measure.
	DurationNS int64 `json:"duration_ns,omitempty"`
	// ResponseSize is the response body length in bytes — on fake-upstream's
	// side, the size it generated (randomBody, independent of trace_id); on
	// load-gen's side, the size it actually received. A correlation signal
	// orthogonal to DurationNS: content-based, not timing-based.
	ResponseSize int64 `json:"response_size,omitempty"`
}

// Writer is a concurrency-safe JSONL appender — both binaries write to their
// own file; the join happens as a separate offline step, never in-process,
// so the harness itself never becomes the thing that could bias the measurement.
type Writer struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, enc: json.NewEncoder(f)}, nil
}

func (w *Writer) Write(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(r)
}

func (w *Writer) Close() error {
	return w.f.Close()
}
