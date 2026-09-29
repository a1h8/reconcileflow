// load-gen is the M0 harness's authoritative client (docs/target/ground-truth-eval-plane-v3.md
// §1): it originates every trace_id. Its local request ordinals are NOT
// protocol stream IDs; protocol-level GT2 capture remains unimplemented.
//
// Concurrency defaults to 100 (docs/target/m0-evaluation-run-001.md §1) specifically to
// stress HTTP/2 connection pooling — the condition under which the fallback-tier
// connection_instance (5-tuple + start + generation) is most likely to misidentify a
// stream. -control lets a run be tagged as a witness cell:
//
//	positive : -pool=false forces one TCP connection per request (no multiplexing) —
//	           D_acc must land near 100% here, or the oracle itself is broken, not OBI.
//	negative : tag only; the actual shuffle of expected_trace_id happens in the offline
//	           join (cmd/join), never here — load-gen must never fabricate wrong data,
//	           only label it for the join step to corrupt on purpose.
//
// -mask implements the Masque A2 calibration read (ground-truth-eval-plane-v3.md §6):
// simulates OBI's direct-propagation failure mode by withholding the traceparent header,
// while still logging the true trace_id as ground truth — a correlator calibrated on this
// is judged against the oracle, never against its own guesses.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"reconcileflow/m0-harness/internal/oracle"
)

func main() {
	target := flag.String("target", "https://localhost:8443", "fake-upstream base URL")
	outPath := flag.String("out", "load-gen.jsonl", "ground-truth JSONL output path")
	concurrency := flag.Int("concurrency", 100, "concurrent requests in flight")
	total := flag.Int("requests", 2000, "total requests to send")
	pool := flag.Bool("pool", true, "reuse HTTP/2 connections (false = positive control: one conn per request)")
	control := flag.String("control", "", "witness cell label: \"\" | positive | negative")
	mask := flag.Bool("mask", false, "Masque A2 (ground-truth-eval-plane-v3.md §6): don't send the "+
		"traceparent header, simulating propagation loss — the JSONL record still logs the true "+
		"trace_id/conn_key/stream_id/timestamp as ground truth, only the wire doesn't carry it")
	truthInPath := flag.Bool("truth-in-path", false, "embed trace_id in the request path (e.g. /truth-<id>), "+
		"visible in OBI's trace_printer output regardless of header propagation — an independent ground-truth "+
		"channel for scoring a correlator's blind (port+timing-only) guess, never fed to the correlator itself. "+
		"fake-upstream's handler is a catch-all, so any path works unmodified")
	seed := flag.String("seed-policy", "variable", "variable only; fixed replay is not implemented")
	timeout := flag.Duration("timeout", 30*time.Second, "deadline per request, including response body")
	requireHTTP2 := flag.Bool("require-http2", true, "mark a request failed unless HTTP/2 was negotiated")
	flag.Parse()
	if *concurrency <= 0 || *total <= 0 || *timeout <= 0 {
		log.Fatal("concurrency, requests and timeout must be positive")
	}
	if *seed != "variable" {
		log.Fatal("only seed-policy=variable is supported; fixed replay is not implemented")
	}

	w, err := oracle.NewWriter(*outPath)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}

	tracker := newConnTracker()
	client := newClient(*pool, *timeout)
	defer client.CloseIdleConnections()

	var wg sync.WaitGroup
	sem := make(chan struct{}, *concurrency)
	var succeeded, failed, writeFailures atomic.Int64

	for i := 0; i < *total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			traceID := randHex(16) // 16 bytes = 32 hex chars, W3C trace-id
			parentID := randHex(8) // 8 bytes = 16 hex chars, W3C parent-id
			traceparent := fmt.Sprintf("00-%s-%s-01", traceID, parentID)

			path := "/"
			if *truthInPath {
				path = "/truth-" + traceID
			}
			record := performRequest(client, tracker, *target+path, traceID, traceparent, *control, *mask, *requireHTTP2)
			if err := w.Write(record); err != nil {
				writeFailures.Add(1)
				log.Printf("write record: %v", err)
			}
			if record.Error != "" {
				failed.Add(1)
			} else {
				succeeded.Add(1)
			}
		}()
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		writeFailures.Add(1)
		log.Printf("close output: %v", err)
	}
	log.Printf("done: attempted=%d succeeded=%d failed=%d write_failures=%d control=%q pool=%v mask=%v truthInPath=%v",
		*total, succeeded.Load(), failed.Load(), writeFailures.Load(), *control, *pool, *mask, *truthInPath)
	if failed.Load() != 0 || writeFailures.Load() != 0 {
		os.Exit(1)
	}
}

func newClient(pool bool, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, // lab self-signed cert only
			ForceAttemptHTTP2: true,
			DisableKeepAlives: !pool,
		},
	}
}

// One record for every attempted request, including build, transport and body failures.
// StreamID remains a local completion ordinal, NEVER a protocol stream ID.
func performRequest(client *http.Client, tracker *connTracker, target, traceID, traceparent, control string, mask, requireHTTP2 bool) (record oracle.Record) {
	started := time.Now()
	record = oracle.Record{
		SchemaVersion: 2, Side: "load-gen", TraceID: traceID, Control: control,
		TimestampNS: started.UnixNano(), StreamID: -1, IdentityKind: "local_ordinal",
	}
	defer func() { record.DurationNS = time.Since(started).Nanoseconds() }()
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { record.ConnKey = tracker.identify(info.Conn) },
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		record.Error = "build: " + err.Error()
		return
	}
	if !mask {
		req.Header.Set("traceparent", traceparent)
	}
	if control != "" {
		req.Header.Set("X-Witness-Control", control)
	}
	resp, err := client.Do(req)
	if err != nil {
		record.Error = "transport: " + err.Error()
		return
	}
	record.Protocol = resp.Proto
	record.StatusCode = resp.StatusCode
	if resp.TLS != nil {
		record.NegotiatedProtocol = resp.TLS.NegotiatedProtocol
	}
	record.ResponseSize, err = io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	record.StreamID = tracker.nextStream(record.ConnKey)
	switch {
	case err != nil:
		record.Error = "body: " + err.Error()
	case closeErr != nil:
		record.Error = "close body: " + closeErr.Error()
	case requireHTTP2 && resp.ProtoMajor != 2:
		record.Error = "protocol: expected HTTP/2, got " + resp.Proto
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		record.Error = fmt.Sprintf("status: %d", resp.StatusCode)
	}
	return
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failing means the environment is broken, not recoverable here
	}
	return hex.EncodeToString(b)
}

// connTracker mirrors fake-upstream's identity scheme from the client's vantage
// point: LocalAddr (client) + RemoteAddr (server) is the same string on both
// sides of one TCP connection, just assembled from opposite ends — see
// internal/oracle for the server-side half.
type connTracker struct {
	mu         sync.Mutex
	generation map[string]int
	keyByConn  map[net.Conn]string // actual connection identity; tuple reuse is a new generation
	streamSeq  map[string]*atomic.Int64
}

func newConnTracker() *connTracker {
	return &connTracker{
		generation: map[string]int{},
		keyByConn:  map[net.Conn]string{},
		streamSeq:  map[string]*atomic.Int64{},
	}
}

func (t *connTracker) identify(c net.Conn) string {
	fiveTuple := c.LocalAddr().String() + "-" + c.RemoteAddr().String()
	t.mu.Lock()
	defer t.mu.Unlock()
	// Reusing the same connection preserves identity; a distinct connection
	// recycling the tuple increments its generation. This still cannot prove
	// agreement with a separate observer that may see unused connections.
	key, seen := t.keyByConn[c]
	if !seen {
		gen := t.generation[fiveTuple]
		t.generation[fiveTuple] = gen + 1
		key = oracle.ConnKey(fiveTuple, gen)
		t.keyByConn[c] = key
		t.streamSeq[key] = &atomic.Int64{}
	}
	return key
}

func (t *connTracker) nextStream(connKey string) int {
	t.mu.Lock()
	seq, ok := t.streamSeq[connKey]
	t.mu.Unlock()
	if !ok {
		return -1 // identify() must run first via GotConn; -1 marks a tracking bug, not a valid stream
	}
	return int(seq.Add(1))
}
