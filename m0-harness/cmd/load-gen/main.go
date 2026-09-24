// load-gen is the M0 harness's authoritative client (docs/target/ground-truth-eval-plane-v3.md
// §1, oracle GT2): it originates every trace_id, so its own log is ground truth by
// construction — no inference needed on this side, only on fake-upstream's.
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
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
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
	seed := flag.String("seed-policy", "variable", "fixed | variable (docs/target §1) — fixed reseeds identically per run, variable does not")
	flag.Parse()

	w, err := oracle.NewWriter(*outPath)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer w.Close()

	tracker := newConnTracker()
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, // lab self-signed cert only
		DisableKeepAlives: !*pool,
	}
	client := &http.Client{Transport: transport}

	if *seed == "fixed" {
		log.Printf("seed_policy=fixed: this run's request sequence is meant to be replayed identically; " +
			"determinism is the caller's responsibility (fixed request bodies/order), the harness does not seed a PRNG here")
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, *concurrency)
	var sent atomic.Int64

	for i := 0; i < *total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			traceID := randHex(16) // 16 bytes = 32 hex chars, W3C trace-id
			parentID := randHex(8) // 8 bytes = 16 hex chars, W3C parent-id
			traceparent := fmt.Sprintf("00-%s-%s-01", traceID, parentID)

			var connKey string
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
				GotConn: func(info httptrace.GotConnInfo) {
					connKey = tracker.identify(info.Conn)
				},
			})

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, *target+"/", nil)
			if err != nil {
				log.Printf("build request: %v", err)
				return
			}
			if !*mask {
				req.Header.Set("traceparent", traceparent)
			}
			if *control != "" {
				req.Header.Set("X-Witness-Control", *control)
			}

			sentAt := time.Now().UnixNano()
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("request failed: %v", err)
				return
			}
			resp.Body.Close()

			streamID := tracker.nextStream(connKey)
			_ = w.Write(oracle.Record{
				Side:        "load-gen",
				ConnKey:     connKey,
				StreamID:    streamID,
				TraceID:     traceID,
				TimestampNS: sentAt,
				Control:     *control,
			})
			sent.Add(1)
		}()
	}
	wg.Wait()
	log.Printf("done: %d/%d requests sent, control=%q pool=%v mask=%v", sent.Load(), *total, *control, *pool, *mask)
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
	keyByTuple map[string]string // caches the assigned key per 5-tuple for this run
	streamSeq  map[string]*atomic.Int64
}

func newConnTracker() *connTracker {
	return &connTracker{
		generation: map[string]int{},
		keyByTuple: map[string]string{},
		streamSeq:  map[string]*atomic.Int64{},
	}
}

func (t *connTracker) identify(c net.Conn) string {
	fiveTuple := c.LocalAddr().String() + "-" + c.RemoteAddr().String()
	t.mu.Lock()
	defer t.mu.Unlock()
	// Mirrors fake-upstream's nextGeneration exactly: first time a 5-tuple is
	// seen -> generation 0, incremented only on 5-tuple reuse. Both sides must
	// agree on this rule since only the rule (not a shared counter) is what
	// keeps generation numbers aligned across the two processes.
	key, seen := t.keyByTuple[fiveTuple]
	if !seen {
		gen := t.generation[fiveTuple]
		t.generation[fiveTuple] = gen + 1
		key = oracle.ConnKey(fiveTuple, gen)
		t.keyByTuple[fiveTuple] = key
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
