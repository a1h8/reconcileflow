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
//
// Phase 4 (docs/target/correlation-phase4-spec.md §3) adds two connection modes:
// -http1 sends HTTP/1.1 on keep-alive connections (one request at a time per
// connection), -conns k spreads HTTP/2 requests over k connections in turn.
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
	baggage := flag.Bool("baggage", false, "send a W3C baggage header exchange_id=<random 128-bit hex>, "+
		"drawn independently of trace_id, and log the value sent (records' baggage field)")
	warmup := flag.Int("warmup", 0, "requests sent first, at the same concurrency and through the same "+
		"client, to -warmup-out: the measured requests then start on open connections")
	warmupOut := flag.String("warmup-out", "", "JSONL output of the -warmup requests (required with -warmup)")
	http1 := flag.Bool("http1", false, "send HTTP/1.1 on keep-alive connections (ALPN http/1.1 only); a request "+
		"that negotiates anything else is a failure, and -require-http2 no longer applies")
	conns := flag.Int("conns", 1, "HTTP/2 connections the requests are spread over, in turn (one client each)")
	flag.Parse()
	if *warmup < 0 || (*warmup > 0) == (*warmupOut == "") {
		log.Fatal("-warmup must be >= 0, and -warmup-out given exactly when -warmup > 0")
	}
	if *concurrency <= 0 || *total <= 0 || *timeout <= 0 {
		log.Fatal("concurrency, requests and timeout must be positive")
	}
	if *conns < 1 || (*conns > 1 && (*http1 || !*pool)) || (*http1 && !*pool) {
		log.Fatal("-conns must be >= 1, and above 1 only with pooled HTTP/2; -http1 needs -pool")
	}
	if *seed != "variable" {
		log.Fatal("only seed-policy=variable is supported; fixed replay is not implemented")
	}

	want := wantAny
	switch {
	case *http1:
		want = wantH1
	case *requireHTTP2:
		want = wantH2
	}
	tracker := newConnTracker()
	clients := make([]*http.Client, *conns)
	for i := range clients {
		if *http1 {
			clients[i] = newHTTP1Client(*timeout, *concurrency)
		} else {
			clients[i] = newClient(*pool, *timeout)
		}
		defer clients[i].CloseIdleConnections()
	}
	send := func(n int, path string) (succeeded, failed, writeFailures int64) {
		return run(clients, tracker, path, n, *concurrency, *target, *control, *mask, *truthInPath, *baggage, want)
	}

	if *warmup > 0 {
		ok, bad, wf := send(*warmup, *warmupOut)
		log.Printf("warm-up: attempted=%d succeeded=%d failed=%d write_failures=%d", *warmup, ok, bad, wf)
	}
	succeeded, failed, writeFailures := send(*total, *outPath)
	log.Printf("done: attempted=%d succeeded=%d failed=%d write_failures=%d control=%q pool=%v mask=%v truthInPath=%v baggage=%v http1=%v conns=%d",
		*total, succeeded, failed, writeFailures, *control, *pool, *mask, *truthInPath, *baggage, *http1, *conns)
	if failed != 0 || writeFailures != 0 {
		os.Exit(1)
	}
}

// run sends n requests, at most concurrency in flight, request i through
// clients[i % len(clients)], and writes one record per attempt to outPath.
func run(clients []*http.Client, tracker *connTracker, outPath string, n, concurrency int,
	target, control string, mask, truthInPath, baggage bool, want wantProto) (int64, int64, int64) {
	w, err := oracle.NewWriter(outPath)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	var succeeded, failed, writeFailures atomic.Int64

	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		client := clients[i%len(clients)]
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			traceID := randHex(16) // 16 bytes = 32 hex chars, W3C trace-id
			parentID := randHex(8) // 8 bytes = 16 hex chars, W3C parent-id
			traceparent := fmt.Sprintf("00-%s-%s-01", traceID, parentID)

			path := "/"
			if truthInPath {
				path = "/truth-" + traceID
			}
			bag := ""
			if baggage {
				bag = "exchange_id=" + randHex(16)
			}
			record := performRequest(client, tracker, target+path, traceID, traceparent, bag, control, mask, want)
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
	return succeeded.Load(), failed.Load(), writeFailures.Load()
}

// wantProto is the protocol a response must have been served in.
type wantProto int

const (
	wantAny wantProto = iota
	wantH2
	wantH1
)

// newHTTP1Client opens at most idle connections and keeps them open between
// requests, so a run at concurrency c uses at most c connections instead of
// opening and closing more (a reopened connection may reuse a port, which
// phase 4 must tell apart by time).
func newHTTP1Client(timeout time.Duration, idle int) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"http/1.1"}}, // lab self-signed cert only
			TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},                  // non-nil and empty: no HTTP/2
			MaxIdleConnsPerHost: idle,
			MaxConnsPerHost:     idle, // a request waits for a free connection instead of opening one more
		},
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
func performRequest(client *http.Client, tracker *connTracker, target, traceID, traceparent, baggage, control string, mask bool, want wantProto) (record oracle.Record) {
	started := time.Now()
	record = oracle.Record{
		SchemaVersion: 2, Side: "load-gen", TraceID: traceID, Control: control, Baggage: baggage,
		TimestampNS: started.UnixNano(), StreamID: -1, IdentityKind: "local_ordinal",
	}
	defer func() { record.DurationNS = time.Since(started).Nanoseconds() }()
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			record.ConnKey = tracker.identify(info.Conn)
			record.WireRank = tracker.handOut(record.ConnKey)
		},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		record.Error = "build: " + err.Error()
		return
	}
	if !mask {
		req.Header.Set("traceparent", traceparent)
	}
	if baggage != "" {
		req.Header.Set("baggage", baggage)
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
	case want == wantH2 && resp.ProtoMajor != 2:
		record.Error = "protocol: expected HTTP/2, got " + resp.Proto
	case want == wantH1 && (resp.ProtoMajor != 1 || record.NegotiatedProtocol != "http/1.1"):
		record.Error = "protocol: expected HTTP/1.1, got " + resp.Proto + " (ALPN " + record.NegotiatedProtocol + ")"
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
	keyByConn  map[net.Conn]string      // actual connection identity; tuple reuse is a new generation
	streamSeq  map[string]*atomic.Int64 // completion ordinal
	wireSeq    map[string]*atomic.Int64 // hand-out order: the wire, in HTTP/1.1
}

func newConnTracker() *connTracker {
	return &connTracker{
		generation: map[string]int{},
		keyByConn:  map[net.Conn]string{},
		streamSeq:  map[string]*atomic.Int64{},
		wireSeq:    map[string]*atomic.Int64{},
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
		t.wireSeq[key] = &atomic.Int64{}
	}
	return key
}

// handOut counts a request handed the connection: its rank on the wire in
// HTTP/1.1 (phase 4 spec 8.20). -1 for a connection never identified.
func (t *connTracker) handOut(connKey string) int {
	t.mu.Lock()
	seq, ok := t.wireSeq[connKey]
	t.mu.Unlock()
	if !ok {
		return -1
	}
	return int(seq.Add(1))
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
