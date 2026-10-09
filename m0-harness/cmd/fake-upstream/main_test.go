package main

import (
	"bufio"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"reconcileflow/m0-harness/internal/oracle"
)

func TestParseTraceparent(t *testing.T) {
	for _, tc := range []struct{ header, want string }{
		{"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "0af7651916cd43dd8448eb211c80319c"},
		{"", ""},                 // masked request: no header at all
		{"00-abc-def", ""},       // too few fields
		{"00-a-b-c-d", ""},       // too many fields
		{"garbage", ""},          // not a traceparent
		{"xx-not-hex-yy", "not"}, // field count is the only check: format is not validated
	} {
		if got := parseTraceparent(tc.header); got != tc.want {
			t.Errorf("parseTraceparent(%q) = %q, want %q", tc.header, got, tc.want)
		}
	}
}

// Generations are per five-tuple and must stay consistent under the
// concurrent ConnContext calls net/http makes, one per connection goroutine.
// The generation table is process-global, so tuples are made unique per run
// (otherwise -count=N would see the previous run's generations).
func TestNextGenerationIsPerTupleAndRaceFree(t *testing.T) {
	const calls = 200
	run := time.Now().UnixNano()
	tuple, other := fmt.Sprintf("test-gen-a-%d", run), fmt.Sprintf("test-gen-b-%d", run)
	var wg sync.WaitGroup
	seen := make([]bool, calls)
	var mu sync.Mutex
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gen := nextGeneration(tuple)
			mu.Lock()
			defer mu.Unlock()
			if gen < 0 || gen >= calls || seen[gen] {
				t.Errorf("generation %d handed out twice or out of range", gen)
				return
			}
			seen[gen] = true
		}()
	}
	wg.Wait()
	if gen := nextGeneration(other); gen != 0 {
		t.Fatalf("a fresh tuple starts at generation %d, want 0", gen)
	}
}

func TestRandomBodyStaysWithinItsDocumentedRange(t *testing.T) {
	sizes := map[int]bool{}
	for range 2000 {
		n := len(randomBody())
		if n < 50 || n > 5000 {
			t.Fatalf("body of %d bytes, want [50, 5000]", n)
		}
		sizes[n] = true
	}
	// Response size is used as a correlation signal: a constant would carry none.
	if len(sizes) < 100 {
		t.Fatalf("only %d distinct sizes over 2000 bodies", len(sizes))
	}
}

func TestSelfSignedCertServesLocalhost(t *testing.T) {
	cert, err := selfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := leaf.VerifyHostname("localhost"); err != nil {
		t.Fatal(err)
	}
	if time.Until(leaf.NotAfter) <= 0 {
		t.Fatal("certificate already expired")
	}
}

// --- cross-binary integration -----------------------------------------------

func buildBinary(t *testing.T, dir, name, pkg string) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}
	out := filepath.Join(dir, name)
	if b, err := exec.Command("go", "build", "-o", out, pkg).CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, b)
	}
	return out
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func readJSONL(t *testing.T, path string) []oracle.Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []oracle.Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r oracle.Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		out = append(out, r)
	}
	return out
}

type run struct {
	loadGen, fakeUpstream []oracle.Record
	loadGenPath, fuPath   string
	bin                   string
}

// pipeline starts a real fake-upstream, drives it with a real load-gen and
// returns both logs once the server has been stopped.
func pipeline(t *testing.T, loadGenArgs ...string) run {
	t.Helper()
	dir := t.TempDir()
	fu := buildBinary(t, dir, "fake-upstream", ".")
	lg := buildBinary(t, dir, "load-gen", "../load-gen")
	addr := freeAddr(t)
	r := run{
		loadGenPath: filepath.Join(dir, "lg.jsonl"),
		fuPath:      filepath.Join(dir, "fu.jsonl"),
		bin:         dir,
	}

	server := exec.Command(fu, "-addr", addr, "-out", r.fuPath)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			server.Process.Kill()
			server.Wait()
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fake-upstream never listened on %s", addr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	args := append([]string{"-target", "https://" + addr, "-out", r.loadGenPath}, loadGenArgs...)
	if b, err := exec.Command(lg, args...).CombinedOutput(); err != nil {
		t.Fatalf("load-gen: %v\n%s", err, b)
	}
	// load-gen only exits once every response body is read, and fake-upstream
	// writes its record before the response: both logs are complete here.
	stop()
	r.loadGen, r.fakeUpstream = readJSONL(t, r.loadGenPath), readJSONL(t, r.fuPath)
	return r
}

func byTraceID(t *testing.T, records []oracle.Record) map[string]oracle.Record {
	t.Helper()
	out := make(map[string]oracle.Record, len(records))
	for _, r := range records {
		if _, dup := out[r.TraceID]; dup || r.TraceID == "" {
			t.Fatalf("trace_id %q missing or not unique", r.TraceID)
		}
		out[r.TraceID] = r
	}
	return out
}

// The positive-control witness: one connection per request. The two binaries
// derive (conn_key, stream_id) independently, from opposite ends of the
// socket; trace_id, which both see, is the referee. Every request must agree,
// or the fallback-tier oracle is broken before OBI is even involved.
func TestPositiveControlKeysAgreeAcrossBinaries(t *testing.T) {
	const n = 100
	r := pipeline(t, "-pool=false", "-control", "positive", "-requests", fmt.Sprint(n), "-concurrency", "10")
	if len(r.loadGen) != n || len(r.fakeUpstream) != n {
		t.Fatalf("load-gen logged %d, fake-upstream %d, want %d each", len(r.loadGen), len(r.fakeUpstream), n)
	}
	observed := byTraceID(t, r.fakeUpstream)
	for _, want := range r.loadGen {
		got, ok := observed[want.TraceID]
		if !ok {
			t.Fatalf("trace_id %s sent but never observed", want.TraceID)
		}
		if got.ConnKey != want.ConnKey || got.StreamID != want.StreamID {
			t.Fatalf("trace_id %s: load-gen (%s, %d) vs fake-upstream (%s, %d)",
				want.TraceID, want.ConnKey, want.StreamID, got.ConnKey, got.StreamID)
		}
		if got.Control != "positive" || got.ResponseSize != want.ResponseSize {
			t.Fatalf("trace_id %s: control %q, sizes %d/%d", want.TraceID, got.Control, got.ResponseSize, want.ResponseSize)
		}
	}
}

// Under HTTP/2 multiplexing the connection key still agrees -- both ends
// format the same socket -- but the local ordinals do not: fake-upstream
// numbers a request on arrival, load-gen on completion, and the injected
// latency reorders them (measured 2026-10-04: 4/300 and 2/300 agreeing on
// a single pooled connection). Only conn_key is asserted; ordinal agreement
// is timing-dependent and is exactly why join refuses HTTP/2 records.
func TestPooledRunAgreesOnConnKeyOnly(t *testing.T) {
	const n = 100
	r := pipeline(t, "-requests", fmt.Sprint(n), "-concurrency", "20")
	observed := byTraceID(t, r.fakeUpstream)
	for _, want := range r.loadGen {
		if got := observed[want.TraceID]; got.ConnKey != want.ConnKey {
			t.Fatalf("trace_id %s: conn_key %q vs %q", want.TraceID, want.ConnKey, got.ConnKey)
		}
	}
}

// join refusing HTTP/2 is deliberate (docs/m0-demo-readiness.md section 2,
// "remains a historical diagnostic"). What this pins is the consequence:
// -require-http2=false only stops flagging HTTP/2 as missing, it does not
// downgrade -- fake-upstream always offers h2 and ForceAttemptHTTP2 is on --
// so join cannot score any run of the current harness, not even the
// positive control whose keys the test above shows agreeing 100%.
func TestJoinCannotScoreTheCurrentHarnessOutput(t *testing.T) {
	r := pipeline(t, "-pool=false", "-require-http2=false", "-control", "positive", "-requests", "20", "-concurrency", "4")
	for _, rec := range r.loadGen {
		if rec.Protocol != "HTTP/2.0" {
			t.Fatalf("load-gen negotiated %q; if HTTP/1.1 is now reachable, revisit this test", rec.Protocol)
		}
	}
	join := buildBinary(t, r.bin, "join", "../join")
	if err := exec.Command(join, "-load-gen", r.loadGenPath, "-fake-upstream", r.fuPath).Run(); err == nil {
		t.Fatal("join accepted HTTP/2 records; update this test and the join README notes")
	}
}
