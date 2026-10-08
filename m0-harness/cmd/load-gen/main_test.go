package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"reconcileflow/m0-harness/internal/oracle"
)

func TestRequestAccounting(t *testing.T) {
	for _, tc := range []struct {
		name      string
		h2        bool
		status    int
		truncated bool
		wantError bool
	}{
		{name: "HTTP2", h2: true, status: 200},
		{name: "HTTP1 rejected", status: 200, wantError: true},
		{name: "status failure", h2: true, status: 503, wantError: true},
		{name: "truncated body", h2: true, status: 200, truncated: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("traceparent") != "parent" {
					t.Error("header lost")
				}
				if tc.truncated {
					w.Header().Set("Content-Length", "100")
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, "ok")
			}))
			server.EnableHTTP2 = tc.h2
			server.StartTLS()
			defer server.Close()
			client := newClient(true, time.Second)
			defer client.CloseIdleConnections()
			r := performRequest(client, newConnTracker(), server.URL, "truth", "parent", "", false, true)
			if (r.Error != "") != tc.wantError {
				t.Fatalf("record: %+v", r)
			}
			if r.TraceID != "truth" || r.SchemaVersion != 2 || r.TimestampNS == 0 || r.DurationNS <= 0 || r.StatusCode != tc.status {
				t.Fatalf("missing accounting: %+v", r)
			}
			if tc.h2 && (r.Protocol != "HTTP/2.0" || r.NegotiatedProtocol != "h2") {
				t.Fatalf("not H2: %+v", r)
			}
		})
	}
}

func TestFailuresKeepIdentity(t *testing.T) {
	client := newClient(true, 50*time.Millisecond)
	defer client.CloseIdleConnections()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	for _, target := range []string{"://invalid", server.URL} {
		r := performRequest(client, newConnTracker(), target, "truth", "parent", "", false, true)
		if r.Error == "" || r.TraceID != "truth" || r.TimestampNS == 0 || r.DurationNS <= 0 {
			t.Fatalf("failure disappeared: %+v", r)
		}
	}
}

type testConn struct{ net.Conn }

func (*testConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234} }
func (*testConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8443} }
func TestConnectionReuseAndTupleRecycling(t *testing.T) {
	tracker := newConnTracker()
	first, second := &testConn{}, &testConn{}
	k1 := tracker.identify(first)
	if tracker.identify(first) != k1 {
		t.Fatal("pool reuse changed identity")
	}
	k2 := tracker.identify(second)
	if k1 == k2 {
		t.Fatal("recycled tuple reused old identity")
	}
	if tracker.nextStream(k1) != 1 || tracker.nextStream(k1) != 2 || tracker.nextStream(k2) != 1 {
		t.Fatal("ordinals must be per connection")
	}
}

func TestNoPoolingUsesDistinctConnections(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := newClient(false, time.Second)
	defer client.CloseIdleConnections()
	tracker := newConnTracker()
	a := performRequest(client, tracker, server.URL, "a", "parent", "", false, true)
	b := performRequest(client, tracker, server.URL, "b", "parent", "", false, true)
	if a.Error != "" || b.Error != "" || a.ConnKey == b.ConnKey {
		t.Fatalf("no-pool witness: %+v %+v", a, b)
	}
}

func TestRedirectIsNotFollowedAndMaskKeepsTruthLocal(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/start" {
			t.Error("redirect was followed")
		}
		if r.Header.Get("traceparent") != "" {
			t.Error("masked context leaked onto wire")
		}
		http.Redirect(w, r, "/other", http.StatusFound)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := newClient(true, time.Second)
	defer client.CloseIdleConnections()
	record := performRequest(client, newConnTracker(), server.URL+"/start", "truth", "parent", "", true, true)
	if record.TraceID != "truth" || record.StatusCode != 302 || record.Error == "" {
		t.Fatalf("redirect must remain a failed logical attempt with its truth: %+v", record)
	}
}

func TestWitnessControlIsLabelledOnTheWire(t *testing.T) {
	got := make(chan string, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("X-Witness-Control")
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := newClient(true, time.Second)
	defer client.CloseIdleConnections()
	r := performRequest(client, newConnTracker(), server.URL, "truth", "parent", "positive", false, true)
	if r.Error != "" || r.Control != "positive" {
		t.Fatalf("record: %+v", r)
	}
	if h := <-got; h != "positive" {
		t.Fatalf("X-Witness-Control = %q, want positive", h)
	}
}

func TestNextStreamOnAnUnidentifiedConnectionIsMarkedNotGuessed(t *testing.T) {
	if got := newConnTracker().nextStream("never-identified|0"); got != -1 {
		t.Fatalf("nextStream = %d, want -1 (tracking bug marker)", got)
	}
}

func TestRandHexHasTheRequestedWidthAndVaries(t *testing.T) {
	a, b := randHex(16), randHex(16)
	if len(a) != 32 || len(b) != 32 || a == b {
		t.Fatalf("randHex(16) = %q, %q", a, b)
	}
	if _, err := hex.DecodeString(a); err != nil {
		t.Fatal(err)
	}
}

// --- main(), run as a subprocess (it calls log.Fatal / os.Exit) ----------------

func TestLoadGenCLIHelper(t *testing.T) {
	if os.Getenv("LOAD_GEN_CLI_HELPER") != "1" {
		t.Skip("subprocess entry point for runLoadGen")
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"load-gen"}, os.Args[i+1:]...)
			break
		}
	}
	main()
	os.Exit(0)
}

func runLoadGen(t *testing.T, args ...string) (exitCode int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestLoadGenCLIHelper$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "LOAD_GEN_CLI_HELPER=1")
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	if err != nil {
		t.Fatal(err)
	}
	return 0
}

// wire is what the server actually received: the traceparent header and path
// of every request, so the log can be checked against the wire, not itself.
type wire struct {
	mu           sync.Mutex
	traceparents []string
	paths        []string
}

func h2Server(t *testing.T, status int) (*httptest.Server, *wire) {
	t.Helper()
	seen := &wire{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.mu.Lock()
		seen.traceparents = append(seen.traceparents, r.Header.Get("traceparent"))
		seen.paths = append(seen.paths, r.URL.Path)
		seen.mu.Unlock()
		w.WriteHeader(status)
		fmt.Fprint(w, "ok")
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, seen
}

func readLog(t *testing.T, path string) []oracle.Record {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []oracle.Record
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		var r oracle.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("bad line %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

var traceparentRE = regexp.MustCompile(`^00-([0-9a-f]{32})-[0-9a-f]{16}-01$`)

// The ground truth is only ground truth if the trace_id logged is exactly the
// one that went out on the wire -- checked against what the server received,
// one record per request, no duplicates.
func TestLoadGenLogsExactlyTheTraceIDsSentOnTheWire(t *testing.T) {
	const n = 60
	server, seen := h2Server(t, http.StatusOK)
	out := filepath.Join(t.TempDir(), "lg.jsonl")
	if code := runLoadGen(t, "-target", server.URL, "-out", out, "-requests", fmt.Sprint(n), "-concurrency", "8"); code != 0 {
		t.Fatalf("exit %d", code)
	}

	records := readLog(t, out)
	if len(records) != n {
		t.Fatalf("%d records for %d requests", len(records), n)
	}
	logged := map[string]bool{}
	for _, r := range records {
		if r.Error != "" || r.Side != "load-gen" || r.ConnKey == "" || r.StreamID < 1 {
			t.Fatalf("record: %+v", r)
		}
		if logged[r.TraceID] {
			t.Fatalf("trace_id %s logged twice", r.TraceID)
		}
		logged[r.TraceID] = true
	}

	seen.mu.Lock()
	defer seen.mu.Unlock()
	if len(seen.traceparents) != n {
		t.Fatalf("server saw %d requests, want %d", len(seen.traceparents), n)
	}
	for _, tp := range seen.traceparents {
		m := traceparentRE.FindStringSubmatch(tp)
		if m == nil {
			t.Fatalf("malformed traceparent on the wire: %q", tp)
		}
		if !logged[m[1]] {
			t.Fatalf("trace_id %s went out on the wire but is not in the log", m[1])
		}
	}
}

// Failed requests are recorded, not dropped, and the run exits non-zero.
func TestLoadGenRecordsFailuresAndExitsNonZero(t *testing.T) {
	const n = 20
	server, _ := h2Server(t, http.StatusBadGateway)
	out := filepath.Join(t.TempDir(), "lg.jsonl")
	if code := runLoadGen(t, "-target", server.URL, "-out", out, "-requests", fmt.Sprint(n), "-concurrency", "4"); code == 0 {
		t.Fatal("exit 0 although every request failed")
	}
	records := readLog(t, out)
	if len(records) != n {
		t.Fatalf("%d records for %d failed requests", len(records), n)
	}
	for _, r := range records {
		if r.Error != "status: 502" {
			t.Fatalf("record: %+v", r)
		}
	}
}

func TestLoadGenMaskAndTruthInPath(t *testing.T) {
	const n = 10
	server, seen := h2Server(t, http.StatusOK)
	out := filepath.Join(t.TempDir(), "lg.jsonl")
	if code := runLoadGen(t, "-target", server.URL, "-out", out, "-requests", fmt.Sprint(n),
		"-concurrency", "2", "-mask", "-truth-in-path", "-control", "negative"); code != 0 {
		t.Fatalf("exit %d", code)
	}
	records := readLog(t, out)
	paths := map[string]bool{}
	seen.mu.Lock()
	for i, tp := range seen.traceparents {
		if tp != "" {
			t.Errorf("masked run leaked traceparent %q", tp)
		}
		paths[seen.paths[i]] = true
	}
	seen.mu.Unlock()
	for _, r := range records {
		if r.Control != "negative" || !paths["/truth-"+r.TraceID] {
			t.Fatalf("record %+v has no matching /truth- path on the wire", r)
		}
	}
}

// Invalid configuration fails before the output file is created, so a
// rejected run never leaves an empty ground-truth log behind.
func TestLoadGenRejectsInvalidConfigurationWithoutWritingALog(t *testing.T) {
	for _, args := range [][]string{
		{"-requests", "0"},
		{"-concurrency", "0"},
		{"-timeout", "0s"},
		{"-seed-policy", "fixed"},
	} {
		out := filepath.Join(t.TempDir(), "lg.jsonl")
		if code := runLoadGen(t, append(args, "-out", out)...); code == 0 {
			t.Fatalf("%v: exit 0", args)
		}
		if _, err := os.Stat(out); !os.IsNotExist(err) {
			t.Fatalf("%v: output file created (%v)", args, err)
		}
	}
}

func TestLoadGenFailsOnAnUnwritableOutput(t *testing.T) {
	out := filepath.Join(t.TempDir(), "missing-dir", "lg.jsonl")
	if code := runLoadGen(t, "-out", out, "-requests", "1"); code == 0 {
		t.Fatal("exit 0 with an unwritable output path")
	}
}
