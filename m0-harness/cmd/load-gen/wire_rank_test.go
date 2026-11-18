package main

import (
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// A request's wire rank counts the requests handed its connection, in the
// order they get it (phase 4 spec 8.20), apart from the completion ordinal.
func TestWireRanksCountHandOutsPerConnection(t *testing.T) {
	tracker := newConnTracker()
	k1 := tracker.identify(&testConn{})
	k2 := tracker.identify(&testConn{})
	if tracker.handOut(k1) != 1 || tracker.handOut(k1) != 2 || tracker.handOut(k2) != 1 {
		t.Fatal("wire ranks must be per connection")
	}
	if tracker.nextStream(k1) != 1 {
		t.Fatal("the completion ordinal is counted apart")
	}
	if tracker.handOut("never-identified|0") != -1 {
		t.Fatal("an unidentified connection is marked, not guessed")
	}
}

// In HTTP/1.1 a connection serves one request at a time and goes back to the
// pool once the answer is read: the order it is handed out is the order on
// the wire, which the server sees. The completion ordinal is not (at c800 a
// request can take a connection back and finish before the one before it has
// taken its number: spec 8.20).
func TestWireRanksAreTheOrderTheServerSawOnEachConnection(t *testing.T) {
	const n, c = 300, 8
	var mu sync.Mutex
	order := map[string][]string{} // client address -> trace ids in arrival order
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tid := strings.Split(r.Header.Get("traceparent"), "-")[1]
		mu.Lock()
		order[r.RemoteAddr] = append(order[r.RemoteAddr], tid)
		mu.Unlock()
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
		fmt.Fprint(w, "ok")
	}))
	server.TLS = &tls.Config{NextProtos: []string{"http/1.1"}}
	server.StartTLS()
	t.Cleanup(server.Close)
	out := filepath.Join(t.TempDir(), "lg.jsonl")
	if code := runLoadGen(t, "-target", server.URL, "-out", out, "-requests", fmt.Sprint(n),
		"-concurrency", fmt.Sprint(c), "-http1"); code != 0 {
		t.Fatalf("exit %d", code)
	}
	mu.Lock()
	defer mu.Unlock()
	rank := map[string]int{}
	for addr, tids := range order {
		for i, tid := range tids {
			rank[addr+" "+tid] = i + 1
		}
	}
	records := readLog(t, out)
	if len(records) != n {
		t.Fatalf("%d records for %d requests", len(records), n)
	}
	for _, r := range records {
		local := strings.SplitN(r.ConnKey, "-", 2)[0]
		if want := rank[local+" "+r.TraceID]; r.WireRank != want || want == 0 {
			t.Fatalf("%s on %s: wire rank %d, the server saw it %d", r.TraceID, r.ConnKey, r.WireRank, want)
		}
	}
}
