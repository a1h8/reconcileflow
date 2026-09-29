package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
