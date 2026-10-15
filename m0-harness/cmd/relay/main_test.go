package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The whole point of relay: the next hop sees the path, and none of the
// context headers the client sent.
func TestRelayForwardsThePathAndNoHeader(t *testing.T) {
	type seen struct{ path, traceparent, baggage, proto string }
	got := make(chan seen, 1)
	next := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- seen{r.URL.RequestURI(), r.Header.Get("traceparent"), r.Header.Get("baggage"), r.Proto}
		w.WriteHeader(http.StatusTeapot)
		io.WriteString(w, "body from next hop")
	}))
	next.EnableHTTP2 = true
	next.StartTLS()
	defer next.Close()

	front := httptest.NewUnstartedServer(newHandler(newClient(time.Second), next.URL))
	front.EnableHTTP2 = true
	front.StartTLS()
	defer front.Close()

	req, _ := http.NewRequest(http.MethodGet, front.URL+"/truth-abc?q=1", nil)
	req.Header.Set("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	req.Header.Set("baggage", "exchange_id=xyz")
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	s := <-got
	if s.path != "/truth-abc?q=1" {
		t.Fatalf("next hop saw path %q", s.path)
	}
	if s.traceparent != "" || s.baggage != "" {
		t.Fatalf("relay copied headers: traceparent %q, baggage %q", s.traceparent, s.baggage)
	}
	if s.proto != "HTTP/2.0" {
		t.Fatalf("relay -> next hop used %s, want HTTP/2.0", s.proto)
	}
	if resp.StatusCode != http.StatusTeapot || string(body) != "body from next hop" {
		t.Fatalf("relay returned %d %q", resp.StatusCode, body)
	}
}

func TestRelayReportsAnUnreachableNextHop(t *testing.T) {
	front := httptest.NewServer(newHandler(newClient(time.Second), "https://127.0.0.1:1"))
	defer front.Close()
	resp, err := http.Get(front.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", resp.StatusCode)
	}
}
