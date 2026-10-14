// relay is an application hop for the propagation measurements: it receives a
// request and makes a NEW outbound request to the next hop, with the same
// method and path and no header copied.
//
// A proxy such as Traefik forwards request headers itself, so a chain of
// proxies carries traceparent and baggage with or without an agent. A real
// service builds its own outbound request: whatever context reaches the next
// hop is then put there by instrumentation (an SDK, or an eBPF agent), not by
// the application. relay is that case, with nothing else in it.
//
// With -fresh-traceparent it plays an application that decides its own trace
// context at a trust boundary: every outbound request carries a NEW random
// traceparent, and -log records, per request, the path and that fresh trace
// id, so the measurement can tell which id reached the inside.
//
// It serves HTTP/2 over a throwaway self-signed certificate and calls the next
// hop over pooled HTTP/2, like the rest of the harness. The outbound request
// is made from the handler's own goroutine with the incoming request's
// context, as an ordinary service would.
package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"reconcileflow/m0-harness/internal/labtls"
	"reconcileflow/m0-harness/internal/oracle"
)

func newClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, // lab self-signed certs only
			ForceAttemptHTTP2: true,
		},
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(b)
}

// newHandler relays each request to next. No incoming header is copied. With a
// non-nil fresh writer, each outbound request gets a new random traceparent,
// logged with the request path.
func newHandler(client *http.Client, next string, fresh *oracle.Writer) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		out, err := http.NewRequestWithContext(r.Context(), r.Method, next+r.URL.RequestURI(), nil)
		if err != nil {
			http.Error(rw, "relay: build: "+err.Error(), http.StatusBadGateway)
			return
		}
		if fresh != nil {
			traceID := randHex(16)
			out.Header.Set("traceparent", fmt.Sprintf("00-%s-%s-01", traceID, randHex(8)))
			if err := fresh.Write(oracle.Record{
				SchemaVersion: 2, Side: "relay-fresh", TraceID: traceID, Path: r.URL.Path,
				TimestampNS: time.Now().UnixNano(), StreamID: -1,
			}); err != nil {
				http.Error(rw, "relay: log: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
		resp, err := client.Do(out)
		if err != nil {
			http.Error(rw, "relay: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		rw.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(rw, resp.Body); err != nil {
			log.Printf("relay: copy body: %v", err)
		}
	})
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9443", "listen address")
	next := flag.String("next", "", "next hop base URL, e.g. https://127.0.0.1:8443")
	timeout := flag.Duration("timeout", 30*time.Second, "deadline per outbound request")
	freshTP := flag.Bool("fresh-traceparent", false, "set a new random traceparent on every outbound request")
	logPath := flag.String("log", "relay-fresh.jsonl", "with -fresh-traceparent: per-request path and fresh trace id")
	flag.Parse()
	if *next == "" {
		log.Fatal("-next is required")
	}
	var fresh *oracle.Writer
	if *freshTP {
		w, err := oracle.NewWriter(*logPath)
		if err != nil {
			log.Fatalf("open log: %v", err)
		}
		defer w.Close()
		fresh = w
	}

	cert, err := labtls.SelfSigned("relay.m0-harness.local")
	if err != nil {
		log.Fatalf("self-signed cert: %v", err)
	}
	client := newClient(*timeout)
	srv := &http.Server{
		Addr:      *addr,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		Handler:   newHandler(client, *next, fresh),
	}
	log.Printf("relay listening on %s, forwarding to %s (no header copied, fresh traceparent: %v)",
		*addr, *next, *freshTP)
	log.Fatal(srv.ListenAndServeTLS("", ""))
}
