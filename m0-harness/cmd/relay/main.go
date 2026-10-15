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
// It serves HTTP/2 over a throwaway self-signed certificate and calls the next
// hop over pooled HTTP/2, like the rest of the harness. The outbound request
// is made from the handler's own goroutine with the incoming request's
// context, as an ordinary service would.
package main

import (
	"crypto/tls"
	"flag"
	"io"
	"log"
	"net/http"
	"time"

	"reconcileflow/m0-harness/internal/labtls"
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

// newHandler relays each request to next. No incoming header is copied.
func newHandler(client *http.Client, next string) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		out, err := http.NewRequestWithContext(r.Context(), r.Method, next+r.URL.RequestURI(), nil)
		if err != nil {
			http.Error(rw, "relay: build: "+err.Error(), http.StatusBadGateway)
			return
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
	flag.Parse()
	if *next == "" {
		log.Fatal("-next is required")
	}

	cert, err := labtls.SelfSigned("relay.m0-harness.local")
	if err != nil {
		log.Fatalf("self-signed cert: %v", err)
	}
	client := newClient(*timeout)
	srv := &http.Server{
		Addr:      *addr,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		Handler:   newHandler(client, *next),
	}
	log.Printf("relay listening on %s, forwarding to %s (no header copied)", *addr, *next)
	log.Fatal(srv.ListenAndServeTLS("", ""))
}
