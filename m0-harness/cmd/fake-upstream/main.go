// fake-upstream is the M0 harness's controlled-latency HTTP/2 server
// (docs/target/ground-truth-eval-plane-v3.md §8/§9, profile C).
//
// It serves over TLS (self-signed, generated at startup — lab only, never
// deploy this cert) so the stdlib's HTTP/2 upgrade kicks in without any
// external dependency. It captures, per request, the fallback-tier
// connection_instance (5-tuple + connection_start + generation) and the
// per-connection stream sequence number, and logs them alongside whatever
// trace_id the client sent — that log is fake-upstream's half of the GT2
// join; load-gen writes the other half.
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"log"
	"math"
	"math/big"
	mrand "math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"reconcileflow/m0-harness/internal/oracle"
)

type connKey int

const connKeyCtx connKey = 0

type connState struct {
	fiveTuple  string
	startNS    int64
	generation int
	streamSeq  atomic.Int64
}

var (
	connCounter     atomic.Int64
	seenFiveTupleMu sync.Mutex
	seenFiveTuple   = map[string]int{} // fiveTuple -> next generation
)

// nextGeneration is synchronized: ConnContext runs in each connection's own
// serve() goroutine in net/http, concurrently across connections — unlike a
// plain map access, this must not race.
func nextGeneration(fiveTuple string) int {
	seenFiveTupleMu.Lock()
	defer seenFiveTupleMu.Unlock()
	gen := seenFiveTuple[fiveTuple]
	seenFiveTuple[fiveTuple] = gen + 1
	return gen
}

func main() {
	addr := flag.String("addr", ":8443", "listen address")
	outPath := flag.String("out", "fake-upstream.jsonl", "ground-truth JSONL output path")
	flag.Parse()

	w, err := oracle.NewWriter(*outPath)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer w.Close()

	cert, err := selfSignedCert()
	if err != nil {
		log.Fatalf("self-signed cert: %v", err)
	}

	srv := &http.Server{
		Addr:      *addr,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			ft := c.RemoteAddr().String() + "-" + c.LocalAddr().String()
			gen := nextGeneration(ft)
			cs := &connState{fiveTuple: ft, startNS: time.Now().UnixNano(), generation: gen}
			connCounter.Add(1)
			return context.WithValue(ctx, connKeyCtx, cs)
		},
		Handler: http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			cs, _ := r.Context().Value(connKeyCtx).(*connState)
			streamID := int(cs.streamSeq.Add(1))
			ci := oracle.ConnectionInstance{
				FiveTuple:         cs.fiveTuple,
				ConnectionStartNS: cs.startNS,
				Generation:        cs.generation,
			}

			traceID, control := parseTraceparent(r.Header.Get("traceparent")), r.Header.Get("X-Witness-Control")

			injectLatencyProfileC()

			// A second correlation signal alongside duration (README "how to improve
			// the signal"): duration alone doesn't discriminate at scale (~700-wide
			// candidate pools, Hungarian assignment showed it's an information
			// problem, not a ranking one). Response size is orthogonal to timing —
			// varied here the way real payloads naturally vary by resource/request,
			// not encoded from trace_id (that would make the correlator's job
			// artificial, not a legitimate content-based signal).
			body := randomBody()

			_ = w.Write(oracle.Record{
				Side:         "fake-upstream",
				ConnKey:      ci.Key(),
				StreamID:     streamID,
				TraceID:      traceID,
				TimestampNS:  time.Now().UnixNano(),
				Control:      control,
				ResponseSize: int64(len(body)),
			})

			rw.WriteHeader(http.StatusOK)
			rw.Write(body)
		}),
	}

	log.Printf("fake-upstream listening on %s (HTTP/2 over self-signed TLS)", *addr)
	log.Fatal(srv.ListenAndServeTLS("", ""))
}

// randomBody varies response size independent of trace_id — a content-based
// correlation signal orthogonal to duration, the way real payloads vary by
// resource/request rather than being encoded for correlation purposes.
// Uniform, not trace_id-derived: a correlator reading this couldn't
// reconstruct trace_id from it even if it wanted to.
func randomBody() []byte {
	n := 50 + mrand.Intn(4951) // uniform [50, 5000] bytes
	b := make([]byte, n)
	mrand.Read(b)
	return b
}

// injectLatencyProfileC approximates docs/target/ground-truth-eval-plane-v3.md §9
// profile C: p50 40ms · p95 300ms · p99 850ms · occasional multi-second stalls.
// Lognormal(mu=-3.219, sigma=1.225) gives p50=40ms, p95=300ms, p99≈690ms — close
// but not exactly calibrated to 850ms; a 0.5% heavy-stall overlay covers the
// "several seconds" tail. Treat this as a first approximation to refine against
// real fake-upstream measurements, not a certified profile-C generator.
func injectLatencyProfileC() {
	const mu, sigma = -3.219, 1.225
	base := math.Exp(mu + sigma*mrand.NormFloat64())
	if mrand.Float64() < 0.005 {
		base += 1.0 + mrand.Float64()*4.0
	}
	time.Sleep(time.Duration(base * float64(time.Second)))
}

func parseTraceparent(h string) string {
	// W3C traceparent: "version-traceid-parentid-flags"
	parts := strings.Split(h, "-")
	if len(parts) != 4 {
		return ""
	}
	return parts[1]
}

func selfSignedCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fake-upstream.m0-harness.local"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	return tls.X509KeyPair(certPEM, keyPEM)
}
