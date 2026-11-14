package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
)

// chunkConn serves a fixed byte stream in reads of at most size bytes, so a
// parser must hold its state across reads cut anywhere.
type chunkConn struct {
	net.Conn
	data []byte
	size int
}

func (c *chunkConn) Read(b []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := min(len(b), c.size, len(c.data))
	copy(b, c.data[:n])
	c.data = c.data[n:]
	return n, nil
}

func record(typ byte, body int) []byte {
	return append([]byte{typ, 3, 3, byte(body >> 8), byte(body)}, make([]byte, body)...)
}

func drain(t *testing.T, c net.Conn) {
	t.Helper()
	if _, err := io.Copy(io.Discard, c); err != nil {
		t.Fatal(err)
	}
}

// The raw stream is the client's TLS records back to back; each record's
// offset is where its 5-byte header starts, counted from the first byte the
// client sent, the same origin as the probe's offset (seq - base).
func TestRawCounterFindsEveryRecordWhateverTheReadSizes(t *testing.T) {
	var stream []byte
	for _, r := range [][]byte{record(22, 300), record(20, 1), record(23, 53), record(23, 600), record(23, 70000%16384)} {
		stream = append(stream, r...)
	}
	want := []tlsRecord{{0, 22, 300}, {305, 20, 1}, {311, 23, 53}, {369, 23, 600}, {974, 23, 70000 % 16384}}
	for _, size := range []int{1, 2, 5, 7, 4096, len(stream)} {
		rc := &rawCounter{Conn: &chunkConn{data: bytes.Clone(stream), size: size}}
		drain(t, rc)
		if got := rc.records(); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("reads of %d bytes: records %v, want %v", size, got, want)
		}
	}
}

// Requests have no body on this bench (GET), so a request starts at 0 and
// right after each blank line ending a header block.
func TestRequestScannerFindsEachRequestStartWhateverTheReadSizes(t *testing.T) {
	a := "GET /truth-a HTTP/1.1\r\nHost: x\r\n\r\n"
	b := "GET /truth-b HTTP/1.1\r\nHost: x\r\nX-Odd: \r\n\r\n"
	c := "GET /truth-c HTTP/1.1\r\nX: \r\r\n\r\n" // a stray CR right before the blank line
	stream := []byte(a + b + c + a)
	want := []int64{0, int64(len(a)), int64(len(a) + len(b)), int64(len(a) + len(b) + len(c)), int64(2*len(a) + len(b) + len(c))}
	for _, size := range []int{1, 3, 4, 4096} {
		rs := &requestScanner{Conn: &chunkConn{data: bytes.Clone(stream), size: size}}
		drain(t, rs)
		if got := rs.requestStarts(); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("reads of %d bytes: starts %v, want %v", size, got, want)
		}
	}
}

// TLS 1.3: each application record carries its length minus 17 bytes of
// plaintext (16-byte tag, 1-byte inner type); the first encrypted record is
// the client's Finished, never handed to the application.
func TestRawOffsetMapsARequestStartToTheRecordItOpens(t *testing.T) {
	records := []tlsRecord{
		{0, 22, 300},          // ClientHello
		{305, 20, 1},          // change_cipher_spec (compatibility)
		{311, 23, 53},         // Finished, encrypted
		{369, 23, 17 + 100},   // request 1: plaintext [0, 100)
		{491, 23, 17 + 40},    // request 2: plaintext [100, 140)
		{553, 23, 17 + 16384}, // request 3 starts here, [140, ...)
	}
	for _, tc := range []struct {
		plain int64
		raw   int64
		ok    bool
	}{
		{0, 369, true},
		{100, 491, true},
		{140, 553, true},
		{50, 0, false},  // inside a record: no exact position, none given
		{141, 0, false}, // inside the last record
		{1 << 40, 0, false},
	} {
		raw, ok := rawOffset(records, tc.plain)
		if raw != tc.raw || ok != tc.ok {
			t.Errorf("rawOffset(%d) = %d, %v, want %d, %v", tc.plain, raw, ok, tc.raw, tc.ok)
		}
	}
	if _, ok := rawOffset([]tlsRecord{{0, 23, 53}, {58, 23, 10}}, 0); ok {
		t.Error("a record shorter than the TLS 1.3 overhead gave a position")
	}
}

// writeLog records where, in the raw stream, each client write starts: Go's
// TLS client sends one record per write once the handshake is done, and its
// HTTP/1.1 transport flushes a bodyless request in one write.
type writeLog struct {
	net.Conn
	mu     sync.Mutex
	n      int64
	starts []int64
}

func (w *writeLog) Write(b []byte) (int, error) {
	w.mu.Lock()
	w.starts = append(w.starts, w.n)
	w.n += int64(len(b))
	w.mu.Unlock()
	return w.Conn.Write(b)
}

// End to end over real TLS 1.3 and HTTP/1.1 keep-alive: the server's
// position for each request is where the client wrote that request, in raw
// bytes, counted from the connection's first byte.
func TestEachRequestGetsTheRawOffsetTheClientWroteItAt(t *testing.T) {
	cert, err := selfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := map[string]int64{}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			cs, _ := r.Context().Value(connKeyCtx).(*connState)
			off, ok := cs.requestOffset(int(cs.streamSeq.Add(1)))
			if !ok {
				t.Errorf("%s: no position", r.URL.Path)
			}
			mu.Lock()
			got[r.URL.Path] = off
			mu.Unlock()
			rw.Write([]byte("ok"))
		}),
		ConnContext: connContext,
	}
	go srv.Serve(offsetListener(ln, offsetsTLSConfig(cert)))
	defer srv.Close()

	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	var wl *writeLog
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"http/1.1"}},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err == nil {
				wl = &writeLog{Conn: c}
				return wl, nil
			}
			return nil, err
		},
		MaxConnsPerHost: 1,
	}}
	paths := []string{"/truth-1", "/truth-2-longer-path", "/truth-3"}
	for _, p := range paths {
		resp, err := client.Get("https://" + ln.Addr().String() + p)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.ProtoMajor != 1 {
			t.Fatalf("negotiated %s, want HTTP/1.1", resp.Proto)
		}
	}
	wl.mu.Lock()
	starts := wl.starts
	wl.mu.Unlock()
	// The last len(paths) writes are the requests, in order.
	if len(starts) < len(paths)+1 {
		t.Fatalf("only %d client writes", len(starts))
	}
	want := starts[len(starts)-len(paths):]
	for i, p := range paths {
		if got[p] != want[i] {
			t.Fatalf("%s: server position %d, client wrote it at %d (all writes: %v)", p, got[p], want[i], starts)
		}
	}
	if got[paths[0]] == 0 {
		t.Fatal("the first request cannot start at 0: the handshake comes first")
	}
}

// Without -stream-offsets nothing changes: no position, HTTP/2 offered.
func TestAPlainConnectionHasNoPosition(t *testing.T) {
	cs := &connState{}
	if _, ok := cs.requestOffset(1); ok {
		t.Fatal("a connection without stream counting gave a position")
	}
}

func TestOffsetsModeSpeaksOnlyHTTP1OverTLS13(t *testing.T) {
	cert, err := selfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	cfg := offsetsTLSConfig(cert)
	if cfg.MinVersion != tls.VersionTLS13 || cfg.MaxVersion != tls.VersionTLS13 {
		t.Fatalf("versions %x..%x, want TLS 1.3 only", cfg.MinVersion, cfg.MaxVersion)
	}
	if fmt.Sprint(cfg.NextProtos) != "[http/1.1]" {
		t.Fatalf("ALPN %v, want [http/1.1]", cfg.NextProtos)
	}
}

// Across binaries: with -stream-offsets every record carries its position,
// and positions on one connection are distinct and grow with arrival order.
func TestStreamOffsetsReachTheLog(t *testing.T) {
	r := pipelineWith(t, []string{"-stream-offsets"}, "-http1", "-truth-in-path", "-requests", "60", "-concurrency", "3")
	if len(r.fakeUpstream) != 60 {
		t.Fatalf("%d records, want 60", len(r.fakeUpstream))
	}
	last := map[string]int64{}
	for _, rec := range r.fakeUpstream {
		if rec.StreamOffset == nil {
			t.Fatalf("%s: no stream_offset", rec.Path)
		}
		if prev, seen := last[rec.ConnKey]; seen && *rec.StreamOffset <= prev {
			t.Fatalf("%s: position %d after %d on the same connection", rec.Path, *rec.StreamOffset, prev)
		}
		last[rec.ConnKey] = *rec.StreamOffset
	}
	if len(last) > 3 {
		t.Fatalf("%d connections for concurrency 3", len(last))
	}
}

func TestWithoutTheFlagTheLogHasNoPosition(t *testing.T) {
	r := pipelineWith(t, nil, "-http1", "-truth-in-path", "-requests", "5", "-concurrency", "1")
	for _, rec := range r.fakeUpstream {
		if rec.StreamOffset != nil {
			t.Fatalf("%s: stream_offset %d without -stream-offsets", rec.Path, *rec.StreamOffset)
		}
	}
}
