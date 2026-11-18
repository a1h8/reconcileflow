// Stream positions for 4c (docs/target/correlation-phase4-spec.md §8.6.1):
// with -stream-offsets, fake-upstream logs for each request the position of
// its first byte in the client's raw TCP stream, counted from the first byte
// the client sent (the ClientHello). stream-probe gives the same position
// from the kernel (seq - base); the two are measured apart, in user space
// here, so a difference is a probe fault.
//
// The hop talks TLS, so positions are in ciphertext bytes. Below TLS,
// rawCounter finds every TLS record in the raw stream; above it,
// requestScanner finds where each request starts in the plaintext. A request
// gets a position only when it starts exactly at the start of a record (what
// Go's TLS client does for a bodyless HTTP/1.1 request); otherwise it gets
// none, never a guess.
package main

import (
	"crypto/tls"
	"net"
	"sync"
)

// tls13Overhead is what a TLS 1.3 application record adds to its plaintext:
// a 16-byte AEAD tag and the 1-byte inner content type (Go never pads).
const tls13Overhead = 17

const recordApplicationData = 23

type tlsRecord struct {
	offset int64 // where the 5-byte header starts in the raw stream
	typ    byte
	length int // body length, header excluded
}

// rawCounter sits between the socket and TLS and parses record headers as
// bytes go by, whatever the read sizes.
type rawCounter struct {
	net.Conn
	mu   sync.Mutex
	n    int64   // raw bytes read so far
	hdr  [5]byte // header being collected
	hdrN int
	left int // body bytes of the current record still to come
	recs []tlsRecord
}

func (c *rawCounter) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, x := range b[:n] {
		switch {
		case c.left > 0:
			c.left--
		default:
			c.hdr[c.hdrN] = x
			c.hdrN++
			if c.hdrN == len(c.hdr) {
				length := int(c.hdr[3])<<8 | int(c.hdr[4])
				c.recs = append(c.recs, tlsRecord{c.n - 4, c.hdr[0], length})
				c.hdrN, c.left = 0, length
			}
		}
		c.n++
	}
	return n, err
}

func (c *rawCounter) records() []tlsRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]tlsRecord(nil), c.recs...)
}

// requestScanner sits above TLS and notes where each request starts in the
// plaintext: at 0, then right after each "\r\n\r\n". Bench requests are GETs
// without a body (the handler refuses a position to any request with one).
type requestScanner struct {
	net.Conn
	mu      sync.Mutex
	n       int64
	matched int // bytes of "\r\n\r\n" matched so far
	starts  []int64
}

var headerEnd = [4]byte{'\r', '\n', '\r', '\n'}

func (s *requestScanner) Read(b []byte) (int, error) {
	n, err := s.Conn.Read(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, x := range b[:n] {
		if s.n == 0 {
			s.starts = append(s.starts, 0)
		}
		s.n++
		switch {
		case x == headerEnd[s.matched]:
			s.matched++
		case x == headerEnd[0]:
			s.matched = 1
		default:
			s.matched = 0
		}
		if s.matched == len(headerEnd) {
			s.starts = append(s.starts, s.n)
			s.matched = 0
		}
	}
	return n, err
}

func (s *requestScanner) requestStarts() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.starts...)
}

// rawOffset gives the raw position of the record whose plaintext starts at
// plain, or false when plain falls inside a record or past the last one.
// The first encrypted record is the client's Finished (TLS 1.3 only).
func rawOffset(records []tlsRecord, plain int64) (int64, bool) {
	var cum int64
	finished := false
	for _, r := range records {
		if r.typ != recordApplicationData {
			continue
		}
		if !finished {
			finished = true
			continue
		}
		if r.length < tls13Overhead {
			return 0, false
		}
		if cum == plain {
			return r.offset, true
		}
		cum += int64(r.length - tls13Overhead)
	}
	return 0, false
}

// streamConn is what http.Server sees: plaintext reads through the scanner,
// over TLS, over the raw counter.
type streamConn struct {
	*requestScanner
	raw *rawCounter
}

// requestOffset gives the raw position of the k-th request (from 1).
func (s *streamConn) requestOffset(k int) (int64, bool) {
	starts := s.requestStarts()
	if k < 1 || k > len(starts) {
		return 0, false
	}
	return rawOffset(s.raw.records(), starts[k-1])
}

// offsetsTLSConfig pins TLS 1.3 (whose record overhead rawOffset relies on)
// and HTTP/1.1 (one request at a time on a connection).
func offsetsTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		NextProtos:   []string{"http/1.1"},
	}
}

type offsetListen struct {
	net.Listener
	cfg *tls.Config
}

// offsetListener does TLS itself, so http.Server serves the plaintext as
// HTTP/1.1 and fake-upstream sees both sides of TLS.
func offsetListener(ln net.Listener, cfg *tls.Config) net.Listener {
	return &offsetListen{ln, cfg}
}

func (l *offsetListen) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	raw := &rawCounter{Conn: c}
	return &streamConn{requestScanner: &requestScanner{Conn: tls.Server(raw, l.cfg)}, raw: raw}, nil
}
