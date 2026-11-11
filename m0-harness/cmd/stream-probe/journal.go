// The journal (docs/target/couverture-asynchrone-spec.md, 4 quater): under
// load the probe only copies. One goroutine drains the kernel's ring buffer
// into batches of raw records, without decoding them; a bounded queue absorbs
// bursts; one goroutine appends the batches to segment files. Decoding is
// done later, apart, by whoever reads the closed segments, in parallel if it
// wants: every record has the same size.
//
// Segment: a 16-byte header ("RFSPJRN1", version, record size), then
// records of journalRecordSize bytes: a type byte, 7 bytes of padding, and
// 80 bytes of payload (an event as the kernel wrote it; or a drops or clock
// reading). A segment being written is seg-N.open, renamed seg-N.seg once
// closed; files are readable by their owner only.
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cilium/ebpf/ringbuf"
)

const (
	journalMagic      = "RFSPJRN1"
	journalVersion    = 1
	eventSize         = 80 // struct stream_event, guarded by TestStreamEventSize
	journalRecordSize = 8 + eventSize
	recEvent          = 1
	recDrops          = 2
	recClock          = 3
)

func journalHeader() []byte {
	h := make([]byte, 16)
	copy(h, journalMagic)
	binary.LittleEndian.PutUint32(h[8:], journalVersion)
	binary.LittleEndian.PutUint32(h[12:], journalRecordSize)
	return h
}

func appendRecord(dst []byte, typ byte, payload []byte) []byte {
	var rec [journalRecordSize]byte
	rec[0] = typ
	copy(rec[8:], payload)
	return append(dst, rec[:]...)
}

// appendEvent journals a ring buffer sample as the kernel wrote it.
func appendEvent(dst, raw []byte) ([]byte, error) {
	if len(raw) != eventSize {
		return dst, fmt.Errorf("event of %d bytes, want %d", len(raw), eventSize)
	}
	return appendRecord(dst, recEvent, raw), nil
}

func appendDrops(dst []byte, tsNS, drops uint64) []byte {
	var p [16]byte
	binary.LittleEndian.PutUint64(p[0:], tsNS)
	binary.LittleEndian.PutUint64(p[8:], drops)
	return appendRecord(dst, recDrops, p[:])
}

func appendClock(dst []byte, c Record) []byte {
	var p [24]byte
	binary.LittleEndian.PutUint64(p[0:], c.TimestampNS)
	binary.LittleEndian.PutUint64(p[8:], *c.RealtimeNS)
	binary.LittleEndian.PutUint64(p[16:], *c.SpreadNS)
	return appendRecord(dst, recClock, p[:])
}

// decodeJournalRecord turns one record back into the JSONL Record.
func decodeJournalRecord(b []byte, watched portSet) (Record, error) {
	if len(b) != journalRecordSize {
		return Record{}, fmt.Errorf("record of %d bytes, want %d", len(b), journalRecordSize)
	}
	p := b[8:]
	switch b[0] {
	case recEvent:
		return decodeRecord(p, watched)
	case recDrops:
		n := binary.LittleEndian.Uint64(p[8:])
		return Record{Kind: "drops", TimestampNS: binary.LittleEndian.Uint64(p), Drops: &n}, nil
	case recClock:
		real, spread := binary.LittleEndian.Uint64(p[8:]), binary.LittleEndian.Uint64(p[16:])
		return Record{Kind: "clock", TimestampNS: binary.LittleEndian.Uint64(p), RealtimeNS: &real, SpreadNS: &spread}, nil
	}
	return Record{}, fmt.Errorf("unknown journal record type %d", b[0])
}

// readSegment decodes a whole segment; a foreign file or a cut record is an error.
func readSegment(path string, watched portSet) ([]Record, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < 16 || !bytes.Equal(raw[:16], journalHeader()) {
		return nil, fmt.Errorf("%s: not a version %d journal segment", path, journalVersion)
	}
	body := raw[16:]
	if len(body)%journalRecordSize != 0 {
		return nil, fmt.Errorf("%s: cut inside a record (%d bytes after the header)", path, len(body))
	}
	out := make([]Record, 0, len(body)/journalRecordSize)
	for i := 0; i < len(body); i += journalRecordSize {
		r, err := decodeJournalRecord(body[i:i+journalRecordSize], watched)
		if err != nil {
			return nil, fmt.Errorf("%s: record %d: %w", path, i/journalRecordSize, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// segmentWriter appends batches to seg-N.open and closes it into seg-N.seg
// when it reaches maxBytes or maxAge.
type segmentWriter struct {
	dir      string
	maxBytes int64
	maxAge   time.Duration
	now      func() time.Time
	n        int
	f        *os.File
	size     int64
	opened   time.Time
}

func newSegmentWriter(dir string, maxBytes int64, maxAge time.Duration, now func() time.Time) (*segmentWriter, error) {
	w := &segmentWriter{dir: dir, maxBytes: maxBytes, maxAge: maxAge, now: now}
	return w, w.open()
}

func (w *segmentWriter) name(ext string) string {
	return filepath.Join(w.dir, fmt.Sprintf("seg-%08d.%s", w.n, ext))
}

func (w *segmentWriter) open() error {
	w.n++
	f, err := os.OpenFile(w.name("open"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(journalHeader()); err != nil {
		f.Close()
		return err
	}
	w.f, w.size, w.opened = f, 0, w.now()
	return nil
}

func (w *segmentWriter) closeCurrent() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return os.Rename(w.name("open"), w.name("seg"))
}

// Write appends whole records; a batch is never split across segments.
func (w *segmentWriter) Write(batch []byte) error {
	if len(batch)%journalRecordSize != 0 {
		return fmt.Errorf("batch of %d bytes is not whole records", len(batch))
	}
	if w.size > 0 && (w.size+int64(len(batch)) > w.maxBytes || w.now().Sub(w.opened) >= w.maxAge) {
		if err := w.closeCurrent(); err != nil {
			return err
		}
		if err := w.open(); err != nil {
			return err
		}
	}
	n, err := w.f.Write(batch)
	w.size += int64(n)
	return err
}

func (w *segmentWriter) Close() error { return w.closeCurrent() }

// deadlineRing is what readBatches needs from *ringbuf.Reader.
type deadlineRing interface {
	SetDeadline(time.Time)
	ReadInto(*ringbuf.Record) error
}

type readStats struct {
	events, bad, readErrors int
}

// readBatches only copies: it fills batches of batchRecords raw records and
// hands each to out when full, when the ring stays quiet for flush, and at
// close. A full queue makes it wait, the ring fills, and the kernel counts
// what it cannot hold: a loss is always declared, never silent.
func readBatches(rd deadlineRing, out chan<- []byte, batchRecords int, flush time.Duration) readStats {
	var st readStats
	var rec ringbuf.Record
	batch := make([]byte, 0, batchRecords*journalRecordSize)
	send := func() {
		if len(batch) > 0 {
			out <- batch
			batch = make([]byte, 0, batchRecords*journalRecordSize)
		}
	}
	for {
		rd.SetDeadline(time.Now().Add(flush))
		err := rd.ReadInto(&rec)
		switch {
		case err == nil:
			var aerr error
			if batch, aerr = appendEvent(batch, rec.RawSample); aerr != nil {
				st.bad++
				continue
			}
			st.events++
			if len(batch) >= batchRecords*journalRecordSize {
				send()
			}
		case errors.Is(err, os.ErrDeadlineExceeded):
			send()
		case errors.Is(err, ringbuf.ErrClosed):
			send()
			return st
		default:
			st.readErrors++
		}
	}
}

// writeBatches appends every batch, then closes the last segment.
func writeBatches(in <-chan []byte, w *segmentWriter) error {
	var first error
	for b := range in {
		if err := w.Write(b); err != nil && first == nil {
			first = err
		}
	}
	if err := w.Close(); err != nil && first == nil {
		first = err
	}
	return first
}
