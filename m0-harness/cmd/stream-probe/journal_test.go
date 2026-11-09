package main

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cilium/ebpf/ringbuf"
)

const testPort = 8443

func rawEvent(t *testing.T, kind uint8, seq uint32) []byte {
	t.Helper()
	ev := v4(kind, 40_000, testPort, seq, 392)
	ev.TimestampNS = uint64(seq) * 10
	return encode(t, ev)
}

// Every record is the same size, so a segment can be cut and decoded anywhere,
// in parallel: an event keeps the kernel's 80 bytes as they came.
func TestJournalRecordsRoundTrip(t *testing.T) {
	var b []byte
	b, err := appendEvent(b, rawEvent(t, kindSend, 7))
	if err != nil {
		t.Fatal(err)
	}
	b = appendDrops(b, 1_000, 3)
	b = appendClock(b, clockRecord(func() uint64 { return 50 }, func() uint64 { return 9_000 }))
	if len(b) != 3*journalRecordSize {
		t.Fatalf("%d bytes, want 3 records of %d", len(b), journalRecordSize)
	}
	var got []Record
	for i := 0; i < len(b); i += journalRecordSize {
		r, err := decodeJournalRecord(b[i:i+journalRecordSize], testPort)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if got[0].Kind != "send" || got[0].Seq != 7 || got[0].TimestampNS != 70 {
		t.Errorf("event %+v", got[0])
	}
	if got[1].Kind != "drops" || *got[1].Drops != 3 || got[1].TimestampNS != 1_000 {
		t.Errorf("drops %+v", got[1])
	}
	if got[2].Kind != "clock" || *got[2].RealtimeNS != 9_000 || got[2].TimestampNS != 50 {
		t.Errorf("clock %+v", got[2])
	}
	if _, err := appendEvent(nil, make([]byte, 79)); err == nil {
		t.Error("an event of the wrong size was journaled")
	}
	if _, err := decodeJournalRecord(make([]byte, journalRecordSize), testPort); err == nil {
		t.Error("a record of unknown type was decoded")
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func segments(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// Segments close by size or by age, are only readable by their owner, and
// are renamed .seg once closed: a reader only takes closed ones.
func TestSegmentWriterRotatesBySizeAndAge(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{time.Unix(100, 0)}
	w, err := newSegmentWriter(dir, 3*journalRecordSize, time.Second, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	one := appendDrops(nil, 1, 0)
	for range 4 { // 3 records fill the first segment
		if err := w.Write(one); err != nil {
			t.Fatal(err)
		}
	}
	clock.t = clock.t.Add(2 * time.Second) // the second segment is now too old
	if err := w.Write(one); err != nil {
		t.Fatal(err)
	}
	got := segments(t, dir)
	want := []string{"seg-00000001.seg", "seg-00000002.seg", "seg-00000003.open"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("segments %v, want %v", got, want)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got = segments(t, dir)
	if got[2] != "seg-00000003.seg" {
		t.Fatalf("the last segment is not closed: %v", got)
	}
	counts := []int{3, 1, 1}
	for i, name := range got {
		recs, err := readSegment(filepath.Join(dir, name), testPort)
		if err != nil || len(recs) != counts[i] {
			t.Fatalf("%s: %d records, %v; want %d", name, len(recs), err, counts[i])
		}
		info, _ := os.Stat(filepath.Join(dir, name))
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s: mode %v, want 0600", name, info.Mode().Perm())
		}
	}
}

func TestReadSegmentRefusesAForeignFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.seg")
	os.WriteFile(path, []byte("not a journal segment at all"), 0o600)
	if _, err := readSegment(path, testPort); err == nil {
		t.Fatal("a foreign file was read as a segment")
	}
	dir := t.TempDir()
	w, err := newSegmentWriter(dir, 1<<20, time.Hour, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(appendDrops(nil, 1, 0)[:journalRecordSize-1]); err == nil {
		t.Fatal("the writer took a torn record")
	}
	w.Write(appendDrops(nil, 1, 0))
	w.Close()
	seg := filepath.Join(dir, "seg-00000001.seg")
	raw, _ := os.ReadFile(seg)
	os.WriteFile(seg, raw[:len(raw)-1], 0o600) // cut inside the last record
	if _, err := readSegment(seg, testPort); err == nil {
		t.Fatal("a segment cut inside a record was read")
	}
}

// scriptedRing hands out samples, then deadline timeouts, then closes.
type scriptedRing struct {
	script []any // []byte sample | error
}

func (s *scriptedRing) SetDeadline(time.Time) {}
func (s *scriptedRing) ReadInto(rec *ringbuf.Record) error {
	if len(s.script) == 0 {
		return ringbuf.ErrClosed
	}
	next := s.script[0]
	s.script = s.script[1:]
	if err, ok := next.(error); ok {
		return err
	}
	rec.RawSample = append(rec.RawSample[:0], next.([]byte)...)
	return nil
}

// The reader only copies: full batches go out at once, a partial one when the
// ring stays quiet past the deadline, the rest when the ring closes.
func TestReadBatchesFlushesFullQuietAndClosed(t *testing.T) {
	ev := rawEvent(t, kindSend, 1)
	ring := &scriptedRing{script: []any{
		ev, ev, ev, // a full batch of 3
		ev, os.ErrDeadlineExceeded, // quiet: flush the partial batch of 1
		os.ErrDeadlineExceeded, // quiet again, nothing to flush
		ev, ev,                 // flushed at close
	}}
	out := make(chan []byte, 10)
	stats := readBatches(ring, out, 3, time.Millisecond)
	close(out)
	var sizes []int
	for b := range out {
		sizes = append(sizes, len(b)/journalRecordSize)
	}
	if len(sizes) != 3 || sizes[0] != 3 || sizes[1] != 1 || sizes[2] != 2 {
		t.Fatalf("batches of %v records, want [3 1 2]", sizes)
	}
	if stats.events != 6 || stats.bad != 0 {
		t.Fatalf("stats %+v", stats)
	}
}

func TestReadBatchesCountsBadSamplesAndReadErrors(t *testing.T) {
	ring := &scriptedRing{script: []any{make([]byte, 12), errors.New("EIO"), rawEvent(t, kindRecv, 2)}}
	out := make(chan []byte, 10)
	stats := readBatches(ring, out, 100, time.Millisecond)
	if stats.events != 1 || stats.bad != 1 || stats.readErrors != 1 {
		t.Fatalf("stats %+v", stats)
	}
}

// End to end through run(): with -journal the probe writes closed segments
// only, a clock record first, the last drops and clock records last.
func TestRunWithAJournal(t *testing.T) {
	withRunSeams(t)
	dir := filepath.Join(t.TempDir(), "journal")
	os.Args = []string{"stream-probe", "-port=8443", "-journal=" + dir}
	if err := run(); err != nil {
		t.Fatalf("run() = %v", err)
	}
	names := segments(t, dir)
	if len(names) == 0 {
		t.Fatal("no segment written")
	}
	var all []Record
	for _, n := range names {
		if !strings.HasSuffix(n, ".seg") {
			t.Fatalf("%s left open", n)
		}
		recs, err := readSegment(filepath.Join(dir, n), 8443)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, recs...)
	}
	if all[0].Kind != "clock" || all[len(all)-1].Kind != "clock" || all[len(all)-2].Kind != "drops" {
		t.Fatalf("records %+v: want clock first, then drops and clock last", all)
	}
}
