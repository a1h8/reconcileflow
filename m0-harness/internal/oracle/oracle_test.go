package oracle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fake-upstream builds its key through ConnectionInstance.Key(), load-gen
// through ConnKey() directly: the join only works if both agree.
func TestKeyAgreesWithConnKey(t *testing.T) {
	ci := ConnectionInstance{FiveTuple: "10.0.0.1:40000-10.0.0.2:8080", Generation: 3}
	if got, want := ci.Key(), ConnKey(ci.FiveTuple, ci.Generation); got != want {
		t.Fatalf("Key() = %q, ConnKey() = %q", got, want)
	}
}

// ConnectionStartNS is measured on two different clocks (accept vs. dial):
// if it leaked into the key, no record would ever join across processes.
func TestKeyIgnoresConnectionStart(t *testing.T) {
	a := ConnectionInstance{FiveTuple: "10.0.0.1:40000-10.0.0.2:8080", ConnectionStartNS: 1, Generation: 0}
	b := a
	b.ConnectionStartNS = 999_999_999
	if a.Key() != b.Key() {
		t.Fatalf("keys differ only by ConnectionStartNS: %q vs %q", a.Key(), b.Key())
	}
}

// Two distinct (five-tuple, generation) pairs must never share a key -- a
// collision is exactly the failure join() now rejects (see cmd/join), and
// the cheapest place to rule it out is the key format itself.
func FuzzConnKeyIsInjective(f *testing.F) {
	f.Add("10.0.0.1:40000-10.0.0.2:8080", 0, "10.0.0.1:40000-10.0.0.2:8080", 1)
	f.Add("[::1]:40000-[::1]:8080", 1, "[::1]:40000-[::1]:8080|1", 0)
	f.Add("a1", 2, "a", 12) // collides if the separator is ever dropped
	f.Add("a|1", 2, "a", 12)
	f.Add("a|-1", 0, "a", -1)
	f.Fuzz(func(t *testing.T, tupleA string, genA int, tupleB string, genB int) {
		if tupleA == tupleB && genA == genB {
			return
		}
		if ka, kb := ConnKey(tupleA, genA), ConnKey(tupleB, genB); ka == kb {
			t.Fatalf("(%q, %d) and (%q, %d) collide on %q", tupleA, genA, tupleB, genB, ka)
		}
	})
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

// Both binaries write from many request goroutines at once. Every record
// must come out as exactly one well-formed line -- an interleaved line would
// be dropped or misparsed by the join, silently biasing D_cov/D_acc.
//
// This checks the property, not Writer.mu: with the mutex removed it still
// passes (verified with a -race mutant), because json.Encoder emits each
// record in a single Write and os.File serialises writes itself. The mutex
// only becomes load-bearing if the file is ever wrapped in a bufio.Writer.
func TestWriterConcurrentWritesStayLineAtomic(t *testing.T) {
	const writers, perWriter = 32, 200
	path := filepath.Join(t.TempDir(), "gt.jsonl")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				r := Record{
					Side:     "load-gen",
					ConnKey:  ConnKey(fmt.Sprintf("10.0.0.1:%d-10.0.0.2:8080", 40000+g), 0),
					StreamID: i,
					TraceID:  fmt.Sprintf("%016x%016x", g, i),
					// Long enough that an unsynchronised write would visibly tear.
					Error: strings.Repeat("x", 512),
				}
				if err := w.Write(r); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	lines := readLines(t, path)
	if len(lines) != writers*perWriter {
		t.Fatalf("got %d lines, want %d", len(lines), writers*perWriter)
	}
	seen := make(map[string]bool, len(lines))
	for n, line := range lines {
		var r Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("line %d is not a whole record: %v", n+1, err)
		}
		if seen[r.TraceID] {
			t.Fatalf("trace_id %s written twice", r.TraceID)
		}
		seen[r.TraceID] = true
	}
}

// cmd/join and the Python analysers (analyze_run.py, correlate_by_duration.py)
// read these field names from the raw JSONL. Renaming a struct tag would
// break them without a single compile error.
func TestRecordJSONFieldNames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gt.jsonl")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	in := Record{
		Side: "load-gen", ConnKey: "5t|0", StreamID: 7, TraceID: "abc",
		TimestampNS: 42, DurationNS: 1000, ResponseSize: 64,
	}
	if err := w.Write(in); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &raw); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"side", "conn_key", "stream_id", "trace_id", "timestamp_ns", "duration_ns", "response_size",
	} {
		if _, ok := raw[field]; !ok {
			t.Errorf("field %q missing from %s", field, lines[0])
		}
	}

	var out Record
	if err := json.Unmarshal([]byte(lines[0]), &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round trip changed the record:\n in  %+v\n out %+v", in, out)
	}
}

func TestNewWriterReportsUnwritablePath(t *testing.T) {
	if _, err := NewWriter(filepath.Join(t.TempDir(), "missing-dir", "gt.jsonl")); err == nil {
		t.Fatal("NewWriter succeeded on a path whose directory does not exist")
	}
}

// A record written after Close must fail loudly, not vanish: a lost record
// is indistinguishable from a request the observer never saw.
func TestWriteAfterCloseFails(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "gt.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(Record{Side: "load-gen"}); err == nil {
		t.Fatal("Write after Close returned nil")
	}
}
