package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeJournal writes n segments of k events each, a drops and a clock
// reading at the end of each, through the real segment writer.
func writeJournal(t *testing.T, dir string, n, k int) []Record {
	t.Helper()
	clock := &fakeClock{time.Unix(0, 0)}
	w, err := newSegmentWriter(dir, 1<<30, time.Second, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	var want []Record
	seq := uint32(0)
	for s := range n {
		var batch []byte
		for range k {
			seq++
			raw := rawEvent(t, kindSend, seq)
			batch, _ = appendEvent(batch, raw)
			r, _ := decodeRecord(raw, portSet{testPort})
			want = append(want, r)
		}
		batch = appendDrops(batch, uint64(s), uint64(s))
		d := uint64(s)
		want = append(want, Record{Kind: "drops", TimestampNS: uint64(s), Drops: &d})
		c := clockRecord(func() uint64 { return uint64(s) }, func() uint64 { return 7 })
		batch = appendClock(batch, c)
		want = append(want, c)
		if err := w.Write(batch); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(2 * time.Second) // next batch, next segment
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return want
}

func jsonLines(t *testing.T, recs []Record) []string {
	t.Helper()
	var out []string
	for _, r := range recs {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(b))
	}
	return out
}

func fileLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	return out
}

// Decoded in parallel, written in segment order: the same lines, in the same
// order, as the probe writes without a journal.
func TestDecodeJournalGivesTheDirectModesLinesInOrder(t *testing.T) {
	dir := t.TempDir()
	want := jsonLines(t, writeJournal(t, dir, 40, 25))
	out := filepath.Join(t.TempDir(), "probe.jsonl")
	n, err := decodeJournal(dir, portSet{testPort}, out, 8)
	if err != nil {
		t.Fatal(err)
	}
	got := fileLines(t, out)
	if n != 40 || strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%d segments, %d lines (want 40, %d); first difference at %d", n, len(got), len(want), firstDiff(got, want))
	}
}

func firstDiff(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// A segment still being written is not read; a foreign or cut segment stops
// the decoding with its name, rather than leaving a hole in the file.
func TestDecodeJournalSkipsOpenSegmentsAndRefusesBadOnes(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, 3, 2)
	os.WriteFile(filepath.Join(dir, "seg-00000004.open"), journalHeader(), 0o600)
	out := filepath.Join(t.TempDir(), "probe.jsonl")
	if n, err := decodeJournal(dir, portSet{testPort}, out, 2); err != nil || n != 3 {
		t.Fatalf("decodeJournal = %d, %v; want the 3 closed segments", n, err)
	}
	bad := filepath.Join(dir, "seg-00000002.seg")
	raw, _ := os.ReadFile(bad)
	os.WriteFile(bad, raw[:len(raw)-3], 0o600)
	if _, err := decodeJournal(dir, portSet{testPort}, out, 2); err == nil || !strings.Contains(err.Error(), "seg-00000002") {
		t.Fatalf("decodeJournal on a cut segment = %v, want an error naming it", err)
	}
}

func TestSegmentsAreTakenInNumberOrderNotNameOrder(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []int{100000000, 2, 99999999} { // past the %08d width, text order differs
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("seg-%08d.seg", n)), journalHeader(), 0o600)
	}
	got, err := closedSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, p := range got {
		names = append(names, filepath.Base(p))
	}
	if strings.Join(names, ",") != "seg-00000002.seg,seg-99999999.seg,seg-100000000.seg" {
		t.Fatalf("order %v", names)
	}
}

// -decode needs no kernel and no privilege: run() decodes and returns
// before loading anything.
func TestRunDecodeTouchesNoKernel(t *testing.T) {
	withRunSeams(t)
	loadCollectionSpec = nil // would panic if called
	dir := t.TempDir()
	writeJournal(t, dir, 2, 3)
	out := filepath.Join(t.TempDir(), "probe.jsonl")
	os.Args = []string{"stream-probe", "-decode", "-journal=" + dir, "-port=8443", "-out=" + out}
	if err := run(); err != nil {
		t.Fatalf("run() = %v", err)
	}
	if len(fileLines(t, out)) != 2*(3+2) {
		t.Fatalf("decode wrote %d lines", len(fileLines(t, out)))
	}
	os.Args = []string{"stream-probe", "-decode", "-port=8443"}
	if err := run(); err == nil || !strings.Contains(err.Error(), "-journal") {
		t.Fatalf("-decode without -journal: %v", err)
	}
}
