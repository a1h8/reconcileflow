package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"reconcileflow/m0-harness/internal/oracle"
)

func rec(connKey string, streamID int, traceID string) oracle.Record {
	return oracle.Record{ConnKey: connKey, StreamID: streamID, TraceID: traceID}
}

func TestJoinComputesDAccOnTheHappyPath(t *testing.T) {
	expected := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 1, "B"),
	}
	observed := []oracle.Record{
		rec("5t|0", 0, "A"), // match
		rec("5t|0", 1, "X"), // joined, wrong trace_id
	}
	result, err := join(expected, observed, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Expected != 2 || result.Joined != 2 || result.Matched != 1 || result.JoinMisses != 0 {
		t.Fatalf("got %+v", result)
	}
	if result.DAcc != 0.5 {
		t.Fatalf("D_acc = %v, want 0.5", result.DAcc)
	}
}

func TestJoinCountsAMissingObservedRecordAsAJoinMiss(t *testing.T) {
	expected := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 1, "B"), // fake-upstream never observed this one
	}
	observed := []oracle.Record{rec("5t|0", 0, "A")}
	result, err := join(expected, observed, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Joined != 1 || result.JoinMisses != 1 {
		t.Fatalf("got %+v", result)
	}
	// The miss stays in the denominator: matched/joined would report 1.0
	// here, hiding the very failure D_acc exists to catch (README, "First
	// results" -- the first version of join made exactly that mistake).
	if result.DAcc != 0.5 {
		t.Fatalf("D_acc = %v, want 0.5 (matched / expected, not / joined)", result.DAcc)
	}
}

func TestJoinRejectsDuplicateKeyInExpected(t *testing.T) {
	expected := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 0, "B"), // same (conn_key, stream_id) as above
	}
	if _, err := join(expected, nil, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}

func TestJoinRejectsDuplicateKeyInObserved(t *testing.T) {
	// Exact empirical repro (2026-10-04): a single expected record, two
	// observed records sharing its key, both equal to its trace_id. Before
	// this check existed, this produced joined=2, matched=2 against
	// expected=1 -- D_acc=2.0 and join_misses=-1, both mathematically
	// impossible for a fraction/count that is supposed to be bounded.
	expected := []oracle.Record{rec("5t|0", 0, "A")}
	observed := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 0, "A"),
	}
	if _, err := join(expected, observed, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}

func TestJoinRejectsMixedSchemaVersions(t *testing.T) {
	expected := []oracle.Record{{SchemaVersion: 2, Protocol: "HTTP/2", ConnKey: "5t|0", StreamID: 0, TraceID: "A"}}
	if _, err := join(expected, nil, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}

// --- negative control (-shuffle) -------------------------------------------

// -shuffle is the witness that D_acc is not circular: with every observed
// trace_id correct, a permuted expected side must collapse to chance. A
// uniform random permutation of n keys has Poisson(1)-distributed fixed
// points, so matched > 10 has probability ~1e-8 per trial -- a failure here
// means the shuffle is not a permutation, not bad luck.
func TestJoinShuffleCollapsesDAccToChance(t *testing.T) {
	const n = 1000
	var expected, observed []oracle.Record
	for i := range n {
		id := fmt.Sprintf("trace-%04d", i)
		expected = append(expected, rec("5t|0", i, id))
		observed = append(observed, rec("5t|0", i, id))
	}

	for trial := range 20 {
		result, err := join(expected, observed, true)
		if err != nil {
			t.Fatal(err)
		}
		if result.Joined != n || result.JoinMisses != 0 {
			t.Fatalf("trial %d: shuffle must not change which keys join, got %+v", trial, result)
		}
		if result.Matched > 10 {
			t.Fatalf("trial %d: matched=%d of %d after shuffle -- not a permutation", trial, result.Matched, n)
		}
	}
}

// The shuffle must permute load-gen's trace_ids, not invent or drop any:
// the control is only fair if the multiset of values is unchanged.
func TestJoinShuffleKeepsEveryTraceIDExactlyOnce(t *testing.T) {
	const n = 200
	var expected []oracle.Record
	for i := range n {
		expected = append(expected, rec("5t|0", i, fmt.Sprintf("trace-%04d", i)))
	}
	// Probe each key with every trace_id in turn: across all probes, each
	// value must match exactly one key.
	matchesPerValue := make(map[string]int)
	result, err := join(expected, nil, true)
	if err != nil || result.Expected != n {
		t.Fatalf("got %+v, %v", result, err)
	}
	for v := range n {
		value := fmt.Sprintf("trace-%04d", v)
		var observed []oracle.Record
		for i := range n {
			observed = append(observed, rec("5t|0", i, value))
		}
		// A fresh shuffle each call: what is checked is that every call is a
		// bijection, so each value lands on exactly one key.
		r, err := join(expected, observed, true)
		if err != nil {
			t.Fatal(err)
		}
		matchesPerValue[value] = r.Matched
	}
	for value, m := range matchesPerValue {
		if m != 1 {
			t.Fatalf("%s matched %d keys after shuffle, want exactly 1", value, m)
		}
	}
}

// --- edge cases -------------------------------------------------------------

func TestJoinWithNoExpectedRecordsReportsZeroNotNaN(t *testing.T) {
	result, err := join(nil, []oracle.Record{rec("5t|0", 0, "A")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.DAcc != 0 || result.Expected != 0 || result.JoinMisses != 0 || result.FakeUpstream != 1 {
		t.Fatalf("got %+v", result)
	}
}

// An observed record nobody sent is not a join miss (misses are counted
// against expected) and must not inflate D_acc.
func TestJoinIgnoresObservedRecordsWithNoExpectedCounterpart(t *testing.T) {
	expected := []oracle.Record{rec("5t|0", 0, "A")}
	observed := []oracle.Record{rec("5t|0", 0, "A"), rec("5t|1", 0, "B")}
	result, err := join(expected, observed, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Joined != 1 || result.Matched != 1 || result.DAcc != 1 || result.FakeUpstream != 2 {
		t.Fatalf("got %+v", result)
	}
}

// --- schema gate --------------------------------------------------------------

func v2(protocol, errMsg string) oracle.Record {
	r := rec("5t|0", 0, "A")
	r.SchemaVersion, r.Protocol, r.Error = 2, protocol, errMsg
	return r
}

func TestJoinAcceptsSuccessfulHTTP11SchemaV2Records(t *testing.T) {
	result, err := join([]oracle.Record{v2("HTTP/1.1", "")}, []oracle.Record{v2("HTTP/1.1", "")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.DAcc != 1 {
		t.Fatalf("got %+v", result)
	}
}

func TestJoinRejectsHTTP2OnTheObservedSideToo(t *testing.T) {
	if _, err := join([]oracle.Record{v2("HTTP/1.1", "")}, []oracle.Record{v2("HTTP/2", "")}, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}

// Deliberate (docs/m0-demo-readiness.md section 2): the legacy join refuses
// failed-request input rather than scoring it, so a single failed schema-v2
// record makes the whole run unscorable here.
func TestJoinRejectsTheWholeRunOnAnyFailedSchemaV2Request(t *testing.T) {
	expected := []oracle.Record{v2("HTTP/1.1", ""), v2("HTTP/1.1", "status: 502")}
	expected[1].StreamID = 1
	if _, err := join(expected, nil, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}

// --- readRecords --------------------------------------------------------------

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log.jsonl")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// readRecords must read back exactly what oracle.Writer produced.
func TestReadRecordsRoundTripsWriterOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	w, err := oracle.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []oracle.Record{rec("5t|0", 0, "A"), rec("5t|0", 1, "B")}
	for _, r := range want {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := readRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// A writer killed mid-record leaves a truncated last line. Dropping it
// silently would turn a capture loss into a smaller, cleaner-looking run.
func TestReadRecordsRejectsATruncatedLine(t *testing.T) {
	path := writeFile(t, `{"side":"load-gen","conn_key":"5t|0","stream_id":0,"trace_id":"A"}`+"\n"+`{"side":"load-ge`)
	if _, err := readRecords(path); err == nil {
		t.Fatal("want an error, got nil")
	}
}

func TestReadRecordsRejectsABlankLine(t *testing.T) {
	path := writeFile(t, `{"side":"load-gen","trace_id":"A"}`+"\n\n"+`{"side":"load-gen","trace_id":"B"}`+"\n")
	if _, err := readRecords(path); err == nil {
		t.Fatal("want an error, got nil")
	}
}

func TestReadRecordsReportsAMissingFile(t *testing.T) {
	if _, err := readRecords(filepath.Join(t.TempDir(), "absent.jsonl")); err == nil {
		t.Fatal("want an error, got nil")
	}
}

// Records past the scanner's 1 MiB cap must fail loudly, not end the read
// early as if the file stopped there.
func TestReadRecordsRejectsALineOverTheScannerCap(t *testing.T) {
	huge := `{"side":"load-gen","trace_id":"A","error":"` + strings.Repeat("x", 1<<20) + `"}` + "\n"
	path := writeFile(t, `{"side":"load-gen","trace_id":"B"}`+"\n"+huge)
	if _, err := readRecords(path); err == nil {
		t.Fatal("want an error, got nil")
	}
}

// --- CLI ------------------------------------------------------------------------

// main() calls log.Fatal, so it runs in a re-executed copy of the test binary.
func runJoinCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestJoinCLIHelper$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "JOIN_CLI_HELPER=1")
	out, err := cmd.Output()
	return string(out), err
}

func TestJoinCLIHelper(t *testing.T) {
	if os.Getenv("JOIN_CLI_HELPER") != "1" {
		t.Skip("subprocess entry point for runJoinCLI")
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"join"}, os.Args[i+1:]...)
			break
		}
	}
	main()
	os.Exit(0)
}

// The printed summary is what the README's result tables are copied from.
func TestJoinCLIPrintsTheDocumentedSummary(t *testing.T) {
	line := `{"side":"%s","conn_key":"5t|0","stream_id":%d,"trace_id":"%s"}` + "\n"
	lg := writeFile(t, fmt.Sprintf(line, "load-gen", 0, "A")+fmt.Sprintf(line, "load-gen", 1, "B"))
	fu := writeFile(t, fmt.Sprintf(line, "fake-upstream", 0, "A"))

	out, err := runJoinCLI(t, "-load-gen", lg, "-fake-upstream", fu)
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	want := "expected=2 joined=1 matched=1 join_misses=1 D_acc=0.5000 (fake_upstream_records=1 shuffle=false)\n"
	if out != want {
		t.Fatalf("got  %q\nwant %q", out, want)
	}
}

func TestJoinCLIExitsNonZeroOnACollision(t *testing.T) {
	line := `{"side":"load-gen","conn_key":"5t|0","stream_id":0,"trace_id":"A"}` + "\n"
	lg := writeFile(t, line+line)
	fu := writeFile(t, "")
	if _, err := runJoinCLI(t, "-load-gen", lg, "-fake-upstream", fu); err == nil {
		t.Fatal("join exited 0 on a duplicate key")
	}
}

func TestJoinCLIExitsNonZeroOnAMissingInput(t *testing.T) {
	fu := writeFile(t, "")
	missing := filepath.Join(t.TempDir(), "absent.jsonl")
	if _, err := runJoinCLI(t, "-load-gen", missing, "-fake-upstream", fu); err == nil {
		t.Fatal("join exited 0 with a missing load-gen log")
	}
	if _, err := runJoinCLI(t, "-load-gen", fu, "-fake-upstream", missing); err == nil {
		t.Fatal("join exited 0 with a missing fake-upstream log")
	}
}
