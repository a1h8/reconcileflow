package main

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
)

// httpEvent and struct http_event have no shared source of truth: this reads
// probe.c itself so a field added on one side only fails here, not as
// silently shifted timestamps in a real run.
func TestHTTPEventMatchesProbeC(t *testing.T) {
	src, err := os.ReadFile("bpf/probe.c")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?s)struct http_event \{(.*?)\};`).FindSubmatch(src)
	if m == nil {
		t.Fatal("struct http_event not found in bpf/probe.c")
	}
	var fields []string
	for _, f := range regexp.MustCompile(`(__u\d+)\s+(\w+);`).FindAllSubmatch(m[1], -1) {
		fields = append(fields, string(f[1])+" "+string(f[2]))
	}
	want := []string{"__u64 pid_tgid", "__u64 timestamp_ns"}
	if strings.Join(fields, ",") != strings.Join(want, ",") {
		t.Fatalf("probe.c struct http_event = %v, httpEvent mirrors %v", fields, want)
	}
	if got := binary.Size(httpEvent{}); got != 16 {
		t.Fatalf("binary.Size(httpEvent) = %d, want 16", got)
	}
}

func sample(t *testing.T, pidTgid, ts uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, httpEvent{PidTgid: pidTgid, TimestampNS: ts}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestDecodeRecord(t *testing.T) {
	// bpf_get_current_pid_tgid() is tgid<<32 | tid: the process ID is the
	// upper half, the thread ID (which varies per goroutine's M) the lower.
	got, err := decodeRecord(sample(t, 4242<<32|4250, 123456789))
	if err != nil {
		t.Fatal(err)
	}
	if got != (Record{PID: 4242, TimestampNS: 123456789}) {
		t.Fatalf("decodeRecord = %+v", got)
	}

	for _, n := range []int{0, 8, 15, 17, 24} {
		if _, err := decodeRecord(make([]byte, n)); err == nil {
			t.Errorf("a %d-byte sample decoded; only 16 bytes is a struct http_event", n)
		}
	}
}

type scriptedReader struct {
	steps    []scriptedStep
	i        int
	overread int // reads past the script: the loop missed its shutdown signal
}

type scriptedStep struct {
	raw []byte
	err error
}

func (s *scriptedReader) Read() (ringbuf.Record, error) {
	if s.i >= len(s.steps) {
		s.overread++
		return ringbuf.Record{}, ringbuf.ErrClosed
	}
	step := s.steps[s.i]
	s.i++
	return ringbuf.Record{RawSample: step.raw}, step.err
}

func (s *scriptedReader) Close() error { return nil }

type fakeDropCounter struct {
	val uint64
	err error
}

func (f fakeDropCounter) Lookup(_, valueOut interface{}) error {
	if f.err != nil {
		return f.err
	}
	*(valueOut.(*uint64)) = f.val
	return nil
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })
	return &buf
}

func TestRunLoopCountsOnlyWhatReachedTheOutput(t *testing.T) {
	logs := captureLog(t)
	rd := &scriptedReader{steps: []scriptedStep{
		{raw: sample(t, 1<<32, 10)},
		{raw: make([]byte, 7)},         // layout drift: must not count as recorded
		{err: errors.New("transient")}, // read error: logged, loop continues
		{raw: sample(t, 2<<32, 20)},
		{err: fmt.Errorf("ringbuffer: %w", ringbuf.ErrClosed)}, // wrapped, as the real reader does
	}}
	var out bytes.Buffer
	st := runLoop(rd, fakeDropCounter{val: 3}, json.NewEncoder(&out))

	if st != (loopStats{recorded: 2, decodeFailures: 1}) {
		t.Fatalf("stats = %+v", st)
	}
	// The real reader wraps ErrClosed; a loop comparing with == would treat
	// shutdown as a read error and spin (the strong-tier-probe bug of
	// 2026-09-23). Only the script's fallback would then stop it.
	if rd.overread != 0 {
		t.Fatalf("loop read %d time(s) past the wrapped ErrClosed", rd.overread)
	}
	if lines := strings.Count(out.String(), "\n"); lines != 2 {
		t.Fatalf("%d lines written, want 2:\n%s", lines, out.String())
	}
	for _, want := range []string{"events dropped, never emitted): 3", "events recorded: 2, undecodable: 1", "transient"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %q:\n%s", want, logs.String())
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrShortWrite }

// Before this split, the loop incremented its count and then discarded the
// encoder's error: a full disk would still have reported every event as
// recorded.
func TestRunLoopDoesNotCountFailedWrites(t *testing.T) {
	captureLog(t)
	rd := &scriptedReader{steps: []scriptedStep{{raw: sample(t, 1<<32, 10)}, {raw: sample(t, 1<<32, 11)}}}
	st := runLoop(rd, nil, json.NewEncoder(failingWriter{}))
	if st != (loopStats{writeFailures: 2}) {
		t.Fatalf("stats = %+v", st)
	}
}

func TestLogDropsSurfacesLookupErrors(t *testing.T) {
	logs := captureLog(t)
	logDrops(fakeDropCounter{err: errors.New("map closed")})
	if !strings.Contains(logs.String(), "map closed") {
		t.Fatalf("log = %q", logs.String())
	}
}

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-binary", "/bin/fu", "-symbol", "pkg.(*T).M", "-out", "x.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg != (config{binPath: "/bin/fu", objPath: "bpf/probe.o", outPath: "x.jsonl", symbol: "pkg.(*T).M"}) {
		t.Fatalf("cfg = %+v", cfg)
	}
	if _, err := parseFlags(nil); err == nil {
		t.Fatal("missing -binary accepted")
	}
	if _, err := parseFlags([]string{"-no-such-flag"}); err == nil {
		t.Fatal("unknown flag accepted")
	}
}

type fakeCloser struct{}

func (fakeCloser) Close() error { return nil }

type uprobeCall struct{ bin, symbol string }

// withRunSeams swaps every kernel-facing call for a fake that succeeds, and
// restores them afterwards. Tests using it must not run in parallel.
func withRunSeams(t *testing.T, steps ...scriptedStep) (outPath string, call *uprobeCall) {
	t.Helper()
	captureLog(t)
	origMemlock, origSpec, origColl := removeMemlock, loadCollectionSpec, newCollection
	origUprobe, origReader := attachUprobe, newRingbufReader
	t.Cleanup(func() {
		removeMemlock, loadCollectionSpec, newCollection = origMemlock, origSpec, origColl
		attachUprobe, newRingbufReader = origUprobe, origReader
	})

	call = &uprobeCall{}
	removeMemlock = func() error { return nil }
	loadCollectionSpec = func(string) (*ebpf.CollectionSpec, error) { return &ebpf.CollectionSpec{}, nil }
	newCollection = func(*ebpf.CollectionSpec) (*ebpf.Collection, error) { return &ebpf.Collection{}, nil }
	attachUprobe = func(bin, symbol string, _ *ebpf.Program) (closer, error) {
		call.bin, call.symbol = bin, symbol
		return fakeCloser{}, nil
	}
	newRingbufReader = func(*ebpf.Map) (ringbufReaderCloser, error) { return &scriptedReader{steps: steps}, nil }
	return filepath.Join(t.TempDir(), "out.jsonl"), call
}

func TestRunHappyPathWritesEventsAndForwardsTheTarget(t *testing.T) {
	out, call := withRunSeams(t, scriptedStep{raw: sample(t, 7<<32, 99)})
	if err := run([]string{"-binary", "/bin/fu", "-symbol", "pkg.(*T).M", "-out", out}); err != nil {
		t.Fatal(err)
	}
	if *call != (uprobeCall{"/bin/fu", "pkg.(*T).M"}) {
		t.Fatalf("uprobe attached to %+v", *call)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != `{"pid":7,"timestamp_ns":99}` {
		t.Fatalf("output = %q", data)
	}
}

// A run that lost events between the ring buffer and the file must not exit
// 0: its "events recorded" line would otherwise read as a clean result.
func TestRunFailsWhenEventsAreLostInUserspace(t *testing.T) {
	out, _ := withRunSeams(t, scriptedStep{raw: make([]byte, 3)})
	err := run([]string{"-binary", "/bin/fu", "-out", out})
	if err == nil || !strings.Contains(err.Error(), "1 undecodable") {
		t.Fatalf("run() = %v", err)
	}
}

func TestRunReportsEachWiringFailure(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name   string
		break_ func()
		want   string
	}{
		{"memlock", func() { removeMemlock = func() error { return boom } }, "remove memlock rlimit"},
		{"spec", func() { loadCollectionSpec = func(string) (*ebpf.CollectionSpec, error) { return nil, boom } }, "load collection spec"},
		{"kernel", func() { newCollection = func(*ebpf.CollectionSpec) (*ebpf.Collection, error) { return nil, boom } }, "load collection into kernel"},
		{"uprobe", func() { attachUprobe = func(string, string, *ebpf.Program) (closer, error) { return nil, boom } }, "attach uprobe"},
		{"ringbuf", func() { newRingbufReader = func(*ebpf.Map) (ringbufReaderCloser, error) { return nil, boom } }, "open ring buffer reader"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, _ := withRunSeams(t)
			tc.break_()
			if err := run([]string{"-binary", "/bin/fu", "-out", out}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run() = %v, want %q", err, tc.want)
			}
		})
	}

	t.Run("output", func(t *testing.T) {
		withRunSeams(t)
		bad := filepath.Join(t.TempDir(), "missing-dir", "out.jsonl")
		if err := run([]string{"-binary", "/bin/fu", "-out", bad}); err == nil || !strings.Contains(err.Error(), "create output") {
			t.Fatalf("run() = %v", err)
		}
	})

	t.Run("missing -binary fails before touching the kernel", func(t *testing.T) {
		withRunSeams(t)
		removeMemlock = func() error { t.Fatal("reached removeMemlock"); return nil }
		if err := run(nil); err == nil || !strings.Contains(err.Error(), "-binary is required") {
			t.Fatalf("run() = %v", err)
		}
	})

	t.Run("-h is not an error", func(t *testing.T) {
		withRunSeams(t)
		if err := run([]string{"-h"}); err != nil {
			t.Fatalf("run(-h) = %v", err)
		}
	})
}

// The default symbol is a Go-internal path (net/http/internal/http2) that
// has moved between Go releases; if a toolchain upgrade renames it, the
// uprobe cannot attach. Checked against a freshly built fake-upstream, the
// binary this probe is pointed at.
func TestDefaultSymbolExistsInFakeUpstream(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}
	bin := filepath.Join(t.TempDir(), "fake-upstream")
	if out, err := exec.Command("go", "build", "-o", bin, "../fake-upstream").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	cfg, err := parseFlags([]string{"-binary", bin})
	if err != nil {
		t.Fatal(err)
	}
	f, err := elf.Open(bin)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	syms, err := f.Symbols()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range syms {
		if s.Name == cfg.symbol && elf.ST_TYPE(s.Info) == elf.STT_FUNC {
			return
		}
	}
	t.Fatalf("default -symbol %q is not a function in fake-upstream", cfg.symbol)
}
