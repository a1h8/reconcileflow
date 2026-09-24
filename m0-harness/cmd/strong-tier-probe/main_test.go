package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"unsafe"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
)

// mkV4 builds the little-endian-first-octet uint32 that BPF_CORE_READ hands
// back for skc_daddr/skc_rcv_saddr — see ipv4String's comment on why byte 0
// is already the first octet.
func mkV4(a, b, c, d byte) uint32 {
	return uint32(a) | uint32(b)<<8 | uint32(c)<<16 | uint32(d)<<24
}

func TestIPv4String(t *testing.T) {
	cases := []struct {
		name string
		in   uint32
		want string
	}{
		{"loopback", mkV4(127, 0, 0, 1), "127.0.0.1"},
		{"zero (the daddr=0.0.0.0 bug this fix targets)", mkV4(0, 0, 0, 0), "0.0.0.0"},
		{"private range", mkV4(192, 168, 1, 117), "192.168.1.117"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ipv4String(c.in); got != c.want {
				t.Errorf("ipv4String(%#x) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestAddrString(t *testing.T) {
	loopbackV6 := [16]byte{}
	copy(loopbackV6[:], net.ParseIP("::1").To16())

	extV6 := [16]byte{}
	copy(extV6[:], net.ParseIP("2607:6bc0::10").To16())

	t.Run("AF_INET6 uses the v6 field", func(t *testing.T) {
		got := addrString(syscall.AF_INET6, mkV4(0, 0, 0, 0), loopbackV6)
		if got != "::1" {
			t.Errorf("addrString(AF_INET6, ...) = %q, want %q", got, "::1")
		}
	})

	t.Run("AF_INET6 with a non-loopback address", func(t *testing.T) {
		got := addrString(syscall.AF_INET6, mkV4(0, 0, 0, 0), extV6)
		if got != "2607:6bc0::10" {
			t.Errorf("addrString(AF_INET6, ...) = %q, want %q", got, "2607:6bc0::10")
		}
	})

	t.Run("AF_INET uses the v4 field, ignores v6", func(t *testing.T) {
		got := addrString(syscall.AF_INET, mkV4(127, 0, 0, 1), loopbackV6)
		if got != "127.0.0.1" {
			t.Errorf("addrString(AF_INET, ...) = %q, want %q", got, "127.0.0.1")
		}
	})

	t.Run("regression: AF_INET6 must not fall through to the v4 zero field", func(t *testing.T) {
		// This is exactly the bug iteration 5 fixed: an AF_INET6 socket's
		// skc_daddr is legitimately always 0.0.0.0 — addrString must pick
		// v6 by family, not by whether v4 looks populated.
		got := addrString(syscall.AF_INET6, mkV4(0, 0, 0, 0), loopbackV6)
		if got == "0.0.0.0" {
			t.Errorf("addrString(AF_INET6, ...) returned the v4 zero field %q instead of the v6 address", got)
		}
	})
}

func TestConnEventSize(t *testing.T) {
	// Regression guard for the hand-kept struct layout: connEvent must mirror
	// struct conn_event in bpf/probe.c byte-for-byte, including padding.
	// binary.Read infers no C struct padding on its own (see the type's
	// doc comment) — a size drift here means a silent field misalignment,
	// not a compile error, so this is the only thing that would catch it.
	const want = 80
	if got := unsafe.Sizeof(connEvent{}); got != want {
		t.Errorf("unsafe.Sizeof(connEvent{}) = %d, want %d (must mirror struct conn_event in bpf/probe.c)", got, want)
	}
}

// encodeConnEvent serializes a connEvent exactly like the real ring buffer
// would deliver it, so decodeRecord can be tested with plain bytes instead
// of a live kernel/BPF map.
func encodeConnEvent(t *testing.T, ev connEvent) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, ev); err != nil {
		t.Fatalf("binary.Write(connEvent): %v", err)
	}
	return buf.Bytes()
}

func TestDecodeRecord(t *testing.T) {
	t.Run("connect side, AF_INET6", func(t *testing.T) {
		var v6 [16]byte
		copy(v6[:], net.ParseIP("::1").To16())
		ev := connEvent{
			SocketCookie: 0xdeadbeef,
			NetnsCookie:  4026531833,
			SAddrV6:      v6,
			DAddrV6:      v6,
			SPort:        56986,
			DPort:        8443,
			PID:          12345,
			Family:       syscall.AF_INET6,
			Side:         1, // SIDE_CONNECT
			TimestampNS:  1790170513836246172,
		}
		rec, err := decodeRecord(encodeConnEvent(t, ev), "boot-id-x")
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		want := Record{
			Side: "connect", SocketCookie: 0xdeadbeef, NetnsCookie: 4026531833,
			BootID: "boot-id-x", SAddr: "::1", DAddr: "::1",
			SPort: 56986, DPort: 8443, PID: 12345, TimestampNS: 1790170513836246172,
		}
		if rec != want {
			t.Errorf("decodeRecord() = %+v, want %+v", rec, want)
		}
	})

	t.Run("accept side, AF_INET", func(t *testing.T) {
		ev := connEvent{
			SAddr:  mkV4(127, 0, 0, 1),
			DAddr:  mkV4(127, 0, 0, 1),
			SPort:  8443,
			DPort:  49176,
			Family: syscall.AF_INET,
			Side:   2, // SIDE_ACCEPT
		}
		rec, err := decodeRecord(encodeConnEvent(t, ev), "boot-id-x")
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if rec.Side != "accept" {
			t.Errorf("Side = %q, want %q", rec.Side, "accept")
		}
		if rec.SAddr != "127.0.0.1" || rec.DAddr != "127.0.0.1" {
			t.Errorf("SAddr/DAddr = %q/%q, want 127.0.0.1/127.0.0.1", rec.SAddr, rec.DAddr)
		}
	})

	t.Run("truncated sample returns an error, not a panic", func(t *testing.T) {
		raw := encodeConnEvent(t, connEvent{})
		if _, err := decodeRecord(raw[:10], "boot-id-x"); err == nil {
			t.Error("decodeRecord(truncated) returned nil error, want a decode error")
		}
	})
}

// fakeDropCounter implements dropCounter without any kernel/BPF dependency.
type fakeDropCounter struct {
	val uint64
	err error
}

func (f fakeDropCounter) Lookup(_ interface{}, valueOut interface{}) error {
	if f.err != nil {
		return f.err
	}
	*(valueOut.(*uint64)) = f.val
	return nil
}

func TestLogDrops(t *testing.T) {
	captureLog := func(t *testing.T, fn func()) string {
		t.Helper()
		var buf bytes.Buffer
		orig := log.Writer()
		log.SetOutput(&buf)
		t.Cleanup(func() { log.SetOutput(orig) })
		fn()
		return buf.String()
	}

	t.Run("zero drops", func(t *testing.T) {
		out := captureLog(t, func() { logDrops(fakeDropCounter{val: 0}) })
		if !strings.Contains(out, "events dropped, never emitted): 0") {
			t.Errorf("log output = %q, want it to report 0 drops", out)
		}
	})

	t.Run("non-zero drops", func(t *testing.T) {
		out := captureLog(t, func() { logDrops(fakeDropCounter{val: 42}) })
		if !strings.Contains(out, "events dropped, never emitted): 42") {
			t.Errorf("log output = %q, want it to report 42 drops", out)
		}
	})

	t.Run("lookup error is logged, not swallowed silently", func(t *testing.T) {
		out := captureLog(t, func() { logDrops(fakeDropCounter{err: errors.New("map closed")}) })
		if !strings.Contains(out, "map closed") {
			t.Errorf("log output = %q, want the underlying error surfaced", out)
		}
	})
}

// scriptedReader is a ringbufReader fed a fixed sequence of (record, error)
// steps — lets runLoop's control flow be tested without a kernel ring buffer.
type scriptedReader struct {
	steps []scriptedStep
	i     int
}

type scriptedStep struct {
	rec ringbuf.Record
	err error
}

func (s *scriptedReader) Read() (ringbuf.Record, error) {
	if s.i >= len(s.steps) {
		// Safety net against a mis-scripted test hanging forever: once the
		// script runs out, behave as if the caller closed the reader.
		return ringbuf.Record{}, ringbuf.ErrClosed
	}
	step := s.steps[s.i]
	s.i++
	return step.rec, step.err
}

func TestRunLoop(t *testing.T) {
	validSample := encodeConnEvent(t, connEvent{
		Side: 1, DPort: 8443, Family: syscall.AF_INET, SAddr: mkV4(127, 0, 0, 1), DAddr: mkV4(127, 0, 0, 1),
	})

	newEncoder := func() (*json.Encoder, *bytes.Buffer) {
		var buf bytes.Buffer
		return json.NewEncoder(&buf), &buf
	}
	captureLog := func(t *testing.T, fn func()) string {
		t.Helper()
		var buf bytes.Buffer
		orig := log.Writer()
		log.SetOutput(&buf)
		t.Cleanup(func() { log.SetOutput(orig) })
		fn()
		return buf.String()
	}

	t.Run("decodes a valid record then shuts down clean on Close", func(t *testing.T) {
		enc, out := newEncoder()
		rd := &scriptedReader{steps: []scriptedStep{
			{rec: ringbuf.Record{RawSample: validSample}},
			{err: ringbuf.ErrClosed},
		}}
		logOut := captureLog(t, func() { runLoop(rd, fakeDropCounter{val: 0}, enc, "boot-id-x") })

		if !strings.Contains(out.String(), `"side":"connect"`) {
			t.Errorf("encoded output = %q, want the decoded record", out.String())
		}
		if !strings.Contains(logOut, "shutting down") {
			t.Errorf("log output = %q, want a clean-shutdown line", logOut)
		}
		if !strings.Contains(logOut, "events dropped, never emitted): 0") {
			t.Errorf("log output = %q, want the drop count reported on shutdown", logOut)
		}
	})

	t.Run("a transient read error is logged and the loop continues", func(t *testing.T) {
		enc, _ := newEncoder()
		rd := &scriptedReader{steps: []scriptedStep{
			{err: errors.New("transient epoll error")},
			{err: ringbuf.ErrClosed},
		}}
		logOut := captureLog(t, func() { runLoop(rd, nil, enc, "boot-id-x") })

		if !strings.Contains(logOut, "ring buffer read error") || !strings.Contains(logOut, "transient epoll error") {
			t.Errorf("log output = %q, want the transient error logged", logOut)
		}
		if !strings.Contains(logOut, "shutting down") {
			t.Errorf("log output = %q, want the loop to still reach clean shutdown", logOut)
		}
	})

	t.Run("a malformed sample is logged and skipped, not fatal", func(t *testing.T) {
		enc, out := newEncoder()
		rd := &scriptedReader{steps: []scriptedStep{
			{rec: ringbuf.Record{RawSample: validSample[:5]}}, // truncated -> decode error
			{err: ringbuf.ErrClosed},
		}}
		logOut := captureLog(t, func() { runLoop(rd, nil, enc, "boot-id-x") })

		if !strings.Contains(logOut, "decode ring buffer record") {
			t.Errorf("log output = %q, want the decode error logged", logOut)
		}
		if out.Len() != 0 {
			t.Errorf("encoded output = %q, want nothing encoded for a malformed sample", out.String())
		}
	})

	t.Run("nil drops map is tolerated, no panic", func(t *testing.T) {
		enc, _ := newEncoder()
		rd := &scriptedReader{steps: []scriptedStep{{err: ringbuf.ErrClosed}}}
		logOut := captureLog(t, func() { runLoop(rd, nil, enc, "boot-id-x") })

		if strings.Contains(logOut, "events dropped") {
			t.Errorf("log output = %q, want no drop-count line when drops is nil", logOut)
		}
		if !strings.Contains(logOut, "shutting down") {
			t.Errorf("log output = %q, want a clean-shutdown line", logOut)
		}
	})
}

// fakeCloser satisfies both closer and ringbufReaderCloser trivially — used
// wherever run()'s wiring just needs *something* to defer .Close() on.
type fakeCloser struct{ err error }

func (f fakeCloser) Close() error { return f.err }

// fakeLinkFactory returns an attachKprobe/attachKretprobe-shaped func that
// succeeds (returning a fakeCloser) or fails with err.
func fakeLinkFactory(err error) func(string, *ebpf.Program, *link.KprobeOptions) (closer, error) {
	return func(string, *ebpf.Program, *link.KprobeOptions) (closer, error) {
		if err != nil {
			return nil, err
		}
		return fakeCloser{}, nil
	}
}

// readyReader is a ringbufReaderCloser whose Read() returns ErrClosed right
// away — lets run()'s happy path reach a clean runLoop shutdown instantly,
// no goroutine/signal needed.
type readyReader struct{}

func (readyReader) Read() (ringbuf.Record, error) { return ringbuf.Record{}, ringbuf.ErrClosed }
func (readyReader) Close() error                  { return nil }

// withRunSeams installs happy-path fakes for every var run() depends on and
// returns a restore func; each field can be overridden by the caller before
// invoking run() to exercise one specific failure branch.
func withRunSeams(t *testing.T) {
	t.Helper()
	origRemoveMemlock, origLoadSpec, origNewColl := removeMemlock, loadCollectionSpec, newCollection
	origKprobe, origKretprobe, origReader := attachKprobe, attachKretprobe, newRingbufReader
	origBootIDPath, origArgs := bootIDPath, os.Args

	removeMemlock = func() error { return nil }
	loadCollectionSpec = func(string) (*ebpf.CollectionSpec, error) { return &ebpf.CollectionSpec{}, nil }
	newCollection = func(*ebpf.CollectionSpec) (*ebpf.Collection, error) { return &ebpf.Collection{}, nil }
	attachKprobe = fakeLinkFactory(nil)
	attachKretprobe = fakeLinkFactory(nil)
	newRingbufReader = func(*ebpf.Map) (ringbufReaderCloser, error) { return readyReader{}, nil }
	bootIDPath = filepath.Join(t.TempDir(), "boot_id")
	if err := os.WriteFile(bootIDPath, []byte("6b205a5d-b47c-4ada-ad34-f8761b5365d9\n"), 0o644); err != nil {
		t.Fatalf("write fake boot_id: %v", err)
	}
	os.Args = []string{"strong-tier-probe", "-out=" + filepath.Join(t.TempDir(), "out.jsonl")}

	t.Cleanup(func() {
		removeMemlock, loadCollectionSpec, newCollection = origRemoveMemlock, origLoadSpec, origNewColl
		attachKprobe, attachKretprobe, newRingbufReader = origKprobe, origKretprobe, origReader
		bootIDPath, os.Args = origBootIDPath, origArgs
	})
}

func TestRun(t *testing.T) {
	// These mutate shared package-level vars — must not run in parallel with
	// each other or with any other test that touches the same seams.
	t.Run("happy path: every wiring step succeeds, runLoop reaches clean shutdown", func(t *testing.T) {
		withRunSeams(t)
		if err := run(); err != nil {
			t.Fatalf("run() = %v, want nil", err)
		}
	})

	t.Run("read boot_id failure", func(t *testing.T) {
		withRunSeams(t)
		bootIDPath = filepath.Join(t.TempDir(), "does-not-exist")
		if err := run(); err == nil || !strings.Contains(err.Error(), "read boot_id") {
			t.Errorf("run() = %v, want an error containing %q", err, "read boot_id")
		}
	})

	t.Run("remove memlock rlimit failure", func(t *testing.T) {
		withRunSeams(t)
		removeMemlock = func() error { return errors.New("rlimit denied") }
		if err := run(); err == nil || !strings.Contains(err.Error(), "remove memlock rlimit") {
			t.Errorf("run() = %v, want an error containing %q", err, "remove memlock rlimit")
		}
	})

	t.Run("load collection spec failure", func(t *testing.T) {
		withRunSeams(t)
		loadCollectionSpec = func(string) (*ebpf.CollectionSpec, error) { return nil, errors.New("bad ELF") }
		if err := run(); err == nil || !strings.Contains(err.Error(), "load collection spec") {
			t.Errorf("run() = %v, want an error containing %q", err, "load collection spec")
		}
	})

	t.Run("load collection into kernel failure", func(t *testing.T) {
		withRunSeams(t)
		newCollection = func(*ebpf.CollectionSpec) (*ebpf.Collection, error) { return nil, errors.New("EPERM") }
		if err := run(); err == nil || !strings.Contains(err.Error(), "load collection into kernel") {
			t.Errorf("run() = %v, want an error containing %q", err, "load collection into kernel")
		}
	})

	t.Run("attach kprobe failure", func(t *testing.T) {
		withRunSeams(t)
		attachKprobe = fakeLinkFactory(errors.New("no such symbol"))
		if err := run(); err == nil || !strings.Contains(err.Error(), "attach kprobe tcp_connect") {
			t.Errorf("run() = %v, want an error containing %q", err, "attach kprobe tcp_connect")
		}
	})

	t.Run("attach kretprobe failure", func(t *testing.T) {
		withRunSeams(t)
		attachKretprobe = fakeLinkFactory(errors.New("no such symbol"))
		if err := run(); err == nil || !strings.Contains(err.Error(), "attach kretprobe inet_csk_accept") {
			t.Errorf("run() = %v, want an error containing %q", err, "attach kretprobe inet_csk_accept")
		}
	})

	t.Run("open ring buffer reader failure", func(t *testing.T) {
		withRunSeams(t)
		newRingbufReader = func(*ebpf.Map) (ringbufReaderCloser, error) { return nil, errors.New("bad map") }
		if err := run(); err == nil || !strings.Contains(err.Error(), "open ring buffer reader") {
			t.Errorf("run() = %v, want an error containing %q", err, "open ring buffer reader")
		}
	})

	t.Run("create output failure", func(t *testing.T) {
		withRunSeams(t)
		os.Args = []string{"strong-tier-probe", "-out=" + filepath.Join(t.TempDir(), "no-such-dir", "out.jsonl")}
		if err := run(); err == nil || !strings.Contains(err.Error(), "create output") {
			t.Errorf("run() = %v, want an error containing %q", err, "create output")
		}
	})
}

func TestReadBootID(t *testing.T) {
	t.Run("real boot_id file", func(t *testing.T) {
		got, err := readBootID(bootIDPath)
		if err != nil {
			t.Fatalf("readBootID(%q) error: %v (expects this file to exist, true on any Linux CI runner)", bootIDPath, err)
		}
		uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
		if !uuidRe.MatchString(got) {
			t.Errorf("readBootID() = %q, want a UUID-shaped string with no surrounding whitespace", got)
		}
	})

	t.Run("trims surrounding whitespace", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "boot_id")
		if err := os.WriteFile(path, []byte("6b205a5d-b47c-4ada-ad34-f8761b5365d9\n"), 0o644); err != nil {
			t.Fatalf("write tmpfile: %v", err)
		}
		got, err := readBootID(path)
		if err != nil {
			t.Fatalf("readBootID(%q): %v", path, err)
		}
		if got != "6b205a5d-b47c-4ada-ad34-f8761b5365d9" {
			t.Errorf("readBootID() = %q, want it trimmed of the trailing newline", got)
		}
	})

	t.Run("missing file returns an error", func(t *testing.T) {
		if _, err := readBootID(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
			t.Error("readBootID(missing path) returned nil error, want one")
		}
	})
}

func TestParseFlags(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		cfg := parseFlags(nil)
		want := config{objPath: "bpf/probe.o", outPath: "strong-tier.jsonl"}
		if cfg != want {
			t.Errorf("parseFlags(nil) = %+v, want %+v", cfg, want)
		}
	})

	t.Run("overrides", func(t *testing.T) {
		cfg := parseFlags([]string{"-obj=custom.o", "-out=custom.jsonl"})
		want := config{objPath: "custom.o", outPath: "custom.jsonl"}
		if cfg != want {
			t.Errorf("parseFlags(overrides) = %+v, want %+v", cfg, want)
		}
	})
}

func TestNewOutputEncoder(t *testing.T) {
	t.Run("writes valid JSONL", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.jsonl")
		out, enc, err := newOutputEncoder(path)
		if err != nil {
			t.Fatalf("newOutputEncoder(%q): %v", path, err)
		}
		if err := enc.Encode(Record{Side: "connect", BootID: "x"}); err != nil {
			t.Fatalf("enc.Encode: %v", err)
		}
		out.Close()

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read back %q: %v", path, err)
		}
		if !strings.Contains(string(data), `"boot_id":"x"`) {
			t.Errorf("output file = %q, want it to contain the encoded record", data)
		}
	})

	t.Run("unwritable path returns an error", func(t *testing.T) {
		if _, _, err := newOutputEncoder(filepath.Join(t.TempDir(), "no-such-dir", "out.jsonl")); err == nil {
			t.Error("newOutputEncoder(bad path) returned nil error, want one")
		}
	})
}
