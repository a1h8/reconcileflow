package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
)

const serverPort = 8443

// mkV4 builds the uint32 BPF_CORE_READ hands back for skc_daddr/skc_rcv_saddr:
// network byte order read as a little-endian u32, so byte 0 is the first octet.
func mkV4(a, b, c, d byte) uint32 {
	return uint32(a) | uint32(b)<<8 | uint32(c)<<16 | uint32(d)<<24
}

func encode(t *testing.T, ev streamEvent) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, ev); err != nil {
		t.Fatalf("binary.Write(streamEvent): %v", err)
	}
	return buf.Bytes()
}

func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })
	fn()
	return buf.String()
}

func lines(t *testing.T, b *bytes.Buffer) []Record {
	t.Helper()
	var out []Record
	dec := json.NewDecoder(b)
	for dec.More() {
		var r Record
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("decode output: %v", err)
		}
		out = append(out, r)
	}
	return out
}

func TestStreamEventSize(t *testing.T) {
	// streamEvent must mirror struct stream_event in bpf/probe.c byte for byte,
	// padding included: a drift is a silent misalignment, not a compile error.
	const want = 80
	if got := unsafe.Sizeof(streamEvent{}); got != want {
		t.Errorf("unsafe.Sizeof(streamEvent{}) = %d, want %d", got, want)
	}
	if got := binary.Size(streamEvent{}); got != want {
		t.Errorf("binary.Size(streamEvent{}) = %d, want %d", got, want)
	}
}

// v4 is one event seen from a socket whose local end is (127.0.0.1, local) and
// remote end (127.0.0.2, remote).
func v4(kind uint8, local, remote uint16, seq, length uint32) streamEvent {
	return streamEvent{
		Socket: 0xffff0001, SAddr: mkV4(127, 0, 0, 1), DAddr: mkV4(127, 0, 0, 2),
		SPort: local, DPort: remote, Seq: seq, Len: length, PID: 77,
		Family: syscall.AF_INET, Kind: kind, TimestampNS: 1_000_000,
	}
}

func TestDecodeRecord(t *testing.T) {
	t.Run("a send from the client: the local end is the client", func(t *testing.T) {
		rec, err := decodeRecord(encode(t, v4(kindSend, 40001, serverPort, 1_000_101, 120)), serverPort)
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		want := Record{
			Kind: "send", ClientAddr: "127.0.0.1", ClientPort: 40001, ServerAddr: "127.0.0.2",
			ServerPort: serverPort, Seq: 1_000_101, Len: 120, PID: 77, Socket: 0xffff0001,
			TimestampNS: 1_000_000,
		}
		if rec != want {
			t.Errorf("decodeRecord() = %+v, want %+v", rec, want)
		}
	})

	t.Run("a read on the server: the local end is the server", func(t *testing.T) {
		rec, err := decodeRecord(encode(t, v4(kindRecv, serverPort, 40001, 1_000_101, 120)), serverPort)
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if rec.Kind != "recv" || rec.ClientAddr != "127.0.0.2" || rec.ClientPort != 40001 ||
			rec.ServerAddr != "127.0.0.1" || rec.ServerPort != serverPort || rec.Seq != 1_000_101 {
			t.Errorf("decodeRecord() = %+v, want the client and server ends swapped back", rec)
		}
	})

	t.Run("connect gives the first data byte: the initial number plus one (the SYN)", func(t *testing.T) {
		rec, err := decodeRecord(encode(t, v4(kindConnect, 40001, serverPort, 1_000_100, 0)), serverPort)
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if rec.Kind != "connect" || rec.Seq != 1_000_101 {
			t.Errorf("decodeRecord() = %+v, want kind connect and seq 1000101", rec)
		}
	})

	t.Run("accept gives the first data byte as read (nothing read yet)", func(t *testing.T) {
		rec, err := decodeRecord(encode(t, v4(kindAccept, serverPort, 40001, 1_000_101, 0)), serverPort)
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if rec.Kind != "accept" || rec.Seq != 1_000_101 || rec.ClientPort != 40001 {
			t.Errorf("decodeRecord() = %+v, want kind accept, seq 1000101, client port 40001", rec)
		}
	})

	t.Run("the initial number wraps like TCP sequence numbers", func(t *testing.T) {
		rec, err := decodeRecord(encode(t, v4(kindConnect, 40001, serverPort, 0xffffffff, 0)), serverPort)
		if err != nil || rec.Seq != 0 {
			t.Errorf("decodeRecord() = %+v, %v, want seq 0 after wrapping", rec, err)
		}
	})

	t.Run("close, seen from either end", func(t *testing.T) {
		for _, ev := range []streamEvent{
			v4(kindClose, 40001, serverPort, 0, 0),
			v4(kindClose, serverPort, 40001, 0, 0),
		} {
			rec, err := decodeRecord(encode(t, ev), serverPort)
			if err != nil {
				t.Fatalf("decodeRecord: %v", err)
			}
			if rec.Kind != "close" || rec.ClientPort != 40001 || rec.ServerPort != serverPort {
				t.Errorf("decodeRecord() = %+v, want kind close on client port 40001", rec)
			}
		}
	})

	t.Run("AF_INET6 addresses", func(t *testing.T) {
		var a, b [16]byte
		copy(a[:], net.ParseIP("::1").To16())
		copy(b[:], net.ParseIP("fd00::2").To16())
		ev := v4(kindSend, 40001, serverPort, 5, 1)
		ev.Family, ev.SAddrV6, ev.DAddrV6 = syscall.AF_INET6, a, b
		rec, err := decodeRecord(encode(t, ev), serverPort)
		if err != nil {
			t.Fatalf("decodeRecord: %v", err)
		}
		if rec.ClientAddr != "::1" || rec.ServerAddr != "fd00::2" {
			t.Errorf("addresses = %q/%q, want ::1/fd00::2", rec.ClientAddr, rec.ServerAddr)
		}
	})

	t.Run("an unknown kind is an error, not a guess", func(t *testing.T) {
		if _, err := decodeRecord(encode(t, v4(9, 40001, serverPort, 0, 0)), serverPort); err == nil {
			t.Error("decodeRecord(kind 9) returned nil error")
		}
	})

	t.Run("a truncated sample is an error, not a panic", func(t *testing.T) {
		raw := encode(t, v4(kindSend, 40001, serverPort, 0, 0))
		if _, err := decodeRecord(raw[:10], serverPort); err == nil {
			t.Error("decodeRecord(truncated) returned nil error")
		}
	})
}

type fakeDropCounter struct {
	vals []uint64 // successive values; the last one repeats
	err  error
	i    int
}

func (f *fakeDropCounter) Lookup(_ interface{}, valueOut interface{}) error {
	if f.err != nil {
		return f.err
	}
	v := f.vals[len(f.vals)-1]
	if f.i < len(f.vals) {
		v = f.vals[f.i]
	}
	f.i++
	*(valueOut.(*uint64)) = v
	return nil
}

func TestDropsRecord(t *testing.T) {
	rec, err := dropsRecord(&fakeDropCounter{vals: []uint64{3}}, 42)
	if err != nil {
		t.Fatalf("dropsRecord: %v", err)
	}
	if rec.Kind != "drops" || rec.Drops == nil || *rec.Drops != 3 || rec.TimestampNS != 42 {
		t.Errorf("dropsRecord() = %+v, want kind drops, 3 drops at 42", rec)
	}
	if _, err := dropsRecord(&fakeDropCounter{err: errors.New("map closed")}, 42); err == nil {
		t.Error("dropsRecord with a failing lookup returned nil error")
	}
}

func TestDropsLoop(t *testing.T) {
	t.Run("one record per tick, with the clock at that tick", func(t *testing.T) {
		var buf bytes.Buffer
		out := &syncEncoder{enc: json.NewEncoder(&buf)}
		ticks := make(chan time.Time, 2)
		ticks <- time.Time{}
		ticks <- time.Time{}
		close(ticks)
		now := uint64(100)
		clock := func() uint64 { now += 1_000_000_000; return now }
		dropsLoop(ticks, &fakeDropCounter{vals: []uint64{0, 5}}, out, clock)
		got := lines(t, &buf)
		if len(got) != 2 || *got[0].Drops != 0 || *got[1].Drops != 5 ||
			got[0].TimestampNS != 1_000_000_100 || got[1].TimestampNS != 2_000_000_100 {
			t.Errorf("records = %+v, want drops 0 then 5, one second apart", got)
		}
	})

	t.Run("a failing lookup is logged and writes nothing", func(t *testing.T) {
		var buf bytes.Buffer
		out := &syncEncoder{enc: json.NewEncoder(&buf)}
		ticks := make(chan time.Time, 1)
		ticks <- time.Time{}
		close(ticks)
		logOut := captureLog(t, func() {
			dropsLoop(ticks, &fakeDropCounter{err: errors.New("map closed")}, out, func() uint64 { return 1 })
		})
		if buf.Len() != 0 || !strings.Contains(logOut, "map closed") {
			t.Errorf("output %q, log %q: want nothing written and the error logged", buf.String(), logOut)
		}
	})
}

func TestSyncEncoderIsSafeAcrossGoroutines(t *testing.T) {
	var buf bytes.Buffer
	out := &syncEncoder{enc: json.NewEncoder(&buf)}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out.Encode(Record{Kind: "send", Seq: uint32(i)})
		}(i)
	}
	wg.Wait()
	if got := len(lines(t, &buf)); got != 50 {
		t.Errorf("%d whole records, want 50", got)
	}
}

type scriptedReader struct {
	steps    []scriptedStep
	i        int
	overread int
}

type scriptedStep struct {
	rec ringbuf.Record
	err error
}

func (s *scriptedReader) Read() (ringbuf.Record, error) {
	if s.i >= len(s.steps) {
		s.overread++
		return ringbuf.Record{}, ringbuf.ErrClosed
	}
	step := s.steps[s.i]
	s.i++
	return step.rec, step.err
}

func TestRunLoop(t *testing.T) {
	valid := encode(t, v4(kindSend, 40001, serverPort, 7, 1))

	t.Run("writes each event, then a last drops record on a clean shutdown", func(t *testing.T) {
		var buf bytes.Buffer
		rd := &scriptedReader{steps: []scriptedStep{
			{rec: ringbuf.Record{RawSample: valid}},
			{err: errors.New("transient epoll error")},
			{rec: ringbuf.Record{RawSample: valid[:5]}},
			{err: fmt.Errorf("ringbuffer: %w", ringbuf.ErrClosed)},
		}}
		logOut := captureLog(t, func() {
			runLoop(rd, &fakeDropCounter{vals: []uint64{2}}, &syncEncoder{enc: json.NewEncoder(&buf)},
				serverPort, func() uint64 { return 9 })
		})
		got := lines(t, &buf)
		if len(got) != 2 || got[0].Kind != "send" || got[1].Kind != "drops" || *got[1].Drops != 2 {
			t.Errorf("records = %+v, want one send then the final drops record", got)
		}
		for _, want := range []string{"transient epoll error", "decode ring buffer record", "shutting down"} {
			if !strings.Contains(logOut, want) {
				t.Errorf("log = %q, want it to contain %q", logOut, want)
			}
		}
		if rd.overread != 0 {
			t.Errorf("read past a wrapped ErrClosed %d times", rd.overread)
		}
	})

	t.Run("no drops map: no drops record, still a clean shutdown", func(t *testing.T) {
		var buf bytes.Buffer
		rd := &scriptedReader{steps: []scriptedStep{{err: ringbuf.ErrClosed}}}
		logOut := captureLog(t, func() {
			runLoop(rd, nil, &syncEncoder{enc: json.NewEncoder(&buf)}, serverPort, func() uint64 { return 9 })
		})
		if buf.Len() != 0 || !strings.Contains(logOut, "shutting down") {
			t.Errorf("output %q, log %q", buf.String(), logOut)
		}
	})
}

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-port=8443", "-out=x.jsonl", "-obj=p.o"})
	if err != nil || cfg != (config{objPath: "p.o", outPath: "x.jsonl", port: 8443}) {
		t.Errorf("parseFlags() = %+v, %v", cfg, err)
	}
	cfg, err = parseFlags([]string{"-port=1"})
	if err != nil || cfg.objPath != "bpf/probe.o" || cfg.outPath != "stream.jsonl" {
		t.Errorf("defaults = %+v, %v", cfg, err)
	}
	for _, args := range [][]string{{}, {"-port=0"}, {"-port=65536"}, {"-port=-1"}} {
		if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), "port") {
			t.Errorf("parseFlags(%v) = %v, want an error about the port", args, err)
		}
	}
}

type fakeCloser struct{}

func (fakeCloser) Close() error { return nil }

type readyReader struct{}

func (readyReader) Read() (ringbuf.Record, error) { return ringbuf.Record{}, ringbuf.ErrClosed }
func (readyReader) Close() error                  { return nil }

// attached records every symbol run() attaches to, in order, and fails on one.
type attached struct {
	syms   []string
	failOn string
}

func (a *attached) factory(kind string) func(string, *ebpf.Program, *link.KprobeOptions) (closer, error) {
	return func(sym string, _ *ebpf.Program, _ *link.KprobeOptions) (closer, error) {
		a.syms = append(a.syms, kind+" "+sym)
		if sym == a.failOn {
			return nil, errors.New("no such symbol")
		}
		return fakeCloser{}, nil
	}
}

func withRunSeams(t *testing.T) (*attached, *uint16) {
	t.Helper()
	origs := []func(){}
	save := func(restore func()) { origs = append(origs, restore) }
	a, k, r, m, l, n, s, o, d := removeMemlock, loadCollectionSpec, newCollection, attachKprobe,
		attachKretprobe, newRingbufReader, setPort, os.Args, mapDrops
	save(func() {
		removeMemlock, loadCollectionSpec, newCollection, attachKprobe = a, k, r, m
		attachKretprobe, newRingbufReader, setPort, os.Args, mapDrops = l, n, s, o, d
	})
	t.Cleanup(func() {
		for _, f := range origs {
			f()
		}
	})

	at := &attached{}
	var port uint16
	removeMemlock = func() error { return nil }
	loadCollectionSpec = func(string) (*ebpf.CollectionSpec, error) { return &ebpf.CollectionSpec{}, nil }
	newCollection = func(*ebpf.CollectionSpec) (*ebpf.Collection, error) { return &ebpf.Collection{}, nil }
	attachKprobe = at.factory("kprobe")
	attachKretprobe = at.factory("kretprobe")
	newRingbufReader = func(*ebpf.Map) (ringbufReaderCloser, error) { return readyReader{}, nil }
	setPort = func(_ *ebpf.Map, p uint16) error { port = p; return nil }
	mapDrops = func(*ebpf.Map) (dropCounter, error) { return &fakeDropCounter{vals: []uint64{0}}, nil }
	os.Args = []string{"stream-probe", "-port=8443", "-out=" + filepath.Join(t.TempDir(), "out.jsonl")}
	return at, &port
}

func TestRun(t *testing.T) {
	t.Run("happy path: port set, every hook attached in order, clean shutdown", func(t *testing.T) {
		at, port := withRunSeams(t)
		if err := run(); err != nil {
			t.Fatalf("run() = %v", err)
		}
		if *port != 8443 {
			t.Errorf("port written to the kernel = %d, want 8443", *port)
		}
		want := []string{
			"kprobe tcp_connect", "kretprobe inet_csk_accept", "kprobe tcp_sendmsg_locked",
			"kprobe tcp_cleanup_rbuf", "kprobe tcp_close",
		}
		if strings.Join(at.syms, ",") != strings.Join(want, ",") {
			t.Errorf("attached %v, want %v", at.syms, want)
		}
	})

	for _, sym := range []string{"tcp_connect", "inet_csk_accept", "tcp_sendmsg_locked", "tcp_cleanup_rbuf", "tcp_close"} {
		t.Run("attach failure names "+sym, func(t *testing.T) {
			at, _ := withRunSeams(t)
			at.failOn = sym
			if err := run(); err == nil || !strings.Contains(err.Error(), sym) {
				t.Errorf("run() = %v, want an error naming %s", err, sym)
			}
		})
	}

	failures := map[string]func(){
		"port":                  func() { os.Args = []string{"stream-probe"} },
		"remove memlock rlimit": func() { removeMemlock = func() error { return errors.New("denied") } },
		"load collection spec": func() {
			loadCollectionSpec = func(string) (*ebpf.CollectionSpec, error) { return nil, errors.New("bad ELF") }
		},
		"load collection into kernel": func() {
			newCollection = func(*ebpf.CollectionSpec) (*ebpf.Collection, error) { return nil, errors.New("EPERM") }
		},
		"set the watched port": func() { setPort = func(*ebpf.Map, uint16) error { return errors.New("no map") } },
		"drops counter": func() {
			mapDrops = func(*ebpf.Map) (dropCounter, error) { return nil, errors.New("map the drops counter: EPERM") }
		},
		"open ring buffer reader": func() {
			newRingbufReader = func(*ebpf.Map) (ringbufReaderCloser, error) { return nil, errors.New("bad map") }
		},
		"create output": func() {
			os.Args = []string{"stream-probe", "-port=8443", "-out=" + filepath.Join(os.TempDir(), "no-such-dir-x", "o.jsonl")}
		},
	}
	for want, breakIt := range failures {
		t.Run(want+" failure", func(t *testing.T) {
			withRunSeams(t)
			breakIt()
			if err := run(); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("run() = %v, want an error containing %q", err, want)
			}
		})
	}
}

// The probe dates events on the kernel's monotonic clock, the agents on the
// wall clock: a clock record reads monotonic, wall, monotonic, back to back,
// and keeps the midpoint and the spread so a reader can convert either way.
func TestClockRecord(t *testing.T) {
	monos := []uint64{1_000, 1_400}
	mono := func() uint64 { v := monos[0]; monos = monos[1:]; return v }
	rec := clockRecord(mono, func() uint64 { return 1_792_000_000_000_000_000 })
	if rec.Kind != "clock" || rec.TimestampNS != 1_200 || rec.RealtimeNS == nil ||
		*rec.RealtimeNS != 1_792_000_000_000_000_000 || rec.SpreadNS == nil || *rec.SpreadNS != 400 {
		t.Errorf("clockRecord() = %+v, want clock at 1200, realtime 1.792e18, spread 400", rec)
	}
}

func TestClockLoop(t *testing.T) {
	var buf bytes.Buffer
	out := &syncEncoder{enc: json.NewEncoder(&buf)}
	ticks := make(chan time.Time, 2)
	ticks <- time.Time{}
	ticks <- time.Time{}
	close(ticks)
	mono := uint64(0)
	clockLoop(ticks, out, func() uint64 { mono += 10; return mono }, func() uint64 { return 7 })
	got := lines(t, &buf)
	if len(got) != 2 || got[0].Kind != "clock" || got[1].TimestampNS != 35 {
		t.Errorf("records = %+v, want two clock records, the second at 35", got)
	}
}

func TestRealClocksAreReadable(t *testing.T) {
	rec := clockRecord(monotonicNS, realtimeNS)
	if *rec.RealtimeNS < 1_700_000_000_000_000_000 || rec.TimestampNS == 0 || *rec.SpreadNS > 1_000_000_000 {
		t.Errorf("real clock record %+v looks wrong", rec)
	}
}

func TestRunWritesAClockRecordFirstAndLast(t *testing.T) {
	withRunSeams(t)
	outPath := strings.TrimPrefix(os.Args[2], "-out=")
	if err := run(); err != nil {
		t.Fatalf("run() = %v", err)
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	got := lines(t, bytes.NewBuffer(raw))
	if len(got) < 2 || got[0].Kind != "clock" || got[len(got)-1].Kind != "clock" {
		t.Errorf("records = %+v, want a clock record first and last", got)
	}
}
