// stream-probe is the Go loader for bpf/probe.c (docs/target/correlation-phase4-spec.md,
// 4c (a)): for every message on the watched TCP port it writes the four-tuple
// and the TCP sequence number of the message's first byte, the same at both
// ends of the connection whatever their clocks. Connection opening (with the
// number of the first data byte), closing, and the ring buffer's lost events,
// once a second, are written too, with the wall and monotonic clocks read
// back to back. Never any payload.
//
// The kernel side keeps no per-request state: each hook reads the socket and
// emits one event, or counts a drop when the ring buffer is full.
//
//	sudo ./stream-probe -port 8443 -out stream.jsonl
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
	"golang.org/x/sys/unix"
)

// Event kinds, as in enum kind in bpf/probe.c.
const (
	kindSend    = 1 // tcp_sendmsg_locked: Seq = write_seq on entry, the first byte sent
	kindRecv    = 2 // tcp_cleanup_rbuf: Seq = copied_seq - copied, the first byte read
	kindConnect = 3 // tcp_connect: Seq = write_seq on entry, the initial number (SYN)
	kindAccept  = 4 // inet_csk_accept return: Seq = copied_seq, the first data byte
	kindClose   = 5 // tcp_close
)

var kindNames = map[uint8]string{
	kindSend: "send", kindRecv: "recv", kindConnect: "connect", kindAccept: "accept", kindClose: "close",
}

// streamEvent mirrors struct stream_event in bpf/probe.c byte for byte,
// including the explicit pad before TimestampNS. Kept in step by hand;
// TestStreamEventSize guards the size.
type streamEvent struct {
	Socket      uint64   // raw struct sock * value: one socket, for this kernel's lifetime
	SAddrV6     [16]byte // skc_v6_rcv_saddr, valid when Family == AF_INET6
	DAddrV6     [16]byte // skc_v6_daddr
	SAddr       uint32   // skc_rcv_saddr, valid when Family == AF_INET
	DAddr       uint32   // skc_daddr
	SPort       uint16   // local port, host order
	DPort       uint16   // remote port, host order
	Seq         uint32
	Len         uint32
	PID         uint32
	Family      uint16
	Kind        uint8
	_           [5]byte
	TimestampNS uint64 // bpf_ktime_get_ns: CLOCK_MONOTONIC
}

// Record is one JSONL line. The four-tuple is written client side first,
// whichever end saw it. For connect and accept, Seq is the number of the first
// data byte the client sends, so that offset = Seq of a message - Seq here.
type Record struct {
	Kind        string  `json:"kind"`
	ClientAddr  string  `json:"client_addr,omitempty"`
	ClientPort  uint16  `json:"client_port,omitempty"`
	ServerAddr  string  `json:"server_addr,omitempty"`
	ServerPort  uint16  `json:"server_port,omitempty"`
	Seq         uint32  `json:"seq"`
	Len         uint32  `json:"len"`
	PID         uint32  `json:"pid,omitempty"`
	Socket      uint64  `json:"socket,omitempty"`
	TimestampNS uint64  `json:"timestamp_ns"`
	Drops       *uint64 `json:"drops,omitempty"` // kind "drops" only: events lost since the start
	// Kind "clock" only: the wall clock (CLOCK_REALTIME) read between two
	// reads of the monotonic clock; TimestampNS is their midpoint, SpreadNS
	// the gap between them, the conversion's uncertainty.
	RealtimeNS *uint64 `json:"realtime_ns,omitempty"`
	SpreadNS   *uint64 `json:"spread_ns,omitempty"`
}

// decodeRecord turns one ring buffer sample into a Record. The end whose local
// port is the watched port is the server.
func decodeRecord(raw []byte, port uint16) (Record, error) {
	var ev streamEvent
	if err := binary.Read(bytes.NewReader(raw), binary.LittleEndian, &ev); err != nil {
		return Record{}, err
	}
	name, ok := kindNames[ev.Kind]
	if !ok {
		return Record{}, fmt.Errorf("unknown event kind %d", ev.Kind)
	}
	local := addrString(ev.Family, ev.SAddr, ev.SAddrV6)
	remote := addrString(ev.Family, ev.DAddr, ev.DAddrV6)
	rec := Record{
		Kind: name, Seq: ev.Seq, Len: ev.Len, PID: ev.PID, Socket: ev.Socket, TimestampNS: ev.TimestampNS,
		ClientAddr: local, ClientPort: ev.SPort, ServerAddr: remote, ServerPort: ev.DPort,
	}
	if ev.SPort == port {
		rec.ClientAddr, rec.ClientPort, rec.ServerAddr, rec.ServerPort = remote, ev.DPort, local, ev.SPort
	}
	if ev.Kind == kindConnect {
		rec.Seq = ev.Seq + 1 // the SYN takes the initial number; data starts after it (wraps like TCP)
	}
	return rec, nil
}

func addrString(family uint16, v4 uint32, v6 [16]byte) string {
	if family == syscall.AF_INET6 {
		return net.IP(v6[:]).String()
	}
	return fmt.Sprintf("%d.%d.%d.%d", byte(v4), byte(v4>>8), byte(v4>>16), byte(v4>>24))
}

// dropCounter is what the loops need from the drops map (*ebpf.Map has it).
type dropCounter interface {
	Lookup(key, valueOut interface{}) error
}

// dropsRecord reads the number of events the ring buffer could not hold.
func dropsRecord(m dropCounter, nowNS uint64) (Record, error) {
	var n uint64
	if err := m.Lookup(uint32(0), &n); err != nil {
		return Record{}, err
	}
	return Record{Kind: "drops", TimestampNS: nowNS, Drops: &n}, nil
}

// syncEncoder lets the event loop and the drops loop share one output.
type syncEncoder struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (s *syncEncoder) Encode(r Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.enc.Encode(r)
}

// dropsLoop writes the drop count at each tick, on the kernel's clock, so that
// a reader knows over which interval the probe lost nothing.
func dropsLoop(ticks <-chan time.Time, m dropCounter, out *syncEncoder, now func() uint64) {
	for range ticks {
		rec, err := dropsRecord(m, now())
		if err != nil {
			log.Printf("read drop counter: %v", err)
			continue
		}
		out.Encode(rec)
	}
}

// clockRecord reads monotonic, wall, monotonic, back to back: events are
// dated on the kernel's monotonic clock, agents' spans on the wall clock.
func clockRecord(mono, real func() uint64) Record {
	before := mono()
	wall := real()
	after := mono()
	spread := after - before
	return Record{Kind: "clock", TimestampNS: before + spread/2, RealtimeNS: &wall, SpreadNS: &spread}
}

// clockLoop writes a clock record at each tick.
func clockLoop(ticks <-chan time.Time, out *syncEncoder, mono, real func() uint64) {
	for range ticks {
		out.Encode(clockRecord(mono, real))
	}
}

// realtimeNS reads CLOCK_REALTIME, the wall clock.
func realtimeNS() uint64 {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_REALTIME, &ts)
	return uint64(ts.Nano())
}

// monotonicNS reads CLOCK_MONOTONIC, the clock bpf_ktime_get_ns uses.
func monotonicNS() uint64 {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts)
	return uint64(ts.Nano())
}

type ringbufReader interface {
	Read() (ringbuf.Record, error)
}

type ringbufReaderCloser interface {
	ringbufReader
	deadlineRing
	Close() error
}

// runLoop writes every event until the reader is closed, then a last drops record.
func runLoop(rd ringbufReader, drops dropCounter, out *syncEncoder, port uint16, now func() uint64) {
	for {
		rec, err := rd.Read()
		if err != nil {
			if errors.Is(err, ringbuf.ErrClosed) { // the real reader wraps the sentinel
				if drops != nil {
					if last, err := dropsRecord(drops, now()); err == nil {
						out.Encode(last)
						log.Printf("ring buffer reserve failures (events dropped, never emitted): %d", *last.Drops)
					} else {
						log.Printf("read drop counter: %v", err)
					}
				}
				log.Println("shutting down")
				return
			}
			log.Printf("ring buffer read error: %v", err)
			continue
		}
		record, err := decodeRecord(rec.RawSample, port)
		if err != nil {
			log.Printf("decode ring buffer record: %v", err)
			continue
		}
		out.Encode(record)
	}
}

type config struct {
	objPath      string
	outPath      string
	port         uint16
	journal      string // directory of segments; empty: JSONL to outPath, as before
	segmentBytes int64
	segmentAge   time.Duration
	batchRecords int
	queueBatches int
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("stream-probe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	objPath := fs.String("obj", "bpf/probe.o", "compiled BPF object (clang -target bpf bpf/probe.c)")
	outPath := fs.String("out", "stream.jsonl", "output JSONL path")
	port := fs.Int("port", 0, "server port to watch (1..65535), required")
	journal := fs.String("journal", "", "write raw segments to this directory instead of JSONL: "+
		"the reader only copies, decoding is done later (couverture-asynchrone-spec.md 4 quater)")
	segmentBytes := fs.Int64("segment-bytes", 64<<20, "with -journal: close a segment at this size")
	segmentAge := fs.Duration("segment-age", time.Second, "with -journal: close a segment at this age")
	batch := fs.Int("batch", 4096, "with -journal: records per batch handed to the writer")
	queue := fs.Int("queue-batches", 256, "with -journal: batches the queue holds before the reader waits")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if *port < 1 || *port > 65535 {
		return config{}, fmt.Errorf("-port must be in 1..65535, got %d", *port)
	}
	if *segmentBytes < journalRecordSize || *segmentAge <= 0 || *batch < 1 || *queue < 1 {
		return config{}, errors.New("-segment-bytes, -segment-age, -batch and -queue-batches must be positive")
	}
	return config{
		objPath: *objPath, outPath: *outPath, port: uint16(*port), journal: *journal,
		segmentBytes: *segmentBytes, segmentAge: *segmentAge, batchRecords: *batch, queueBatches: *queue,
	}, nil
}

type closer interface {
	Close() error
}

// Seams over the kernel-facing calls; tests replace them with fakes.
var (
	removeMemlock      = rlimit.RemoveMemlock
	loadCollectionSpec = ebpf.LoadCollectionSpec
	newCollection      = ebpf.NewCollection
	attachKprobe       = func(sym string, p *ebpf.Program, o *link.KprobeOptions) (closer, error) {
		return link.Kprobe(sym, p, o)
	}
	attachKretprobe = func(sym string, p *ebpf.Program, o *link.KprobeOptions) (closer, error) {
		return link.Kretprobe(sym, p, o)
	}
	newRingbufReader = func(m *ebpf.Map) (ringbufReaderCloser, error) { return ringbuf.NewReader(m) }
	mapDrops         = mapCounter
	setPort          = func(m *ebpf.Map, port uint16) error { return m.Update(uint32(0), port, ebpf.UpdateAny) }
)

// hooks, in attach order: the connection opens before any message is sent.
var hooks = []struct {
	ret     bool
	symbol  string
	program string
}{
	{false, "tcp_connect", "on_tcp_connect"},
	{true, "inet_csk_accept", "on_inet_csk_accept"},
	{false, "tcp_sendmsg_locked", "on_tcp_sendmsg_locked"},
	{false, "tcp_cleanup_rbuf", "on_tcp_cleanup_rbuf"},
	{false, "tcp_close", "on_tcp_close"},
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		return fmt.Errorf("flags: %w", err)
	}
	if err := removeMemlock(); err != nil {
		return fmt.Errorf("remove memlock rlimit: %w", err)
	}
	spec, err := loadCollectionSpec(cfg.objPath)
	if err != nil {
		return fmt.Errorf("load collection spec from %s: %w", cfg.objPath, err)
	}
	coll, err := newCollection(spec)
	if err != nil {
		return fmt.Errorf("load collection into kernel (needs CAP_BPF/root): %w", err)
	}
	defer coll.Close()
	if err := setPort(coll.Maps["watched"], cfg.port); err != nil {
		return fmt.Errorf("set the watched port: %w", err)
	}
	for _, h := range hooks {
		attach, what := attachKprobe, "kprobe"
		if h.ret {
			attach, what = attachKretprobe, "kretprobe"
		}
		l, err := attach(h.symbol, coll.Programs[h.program], nil)
		if err != nil {
			return fmt.Errorf("attach %s %s: %w", what, h.symbol, err)
		}
		defer l.Close()
	}
	rd, err := newRingbufReader(coll.Maps["events"])
	if err != nil {
		return fmt.Errorf("open ring buffer reader: %w", err)
	}
	defer rd.Close()
	f, where, err := openOutput(cfg)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer f.Close()
	log.Printf("stream-probe attached on port %d, writing to %s", cfg.port, where)
	drops, err := mapDrops(coll.Maps["drops"])
	if err != nil {
		return err
	}
	// Everything that needs root is done: give it all up before the first event.
	if err := dropPrivileges(f); err != nil {
		return fmt.Errorf("drop privileges: %w", err)
	}
	log.Printf("privileges dropped: uid %d, no capability", os.Getuid())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		rd.Close() // unblocks the reader with ringbuf.ErrClosed
	}()
	if cfg.journal != "" {
		return runJournal(cfg, rd, drops)
	}
	out := &syncEncoder{enc: json.NewEncoder(f)}
	out.Encode(clockRecord(monotonicNS, realtimeNS))
	clockTicker := time.NewTicker(time.Second)
	defer clockTicker.Stop()
	go clockLoop(clockTicker.C, out, monotonicNS, realtimeNS)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	go dropsLoop(ticker.C, drops, out, monotonicNS)
	runLoop(rd, drops, out, cfg.port, monotonicNS)
	out.Encode(clockRecord(monotonicNS, realtimeNS))
	return nil
}

// openOutput creates the JSONL file, or the journal directory (owner only);
// either is handed to the sudo user before privileges are dropped.
func openOutput(cfg config) (*os.File, string, error) {
	if cfg.journal == "" {
		f, err := os.Create(cfg.outPath)
		return f, cfg.outPath, err
	}
	if err := os.MkdirAll(cfg.journal, 0o700); err != nil {
		return nil, "", err
	}
	f, err := os.Open(cfg.journal)
	return f, cfg.journal + " (journal)", err
}

// runJournal: the reader copies into batches, a bounded queue absorbs bursts,
// the writer appends them to segments; drops and clock readings go through
// the same queue, so a segment holds everything needed to read it alone.
func runJournal(cfg config, rd ringbufReaderCloser, drops dropCounter) error {
	w, err := newSegmentWriter(cfg.journal, cfg.segmentBytes, cfg.segmentAge, time.Now)
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	queue := make(chan []byte, cfg.queueBatches)
	written := make(chan error, 1)
	go func() { written <- writeBatches(queue, w) }()
	queue <- appendClock(nil, clockRecord(monotonicNS, realtimeNS))

	stop := make(chan struct{})
	var ticking sync.WaitGroup
	ticking.Add(1)
	go func() {
		defer ticking.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				queue <- tickRecords(drops)
			}
		}
	}()
	st := readBatches(rd, queue, cfg.batchRecords, 100*time.Millisecond)
	close(stop)
	ticking.Wait()
	queue <- tickRecords(drops)
	close(queue)
	err = <-written
	log.Printf("journal: %d events copied, %d bad samples, %d read errors", st.events, st.bad, st.readErrors)
	log.Println("shutting down")
	return err
}

// tickRecords: the drop count, then the two clocks.
func tickRecords(drops dropCounter) []byte {
	var b []byte
	if r, err := dropsRecord(drops, monotonicNS()); err == nil {
		b = appendDrops(b, r.TimestampNS, *r.Drops)
		log.Printf("ring buffer reserve failures (events dropped, never emitted): %d", *r.Drops)
	} else {
		log.Printf("read drop counter: %v", err)
	}
	return appendClock(b, clockRecord(monotonicNS, realtimeNS))
}
