// http-signal-probe is the POC for docs/target/reconcileflow-traceability-spec.md
// §4: does a stateless-in-kernel eBPF design (bpf/probe.c — a single uprobe,
// no maps beyond the ring buffer and a drop counter) sustain the exact load
// that broke OBI's direct propagation (OBI#3571 — a hard cliff at ~500
// requests on one connection-pooled backend connection), with zero event
// loss? This loader mirrors cmd/strong-tier-probe's structure deliberately —
// same architecture, already proven at n=10000/0 drops for kernel-level
// events; this extends it one layer up, to the Go HTTP handler entry point,
// without adding any bounded per-request state.
package main

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
)

// httpEvent mirrors struct http_event in bpf/probe.c byte-for-byte.
type httpEvent struct {
	PidTgid     uint64
	TimestampNS uint64
}

type Record struct {
	PID         uint32 `json:"pid"`
	TimestampNS uint64 `json:"timestamp_ns"`
}

// closer is the one method run() needs from an attached uprobe -- the same
// narrowing as cmd/strong-tier-probe (link.Link is sealed, so a test fake
// cannot implement it).
type closer interface {
	Close() error
}

// Seams over the cilium/ebpf entry points, same pattern as
// cmd/strong-tier-probe: each defaults to the real call, tests reassign and
// restore them to run the whole wiring in run() without a kernel or CAP_BPF.
var (
	removeMemlock      = rlimit.RemoveMemlock
	loadCollectionSpec = ebpf.LoadCollectionSpec
	newCollection      = ebpf.NewCollection
	attachUprobe       = func(binPath, symbol string, p *ebpf.Program) (closer, error) {
		ex, err := link.OpenExecutable(binPath)
		if err != nil {
			return nil, fmt.Errorf("open executable %s: %w", binPath, err)
		}
		return ex.Uprobe(symbol, p, nil)
	}
	newRingbufReader = func(m *ebpf.Map) (ringbufReaderCloser, error) { return ringbuf.NewReader(m) }
)

type ringbufReader interface {
	Read() (ringbuf.Record, error)
}

type ringbufReaderCloser interface {
	ringbufReader
	Close() error
}

type dropCounter interface {
	Lookup(key, valueOut interface{}) error
}

type config struct {
	binPath string
	objPath string
	outPath string
	symbol  string
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("http-signal-probe", flag.ContinueOnError)
	binPath := fs.String("binary", "", "path to the traced Go executable (must match the running process's own binary)")
	objPath := fs.String("obj", "bpf/probe.o", "compiled BPF object")
	outPath := fs.String("out", "http-signal.jsonl", "output JSONL path")
	symbol := fs.String("symbol", autoSymbol, "Go symbol to uprobe; \"auto\" picks the HTTP/2 "+
		"request-dispatch entry present in -binary (its name depends on the Go version that built it)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if *binPath == "" {
		return config{}, errors.New("-binary is required")
	}
	return config{binPath: *binPath, objPath: *objPath, outPath: *outPath, symbol: *symbol}, nil
}

const autoSymbol = "auto"

// http2DispatchSymbols is HTTP/2's per-request dispatch entry, newest Go
// first: Go 1.27 has it under net/http/internal/http2, Go 1.22 still bundles
// it into net/http (both checked with go tool nm on a built fake-upstream).
// The 2026-09-25 README result was measured on this entry point because
// net/http.(*serverHandler).ServeHTTP reportedly never fired on HTTP/2.
var http2DispatchSymbols = []string{
	"net/http/internal/http2.(*serverConn).runHandler",
	"net/http.(*http2serverConn).runHandler",
}

// resolveSymbol returns the first http2DispatchSymbols entry that is a
// function in the ELF binary at path -- the binary the uprobe attaches to.
func resolveSymbol(path string) (string, error) {
	f, err := elf.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	syms, err := f.Symbols()
	if err != nil {
		return "", err
	}
	present := make(map[string]bool, len(syms))
	for _, s := range syms {
		if elf.ST_TYPE(s.Info) == elf.STT_FUNC {
			present[s.Name] = true
		}
	}
	for _, name := range http2DispatchSymbols {
		if present[name] {
			return name, nil
		}
	}
	return "", fmt.Errorf("none of %v is a function in %s", http2DispatchSymbols, path)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if errors.Is(err, flag.ErrHelp) {
		return nil // -h: usage already printed, as flag.ExitOnError did before
	}
	if err != nil {
		return err
	}

	if cfg.symbol == autoSymbol {
		if cfg.symbol, err = resolveSymbol(cfg.binPath); err != nil {
			return fmt.Errorf("resolve -symbol auto: %w", err)
		}
		log.Printf("-symbol auto resolved to %s", cfg.symbol)
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

	up, err := attachUprobe(cfg.binPath, cfg.symbol, coll.Programs["on_serve_http"])
	if err != nil {
		return fmt.Errorf("attach uprobe %s: %w", cfg.symbol, err)
	}
	defer up.Close()

	rd, err := newRingbufReader(coll.Maps["events"])
	if err != nil {
		return fmt.Errorf("open ring buffer reader: %w", err)
	}
	defer rd.Close()

	out, err := os.Create(cfg.outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		rd.Close()
	}()

	log.Printf("http-signal-probe attached (%s in %s), writing to %s", cfg.symbol, cfg.binPath, cfg.outPath)

	var drops dropCounter
	if m := coll.Maps["drops"]; m != nil {
		drops = m // typed-nil guard, as in cmd/strong-tier-probe
	}
	st := runLoop(rd, drops, json.NewEncoder(out))
	if st.decodeFailures != 0 || st.writeFailures != 0 {
		return fmt.Errorf("events lost in userspace: %d undecodable, %d not written", st.decodeFailures, st.writeFailures)
	}
	return nil
}

// loopStats separates what reached the output file from what was read but
// lost on the way: the "zero loss" claim rests on the first number, and it
// must not silently include the other two.
type loopStats struct {
	recorded       uint64
	decodeFailures uint64
	writeFailures  uint64
}

func runLoop(rd ringbufReader, drops dropCounter, enc *json.Encoder) loopStats {
	var st loopStats
	for {
		raw, err := rd.Read()
		if err != nil {
			if errors.Is(err, ringbuf.ErrClosed) {
				if drops != nil {
					logDrops(drops)
				}
				log.Printf("shutting down, events recorded: %d, undecodable: %d, write failures: %d",
					st.recorded, st.decodeFailures, st.writeFailures)
				return st
			}
			log.Printf("ring buffer read error: %v", err)
			continue
		}

		rec, err := decodeRecord(raw.RawSample)
		if err != nil {
			st.decodeFailures++
			log.Printf("decode ring buffer record: %v", err)
			continue
		}
		if err := enc.Encode(rec); err != nil {
			st.writeFailures++
			log.Printf("write record: %v", err)
			continue
		}
		st.recorded++
	}
}

// decodeRecord is the only place httpEvent's wire layout and Record's JSON
// shape meet. A sample of the wrong size means probe.c and httpEvent have
// drifted apart; it is rejected, not truncated or zero-padded into a record.
func decodeRecord(raw []byte) (Record, error) {
	var ev httpEvent
	if want := binary.Size(ev); len(raw) != want {
		return Record{}, fmt.Errorf("sample is %d bytes, struct http_event is %d", len(raw), want)
	}
	if err := binary.Read(bytes.NewReader(raw), binary.LittleEndian, &ev); err != nil {
		return Record{}, err
	}
	return Record{PID: uint32(ev.PidTgid >> 32), TimestampNS: ev.TimestampNS}, nil
}

func logDrops(m dropCounter) {
	var n uint64
	if err := m.Lookup(uint32(0), &n); err != nil {
		log.Printf("read drop counter: %v", err)
		return
	}
	log.Printf("ring buffer reserve failures (events dropped, never emitted): %d", n)
}
