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
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
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

func main() {
	binPath := flag.String("binary", "", "path to the traced Go executable (must match the running process's own binary)")
	objPath := flag.String("obj", "bpf/probe.o", "compiled BPF object")
	outPath := flag.String("out", "http-signal.jsonl", "output JSONL path")
	symbol := flag.String("symbol", "net/http.(*serverHandler).ServeHTTP", "Go symbol to uprobe")
	flag.Parse()

	if *binPath == "" {
		log.Fatal("-binary is required")
	}

	if err := rlimit.RemoveMemlock(); err != nil {
		log.Fatalf("remove memlock rlimit: %v", err)
	}

	spec, err := ebpf.LoadCollectionSpec(*objPath)
	if err != nil {
		log.Fatalf("load collection spec from %s: %v", *objPath, err)
	}

	coll, err := ebpf.NewCollection(spec)
	if err != nil {
		log.Fatalf("load collection into kernel (needs CAP_BPF/root): %v", err)
	}
	defer coll.Close()

	ex, err := link.OpenExecutable(*binPath)
	if err != nil {
		log.Fatalf("open executable %s: %v", *binPath, err)
	}

	up, err := ex.Uprobe(*symbol, coll.Programs["on_serve_http"], nil)
	if err != nil {
		log.Fatalf("attach uprobe %s: %v", *symbol, err)
	}
	defer up.Close()

	rd, err := ringbuf.NewReader(coll.Maps["events"])
	if err != nil {
		log.Fatalf("open ring buffer reader: %v", err)
	}
	defer rd.Close()

	out, err := os.Create(*outPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		rd.Close()
	}()

	log.Printf("http-signal-probe attached (%s in %s), writing to %s", *symbol, *binPath, *outPath)

	var ev httpEvent
	var count uint64
	for {
		rec, err := rd.Read()
		if err != nil {
			if errors.Is(err, ringbuf.ErrClosed) {
				logDrops(coll.Maps["drops"])
				log.Printf("shutting down, events recorded: %d", count)
				return
			}
			log.Printf("ring buffer read error: %v", err)
			continue
		}

		if err := binary.Read(bytes.NewReader(rec.RawSample), binary.LittleEndian, &ev); err != nil {
			log.Printf("decode ring buffer record: %v", err)
			continue
		}

		count++
		_ = enc.Encode(Record{
			PID:         uint32(ev.PidTgid >> 32),
			TimestampNS: ev.TimestampNS,
		})
	}
}

func logDrops(m *ebpf.Map) {
	if m == nil {
		return
	}
	var n uint64
	if err := m.Lookup(uint32(0), &n); err != nil {
		log.Printf("read drop counter: %v", err)
		return
	}
	log.Printf("ring buffer reserve failures (events dropped, never emitted): %d", n)
}
