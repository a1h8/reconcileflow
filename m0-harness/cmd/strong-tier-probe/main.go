// strong-tier-probe is the Go loader for probe.c (docs/target/ground-truth-eval-plane-v3.md,
// oracle.connection_instance.strong: boot_id + network_namespace + socket_cookie).
//
// STATUS — iteration 2 (2026-09-23). This sandbox has no CAP_BPF and cannot
// grant it (interactive sudo only, unavailable to this tool); the user ran
// iteration 1 for real, with sudo, on the target kernel, past the privilege
// wall, and hit a verifier rejection: bpf_get_socket_cookie() is invalid for
// a BPF_PROG_TYPE_KPROBE program. Fixed in bpf/probe.c (see its STATUS
// comment) by switching to the raw socket pointer and a CO-RE netns read —
// neither of those has been checked against a real kernel yet either. Whoever
// runs this next needs one of:
//
//	sudo setcap cap_bpf,cap_perfmon,cap_sys_resource+eip ./strong-tier-probe
//	# or, simplest: sudo ./strong-tier-probe
//
// and should expect this may still take another round before it loads clean —
// nothing beyond static ELF inspection (readelf -S) and a compile pass has
// been verified here.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
)

// connEvent mirrors struct conn_event in probe.c byte-for-byte, including the
// explicit 5-byte pad before timestamp_ns to keep it 8-byte aligned.
// binary.Read does not infer C struct padding on its own — this must stay in
// lockstep with probe.c by hand, there is no shared source of truth between
// the two languages here.
type connEvent struct {
	SocketCookie uint64 // raw `struct sock *` value, not a SO_COOKIE-style counter — see bpf/probe.c STATUS
	NetnsCookie  uint64 // net->ns.inum via CO-RE, not the bpf_get_netns_cookie() helper
	SAddrV6      [16]byte
	DAddrV6      [16]byte
	SAddr        uint32
	DAddr        uint32
	SPort        uint16
	DPort        uint16
	PID          uint32
	Family       uint16 // AF_INET or AF_INET6 — picks which of SAddr/SAddrV6 (and DAddr/DAddrV6) is valid
	Side         uint8
	_            [5]byte
	TimestampNS  uint64
}

type Record struct {
	Side         string `json:"side"`          // "connect" | "accept"
	SocketCookie uint64 `json:"socket_cookie"` // raw struct sock* value, see bpf/probe.c STATUS
	NetnsCookie  uint64 `json:"netns_cookie"`  // net->ns.inum via CO-RE
	BootID       string `json:"boot_id"`
	SAddr        string `json:"saddr"`
	DAddr        string `json:"daddr"`
	SPort        uint16 `json:"sport"`
	DPort        uint16 `json:"dport"`
	PID          uint32 `json:"pid"`
	TimestampNS  uint64 `json:"timestamp_ns"`
}

// closer is the one method run() needs from a link.Kprobe/Kretprobe result
// (link.Link is a sealed interface — an unexported isLink() method blocks
// implementing it outside cilium/ebpf — so attachKprobe/attachKretprobe are
// typed to this narrower interface instead: link.Link satisfies it for free,
// and a test fake only needs Close(), no sealed method required).
type closer interface {
	Close() error
}

// Seams over the cilium/ebpf entry points main()'s wiring calls — each
// defaults to the real function; tests reassign and t.Cleanup-restore them
// to run the whole wiring sequence in run() against fakes, no kernel/CAP_BPF
// required. ebpf.Collection{} is a safe zero value for a faked newCollection
// to return (Close() ranges over nil Programs/Maps, a no-op) — see the
// package's own Close() implementation.
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
)

// ringbufReader is the one method the event loop needs from *ringbuf.Reader —
// narrowed to an interface so the loop below is testable with a scripted
// fake, no kernel ring buffer required.
type ringbufReader interface {
	Read() (ringbuf.Record, error)
}

// ringbufReaderCloser adds Close to ringbufReader — what main()'s wiring
// needs on top of what runLoop needs, so newRingbufReader can be faked too.
type ringbufReaderCloser interface {
	ringbufReader
	Close() error
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run is main()'s body, returning errors instead of calling log.Fatalf
// directly — the standard "thin main, testable run" split. Every kernel-
// facing step goes through the seams above, so this whole sequence (attach
// order, error wrapping, the drops-map typed-nil guard) is exercisable in a
// unit test with fakes, not just by attaching a real probe as we've done
// manually with sudo all session.
func run() error {
	cfg := parseFlags(os.Args[1:])

	bootID, err := readBootID(bootIDPath)
	if err != nil {
		return fmt.Errorf("read boot_id: %w", err)
	}

	// BPF programs and maps are memlock-accounted; on kernels before 5.11 (and
	// as good practice generally) this must be raised before loading, or the
	// load fails with EPERM even when run as root.
	if err := removeMemlock(); err != nil {
		return fmt.Errorf("remove memlock rlimit: %w", err)
	}

	spec, err := loadCollectionSpec(cfg.objPath)
	if err != nil {
		return fmt.Errorf("load collection spec from %s: %w", cfg.objPath, err)
	}

	coll, err := newCollection(spec)
	if err != nil {
		return fmt.Errorf("load collection into kernel (needs CAP_BPF/root — see STATUS comment): %w", err)
	}
	defer coll.Close()

	kp, err := attachKprobe("tcp_connect", coll.Programs["on_tcp_connect"], nil)
	if err != nil {
		return fmt.Errorf("attach kprobe tcp_connect: %w", err)
	}
	defer kp.Close()

	krp, err := attachKretprobe("inet_csk_accept", coll.Programs["on_inet_csk_accept"], nil)
	if err != nil {
		return fmt.Errorf("attach kretprobe inet_csk_accept: %w", err)
	}
	defer krp.Close()

	rd, err := newRingbufReader(coll.Maps["events"])
	if err != nil {
		return fmt.Errorf("open ring buffer reader: %w", err)
	}
	defer rd.Close()

	out, enc, err := newOutputEncoder(cfg.outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		rd.Close() // unblocks rd.Read() below with ringbuf.ErrClosed
	}()

	log.Printf("strong-tier-probe attached (tcp_connect + inet_csk_accept), boot_id=%s, writing to %s", bootID, cfg.outPath)

	var drops dropCounter
	if m := coll.Maps["drops"]; m != nil {
		drops = m // typed-nil guard: assign only a genuine map, never a nil *ebpf.Map boxed into the interface
	}
	runLoop(rd, drops, enc, bootID)
	return nil
}

// runLoop is main()'s event loop: decode each ring buffer sample, log and
// skip anything malformed, and shut down cleanly (reporting the drop count)
// once rd is closed. decodeRecord/logDrops are already pure; this is the
// piece that wires them together, previously only exercisable by attaching
// a real probe and running traffic — now testable with a fake ringbufReader.
func runLoop(rd ringbufReader, drops dropCounter, enc *json.Encoder, bootID string) {
	for {
		rec, err := rd.Read()
		if err != nil {
			// rd.Read() wraps the sentinel (fmt.Errorf("ringbuffer: %w", ErrClosed)),
			// so a direct == against ringbuf.ErrClosed never matches on a real
			// shutdown — that bug sent every Ctrl+C into the log.Printf/continue
			// branch below in a tight loop instead of exiting (caught 2026-09-23
			// when the process had to be killed rather than shutting down clean).
			if errors.Is(err, ringbuf.ErrClosed) {
				if drops != nil {
					logDrops(drops)
				}
				log.Println("shutting down")
				return
			}
			log.Printf("ring buffer read error: %v", err)
			continue
		}

		record, err := decodeRecord(rec.RawSample, bootID)
		if err != nil {
			log.Printf("decode ring buffer record: %v", err)
			continue
		}
		_ = enc.Encode(record)
	}
}

// bootIDPath is the one real boot_id source. A var, not a const: readBootID
// itself stays a pure function of its argument (testable against a tmpfile),
// and run()'s own read-boot_id error path is exercisable by overriding this
// var to a bad path, the same seam pattern as the vars above.
var bootIDPath = "/proc/sys/kernel/random/boot_id"

type config struct {
	objPath string
	outPath string
}

// parseFlags is main()'s flag handling, pulled out so it's testable without
// touching the process-global flag.CommandLine (flag.NewFlagSet gives each
// call its own set, safe to invoke repeatedly in tests).
func parseFlags(args []string) config {
	fs := flag.NewFlagSet("strong-tier-probe", flag.ExitOnError)
	objPath := fs.String("obj", "bpf/probe.o", "compiled BPF object (clang -target bpf bpf/probe.c)")
	outPath := fs.String("out", "strong-tier.jsonl", "output JSONL path")
	_ = fs.Parse(args)
	return config{objPath: *objPath, outPath: *outPath}
}

// newOutputEncoder opens the JSONL output file and wraps it in an encoder —
// split from main() so the file-creation error path and the encoder wiring
// are testable against a tmpdir, no kernel/CAP_BPF required.
func newOutputEncoder(path string) (*os.File, *json.Encoder, error) {
	out, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return out, json.NewEncoder(out), nil
}

// decodeRecord turns one raw ring buffer sample into the JSONL Record —
// pulled out of the read loop above so it's testable with a plain byte
// slice, no kernel/CAP_BPF required. This is the only place connEvent's
// wire layout and Record's JSON shape actually meet.
func decodeRecord(raw []byte, bootID string) (Record, error) {
	var ev connEvent
	if err := binary.Read(bytes.NewReader(raw), binary.LittleEndian, &ev); err != nil {
		return Record{}, err
	}

	side := "connect"
	if ev.Side == 2 {
		side = "accept"
	}

	return Record{
		Side:         side,
		SocketCookie: ev.SocketCookie,
		NetnsCookie:  ev.NetnsCookie,
		BootID:       bootID,
		SAddr:        addrString(ev.Family, ev.SAddr, ev.SAddrV6),
		DAddr:        addrString(ev.Family, ev.DAddr, ev.DAddrV6),
		SPort:        ev.SPort,
		DPort:        ev.DPort,
		PID:          ev.PID,
		TimestampNS:  ev.TimestampNS,
	}, nil
}

// dropCounter is the one method logDrops needs from *ebpf.Map — narrowed to
// an interface so it's testable with a fake, no kernel map required. *ebpf.Map
// satisfies this already (see its Lookup(key, valueOut any) error method).
type dropCounter interface {
	Lookup(key, valueOut interface{}) error
}

// logDrops reports how many events bpf_ringbuf_reserve() failed to reserve
// (buffer full) — these never reached userspace at all, so no amount of
// pairing/join analysis on the JSONL output can see them. A non-zero count
// means the "100%" pairing numbers only cover what the ring buffer could hold.
func logDrops(m dropCounter) {
	var n uint64
	if err := m.Lookup(uint32(0), &n); err != nil {
		log.Printf("read drop counter: %v", err)
		return
	}
	log.Printf("ring buffer reserve failures (events dropped, never emitted): %d", n)
}

func readBootID(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func ipv4String(be uint32) string {
	// skc_rcv_saddr/skc_daddr are stored network-byte-order (big-endian) in the
	// kernel; read as a raw u32 by BPF_CORE_READ, so byte 0 here is already the
	// first octet — no additional byte-swap needed, only decomposition.
	return fmt.Sprintf("%d.%d.%d.%d", byte(be), byte(be>>8), byte(be>>16), byte(be>>24))
}

// addrString picks the valid address representation by family: skc_daddr/
// skc_rcv_saddr are IPv4-only fields that the kernel never populates for an
// AF_INET6 socket (this is what caused the "daddr=0.0.0.0" symptom on this
// box, where loopback traffic to `localhost` resolves to `::1`) — the real
// address for those sockets is skc_v6_daddr/skc_v6_rcv_saddr instead.
func addrString(family uint16, v4 uint32, v6 [16]byte) string {
	if family == syscall.AF_INET6 {
		// skc_v6_daddr/skc_v6_rcv_saddr are stored in network byte order,
		// exactly what net.IP expects — no byte-swap needed here either.
		return net.IP(v6[:]).String()
	}
	return ipv4String(v4)
}
