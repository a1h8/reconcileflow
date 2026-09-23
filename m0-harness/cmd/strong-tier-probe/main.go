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
	"flag"
	"fmt"
	"log"
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
// 7-byte pad the C compiler inserts before timestamp_ns to keep it 8-byte
// aligned. binary.Read does not infer C struct padding on its own — this must
// stay in lockstep with probe.c by hand, there is no shared source of truth
// between the two languages here.
type connEvent struct {
	SocketCookie uint64 // raw `struct sock *` value, not a SO_COOKIE-style counter — see bpf/probe.c STATUS
	NetnsCookie  uint64 // net->ns.inum via CO-RE, not the bpf_get_netns_cookie() helper
	SAddr        uint32
	DAddr        uint32
	SPort        uint16
	DPort        uint16
	PID          uint32
	Side         uint8
	_            [7]byte
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

func main() {
	objPath := flag.String("obj", "bpf/probe.o", "compiled BPF object (clang -target bpf bpf/probe.c)")
	outPath := flag.String("out", "strong-tier.jsonl", "output JSONL path")
	flag.Parse()

	bootID, err := readBootID()
	if err != nil {
		log.Fatalf("read boot_id: %v", err)
	}

	// BPF programs and maps are memlock-accounted; on kernels before 5.11 (and
	// as good practice generally) this must be raised before loading, or the
	// load fails with EPERM even when run as root.
	if err := rlimit.RemoveMemlock(); err != nil {
		log.Fatalf("remove memlock rlimit: %v", err)
	}

	spec, err := ebpf.LoadCollectionSpec(*objPath)
	if err != nil {
		log.Fatalf("load collection spec from %s: %v", *objPath, err)
	}

	coll, err := ebpf.NewCollection(spec)
	if err != nil {
		log.Fatalf("load collection into kernel (needs CAP_BPF/root — see STATUS comment): %v", err)
	}
	defer coll.Close()

	kp, err := link.Kprobe("tcp_connect", coll.Programs["on_tcp_connect"], nil)
	if err != nil {
		log.Fatalf("attach kprobe tcp_connect: %v", err)
	}
	defer kp.Close()

	krp, err := link.Kretprobe("inet_csk_accept", coll.Programs["on_inet_csk_accept"], nil)
	if err != nil {
		log.Fatalf("attach kretprobe inet_csk_accept: %v", err)
	}
	defer krp.Close()

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
		rd.Close() // unblocks rd.Read() below with ringbuf.ErrClosed
	}()

	log.Printf("strong-tier-probe attached (tcp_connect + inet_csk_accept), boot_id=%s, writing to %s", bootID, *outPath)

	var ev connEvent
	for {
		rec, err := rd.Read()
		if err != nil {
			if err == ringbuf.ErrClosed {
				log.Println("shutting down")
				return
			}
			log.Printf("ring buffer read error: %v", err)
			continue
		}

		if err := binary.Read(bytes.NewReader(rec.RawSample), binary.LittleEndian, &ev); err != nil {
			log.Printf("decode ring buffer record: %v", err)
			continue
		}

		side := "connect"
		if ev.Side == 2 {
			side = "accept"
		}

		_ = enc.Encode(Record{
			Side:         side,
			SocketCookie: ev.SocketCookie,
			NetnsCookie:  ev.NetnsCookie,
			BootID:       bootID,
			SAddr:        ipv4String(ev.SAddr),
			DAddr:        ipv4String(ev.DAddr),
			SPort:        ev.SPort,
			DPort:        ev.DPort,
			PID:          ev.PID,
			TimestampNS:  ev.TimestampNS,
		})
	}
}

func readBootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
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
