package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cilium/ebpf"
)

// Phase 4 spec 8.13: the probe can watch several server ports and, on
// request, keep the server -> client direction too (dir "s2c"), so that the
// requests read and the responses sent by a proxy are seen as well as its
// calls. Without -both-directions nothing changes.

const inPort, upPort = 44765, 37491

func TestParseFlagsTakesSeveralPortsAndTheDirectionOption(t *testing.T) {
	cfg, err := parseFlags([]string{"-ports=44765,37491", "-both-directions"})
	if err != nil || cfg.ports != (portSet{inPort, upPort}) || !cfg.bothDirections {
		t.Errorf("parseFlags(-ports, -both-directions) = %+v, %v", cfg, err)
	}
	cfg, err = parseFlags([]string{"-port=8443"})
	if err != nil || cfg.ports != (portSet{8443}) || cfg.bothDirections {
		t.Errorf("parseFlags(-port) = %+v, %v", cfg, err)
	}
	for _, args := range [][]string{
		{"-ports=1,0"}, {"-ports=1,65536"}, {"-ports=1,x"}, {"-ports=1,1"}, {"-ports="},
		{"-ports=1,2,3,4,5,6,7,8,9"}, {"-port=1", "-ports=2"},
	} {
		if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), "port") {
			t.Errorf("parseFlags(%v) = %v, want an error about the ports", args, err)
		}
	}
}

func TestDecodeRecordTellsTheServerToClientDirection(t *testing.T) {
	watched := portSet{inPort, upPort}
	cases := []struct {
		name          string
		ev            streamEvent
		dir           string
		client, sport uint16
	}{
		{"the proxy sends its call", v4(kindSend, 40001, upPort, 1, 10), "", 40001, upPort},
		{"the upstream reads the call", v4(kindRecv, upPort, 40001, 1, 10), "", 40001, upPort},
		{"the upstream sends its response", v4(kindSend, upPort, 40001, 1, 10), "s2c", 40001, upPort},
		{"the proxy reads the response", v4(kindRecv, 40001, upPort, 1, 10), "s2c", 40001, upPort},
		{"the proxy reads a request", v4(kindRecv, inPort, 50002, 1, 10), "", 50002, inPort},
		{"the proxy sends its response", v4(kindSend, inPort, 50002, 1, 10), "s2c", 50002, inPort},
	}
	for _, c := range cases {
		rec, err := decodeRecord(encode(t, c.ev), watched)
		if err != nil || rec.Dir != c.dir || rec.ClientPort != c.client || rec.ServerPort != c.sport {
			t.Errorf("%s: decodeRecord() = %+v, %v; want dir %q, client %d, server %d",
				c.name, rec, err, c.dir, c.client, c.sport)
		}
	}
}

func TestRunWritesEveryPortAndTheDirectionOption(t *testing.T) {
	withRunSeams(t)
	var ports []uint16
	var both, set bool
	setPorts = func(_ *ebpf.Map, p []uint16) error { ports = p; return nil }
	setBothDirections = func(_ *ebpf.Map, b bool) error { both, set = b, true; return nil }
	os.Args = []string{"stream-probe", "-ports=44765,37491", "-both-directions",
		"-out=" + filepath.Join(t.TempDir(), "o.jsonl")}
	if err := run(); err != nil {
		t.Fatalf("run() = %v", err)
	}
	if len(ports) != 2 || ports[0] != inPort || ports[1] != upPort || !set || !both {
		t.Errorf("written to the kernel: ports %v, both directions %v (set %v)", ports, both, set)
	}
}

// The kernel side keeps the server -> client direction only when asked: each
// hook that may emit it reads the option first.
func TestTheKernelKeepsTheOtherDirectionOnlyWhenAsked(t *testing.T) {
	src := probeSource(t)
	for _, hook := range []string{"on_tcp_sendmsg_locked", "on_tcp_cleanup_rbuf"} {
		body := src[strings.Index(src, hook):]
		body = body[:strings.Index(body, "\n}")]
		if !strings.Contains(body, "both_directions()") {
			t.Errorf("%s does not read the direction option", hook)
		}
	}
	if !strings.Contains(src, "BPF_MAP_TYPE_HASH") {
		t.Error("the watched ports must be a set (hash map)")
	}
}

// The probe source still builds for the kernel.
func TestTheProbeCompiles(t *testing.T) {
	clang, err := exec.LookPath("clang")
	if err != nil {
		t.Skip("clang not found")
	}
	headers := os.Getenv("BPF_HEADERS")
	if headers == "" {
		headers = "../strong-tier-probe/bpf" // where hn-propagation.sh looks by default
	}
	if _, err := os.Stat(filepath.Join(headers, "vmlinux.h")); err != nil {
		t.Skip("no vmlinux.h in " + headers + ": set BPF_HEADERS")
	}
	out := filepath.Join(t.TempDir(), "probe.o")
	cmd := exec.Command(clang, "-target", "bpf", "-D__TARGET_ARCH_x86", "-I"+headers,
		"-I"+filepath.Join(headers, "vendor"), "-g", "-O2", "-c", "bpf/probe.c", "-o", out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("clang: %v\n%s", err, b)
	}
}
