package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseSteps(t *testing.T) {
	got, err := parseSteps("10000, 20000,50000")
	if err != nil || len(got) != 3 || got[0] != 10000 || got[2] != 50000 {
		t.Fatalf("parseSteps = %v, %v", got, err)
	}
	for _, bad := range []string{"", "0", "-5", "abc", "10,,20"} {
		if _, err := parseSteps(bad); err == nil {
			t.Errorf("parseSteps(%q) accepted", bad)
		}
	}
}

// due is how many messages a connection should have sent after elapsed at
// rate per second: pacing by elapsed time, not by sleeps, so a late wake-up
// catches up instead of lowering the rate.
func TestDue(t *testing.T) {
	for _, tc := range []struct {
		rate    float64
		elapsed time.Duration
		want    int64
	}{
		{1000, time.Second, 1000},
		{1000, 1500 * time.Millisecond, 1500},
		{333.3, time.Second, 333},
		{1000, 0, 0},
	} {
		if got := due(tc.rate, tc.elapsed); got != tc.want {
			t.Errorf("due(%v, %v) = %d, want %d", tc.rate, tc.elapsed, got, tc.want)
		}
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// One short step on loopback: the step record holds the target, the kernel
// clock bounds, and as many messages sent as received.
func TestOneStepOnLoopback(t *testing.T) {
	out := filepath.Join(t.TempDir(), "burst.jsonl")
	cfg := config{port: freePort(t), conns: 2, size: 100, steps: []int{2000}, stepFor: 300 * time.Millisecond, out: out}
	if err := run(cfg); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var recs []stepRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r stepRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	if len(recs) != 2 {
		t.Fatalf("%d records, want one step and the total", len(recs))
	}
	step, total := recs[0], recs[1]
	if step.TargetPerS != 2000 || step.EndNS <= step.StartNS {
		t.Fatalf("step %+v", step)
	}
	if step.Sent < 500 || step.Sent > 700 { // 2000/s for 0.3 s = 600
		t.Fatalf("sent %d in the step, want about 600", step.Sent)
	}
	if total.Kind != "total" || total.Sent != step.Sent || total.Received != total.Sent {
		t.Fatalf("total %+v: every message sent must be received", total)
	}
}
