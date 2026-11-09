package streamcore

import (
	"fmt"
	"strings"
	"testing"
)

const ms = uint64(1_000_000) // ns

var cfg = Config{Lateness: 100 * ms, Idle: 90_000 * ms, Retain: 300_000 * ms, MaxOpen: 100}

func ev(kind string, port int, seq, length uint32, ts uint64) Event {
	return Event{Kind: kind, Client: fmt.Sprintf("127.0.0.1:%d", port), Server: "127.0.0.1:4000", Seq: seq, Len: length, TS: ts}
}

func drops(n, ts uint64) Event { return Event{Kind: "drops", Drops: n, TS: ts} }

func run(c Config, events ...Event) []Batch {
	var out []Batch
	a := New(c, func(b Batch) { out = append(out, b) })
	for _, e := range events {
		a.Add(e)
	}
	a.Flush()
	return out
}

func offsets(b Batch) string {
	var s []string
	for _, m := range b.Messages {
		s = append(s, fmt.Sprintf("%s@%d", m.Kind, m.Offset))
	}
	return strings.Join(s, " ")
}

// a whole connection: opened at both ends, two requests each way, closed twice
func wholeConn(port int, base uint32, t0 uint64) []Event {
	return []Event{
		ev("connect", port, base, 0, t0),
		ev("accept", port, base, 0, t0+1*ms),
		ev("send", port, base, 300, t0+2*ms),
		ev("recv", port, base, 300, t0+3*ms),
		ev("send", port, base+300, 392, t0+10*ms),
		ev("recv", port, base+300, 392, t0+11*ms),
		ev("close", port, 0, 0, t0+20*ms),
		ev("close", port, 0, 0, t0+21*ms),
	}
}

func TestTwoInterleavedConnectionsGiveTwoBatchesInOffsetOrder(t *testing.T) {
	a, b := wholeConn(30001, 1000, 0), wholeConn(30002, 7000, 5*ms)
	var in []Event
	in = append(in, drops(0, 0))
	for i := range a {
		in = append(in, a[i], b[i])
	}
	in = append(in, drops(0, 2_000*ms))
	out := run(cfg, in...)
	if len(out) != 2 {
		t.Fatalf("%d batches, want 2", len(out))
	}
	for i, port := range []int{30001, 30002} {
		got := out[i]
		if got.Client != fmt.Sprintf("127.0.0.1:%d", port) || got.End != "closed" || got.Loss != "none" {
			t.Errorf("batch %d: %s end %s loss %s", i, got.Client, got.End, got.Loss)
		}
		if offsets(got) != "send@0 recv@0 send@300 recv@300" {
			t.Errorf("batch %d messages %s", i, offsets(got))
		}
	}
}

// Events from different CPUs arrive a little out of order: inside the
// lateness, they are put back in time order before anything is decided.
func TestDisorderInsideTheLatenessIsUndone(t *testing.T) {
	in := wholeConn(30001, 1000, 0)
	in[1], in[2] = in[2], in[1] // a send before the accept that precedes it
	in[0], in[3] = in[3], in[0] // a read before the connect
	out := run(cfg, append(append([]Event{drops(0, 0)}, in...), drops(0, 2_000*ms))...)
	if len(out) != 1 || out[0].End != "closed" || offsets(out[0]) != "send@0 recv@0 send@300 recv@300" {
		t.Fatalf("%+v", out)
	}
}

func TestAnIdleConnectionIsHandedOverAsIdle(t *testing.T) {
	in := wholeConn(30001, 1000, 0)[:6]                  // never closed
	in = append(in, ev("send", 30009, 1, 1, 200_000*ms)) // time goes on elsewhere
	out := run(cfg, in...)
	if out[0].Client != "127.0.0.1:30001" || out[0].End != "idle" {
		t.Fatalf("first batch %+v, want 30001 idle", out[0])
	}
}

func TestAReusedFourTupleStartsANewConnection(t *testing.T) {
	first := wholeConn(30001, 1000, 0)[:6] // not closed
	second := wholeConn(30001, 90_000, 50*ms)
	out := run(cfg, append(first, second...)...)
	if len(out) != 2 || out[0].End != "reused" || out[1].End != "closed" {
		t.Fatalf("%+v", out)
	}
	if out[1].ClientBase == nil || *out[1].ClientBase != 90_000 {
		t.Fatalf("second connection base %v", out[1].ClientBase)
	}
}

// A connection opened before the probe started has no base: its events are
// kept apart, then handed over as orphans, never given an offset.
func TestEventsWithoutAnOpeningBecomeOrphansAfterTheRetention(t *testing.T) {
	in := []Event{ev("send", 30001, 5, 10, 0), ev("recv", 30001, 5, 10, 1*ms), ev("send", 30009, 1, 1, 400_000*ms)}
	out := run(cfg, in...)
	if out[0].End != "orphan" || len(out[0].Messages) != 2 || out[0].Messages[0].Offset != 0 || out[0].ClientBase != nil {
		t.Fatalf("%+v", out[0])
	}
}

func TestLossIsNoneDeclaredOrUnknown(t *testing.T) {
	clean := append(append([]Event{drops(0, 0)}, wholeConn(30001, 1000, 0)...), drops(0, 2_000*ms))
	lossy := append(append([]Event{drops(0, 0)}, wholeConn(30001, 1000, 0)...), drops(4, 2_000*ms))
	unknown := wholeConn(30001, 1000, 0) // no drop reading after it
	for name, tc := range map[string]struct {
		in   []Event
		want string
	}{"none": {clean, "none"}, "declared": {lossy, "declared"}, "unknown": {unknown, "unknown"}} {
		if got := run(cfg, tc.in...); got[0].Loss != tc.want {
			t.Errorf("%s: loss %s", name, got[0].Loss)
		}
	}
}

func TestBeyondMaxOpenTheOldestIsEvicted(t *testing.T) {
	c := cfg
	c.MaxOpen = 2
	var in []Event
	for i := range 3 {
		in = append(in, wholeConn(30001+i, uint32(1000*(i+1)), uint64(i)*ms)[:2]...)
	}
	out := run(c, in...)
	if out[0].Client != "127.0.0.1:30001" || out[0].End != "evicted" {
		t.Fatalf("first batch %+v, want 30001 evicted", out[0])
	}
	for _, b := range out[1:] {
		if b.End != "end" {
			t.Fatalf("the others are handed over at the end: %+v", b)
		}
	}
}

func TestTheSameInputGivesTheSameBatches(t *testing.T) {
	var in []Event
	for i := range 20 {
		in = append(in, wholeConn(30001+i%5, uint32(1000+i*500), uint64(i)*30*ms)...)
	}
	a, b := run(cfg, in...), run(cfg, in...)
	if show(a) != show(b) || len(a) != 20 {
		t.Fatalf("two runs on the same input differ, or not 20 batches:\n%s\n%s", show(a), show(b))
	}
}

// show prints batches by value (bases are pointers).
func show(bs []Batch) string {
	var s []string
	for _, b := range bs {
		base := func(p *uint32) string {
			if p == nil {
				return "-"
			}
			return fmt.Sprint(*p)
		}
		s = append(s, fmt.Sprintf("%s %s %s/%s %s %s %d-%d [%s]", b.Client, b.Server,
			base(b.ClientBase), base(b.ServerBase), b.End, b.Loss, b.First, b.Last, offsets(b)))
	}
	return strings.Join(s, "\n")
}

func TestOneCloseForTwoEndsLeavesTheConnectionOpen(t *testing.T) {
	in := wholeConn(30001, 1000, 0)
	out := run(cfg, in[:len(in)-1]...) // the server side never closes
	if out[0].End != "end" {
		t.Fatalf("end %s, want end (still open at the end of the input)", out[0].End)
	}
}

// A connection active again before its idle deadline is not idle at that deadline.
func TestActivityPushesTheIdleDeadline(t *testing.T) {
	in := wholeConn(30001, 1000, 0)[:4]
	in = append(in, ev("send", 30001, 1300, 10, 80_000*ms), ev("send", 30009, 1, 1, 100_000*ms))
	if out := run(cfg, in...); out[0].End != "end" {
		t.Fatalf("end %s at 100 s, active at 80 s with 90 s idle: want still open", out[0].End)
	}
}

// Orphans go after Retain from their first event, however active since.
func TestOrphansGoAfterTheRetentionFromTheirFirstEvent(t *testing.T) {
	in := []Event{ev("send", 30001, 5, 10, 0), ev("send", 30001, 15, 10, 250_000*ms)}
	in = append(in, wholeConn(30002, 1000, 310_000*ms)...)
	in = append(in, drops(0, 309_000*ms), drops(0, 312_000*ms))
	out := run(cfg, in...)
	if out[0].End != "orphan" || out[1].Client != "127.0.0.1:30002" {
		t.Fatalf("order %s, %s: the orphan goes at 300 s, before 30002 closes", out[0].End, out[1].Client)
	}
}

// Loss over a connection's life is read up to the first reading after it,
// not a later one: a loss after it is not its loss.
func TestLossIsReadUpToTheFirstReadingAfterTheConnection(t *testing.T) {
	in := append([]Event{drops(0, 0)}, wholeConn(30001, 1000, 0)[:4]...) // still open at the end
	in = append(in, drops(0, 1_000*ms), drops(5, 3_000*ms))
	if out := run(cfg, in...); out[0].Loss != "none" {
		t.Fatalf("loss %s: the loss at 3 s came after the connection's last event", out[0].Loss)
	}
}
