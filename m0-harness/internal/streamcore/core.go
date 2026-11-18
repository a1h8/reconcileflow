// Package streamcore assembles the stream probe's events into one batch per
// connection, on event time (docs/target/couverture-asynchrone-spec.md,
// 4 quater, components 3 to 5). No kernel, no I/O: events in, batches out.
//
//   - Reordering: events are held until the watermark (latest time seen minus
//     Lateness) has passed them, then handled in time order, so the disorder
//     between CPUs is undone before anything is decided.
//   - State per connection (four-tuple and incarnation): the base of each end
//     (connect at the client, accept at the server), its messages with their
//     offset, its closes.
//   - A connection is over when every end seen has closed ("closed"), when it
//     has been quiet for Idle ("idle"), when its four-tuple opens again
//     ("reused"), when more than MaxOpen are open ("evicted", the oldest), or
//     at the end of the input ("end"). Events of a four-tuple with no opening
//     seen (opened before the probe) are kept apart and handed over after
//     Retain ("orphan"), never given an offset.
//   - Each batch says whether the probe declared a loss over its life: "none"
//     (drop readings before and after it, equal), "declared", or "unknown"
//     (no reading on one side). A finished connection waits for the first
//     drop reading after it, so that this is known; at the end of the input
//     it goes as it is.
//
// The same input always gives the same batches in the same order.
package streamcore

import "container/heap"

type Config struct {
	Lateness, Idle, Retain uint64 // ns
	MaxOpen                int
}

// Event is one decoded probe record. Kind: connect, accept, send, recv,
// close, drops (Drops: count since the probe started).
type Event struct {
	Kind           string
	Client, Server string // "addr:port"
	Seq, Len       uint32
	TS             uint64 // ns, the probe's monotonic clock
	Drops          uint64
}

type Message struct {
	Kind   string // send | recv
	Offset uint32 // seq - base of its end; 0 for an orphan
	Seq    uint32
	Len    uint32
	TS     uint64
}

type Batch struct {
	Client, Server         string
	ClientBase, ServerBase *uint32 // nil: that end's opening was not seen
	End                    string  // closed | idle | reused | evicted | end | orphan
	Loss                   string  // none | declared | unknown
	First, Last            uint64
	Messages               []Message
}

type key struct{ client, server string }

type conn struct {
	key                    key
	clientBase, serverBase *uint32
	closes                 int
	first, last            uint64
	messages               []Message
	opened                 uint64 // order of opening, for eviction
	version                uint64 // bumped on activity: stale deadlines are skipped
	orphan                 bool
}

type mark struct{ ts, drops uint64 }

type Assembler struct {
	cfg     Config
	emit    func(Batch)
	pending eventHeap
	arrived uint64
	maxTS   uint64

	open      map[key]*conn
	orphans   map[key]*conn
	openOrder []*conn
	opened    uint64
	deadlines deadlineHeap
	marks     []mark
	waiting   []Batch // finished, waiting for a drop reading after them
}

func New(cfg Config, emit func(Batch)) *Assembler {
	return &Assembler{cfg: cfg, emit: emit, open: map[key]*conn{}, orphans: map[key]*conn{}}
}

// Add takes one event, in any order within Lateness.
func (a *Assembler) Add(e Event) {
	a.arrived++
	heap.Push(&a.pending, pendingEvent{e, a.arrived})
	if e.TS > a.maxTS {
		a.maxTS = e.TS
	}
	watermark := uint64(0)
	if a.maxTS > a.cfg.Lateness {
		watermark = a.maxTS - a.cfg.Lateness
	}
	for a.pending.Len() > 0 && a.pending[0].e.TS <= watermark {
		a.process(heap.Pop(&a.pending).(pendingEvent).e)
	}
}

// Flush handles what is still held and hands over everything, at the end of the input.
func (a *Assembler) Flush() {
	for a.pending.Len() > 0 {
		a.process(heap.Pop(&a.pending).(pendingEvent).e)
	}
	for _, c := range a.openOrder {
		if a.open[c.key] == c {
			a.finish(c, "end")
		}
	}
	for _, c := range sortedOrphans(a.orphans) {
		a.finish(c, "orphan")
	}
	for _, b := range a.waiting {
		b.Loss = a.loss(b.First, b.Last)
		a.emit(b)
	}
	a.waiting = nil
}

func (a *Assembler) process(e Event) {
	a.expire(e.TS)
	k := key{e.Client, e.Server}
	switch e.Kind {
	case "drops":
		a.marks = append(a.marks, mark{e.TS, e.Drops})
		a.release()
		return
	case "connect", "accept":
		c := a.open[k]
		base := e.Seq
		if c != nil && ((e.Kind == "connect" && c.clientBase != nil) || (e.Kind == "accept" && c.serverBase != nil)) {
			a.finish(c, "reused")
			c = nil
		}
		if c == nil {
			c = a.start(k, e.TS)
		}
		if e.Kind == "connect" {
			c.clientBase = &base
		} else {
			c.serverBase = &base
		}
		a.touch(c, e.TS)
	case "send", "recv":
		c := a.open[k]
		if c == nil || (e.Kind == "send" && c.clientBase == nil) || (e.Kind == "recv" && c.serverBase == nil) {
			a.orphanEvent(k, e)
			return
		}
		base := *c.clientBase
		if e.Kind == "recv" {
			base = *c.serverBase
		}
		c.messages = append(c.messages, Message{e.Kind, e.Seq - base, e.Seq, e.Len, e.TS})
		a.touch(c, e.TS)
	case "close":
		c := a.open[k]
		if c == nil {
			return
		}
		c.closes++
		a.touch(c, e.TS)
		ends := 0
		if c.clientBase != nil {
			ends++
		}
		if c.serverBase != nil {
			ends++
		}
		if c.closes >= ends {
			a.finish(c, "closed")
		}
	}
}

func (a *Assembler) start(k key, ts uint64) *conn {
	a.opened++
	c := &conn{key: k, first: ts, opened: a.opened}
	a.open[k] = c
	a.openOrder = append(a.openOrder, c)
	if a.cfg.MaxOpen > 0 && len(a.open) > a.cfg.MaxOpen {
		for len(a.openOrder) > 0 {
			oldest := a.openOrder[0]
			a.openOrder = a.openOrder[1:]
			if a.open[oldest.key] == oldest && oldest != c {
				a.finish(oldest, "evicted")
				break
			}
		}
	}
	return c
}

func (a *Assembler) touch(c *conn, ts uint64) {
	c.last = ts
	c.version++
	limit := a.cfg.Idle
	if c.orphan {
		limit = a.cfg.Retain
	}
	heap.Push(&a.deadlines, deadline{at: deadlineAt(c, limit), c: c, version: c.version})
}

func deadlineAt(c *conn, limit uint64) uint64 {
	if c.orphan {
		return c.first + limit // orphans go after Retain from their first event
	}
	return c.last + limit
}

func (a *Assembler) orphanEvent(k key, e Event) {
	c := a.orphans[k]
	if c == nil {
		c = &conn{key: k, first: e.TS, orphan: true}
		a.orphans[k] = c
	}
	c.messages = append(c.messages, Message{Kind: e.Kind, Seq: e.Seq, Len: e.Len, TS: e.TS})
	a.touch(c, e.TS)
}

// expire hands over what has been quiet too long, or kept apart too long, by now.
func (a *Assembler) expire(now uint64) {
	for a.deadlines.Len() > 0 && a.deadlines[0].at <= now {
		d := heap.Pop(&a.deadlines).(deadline)
		c := d.c
		if d.version != c.version {
			continue
		}
		if c.orphan && a.orphans[c.key] == c {
			a.finish(c, "orphan")
		} else if !c.orphan && a.open[c.key] == c {
			a.finish(c, "idle")
		}
	}
}

func (a *Assembler) finish(c *conn, end string) {
	if c.orphan {
		delete(a.orphans, c.key)
	} else {
		delete(a.open, c.key)
	}
	c.version++ // its pending deadlines are now stale
	a.waiting = append(a.waiting, Batch{
		Client: c.key.client, Server: c.key.server,
		ClientBase: c.clientBase, ServerBase: c.serverBase,
		End: end, First: c.first, Last: c.last, Messages: sortedMessages(c.messages),
	})
}

// release hands over every finished connection: events are handled in time
// order, so a drop reading comes after all of them.
func (a *Assembler) release() {
	for _, b := range a.waiting {
		b.Loss = a.loss(b.First, b.Last)
		a.emit(b)
	}
	a.waiting = nil
}

// loss compares the last drop reading at or before first with the first one
// at or after last.
func (a *Assembler) loss(first, last uint64) string {
	var before, after *uint64
	for i := range a.marks {
		m := a.marks[i]
		if m.ts <= first {
			before = &a.marks[i].drops
		}
		if m.ts >= last && after == nil {
			after = &a.marks[i].drops
		}
	}
	switch {
	case before == nil || after == nil:
		return "unknown"
	case *after == *before:
		return "none"
	}
	return "declared"
}
