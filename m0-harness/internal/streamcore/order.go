package streamcore

import "sort"

// pendingEvent orders held events by time, then by arrival.
type pendingEvent struct {
	e       Event
	arrival uint64
}

type eventHeap []pendingEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].e.TS != h[j].e.TS {
		return h[i].e.TS < h[j].e.TS
	}
	return h[i].arrival < h[j].arrival
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(pendingEvent)) }
func (h *eventHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

type deadline struct {
	at      uint64
	c       *conn
	version uint64
}

type deadlineHeap []deadline

func (h deadlineHeap) Len() int { return len(h) }
func (h deadlineHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].c.first < h[j].c.first
}
func (h deadlineHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *deadlineHeap) Push(x any)   { *h = append(*h, x.(deadline)) }
func (h *deadlineHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// sortedMessages: sends then reads would lose the exchange; by offset, then
// sends before reads at one offset, then time.
func sortedMessages(m []Message) []Message {
	out := append([]Message(nil), m...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Offset != out[j].Offset {
			return out[i].Offset < out[j].Offset
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind == "send"
		}
		return out[i].TS < out[j].TS
	})
	return out
}

func sortedOrphans(m map[key]*conn) []*conn {
	out := make([]*conn, 0, len(m))
	for _, c := range m {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].first != out[j].first {
			return out[i].first < out[j].first
		}
		if out[i].key.client != out[j].key.client {
			return out[i].key.client < out[j].key.client
		}
		return out[i].key.server < out[j].key.server
	})
	return out
}
