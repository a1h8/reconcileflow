package main

import (
	"strings"
	"testing"
)

// A read that reaches the peer's close (FIN) has moved copied_seq one past its
// last byte before tcp_cleanup_rbuf runs (net/ipv4/tcp.c, tcp_recvmsg_locked:
// found_fin_ok increments *seq, then tcp_cleanup_rbuf(sk, copied)), so
// copied_seq - copied lands one byte late. Seen on every last read after a
// close at c800 (phase 4 spec 8.18.1). The FIN was received in order when the
// socket is in CLOSE_WAIT, CLOSING or LAST_ACK (tcp_fin), and that read took
// it when nothing received is left: copied_seq == rcv_nxt, which counts the
// FIN. Then the read starts one byte earlier.
func TestAReadThatReachesTheCloseStartsOneByteEarlier(t *testing.T) {
	src := probeSource(t)
	if !strings.Contains(src, "fin_taken") {
		t.Fatal("no fin_taken helper: the one-byte correction must be named")
	}
	helper := src[strings.Index(src, "fin_taken"):]
	helper = helper[:strings.Index(helper, "\n}")]
	for _, want := range []string{
		"copied_seq",
		"rcv_nxt",
		"skc_state",
		"TCP_CLOSE_WAIT",
		"TCP_CLOSING",
		"TCP_LAST_ACK",
		"==",
	} {
		if !strings.Contains(helper, want) {
			t.Errorf("fin_taken does not use %s", want)
		}
	}
	start := strings.Index(src, "BPF_KPROBE(on_tcp_cleanup_rbuf")
	if start < 0 {
		t.Fatal("no on_tcp_cleanup_rbuf")
	}
	body := src[start:]
	body = body[:strings.Index(body, "\n}")]
	if !strings.Contains(body, "- fin_taken(") {
		t.Error("the read's first byte must be moved back by fin_taken")
	}
}
