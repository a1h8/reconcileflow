// stream-probe: the TCP sequence number of each message's first byte on the
// watched server ports, for both ends of the connection
// (docs/target/correlation-phase4-spec.md, 4c (a)). The client writes and the
// server reads in the same sequence space, so the same byte has the same
// number at both ends, whatever their clocks. Never any payload.
//
// No per-request state in the kernel: each hook reads the socket and emits one
// event into the ring buffer, or counts a drop when it is full.
//
//   kprobe    tcp_connect(sk)              client opens: write_seq = initial number (SYN)
//   kretprobe inet_csk_accept -> sk        server accepts: copied_seq = first data byte
//   kprobe    tcp_sendmsg_locked(sk, msg, size)  client sends: write_seq = first byte of this send
//   kprobe    tcp_cleanup_rbuf(sk, copied) server has read: copied_seq - copied = first byte read
//   kprobe    tcp_close(sk, timeout)       either end closes
//
// The send is hooked on tcp_sendmsg_locked, not tcp_sendmsg: a socket an
// agent puts in a sockmap (sk_msg) sends through tcp_bpf_sendmsg, which skips
// tcp_sendmsg; both end in tcp_sendmsg_locked. On that path one write may
// leave in several calls: the later ones start inside the message.
//
// The client -> server direction is kept: sends whose remote port is watched,
// reads whose local port is. With the option set (key 0 of "options", phase 4
// spec 8.13), the server -> client direction too: sends whose local port is
// watched (responses), reads whose remote port is. Known limit: a read with MSG_PEEK
// does not move copied_seq, so its event is wrong; Go's net package never peeks.
// tcp_sock has no initial-number field on kernel 7.0 (BTF checked 2026-11-08),
// hence the connect and accept events, which give the base of the stream.
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_endian.h>

char LICENSE[] SEC("license") = "GPL";

enum kind {
	KIND_SEND = 1,
	KIND_RECV = 2,
	KIND_CONNECT = 3,
	KIND_ACCEPT = 4,
	KIND_CLOSE = 5,
};

struct stream_event {
	__u64 socket;       // raw struct sock * value
	__u8 saddr_v6[16];  // skc_v6_rcv_saddr, valid when family == AF_INET6
	__u8 daddr_v6[16];  // skc_v6_daddr
	__u32 saddr;        // skc_rcv_saddr, valid when family == AF_INET
	__u32 daddr;        // skc_daddr
	__u16 sport;        // local port, host order
	__u16 dport;        // remote port, host order
	__u32 seq;
	__u32 len;
	__u32 pid;
	__u16 family;
	__u8 kind;
	__u8 _pad0[5];      // keeps timestamp_ns 8-byte aligned; mirrored by hand in main.go
	__u64 timestamp_ns; // bpf_ktime_get_ns
};

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 24); // 16 MiB
} events SEC(".maps");

// bpf_ringbuf_reserve failures: events that never reached user space.
// Mappable: the loader maps it while privileged, then reads it as plain
// memory after giving up every privilege.
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(map_flags, BPF_F_MMAPABLE);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u64);
} drops SEC(".maps");

// The watched server ports, host order, written by the loader: a set.
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8);
	__type(key, __u16);
	__type(value, __u8);
} watched SEC(".maps");

// key 0: 1 keeps the server -> client direction too, written by the loader.
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u8);
} options SEC(".maps");

static __always_inline int is_watched(__u16 port)
{
	return bpf_map_lookup_elem(&watched, &port) != 0;
}

static __always_inline int both_directions(void)
{
	__u32 key = 0;
	__u8 *on = bpf_map_lookup_elem(&options, &key);
	return on && *on;
}

static __always_inline __u16 local_port(struct sock *sk)
{
	return BPF_CORE_READ(sk, __sk_common.skc_num);
}

static __always_inline __u16 remote_port(struct sock *sk)
{
	return bpf_ntohs(BPF_CORE_READ(sk, __sk_common.skc_dport));
}

static __always_inline void emit(struct sock *sk, enum kind kind, __u32 seq, __u32 len)
{
	struct stream_event *ev = bpf_ringbuf_reserve(&events, sizeof(*ev), 0);
	if (!ev) {
		__u32 key = 0;
		__u64 *cnt = bpf_map_lookup_elem(&drops, &key);
		if (cnt)
			__sync_fetch_and_add(cnt, 1);
		return; // full: drop and count, never block the kernel path
	}
	ev->socket = (__u64)(void *)sk;
	BPF_CORE_READ_INTO(&ev->saddr_v6, sk, __sk_common.skc_v6_rcv_saddr);
	BPF_CORE_READ_INTO(&ev->daddr_v6, sk, __sk_common.skc_v6_daddr);
	ev->saddr = BPF_CORE_READ(sk, __sk_common.skc_rcv_saddr);
	ev->daddr = BPF_CORE_READ(sk, __sk_common.skc_daddr);
	ev->sport = local_port(sk);
	ev->dport = remote_port(sk);
	ev->seq = seq;
	ev->len = len;
	ev->pid = bpf_get_current_pid_tgid() >> 32;
	ev->family = BPF_CORE_READ(sk, __sk_common.skc_family);
	ev->kind = kind;
	__builtin_memset(ev->_pad0, 0, sizeof(ev->_pad0));
	ev->timestamp_ns = bpf_ktime_get_ns();
	bpf_ringbuf_submit(ev, 0);
}

SEC("kprobe/tcp_connect")
int BPF_KPROBE(on_tcp_connect, struct sock *sk)
{
	if (!is_watched(remote_port(sk)))
		return 0;
	struct tcp_sock *tp = (struct tcp_sock *)sk;
	emit(sk, KIND_CONNECT, BPF_CORE_READ(tp, write_seq), 0);
	return 0;
}

SEC("kretprobe/inet_csk_accept")
int BPF_KRETPROBE(on_inet_csk_accept, struct sock *sk)
{
	if (!sk || !is_watched(local_port(sk)))
		return 0;
	struct tcp_sock *tp = (struct tcp_sock *)sk;
	emit(sk, KIND_ACCEPT, BPF_CORE_READ(tp, copied_seq), 0);
	return 0;
}

SEC("kprobe/tcp_sendmsg_locked")
int BPF_KPROBE(on_tcp_sendmsg_locked, struct sock *sk, struct msghdr *msg, size_t size)
{
	if (size == 0)
		return 0;
	if (!is_watched(remote_port(sk)) && !(both_directions() && is_watched(local_port(sk))))
		return 0;
	struct tcp_sock *tp = (struct tcp_sock *)sk;
	emit(sk, KIND_SEND, BPF_CORE_READ(tp, write_seq), (__u32)size);
	return 0;
}

SEC("kprobe/tcp_cleanup_rbuf")
int BPF_KPROBE(on_tcp_cleanup_rbuf, struct sock *sk, int copied)
{
	if (copied <= 0)
		return 0;
	if (!is_watched(local_port(sk)) && !(both_directions() && is_watched(remote_port(sk))))
		return 0;
	struct tcp_sock *tp = (struct tcp_sock *)sk;
	emit(sk, KIND_RECV, BPF_CORE_READ(tp, copied_seq) - (__u32)copied, (__u32)copied);
	return 0;
}

SEC("kprobe/tcp_close")
int BPF_KPROBE(on_tcp_close, struct sock *sk)
{
	if (!is_watched(local_port(sk)) && !is_watched(remote_port(sk)))
		return 0;
	emit(sk, KIND_CLOSE, 0, 0);
	return 0;
}
