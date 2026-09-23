// strong-tier-probe observes both ends of a TCP connection from a single
// privileged vantage point — this is what makes the "strong" tier strong: one
// observer sees both sides at once, so there is no cross-process ordering
// race like the one measured in m0-harness/README.md at n=10000/c=200
// (fallback tier, 369 join misses).
//
// docs/target/ground-truth-eval-plane-v3.md, oracle.connection_instance.strong:
// [boot_id, network_namespace, socket_cookie]. boot_id is not a kernel concept —
// it is read once in userspace (/proc/sys/kernel/random/boot_id) and attached
// to every event by the Go loader, not duplicated here. socket_cookie here is
// the raw `struct sock *` pointer value, not the SO_COOKIE-style helper — see
// "Iteration 2" below for why.
//
// Two attach points, one ring buffer:
//   kprobe  tcp_connect(struct sock *sk)          -> client-side connect
//   kretprobe inet_csk_accept                     -> server-side accept (socket
//                                                     is the return value, not
//                                                     an argument, hence kretprobe)
//
// STATUS — iteration 2 (2026-09-23). Iteration 1 compiled and had correct ELF
// structure but was never loaded (no CAP_BPF in the sandbox). The user then
// ran it for real, with sudo, on the target kernel (7.0.0-31-generic) and hit:
//
//   program on_tcp_connect: load program: invalid argument: program of this
//   type cannot use helper bpf_get_socket_cookie#46
//
// Cause: bpf_get_socket_cookie(struct sock *) is only valid for
// BPF_PROG_TYPE_SOCK_OPS / BPF_PROG_TYPE_CGROUP_SOCK_ADDR / sk_buff-based
// filter types — not for a plain BPF_PROG_TYPE_KPROBE, regardless of the
// argument type it's handed. Fixed by using the socket pointer's raw value
// instead (legal in any program type: it's a function argument, not a
// restricted helper call). bpf_get_netns_cookie(sk) was pre-emptively
// replaced the same way, before it could fail on the identical restriction —
// switched to a CO-RE read of the netns inode number instead. This CO-RE read
// is itself still unverified against the real kernel's BTF layout; expect it
// may need another iteration.
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_endian.h>

char LICENSE[] SEC("license") = "GPL";

enum side {
	SIDE_CONNECT = 1,
	SIDE_ACCEPT = 2,
};

struct conn_event {
	__u64 socket_cookie; // raw `struct sock *` value — stable per-socket identity for
	                     // this kernel's lifetime, not a SO_COOKIE-style monotonic counter
	__u64 netns_cookie;  // net->ns.inum, read via CO-RE, not the bpf_get_netns_cookie() helper
	__u8 saddr_v6[16];   // skc_v6_rcv_saddr — valid when family == AF_INET6
	__u8 daddr_v6[16];   // skc_v6_daddr — valid when family == AF_INET6
	__u32 saddr;         // skc_rcv_saddr — valid when family == AF_INET
	__u32 daddr;         // skc_daddr — valid when family == AF_INET
	__u16 sport;
	__u16 dport;
	__u32 pid;
	__u16 family;        // skc_family: picks which of the two addr representations above is valid
	__u8 side;
	__u8 _pad0[5];       // explicit pad to keep timestamp_ns 8-byte aligned; mirrored by hand in main.go
	__u64 timestamp_ns;
};

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 20); // 1 MiB
} events SEC(".maps");

// Counts bpf_ringbuf_reserve() failures (buffer full) — the "100% pairing"
// numbers in README.md only cover events that made it into the ring buffer;
// this map is what lets main.go report how many never did.
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u64);
} drops SEC(".maps");

static __always_inline void emit(struct sock *sk, enum side side)
{
	struct conn_event *ev = bpf_ringbuf_reserve(&events, sizeof(*ev), 0);
	if (!ev) {
		__u32 key = 0;
		__u64 *cnt = bpf_map_lookup_elem(&drops, &key);
		if (cnt)
			__sync_fetch_and_add(cnt, 1);
		return; // buffer full: drop, never block the kernel path
	}

	ev->socket_cookie = (__u64)(void *)sk;
	ev->netns_cookie = BPF_CORE_READ(sk, __sk_common.skc_net.net, ns.inum);
	BPF_CORE_READ_INTO(&ev->saddr_v6, sk, __sk_common.skc_v6_rcv_saddr);
	BPF_CORE_READ_INTO(&ev->daddr_v6, sk, __sk_common.skc_v6_daddr);
	ev->saddr = BPF_CORE_READ(sk, __sk_common.skc_rcv_saddr);
	ev->daddr = BPF_CORE_READ(sk, __sk_common.skc_daddr);
	ev->sport = BPF_CORE_READ(sk, __sk_common.skc_num);
	ev->dport = bpf_ntohs(BPF_CORE_READ(sk, __sk_common.skc_dport));
	ev->pid = bpf_get_current_pid_tgid() >> 32;
	ev->family = BPF_CORE_READ(sk, __sk_common.skc_family);
	ev->side = side;
	ev->timestamp_ns = bpf_ktime_get_ns();

	bpf_ringbuf_submit(ev, 0);
}

// tcp_connect(struct sock *sk) — stable, widely used tracing point (same one
// bcc's tcpconnect.py and Cilium's connect tracing use). Fires on the client
// side, before the handshake completes.
SEC("kprobe/tcp_connect")
int BPF_KPROBE(on_tcp_connect, struct sock *sk)
{
	emit(sk, SIDE_CONNECT);
	return 0;
}

// inet_csk_accept returns struct sock* — the accepted socket is the return
// value, not an argument, so this must be a kretprobe, not a kprobe.
SEC("kretprobe/inet_csk_accept")
int BPF_KRETPROBE(on_inet_csk_accept, struct sock *sk)
{
	if (!sk)
		return 0; // accept() can return NULL/error; nothing to record
	emit(sk, SIDE_ACCEPT);
	return 0;
}
