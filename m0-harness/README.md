# M0 harness

First runnable slice of the Correlation Evaluation Harness
(`docs/target/ground-truth-eval-plane-v3.md`, `docs/target/m0-evaluation-run-001.md` §0.1).
Implements approach (B): build against the declared real `target_stack`
(Go / generic HTTP-2 / `net/http` / header propagation), validated with two
witness cells before trusting any measurement.

## What's here

- `cmd/fake-upstream` — HTTP/2 server (self-signed TLS, lab only) injecting
  the protocol's latency profile C, logging the fallback-tier connection
  identity it observes per request.
- `cmd/load-gen` — HTTP/2 client, concurrency-controlled, originates a
  `traceparent` per request (so it's the authoritative half of the oracle by
  construction), logging the same connection identity from its side.
- `cmd/join` — offline join of the two logs on `(conn_key, stream_id)`,
  reporting `D_acc`. Deliberately a separate process: the measurement must
  not run inside either side it measures.
- `internal/oracle` — shared record format and the **single** `ConnKey`
  function. Both binaries must call it, not reformat the key by hand — see
  "Lesson" below for why that matters.

## Building

`bin/`, generated/vendored BPF inputs, and `*.jsonl` run output are gitignored —
build artifacts and third-party headers don't belong in history. To reproduce:

```
# Go binaries
go build -o bin/fake-upstream ./cmd/fake-upstream
go build -o bin/load-gen ./cmd/load-gen
go build -o bin/join ./cmd/join
go build -o bin/strong-tier-probe ./cmd/strong-tier-probe

# BPF object (needs clang with a BPF backend — gcc has none; see "Getting
# clang without root" below if your environment lacks one)
bpftool btf dump file /sys/kernel/btf/vmlinux format c > cmd/strong-tier-probe/bpf/vmlinux.h
mkdir -p cmd/strong-tier-probe/bpf/vendor/bpf
BASE=https://raw.githubusercontent.com/libbpf/libbpf/v1.4.0/src
for f in bpf_helpers.h bpf_helper_defs.h bpf_tracing.h bpf_core_read.h bpf_endian.h; do
  curl -sSL -o "cmd/strong-tier-probe/bpf/vendor/bpf/$f" "$BASE/$f"
done
clang -target bpf -D__TARGET_ARCH_x86 \
  -Icmd/strong-tier-probe/bpf -Icmd/strong-tier-probe/bpf/vendor \
  -g -O2 -c cmd/strong-tier-probe/bpf/probe.c -o cmd/strong-tier-probe/bpf/probe.o
```

## Running one witness cell

Each experimental condition needs its **own** `fake-upstream` instance and
its own log file. Reusing one long-running server across multiple `load-gen`
invocations lets the OS's ephemeral-port reuse between runs corrupt the join
(see Lesson). One cell:

```
./bin/fake-upstream -addr :18443 -out /tmp/fu.jsonl &
./bin/load-gen -target https://localhost:18443 -out /tmp/lg.jsonl -concurrency 100 -requests 500
./bin/join -load-gen /tmp/lg.jsonl -fake-upstream /tmp/fu.jsonl
kill %1   # tear down before the next cell
```

Witness cells (§0.1):

- **Positive** — `load-gen -pool=false -control positive`: one TCP connection
  per request, no multiplexing. `D_acc` must land near 1.0, or the oracle
  itself is broken, not the system under test.
- **Negative** — `join -shuffle`: permutes the expected `trace_id` before
  joining. `D_acc` must collapse to ~0, or the measurement is circular.

## First results (2026-09-23)

`D_acc` = matched / **expected** (every request `load-gen` sent) — a join miss
counts as a failure, it is not excluded from the denominator. Excluding misses
was the first version of `join`; fixed after the scale run below showed why
that quietly hides exactly the failures this metric exists to catch.

| Cell | Scale | D_acc | Join misses | Meaning |
|---|---|---|---|---|
| Normal (pooling on) | n=500, c=100 | 1.0000 | 0 | fallback-tier oracle holds at this scale |
| Positive (no pooling, isolated) | n=500, c=100 | 1.0000 | 0 | baseline confirmed clean |
| Negative (shuffled) | n=500, c=100 | 0.0000 | 0 | not circular |
| **Normal, sustained load** | **n=10000, c=200** | **0.9631** | **369** | **fallback-tier oracle degrades under sustained port churn** |

**The scale result is the real finding, not the small-n ones.** All 369 misses
have the identical signature: `load-gen` assigns `generation=0` to a 5-tuple,
`fake-upstream` assigns `generation=1` to the *same* 5-tuple, in the *same*
run. Both sides correctly count "how many times have I seen this 5-tuple," but
under sustained concurrency the OS recycles ephemeral ports fast enough that
the two independent processes can observe the recycling in a different
relative order — there is no atomicity between them. This is not the
cross-run contamination bug below (that one was single-run-isolation hygiene);
this is the fallback tier failing on its own terms, at moderate scale, inside
one clean run.

This is the fallback-tier oracle only — `docs/target/ground-truth-eval-plane-v3.md`'s
strong tier (`boot_id`, `network_namespace`, `socket_cookie`) is kernel-assigned
and does not suffer this two-process ordering skew; it needs an eBPF probe, not
built here. Read the 96.3% above as concrete evidence *for* building that strong
tier before trusting the fallback one past a few hundred concurrent connections
— not as a harness bug to silence.

## Lesson: a real bug the harness caught on itself

The first attempt at the positive-control cell reused the *same* long-running
`fake-upstream` process (and its cumulative log) from the normal-run cell.
`D_acc` came out at 0.9760, not 1.0 — 13 mismatches. Root cause: `fake-upstream`
had been running continuously and correctly noticed 13 ephemeral ports get
reused by the OS across the two `load-gen` invocations (assigning `generation=1`
to those), while the second `load-gen` was a fresh process with no memory of the
first run's ports (it always starts counting generations at 0). Same 5-tuple,
disagreeing generation number, broken join key — 13 lost joins.

Not a TCP-level or OBI-level ambiguity: a test-isolation mistake (long-lived
server, short-lived client, shared port range). Fixed by giving every cell its
own `fake-upstream` instance. Left in this README instead of silently
corrected, because it is itself a small illustration of exactly the failure
mode (state that outlives what a "run" is supposed to mean) the wider M0
protocol exists to catch before it reaches a real measurement.

## Strong-tier probe (`cmd/strong-tier-probe`) — iteration 2, still not loaded clean

An eBPF probe for the strong-tier identity (`docs/target/ground-truth-eval-plane-v3.md`,
`boot_id + network_namespace + socket_cookie`). Design: a single privileged observer
(kprobe on `tcp_connect`, kretprobe on `inet_csk_accept`) sees both ends of a connection
from one vantage point — this is what the fallback tier structurally cannot do, since it
relies on two independent unprivileged processes each guessing (see the 369 join misses
above).

**Verified without loading it (both iterations):**
- `bpf/probe.c` compiles clean under `clang -target bpf` (portable clang 17.0.6, no
  system install — see below) against a `vmlinux.h` generated locally via
  `bpftool btf dump file /sys/kernel/btf/vmlinux format c`, and libbpf's
  `bpf_helpers.h`/`bpf_core_read.h`/`bpf_tracing.h`/`bpf_endian.h` vendored from
  `github.com/libbpf/libbpf` v1.4.0.
- `readelf -S bpf/probe.o` shows well-formed `kprobe/tcp_connect`, `kretprobe/inet_csk_accept`,
  `.maps`, `license`, `.BTF`, `.BTF.ext` sections.
- `cmd/strong-tier-probe/main.go` (the Go loader, `cilium/ebpf`) builds and `go vet`s clean.

**Iteration 1 confirmed the privilege wall, then the user got past it.** `bpftool prog load`
failed with `EPERM` here (no `CAP_BPF`, `sudo` needs interactive auth this tool can't
provide) — expected. The user then ran the compiled binary with `sudo` on the real target
kernel (`7.0.0-31-generic`) and hit the first real verifier error:

```
program on_tcp_connect: load program: invalid argument: program of this
type cannot use helper bpf_get_socket_cookie#46
```

`bpf_get_socket_cookie(struct sock *)` is only valid for `BPF_PROG_TYPE_SOCK_OPS` /
`BPF_PROG_TYPE_CGROUP_SOCK_ADDR` / `sk_buff`-based filter types — not a plain kprobe,
no matter what argument type it's handed. **Fix (iteration 2):** `socket_cookie` is now
the raw `struct sock *` pointer value (legal in any program type — it's a function
argument, not a helper call), and `netns_cookie` was pre-emptively switched from
`bpf_get_netns_cookie()` (same restriction class, very likely to fail the same way) to a
CO-RE read of `net->ns.inum`. Both changes recompile clean and keep the same ELF section
structure — that's all that's been checked so far.

### Iteration 3 (2026-09-23) — it loaded, and the core hypothesis holds

The user ran the iteration-2 fix for real, with `sudo`, on the target kernel. It attached
clean:

```
strong-tier-probe attached (tcp_connect + inet_csk_accept), boot_id=6b205a5d-..., writing to /tmp/strong-tier.jsonl
```

Two real measurements against live traffic, same scales as the fallback-tier results above:

| Scale | Connect events | Paired to an accept | Method |
|---|---|---|---|
| n=1000, c=100 | 354 | 354/354 (100%) | naive 1:1 port match |
| **n=10000, c=200** | **3927** | **3927/3927 (100%)**, incl. 328 same-port collisions | **per-port, time-ordered pairing** |

At n=10000/c=200 — the exact scale where the fallback tier degraded to 96.3% (369 join
misses, §"real bug" above) — a naive port-only match already leaves 0 unpaired connects,
but 328 client ports were reused within the run and matched more than one accept event
ambiguously. Those 328 are resolved by sorting each port's connect/accept events by the
**single observer's own timestamp** and pairing them in order (a socket must be accepted
before its port can be reused for a new connection) — 3927/3927 correct, zero ambiguity
left. This is the core hypothesis confirmed empirically: a single privileged observer with
one clock does not suffer the cross-process ordering race that broke the fallback tier at
this same scale.

**What this does not yet show:**
- This measures connect/accept pairing at the kernel level only. It has not yet been joined
  against `load-gen`'s actual `trace_id` records to confirm full end-to-end attribution — that
  three-way join (strong-tier events + load-gen + fake-upstream) is the next step, not done here.
- The pairing method used (per-port temporal ordering) is a minimal proof of concept, not
  the full `boot_id + netns + socket_cookie` composite key from the protocol design — good
  enough to validate the single-observer hypothesis, not yet the finished strong-tier oracle.
- `daddr=0.0.0.0` on every connect-side event (confirmed on all 3927 + 354, not a fluke) is
  still unexplained and unfixed — likely `tcp_connect()`'s kprobe firing before
  `skc_daddr` is populated for this call path. Doesn't block port-based pairing, but the
  field is broken and any future consumer keying on destination address at connect time
  needs to know that.

### Getting `clang` without root

The sandbox had no `clang` (needed for the BPF C target) and no package-manager root
access. Fixed without privilege: downloaded the official portable LLVM+clang 17.0.6
release tarball from GitHub and extracted `bin/clang*` + `lib/*` locally — no install
step, works from any directory. `gcc` cannot substitute here: BPF code generation is
LLVM-only, GCC has no BPF backend.

## Known limitations of this first slice

- Fallback-tier identity only (no eBPF strong tier).
- Latency profile C is an approximate lognormal fit (p50/p95 match, p99 ≈
  690ms vs. the 850ms target) — not a certified generator, see
  `cmd/fake-upstream/main.go`.
- `seed_policy=fixed` (protocol §1) is accepted as a flag but the harness does
  not itself seed a deterministic PRNG for request timing/order — the caller
  is responsible for that today.
- No k8s/eBPF deployment yet — this runs two local processes over `localhost`.
