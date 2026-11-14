// burst drives TCP messages through one port at rising rates, to find where
// stream-probe starts losing events (a saturation test, phase 4, 4c). No
// HTTP, no TLS: a server reads fixed-size messages, a client sends them on
// -conns connections, one write per message, paced by elapsed time. Each
// step is logged with its bounds on CLOCK_MONOTONIC, the probe's clock, so
// the probe's events and declared losses can be counted per step.
//
//	burst -port 18500 -steps 10000,50000,100000 -step 5s -out burst.jsonl
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

type config struct {
	port    int
	conns   int
	size    int
	steps   []int // messages per second, all connections together
	stepFor time.Duration
	out     string
}

// stepRecord is one JSONL line: a step, or the total once everything is read.
type stepRecord struct {
	Kind       string `json:"kind"` // "step" | "total"
	TargetPerS int    `json:"target_per_s,omitempty"`
	StartNS    int64  `json:"start_ns,omitempty"` // CLOCK_MONOTONIC
	EndNS      int64  `json:"end_ns,omitempty"`
	Sent       int64  `json:"sent"`
	Received   int64  `json:"received,omitempty"`
}

func parseSteps(s string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("steps must be positive integers, got %q", s)
		}
		out = append(out, n)
	}
	return out, nil
}

// due is how many messages should have left after elapsed at rate per second.
func due(rate float64, elapsed time.Duration) int64 {
	return int64(rate * elapsed.Seconds())
}

func monotonicNS() int64 {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts)
	return ts.Nano()
}

func run(cfg config) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.port))
	if err != nil {
		return err
	}
	defer ln.Close()
	var received atomic.Int64
	var readers sync.WaitGroup
	readers.Add(cfg.conns) // known in advance: never Add while Wait may run
	go func() {
		for range cfg.conns {
			c, err := ln.Accept()
			if err != nil {
				readers.Done()
				continue
			}
			go func() {
				defer readers.Done()
				defer c.Close()
				buf := make([]byte, cfg.size)
				for {
					if _, err := io.ReadFull(c, buf); err != nil {
						return
					}
					received.Add(1)
				}
			}()
		}
	}()

	conns := make([]net.Conn, cfg.conns)
	for i := range conns {
		if conns[i], err = net.Dial("tcp", ln.Addr().String()); err != nil {
			return err
		}
	}
	f, err := os.Create(cfg.out)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	msg := make([]byte, cfg.size)

	var total int64
	for _, rate := range cfg.steps {
		perConn := float64(rate) / float64(cfg.conns)
		var sent atomic.Int64
		var wg sync.WaitGroup
		start := time.Now()
		startNS := monotonicNS()
		var failed atomic.Value
		for _, c := range conns {
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				var mine int64
				for {
					elapsed := time.Since(start)
					if elapsed >= cfg.stepFor {
						return
					}
					for target := due(perConn, elapsed); mine < target; mine++ {
						if _, err := c.Write(msg); err != nil {
							failed.Store(err)
							return
						}
						sent.Add(1)
					}
					time.Sleep(200 * time.Microsecond)
				}
			}(c)
		}
		wg.Wait()
		if err, _ := failed.Load().(error); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		rec := stepRecord{Kind: "step", TargetPerS: rate, StartNS: startNS, EndNS: monotonicNS(), Sent: sent.Load()}
		total += rec.Sent
		if err := enc.Encode(rec); err != nil {
			return err
		}
		log.Printf("step %d/s: sent %d in %v", rate, rec.Sent, cfg.stepFor)
	}
	for _, c := range conns {
		c.Close()
	}
	readers.Wait()
	return enc.Encode(stepRecord{Kind: "total", Sent: total, Received: received.Load()})
}

func main() {
	port := flag.Int("port", 0, "port the server listens on (127.0.0.1), the one the probe watches")
	conns := flag.Int("conns", 16, "client connections")
	size := flag.Int("size", 400, "message size in bytes (one write each)")
	steps := flag.String("steps", "10000,20000,50000,100000,200000,400000,800000", "messages per second, one step each")
	stepFor := flag.Duration("step", 5*time.Second, "duration of each step")
	out := flag.String("out", "burst.jsonl", "JSONL output: one record per step, then the total")
	flag.Parse()
	parsed, err := parseSteps(*steps)
	if err == nil && (*port < 1 || *port > 65535 || *conns < 1 || *size < 1) {
		err = errors.New("-port in 1..65535, -conns and -size positive")
	}
	if err != nil {
		log.Fatal(err)
	}
	if err := run(config{port: *port, conns: *conns, size: *size, steps: parsed, stepFor: *stepFor, out: *out}); err != nil {
		log.Fatal(err)
	}
}
