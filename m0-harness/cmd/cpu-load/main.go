// cpu-load keeps a fixed number of cores busy until it is stopped: the known,
// reproducible foreign load of the timing path's loaded rounds
// (docs/target/correlation-phase3-spec.md §6.3). Its process name lets the
// CPU sampling tell this declared load from any other.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
)

func main() {
	workers := flag.Int("workers", 2, "cores kept busy")
	flag.Parse()
	if *workers < 1 {
		log.Fatal("-workers must be >= 1")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	burn(ctx, *workers)
}

// burn runs workers busy loops, each on its own OS thread, until ctx ends,
// and returns how many rounds each one ran.
func burn(ctx context.Context, workers int) []uint64 {
	if runtime.GOMAXPROCS(0) < workers+1 {
		runtime.GOMAXPROCS(workers + 1)
	}
	rounds := make([]uint64, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.LockOSThread()
			x := uint64(1)
			for {
				select {
				case <-ctx.Done():
					sink.Add(x)
					return
				default:
				}
				for range 100_000 {
					x = x*6364136223846793005 + 1442695040888963407
				}
				rounds[w]++
			}
		}()
	}
	wg.Wait()
	return rounds
}

// sink keeps the busy loop's result alive, so the compiler cannot drop the loop.
var sink atomic.Uint64
