package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestEveryWorkerRunsUntilStopped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	began := time.Now()
	rounds := burn(ctx, 3)
	if wall := time.Since(began); wall < 200*time.Millisecond || wall > 2*time.Second {
		t.Fatalf("burn returned after %v, want it to stop when the context ends", wall)
	}
	if len(rounds) != 3 {
		t.Fatalf("%d workers, want 3", len(rounds))
	}
	for w, n := range rounds {
		if n == 0 {
			t.Fatalf("worker %d never ran", w)
		}
	}
}

func TestInvalidWorkerCountsAreRefused(t *testing.T) {
	for _, arg := range []string{"-workers=0", "-workers=-1"} {
		out, err := exec.Command("go", "run", ".", arg).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "-workers must be >= 1") {
			t.Fatalf("%s: err=%v out=%s", arg, err, out)
		}
	}
}
