// join is the offline oracle join (docs/target/ground-truth-eval-plane-v3.md, GT2):
// it reads load-gen's authoritative trace_id-per-(conn_key,stream_id) and
// fake-upstream's observed trace_id-per-(conn_key,stream_id), joins on the key,
// and reports D_acc — the fraction where they agree.
//
// This is deliberately a separate process from both harness binaries: the
// measurement must not be able to bias itself by running inside the thing it
// measures (same principle as §6 "the monitor never monitors itself" in
// docs/target/platform-architecture-v4.md).
//
// -shuffle applies the negative-control witness (docs/target/m0-evaluation-run-001.md
// §0.1): randomly permute load-gen's expected trace_ids before joining. D_acc must
// collapse to chance level, or the measurement is circular.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"

	"reconcileflow/m0-harness/internal/oracle"
)

func main() {
	loadGenPath := flag.String("load-gen", "load-gen.jsonl", "load-gen JSONL path")
	fakeUpstreamPath := flag.String("fake-upstream", "fake-upstream.jsonl", "fake-upstream JSONL path")
	shuffle := flag.Bool("shuffle", false, "negative control: permute expected trace_id before joining")
	flag.Parse()

	expected, err := readRecords(*loadGenPath)
	if err != nil {
		log.Fatalf("read load-gen log: %v", err)
	}
	observed, err := readRecords(*fakeUpstreamPath)
	if err != nil {
		log.Fatalf("read fake-upstream log: %v", err)
	}

	type key struct {
		connKey  string
		streamID int
	}
	expectedByKey := make(map[key]string, len(expected))
	traceIDs := make([]string, 0, len(expected))
	for _, r := range expected {
		expectedByKey[key{r.ConnKey, r.StreamID}] = r.TraceID
		traceIDs = append(traceIDs, r.TraceID)
	}

	if *shuffle {
		rand.Shuffle(len(traceIDs), func(i, j int) { traceIDs[i], traceIDs[j] = traceIDs[j], traceIDs[i] })
		i := 0
		for k := range expectedByKey {
			expectedByKey[k] = traceIDs[i]
			i++
		}
		log.Printf("negative control: expected trace_id permuted across %d keys", len(traceIDs))
	}

	var matched, joined int
	for _, o := range observed {
		want, ok := expectedByKey[key{o.ConnKey, o.StreamID}]
		if !ok {
			continue // no matching load-gen record for this (conn_key, stream_id) — a join miss
		}
		joined++
		if want == o.TraceID {
			matched++
		}
	}

	// D_acc's denominator is every request load-gen actually sent, not just the
	// ones that happened to join — a join miss is exactly the fallback-tier
	// oracle failing to track a request, i.e. the failure this metric exists to
	// catch. Reporting matched/joined instead of matched/expected would quietly
	// exclude the very failures the witness cells are supposed to surface.
	dAcc := 0.0
	if len(expected) > 0 {
		dAcc = float64(matched) / float64(len(expected))
	}
	fmt.Printf(
		"expected=%d joined=%d matched=%d join_misses=%d D_acc=%.4f (fake_upstream_records=%d shuffle=%v)\n",
		len(expected), joined, matched, len(expected)-joined, dAcc, len(observed), *shuffle,
	)
}

func readRecords(path string) ([]oracle.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []oracle.Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var r oracle.Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("parse line: %w", err)
		}
		out = append(out, r)
	}
	return out, sc.Err()
}
