// join is a legacy ordinal-join diagnostic, NOT the protocol-level GT2 oracle:
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

type joinKey struct {
	connKey  string
	streamID int
}

type joinResult struct {
	Expected     int
	Joined       int
	Matched      int
	JoinMisses   int
	FakeUpstream int
	DAcc         float64
}

// join computes D_acc for the legacy ordinal diagnostic. (conn_key, stream_id)
// is only unique if both sides' independently-assigned generation counters
// happened to agree (see README "Lesson") -- not guaranteed, so a collision on
// either side is rejected rather than silently mis-joined: a collision in
// expected would silently drop whichever load-gen record lost the map
// overwrite; a collision in observed lets more than one fake-upstream record
// match the same expected entry, inflating `joined`/`matched` past
// `len(expected)` -- confirmed empirically to reach D_acc=2.0 and a negative
// join_misses on two observed records sharing one key.
func join(expected, observed []oracle.Record, shuffle bool) (joinResult, error) {
	for _, records := range [][]oracle.Record{expected, observed} {
		for _, r := range records {
			if r.SchemaVersion >= 2 && (r.Protocol != "HTTP/1.1" || r.Error != "") {
				return joinResult{}, fmt.Errorf(
					"legacy ordinal join requires successful HTTP/1.1 records; local ordinals are not an HTTP/2 oracle",
				)
			}
		}
	}

	expectedByKey := make(map[joinKey]string, len(expected))
	traceIDs := make([]string, 0, len(expected))
	for _, r := range expected {
		k := joinKey{r.ConnKey, r.StreamID}
		if _, dup := expectedByKey[k]; dup {
			return joinResult{}, fmt.Errorf(
				"duplicate (conn_key=%s, stream_id=%d) in load-gen's own log -- "+
					"the fallback-tier key is not unique even on its authoritative side",
				r.ConnKey, r.StreamID,
			)
		}
		expectedByKey[k] = r.TraceID
		traceIDs = append(traceIDs, r.TraceID)
	}

	if shuffle {
		rand.Shuffle(len(traceIDs), func(i, j int) { traceIDs[i], traceIDs[j] = traceIDs[j], traceIDs[i] })
		i := 0
		for k := range expectedByKey {
			expectedByKey[k] = traceIDs[i]
			i++
		}
	}

	seenObserved := make(map[joinKey]bool, len(observed))
	var matched, joined int
	for _, o := range observed {
		k := joinKey{o.ConnKey, o.StreamID}
		if seenObserved[k] {
			return joinResult{}, fmt.Errorf(
				"duplicate (conn_key=%s, stream_id=%d) in fake-upstream's own log -- "+
					"independently assigned generations collided (see README Lesson); "+
					"counting it again would inflate D_acc past 100%%",
				o.ConnKey, o.StreamID,
			)
		}
		seenObserved[k] = true
		want, ok := expectedByKey[k]
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
	return joinResult{
		Expected:     len(expected),
		Joined:       joined,
		Matched:      matched,
		JoinMisses:   len(expected) - joined,
		FakeUpstream: len(observed),
		DAcc:         dAcc,
	}, nil
}

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

	log.Print("legacy diagnostic only: local ordinals and independently assigned connection generations do not establish GT2")
	if *shuffle {
		log.Printf("negative control: expected trace_id will be permuted across %d keys", len(expected))
	}

	result, err := join(expected, observed, *shuffle)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"expected=%d joined=%d matched=%d join_misses=%d D_acc=%.4f (fake_upstream_records=%d shuffle=%v)\n",
		result.Expected, result.Joined, result.Matched, result.JoinMisses, result.DAcc, result.FakeUpstream, *shuffle,
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
