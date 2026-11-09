// Decoding the journal, apart from the capture (couverture-asynchrone-spec.md
// 4 quater): closed segments are decoded in parallel, by -workers goroutines,
// and written in segment order to the same JSONL the probe writes without a
// journal, so the analysis tools read it unchanged. No kernel, no privilege.
//
//	stream-probe -decode -journal DIR -port 8443 -out probe.jsonl
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var closedSegment = regexp.MustCompile(`^seg-(\d+)\.seg$`)

// closedSegments lists seg-N.seg in N order; seg-N.open is still being written.
func closedSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type seg struct {
		n    int
		path string
	}
	var segs []seg
	for _, e := range entries {
		if m := closedSegment.FindStringSubmatch(e.Name()); m != nil {
			n, _ := strconv.Atoi(m[1])
			segs = append(segs, seg{n, filepath.Join(dir, e.Name())})
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].n < segs[j].n })
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = s.path
	}
	return out, nil
}

// decodeJournal decodes every closed segment of dir into out, in order, and
// returns how many segments it read. A foreign or cut segment stops it.
func decodeJournal(dir string, port uint16, out string, workers int) (int, error) {
	paths, err := closedSegments(dir)
	if err != nil {
		return 0, err
	}
	type result struct {
		recs []Record
		err  error
	}
	results := make([]chan result, len(paths))
	for i := range results {
		results[i] = make(chan result, 1)
	}
	next := make(chan int)
	for range max(workers, 1) {
		go func() {
			for i := range next {
				recs, err := readSegment(paths[i], port)
				results[i] <- result{recs, err}
			}
		}()
	}
	go func() {
		for i := range paths {
			next <- i
		}
		close(next)
	}()

	f, err := os.Create(out)
	if err != nil {
		return 0, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(bw)
	for i := range paths {
		r := <-results[i] // in segment order, whatever order the workers finish in
		if r.err != nil {
			f.Close()
			return i, r.err
		}
		for _, rec := range r.recs {
			if err := enc.Encode(rec); err != nil {
				f.Close()
				return i, err
			}
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return len(paths), err
	}
	return len(paths), f.Close()
}

func runDecode(cfg config) error {
	if cfg.journal == "" {
		return fmt.Errorf("-decode needs -journal")
	}
	n, err := decodeJournal(cfg.journal, cfg.port, cfg.outPath, cfg.workers)
	if err != nil {
		return fmt.Errorf("decode journal: %w", err)
	}
	log.Printf("decoded %d segments of %s into %s", n, cfg.journal, cfg.outPath)
	return nil
}
