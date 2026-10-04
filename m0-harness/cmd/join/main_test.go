package main

import (
	"testing"

	"reconcileflow/m0-harness/internal/oracle"
)

func rec(connKey string, streamID int, traceID string) oracle.Record {
	return oracle.Record{ConnKey: connKey, StreamID: streamID, TraceID: traceID}
}

func TestJoinComputesDAccOnTheHappyPath(t *testing.T) {
	expected := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 1, "B"),
	}
	observed := []oracle.Record{
		rec("5t|0", 0, "A"), // match
		rec("5t|0", 1, "X"), // joined, wrong trace_id
	}
	result, err := join(expected, observed, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Expected != 2 || result.Joined != 2 || result.Matched != 1 || result.JoinMisses != 0 {
		t.Fatalf("got %+v", result)
	}
	if result.DAcc != 0.5 {
		t.Fatalf("D_acc = %v, want 0.5", result.DAcc)
	}
}

func TestJoinCountsAMissingObservedRecordAsAJoinMiss(t *testing.T) {
	expected := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 1, "B"), // fake-upstream never observed this one
	}
	observed := []oracle.Record{rec("5t|0", 0, "A")}
	result, err := join(expected, observed, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Joined != 1 || result.JoinMisses != 1 {
		t.Fatalf("got %+v", result)
	}
}

func TestJoinRejectsDuplicateKeyInExpected(t *testing.T) {
	expected := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 0, "B"), // same (conn_key, stream_id) as above
	}
	if _, err := join(expected, nil, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}

func TestJoinRejectsDuplicateKeyInObserved(t *testing.T) {
	// Exact empirical repro (2026-10-04): a single expected record, two
	// observed records sharing its key, both equal to its trace_id. Before
	// this check existed, this produced joined=2, matched=2 against
	// expected=1 -- D_acc=2.0 and join_misses=-1, both mathematically
	// impossible for a fraction/count that is supposed to be bounded.
	expected := []oracle.Record{rec("5t|0", 0, "A")}
	observed := []oracle.Record{
		rec("5t|0", 0, "A"),
		rec("5t|0", 0, "A"),
	}
	if _, err := join(expected, observed, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}

func TestJoinRejectsMixedSchemaVersions(t *testing.T) {
	expected := []oracle.Record{{SchemaVersion: 2, Protocol: "HTTP/2", ConnKey: "5t|0", StreamID: 0, TraceID: "A"}}
	if _, err := join(expected, nil, false); err == nil {
		t.Fatal("want an error, got nil")
	}
}
