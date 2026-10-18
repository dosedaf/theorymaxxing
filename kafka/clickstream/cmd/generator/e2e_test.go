package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"clickstream/internal/codec"
	"clickstream/internal/ledger"
	"clickstream/internal/testenv"
)

// TestGeneratorEndToEnd runs the faucet against an in-memory broker and
// registry, then reconciles: every ledger row must exist in events.raw at its
// recorded partition/offset, and Kafka must contain nothing else.
func TestGeneratorEndToEnd(t *testing.T) {
	env := testenv.New(t)
	gt := filepath.Join(t.TempDir(), "gt.jsonl")

	cfg := config{
		brokers: env.Brokers, srURL: env.SRURL, topic: "events.raw",
		partitions: 4, rf: 1, retention: time.Hour,
		rate: 400, duration: 2 * time.Second,
		users: 50, products: 10, seed: 7, keyBy: "user", schemaVer: 2,
		dupRate: 0.06, malformedRte: 0.06, lateRate: 0.06,
		lateMax: 200 * time.Millisecond, oooMax: 100 * time.Millisecond,
		ringSize: 128, gtFile: gt,
		linger: 5 * time.Millisecond, compression: "snappy", maxBuffered: 10000,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}

	rows := readLedger(t, gt)
	if len(rows) < 100 {
		t.Fatalf("expected a few hundred ledger rows at 400 eps / 2s, got %d", len(rows))
	}
	var dups, malformed, late, semantic, wireCorrupt int
	for _, r := range rows {
		if r.IsDuplicate {
			dups++
		}
		if r.IsMalformed {
			malformed++
			if isWireCorrupt(r.MalformedType) {
				wireCorrupt++
			} else {
				semantic++
			}
		}
		if r.IsLate {
			late++
		}
	}
	if dups == 0 || malformed == 0 || late == 0 || semantic == 0 || wireCorrupt == 0 {
		t.Fatalf("noise incomplete: dups=%d malformed=%d (semantic=%d wire=%d) late=%d",
			dups, malformed, semantic, wireCorrupt, late)
	}

	// Consume everything back from the fake broker.
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(env.Brokers...),
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kcl.Close()

	type rec struct {
		ts  int64
		val []byte
	}
	got := make(map[string]rec) // "partition:offset" -> record
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < len(rows) && time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		fetches := kcl.PollFetches(pollCtx)
		cancel()
		fetches.EachError(func(topic string, p int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return
			}
			t.Fatalf("fetch error %s/%d: %v", topic, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			got[fmt.Sprintf("%d:%d", r.Partition, r.Offset)] = rec{ts: r.Timestamp.UnixMilli(), val: r.Value}
		})
	}
	if len(got) != len(rows) {
		t.Fatalf("kafka has %d records, ledger has %d rows", len(got), len(rows))
	}

	// 1:1 reconciliation, including partition/offset and timestamps.
	srcl, err := sr.NewClient(sr.URLs(env.SRURL))
	if err != nil {
		t.Fatal(err)
	}
	dec := codec.NewDecoder(srcl)
	decodedClean := 0
	for _, r := range rows {
		key := fmt.Sprintf("%d:%d", r.Partition, r.Offset)
		k, ok := got[key]
		if !ok {
			t.Fatalf("ledger row %s (event %s) missing from kafka at %s", r.EventID, r.EventID, key)
		}
		if !isWireCorrupt(r.MalformedType) && (len(k.val) < 5 || k.val[0] != 0) {
			t.Fatalf("record at %s has no wire header: %d bytes (malformed_type=%q)", key, len(k.val), r.MalformedType)
		}
		if !r.IsMalformed && k.ts != r.EventTS {
			t.Fatalf("record at %s ts=%d, ledger says %d", key, k.ts, r.EventTS)
		}

		native, err := dec.Decode(k.val)
		switch {
		case isWireCorrupt(r.MalformedType):
			if err == nil {
				t.Fatalf("wire-corrupt record at %s (%s) decoded successfully; DLQ classification broken", key, r.MalformedType)
			}
		case r.IsMalformed: // semantic violation: valid avro, lying values
			if err != nil {
				t.Fatalf("semantic record at %s (%s) failed to decode: %v", key, r.MalformedType, err)
			}
			if native["event_id"] != r.EventID {
				t.Fatalf("semantic record at %s: event_id %v != ledger %s", key, native["event_id"], r.EventID)
			}
		default:
			if err != nil {
				t.Fatalf("clean record at %s failed to decode: %v", key, err)
			}
			if native["event_id"] != r.EventID {
				t.Fatalf("clean record at %s: event_id %v != ledger %s", key, native["event_id"], r.EventID)
			}
			if decodedClean < 25 {
				decodedClean++
				if r.EventType == "purchase" && native["revenue"] == nil {
					t.Fatalf("purchase %s has nil revenue", r.EventID)
				}
			}
		}
	}
}

// readLedger parses the ground truth JSONL.
func readLedger(t *testing.T, path string) []ledger.Row {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []ledger.Row
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r ledger.Row
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("bad ledger line %q: %v", line, err)
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func isWireCorrupt(malformedType string) bool {
	switch malformedType {
	case malGarbage, malBadMagic, malCorruptBdy:
		return true
	default:
		return false
	}
}

// TestGeneratorBackfill covers the simulated-clock path: a half "day" of
// event time at high rate, compressed into wall seconds. Event timestamps
// must land inside the requested window even though wall time was seconds.
func TestGeneratorBackfill(t *testing.T) {
	env := testenv.New(t)
	gt := filepath.Join(t.TempDir(), "gt.jsonl")

	cfg := config{
		brokers: env.Brokers, srURL: env.SRURL, topic: "events.raw",
		partitions: 4, rf: 1, retention: time.Hour,
		rate:  1000,
		users: 100, products: 20, seed: 99, keyBy: "session", schemaVer: 1,
		dupRate: 0.05, malformedRte: 0.05, lateRate: 0.05,
		lateMax: 30 * time.Second, oooMax: 2 * time.Second,
		ringSize:         256,
		backfill:         true,
		backfillStart:    "2026-01-15T00:00:00Z",
		backfillDuration: time.Minute,
		backfillSpeedup:  1000,
		gtFile:           gt,
		compression:      "snappy",
		maxBuffered:      10000,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}

	rows := readLedger(t, gt)
	if len(rows) < 30000 { // 1min * 1000/s = 60k new, plus dups
		t.Fatalf("backfill too small: %d rows", len(rows))
	}
	start, end := int64(0), int64(0)
	for _, r := range rows {
		if start == 0 || r.EventTS < start {
			start = r.EventTS
		}
		if r.EventTS > end {
			end = r.EventTS
		}
	}
	wantStart := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC).UnixMilli()
	if abs64(start-wantStart) > 5_000 { // ooo skew <= 2s
		t.Fatalf("backfill event-time starts at %s, want ~%s", msUTC(start), msUTC(wantStart))
	}
	if end-start < 50*1000 {
		t.Fatalf("backfill covered only %dms of event time", end-start)
	}

	// v1 payloads still decode via the registry, and must not carry v2 fields.
	vals := consumeAll(t, env.Brokers, cfg.topic)
	srcl, err := sr.NewClient(sr.URLs(env.SRURL))
	if err != nil {
		t.Fatal(err)
	}
	dec := codec.NewDecoder(srcl)
	checked := 0
	for _, r := range rows {
		if checked >= 25 {
			break
		}
		if r.IsMalformed && !isSemanticType(r.MalformedType) {
			continue // wire-corrupt values cannot be decoded by design
		}
		k, ok := vals[fmt.Sprintf("%d:%d", r.Partition, r.Offset)]
		if !ok {
			continue
		}
		native, err := dec.Decode(k)
		if err != nil {
			t.Fatalf("backfill record p%d/o%d does not decode: %v", r.Partition, r.Offset, err)
		}
		if _, has := native["device"]; has {
			t.Fatal("v1 backfill payload carries v2 field device")
		}
		if native["event_id"] != r.EventID {
			t.Fatalf("backfill event_id %v != ledger %s", native["event_id"], r.EventID)
		}
		checked++
	}
	if checked < 25 {
		t.Fatalf("only decoded %d backfill records", checked)
	}
}

// consumeAll drains a topic from the beginning, returning "partition:offset" -> value.
func consumeAll(t *testing.T, brokers []string, topic string) map[string][]byte {
	t.Helper()
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kcl.Close()

	out := make(map[string][]byte)
	deadline := time.Now().Add(30 * time.Second)
	var lastGrowth time.Time
	for time.Now().Before(deadline) {
		before := len(out)
		pollCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		fetches := kcl.PollFetches(pollCtx)
		cancel()
		fetches.EachError(func(topic string, p int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return
			}
			t.Fatalf("fetch error %s/%d: %v", topic, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			out[fmt.Sprintf("%d:%d", r.Partition, r.Offset)] = r.Value
		})
		if len(out) > before {
			lastGrowth = time.Now()
		} else if len(out) > 0 && time.Since(lastGrowth) > 2*time.Second {
			return out // drained
		}
	}
	return out
}

func abs64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}

func isSemanticType(malformedType string) bool {
	return malformedType != "" && !isWireCorrupt(malformedType)
}

func msUTC(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}
