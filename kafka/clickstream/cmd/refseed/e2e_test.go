package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"clickstream/internal/codec"
	"clickstream/internal/testenv"
)

// TestRefseedEndToEnd seeds the compacted topics against the in-memory
// broker, lets the mutation loop run briefly, then verifies the snapshot is
// complete and every value decodes against the registry.
func TestRefseedEndToEnd(t *testing.T) {
	env := testenv.New(t)

	cfg := config{
		brokers: env.Brokers, srURL: env.SRURL,
		usersTopic: "ref.users", productsTopic: "ref.products",
		users: 30, products: 8, seed: 5,
		partitions: 3, rf: 1,
		mutateRate: 50, duration: 500 * time.Millisecond,
		linger: 5 * time.Millisecond, compression: "snappy",
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}

	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(env.Brokers...),
		kgo.ConsumeTopics(cfg.usersTopic, cfg.productsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kcl.Close()

	srcl, err := sr.NewClient(sr.URLs(env.SRURL))
	if err != nil {
		t.Fatal(err)
	}
	dec := codec.NewDecoder(srcl)

	userIDs := make(map[string]bool)
	productIDs := make(map[string]bool)
	records, tombstones := 0, 0

	deadline := time.Now().Add(15 * time.Second)
	for (len(userIDs) < cfg.users || len(productIDs) < cfg.products) && time.Now().Before(deadline) {
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
			records++
			if len(r.Value) == 0 { // tombstone (compacted topic delete)
				tombstones++
				return
			}
			native, err := dec.Decode(r.Value)
			if err != nil {
				t.Fatalf("ref value on %s/%d:%d does not decode: %v", r.Topic, r.Partition, r.Offset, err)
			}
			switch r.Topic {
			case cfg.usersTopic:
				userIDs[fmt.Sprint(native["user_id"])] = true
			case cfg.productsTopic:
				productIDs[fmt.Sprint(native["product_id"])] = true
			}
		})
	}

	if len(userIDs) != cfg.users {
		t.Fatalf("snapshot incomplete: %d/%d users seen", len(userIDs), cfg.users)
	}
	if len(productIDs) != cfg.products {
		t.Fatalf("snapshot incomplete: %d/%d products seen", len(productIDs), cfg.products)
	}
	if records <= cfg.users+cfg.products {
		t.Fatalf("no mutations observed on top of the snapshot: %d records for %d snapshot rows",
			records, cfg.users+cfg.products)
	}
	t.Logf("users=%d products=%d records=%d tombstones=%d", len(userIDs), len(productIDs), records, tombstones)
}
