// Package topics provisions the faucet topics with the configs the exercise
// expects. It only ever creates what is missing -- existing topics are left
// untouched, so you can manage them yourself and pass --no-provision.
package topics

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Spec describes one topic to ensure.
type Spec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]*string
}

// RawEvent returns the spec for a raw clickstream topic.
//
// 12 partitions: enough parallelism to watch partition assignment, rebalances
// and hot-partition effects; keyed records hash across them. 24h retention:
// a day of history for replay exercises without filling the disk.
func RawEvent(name string, partitions int32, rf int16, retention time.Duration) Spec {
	return Spec{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: rf,
		Configs: map[string]*string{
			"retention.ms": kadm.StringPtr(fmt.Sprint(retention.Milliseconds())),
		},
	}
}

// Compacted returns the spec for a reference-data topic.
//
// Compacted with aggressive dev-friendly settings (small segments, low
// min-cleanable ratio, short delete retention) so tombstones disappear and
// consumers reread the full state quickly.
func Compacted(name string, partitions int32, rf int16) Spec {
	return Spec{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: rf,
		Configs: map[string]*string{
			"cleanup.policy":            kadm.StringPtr("compact"),
			"min.cleanable.dirty.ratio": kadm.StringPtr("0.05"),
			"segment.ms":                kadm.StringPtr("10000"),
			"delete.retention.ms":       kadm.StringPtr("100"),
			"min.compaction.lag.ms":     kadm.StringPtr("0"),
			"max.compaction.lag.ms":     kadm.StringPtr("60000"),
		},
	}
}

// Ensure lists existing topics and creates any missing ones. An existing
// topic is never reconfigured.
func Ensure(ctx context.Context, adm *kadm.Client, specs []Spec) error {
	existing, err := adm.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	for _, s := range specs {
		if _, ok := existing[s.Name]; ok {
			log.Printf("topic %s exists, leaving as-is", s.Name)
			continue
		}
		resp, err := adm.CreateTopic(ctx, s.Partitions, s.ReplicationFactor, s.Configs, s.Name)
		if err != nil {
			return fmt.Errorf("create topic %s: %w", s.Name, err)
		}
		if resp.Err != nil {
			if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
				log.Printf("topic %s already exists (created concurrently)", s.Name)
				continue
			}
			return fmt.Errorf("create topic %s: %v %s", s.Name, resp.Err, resp.ErrMessage)
		}
		log.Printf("created topic %s: %d partitions, rf=%d, configs=%v",
			s.Name, s.Partitions, s.ReplicationFactor, configNames(s.Configs))
	}
	return nil
}

func configNames(cfgs map[string]*string) []string {
	names := make([]string, 0, len(cfgs))
	for k := range cfgs {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
