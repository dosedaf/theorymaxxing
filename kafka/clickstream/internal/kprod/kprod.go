// Package kprod builds the faucet's franz-go producer with tuned defaults:
// idempotent produce, all-ISR acks, configurable linger and compression.
package kprod

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config carries the knobs both faucets expose as flags.
type Config struct {
	Brokers            []string
	Linger             time.Duration
	Compression        string // none | gzip | snappy | lz4 | zstd
	MaxBufferedRecords int
}

// New builds a producer client. Idempotence is on by default in franz-go, so
// delivery is exactly-once per producer session (retries cannot duplicate).
func New(cfg Config) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers configured")
	}
	compression, err := parseCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(compression),
	}
	if cfg.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(cfg.Linger))
	}
	if cfg.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(cfg.MaxBufferedRecords))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return cl, nil
}

func parseCompression(name string) (kgo.CompressionCodec, error) {
	switch name {
	case "", "snappy":
		return kgo.SnappyCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf("unknown compression %q (none|gzip|snappy|lz4|zstd)", name)
	}
}
