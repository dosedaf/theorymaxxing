# clickstream — data faucets for a streaming pipeline exercise

This is **only the data layer** for the clickstream pipeline project. Two
faucets pour processable, deliberately noisy data into your Kafka cluster, and
journal ground truth so you can prove your pipeline is correct:

```
                        ┌──────────────┐
                        │   refseed    │──▶ ref.users    (compacted)
                        │              │──▶ ref.products (compacted)
                        └──────────────┘
┌──────────────┐
│  generator   │──▶ events.raw ──▶ [your ingest ▶ enrichment ▶ aggregator ▶ sink]
│              │
│ (ledger.txt) │──▶ ground_truth.jsonl  (the oracle, bypasses Kafka)
└──────────────┘
```

You own everything to the right of `events.raw`: ingest, enrichment,
aggregation, sink, plus all cluster/topic/monitoring decisions. If a faucet
needs to change to unblock you, that's a bug in this module.

## Requirements

- Kafka broker(s) reachable (KRaft or ZK, any recent version)
- Confluent Schema Registry (the faucets register Avro schemas on startup)
- Go 1.24+ to build

## Quickstart

```bash
# 1. seed reference data (compacted topics; same seed as generator!)
go run ./cmd/refseed --snapshot-only

# 2. start the clickstream faucet (Ctrl-C for a clean shutdown + summary)
go run ./cmd/generator

# 3. start the mutation loop so your enrichment join has a moving table
go run ./cmd/refseed --mutate-rate 5
```

Defaults assume `localhost:9092` and `http://localhost:8081`; override with
`--brokers` / `--sr-url` or `KAFKA_BROKERS` / `SCHEMA_REGISTRY_URL`.

Both faucets create their topics on first run with the configs described
below. Manage topics yourself? Pass `--no-provision` and the faucets won't
touch the cluster's metadata.

## Data contract

### Wire format

Values on `events.raw`, `ref.users`, `ref.products` are Avro binary with the
Confluent wire header:

```
byte 0:      magic 0x00
bytes 1-4:   schema ID, big-endian uint32
bytes 5..:   Avro binary payload
```

Subjects follow the default TopicNameStrategy: `events.raw-value`,
`ref.users-value`, `ref.products-value`. Schemas live in `schemas/` and are
also embedded into the binaries; a unit test keeps the two in sync.

**Event schema (v1 → v2):** `clickstream-event-v2.avsc` adds optional
`device` and `referrer` with defaults — BACKWARD compatible. The generator
pins subject compatibility to BACKWARD and registers idempotently (same text
→ same schema ID). Run `--schema-version 2` (default) or `1`; flip mid-day to
watch consumers cope with evolution.

**Timestamps:** Kafka record timestamp = event time (`CreateTime`), and
`event_ts` in the payload says the same thing. Event time is NOT monotonic —
see the noise section.

**Keys:** `--key-by user` (default) or `--key-by session`. Same key → same
partition; flipping the key strategy changes partition distribution, join
locality, and hot-partition behavior. That's the point.

### Malformed records (the DLQ fodder)

`--malformed-rate` (default 2%) of new events are corrupted into four
families, each hitting a different consumer failure point:

| type | wire bytes | consumer failure | event_id decodable? |
|---|---|---|---|
| `garbage_bytes` | random noise, no header | framing fails | no |
| `bad_magic` | header with magic `0x07` | magic check fails | no |
| `corrupt_body` | real header + 2-byte body | Avro decode fails | no |
| `semantic:*` | valid Avro, lying values | your validation rules | **yes** |

Semantic violations (one per record): `neg_revenue`, `zero_qty`,
`empty_user`, `unknown_product`, `epoch_ts` (event_ts=0), `future_ts`
(+30y). For these, the payload lies but the ledger records the *intended*
truth, so a DLQ row keyed by event_id maps 1:1 to a ledger row.

Suggested DLQ envelope: `schemas/dlq-record.avsc` (reference contract — the
faucets don't produce it).

### Reference data

`ref.users` / `ref.products` are compacted, keyed by `user_id` /
`product_id`. `refseed` writes a full snapshot, then mutates at
`--mutate-rate`: plan changes, loyalty drift, relocations, active toggles,
price moves, stock drift, renames — and 10% of product mutations are
**tombstones** (deleted products; your enrichment join must handle deletes).
Compaction is tuned aggressive for dev (10s segments, low cleanable ratio)
so tombstones actually disappear quickly.

Note: event `revenue` is computed by the generator from the price at purchase
time and travels in the event itself — the catalog join enriches
(name/category/etc.), it doesn't need to recompute revenue.

### metrics.1m

The generator doesn't produce it — that's your aggregator's job.
`schemas/metric-1m.avsc` is the suggested output contract (windowed metrics
keyed by `dim_type:dim_key:window_start`, with `updated_at` for
last-write-wins upserts).

## Noise knobs (generator)

| flag | default | effect |
|---|---|---|
| `--rate` | 200 | events/sec (steady, ticker-driven) |
| `--dup-rate` | 3% | exact re-emission of a recent clean event (same event_id, same bytes, same partition) |
| `--malformed-rate` | 2% | four-family corruption, see above |
| `--late-rate` / `--late-max` | 2% / 45s | events held off-wire, then emitted late |
| `--ooo-max` | 5s | every event's timestamp trails stream-now by [0, max) |
| `--schema-version` | 2 | Avro version produced |
| `--key-by` | user | partition key choice |
| `--users` / `--products` / `--seed` | 500 / 60 / 42 | universe size + determinism |
| `--duration` | 0 | wall-clock stop (0 = until Ctrl-C) |

All rolls come from one seeded rng in a fixed order: same flags → same event
sequence, same universe, same dup/late/malformed decisions. Absolute wall
clock times still differ between runs.

### Backfill mode

```bash
go run ./cmd/generator --backfill                 # blast a simulated 24h
go run ./cmd/generator --backfill --backfill-start 2026-10-01T00:00:00Z \
    --backfill-duration 12h --backfill-max-eps 5000
```

Simulated event-time advances at `--rate` eps regardless of wall speed; late
holds shrink by `--backfill-speedup` (they stay late *relative to event
time*, which is what exercises watermarks). Append to a backfill run's ledger
to reconcile a replayed day.

## Ground truth ledger

Every generator emission is journaled to `ground_truth.jsonl` **only after
Kafka acks it** — the ledger is what the cluster actually has. JSONL, one row
per emission:

```json
{"event_id":"...","event_type":"purchase","event_ts_ms":1767000000123,
 "user_id":"u-000042","session_id":"s-00000017","page":"/checkout",
 "product_id":"p-0003","qty":2,"revenue":59.98,"device":"mobile",
 "referrer":"google.com","schema_version":2,"key":"u-000042",
 "partition":7,"offset":1234,"is_duplicate":false,"is_malformed":false,
 "malformed_type":"","is_late":false,"hold_ms":0,"emitted_at_ms":1767000001300}
```

- duplicates: a second row with the same `event_id`, `is_duplicate: true`
- malformed: intended values + `malformed_type`; for wire-level corruption
  the event_id is NOT recoverable on the wire (count-based reconciliation)
- SIGINT/SIGTERM flush the ledger; `kill -9` may lose the last buffered rows
  (and the last unacked records — they stay consistent)

### Reconciliation math

After your pipeline ran, with ledger file L:

- **events.clean** should contain exactly the ledger rows with
  `!is_malformed`, deduped by `event_id`:
  `count(clean) == L.rows - L.duplicates - L.malformed` (where malformed is
  counted per distinct event_id when you DLQ semantic violations by ID —
  simpler: compare `count(clean distinct event_id)` to
  `count(distinct event_id where !is_malformed)`)
- **events.dlq** should hold `L.malformed` rows if you DLQ every malformed
  record (dups of malformed are never generated)
- **revenue** summed from clean purchases (or `metrics.1m` revenue) must equal
  the generator's printed `ground-truth revenue`
- **windows/lateness**: ledger rows carry true `event_ts_ms` and
  `emitted_at_ms`, so expected window contents under your watermark policy are
  computable in plain SQL/jq — late rows beyond your allowed-lateness are
  *supposed* to be dropped; everything else must be there

The end-of-run summary prints the exact expected numbers, e.g.:

```
reconcile hint: distinct clean events = delivered - duplicate rows = 10432
```

## Topic configs the faucets create

| topic | partitions | rf | configs |
|---|---|---|---|
| `events.raw` (`--topic`, `--partitions`, `--rf`) | 12 | 1 | `retention.ms=24h` |
| `ref.users`, `ref.products` (`--users-topic`, `--products-topic`) | 6 | 1 | `cleanup.policy=compact`, `segment.ms=10s`, `min.cleanable.dirty.ratio=0.05`, `delete.retention.ms=100`, `max.compaction.lag.ms=60s` |

Existing topics are never reconfigured — recreate them yourself if you want
different numbers, or run `--no-provision` from the start.

## Flags

Full reference: `go run ./cmd/generator --help`, `go run ./cmd/refseed --help`.

refseed essentials: `--snapshot-only`, `--mutate-rate`, `--duration`,
`--seed/--users/--products` (**must match the generator**), `--linger`,
`--compression`.

## Layout

```
schemas/                  Avro contracts (events v1/v2, ref user/product, metric-1m, dlq)
cmd/generator/            raw clickstream faucet + ground truth ledger
cmd/refseed/              reference data faucet (snapshot + CDC-style mutations)
internal/sim/             seeded universe + session funnel engine
internal/codec/           Avro/SR wire format: register, encode, decode helpers
internal/ledger/          ground-truth JSONL writer
internal/kprod/           tuned franz-go producer factory (idempotent, acks=all)
internal/topics/          kadm provisioning helpers (create-if-missing only)
```

The codec package also ships a `Decoder` helper (fetch-by-schema-ID +
goavro) you can copy into your services for consuming these topics.
