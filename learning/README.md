# learning/

Operating system for learning. One pipeline, not four projects:

```
Kafka learning → brain dumps → blog posts → interview material
Kafka learning → mini-PeerDB → benchmark experiments → thesis gap → thesis topic
```

The thesis is not a separate thing. The mini-PeerDB experiments
(at-least-once vs exactly-once, batching vs latency, backpressure,
throughput vs freshness) are thesis reconnaissance.

## Files

| File | What it is |
|---|---|
| [daily.md](daily.md) | Day template + efficiency rules |
| [weekly.md](weekly.md) | Weekly structure, Sunday ritual, goal setting |
| [merangkum.md](merangkum.md) | Blog post format (the Merangkum upgrade) |
| [gap-list.md](gap-list.md) | Living gap list — what you read but couldn't recall |
| [dumps/TEMPLATE.md](dumps/TEMPLATE.md) | Copy for every brain dump session |

## The two non-negotiables

1. **Dump before you stand up.** Every learning session ends with retrieval
   at the desk, minutes after reading. Not next day. Not "later."
2. **Same-day application.** Whatever Block 1 taught, Block 2 must touch.
   If it can't be applied that day, it goes on the gap list.

## Current focus stack

1. **Concepts (Block 1):** Kafka — how it works, semantics. Primary source:
   Kafka: The Definitive Guide. DDIA + The Log = lookup, not cover-to-cover.
2. **Build (Block 2):** mini-PeerDB in Go (pglogrepl → batching → ClickHouse).
   Flagship artifact: benchmark vs PeerDB vs ClickPipes.
3. **Thesis:** streaming/Kafka. Papers read in light hours; the gap the
   benchmark exposes is the thesis candidate.
4. **Blog:** Merangkum series (DDIA Replication, The Log, ClickHouse/OLAP).
