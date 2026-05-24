# Kafka Partition Strategy

> **Portfolio reference:** Why we key by ticker, how murmur2 hashing assigns partitions, and what breaks if you use round-robin instead.

## Why Key-Based Partitioning?

We key messages by **ticker symbol** (`producer.send(..., key=ticker)`). This means:

- All AAPL trades go to the **same partition**
- All NVDA trades go to the **same partition**
- Ordering is guaranteed **within** a partition (not across partitions)

Run `src/show_partitions.py` (with Redpanda up) to generate the actual ticker → partition map in `docs/kafka_partition_map.md`.

## Partition Assignment

With **3 partitions** and **25 tickers**, Kafka's default partitioner (murmur2 hash) distributes roughly evenly:

```text
partition = hash(ticker_bytes) % num_partitions
```

Typical result: ~8–9 tickers per partition with low skew.

## Why This Matters

1. **Ordering guarantee** — Trade A for AAPL always arrives before Trade B for AAPL (if A was produced first). Critical for VWAP and event-time windowing.
2. **Parallelism** — Spark Structured Streaming reads each partition with a separate task. More partitions = more parallel read throughput (up to consumer limit).
3. **Consumer groups** — A second consumer (e.g. batch reconciliation) can read the same topic without stealing messages from the streaming consumer.

## What If We Didn't Key By Ticker?

Without a key, Kafka uses **round-robin** partitioning:

- Trade 1 for AAPL → partition 0
- Trade 2 for AAPL → partition 1
- Trade 3 for AAPL → partition 2

AAPL trades scatter across partitions with **no ordering guarantee**. Spark might process partition 1 before partition 0 — wrong sequence for time-sensitive aggregations.

## Scaling Considerations

| Topic | Guidance |
|-------|----------|
| **More partitions** | Higher parallelism; more coordination overhead |
| **Partition rebalancing** | Adding partitions to an existing topic **changes** key→partition mapping — set count upfront |
| **Local dev** | 3 partitions matches `KAFKA_PARTITIONS` in `config.py` |
| **Production** | Often 6–12 partitions for medium throughput; align with Spark executor cores |

## Related Files

| File | Purpose |
|------|---------|
| `src/show_partitions.py` | Generates live partition map report |
| `docs/kafka_partition_map.md` | Generated ticker → partition table |
| `src/producer.py` / `trade_simulator.py` | `key=ticker` on every send |
