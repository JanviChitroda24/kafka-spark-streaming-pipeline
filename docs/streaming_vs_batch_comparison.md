# Streaming vs Batch Processing — Comparison Guide

> **Portfolio reference:** Standalone interview doc tying this project to batch experience (TCS) and streaming choices (Kafka, Spark, watermarks).

## When to Use Which

| Factor | Batch | Streaming |
|--------|-------|-----------|
| Latency tolerance | Hours to days | Seconds to minutes |
| Data completeness | Full dataset available | Partial, arriving continuously |
| Processing complexity | Unlimited (joins, ML, etc.) | Constrained by state/memory |
| Cost | Pay per run | Pay continuously |
| Error recovery | Rerun the job | Checkpoints + replay |
| Example | Monthly financial reports | Real-time fraud detection |

## My Experience at TCS

At TCS, most of our work was **batch processing with PySpark** — large-scale reconciliation, revenue aggregations, processing 250GB+ monthly. We used **Spark Structured Streaming** when we needed near real-time processing with the same PySpark codebase. I also worked on some **Flink jobs** for sub-second anomaly detection, but PySpark was our primary engine.

### Why We Chose Each

| Scenario | Choice | Reasoning |
|----------|--------|-----------|
| Revenue reconciliation (250GB monthly) | Batch PySpark | Hours of latency fine, complex joins needed |
| Real-time transaction monitoring | Spark Structured Streaming | Same team, same API as batch, seconds latency OK |
| Sub-second anomaly alerts | Flink | True event-by-event, <100ms latency required |
| Data warehouse loading | Batch PySpark + Airflow | Overnight ETL, well-understood pattern |

## Kafka vs Kinesis

| Factor | Kafka | Kinesis |
|--------|-------|---------|
| Cloud | Multi-cloud or on-prem | AWS-native |
| Control | More tuning options, higher throughput | Fully managed, less overhead |
| Ecosystem | Kafka Connect, Schema Registry | Lambda, Firehose, Glue integration |
| Team expertise | If team knows Kafka | If team is AWS-heavy |

At TCS, we used **Kafka** because the client had on-prem infrastructure. In my AWS e-commerce project, I used **Kinesis** because everything was already in AWS.

## Flink vs Spark Streaming

| Factor | Flink | Spark Structured Streaming |
|--------|-------|---------------------------|
| Processing model | True event-by-event | Micro-batch |
| Latency | Sub-second | Seconds to minutes |
| State management | Native, optimized | Good but heavier |
| API | Separate streaming API | Same DataFrame API as batch |
| Use case | Complex event processing, low-latency | Unified batch + streaming, ML pipelines |

I prefer **Spark Structured Streaming** because the API is almost identical to batch PySpark — same DataFrame operations, easier to maintain, and the team already knew Spark. We only used Flink when we absolutely needed sub-second latency.

## Watermark Trade-offs

| Watermark Duration | Pros | Cons |
|-------------------|------|------|
| Short (5 sec) | Results appear faster, less state in memory | More late data dropped |
| Medium (10 sec) | Good balance for most use cases | — |
| Long (60 sec) | Captures nearly all late data | Results delayed, more memory usage |

In this project, I use a **10-second watermark** because stock trade data typically arrives within a few seconds. In my TCS project, we used a **30-second watermark** for billing data that came from multiple regional systems with varying network latency.

## How This Project Bridges Both

This project demonstrates both paradigms:

1. **Streaming:** Kafka → Spark Structured Streaming → Delta Lake (real-time VWAP, anomaly detection)
2. **Batch:** Delta Lake → Snowflake loader, batch reconciliation (recompute VWAP from raw data)
3. **Validation:** Compare streaming VWAP vs batch-recomputed VWAP to prove correctness

The Delta Lake layer acts as the bridge — streaming writes to it continuously, batch reads from it periodically. This is the **lakehouse architecture** pattern.
