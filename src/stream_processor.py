# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
Core Streaming Processor: Kafka → Spark Structured Streaming → Delta Lake

Runs 4 streaming queries simultaneously from one Kafka source:
  1. Raw trades → Delta (bronze — untransformed event log)
  2. 1-minute VWAP per ticker → Delta (silver — granular analytics)
  3. 5-minute VWAP per ticker → Delta (silver — smoother signal)
  4. Anomaly bounds per 1-min window → Delta (gold — outlier detection input)

All queries share the same parsed Kafka stream but write to separate Delta
tables with separate checkpoints — independent failure/recovery per sink.

Handles late data with 10-second watermarks (WATERMARK_DELAY in config.py).

Run (with producer active in another terminal):
  python stream_processor.py           # Delta sinks only
  python stream_processor.py --debug   # + console preview of 1-min VWAP
"""

from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    lit,
    max as _max,
    min as _min,
    sum as _sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config import (
    ANOMALY_THRESHOLD_PCT,
    CHECKPOINT_BASE,
    DELTA_ANOMALY_ALERTS,
    DELTA_RAW_TRADES,
    DELTA_VWAP_1MIN,
    DELTA_VWAP_5MIN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    SPARK_TRIGGER_INTERVAL,
    WATERMARK_DELAY,
)
from spark_config import get_spark_session

# Same schema as stream_reader.py — must match Hour 2 producer JSON contract
TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType(), False),
    StructField("ticker", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("side", StringType(), False),
    StructField("trade_type", StringType(), False),
    StructField("bid_price", DoubleType(), True),
    StructField("ask_price", DoubleType(), True),
    StructField("timestamp", StringType(), False),
    StructField("exchange", StringType(), True),
    StructField("source", StringType(), True),
])


def read_from_kafka(spark):
    """
    Single Kafka source shared by all 4 downstream queries.

    Enriches raw trades with derived columns used by VWAP and anomaly logic:
      - event_time     → parsed trade timestamp for windowing (event time, not Kafka time)
      - dollar_volume  → price × quantity (VWAP numerator component)
      - buy_volume     → quantity where side=BUY (buy pressure metric)
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        # Don't crash if topic is recreated during dev — offsets may become invalid
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            from_json(col("value").cast("string"), TRADE_SCHEMA).alias("trade"),
            col("timestamp").alias("kafka_ingest_time"),
        )
        .select("trade.*", "kafka_ingest_time")
        # event_time drives all window aggregations — must be timestamp type
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("dollar_volume", col("price") * col("quantity"))
        .withColumn("is_buy", (col("side") == "BUY").cast("integer"))
        .withColumn("buy_volume", col("is_buy") * col("quantity"))
    )
    return parsed


def compute_windowed_vwap(parsed_stream, window_duration: str):
    """
    Tumbling window aggregation: VWAP and supporting metrics per ticker.

    VWAP = Σ(price × quantity) / Σ(quantity) = Σ(dollar_volume) / Σ(quantity)

    Watermark drops events arriving > WATERMARK_DELAY late — bounds state growth.
    """
    watermarked = parsed_stream.withWatermark("event_time", WATERMARK_DELAY)
    time_window = window(col("event_time"), window_duration)

    return (
        watermarked
        .groupBy(time_window.alias("window"), col("ticker"))
        .agg(
            (_sum("dollar_volume") / _sum("quantity")).alias("vwap"),
            _min("price").alias("low_price"),
            _max("price").alias("high_price"),
            avg("price").alias("avg_price"),
            _sum("quantity").alias("total_volume"),
            _sum("dollar_volume").alias("total_dollar_volume"),
            count("trade_id").alias("trade_count"),
            _sum("buy_volume").alias("buy_volume"),
            avg("bid_price").alias("avg_bid"),
            avg("ask_price").alias("avg_ask"),
        )
        .withColumn("sell_volume", col("total_volume") - col("buy_volume"))
        .withColumn(
            "buy_pressure",
            (col("buy_volume") / col("total_volume") * 100).cast("decimal(5, 2)"),
        )
        .withColumn(
            "avg_spread",
            (col("avg_ask") - col("avg_bid")).cast("decimal(10, 4)"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("window_duration", lit(window_duration))
        .drop("window")
    )


def write_to_delta(stream_df, delta_path: str, checkpoint_suffix: str, output_mode: str = "append"):
    """
    Start a Structured Streaming query writing to a Delta table.

    Each sink gets its own checkpoint — if VWAP fails, raw trades keep flowing.
    Checkpoint stores Kafka offsets + write progress for exactly-once semantics.
    """
    return (
        stream_df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_suffix}")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start(delta_path)
    )


def run_streaming_pipeline(debug_mode: bool = False) -> None:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("STOCK STREAMING PIPELINE")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
    print(f"  Delta: {DELTA_RAW_TRADES.rsplit('/', 1)[0]}")
    print(f"  Watermark: {WATERMARK_DELAY} | Trigger: {SPARK_TRIGGER_INTERVAL}")
    print(f"  Anomaly threshold: {ANOMALY_THRESHOLD_PCT}%")
    print("=" * 60)

    trades = read_from_kafka(spark)

    # Query 1: Bronze — append every trade unchanged for batch reconciliation later
    q1 = write_to_delta(
        trades.select(
            "trade_id", "ticker", "price", "quantity", "side",
            "trade_type", "exchange", "source", "event_time",
            "kafka_ingest_time", "dollar_volume",
        ),
        DELTA_RAW_TRADES,
        "raw_trades",
    )
    print("✓ Query 1: raw_trades → Delta")

    # Query 2: 1-min VWAP — granular analytics (updates every minute per ticker)
    vwap_1min = compute_windowed_vwap(trades, "1 minute")
    q2 = write_to_delta(vwap_1min, DELTA_VWAP_1MIN, "vwap_1min")
    print("✓ Query 2: 1-min VWAP → Delta")

    # Query 3: 5-min VWAP — smoother signal, less noise for trend views
    vwap_5min = compute_windowed_vwap(trades, "5 minutes")
    q3 = write_to_delta(vwap_5min, DELTA_VWAP_5MIN, "vwap_5min")
    print("✓ Query 3: 5-min VWAP → Delta")

    # Query 4: Window-level anomaly bounds (±2% from avg price in 1-min window)
    # Later: join individual trades against these bounds to flag outliers
    watermarked_trades = trades.withWatermark("event_time", WATERMARK_DELAY)
    windowed_avg = (
        watermarked_trades
        .groupBy(window(col("event_time"), "1 minute").alias("window"), col("ticker"))
        .agg(
            avg("price").alias("window_avg_price"),
            _sum("quantity").alias("window_volume"),
            count("trade_id").alias("window_trade_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn(
            "anomaly_upper",
            col("window_avg_price") * (1 + ANOMALY_THRESHOLD_PCT / 100),
        )
        .withColumn(
            "anomaly_lower",
            col("window_avg_price") * (1 - ANOMALY_THRESHOLD_PCT / 100),
        )
        .withColumn("threshold_pct", lit(ANOMALY_THRESHOLD_PCT))
        .drop("window")
    )
    q4 = write_to_delta(windowed_avg, DELTA_ANOMALY_ALERTS, "anomaly_alerts")
    print("✓ Query 4: anomaly detection → Delta")

    if debug_mode:
        (
            vwap_1min.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .option("numRows", 10)
            .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
            .queryName("debug_vwap")
            .start()
        )
        print("✓ Debug: 1-min VWAP → console")

    print("\nAll queries running. Press Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping all queries...")
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        print("Pipeline stopped.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stock streaming processor")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Also print 1-min VWAP batches to console",
    )
    args = parser.parse_args()
    run_streaming_pipeline(args.debug)
