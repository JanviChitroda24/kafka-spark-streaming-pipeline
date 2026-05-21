"""
Kafka → Spark Structured Streaming smoke test.

Reads trade events from the stock_trades topic, parses JSON into columns,
and prints batches to the console every 5 seconds.

Why this file exists before the full processor:
- Proves the end-to-end link: Producer → Kafka → Spark (3rd milestone)
- Validates TRADE_SCHEMA matches what 2nd milestone producers actually send
- Faster debug cycle than writing to Delta — console output is immediate

Run with producer active in a second terminal:
  Terminal 1: python producer.py --mode simulated --eps 50 --duration 120
  Terminal 2: python stream_reader.py
"""

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from spark_config import get_spark_session

# Must mirror the JSON schema from trade_simulator.py / finnhub_producer.py
# Nullable=True on optional fields (bid/ask/exchange/source) — Finnhub sends nulls
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


def test_read() -> None:
    spark = get_spark_session("TestKafkaRead")

    # Step 1: Raw Kafka stream — every row is (key, value, topic, partition, offset, timestamp)
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # "latest" = only new messages after Spark starts — avoids replaying old test data
        .option("startingOffsets", "latest")
        .load()
    )

    # Step 2: Parse JSON value bytes → typed columns; keep kafka metadata for debugging
    parsed = (
        raw.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), TRADE_SCHEMA).alias("trade"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("kafka_key", "trade.*", "kafka_timestamp")
    )

    # Step 3: Console sink — append mode prints each micro-batch as a table
    query = (
        parsed.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)   # show full column values
        .option("numRows", 20)       # max rows printed per batch
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Run for 60 seconds then clean shutdown — enough to verify data is flowing
    query.awaitTermination(timeout=60)
    query.stop()
    spark.stop()


if __name__ == "__main__":
    test_read()
