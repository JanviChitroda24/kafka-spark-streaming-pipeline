"""Test VWAP calculation logic with static Spark batch data."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Minimal local Spark session — one JVM for all VWAP tests in this module."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TestVWAP")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _compute_vwap(spark, data):
    """VWAP = sum(price × quantity) / sum(quantity) — same formula as stream_processor."""
    df = spark.createDataFrame(data, ["ticker", "price", "quantity"])
    row = (
        df.withColumn("dv", df.price * df.quantity)
        .groupBy("ticker")
        .agg({"dv": "sum", "quantity": "sum"})
        .collect()[0]
    )
    return row["sum(dv)"] / row["sum(quantity)"]


class TestVWAP:
    def test_vwap_simple(self, spark):
        """100 @ $10 + 200 @ $12 = (1000+2400)/300 = $11.33"""
        data = [("AAPL", 10.0, 100), ("AAPL", 12.0, 200)]
        vwap = _compute_vwap(spark, data)
        assert abs(vwap - 11.333) < 0.01

    def test_vwap_single_trade(self, spark):
        """Single trade: VWAP = trade price."""
        data = [("NVDA", 140.0, 500)]
        vwap = _compute_vwap(spark, data)
        assert abs(vwap - 140.0) < 0.01

    def test_vwap_equal_quantities(self, spark):
        """Equal quantities: VWAP = simple average of prices."""
        data = [("TSLA", 280.0, 100), ("TSLA", 290.0, 100)]
        vwap = _compute_vwap(spark, data)
        assert abs(vwap - 285.0) < 0.01

    def test_vwap_heavily_weighted(self, spark):
        """Large trade dominates: 1 @ $100 + 10000 @ $200 ≈ $200."""
        data = [("META", 100.0, 1), ("META", 200.0, 10000)]
        vwap = _compute_vwap(spark, data)
        assert abs(vwap - 200.0) < 0.1
