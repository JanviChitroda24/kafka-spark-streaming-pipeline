"""
Post-run verification for all 4 Delta tables.

Run after stopping the producer + stream_processor to confirm data landed correctly:
  cd src
  python verify_delta.py

Why a separate script instead of checking folders manually:
- Row counts catch empty tables or silent write failures
- show(5) validates schema and sample values (VWAP, timestamps, tickers)
- Reusable in Hour 5 integration test and before Snowflake load
"""

from config import (
    DELTA_ANOMALY_ALERTS,
    DELTA_RAW_TRADES,
    DELTA_VWAP_1MIN,
    DELTA_VWAP_5MIN,
)
from spark_config import get_spark_session


def verify() -> None:
    spark = get_spark_session("DeltaVerifier")
    spark.sparkContext.setLogLevel("WARN")

    # Maps friendly name → Delta path from config.py (repo-root relative)
    tables = {
        "raw_trades": DELTA_RAW_TRADES,
        "vwap_1min": DELTA_VWAP_1MIN,
        "vwap_5min": DELTA_VWAP_5MIN,
        "anomaly_alerts": DELTA_ANOMALY_ALERTS,
    }

    print("=" * 60)
    print("DELTA TABLE VERIFICATION")
    print("=" * 60)

    for name, path in tables.items():
        print(f"\n=== {name.upper()} ===")
        print(f"Path: {path}")
        try:
            df = spark.read.format("delta").load(path)
            row_count = df.count()
            print(f"Row count: {row_count:,}")
            if row_count == 0:
                print("  ⚠ Table exists but is empty — was the processor running?")
            else:
                df.show(5, truncate=False)
        except Exception as e:
            print(f"  Not found or error: {e}")

    print("\n" + "=" * 60)
    print("Verification complete.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    verify()
