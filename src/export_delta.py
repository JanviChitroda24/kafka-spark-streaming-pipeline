"""
Export Delta tables to CSV for analysis and portability.

Spark writes CSV as a folder with part-*.csv files (standard distributed write).
Useful for:
- Sharing a snapshot without Delta/Spark
- Opening in Excel or pandas for ad-hoc checks
- Feeding external tools that don't read Delta

Run:
  cd src
  python export_delta.py

Output:
  data/export/vwap_1min/
  data/export/vwap_5min/
  data/export/raw_trades/
"""

import os

from config import DELTA_RAW_TRADES, DELTA_VWAP_1MIN, DELTA_VWAP_5MIN
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
EXPORT_DIR = os.path.join(_REPO_ROOT, "data", "export")


def export() -> None:
    spark = get_spark_session("Export")
    spark.sparkContext.setLogLevel("ERROR")

    os.makedirs(EXPORT_DIR, exist_ok=True)

    tables = [
        ("vwap_1min", DELTA_VWAP_1MIN),
        ("vwap_5min", DELTA_VWAP_5MIN),
        ("raw_trades", DELTA_RAW_TRADES),
    ]

    print(f"Export directory: {EXPORT_DIR}\n")

    for name, path in tables.items():
        if not os.path.exists(path):
            print(f"✗ {name}: Delta table not found at {path}")
            continue

        df = spark.read.format("delta").load(path)
        count = df.count()
        out_path = os.path.join(EXPORT_DIR, name)

        # coalesce(1) → single CSV part file (fine for demo-scale data)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
        print(f"✓ {name}: {count:,} rows → {out_path}")

    spark.stop()
    print("\nExport complete.")


if __name__ == "__main__":
    export()
