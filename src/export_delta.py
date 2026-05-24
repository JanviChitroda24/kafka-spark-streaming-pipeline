"""Export Delta tables to CSV for analysis and portability."""

import os
from spark_config import get_spark_session
from config import DELTA_VWAP_1MIN, DELTA_VWAP_5MIN, DELTA_RAW_TRADES

def export():
    spark = get_spark_session("Export")
    spark.sparkContext.setLogLevel("ERROR")

    export_dir = os.path.join(os.path.dirname(__file__), "..", "data", "export")
    os.makedirs(export_dir, exist_ok=True)

    tables = [
        ("vwap_1min", DELTA_VWAP_1MIN),
        ("vwap_5min", DELTA_VWAP_5MIN),
        ("raw_trades", DELTA_RAW_TRADES),
    ]

    for name, path in tables:
        if os.path.exists(path):
            df = spark.read.format("delta").load(path)
            count = df.count()
            out_path = os.path.join(export_dir, name)
            df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
            print(f"✓ {name}: {count:,} rows → {out_path}")
        else:
            print(f"✗ {name}: Delta table not found at {path}")

    spark.stop()
    print("\nExport complete.")

if __name__ == "__main__":
    export()
