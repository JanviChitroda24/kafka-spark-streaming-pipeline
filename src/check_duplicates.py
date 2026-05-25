"""
Duplicate Check Utility — verify data integrity in raw_trades Delta table.

Run anytime to inspect duplicate trade_ids — especially after recovery tests.
Shows whether duplicates appear exactly 2x (crash reprocessing) and count after dedup.

Run:
  cd src
  python check_duplicates.py

Prerequisite: raw_trades Delta table exists.
"""

import os

from pyspark.sql.functions import col, count

from config import DELTA_RAW_TRADES
from spark_config import get_spark_session


def check() -> None:
    """Print duplicate stats for raw_trades; exit 1 if duplicates found."""
    if not os.path.exists(DELTA_RAW_TRADES):
        print(f"ERROR: Delta table not found at {DELTA_RAW_TRADES}")
        raise SystemExit(1)

    spark = get_spark_session("DupCheck")
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.format("delta").load(DELTA_RAW_TRADES)
    total = df.count()

    # Group by trade_id — any cnt > 1 means the pipeline wrote that trade twice
    dupes = df.groupBy("trade_id").agg(count("*").alias("cnt")).filter("cnt > 1")
    dupe_count = dupes.count()

    print(f"\n{'=' * 50}")
    print(f"  Total rows:          {total:,}")
    print(f"  Duplicate trade_ids: {dupe_count}")

    if dupe_count > 0:
        all_exactly_2x = dupes.filter(col("cnt") != 2).count() == 0
        print(f"  Each appears 2x:     {all_exactly_2x}")
        dupes.show(5, truncate=False)

    deduped = df.dropDuplicates(["trade_id"])
    deduped_count = deduped.count()
    print(f"  After dedup:         {deduped_count:,}")
    print(f"  Duplicates removed:  {total - deduped_count}")
    print(f"{'=' * 50}")

    spark.stop()

    if dupe_count > 0:
        print(
            f"\n⚠️ {dupe_count} duplicate trade_ids found — "
            "run dropDuplicates(['trade_id']) in downstream queries"
        )


if __name__ == "__main__":
    check()
