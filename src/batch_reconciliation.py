# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
Streaming vs Batch Reconciliation

Recomputes 1-min VWAP from raw_trades in batch mode, compares against
streaming output in vwap_1min. They should match (>99% exact).

Small mismatches are expected — streaming drops late events beyond the
watermark; batch includes all events. Batch also deduplicates recovery-test rows.

Maps to TCS: "PySpark batch reconciliation to cross-check streaming output."

Output (overwrite each run):
  docs/reconciliation_report.md

Run:
  cd src
  python batch_reconciliation.py

Prerequisite: raw_trades + vwap_1min Delta tables populated.
"""

import os
from datetime import datetime

from pyspark.sql.functions import abs as spark_abs, col, count, sum as spark_sum, window

from config import DELTA_RAW_TRADES, DELTA_VWAP_1MIN
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "reconciliation_report.md")

EXACT_MATCH_PCT = 0.01
PASS_THRESHOLD_PCT = 99.0
CLOSE_THRESHOLD_PCT = 95.0


def _to_markdown(df) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def reconcile() -> None:
    """Batch-recompute VWAP, join to streaming, write reconciliation report."""
    if not os.path.exists(DELTA_RAW_TRADES) or not os.path.exists(DELTA_VWAP_1MIN):
        print("ERROR: Missing Delta tables. Run producer + stream_processor first.")
        raise SystemExit(1)

    spark = get_spark_session("BatchReconciliation")
    spark.sparkContext.setLogLevel("ERROR")

    lines: list[str] = []
    lines.append("# Streaming vs Batch Reconciliation Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(
        "\n**Method:** Recompute VWAP from raw trades in batch, compare against streaming output"
    )
    lines.append("\n---\n")

    print("=" * 60)
    print("STREAMING vs BATCH RECONCILIATION")
    print("=" * 60)

    # Batch side: full history, no watermark — dedup in case recovery test left duplicates
    raw = spark.read.format("delta").load(DELTA_RAW_TRADES).dropDuplicates(["trade_id"])
    raw_count = raw.count()
    print(f"\nRaw trades (deduped): {raw_count:,}")

    batch_vwap = (
        raw.groupBy(window(col("event_time"), "1 minute").alias("win"), "ticker")
        .agg(
            (spark_sum(col("price") * col("quantity")) / spark_sum("quantity")).alias("batch_vwap"),
            count("trade_id").alias("batch_trade_count"),
            spark_sum("quantity").alias("batch_volume"),
        )
        .withColumn("window_start", col("win.start"))
        .withColumn("window_end", col("win.end"))
        .drop("win")
    )

    stream_vwap = spark.read.format("delta").load(DELTA_VWAP_1MIN).select(
        col("window_start"),
        col("ticker"),
        col("vwap").alias("stream_vwap"),
        col("trade_count").alias("stream_trade_count"),
        col("total_volume").alias("stream_volume"),
    )

    comparison = (
        batch_vwap.join(stream_vwap, ["window_start", "ticker"], "inner")
        .withColumn("vwap_diff", spark_abs(col("batch_vwap") - col("stream_vwap")))
        .withColumn(
            "vwap_diff_pct",
            spark_abs(col("batch_vwap") - col("stream_vwap")) / col("batch_vwap") * 100,
        )
        .withColumn("count_match", col("batch_trade_count") == col("stream_trade_count"))
    )

    total_windows = comparison.count()
    matching = comparison.filter(col("vwap_diff_pct") < EXACT_MATCH_PCT).count()
    close_match = comparison.filter(col("vwap_diff_pct") < 1.0).count()
    mismatched = total_windows - matching
    match_pct = (matching / max(total_windows, 1)) * 100

    print("\n--- RESULTS ---")
    print(f"Total windows compared: {total_windows}")
    print(f"Exact match (diff < 0.01%): {matching} ({match_pct:.1f}%)")
    print(f"Close match (diff < 1%): {close_match}")
    print(f"Mismatched: {mismatched}")

    lines.append("## Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Raw trades (deduped) | {raw_count:,} |")
    lines.append(f"| Windows compared | {total_windows} |")
    lines.append(f"| Exact match (< 0.01%) | {matching} ({match_pct:.1f}%) |")
    lines.append(f"| Close match (< 1%) | {close_match} |")
    lines.append(f"| Mismatched | {mismatched} |")

    if mismatched > 0:
        lines.append("\n## Mismatched Windows (Top 10)\n")
        mismatch_pdf = (
            comparison.filter(col("vwap_diff_pct") >= EXACT_MATCH_PCT)
            .orderBy("vwap_diff_pct", ascending=False)
            .select(
                "window_start",
                "ticker",
                "batch_vwap",
                "stream_vwap",
                "vwap_diff_pct",
                "count_match",
            )
            .limit(10)
            .toPandas()
        )
        mismatch_pdf["batch_vwap"] = mismatch_pdf["batch_vwap"].round(4)
        mismatch_pdf["stream_vwap"] = mismatch_pdf["stream_vwap"].round(4)
        mismatch_pdf["vwap_diff_pct"] = mismatch_pdf["vwap_diff_pct"].round(4)
        lines.append(_to_markdown(mismatch_pdf))
        lines.append(
            "\n\n> **Note:** Small mismatches are expected. Streaming drops late events "
            "beyond the watermark that batch includes. Batch also deduplicates recovery-test "
            "rows that streaming counted."
        )

    lines.append("\n## Verdict\n")
    if match_pct >= PASS_THRESHOLD_PCT:
        lines.append(f"✅ **PASSED — {match_pct:.1f}% reconciliation rate (above 99% threshold)**")
        print(f"\n✅ PASSED — {match_pct:.1f}% reconciliation rate")
    elif match_pct >= CLOSE_THRESHOLD_PCT:
        lines.append(
            f"⚠️ **CLOSE — {match_pct:.1f}% reconciliation rate. "
            "Minor watermark/dedup differences.**"
        )
        print(f"\n⚠️ CLOSE — {match_pct:.1f}% reconciliation rate")
    else:
        lines.append(f"❌ **FAILED — {match_pct:.1f}% reconciliation rate. Investigate mismatches.**")
        print(f"\n❌ FAILED — {match_pct:.1f}% reconciliation rate")

    lines.append("\n## Why Mismatches Happen\n")
    lines.append(
        "1. **Watermark drops:** Streaming drops events beyond the 10-second watermark. "
        "Batch processes ALL events."
    )
    lines.append(
        "2. **Recovery duplicates:** SIGKILL recovery may have added duplicate rows. "
        "Batch deduplicates on `trade_id`, streaming doesn't."
    )
    lines.append(
        "3. **Window boundaries:** Micro-batch timing can assign a trade to a different "
        "window than batch grouping."
    )
    lines.append("\nThese are expected trade-offs, not bugs.")

    lines.append("\n---")
    lines.append(f"\n*Report generated by `batch_reconciliation.py` at {datetime.now().isoformat()}*")

    spark.stop()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))

    print(f"\nReport saved to: {REPORT_PATH}")

    if match_pct < CLOSE_THRESHOLD_PCT:
        raise SystemExit(1)


if __name__ == "__main__":
    reconcile()
