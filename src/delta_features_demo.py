"""
Delta Lake Features Demo — time travel, history, schema enforcement.

Demonstrates two key Delta Lake capabilities:
1. Time travel: query any previous version of your data via the transaction log
2. Schema enforcement: reject writes with wrong types or extra columns

Output (overwrite each run):
  docs/delta_features_report.md

Run:
  cd src
  python delta_features_demo.py

Prerequisite: vwap_1min Delta table populated (run stream_processor first).
"""

import os
from datetime import datetime

from delta.tables import DeltaTable

from config import DELTA_VWAP_1MIN
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "delta_features_report.md")


def demo() -> None:
    """Run time-travel and schema-enforcement demos; write markdown report."""
    lines: list[str] = []
    lines.append("# Delta Lake Features Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"\n**Table:** vwap_1min (`{DELTA_VWAP_1MIN}`)")
    lines.append("\n---\n")

    if not os.path.exists(DELTA_VWAP_1MIN):
        lines.append("❌ **Delta table not found.** Run producer + `stream_processor.py` first.")
        _write_report(lines)
        print("\n".join(lines))
        print(f"\nReport saved to: {REPORT_PATH}")
        return

    spark = get_spark_session("DeltaDemo")
    spark.sparkContext.setLogLevel("ERROR")

    # === TIME TRAVEL ===
    lines.append("## 1. Time Travel — Version History\n")
    lines.append(
        "Delta Lake keeps a transaction log (_delta_log/) of every write. "
        "You can query any previous version with `versionAsOf`.\n"
    )

    print("\n=== TIME TRAVEL — Version History ===")
    dt = DeltaTable.forPath(spark, DELTA_VWAP_1MIN)
    history = dt.history()
    history_rows = history.select(
        "version", "timestamp", "operation", "operationMetrics"
    ).collect()

    lines.append("| Version | Timestamp | Operation | Rows Written |")
    lines.append("|---------|-----------|-----------|--------------|")
    for row in history_rows:
        metrics = row["operationMetrics"] or {}
        rows_written = metrics.get("numOutputRows", "N/A")
        lines.append(
            f"| {row['version']} | {row['timestamp']} | {row['operation']} | {rows_written} |"
        )

    history.select("version", "timestamp", "operation", "operationMetrics").show(
        truncate=False
    )

    version_count = history.count()
    lines.append(f"\n**Total versions:** {version_count}")

    if version_count > 1:
        # versionAsOf=0 → table as it existed after the first commit
        current = spark.read.format("delta").load(DELTA_VWAP_1MIN).count()
        old = (
            spark.read.format("delta")
            .option("versionAsOf", 0)
            .load(DELTA_VWAP_1MIN)
            .count()
        )
        lines.append("\n### Version Comparison\n")
        lines.append("| Version | Row Count |")
        lines.append("|---------|-----------|")
        lines.append(f"| Version 0 (first write) | {old:,} |")
        lines.append(f"| Current (latest) | {current:,} |")
        lines.append(f"| Difference (Δ) | {current - old:,} |")
        lines.append("\n✅ **Time travel works — can query any historical version.**")
        print(f"\nVersion 0: {old:,} rows | Current: {current:,} rows | Δ: {current - old:,}")
    else:
        lines.append(
            "\n*Only 1 version exists. Run the pipeline again to see version comparison.*"
        )

    # === SCHEMA ENFORCEMENT ===
    lines.append("\n## 2. Schema Enforcement\n")
    lines.append(
        "Delta Lake rejects writes that don't match the existing table schema.\n"
    )
    lines.append(
        "**Test:** Attempt to append a row with wrong types "
        "(string instead of float for VWAP) and an extra column.\n"
    )

    print("\n=== SCHEMA ENFORCEMENT ===")
    # Deliberately bad: vwap should be double, extra_col doesn't exist in table
    bad = spark.createDataFrame(
        [("AAPL", "not_number", "extra")],
        ["ticker", "vwap", "extra_col"],
    )
    try:
        bad.write.format("delta").mode("append").save(DELTA_VWAP_1MIN)
        lines.append("❌ **FAIL — Bad schema was accepted (shouldn't happen)**")
        print("ERROR: Bad schema accepted (shouldn't happen)")
    except Exception as e:
        error_type = type(e).__name__
        lines.append("```")
        lines.append(f"Error type: {error_type}")
        lines.append(f"Message: {str(e)[:200]}")
        lines.append("```")
        lines.append(
            "\n✅ **PASS — Delta Lake blocked the bad write. Data integrity protected.**"
        )
        print(f"Schema enforcement blocked bad write: {error_type}")

    # === SUMMARY ===
    lines.append("\n## 3. Why This Matters\n")
    lines.append(
        "- **Time travel** lets you debug bad data by querying previous versions, "
        "or roll back a corrupted table."
    )
    lines.append(
        "- **Schema enforcement** prevents bugs in the streaming processor from "
        "silently writing garbage data."
    )
    lines.append(
        "- Both features come with Delta Lake — use `.format('delta')` instead of "
        "`.format('parquet')`."
    )

    lines.append("\n---")
    lines.append(
        f"\n*Report generated by `delta_features_demo.py` at {datetime.now().isoformat()}*"
    )

    spark.stop()

    _write_report(lines)
    print()
    for line in lines:
        print(line)
    print(f"\nReport saved to: {REPORT_PATH}")


def _write_report(lines: list[str]) -> None:
    """Overwrite docs/delta_features_report.md (same pattern as other verification scripts)."""
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    demo()
