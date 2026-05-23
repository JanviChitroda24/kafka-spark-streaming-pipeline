"""
Pipeline Health Monitor — Kafka lag, Delta stats, data freshness.

One command to check the health of the entire pipeline:
- Kafka topic status (partitions, offsets)
- Delta table row counts
- Latest record timestamps (data freshness)
- Overall health verdict

Output (overwrite each run):
  docs/pipeline_health_report.md

Run:
  cd src
  python stream_monitor.py

When to use:
  Before a run  — Is Kafka up? Do I have stale data to clean?
  During a run  — Are row counts growing? Is data fresh?
  After a run   — How many rows landed? What's the latest window?
  Before a demo — Is everything healthy? Can I start?
"""

import os
import subprocess
from datetime import datetime

from config import (
    DELTA_ANOMALY_ALERTS,
    DELTA_RAW_TRADES,
    DELTA_VWAP_1MIN,
    DELTA_VWAP_5MIN,
)
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "pipeline_health_report.md")


def get_kafka_info() -> tuple[str, bool]:
    """
    Get Kafka topic details from Redpanda via rpk.

    Returns (output_text, success). Uses subprocess instead of kafka-python
    because one rpk call returns partition offsets in readable text.
    """
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "redpanda",
                "rpk",
                "topic",
                "describe",
                "stock_trades",
                "--print-partitions",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "Unknown error").strip()
            return f"Error: {err}", False
        return result.stdout.strip(), True
    except FileNotFoundError:
        return "Error: docker not found — is Docker Desktop running?", False
    except subprocess.TimeoutExpired:
        return "Error: timed out waiting for rpk (is Redpanda up?)", False
    except Exception as e:
        return f"Error: {e}", False


def run_monitor() -> None:
    lines: list[str] = []
    lines.append("# Pipeline Health Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("\n---\n")

    # --- Kafka ---
    lines.append("## 1. Kafka (Redpanda) Status\n")
    kafka_output, kafka_ok = get_kafka_info()
    if kafka_ok:
        lines.append("✅ **Redpanda is running**\n")
        lines.append("```")
        lines.append(kafka_output)
        lines.append("```")
    else:
        lines.append("❌ **Redpanda is NOT running**\n")
        lines.append(f"Error: {kafka_output}")

    # --- Delta Tables ---
    lines.append("\n## 2. Delta Table Health\n")
    spark = get_spark_session("Monitor")
    spark.sparkContext.setLogLevel("ERROR")

    # (path, freshness column) — windowed tables use window_start, raw uses event_time
    tables = {
        "raw_trades": (DELTA_RAW_TRADES, "event_time"),
        "vwap_1min": (DELTA_VWAP_1MIN, "window_start"),
        "vwap_5min": (DELTA_VWAP_5MIN, "window_start"),
        "anomaly_alerts": (DELTA_ANOMALY_ALERTS, "window_start"),
    }

    lines.append("| Table | Rows | Latest Record | Status |")
    lines.append("|-------|------|---------------|--------|")

    table_health: dict[str, dict] = {}
    for name, (path, ts_col) in tables.items():
        if os.path.exists(path):
            try:
                df = spark.read.format("delta").load(path)
                count = df.count()
                if count > 0 and ts_col in df.columns:
                    latest = str(df.agg({ts_col: "max"}).collect()[0][0])
                else:
                    latest = "N/A"
                status = "✅ OK" if count > 0 else "⚠️ Empty"
                table_health[name] = {"count": count, "latest": latest, "ok": count > 0}
                lines.append(f"| {name} | {count:,} | {latest} | {status} |")
            except Exception as e:
                table_health[name] = {"count": 0, "latest": "N/A", "ok": False}
                lines.append(f"| {name} | ERROR | {str(e)[:40]} | ❌ Error |")
        else:
            table_health[name] = {"count": 0, "latest": "N/A", "ok": False}
            lines.append(f"| {name} | — | Not created yet | ⚠️ Missing |")

    # --- Data Volume Summary ---
    lines.append("\n## 3. Data Volume Summary\n")
    total_raw = table_health.get("raw_trades", {}).get("count", 0)
    total_vwap1 = table_health.get("vwap_1min", {}).get("count", 0)
    total_vwap5 = table_health.get("vwap_5min", {}).get("count", 0)
    total_alerts = table_health.get("anomaly_alerts", {}).get("count", 0)

    lines.append(f"- **Total raw trades:** {total_raw:,}")
    lines.append(f"- **1-min VWAP windows:** {total_vwap1:,}")
    lines.append(f"- **5-min VWAP windows:** {total_vwap5:,}")
    lines.append(f"- **Anomaly alert windows:** {total_alerts:,}")

    # ~25 tickers × 1 row per ticker per minute → rough run duration estimate
    if total_raw > 0 and total_vwap1 > 0:
        approx_minutes = total_vwap1 // 25 if total_vwap1 >= 25 else 1
        lines.append(f"- **Approximate run duration:** ~{approx_minutes} minutes")

    # --- Overall Verdict ---
    lines.append("\n## 4. Overall Health\n")
    all_tables_ok = all(t.get("ok", False) for t in table_health.values())

    if kafka_ok and all_tables_ok:
        lines.append("✅ **ALL SYSTEMS HEALTHY — Pipeline is fully operational.**")
    elif kafka_ok and not all_tables_ok:
        lines.append("⚠️ **Kafka is running but some Delta tables are missing/empty.**")
        lines.append("\nRun the producer + processor to populate tables.")
    elif not kafka_ok:
        lines.append("❌ **Redpanda is not running.**")
        lines.append("\nStart it with: `docker compose up -d`")

    lines.append("\n---")
    lines.append(
        f"\n*Report generated by `stream_monitor.py` at {datetime.now().isoformat()}*"
    )

    spark.stop()

    # Write report (overwrite mode — same pattern as verify_delta.py)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # Also print to console
    print()
    for line in lines:
        print(line)
    print(f"\nReport saved to: {REPORT_PATH}")


if __name__ == "__main__":
    run_monitor()
