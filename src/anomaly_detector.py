"""
Anomaly Detection on Streaming Data — find individual outlier trades.

Hour 4 wrote window-level bounds to anomaly_alerts (avg price ± 2%).
This script joins raw_trades back to those windows to find WHICH trades
actually breached the bounds — not just "this window had bounds."

Maps to TCS: detecting billing anomalies in near real-time.

Output (overwrite each run):
  docs/anomaly_detection_report.md

Run:
  cd src
  python anomaly_detector.py

Prerequisite: raw_trades + anomaly_alerts Delta tables (stream_processor).
"""

import os
from datetime import datetime

from pyspark.sql.functions import col, round as spark_round

from config import ANOMALY_THRESHOLD_PCT, DELTA_ANOMALY_ALERTS, DELTA_RAW_TRADES
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "anomaly_detection_report.md")


def _to_markdown(df) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def find_anomalies() -> None:
    """Join raw trades to anomaly windows; write markdown report."""
    if not os.path.exists(DELTA_RAW_TRADES) or not os.path.exists(DELTA_ANOMALY_ALERTS):
        print("ERROR: Missing Delta tables. Run producer + stream_processor first.")
        raise SystemExit(1)

    spark = get_spark_session("AnomalyReport")
    spark.sparkContext.setLogLevel("ERROR")

    alerts = spark.read.format("delta").load(DELTA_ANOMALY_ALERTS)
    raw = spark.read.format("delta").load(DELTA_RAW_TRADES)

    lines: list[str] = []
    lines.append("# Anomaly Detection Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(
        f"\n**Threshold:** {ANOMALY_THRESHOLD_PCT}% deviation from 1-minute window average"
    )
    lines.append("\n---\n")

    # Join: match each trade to its 1-min window by ticker + event_time in [start, end)
    anomalous_trades = (
        raw.alias("t")
        .join(
            alerts.alias("a"),
            (col("t.ticker") == col("a.ticker"))
            & (col("t.event_time") >= col("a.window_start"))
            & (col("t.event_time") < col("a.window_end")),
            "inner",
        )
        .filter(
            (col("t.price") > col("a.anomaly_upper"))
            | (col("t.price") < col("a.anomaly_lower"))
        )
        .select(
            col("t.trade_id"),
            col("t.ticker"),
            col("t.price"),
            col("t.quantity"),
            col("t.side"),
            col("t.trade_type"),
            col("t.event_time"),
            col("a.window_avg_price"),
            col("a.anomaly_upper"),
            col("a.anomaly_lower"),
            col("a.window_start"),
        )
        .withColumn(
            "deviation_pct",
            spark_round(
                ((col("price") - col("window_avg_price")) / col("window_avg_price") * 100),
                2,
            ),
        )
    )

    total_anomalies = anomalous_trades.count()
    total_trades = raw.count()
    total_windows = alerts.count()
    anomaly_rate = (total_anomalies / total_trades * 100) if total_trades > 0 else 0.0

    # --- Summary ---
    lines.append("## 1. Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total raw trades | {total_trades:,} |")
    lines.append(f"| Total windows analyzed | {total_windows:,} |")
    lines.append(f"| Anomalous trades found | {total_anomalies:,} |")
    lines.append(f"| Anomaly rate | {anomaly_rate:.2f}% |")

    # --- By ticker ---
    lines.append("\n## 2. Anomalies by Ticker\n")
    if total_anomalies > 0:
        by_ticker = (
            anomalous_trades.groupBy("ticker")
            .count()
            .orderBy("count", ascending=False)
            .toPandas()
        )
        lines.append("| Ticker | Anomalous Trades |")
        lines.append("|--------|-----------------|")
        for _, row in by_ticker.iterrows():
            lines.append(f"| {row['ticker']} | {row['count']} |")
    else:
        lines.append(
            "No anomalous trades detected. Expected with simulated data "
            "(small Gaussian price drift)."
        )

    # --- Sample trades ---
    lines.append("\n## 3. Sample Anomalous Trades\n")
    if total_anomalies > 0:
        sample = (
            anomalous_trades.orderBy(col("deviation_pct").desc())
            .limit(20)
            .toPandas()
        )
        sample["price"] = sample["price"].round(2)
        sample["window_avg_price"] = sample["window_avg_price"].round(2)
        sample["deviation_pct"] = sample["deviation_pct"].astype(float)
        lines.append("Top 20 anomalous trades by deviation:\n")
        display_cols = [
            "ticker",
            "price",
            "window_avg_price",
            "deviation_pct",
            "quantity",
            "side",
            "trade_type",
            "event_time",
        ]
        lines.append(_to_markdown(sample[display_cols]))
    else:
        lines.append("No anomalous trades to display.")

    # --- Direction ---
    lines.append("\n## 4. Anomaly Direction\n")
    if total_anomalies > 0:
        above = anomalous_trades.filter(col("price") > col("anomaly_upper")).count()
        below = anomalous_trades.filter(col("price") < col("anomaly_lower")).count()
        lines.append("| Direction | Count | % of Anomalies |")
        lines.append("|-----------|-------|----------------|")
        lines.append(f"| Price ABOVE upper bound | {above} | {above / total_anomalies * 100:.1f}% |")
        lines.append(f"| Price BELOW lower bound | {below} | {below / total_anomalies * 100:.1f}% |")
    else:
        lines.append("N/A — no anomalies detected.")

    # --- Production context ---
    lines.append("\n## 5. Production Context\n")
    lines.append("In production, anomalous trades would trigger real-time alerts via:")
    lines.append("- Kafka topic for downstream alert consumers")
    lines.append("- PagerDuty/Slack integration for on-call engineers")
    lines.append("- Dashboard highlighting anomalous windows in red")
    lines.append(
        "\nAt TCS, I implemented similar anomaly detection on billing data — flagging "
        "transactions that deviated >3% from historical patterns. This reduced revenue "
        "leakage by catching billing errors within minutes instead of end-of-month reconciliation."
    )

    lines.append("\n---")
    lines.append(f"\n*Report generated by `anomaly_detector.py` at {datetime.now().isoformat()}*")

    spark.stop()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\nReport saved to: {REPORT_PATH}")
    print(
        f"\nTotal anomalies found: {total_anomalies:,} out of {total_trades:,} trades "
        f"({anomaly_rate:.2f}%)"
    )


if __name__ == "__main__":
    find_anomalies()
