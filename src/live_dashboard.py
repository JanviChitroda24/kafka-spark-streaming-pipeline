"""
Live Terminal Dashboard — refreshes every 15 seconds.

Shows:
  - 1-min VWAP for top tickers (latest window)
  - Pipeline health stats (row counts, freshness)
  - Anomaly window bounds (latest window)

Uses the rich library for formatted terminal output.
Screenshot this for README and LinkedIn while producer + processor are running.

Run:
  cd src
  python live_dashboard.py

Prerequisite: Delta tables populated (producer + stream_processor active or recent run).
"""

import time
from datetime import datetime

from pyspark.sql.functions import col, max as spark_max
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table

from config import DELTA_ANOMALY_ALERTS, DELTA_RAW_TRADES, DELTA_VWAP_1MIN
from spark_config import get_spark_session

console = Console()
REFRESH_SECONDS = 15
TOP_VWAP_ROWS = 15
TOP_ANOMALY_ROWS = 5


def build_dashboard() -> Panel:
    """
    Read Delta tables, build three rich tables, return bordered panel.

    Creates and stops a Spark session each call — avoids holding file locks
    on Delta tables while the streaming processor is writing.
    """
    spark = get_spark_session("Dashboard")
    spark.sparkContext.setLogLevel("ERROR")

    # --- VWAP: latest completed 1-min window, top tickers by volume ---
    vwap_table = Table(title="1-Min VWAP (Latest Window)", show_lines=False)
    vwap_table.add_column("Ticker", style="cyan", width=8)
    vwap_table.add_column("VWAP", justify="right", width=10)
    vwap_table.add_column("Volume", justify="right", width=12)
    vwap_table.add_column("Trades", justify="right", width=8)
    vwap_table.add_column("Buy %", justify="right", width=8)

    try:
        vwap = spark.read.format("delta").load(DELTA_VWAP_1MIN)
        latest_window = vwap.agg(spark_max("window_start")).collect()[0][0]
        if latest_window:
            latest = (
                vwap.filter(col("window_start") == latest_window)
                .orderBy("total_volume", ascending=False)
                .limit(TOP_VWAP_ROWS)
                .collect()
            )
            for row in latest:
                vwap_table.add_row(
                    row["ticker"],
                    f"${float(row['vwap']):.2f}",
                    f"{row['total_volume']:,}",
                    str(row["trade_count"]),
                    f"{float(row['buy_pressure']):.1f}%",
                )
        else:
            vwap_table.add_row("—", "No data", "—", "—", "—")
    except Exception:
        vwap_table.add_row("—", "No data", "—", "—", "—")

    # --- Health: row counts + latest event_time (freshness indicator) ---
    stats_table = Table(title="Pipeline Health", show_lines=False)
    stats_table.add_column("Metric", width=20)
    stats_table.add_column("Value", justify="right", width=20)

    try:
        raw = spark.read.format("delta").load(DELTA_RAW_TRADES)
        vwap = spark.read.format("delta").load(DELTA_VWAP_1MIN)

        stats_table.add_row("Raw trades", f"{raw.count():,}")
        stats_table.add_row("VWAP windows", f"{vwap.count():,}")
        stats_table.add_row("Tickers", str(raw.select("ticker").distinct().count()))

        latest_ts = raw.agg(spark_max("event_time")).collect()[0][0]
        stats_table.add_row("Latest trade", str(latest_ts)[:19] if latest_ts else "—")
    except Exception:
        stats_table.add_row("Status", "No data yet")

    stats_table.add_row("Dashboard time", datetime.now().strftime("%H:%M:%S"))
    stats_table.add_row("Author", "Janvi Chitroda")

    # --- Anomalies: latest window bounds for highest-volume tickers ---
    anomaly_table = Table(title="Anomaly Windows", show_lines=False)
    anomaly_table.add_column("Ticker", style="red", width=8)
    anomaly_table.add_column("Avg Price", justify="right", width=10)
    anomaly_table.add_column("Upper", justify="right", width=10)
    anomaly_table.add_column("Lower", justify="right", width=10)

    try:
        alerts = spark.read.format("delta").load(DELTA_ANOMALY_ALERTS)
        latest_alert_window = alerts.agg(spark_max("window_start")).collect()[0][0]
        if latest_alert_window:
            recent = (
                alerts.filter(col("window_start") == latest_alert_window)
                .orderBy("window_volume", ascending=False)
                .limit(TOP_ANOMALY_ROWS)
                .collect()
            )
            for row in recent:
                anomaly_table.add_row(
                    row["ticker"],
                    f"${float(row['window_avg_price']):.2f}",
                    f"${float(row['anomaly_upper']):.2f}",
                    f"${float(row['anomaly_lower']):.2f}",
                )
        else:
            anomaly_table.add_row("—", "No data", "—", "—")
    except Exception:
        anomaly_table.add_row("—", "No data", "—", "—")

    spark.stop()

    return Panel(
        Group(vwap_table, "", stats_table, "", anomaly_table),
        title="[bold blue]Stock Streaming Pipeline Dashboard[/bold blue]",
        border_style="blue",
    )


def run_dashboard(refresh_seconds: int = REFRESH_SECONDS) -> None:
    """Clear screen, rebuild dashboard, sleep — loop until Ctrl+C."""
    print("Starting live dashboard (Ctrl+C to stop)...")
    print(f"Refreshing every {refresh_seconds}s\n")

    try:
        while True:
            console.clear()
            console.print(build_dashboard())
            time.sleep(refresh_seconds)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")


if __name__ == "__main__":
    run_dashboard()
