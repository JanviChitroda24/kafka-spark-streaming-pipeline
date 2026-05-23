"""
Streaming Pipeline Analysis — generates insights from VWAP data.

Replaces a Databricks notebook with a local script that produces a markdown
report with the same analysis:
  1. VWAP summary by ticker (5-min windows)
  2. Volume leaders
  3. Buy vs sell pressure
  4. Liquidity analysis (bid-ask spread)
  5. Price volatility (high-low range)
  6. Data source breakdown

Output (overwrite each run):
  docs/streaming_analysis_report.md

Run:
  cd src
  python streaming_analysis.py

Prerequisite: Delta tables populated (producer + stream_processor).
"""

import os
from datetime import datetime

import pandas as pd

from config import DELTA_RAW_TRADES, DELTA_VWAP_1MIN, DELTA_VWAP_5MIN
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "streaming_analysis_report.md")


def _to_markdown(df: pd.DataFrame) -> str:
    """Convert DataFrame to markdown table (requires tabulate from requirements.txt)."""
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def run_analysis() -> None:
    lines: list[str] = []
    lines.append("# Streaming Pipeline Analysis Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("\n---\n")

    # Verify Delta tables exist before starting Spark
    missing = [
        name
        for name, path in [
            ("vwap_1min", DELTA_VWAP_1MIN),
            ("vwap_5min", DELTA_VWAP_5MIN),
            ("raw_trades", DELTA_RAW_TRADES),
        ]
        if not os.path.exists(path)
    ]
    if missing:
        lines.append(f"❌ **Missing Delta tables:** {', '.join(missing)}")
        lines.append("\nRun producer + `stream_processor.py` first.")
        _write_report(lines)
        print("\n".join(lines))
        print(f"\nReport saved to: {REPORT_PATH}")
        return

    spark = get_spark_session("Analysis")
    spark.sparkContext.setLogLevel("ERROR")

    # Load Delta → pandas for lightweight aggregations and markdown tables
    vwap_1min = spark.read.format("delta").load(DELTA_VWAP_1MIN).toPandas()
    vwap_5min = spark.read.format("delta").load(DELTA_VWAP_5MIN).toPandas()
    raw = spark.read.format("delta").load(DELTA_RAW_TRADES).toPandas()

    lines.append(
        f"**Data Summary:** {len(raw):,} raw trades, "
        f"{len(vwap_1min)} one-minute windows, {len(vwap_5min)} five-minute windows"
    )
    lines.append(f"\n**Tickers tracked:** {raw['ticker'].nunique()}")
    lines.append(f"\n**Sources:** {', '.join(raw['source'].unique())}")
    lines.append("\n---\n")

    # --- 1. VWAP Summary by Ticker (5-min) ---
    lines.append("## 1. VWAP Summary by Ticker (5-Minute Windows)\n")
    summary = vwap_5min[["ticker", "vwap", "total_volume", "trade_count", "buy_pressure"]].copy()
    summary["vwap"] = summary["vwap"].round(2)
    summary["buy_pressure"] = summary["buy_pressure"].astype(float).round(1)
    summary = summary.sort_values("total_volume", ascending=False)
    lines.append(_to_markdown(summary))

    # --- 2. Volume Leaders ---
    lines.append("\n\n## 2. Volume Leaders\n")
    lines.append("Top 5 tickers by total volume traded:\n")
    top5 = summary.nlargest(5, "total_volume")[["ticker", "total_volume", "trade_count", "vwap"]]
    lines.append(_to_markdown(top5))

    bottom5 = summary.nsmallest(5, "total_volume")[["ticker", "total_volume", "trade_count", "vwap"]]
    lines.append("\n\nBottom 5 tickers by volume:\n")
    lines.append(_to_markdown(bottom5))

    # --- 3. Buy vs Sell Pressure ---
    lines.append("\n\n## 3. Buy vs Sell Pressure\n")
    lines.append("Buy pressure > 55% suggests bullish sentiment, < 45% suggests bearish.\n")
    pressure = vwap_5min[["ticker", "buy_pressure", "buy_volume", "sell_volume"]].copy()
    pressure["buy_pressure"] = pressure["buy_pressure"].astype(float).round(1)
    pressure = pressure.sort_values("buy_pressure", ascending=False)

    bullish = pressure[pressure["buy_pressure"] > 55]
    bearish = pressure[pressure["buy_pressure"] < 45]

    if len(bullish) > 0:
        lines.append("**Bullish (buy pressure > 55%):**\n")
        lines.append(_to_markdown(bullish[["ticker", "buy_pressure"]]))
    if len(bearish) > 0:
        lines.append("\n\n**Bearish (buy pressure < 45%):**\n")
        lines.append(_to_markdown(bearish[["ticker", "buy_pressure"]]))

    # --- 4. Liquidity Analysis (Spread) ---
    lines.append("\n\n## 4. Liquidity Analysis (Bid-Ask Spread)\n")
    lines.append("Tighter spread = more liquid. Spread in dollars; bps = basis points of VWAP.\n")
    liquidity = vwap_5min[["ticker", "avg_spread", "vwap"]].copy()
    liquidity["avg_spread"] = liquidity["avg_spread"].astype(float).round(4)
    liquidity["spread_bps"] = ((liquidity["avg_spread"] / liquidity["vwap"]) * 10000).round(1)
    liquidity = liquidity.sort_values("spread_bps")

    lines.append("**Most liquid (tightest spread):**\n")
    lines.append(_to_markdown(liquidity.head(5)[["ticker", "avg_spread", "spread_bps"]]))
    lines.append("\n\n**Least liquid (widest spread):**\n")
    lines.append(_to_markdown(liquidity.tail(5)[["ticker", "avg_spread", "spread_bps"]]))

    # --- 5. Price Volatility ---
    lines.append("\n\n## 5. Price Range (Volatility Proxy)\n")
    lines.append("High-Low range as percentage of VWAP — wider range = more volatile.\n")
    vol = vwap_5min[["ticker", "low_price", "high_price", "vwap"]].copy()
    vol["range_pct"] = (((vol["high_price"] - vol["low_price"]) / vol["vwap"]) * 100).round(2)
    vol = vol.sort_values("range_pct", ascending=False)

    lines.append("**Most volatile:**\n")
    lines.append(_to_markdown(vol.head(5)[["ticker", "low_price", "high_price", "vwap", "range_pct"]]))
    lines.append("\n\n**Least volatile:**\n")
    lines.append(_to_markdown(vol.tail(5)[["ticker", "low_price", "high_price", "vwap", "range_pct"]]))

    # --- 6. Data Source Breakdown ---
    lines.append("\n\n## 6. Data Source Breakdown\n")
    source_counts = raw.groupby("source").agg(
        trades=("trade_id", "count"),
        tickers=("ticker", "nunique"),
    ).reset_index()
    lines.append(_to_markdown(source_counts))

    lines.append("\n\n---")
    lines.append(f"\n*Report generated by `streaming_analysis.py` at {datetime.now().isoformat()}*")

    spark.stop()

    _write_report(lines)
    print(f"Report saved to: {REPORT_PATH}")


def _write_report(lines: list[str]) -> None:
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    run_analysis()
