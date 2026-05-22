"""
Watermark verification — generates a markdown report proving late-data handling.

Run AFTER late_data_demo.py + stream_processor.py:
  cd src
  python verify_watermark.py

Output (overwrite each run):
  docs/watermark_verification_report.md

Why a file report instead of console-only:
- Portfolio evidence you can link in README
- Clean tables for live demos / interviews
- Compare pass/fail across runs without scrolling terminal history
"""

import os
from datetime import datetime

from config import DELTA_RAW_TRADES, DELTA_VWAP_1MIN
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "watermark_verification_report.md")

# Must match WATERMARK_DELAY in config.py
WATERMARK_SECONDS = 10

# AAPL should never exceed this after demo — Phase 3 uses $999.99 canary
AAPL_VWAP_CONTAMINATION_THRESHOLD = 500

# High-priced tickers where VWAP > $500 is normal, not watermark failure
HIGH_PRICE_TICKERS = {"LLY", "COST", "TMO", "MA", "BRK-B", "MSFT", "META"}


def generate_report() -> None:
    spark = get_spark_session("WatermarkVerifier")
    spark.sparkContext.setLogLevel("ERROR")

    lines: list[str] = []
    lines.append("# Watermark Verification Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"\n**Watermark:** {WATERMARK_SECONDS} seconds")
    lines.append("\n**Test:** 3-phase late data demo (on-time → late within → late beyond)")
    lines.append("\n---\n")

    raw = spark.read.format("delta").load(DELTA_RAW_TRADES)
    demo = raw.filter("source = 'late_data_demo'")

    ontime = demo.filter("trade_id LIKE 'ontime%'")
    late_within = demo.filter("trade_id LIKE 'late-within%'")
    late_beyond = demo.filter("trade_id LIKE 'late-beyond%'")

    ontime_count = ontime.count()
    late_within_count = late_within.count()
    late_beyond_count = late_beyond.count()

    # --- Check 1: Demo events landed in bronze raw layer ---
    lines.append("## 1. Late Data Demo Events in Raw Trades\n")
    lines.append("| Phase | Expected | Actual | Price | Quantity | Status |")
    lines.append("|-------|----------|--------|-------|----------|--------|")
    lines.append(
        f"| On-time (Phase 1) | 20 | {ontime_count} | ~$210 | 100 | "
        f"{'✅ PASS' if ontime_count == 20 else '❌ FAIL'} |"
    )
    lines.append(
        f"| Late within watermark (Phase 2) | 5 | {late_within_count} | $211 | 500 | "
        f"{'✅ PASS' if late_within_count == 5 else '❌ FAIL'} |"
    )
    lines.append(
        f"| Late beyond watermark (Phase 3) | 5 | {late_beyond_count} | $999.99 | 9999 | "
        f"{'✅ Stored in raw (expected)' if late_beyond_count == 5 else '❌ UNEXPECTED'} |"
    )
    lines.append(
        "\n> **Note:** Raw trades stores ALL events regardless of watermark. "
        "Watermarks only affect windowed aggregations (VWAP).\n"
    )

    # --- Check 2: AAPL VWAP not poisoned by $999.99 ---
    lines.append("## 2. VWAP Contamination Check (Critical)\n")
    vwap = spark.read.format("delta").load(DELTA_VWAP_1MIN)
    aapl_vwap = vwap.filter("ticker = 'AAPL'").orderBy("window_start")
    aapl_rows = aapl_vwap.select(
        "ticker", "vwap", "total_volume", "trade_count", "window_start", "window_end"
    ).collect()

    max_vwap = 0.0
    lines.append("| Ticker | VWAP | Volume | Trades | Window Start | Window End |")
    lines.append("|--------|------|--------|--------|-------------|------------|")
    for row in aapl_rows:
        vwap_val = round(float(row["vwap"] or 0), 4)
        max_vwap = max(max_vwap, vwap_val)
        lines.append(
            f"| {row['ticker']} | ${vwap_val} | {row['total_volume']:,} | "
            f"{row['trade_count']} | {row['window_start']} | {row['window_end']} |"
        )

    contaminated = max_vwap > AAPL_VWAP_CONTAMINATION_THRESHOLD
    lines.append(f"\n**Max AAPL VWAP: ${round(max_vwap, 4)}**")
    if not contaminated:
        lines.append(
            "\n✅ **PASS — No $999.99 contamination in VWAP. "
            "Watermark correctly dropped late data.**"
        )
    else:
        lines.append(
            "\n❌ **FAIL — VWAP contaminated! Watermark may not be working correctly.**"
        )

    # --- Check 3: No accidental high VWAP on normal tickers ---
    lines.append("\n## 3. Global Contamination Check\n")
    all_vwap = vwap.collect()
    contaminated_tickers = [
        row["ticker"]
        for row in all_vwap
        if row["vwap"]
        and row["vwap"] > AAPL_VWAP_CONTAMINATION_THRESHOLD
        and row["ticker"] not in HIGH_PRICE_TICKERS
    ]
    if not contaminated_tickers:
        lines.append("✅ **PASS — No unexpected VWAP values above $500 in any ticker.**")
    else:
        lines.append(f"❌ **FAIL — Unexpected high VWAP in: {contaminated_tickers}**")

    # --- Summary ---
    lines.append("\n## 4. Summary\n")
    all_pass = (
        ontime_count == 20
        and late_within_count == 5
        and late_beyond_count == 5
        and not contaminated
        and not contaminated_tickers
    )
    if all_pass:
        lines.append("| Test | Result |")
        lines.append("|------|--------|")
        lines.append("| On-time events stored in raw | ✅ PASS |")
        lines.append("| Late-within events stored in raw | ✅ PASS |")
        lines.append("| Late-beyond events stored in raw (expected) | ✅ PASS |")
        lines.append("| VWAP not contaminated by $999.99 | ✅ PASS |")
        lines.append("| No unexpected high VWAP values | ✅ PASS |")
        lines.append("\n**Overall: ✅ ALL TESTS PASSED — Watermarks working correctly.**")
    else:
        lines.append("**Overall: ❌ SOME TESTS FAILED — Review above details.**")

    lines.append("\n---")
    lines.append(f"\n*Report generated by `verify_watermark.py` at {datetime.now().isoformat()}*")

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    report_text = "\n".join(lines)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"\nReport saved to: {REPORT_PATH}")
    print("=" * 50)
    print(report_text)

    spark.stop()


if __name__ == "__main__":
    generate_report()
