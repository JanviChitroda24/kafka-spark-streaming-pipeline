"""
Data Quality Checks — 7 validation checks on streaming VWAP output.

Run this AFTER stream_processor.py to validate Delta table integrity.
In production, this runs between processing and Snowflake loading.
Dagster will call this and block the load if any check fails (exit code 1).

Output (overwrite each run):
  docs/data_quality_report.md

Run:
  cd src
  python data_quality_checks.py
"""

import os
from datetime import datetime

from config import DELTA_VWAP_1MIN, DELTA_VWAP_5MIN, TICKERS
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "data_quality_report.md")


def run_checks() -> None:
    """Run 7 DQ checks on vwap_1min and vwap_5min; write report; exit 1 if any fail."""
    lines: list[str] = []
    lines.append("# Data Quality Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("\n---\n")

    missing = [
        name
        for name, path in [("vwap_1min", DELTA_VWAP_1MIN), ("vwap_5min", DELTA_VWAP_5MIN)]
        if not os.path.exists(path)
    ]
    if missing:
        lines.append(f"❌ **Missing Delta tables:** {', '.join(missing)}")
        lines.append("\nRun producer + `stream_processor.py` first.")
        _write_report(lines)
        for line in lines:
            print(line)
        print(f"\nReport saved to: {REPORT_PATH}")
        raise SystemExit(1)

    spark = get_spark_session("DataQuality")
    spark.sparkContext.setLogLevel("ERROR")

    # Pandas is fine here — VWAP tables are small (hundreds of rows, not millions)
    df1 = spark.read.format("delta").load(DELTA_VWAP_1MIN).toPandas()
    df5 = spark.read.format("delta").load(DELTA_VWAP_5MIN).toPandas()

    lines.append(
        f"**Tables checked:** vwap_1min ({len(df1)} rows), vwap_5min ({len(df5)} rows)"
    )
    lines.append("\n---\n")
    lines.append("## Validation Results\n")
    lines.append("| # | Check | Table | Result | Details |")
    lines.append("|---|-------|-------|--------|---------|")

    results: list[bool] = []

    # Check 1: No null VWAPs — null aggregate means broken window logic
    null_count_1 = int(df1["vwap"].isna().sum())
    null_count_5 = int(df5["vwap"].isna().sum())
    total_nulls = null_count_1 + null_count_5
    passed = total_nulls == 0
    results.append(passed)
    lines.append(
        f"| 1 | No null VWAPs | Both | {'✅ PASS' if passed else '❌ FAIL'} | "
        f"1min: {null_count_1} nulls, 5min: {null_count_5} nulls |"
    )

    # Check 2: VWAP in realistic range — catches divide-by-zero or bad price data
    out_of_range_1 = int(((df1["vwap"] < 1) | (df1["vwap"] > 2000)).sum())
    out_of_range_5 = int(((df5["vwap"] < 1) | (df5["vwap"] > 2000)).sum())
    total_oor = out_of_range_1 + out_of_range_5
    passed = total_oor == 0
    results.append(passed)
    lines.append(
        f"| 2 | VWAP in range ($1-$2000) | Both | {'✅ PASS' if passed else '❌ FAIL'} | "
        f"{total_oor} out-of-range rows |"
    )

    # Check 3: Buy pressure is a percentage — must be 0–100
    bp1 = df1["buy_pressure"].astype(float)
    bp5 = df5["buy_pressure"].astype(float)
    bad_bp_1 = int(((bp1 < 0) | (bp1 > 100)).sum())
    bad_bp_5 = int(((bp5 < 0) | (bp5 > 100)).sum())
    total_bad_bp = bad_bp_1 + bad_bp_5
    passed = total_bad_bp == 0
    results.append(passed)
    lines.append(
        f"| 3 | Buy pressure 0-100% | Both | {'✅ PASS' if passed else '❌ FAIL'} | "
        f"{total_bad_bp} invalid rows |"
    )

    # Check 4: Volume always positive — empty windows shouldn't land in VWAP table
    neg_vol_1 = int((df1["total_volume"] <= 0).sum())
    neg_vol_5 = int((df5["total_volume"] <= 0).sum())
    total_neg = neg_vol_1 + neg_vol_5
    passed = total_neg == 0
    results.append(passed)
    lines.append(
        f"| 4 | Volume positive | Both | {'✅ PASS' if passed else '❌ FAIL'} | "
        f"{total_neg} non-positive rows |"
    )

    # Check 5: All 25 tickers present — completeness check on 5-min table
    tickers_in_data = set(df5["ticker"].unique())
    expected_tickers = set(TICKERS)
    missing_tickers = expected_tickers - tickers_in_data
    passed = len(missing_tickers) == 0
    results.append(passed)
    detail = "All 25 present" if passed else f"Missing: {', '.join(sorted(missing_tickers))}"
    lines.append(
        f"| 5 | All 25 tickers present | 5min | {'✅ PASS' if passed else '❌ FAIL'} | {detail} |"
    )

    # Check 6: 1-min windows span exactly 60 seconds — window config sanity check
    df1 = df1.copy()
    df1["window_start"] = df1["window_start"].astype("datetime64[ns]")
    df1["window_end"] = df1["window_end"].astype("datetime64[ns]")
    df1["duration_sec"] = (df1["window_end"] - df1["window_start"]).dt.total_seconds()
    bad_duration = int((df1["duration_sec"] != 60).sum())
    passed = bad_duration == 0
    results.append(passed)
    lines.append(
        f"| 6 | Window duration = 60s | 1min | {'✅ PASS' if passed else '❌ FAIL'} | "
        f"{bad_duration} incorrect windows |"
    )

    # Check 7: High >= Low — basic OHLC integrity on aggregated prices
    bad_hl_1 = int((df1["high_price"] < df1["low_price"]).sum())
    bad_hl_5 = int((df5["high_price"] < df5["low_price"]).sum())
    total_bad_hl = bad_hl_1 + bad_hl_5
    passed = total_bad_hl == 0
    results.append(passed)
    lines.append(
        f"| 7 | High >= Low | Both | {'✅ PASS' if passed else '❌ FAIL'} | "
        f"{total_bad_hl} violations |"
    )

    # Summary
    passed_count = sum(results)
    total_count = len(results)
    lines.append("\n## Summary\n")
    lines.append(f"**{passed_count}/{total_count} checks passed.**\n")

    if passed_count == total_count:
        lines.append(
            "✅ **ALL CHECKS PASSED — Data quality verified. Safe to load to Snowflake.**"
        )
    else:
        failed_nums = [i + 1 for i, p in enumerate(results) if not p]
        lines.append(
            f"❌ **{total_count - passed_count} CHECK(S) FAILED — "
            f"Review checks {failed_nums} before loading.**"
        )

    lines.append("\n---")
    lines.append(
        f"\n*Report generated by `data_quality_checks.py` at {datetime.now().isoformat()}*"
    )

    spark.stop()

    _write_report(lines)
    for line in lines:
        print(line)
    print(f"\nReport saved to: {REPORT_PATH}")

    # Non-zero exit for Dagster / CI — block downstream Snowflake load on failure
    if passed_count < total_count:
        raise SystemExit(1)


def _write_report(lines: list[str]) -> None:
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    run_checks()
