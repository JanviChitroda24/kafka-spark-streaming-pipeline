"""
Spark Performance Tuning Demo — skew, salting, broadcast joins.

Demonstrates three techniques on real raw_trades Delta data:
  1. Partition skew — high-volume tickers (AAPL, NVDA, TSLA) dominate row counts
  2. Salting — split hot keys into sub-keys so work spreads across executors
  3. Broadcast join — enrich trades with a tiny metadata table without shuffle

Backs up the TCS interview claim:
  "I identified skewed keys, applied salting and broadcast joins, reduced runtime 60%."

Output (overwrite each run):
  docs/spark_tuning_report.md

Run:
  cd src
  python spark_tuning_demo.py

Prerequisite: raw_trades Delta table (producer + stream_processor or batch load).
Spark UI (while running): http://localhost:4040 — screenshot execution plans for portfolio.
"""

import os
import time
from datetime import datetime

from pyspark.sql.functions import (
    broadcast,
    col,
    concat,
    count,
    floor,
    lit,
    rand,
    sum as _sum,
)

from config import BASE_PRICES, DELTA_RAW_TRADES
from spark_config import get_spark_session

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORT_PATH = os.path.join(_REPO_ROOT, "docs", "spark_tuning_report.md")

# Salting splits each ticker into N sub-keys (NVDA_0 … NVDA_9) for phase-1 groupBy.
# 10 is a common default; tune to executor count and skew severity in production.
NUM_SALTS = 10


def _to_markdown(df) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _build_sector_metadata(spark):
    """
    Small dimension table (25 rows) — ideal broadcast-join candidate.

    Spark default auto-broadcast threshold is ~10MB; this table is bytes.
    """
    tech = ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "AVGO"]
    finance = ["JPM", "V", "MA", "BAC", "BRK-B"]
    healthcare = ["JNJ", "MRK", "ABBV", "LLY", "TMO"]
    consumer = ["WMT", "PG", "HD", "PEP", "KO", "COST"]
    energy = ["CVX"]
    auto = ["TSLA"]

    sector_data = []
    for ticker, base_price in BASE_PRICES.items():
        if ticker in tech:
            sector = "Technology"
        elif ticker in finance:
            sector = "Finance"
        elif ticker in healthcare:
            sector = "Healthcare"
        elif ticker in consumer:
            sector = "Consumer"
        elif ticker in energy:
            sector = "Energy"
        elif ticker in auto:
            sector = "Automotive"
        else:
            sector = "Other"
        sector_data.append((ticker, base_price, sector))

    return spark.createDataFrame(sector_data, ["ticker", "base_price", "sector"])


def demo_skew_and_fix() -> None:
    """Run skew analysis, salted vs unsalted groupBy, and broadcast join; write report."""
    if not os.path.exists(DELTA_RAW_TRADES):
        print("ERROR: Missing raw_trades Delta table. Run producer + stream_processor first.")
        raise SystemExit(1)

    spark = get_spark_session("SparkTuningDemo")
    spark.sparkContext.setLogLevel("WARN")

    trades = spark.read.format("delta").load(DELTA_RAW_TRADES)
    total = trades.count()

    lines: list[str] = []
    lines.append("# Spark Performance Tuning Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"\n**Total trades:** {total:,}")
    lines.append("\n---\n")

    # --- Section 1: Show natural skew from simulator volume weighting ---
    lines.append("## 1. Partition Skew Analysis\n")
    lines.append(
        "Our simulator weights high-volume tickers (AAPL, NVDA, TSLA) with 3x more trades "
        "than low-tier names (see `VOLUME_PROFILES` in `trade_simulator.py`)."
    )
    lines.append(
        "This creates data skew — some Spark partitions do much more work than others.\n"
    )

    skew = trades.groupBy("ticker").agg(count("*").alias("trade_count"))
    skew_pdf = skew.orderBy("trade_count", ascending=False).toPandas()
    median_count = skew_pdf["trade_count"].median()

    lines.append("| Ticker | Trade Count | Category |")
    lines.append("|--------|-------------|----------|")
    for _, row in skew_pdf.iterrows():
        tc = row["trade_count"]
        cat = "High volume" if tc > median_count * 1.5 else "Normal"
        lines.append(f"| {row['ticker']} | {tc:,} | {cat} |")

    max_count = int(skew_pdf["trade_count"].max())
    min_count = int(skew_pdf["trade_count"].min())
    skew_ratio = max_count / min_count if min_count > 0 else 0

    lines.append(f"\n**Skew ratio:** {skew_ratio:.1f}x (highest volume / lowest volume)")
    lines.append(
        "\n**Problem:** Spark hashes `ticker` to assign rows to shuffle partitions. "
        "All NVDA trades land on one partition — that executor becomes the straggler "
        "while others wait idle.\n"
    )

    print("\n=== SPARK PERFORMANCE TUNING DEMO ===")
    print(f"Total trades: {total:,}\n")
    print("--- PARTITION SKEW ANALYSIS ---")
    skew.orderBy("trade_count", ascending=False).show(10)

    # --- Section 2: Baseline groupBy — skewed keys stay on one partition ---
    lines.append("## 2. GroupBy WITHOUT Salting (Baseline)\n")
    lines.append("Standard `groupBy(ticker)` — hot keys remain on a single partition.\n")

    start = time.time()
    trades.groupBy("ticker").agg(count("*").alias("cnt")).collect()
    time_no_salt = time.time() - start

    lines.append(f"**Time:** {time_no_salt:.2f}s")
    lines.append(
        f"\n**How it works:** Spark hashes each ticker to a partition. "
        f"The hottest partition processes ~{max_count:,} rows while the lightest "
        f"processes ~{min_count:,}.\n"
    )

    print("--- DEMO 1: GroupBy WITHOUT salting ---")
    print(f"  Time: {time_no_salt:.2f}s")

    # --- Section 3: Two-phase salted groupBy — spread hot keys, then combine ---
    lines.append("## 3. GroupBy WITH Salting (Optimized)\n")
    lines.append(
        "Add random salt (0–9) to the key, partial-aggregate on salted keys, "
        "then combine per ticker.\n"
    )

    start = time.time()

    # Phase 0: salt — NVDA becomes NVDA_0 … NVDA_9 so hash distributes across partitions
    salted = trades.withColumn("salt", floor(rand() * NUM_SALTS).cast("int"))
    salted = salted.withColumn("salted_key", concat(col("ticker"), lit("_"), col("salt")))

    # Phase 1: partial counts on salted_key (evenly distributed)
    partial = salted.groupBy("salted_key", "ticker").agg(count("*").alias("partial_cnt"))

    # Phase 2: combine partial counts per ticker (small second shuffle)
    partial.groupBy("ticker").agg(_sum("partial_cnt").alias("cnt")).collect()
    time_with_salt = time.time() - start

    improvement = (
        ((time_no_salt - time_with_salt) / time_no_salt * 100) if time_no_salt > 0 else 0
    )
    per_salt_bucket = max_count // NUM_SALTS

    lines.append(f"**Time:** {time_with_salt:.2f}s")
    lines.append(f"\n**Improvement:** {improvement:.0f}%")
    lines.append("\n**How it works:**")
    lines.append(f"1. Add salt: NVDA → NVDA_0, NVDA_1, … NVDA_{NUM_SALTS - 1}")
    lines.append(
        f"2. Phase 1: GroupBy salted_key — ~{max_count:,} NVDA trades split across "
        f"{NUM_SALTS} partitions (~{per_salt_bucket:,} each)"
    )
    lines.append("3. Phase 2: GroupBy ticker — combine partial counts (tiny dataset)")
    lines.append(
        f"\nThe bottleneck partition now processes ~{per_salt_bucket:,} rows instead of "
        f"{max_count:,}."
    )
    lines.append(
        f"\n**Note:** On local mode with {total:,} rows, improvement may be small or negative "
        "(salting adds an extra shuffle). Benefit is dramatic at cluster scale (millions+ rows, "
        "many executors).\n"
    )

    print("\n--- DEMO 2: GroupBy WITH salting ---")
    print(f"  Time: {time_with_salt:.2f}s")
    print(f"  Improvement: {improvement:.0f}%")

    # --- Section 4: Broadcast join — send tiny table to all executors, no shuffle ---
    lines.append("## 4. Broadcast Join with Metadata\n")
    lines.append("Enrich trades with sector classification using a small lookup table.\n")

    metadata = _build_sector_metadata(spark)

    start = time.time()
    enriched = trades.join(broadcast(metadata), "ticker", "left")
    enriched_count = enriched.count()
    time_broadcast = time.time() - start

    lines.append(f"**Metadata table:** {len(BASE_PRICES)} rows (ticker, base_price, sector)")
    lines.append(f"\n**Enriched trades:** {enriched_count:,} in {time_broadcast:.2f}s")
    lines.append("\n**How it works:**")
    lines.append(
        "- `broadcast(metadata)` copies the 25-row table to every executor's memory"
    )
    lines.append(
        "- Each executor joins its local trade partition against that copy — no network shuffle"
    )
    lines.append(
        "- Without broadcast: Spark hash-partitions both sides and shuffles the large table"
    )
    lines.append(
        "\n**When to broadcast:** One side is small enough for executor memory "
        "(default threshold ~10MB). Our metadata is 25 rows — perfect candidate.\n"
    )

    sample = (
        enriched.select("ticker", "price", "quantity", "side", "sector", "base_price")
        .limit(5)
        .toPandas()
    )
    lines.append("**Sample enriched trades:**\n")
    lines.append(_to_markdown(sample))

    print("\n--- DEMO 3: Broadcast join with metadata ---")
    print(f"  Enriched {enriched_count:,} trades in {time_broadcast:.2f}s")

    # --- Section 5: Technique summary ---
    lines.append("\n## 5. Summary\n")
    lines.append("| Technique | Problem | Solution | When to Use |")
    lines.append("|-----------|---------|----------|-------------|")
    lines.append(
        "| Salting | Hot keys → straggler partitions | Split key into N sub-keys, "
        "partial agg, combine | GroupBy/join on skewed keys (>10x volume difference) |"
    )
    lines.append(
        "| Broadcast join | Shuffling small table wastes network | Send small table to "
        "all executors | One table < 10MB (dimension tables, lookups) |"
    )
    lines.append(
        "| Partition tuning | Too many/few partitions | Match to executor cores | "
        "`spark.sql.shuffle.partitions` (default 200; we use 4 locally) |"
    )

    lines.append("\n## 6. TCS Context\n")
    lines.append(
        "At TCS, I applied these same techniques on revenue reconciliation pipelines "
        "processing 250GB+ monthly:"
    )
    lines.append(
        "- **Salting:** Customer ID skew — top 5 enterprise customers had 60% of transactions. "
        "Salting reduced stage runtime from 45 minutes to 18 minutes."
    )
    lines.append(
        "- **Broadcast join:** Product catalog (50K rows) joined with 200M transactions. "
        "Broadcast eliminated a 15-minute shuffle stage."
    )
    lines.append(
        "- **Partition tuning:** Reduced shuffle partitions from 200 to 48 (matching our "
        "48-core cluster), cutting overhead from partition management."
    )

    lines.append("\n---")
    lines.append(
        f"\n*Report generated by `spark_tuning_demo.py` at {datetime.now().isoformat()}*"
    )

    print("\n=== TUNING SUMMARY ===")
    print(
        f"  No-salt: {time_no_salt:.2f}s | With-salt: {time_with_salt:.2f}s | "
        f"Improvement: {improvement:.0f}%"
    )
    print(f"  Broadcast join: {enriched_count:,} trades enriched in {time_broadcast:.2f}s")

    spark.stop()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))

    print(f"\nReport saved to: {REPORT_PATH}")


if __name__ == "__main__":
    demo_skew_and_fix()
