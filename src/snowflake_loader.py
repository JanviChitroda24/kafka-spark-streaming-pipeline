# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
Load Delta Lake VWAP tables into Snowflake STREAMING_ANALYTICS schema.

Bridges streaming (local Delta files) → analytics (Snowflake warehouse):
  data/delta/vwap_1min  →  MARKET_DATA_DB.STREAMING_ANALYTICS.VWAP_1MIN
  data/delta/vwap_5min  →  MARKET_DATA_DB.STREAMING_ANALYTICS.VWAP_5MIN

Why not query Delta directly in production?
- Analysts use SQL in Snowflake, not Spark on a laptop
- Same warehouse as Project 02 (dbt) — unified analytics layer
- TRUNCATE + reload gives clean overwrite for dev/demo runs

Prerequisites:
  1. Run SQL in Snowflake UI to create STREAMING_ANALYTICS tables (see notes)
  2. Fill .env in repo root with Snowflake credentials (.gitignore'd)
  3. Delta tables populated from stream_processor.py

Run:
  cd src
  python snowflake_loader.py

Verify:
  SELECT * FROM STREAMING_ANALYTICS.VWAP_1MIN LIMIT 10;
  SELECT COUNT(*) FROM STREAMING_ANALYTICS.VWAP_1MIN;
  SELECT * FROM STREAMING_ANALYTICS.VWAP_5MIN LIMIT 10;
"""

import os
from decimal import Decimal

from dotenv import load_dotenv

# Load .env BEFORE reading config — credentials must be in environment
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_REPO_ROOT, ".env"))

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config import (
    DELTA_VWAP_1MIN,
    DELTA_VWAP_5MIN,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)
from spark_config import get_spark_session

# Delta columns that exist locally but not in Snowflake DDL
DROP_COLUMNS = {"WINDOW_DURATION"}


def _normalize_timestamps_for_snowflake(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Convert timestamp columns to plain strings for Snowflake TIMESTAMP_NTZ.

    write_pandas mishandles nanosecond datetime64 — sending as string
    lets Snowflake parse it cleanly into TIMESTAMP_NTZ.
    """
    for col in pdf.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf[col]):
            # Strip timezone if present, then convert to string
            if getattr(pdf[col].dt, "tz", None) is not None:
                pdf[col] = pdf[col].dt.tz_convert("UTC").dt.tz_localize(None)
            pdf[col] = pdf[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return pdf


def get_snowflake_connection():
    """
    Connect using credentials from .env (via os.getenv after load_dotenv).

    Fails fast with clear message if account/user/password missing.
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT)
    user = os.getenv("SNOWFLAKE_USER", SNOWFLAKE_USER)
    password = os.getenv("SNOWFLAKE_PASSWORD", SNOWFLAKE_PASSWORD)
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE)

    if not all([account, user, password]):
        print("ERROR: Snowflake credentials not set.")
        print("Create a .env file in the repo root with:")
        print("  SNOWFLAKE_ACCOUNT=your_account")
        print("  SNOWFLAKE_USER=your_username")
        print("  SNOWFLAKE_PASSWORD=your_password")
        print("  SNOWFLAKE_WAREHOUSE=MARKET_DATA_WH")
        raise SystemExit(1)

    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=warehouse,
    )


def _delta_to_pandas(spark, delta_path: str) -> pd.DataFrame:
    """Read Delta with Spark, convert to pandas with Snowflake-friendly types."""
    df = spark.read.format("delta").load(delta_path)
    pdf = df.toPandas()
    pdf.columns = [c.upper() for c in pdf.columns]

    for col in list(pdf.columns):
        if col in DROP_COLUMNS:
            pdf = pdf.drop(columns=[col])

    pdf = _normalize_timestamps_for_snowflake(pdf)

    # Spark Decimal → float; Snowflake write_pandas handles float cleanly
    for col in pdf.columns:
        if pdf[col].apply(lambda x: isinstance(x, Decimal)).any():
            pdf[col] = pdf[col].astype(float)

    return pdf


def load_delta_to_snowflake(delta_path: str, table_name: str, spark=None) -> None:
    """
    Read one Delta path, truncate target Snowflake table, bulk load via write_pandas.

    TRUNCATE before load = overwrite semantics for dev. Production would use MERGE
    on (ticker, window_start) for idempotent upserts.
    """
    own_spark = spark is None
    if own_spark:
        spark = get_spark_session("SnowflakeLoader")

    row_count = spark.read.format("delta").load(delta_path).count()
    print(f"\nRead {row_count:,} rows from {delta_path}")

    if row_count == 0:
        print("No data. Skipping.")
        if own_spark:
            spark.stop()
        return

    pdf = _delta_to_pandas(spark, delta_path)

    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_SCHEMA}.{table_name}")
        cursor.close()
        print(f"Truncated {SNOWFLAKE_SCHEMA}.{table_name}")

        _success, _chunks, num_rows, _output = write_pandas(
            conn,
            pdf,
            table_name.upper(),
            auto_create_table=False,
        )
        print(
            f"✓ Loaded {num_rows:,} rows → "
            f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}"
        )
    except Exception as e:
        print(f"✗ Error loading {table_name}: {e}")
        raise
    finally:
        conn.close()

    if own_spark:
        spark.stop()


def load_all() -> None:
    print("=" * 60)
    print("SNOWFLAKE LOADER — Streaming Results")
    print(f"  Target: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    print("=" * 60)

    spark = get_spark_session("SnowflakeLoader")

    load_delta_to_snowflake(DELTA_VWAP_1MIN, "VWAP_1MIN", spark)
    load_delta_to_snowflake(DELTA_VWAP_5MIN, "VWAP_5MIN", spark)

    spark.stop()

    print("\n" + "=" * 60)
    print("Done! Verify in Snowflake:")
    print("  SELECT * FROM STREAMING_ANALYTICS.VWAP_1MIN LIMIT 10;")
    print("  SELECT COUNT(*) FROM STREAMING_ANALYTICS.VWAP_1MIN;")
    print("  SELECT * FROM STREAMING_ANALYTICS.VWAP_5MIN LIMIT 10;")
    print("=" * 60)


if __name__ == "__main__":
    load_all()
