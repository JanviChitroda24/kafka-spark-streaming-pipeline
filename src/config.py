"""
Centralized configuration for the streaming pipeline.
All connection details, paths, and tuning parameters in one place.
Other modules import from here — never hardcode values.
"""

import os

# --- Kafka / Redpanda ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_trades")
KAFKA_PARTITIONS = 3
KAFKA_RETENTION_MS = 3600000  # 1 hour

# --- Finnhub ---
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FINNHUB_WS_URL = "wss://ws.finnhub.io?token={key}"

# --- Spark ---
SPARK_APP_NAME = "StockStreamProcessor"
SPARK_MASTER = "local[*]"
SPARK_SHUFFLE_PARTITIONS = 4  # Low for local mode
SPARK_DRIVER_MEMORY = "2g"
SPARK_TRIGGER_INTERVAL = "10 seconds"
WATERMARK_DELAY = "10 seconds"

# --- Delta Lake ---
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "./data/delta")
DELTA_RAW_TRADES = f"{DELTA_BASE_PATH}/raw_trades"
DELTA_VWAP_1MIN = f"{DELTA_BASE_PATH}/vwap_1min"
DELTA_VWAP_5MIN = f"{DELTA_BASE_PATH}/vwap_5min"
DELTA_ANOMALY_ALERTS = f"{DELTA_BASE_PATH}/anomaly_alerts"
CHECKPOINT_BASE = f"{DELTA_BASE_PATH}/checkpoints"

# --- Snowflake ---
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_DATABASE = "MARKET_DATA_DB"
SNOWFLAKE_SCHEMA = "STREAMING_ANALYTICS"
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "MARKET_DATA_WH")

# --- Tickers (same 25 from Week 1) ---
TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
    "META", "TSLA", "BRK-B", "JPM", "V",
    "JNJ", "WMT", "PG", "MA", "HD",
    "CVX", "MRK", "ABBV", "PEP", "KO",
    "COST", "BAC", "AVGO", "TMO", "LLY"
]

# Approximate base prices for the simulator
BASE_PRICES = {
    "AAPL": 210, "MSFT": 470, "NVDA": 140, "GOOGL": 185, "AMZN": 215,
    "META": 530, "TSLA": 280, "BRK-B": 475, "JPM": 230, "V": 310,
    "JNJ": 165, "WMT": 95, "PG": 170, "MA": 530, "HD": 390,
    "CVX": 155, "MRK": 130, "ABBV": 195, "PEP": 170, "KO": 65,
    "COST": 950, "BAC": 45, "AVGO": 185, "TMO": 575, "LLY": 950
}

# Anomaly detection threshold
ANOMALY_THRESHOLD_PCT = 2.0  # Flag trades >2% away from window VWAP
