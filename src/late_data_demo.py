"""
Late Data Handling Demo — key interview artifact for streaming.

Proves watermarks work in practice, not just in theory. Sends 3 phases of
AAPL trades to Kafka while Spark Structured Streaming is running:

  Phase 1: 20 on-time events (~$210)     → establish baseline 1-min window
  Phase 2: 5 late events (5s late)       → WITHIN 10s watermark → should update VWAP
  Phase 3: 5 very late events (60s late) → BEYOND watermark → should be DROPPED

If watermarks work: $999.99 never appears in VWAP tables.
If broken: Phase 3 poison prices leak into aggregates — interview red flag.

Prerequisites:
  1. Clean old Delta/checkpoints from prior runs (see notes)
  2. Terminal 1: python stream_processor.py --debug
  3. Terminal 2: python late_data_demo.py
  4. Verify: python verify_delta.py — no $999.99 in vwap_1min / vwap_5min
"""

import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# Single ticker keeps the demo easy to read in verify_delta output
DEMO_TICKER = "AAPL"
BASE_PRICE = 210.00

# Must match WATERMARK_DELAY in config.py — demo is designed around this value
WATERMARK_SECONDS = 10


def _create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )


def _send_trade(producer: KafkaProducer, trade: dict) -> None:
    producer.send(KAFKA_TOPIC, key=DEMO_TICKER, value=trade)


def run_late_data_demo() -> None:
    producer = _create_producer()

    print("=== LATE DATA DEMO ===")
    print(f"Ticker: {DEMO_TICKER} | Watermark: {WATERMARK_SECONDS} seconds\n")

    # Anchor time for Phase 1 — all "on-time" events cluster around now
    now = datetime.now(timezone.utc)

    # -------------------------------------------------------------------------
    # Phase 1: On-time events — normal prices, sequential timestamps
    # Establishes a 1-minute window with VWAP ~$210 before we test lateness
    # -------------------------------------------------------------------------
    print("Phase 1: 20 on-time events...")
    for i in range(20):
        event_time = now + timedelta(seconds=i * 0.5)
        trade = {
            "trade_id": f"ontime-{i}",
            "ticker": DEMO_TICKER,
            "price": round(BASE_PRICE + random.uniform(-0.5, 0.5), 2),
            "quantity": 100,
            "side": "BUY",
            "trade_type": "round_lot",
            "bid_price": round(BASE_PRICE - 0.05, 2),
            "ask_price": round(BASE_PRICE + 0.05, 2),
            "timestamp": event_time.isoformat(),
            "exchange": "NYSE",
            "source": "late_data_demo",
        }
        _send_trade(producer, trade)

    producer.flush()
    print("  Waiting 15s for Spark to process Phase 1...")
    time.sleep(15)

    # -------------------------------------------------------------------------
    # Phase 2: Late but WITHIN watermark
    # Timestamps point to Phase 1 window (now - 5s effective lateness)
    # Spark should still include these — they arrive before watermark closes window
    # Price $211 + qty 500 makes the VWAP shift visibly upward if included
    # -------------------------------------------------------------------------
    print("\nPhase 2: 5 LATE events (timestamps in Phase 1 window — WITHIN watermark)...")
    for i in range(5):
        trade = {
            "trade_id": f"late-within-{i}",
            "ticker": DEMO_TICKER,
            "price": round(BASE_PRICE + 1.00, 2),  # $211 — easy to spot in output
            "quantity": 500,
            "side": "SELL",
            "trade_type": "round_lot",
            "bid_price": 210.95,
            "ask_price": 211.05,
            # Same event-time window as Phase 1 — simulates 5s processing delay
            "timestamp": (now + timedelta(seconds=i * 0.5)).isoformat(),
            "exchange": "NYSE",
            "source": "late_data_demo",
        }
        _send_trade(producer, trade)

    producer.flush()
    print("  Waiting 15s for Spark to process Phase 2...")
    time.sleep(15)

    # -------------------------------------------------------------------------
    # Phase 3: Very late — BEYOND watermark
    # event_time is 60 seconds BEFORE now — Spark's watermark has advanced past this
    # These should be dropped from windowed aggregations (VWAP, anomaly)
    # $999.99 is deliberately absurd — if it appears, watermark failed
    # -------------------------------------------------------------------------
    print("\nPhase 3: 5 VERY LATE events (60s old event_time — BEYOND watermark)...")
    for i in range(5):
        trade = {
            "trade_id": f"late-beyond-{i}",
            "ticker": DEMO_TICKER,
            "price": 999.99,
            "quantity": 9999,
            "side": "BUY",
            "trade_type": "block",
            "bid_price": 999.90,
            "ask_price": 1000.00,
            "timestamp": (now - timedelta(seconds=60)).isoformat(),
            "exchange": "NYSE",
            "source": "late_data_demo",
        }
        _send_trade(producer, trade)

    producer.flush()
    producer.close()

    print("\n=== Demo complete ===")
    print("Expected results:")
    print("  ✓ Phase 1: VWAP ~$210 in vwap_1min")
    print("  ✓ Phase 2: VWAP shifts toward ~$211 (late-within events counted)")
    print("  ✗ Phase 3: $999.99 must NOT appear in vwap_1min or vwap_5min")
    print("\nRun: python verify_delta.py")
    print("Filter raw_trades for price=999.99 — may exist in bronze layer,")
    print("but VWAP tables prove watermark dropped them from aggregations.")


if __name__ == "__main__":
    run_late_data_demo()
