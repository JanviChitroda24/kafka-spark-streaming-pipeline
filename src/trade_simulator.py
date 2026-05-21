"""
Stock Trade Event Simulator (Synthetic Mode)

Generates realistic trade events for 25 tickers and publishes them to Kafka.
Uses the same ticker universe as Project 01 / Project 02 so the streaming pipeline
stays consistent across the portfolio.

Why a simulator exists:
- Markets are only open 9:30 AM – 4:00 PM ET — you can't demo at midnight
- No API key or rate limits required
- Reproducible load testing (~100 events/sec on demand)
- Downstream Spark doesn't care whether data is real or synthetic — same topic, same schema
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    TICKERS,
    BASE_PRICES,
)

# Volume tiers mimic real market behavior: mega-caps trade far more often than mid-caps.
# Used to weight random ticker selection — AAPL appears ~3x more often than a low-tier name.
VOLUME_PROFILES = {
    "high": ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"],
    "medium": ["GOOGL", "JPM", "V", "MA", "BAC", "AVGO"],
    "low": [
        "JNJ", "WMT", "PG", "HD", "CVX", "MRK", "ABBV",
        "PEP", "KO", "COST", "TMO", "LLY", "BRK-B",
    ],
}


def get_volume_weight(ticker: str) -> float:
    """Return selection weight so high-volume tickers dominate the event stream."""
    if ticker in VOLUME_PROFILES["high"]:
        return 3.0
    if ticker in VOLUME_PROFILES["medium"]:
        return 1.5
    return 1.0


def generate_trade(ticker: str, current_prices: dict) -> dict:
    """
    Generate one synthetic trade event.

    Price uses a random walk (Gaussian noise) so consecutive trades for the
    same ticker look correlated — not independent random prices.
    """
    # ~0.1% std dev per tick — small enough to look realistic, big enough to move VWAP
    price_change_pct = random.gauss(0, 0.001)
    new_price = round(current_prices[ticker] * (1 + price_change_pct), 2)
    current_prices[ticker] = new_price  # mutate in place — prices drift over the session

    # Trade size distribution: retail odd lots, standard round lots, institutional blocks
    trade_type = random.choices(
        ["odd_lot", "round_lot", "block"],
        weights=[0.5, 0.4, 0.1],
    )[0]

    if trade_type == "odd_lot":
        quantity = random.randint(1, 99)
    elif trade_type == "round_lot":
        quantity = random.choice([100, 200, 300, 500])
    else:
        quantity = random.choice([1000, 2000, 5000, 10000])

    # Bid-ask spread in basis points — liquid names have tighter spreads
    spread_bps = (
        random.uniform(1, 5)
        if ticker in VOLUME_PROFILES["high"]
        else random.uniform(3, 15)
    )
    spread = new_price * (spread_bps / 10000)

    return {
        "trade_id": str(uuid.uuid4()),       # unique event ID — idempotency / dedup later
        "ticker": ticker,
        "price": new_price,
        "quantity": quantity,
        "side": random.choice(["BUY", "SELL"]),
        "trade_type": trade_type,
        "bid_price": round(new_price - spread / 2, 2),
        "ask_price": round(new_price + spread / 2, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),  # ISO 8601 for Spark parsing
        "exchange": random.choice(["NYSE", "NASDAQ", "ARCA"]),
        "source": "simulator",               # lets downstream filter or audit data origin
    }


def create_producer() -> KafkaProducer:
    """
    Shared Kafka producer factory (also used by finnhub_producer.py pattern).

    acks='all'  → wait for all in-sync replicas before confirming (durability)
    retries=3   → transient network blips don't lose events
    key=ticker  → Kafka routes all AAPL events to the same partition (ordering per symbol)
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def run_simulator(events_per_second: int = 100, duration_seconds: int = 300) -> None:
    """
    Publish synthetic trades at a target rate for a fixed duration.

    Rate limiting works in 1-second batches: send N events, then sleep the remainder
    of the second. Simpler than per-event sleep and easier to tune.
    """
    producer = create_producer()
    current_prices = dict(BASE_PRICES)  # copy — don't mutate the config module dict

    # Build weighted ticker pool: high-volume names appear more often in random.choice()
    weighted_tickers: list[str] = []
    for ticker in TICKERS:
        weight = get_volume_weight(ticker)
        weighted_tickers.extend([ticker] * int(weight * 10))

    print(f"[SIMULATOR] Starting: {events_per_second} eps, {duration_seconds}s duration")
    total_sent = 0
    start_time = time.time()
    batch_start = time.time()
    batch_count = 0

    try:
        while True:
            elapsed = time.time() - start_time
            if duration_seconds > 0 and elapsed >= duration_seconds:
                break

            ticker = random.choice(weighted_tickers)
            trade = generate_trade(ticker, current_prices)

            # key=ticker ensures partition affinity — critical for ordered per-symbol processing
            producer.send(KAFKA_TOPIC, key=ticker, value=trade)
            total_sent += 1
            batch_count += 1

            # Throttle: after each batch of events_per_second, wait out the rest of the second
            if batch_count >= events_per_second:
                batch_elapsed = time.time() - batch_start
                if batch_elapsed < 1.0:
                    time.sleep(1.0 - batch_elapsed)
                batch_start = time.time()
                batch_count = 0

            if total_sent % (events_per_second * 10) == 0:
                rate = total_sent / (time.time() - start_time)
                print(f"  Sent {total_sent:,} events ({rate:.0f}/sec)")

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()   # block until all buffered messages are delivered
        producer.close()
        elapsed = time.time() - start_time
        print(
            f"Done. {total_sent:,} events in {elapsed:.1f}s "
            f"({total_sent / max(elapsed, 1):.0f}/sec)"
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Synthetic stock trade generator")
    parser.add_argument("--eps", type=int, default=100, help="Target events per second")
    parser.add_argument("--duration", type=int, default=300, help="Run duration in seconds (0 = infinite)")
    args = parser.parse_args()
    run_simulator(args.eps, args.duration)
