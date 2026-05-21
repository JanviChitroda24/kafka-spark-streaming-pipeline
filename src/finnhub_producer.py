"""
Finnhub WebSocket → Kafka Bridge (Real Market Data Mode)

Connects to Finnhub's free WebSocket feed for live US stock trades, transforms
each message into our unified trade schema, and publishes to the same Kafka
topic as the simulator.

Why a separate file from the simulator:
- Different I/O model: WebSocket callbacks vs a synchronous loop
- Keeps finnhub-specific parsing isolated — swap vendors without touching simulator
- producer.py picks which one to run; Spark never sees the difference

Free tier: 50 symbols, real-time US trades, no credit card.
Sign up at https://finnhub.io/ and set FINNHUB_API_KEY in your environment.
"""

import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
import websocket

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    FINNHUB_API_KEY,
    FINNHUB_WS_URL,
    TICKERS,
)


def create_producer() -> KafkaProducer:
    """Same producer settings as simulator — one topic, one contract, two sources."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


# Module-level state — WebSocket callbacks don't receive custom context objects easily
producer = create_producer()
total_sent = 0
start_time = time.time()


def on_message(ws, message: str) -> None:
    """
    Handle each WebSocket frame from Finnhub.

    Finnhub sends batches: {"type": "trade", "data": [{s, p, v, t}, ...]}
    We flatten each trade in the batch into our schema and publish individually.
    """
    global total_sent

    data = json.loads(message)

    # Ignore heartbeats, subscription confirmations, and other non-trade messages
    if data.get("type") != "trade":
        return

    for trade in data.get("data", []):
        ticker = trade.get("s", "")
        if ticker not in TICKERS:
            continue  # Finnhub may send symbols outside our 25-ticker universe

        # Map Finnhub fields → unified schema (must match simulator output shape)
        event = {
            "trade_id": str(uuid.uuid4()),
            "ticker": ticker,
            "price": round(trade.get("p", 0.0), 2),
            "quantity": trade.get("v", 0),
            "side": "UNKNOWN",  # Finnhub free tier does not expose aggressor side
            "trade_type": "round_lot" if trade.get("v", 0) >= 100 else "odd_lot",
            "bid_price": None,  # not available on free tier — nullable in schema
            "ask_price": None,
            "timestamp": datetime.fromtimestamp(
                trade.get("t", 0) / 1000, tz=timezone.utc
            ).isoformat(),  # Finnhub timestamp is milliseconds since epoch
            "exchange": "FINNHUB",
            "source": "finnhub_live",  # downstream can distinguish real vs simulated
        }

        producer.send(KAFKA_TOPIC, key=ticker, value=event)
        total_sent += 1

        if total_sent % 100 == 0:
            elapsed = time.time() - start_time
            print(
                f"  [FINNHUB] Sent {total_sent:,} real trades "
                f"({total_sent / max(elapsed, 1):.1f}/sec)"
            )


def on_error(ws, error) -> None:
    print(f"[FINNHUB] WebSocket error: {error}")


def on_close(ws, close_status, close_msg) -> None:
    print(f"[FINNHUB] Connection closed: {close_status} {close_msg}")
    producer.flush()
    producer.close()


def on_open(ws) -> None:
    """Subscribe to all 25 tickers as soon as the WebSocket handshake completes."""
    print(f"[FINNHUB] Connected. Subscribing to {len(TICKERS)} tickers...")
    for ticker in TICKERS:
        ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
    print(
        "[FINNHUB] Subscribed. Waiting for trades "
        "(market must be open 9:30–4:00 ET, Mon–Fri)..."
    )


def run_finnhub_producer() -> None:
    """Connect and block until the WebSocket closes or errors."""
    if not FINNHUB_API_KEY:
        print("ERROR: Set FINNHUB_API_KEY environment variable.")
        print("Get a free key at https://finnhub.io/")
        return

    url = FINNHUB_WS_URL.format(key=FINNHUB_API_KEY)
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()  # blocking — runs until disconnect; use Ctrl+C to stop


if __name__ == "__main__":
    run_finnhub_producer()
