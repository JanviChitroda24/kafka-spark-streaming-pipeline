"""
Test Finnhub WebSocket connection using FINNHUB_API_KEY from .env.

Verifies the API key is set and the WebSocket endpoint accepts the connection.
Works even when US markets are closed — we only test connectivity, not live trades.

Prerequisite:
  FINNHUB_API_KEY in repo root .env (see https://finnhub.io/)

Run:
  cd src
  python test_finnhub_connection.py
"""

import os

from dotenv import load_dotenv
import websocket

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_REPO_ROOT, ".env"))

from config import FINNHUB_WS_URL


def test_connection() -> None:
    """Connect to Finnhub WebSocket, then close. Raises SystemExit on failure."""
    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not key:
        print("ERROR: FINNHUB_API_KEY not set in .env")
        print(f"Add it to: {os.path.join(_REPO_ROOT, '.env')}")
        raise SystemExit(1)

    masked = f"{key[:5]}...{key[-3:]}" if len(key) > 8 else "(key too short to mask)"
    print(f"API key found: {masked}")

    url = FINNHUB_WS_URL.format(key=key)
    print(f"Connecting to Finnhub WebSocket...")

    try:
        ws = websocket.create_connection(url, timeout=10)
    except Exception as e:
        print(f"ERROR: WebSocket connection failed: {e}")
        raise SystemExit(1)

    print("WebSocket connected successfully!")
    ws.close()
    print("Connection test passed.")


if __name__ == "__main__":
    test_connection()
