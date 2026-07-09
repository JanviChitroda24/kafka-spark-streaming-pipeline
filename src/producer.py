# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
Unified Producer Entry Point

Single CLI for both data sources — avoids remembering two different scripts.

Modes:
  --mode simulated  → Python trade simulator (~100 events/sec, works anytime)
  --mode real       → Finnhub WebSocket (live trades, market hours only)
  --mode auto       → picks real during market hours, simulated otherwise

Run from the src/ directory:
  cd src && python producer.py --mode simulated --eps 50 --duration 10
"""

import argparse
from datetime import datetime


def is_market_open() -> bool:
    """
    Return True if US equity markets are open (9:30 AM – 4:00 PM ET, Mon–Fri).

    Uses zoneinfo (Python 3.9+) instead of pytz — no extra dependency.
    Does not account for market holidays — good enough for dev/demo auto mode.
    """
    from zoneinfo import ZoneInfo

    now = datetime.now(ZoneInfo("America/New_York"))
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Trade Producer")
    parser.add_argument(
        "--mode",
        choices=["simulated", "real", "auto"],
        default="simulated",
        help="Data source: synthetic simulator, Finnhub live feed, or auto-detect",
    )
    parser.add_argument(
        "--eps",
        type=int,
        default=100,
        help="Events per second (simulated mode only)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="Run duration in seconds (simulated mode only; 0 = run until Ctrl+C)",
    )
    args = parser.parse_args()

    mode = args.mode
    if mode == "auto":
        mode = "real" if is_market_open() else "simulated"
        print(f"[AUTO] Market {'open' if mode == 'real' else 'closed'} → using {mode} mode")

    # Lazy imports — only load WebSocket stack when running real mode
    if mode == "real":
        from finnhub_producer import run_finnhub_producer

        run_finnhub_producer()
    else:
        from trade_simulator import run_simulator

        run_simulator(args.eps, args.duration)


if __name__ == "__main__":
    main()
