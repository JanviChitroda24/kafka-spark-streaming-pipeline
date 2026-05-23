"""Tests for trade simulator — validates event structure and price behavior."""

import sys
from pathlib import Path

# Allow imports from src/ when pytest runs from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from config import BASE_PRICES, TICKERS
from trade_simulator import generate_trade


class TestTradeGeneration:
    def setup_method(self):
        # Fresh price dict each test — generate_trade mutates prices in place
        self.prices = dict(BASE_PRICES)

    def test_required_fields(self):
        trade = generate_trade("AAPL", self.prices)
        for field in ["trade_id", "ticker", "price", "quantity", "side", "source"]:
            assert field in trade, f"Missing field: {field}"

    def test_price_positive(self):
        for _ in range(1000):
            assert generate_trade("AAPL", self.prices)["price"] > 0

    def test_bid_less_than_ask(self):
        for _ in range(100):
            trade = generate_trade("AAPL", self.prices)
            assert trade["bid_price"] < trade["ask_price"]

    def test_source_is_simulator(self):
        assert generate_trade("AAPL", self.prices)["source"] == "simulator"

    def test_all_tickers_have_prices(self):
        for ticker in TICKERS:
            assert ticker in BASE_PRICES, f"Missing base price for {ticker}"

    def test_price_drift_bounded(self):
        """After 100 trades, price shouldn't drift more than 5% from start."""
        start = self.prices["AAPL"]
        for _ in range(100):
            generate_trade("AAPL", self.prices)
        drift = abs(self.prices["AAPL"] - start) / start
        assert drift < 0.05, f"Price drifted {drift:.2%} — too much"

    def test_quantity_positive(self):
        for _ in range(100):
            trade = generate_trade("NVDA", self.prices)
            assert trade["quantity"] > 0

    def test_side_valid(self):
        for _ in range(100):
            trade = generate_trade("TSLA", self.prices)
            assert trade["side"] in ("BUY", "SELL")

    def test_trade_type_valid(self):
        for _ in range(100):
            trade = generate_trade("MSFT", self.prices)
            assert trade["trade_type"] in ("odd_lot", "round_lot", "block")
