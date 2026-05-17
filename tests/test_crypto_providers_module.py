from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tradinglab_data.crypto.providers.ccxt_exchange import CCXTExchangeProvider, normalize_canonical_symbol
from tradinglab_data.crypto.providers.coingecko_provider import CoinGeckoProvider


class FakeCCXTClient:
    expected_since = 1713571200000

    def load_markets(self):
        return {
            "BTC/USDT": {"symbol": "BTC/USDT", "active": True, "spot": True, "quote": "USDT"},
            "ETH/EUR": {"symbol": "ETH/EUR", "active": True, "spot": True, "quote": "EUR"},
            "OLD/USDT": {"symbol": "OLD/USDT", "active": False, "spot": True, "quote": "USDT"},
            "PERP/USDT": {"symbol": "PERP/USDT", "active": True, "spot": False, "quote": "USDT"},
        }

    def fetch_ohlcv(self, source_symbol, *, timeframe, since, limit):
        assert source_symbol == "BTC/USDT"
        assert timeframe == "1h"
        assert since == self.expected_since
        assert limit == 10
        return [
            [1713571200000, 1.0, 2.0, 0.5, 1.5, 100.0],
            [1713574800000, 2.0, 3.0, 1.5, 2.5, 200.0],
        ]


def test_ccxt_provider_symbol_helpers_and_market_filtering():
    provider = CCXTExchangeProvider("binance", quote_assets=("USDT",), _client=FakeCCXTClient())

    assert normalize_canonical_symbol("btc/usdt") == ("BTC", "USDT")
    with pytest.raises(ValueError, match="Invalid crypto symbol"):
        normalize_canonical_symbol("BTC")
    assert provider.normalize_symbol("btc-usdt") == "BTC_USDT"
    with pytest.raises(ValueError, match="must include a quote"):
        provider.normalize_symbol("BTC")
    assert provider.supports_interval("1h")
    assert not provider.supports_interval("5m")
    assert provider.list_symbols() == ["BTC_USDT"]


def test_ccxt_provider_fetch_ohlcv_filters_end_and_validates_interval():
    provider = CCXTExchangeProvider("binance", _client=FakeCCXTClient())

    with pytest.raises(ValueError, match="Unsupported crypto interval"):
        provider.fetch_ohlcv("BTC_USDT", "5m")

    frame = provider.fetch_ohlcv(
        "BTC_USDT",
        "1h",
        start=datetime(2024, 4, 20, tzinfo=timezone.utc),
        end=datetime(2024, 4, 20, 1, tzinfo=timezone.utc),
        limit=10,
    )

    assert frame.height == 1
    assert frame.get_column("source_symbol").to_list() == ["BTC/USDT"]


def test_ccxt_provider_converts_aware_start_to_utc():
    client = FakeCCXTClient()
    client.expected_since = 1713564000000
    provider = CCXTExchangeProvider("binance", _client=client)

    frame = provider.fetch_ohlcv(
        "BTC_USDT",
        "1h",
        start=datetime(2024, 4, 20, 0, tzinfo=timezone(timedelta(hours=2))),
        end=datetime(2024, 4, 20, 3, tzinfo=timezone(timedelta(hours=2))),
        limit=10,
    )

    assert frame.height == 1


def test_ccxt_provider_create_rejects_unknown_exchange(monkeypatch):
    provider = CCXTExchangeProvider("missing")
    monkeypatch.setattr("tradinglab_data.crypto.providers.ccxt_exchange._import_ccxt", lambda: object())

    with pytest.raises(ValueError, match="Unsupported ccxt exchange"):
        provider._client_or_create()


def test_coingecko_provider_fetch_markets_filters_non_dict_items(monkeypatch):
    calls: list[dict[str, object]] = []

    class FakeResponse:
        def raise_for_status(self):
            calls.append({"raised": False})

        def json(self):
            return [{"id": "bitcoin"}, "bad", {"id": "ethereum"}]

    def fake_get(url, *, params, timeout):
        calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("tradinglab_data.crypto.providers.coingecko_provider.requests.get", fake_get)

    out = CoinGeckoProvider(base_url="https://example.test", timeout_seconds=3).fetch_markets(
        vs_currency="eur",
        per_page=2,
        page=3,
        sparkline=True,
    )

    assert out == [{"id": "bitcoin"}, {"id": "ethereum"}]
    assert calls[0]["url"] == "https://example.test/coins/markets"
    assert calls[0]["params"]["sparkline"] == "true"


def test_coingecko_provider_rejects_non_list_payload(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"id": "bitcoin"}

    monkeypatch.setattr(
        "tradinglab_data.crypto.providers.coingecko_provider.requests.get",
        lambda *args, **kwargs: FakeResponse(),
    )

    with pytest.raises(ValueError, match="must be a list"):
        CoinGeckoProvider().fetch_markets()
