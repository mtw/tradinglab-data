from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tradinglab_data.config import Config
from tradinglab_data.crypto.providers.binance_ccxt import BinanceCCXTProvider
from tradinglab_data.crypto.providers.coingecko_provider import CoinGeckoProvider
from tradinglab_data.crypto.workflows import crypto_refresh_universe_from_config


def _skip_for_network_issue(exc: Exception) -> None:
    text = f"{type(exc).__name__}: {exc}".lower()
    transient_markers = [
        "rate limit",
        "network",
        "timed out",
        "temporarily unavailable",
        "service unavailable",
        "forbidden",
        "not known",
        "name or service not known",
        "nodename nor servname provided",
    ]
    if any(marker in text for marker in transient_markers):
        pytest.skip(f"upstream/network unavailable: {exc}")
    raise exc


@pytest.mark.network
@pytest.mark.parametrize("interval", ["1d", "1h", "15m"])
def test_binance_provider_fetches_btc_usdt(interval: str):
    provider = BinanceCCXTProvider()
    start = datetime.now(timezone.utc) - timedelta(days=7)
    try:
        frame = provider.fetch_ohlcv("BTC_USDT", interval, start=start, limit=10)
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    if frame.is_empty():
        pytest.skip(f"no live data returned for {interval}")

    assert "timestamp" in frame.columns
    assert "symbol" in frame.columns
    assert frame.get_column("symbol").unique().to_list() == ["BTC_USDT"]
    assert frame.get_column("exchange").unique().to_list() == ["binance"]
    assert frame.get_column("interval").unique().to_list() == [interval]


@pytest.mark.network
def test_binance_provider_lists_symbols_includes_btc_usdt():
    provider = BinanceCCXTProvider()
    try:
        symbols = provider.list_symbols()
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    assert "BTC_USDT" in symbols


@pytest.mark.network
def test_coingecko_provider_fetches_markets():
    provider = CoinGeckoProvider()
    try:
        payload = provider.fetch_markets(per_page=5, page=1)
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    if not payload:
        pytest.skip("no CoinGecko data returned")

    assert isinstance(payload, list)
    assert "id" in payload[0]


@pytest.mark.network
def test_crypto_refresh_universe_smoke(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'universe.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  crypto_root: {tmp_path / 'crypto'}",
                f"  crypto_registry_json: {tmp_path / 'meta' / 'crypto' / 'registry.json'}",
                f"  crypto_universe_dir: {tmp_path / 'meta' / 'crypto' / 'universes'}",
                f"  runs_root: {tmp_path / 'runs'}",
                "crypto:",
                "  exchange: binance",
                "  market_type: spot",
                "  quote_assets: [USDT]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "meta").mkdir(parents=True, exist_ok=True)
    (tmp_path / "meta" / "universe.csv").write_text("symbol\nAAPL\n", encoding="utf-8")
    cfg = Config.load(config_path)

    try:
        result = crypto_refresh_universe_from_config(
            cfg,
            exchange="binance",
            provider_name="coingecko",
            universe="crypto_high_liquidity",
            limit=5,
        )
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    selected = result["symbols_selected"]
    if not selected:
        pytest.skip("no live symbols selected during refresh-universe smoke")

    assert result["universe"] == "crypto_high_liquidity"
    assert len(selected) <= 5
