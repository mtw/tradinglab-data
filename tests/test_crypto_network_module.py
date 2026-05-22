from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
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
        "not known",
        "name or service not known",
        "nodename nor servname provided",
        "temporary failure in name resolution",
        "connection reset",
        "connection aborted",
        "connection refused",
        "max retries exceeded",
        "ssl",
    ]
    if any(marker in text for marker in transient_markers):
        pytest.skip(f"upstream/network unavailable: {exc}")
    raise exc


def test_skip_for_network_issue_skips_transient_dns_errors():
    with pytest.raises(pytest.skip.Exception):
        _skip_for_network_issue(RuntimeError("Name or service not known"))


def test_skip_for_network_issue_skips_timeouts():
    with pytest.raises(pytest.skip.Exception):
        _skip_for_network_issue(TimeoutError("request timed out"))


def test_skip_for_network_issue_reraises_forbidden_responses():
    with pytest.raises(RuntimeError, match="403 forbidden"):
        _skip_for_network_issue(RuntimeError("403 forbidden"))


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

    assert frame.is_empty() is False, f"live Binance fetch returned empty data for {interval}"

    assert frame.columns == [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "provider",
        "exchange",
        "market_type",
        "symbol",
        "base_asset",
        "quote_asset",
        "interval",
        "is_closed",
        "ingested_at",
        "source_symbol",
    ]
    assert frame.get_column("open").null_count() == 0
    assert frame.get_column("high").null_count() == 0
    assert frame.get_column("low").null_count() == 0
    assert frame.get_column("close").null_count() == 0
    assert frame.get_column("volume").null_count() == 0
    assert frame.get_column("symbol").unique().to_list() == ["BTC_USDT"]
    assert frame.get_column("provider").unique().to_list() == ["ccxt"]
    assert frame.get_column("exchange").unique().to_list() == ["binance"]
    assert frame.get_column("market_type").unique().to_list() == ["spot"]
    assert frame.get_column("base_asset").unique().to_list() == ["BTC"]
    assert frame.get_column("quote_asset").unique().to_list() == ["USDT"]
    assert frame.get_column("interval").unique().to_list() == [interval]
    assert frame.get_column("source_symbol").unique().to_list() == ["BTC/USDT"]
    assert set(frame.get_column("is_closed").unique().to_list()).issubset({True, False})


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

    assert isinstance(payload, list)
    assert payload, "CoinGecko returned an empty markets payload"
    first = payload[0]
    assert "id" in first
    assert "symbol" in first
    assert "name" in first
    assert "market_cap" in first
    assert "total_volume" in first


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
    assert result["universe"] == "crypto_high_liquidity"
    assert result["provider"] == "coingecko"
    assert result["exchange"] == "binance"
    assert result["market_type"] == "spot"
    assert selected, "refresh-universe returned no selected symbols"
    assert len(selected) <= 5
    registry_path = Path(result["registry_path"])
    universe_path = Path(result["universe_path"])
    assert registry_path.exists()
    assert universe_path.exists()

    registry_payload = json.loads(registry_path.read_text(encoding="utf-8"))
    assert isinstance(registry_payload, list)
    persisted_registry_symbols = {str(item.get("symbol_canonical", "")) for item in registry_payload if isinstance(item, dict)}
    assert set(selected).issubset(persisted_registry_symbols)

    universe_frame = pl.read_csv(universe_path)
    assert "symbol" in universe_frame.columns
    assert universe_frame.get_column("symbol").to_list() == selected
