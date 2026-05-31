from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from tradinglab_data.crypto.providers.binance_ccxt import _normalize_ohlcv_rows
from tradinglab_data.crypto.registry import (
    _dynamic_registry_entries,
    _dynamic_universes,
    _load_json,
    _registry_entry_key,
    load_crypto_registry,
    load_crypto_universes,
    merge_dynamic_registry,
    resolve_crypto_universe,
    write_dynamic_registry,
    write_dynamic_universe,
)
from tradinglab_data.crypto.storage import atomic_write_parquet, read_crypto_parquet
from tradinglab_data.crypto.symbols import normalize_crypto_symbol, split_crypto_symbol, to_source_symbol
from tradinglab_data.crypto.validation import (
    filter_closed_bars,
    merge_crypto_frames,
    normalize_crypto_frame_schema,
    validate_crypto_ohlcv_frame,
)
from tradinglab_data.crypto.verify import CryptoVerifyConfig, run_crypto_verify_checks
from tradinglab_data.crypto.workflows import (
    _as_float,
    _as_int,
    _coingecko_item_to_metadata,
    _fetch_symbol_history,
    _frames_equal,
    _interval_delta,
    _provider_for,
    _read_crypto_config,
    crypto_backfill_from_config,
    crypto_diff_universe_from_config,
    crypto_inspect_from_config,
    crypto_list_symbols_from_config,
    crypto_prune_from_config,
    crypto_refresh_universe_from_config,
    crypto_show_universe_from_config,
    crypto_validate_from_config,
)


def _crypto_frame(symbol: str = "BTC_USDT", *, timestamps: list[str] | None = None) -> pl.DataFrame:
    use_timestamps = timestamps or ["2026-04-18T00:00:00", "2026-04-18T01:00:00"]
    base_asset, quote_asset = split_crypto_symbol(symbol)
    frame = pl.DataFrame(
        {
            "timestamp": use_timestamps,
            "open": [10.0 + idx for idx, _ in enumerate(use_timestamps)],
            "high": [10.5 + idx for idx, _ in enumerate(use_timestamps)],
            "low": [9.5 + idx for idx, _ in enumerate(use_timestamps)],
            "close": [10.2 + idx for idx, _ in enumerate(use_timestamps)],
            "volume": [100.0 + idx for idx, _ in enumerate(use_timestamps)],
            "provider": ["ccxt"] * len(use_timestamps),
            "exchange": ["binance"] * len(use_timestamps),
            "market_type": ["spot"] * len(use_timestamps),
            "symbol": [symbol] * len(use_timestamps),
            "base_asset": [base_asset] * len(use_timestamps),
            "quote_asset": [quote_asset] * len(use_timestamps),
            "interval": ["1h"] * len(use_timestamps),
            "is_closed": [True] * len(use_timestamps),
            "ingested_at": ["2026-04-20T00:00:00"] * len(use_timestamps),
            "source_symbol": [to_source_symbol(symbol)] * len(use_timestamps),
        }
    )
    return frame.with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )


def test_crypto_symbol_helpers_normalize_and_split():
    assert normalize_crypto_symbol("btc/usdt") == "BTC_USDT"
    assert split_crypto_symbol("btc-usdt") == ("BTC", "USDT")
    assert to_source_symbol("btc_usdt") == "BTC/USDT"


def test_resolve_crypto_universe_returns_curated_majors():
    entries = resolve_crypto_universe("crypto_majors")
    assert [entry["symbol_canonical"] for entry in entries] == ["BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "BNB_USDT"]


def test_normalize_ohlcv_rows_attaches_metadata():
    frame = _normalize_ohlcv_rows(
        [
            [1713571200000, 1.0, 2.0, 0.5, 1.5, 100.0],
            [1713574800000, 2.0, 3.0, 1.5, 2.5, 200.0],
        ],
        symbol="BTC_USDT",
        exchange="binance",
        market_type="spot",
        interval="1h",
        provider="ccxt",
        base_asset="BTC",
        quote_asset="USDT",
        source_symbol="BTC/USDT",
        effective_time=datetime(2024, 4, 20, 1, 30, tzinfo=timezone.utc),
    )
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
    assert frame.get_column("symbol").to_list() == ["BTC_USDT", "BTC_USDT"]
    assert frame.get_column("is_closed").to_list() == [True, False]


def test_filter_closed_bars_drops_open_bar():
    frame = _crypto_frame(timestamps=["2026-04-20T08:00:00", "2026-04-20T09:00:00"])
    filtered = filter_closed_bars(frame, interval="1h", now_ts=datetime(2026, 4, 20, 9, 30, tzinfo=timezone.utc))
    assert filtered.get_column("timestamp").to_list() == [datetime(2026, 4, 20, 8, 0)]


def test_filter_closed_bars_and_normalize_schema_empty_paths():
    empty = pl.DataFrame(schema=_crypto_frame().schema)
    assert filter_closed_bars(empty, interval="1h").is_empty()
    assert normalize_crypto_frame_schema(pl.DataFrame()).is_empty()


def test_merge_and_validate_crypto_frames():
    old = _crypto_frame(timestamps=["2026-04-18T00:00:00", "2026-04-18T01:00:00"])
    new = _crypto_frame(timestamps=["2026-04-18T01:00:00", "2026-04-18T02:00:00"])
    merged = merge_crypto_frames(old, new)
    validate_crypto_ohlcv_frame(merged, interval="1h")
    assert merged.height == 3


def test_validate_crypto_ohlcv_frame_reports_edge_errors():
    with pytest.raises(ValueError, match="timestamp contains nulls"):
        validate_crypto_ohlcv_frame(_crypto_frame().with_columns(pl.lit(None).cast(pl.Datetime).alias("timestamp")), interval="1h")
    with pytest.raises(ValueError, match="timestamp values must be unique"):
        validate_crypto_ohlcv_frame(_crypto_frame(timestamps=["2026-04-18T00:00:00", "2026-04-18T00:00:00"]), interval="1h")
    with pytest.raises(ValueError, match="rows must be sorted"):
        validate_crypto_ohlcv_frame(_crypto_frame(timestamps=["2026-04-18T01:00:00", "2026-04-18T00:00:00"]), interval="1h")
    with pytest.raises(ValueError, match="canonical crypto history may only contain closed bars"):
        validate_crypto_ohlcv_frame(_crypto_frame().with_columns(pl.lit(False).alias("is_closed")), interval="1h")
    with pytest.raises(ValueError, match="ohlcv constraints failed"):
        validate_crypto_ohlcv_frame(_crypto_frame().with_columns(pl.lit(-1.0).alias("volume")), interval="1h")
    with pytest.raises(ValueError, match="provider contains nulls"):
        validate_crypto_ohlcv_frame(_crypto_frame().with_columns(pl.lit(None).cast(pl.String).alias("provider")), interval="1h")
    with pytest.raises(ValueError, match="interval continuity failed"):
        validate_crypto_ohlcv_frame(_crypto_frame(timestamps=["2026-04-18T00:00:00", "2026-04-18T03:00:00"]), interval="1h")


def test_merge_crypto_frames_and_storage_helpers_cover_empty_and_cleanup(tmp_path: Path, monkeypatch):
    incoming = _crypto_frame()
    merged = merge_crypto_frames(None, incoming)
    assert merged.height == incoming.height

    path = tmp_path / "x" / "BTC_USDT.parquet"
    atomic_write_parquet(path, incoming)
    assert read_crypto_parquet(path).height == 2
    assert read_crypto_parquet(tmp_path / "missing.parquet") is None

    monkeypatch.setattr("tradinglab_data.crypto.storage.os.replace", lambda src, dst: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError, match="boom"):
        atomic_write_parquet(tmp_path / "x" / "BROKEN.parquet", incoming)


def test_merge_crypto_frames_normalizes_datetime_units():
    existing = _crypto_frame(timestamps=["2026-04-18T00:00:00"]).with_columns(
        pl.col("timestamp").cast(pl.Datetime("us")),
        pl.col("ingested_at").cast(pl.Datetime("us")),
    )
    incoming = _crypto_frame(timestamps=["2026-04-18T01:00:00"]).with_columns(
        pl.col("timestamp").cast(pl.Datetime("ms")),
        pl.col("ingested_at").cast(pl.Datetime("ms")),
    )

    merged = merge_crypto_frames(existing, incoming)

    assert merged.schema["timestamp"] == pl.Datetime("us")
    assert merged.schema["ingested_at"] == pl.Datetime("us")
    assert merged.height == 2


def test_normalize_crypto_frame_schema_enforces_canonical_types():
    frame = _crypto_frame().with_columns(
        pl.col("timestamp").cast(pl.Datetime("ms")),
        pl.col("ingested_at").cast(pl.Datetime("ms")),
    )

    normalized = normalize_crypto_frame_schema(frame)

    assert normalized.schema["timestamp"] == pl.Datetime("us")
    assert normalized.schema["ingested_at"] == pl.Datetime("us")


def test_crypto_backfill_and_validate_roundtrip(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    class FakeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT"]

        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            return _crypto_frame(symbol=symbol)

    import tradinglab_data.crypto.workflows as crypto_workflows

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeProvider())
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {
                "exchange": "binance",
                "market_type": "spot",
                "default_universe": "crypto_majors",
                "quote_assets": ["USDT"],
                "max_batch_limit": 1000,
                "incremental_lookback_bars": 100,
                "full_backfill_limit": 100,
                "validate_continuity": True,
            },
        }
    )

    result = crypto_backfill_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"])
    validate_result = crypto_validate_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"])
    out_path = tmp_path / "crypto" / "binance" / "spot" / "1h" / "BTC_USDT.parquet"

    assert result["files_written"] == 1
    assert out_path.exists()
    assert validate_result["ok"] is True


def test_provider_for_supports_all_known_exchanges(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    seen: list[tuple[str, str, tuple[str, ...]]] = []

    class _DummyProvider:
        def __init__(self, *, exchange_name: str, market_type: str, quote_assets: tuple[str, ...]):
            seen.append((exchange_name, market_type, quote_assets))

    monkeypatch.setattr(crypto_workflows, "BinanceCCXTProvider", _DummyProvider)
    monkeypatch.setattr(crypto_workflows, "KrakenCCXTProvider", _DummyProvider)
    monkeypatch.setattr(crypto_workflows, "CoinbaseCCXTProvider", _DummyProvider)

    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"quote_assets": ["USDT"], "market_type": "spot"},
        }
    )

    for exchange in ("binance", "kraken", "coinbase"):
        _provider_for(_read_crypto_config(cfg, exchange=exchange))

    assert seen == [
        ("binance", "spot", ("USDT",)),
        ("kraken", "spot", ("USDT",)),
        ("coinbase", "spot", ("USDT",)),
    ]


def test_provider_for_rejects_unsupported_exchange(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "unsupported"},
        }
    )

    with pytest.raises(ValueError, match="Unsupported crypto exchange"):
        _provider_for(_read_crypto_config(cfg))


def test_crypto_list_symbols_from_config_uses_provider(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    class FakeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT", "ETH_USDT"]

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeProvider())
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            }
        }
    )

    assert crypto_list_symbols_from_config(cfg) == ["BTC_USDT", "ETH_USDT"]


def test_crypto_incremental_update_marks_unchanged_when_no_new_closed_rows(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    class FakeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT"]

        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            return pl.DataFrame(schema={"timestamp": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "provider": pl.String, "exchange": pl.String, "market_type": pl.String, "symbol": pl.String, "base_asset": pl.String, "quote_asset": pl.String, "interval": pl.String, "is_closed": pl.Boolean, "ingested_at": pl.Datetime, "source_symbol": pl.String})

    import tradinglab_data.crypto.workflows as crypto_workflows

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeProvider())
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )
    crypto_backfill_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"])
    result = crypto_backfill_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"], incremental=True)

    assert result["files_written"] == 0
    assert result["unchanged_symbols"] == ["BTC_USDT"]


def test_fetch_symbol_history_batches_and_advances_cursor():
    class FakeProvider:
        def __init__(self) -> None:
            self.starts: list[datetime | None] = []

        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            self.starts.append(start)
            if len(self.starts) == 1:
                return _crypto_frame(
                    timestamps=[
                        "2026-04-18T00:00:00",
                        "2026-04-18T01:00:00",
                    ]
                )
            if len(self.starts) == 2:
                return _crypto_frame(timestamps=["2026-04-18T02:00:00"])
            return pl.DataFrame(schema=_crypto_frame().schema)

    provider = FakeProvider()
    start = datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc)

    frame = _fetch_symbol_history(provider, "BTC_USDT", "1h", start=start, total_limit=3, batch_limit=2)

    assert frame.height == 3
    assert provider.starts[0] == start
    assert provider.starts[1] == datetime(2026, 4, 18, 2, 0, tzinfo=timezone.utc)


def test_fetch_symbol_history_returns_empty_frame_when_provider_has_no_data():
    class FakeProvider:
        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            return pl.DataFrame(schema=_crypto_frame().schema)

    frame = _fetch_symbol_history(FakeProvider(), "BTC_USDT", "1h", start=None, total_limit=10, batch_limit=5)

    assert frame.is_empty()


def test_fetch_symbol_history_breaks_on_short_batch_and_missing_cursor():
    class ShortProvider:
        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            return _crypto_frame(timestamps=["2026-04-18T00:00:00"])

    frame = _fetch_symbol_history(ShortProvider(), "BTC_USDT", "1h", start=None, total_limit=10, batch_limit=5)
    assert frame.height == 1

    class MissingCursorProvider:
        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            return _crypto_frame().with_columns(pl.lit(None).cast(pl.Datetime).alias("timestamp"))

    frame = _fetch_symbol_history(MissingCursorProvider(), "BTC_USDT", "1h", start=None, total_limit=10, batch_limit=2)
    assert frame.height == 1


def test_interval_delta_rejects_unsupported_interval():
    with pytest.raises(ValueError, match="Unsupported crypto interval"):
        _interval_delta("5m")


def test_crypto_backfill_skips_non_tradable_symbol(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    class FakeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT"]

        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            return _crypto_frame(symbol=symbol)

    import tradinglab_data.crypto.workflows as crypto_workflows

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeProvider())
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )

    result = crypto_backfill_from_config(cfg, interval="1h", symbols_override=["ETH_USDT"])

    assert result["files_written"] == 0
    assert result["skipped_symbols"] == ["ETH_USDT"]


def test_crypto_validate_reports_missing_file(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )

    result = crypto_validate_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"])
    assert result["ok"] is False
    assert result["dirty_files"]


def test_crypto_validate_reports_invalid_parquet(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )
    out_path = tmp_path / "crypto" / "binance" / "spot" / "1h"
    out_path.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"bad": [1]}).write_parquet(out_path / "BTC_USDT.parquet")

    result = crypto_validate_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"])

    assert result["ok"] is False
    assert any("invalid_crypto_parquet" in error for error in result["errors"])


def test_coingecko_item_filter_rejects_stablecoin(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {"parquet_root": tmp_path / "daily", "crypto_root": tmp_path / "crypto", "runs_root": tmp_path / "runs", "universe_csv": tmp_path / "meta" / "universe.csv"},
            "crypto": {"quote_assets": ["USDT"], "stablecoin_ids": ["tether"]},
        }
    )
    crypto_cfg = _read_crypto_config(cfg)

    entry = _coingecko_item_to_metadata(
        {"id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3, "market_cap": 1.0, "total_volume": 2.0},
        crypto_cfg=crypto_cfg,
        universe_name="crypto_high_liquidity",
    )

    assert entry is None


def test_registry_helpers_cover_json_dynamic_and_write_paths(dummy_cfg_factory, tmp_path: Path, monkeypatch):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": tmp_path / "meta" / "crypto" / "registry.json",
                "crypto_universe_dir": tmp_path / "meta" / "crypto" / "universes",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            }
        }
    )
    assert _load_json(tmp_path / "missing.json") is None
    assert _dynamic_registry_entries(None) == []
    assert _dynamic_universes(None) == {}
    assert _registry_entry_key({"symbol_canonical": "bad symbol"}) is None
    with pytest.raises(ValueError, match="Unknown crypto universe"):
        resolve_crypto_universe("missing", cfg=cfg)

    registry_entries = [
        {"symbol_canonical": "BTC_USDT", "source_symbol": "BTC/USDT", "exchange": "binance", "market_type": "spot", "base_asset": "BTC", "quote_asset": "USDT", "is_active": True, "universe_tags": ["crypto"]},
        {"symbol_canonical": "ETH_EUR", "source_symbol": "ETH/EUR", "exchange": "coinbase", "market_type": "spot", "quote_asset": "EUR"},
        "junk",
    ]
    cfg.get("paths", "crypto_registry_json").parent.mkdir(parents=True, exist_ok=True)
    cfg.get("paths", "crypto_registry_json").write_text(json.dumps(registry_entries), encoding="utf-8")
    cfg.get("paths", "crypto_universe_dir").mkdir(parents=True, exist_ok=True)
    (cfg.get("paths", "crypto_universe_dir") / "dyn.json").write_text(json.dumps({"symbols": ["btc/usdt", " "]}, indent=2), encoding="utf-8")
    (cfg.get("paths", "crypto_universe_dir") / "bad.json").write_text(json.dumps(["bad"]), encoding="utf-8")
    (cfg.get("paths", "crypto_universe_dir") / "bad2.json").write_text(json.dumps({"symbols": "bad"}), encoding="utf-8")

    assert [entry["symbol_canonical"] for entry in _dynamic_registry_entries(cfg)] == ["BTC_USDT", "ETH_EUR"]
    assert _dynamic_universes(cfg) == {"dyn": ("BTC_USDT",)}
    assert any(entry["symbol_canonical"] == "BTC_USDT" for entry in load_crypto_registry(cfg=cfg))
    assert load_crypto_registry(cfg=cfg, quote_assets=("EUR",)) == []

    import tradinglab_data.crypto.registry as registry_mod
    original_normalize = registry_mod.normalize_crypto_symbol
    monkeypatch.setattr(registry_mod, "normalize_crypto_symbol", lambda symbol: "" if symbol == "EMPTY_EMPTY" else original_normalize(symbol))
    cfg.get("paths", "crypto_registry_json").write_text(json.dumps([{"symbol_canonical": "EMPTY_EMPTY"}]), encoding="utf-8")
    assert _dynamic_registry_entries(cfg) == []

    written_registry = write_dynamic_registry(cfg, [{"symbol_canonical": "ETH_USDT"}])
    written_universe = write_dynamic_universe(cfg, "dyn2", ["ETH_USDT"], {"provider": "x"})
    assert written_registry.exists()
    assert written_universe.exists()

    cfg.get("paths", "crypto_registry_json").write_text(json.dumps(["preserved", {"symbol_canonical": "bad symbol"}]), encoding="utf-8")
    merge_dynamic_registry(cfg, [{"symbol_canonical": "ETH_USDT", "exchange": "binance", "market_type": "spot"}])
    merged_text = cfg.get("paths", "crypto_registry_json").read_text(encoding="utf-8")
    assert "ETH_USDT" in merged_text
    assert "preserved" in merged_text
    assert "bad symbol" in merged_text


def test_crypto_refresh_universe_writes_dynamic_registry(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    class FakeExchangeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT", "ETH_USDT", "DOGE_USDT"]

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeExchangeProvider())
    monkeypatch.setattr(
        crypto_workflows.CoinGeckoProvider,
        "fetch_markets",
        lambda self, **kwargs: [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "market_cap": 10.0, "total_volume": 5.0},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2, "market_cap": 9.0, "total_volume": 4.0},
            {"id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3, "market_cap": 8.0, "total_volume": 7.0},
            {"id": "wrapped-bitcoin", "symbol": "wbtc", "name": "Wrapped Bitcoin", "market_cap_rank": 4, "market_cap": 7.0, "total_volume": 3.0},
        ],
    )
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": tmp_path / "meta" / "crypto" / "registry.json",
                "crypto_universe_dir": tmp_path / "meta" / "crypto" / "universes",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {
                "exchange": "binance",
                "market_type": "spot",
                "quote_assets": ["USDT"],
                "universe_refresh_limit": 10,
                "universe_refresh_pages": 1,
                "stablecoin_ids": ["tether"],
                "exclude_wrapped_assets": True,
            },
        }
    )

    result = crypto_refresh_universe_from_config(cfg, universe="crypto_dynamic")
    universes = load_crypto_universes(cfg)
    resolved = resolve_crypto_universe("crypto_dynamic", cfg=cfg)

    assert result["symbols_selected"] == ["BTC_USDT", "ETH_USDT"]
    assert universes["crypto_dynamic"] == ("BTC_USDT", "ETH_USDT")
    assert [entry["symbol_canonical"] for entry in resolved] == ["BTC_USDT", "ETH_USDT"]
    assert Path(result["registry_path"]).exists()
    assert Path(result["universe_path"]).exists()


def test_crypto_refresh_universe_preserves_unrelated_dynamic_registry_entries(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    class FakeExchangeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT", "ETH_USDT"]

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeExchangeProvider())
    monkeypatch.setattr(
        crypto_workflows.CoinGeckoProvider,
        "fetch_markets",
        lambda self, **kwargs: [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "market_cap": 10.0, "total_volume": 5.0},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 2, "market_cap": 9.0, "total_volume": 4.0},
        ],
    )
    registry_path = tmp_path / "meta" / "crypto" / "registry.json"
    universe_dir = tmp_path / "meta" / "crypto" / "universes"
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": registry_path,
                "crypto_universe_dir": universe_dir,
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {
                "exchange": "binance",
                "market_type": "spot",
                "quote_assets": ["USDT"],
                "universe_refresh_limit": 10,
                "universe_refresh_pages": 1,
            },
        }
    )
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(
            [
                {
                    "coingecko_id": "dogecoin",
                    "symbol_canonical": "DOGE_USDT",
                    "source_symbol": "DOGE/USDT",
                    "name": "Dogecoin",
                    "base_asset": "DOGE",
                    "quote_asset": "USDT",
                    "market_cap_rank": 9,
                    "market_cap": 3.0,
                    "total_volume": 2.0,
                    "exchange": "binance",
                    "market_type": "spot",
                    "is_active": True,
                    "universe_tags": ["crypto", "crypto_other_dynamic"],
                }
            ]
        ),
        encoding="utf-8",
    )
    universe_dir.mkdir(parents=True, exist_ok=True)
    (universe_dir / "crypto_other_dynamic.json").write_text(
        json.dumps({"universe": "crypto_other_dynamic", "symbols": ["DOGE_USDT"]}),
        encoding="utf-8",
    )

    crypto_refresh_universe_from_config(cfg, universe="crypto_dynamic")

    resolved_other = resolve_crypto_universe("crypto_other_dynamic", cfg=cfg)
    assert [entry["symbol_canonical"] for entry in resolved_other] == ["DOGE_USDT"]
    persisted_symbols = {entry["symbol_canonical"] for entry in json.loads(registry_path.read_text(encoding="utf-8"))}
    assert persisted_symbols == {"BTC_USDT", "ETH_USDT", "DOGE_USDT"}


def test_coingecko_selection_stops_on_empty_pages_duplicates_and_limit(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    class FakeExchangeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT", "ETH_USDT"]

    pages = {
        1: [
            {"id": "dogecoin", "symbol": "doge", "name": "Dogecoin", "market_cap_rank": 1, "market_cap": 10.0, "total_volume": 10.0},
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 2, "market_cap": 9.0, "total_volume": 9.0},
            {"id": "bitcoin-dup", "symbol": "btc", "name": "Bitcoin Dup", "market_cap_rank": 3, "market_cap": 8.0, "total_volume": 8.0},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 4, "market_cap": 7.0, "total_volume": 7.0},
        ],
        2: [],
    }

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeExchangeProvider())
    monkeypatch.setattr(crypto_workflows.CoinGeckoProvider, "fetch_markets", lambda self, **kwargs: pages[kwargs["page"]])
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": tmp_path / "meta" / "crypto" / "registry.json",
                "crypto_universe_dir": tmp_path / "meta" / "crypto" / "universes",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {
                "exchange": "binance",
                "market_type": "spot",
                "quote_assets": ["USDT"],
                "universe_refresh_limit": 1,
                "universe_refresh_pages": 2,
            },
        }
    )

    result = crypto_refresh_universe_from_config(cfg, universe="crypto_dynamic")
    assert result["symbols_selected"] == ["BTC_USDT"]


def test_crypto_refresh_universe_skips_invalid_coingecko_symbol(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    class FakeExchangeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT", "ETH_USDT"]

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeExchangeProvider())
    monkeypatch.setattr(
        crypto_workflows.CoinGeckoProvider,
        "fetch_markets",
        lambda self, **kwargs: [
            {"id": "bad-heloc", "symbol": "FIGR_HELOC", "name": "Bad Heloc", "market_cap_rank": 1, "market_cap": 10.0, "total_volume": 5.0},
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 2, "market_cap": 9.0, "total_volume": 4.0},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap_rank": 3, "market_cap": 8.0, "total_volume": 3.0},
        ],
    )
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": tmp_path / "meta" / "crypto" / "registry.json",
                "crypto_universe_dir": tmp_path / "meta" / "crypto" / "universes",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {
                "exchange": "binance",
                "market_type": "spot",
                "quote_assets": ["USDT"],
                "universe_refresh_limit": 10,
                "universe_refresh_pages": 1,
            },
        }
    )

    result = crypto_refresh_universe_from_config(cfg, universe="crypto_dynamic")

    assert result["symbols_selected"] == ["BTC_USDT", "ETH_USDT"]


def test_crypto_show_diff_inspect_and_prune(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": tmp_path / "meta" / "crypto" / "registry.json",
                "crypto_universe_dir": tmp_path / "meta" / "crypto" / "universes",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "quote_assets": ["USDT"]},
        }
    )
    _write_path = tmp_path / "crypto" / "binance" / "spot" / "1h"
    _write_path.mkdir(parents=True, exist_ok=True)
    _crypto_frame().write_parquet(_write_path / "BTC_USDT.parquet")
    _crypto_frame(symbol="DOGE_USDT").write_parquet(_write_path / "DOGE_USDT.parquet")

    shown = crypto_show_universe_from_config(cfg, universe="crypto_majors")
    diff = crypto_diff_universe_from_config(cfg, left_universe="crypto_majors", right_universe="crypto_core")
    inspect = crypto_inspect_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"])
    pruned = crypto_prune_from_config(cfg, interval="1h", universe="crypto_majors")

    assert "BTC_USDT" in shown
    assert "DOGE_USDT" in diff["right_only"]
    assert inspect[0]["exists"] is True
    assert any(path.endswith("DOGE_USDT.parquet") for path in pruned)

    try:
        crypto_prune_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"], apply=True)
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "symbols_override is not a complete keep set" in str(exc)


def test_crypto_inspect_handles_missing_and_existing_files(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "quote_assets": ["USDT"]},
        }
    )
    root = tmp_path / "crypto" / "binance" / "spot" / "1h"
    root.mkdir(parents=True, exist_ok=True)
    _crypto_frame(symbol="BTC_USDT").write_parquet(root / "BTC_USDT.parquet")

    inspect = crypto_inspect_from_config(cfg, interval="1h", symbols_override=["BTC_USDT", "ETH_USDT"])

    assert inspect[0]["exists"] is True
    assert inspect[0]["rows"] == 2
    assert inspect[0]["start"] is not None
    assert inspect[1]["exists"] is False
    assert inspect[1]["rows"] == 0


def test_crypto_prune_apply_removes_files_and_handles_missing_root(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "quote_assets": ["USDT"]},
        }
    )

    assert crypto_prune_from_config(cfg, interval="1h", universe="crypto_majors") == []

    root = tmp_path / "crypto" / "binance" / "spot" / "1h"
    root.mkdir(parents=True, exist_ok=True)
    _crypto_frame(symbol="BTC_USDT").write_parquet(root / "BTC_USDT.parquet")
    _crypto_frame(symbol="DOGE_USDT").write_parquet(root / "DOGE_USDT.parquet")

    pruned = crypto_prune_from_config(cfg, interval="1h", universe="crypto_majors", apply=True)

    assert any(path.endswith("DOGE_USDT.parquet") for path in pruned)
    assert not (root / "DOGE_USDT.parquet").exists()
    assert (root / "BTC_USDT.parquet").exists()


def test_frames_equal_detects_mismatch_and_handles_equals_failure(monkeypatch):
    left = pl.DataFrame({"a": [1]})
    right = pl.DataFrame({"a": [2]})
    assert _frames_equal(left, right) is False
    assert _frames_equal(left, pl.DataFrame({"b": [1]})) is False

    monkeypatch.setattr(pl.DataFrame, "equals", lambda self, other: (_ for _ in ()).throw(RuntimeError("boom")))
    assert _frames_equal(left, left) is False


def test_crypto_verify_detects_missing_invalid_and_stale(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.verify as crypto_verify

    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )
    root = tmp_path / "crypto" / "binance" / "spot" / "1h"
    root.mkdir(parents=True, exist_ok=True)
    _crypto_frame(symbol="BTC_USDT", timestamps=["2026-04-20T16:00:00", "2026-04-20T17:00:00"]).write_parquet(root / "BTC_USDT.parquet")
    _crypto_frame(symbol="ETH_USDT", timestamps=["2026-04-18T00:00:00", "2026-04-18T01:00:00"]).write_parquet(root / "ETH_USDT.parquet")
    pl.DataFrame({"bad": [1]}).write_parquet(root / "SOL_USDT.parquet")
    monkeypatch.setattr(crypto_verify, "_is_stale", lambda value, interval, stale_multiple: value == datetime(2026, 4, 18, 1, 0))

    result = run_crypto_verify_checks(cfg, CryptoVerifyConfig(interval="1h", universe="crypto_majors"))

    assert result["ok"] is False
    assert "BNB_USDT" in result["missing_symbols"]
    assert any(item["symbol"] == "SOL_USDT" and "invalid" in item["reasons"] for item in result["dirty_symbols"])
    assert any(item["symbol"] == "ETH_USDT" and "stale" in item["reasons"] for item in result["dirty_symbols"])


def test_crypto_verify_zero_byte_empty_and_stale_helpers(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.verify as crypto_verify
    original_is_stale = crypto_verify._is_stale

    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )
    root = tmp_path / "crypto" / "binance" / "spot" / "1h"
    root.mkdir(parents=True, exist_ok=True)
    (root / "BTC_USDT.parquet").write_bytes(b"")
    pl.DataFrame(
        {
            "timestamp": pl.Series([], dtype=pl.Datetime),
            "open": pl.Series([], dtype=pl.Float64),
            "high": pl.Series([], dtype=pl.Float64),
            "low": pl.Series([], dtype=pl.Float64),
            "close": pl.Series([], dtype=pl.Float64),
            "volume": pl.Series([], dtype=pl.Float64),
            "provider": pl.Series([], dtype=pl.String),
            "exchange": pl.Series([], dtype=pl.String),
            "market_type": pl.Series([], dtype=pl.String),
            "symbol": pl.Series([], dtype=pl.String),
            "base_asset": pl.Series([], dtype=pl.String),
            "quote_asset": pl.Series([], dtype=pl.String),
            "interval": pl.Series([], dtype=pl.String),
            "is_closed": pl.Series([], dtype=pl.Boolean),
            "ingested_at": pl.Series([], dtype=pl.Datetime),
            "source_symbol": pl.Series([], dtype=pl.String),
        }
    ).write_parquet(root / "ETH_USDT.parquet")
    monkeypatch.setattr(crypto_verify, "_is_stale", lambda value, interval, stale_multiple: False)

    result = run_crypto_verify_checks(cfg, CryptoVerifyConfig(interval="1h", universe="crypto_majors", max_missing_ratio=1.0))

    assert any(item["symbol"] == "BTC_USDT" and "zero_byte" in item["reasons"] for item in result["dirty_symbols"])
    assert any(item["symbol"] == "ETH_USDT" and "empty_file" in item["reasons"] for item in result["dirty_symbols"])
    assert "zero_byte_files:1>0" in result["errors"]
    assert original_is_stale("bad", interval="1h", stale_multiple=2) is True
    assert original_is_stale(datetime(2000, 1, 1), interval="1h", stale_multiple=2) is True


def test_crypto_refresh_universe_rejects_unsupported_provider(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": tmp_path / "meta" / "crypto" / "registry.json",
                "crypto_universe_dir": tmp_path / "meta" / "crypto" / "universes",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            }
        }
    )

    with pytest.raises(ValueError, match="Unsupported crypto universe refresh provider"):
        crypto_refresh_universe_from_config(cfg, provider_name="other")


def test_coingecko_item_to_metadata_filters_missing_and_thresholded_values(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {"parquet_root": tmp_path / "daily", "crypto_root": tmp_path / "crypto", "runs_root": tmp_path / "runs", "universe_csv": tmp_path / "meta" / "universe.csv"},
            "crypto": {
                "quote_assets": ["USDT"],
                "excluded_symbols": ["SCAM"],
                "universe_refresh_min_market_cap": 5.0,
                "universe_refresh_min_volume": 5.0,
            },
        }
    )
    crypto_cfg = _read_crypto_config(cfg)

    assert _coingecko_item_to_metadata({}, crypto_cfg=crypto_cfg, universe_name="u") is None
    assert (
        _coingecko_item_to_metadata(
            {"id": "coin", "symbol": "scam", "name": "Scam", "market_cap": 10.0, "total_volume": 10.0},
            crypto_cfg=crypto_cfg,
            universe_name="u",
        )
        is None
    )
    assert (
        _coingecko_item_to_metadata(
            {"id": "coin", "symbol": "btc", "name": "Bitcoin", "market_cap": 1.0, "total_volume": 10.0},
            crypto_cfg=crypto_cfg,
            universe_name="u",
        )
        is None
    )
    assert (
        _coingecko_item_to_metadata(
            {"id": "coin", "symbol": "btc", "name": "Bitcoin", "market_cap": 10.0, "total_volume": 1.0},
            crypto_cfg=crypto_cfg,
            universe_name="u",
        )
        is None
    )


def test_coingecko_item_to_metadata_filters_exclusions_and_wrapped_assets(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {"parquet_root": tmp_path / "daily", "crypto_root": tmp_path / "crypto", "runs_root": tmp_path / "runs", "universe_csv": tmp_path / "meta" / "universe.csv"},
            "crypto": {"quote_assets": ["USDT"], "excluded_ids": ["coin"], "exclude_wrapped_assets": True},
        }
    )
    crypto_cfg = _read_crypto_config(cfg)
    assert _coingecko_item_to_metadata({"id": "coin", "symbol": "btc", "name": "Bitcoin"}, crypto_cfg=crypto_cfg, universe_name="u") is None
    assert _coingecko_item_to_metadata({"id": "wrapped-bitcoin", "symbol": "wbtc", "name": "Wrapped Bitcoin"}, crypto_cfg=crypto_cfg, universe_name="u") is None
    entry = _coingecko_item_to_metadata({"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "market_cap": 10.0, "total_volume": 10.0}, crypto_cfg=crypto_cfg, universe_name="u")
    assert entry["market_cap_rank"] == 1


def test_numeric_helpers_return_none_for_non_numeric_values():
    assert _as_float("1") is None
    assert _as_float(1) == 1.0
    assert _as_int(1.2) is None
    assert _as_int(3) == 3


def test_workflow_misc_helpers_cover_types_and_universe_show(dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {"parquet_root": tmp_path / "daily", "crypto_root": tmp_path / "crypto", "runs_root": tmp_path / "runs", "universe_csv": tmp_path / "meta" / "universe.csv"},
            "crypto": {"exchange": "binance", "market_type": "spot", "quote_assets": ["USDT"]},
        }
    )
    assert crypto_show_universe_from_config(cfg, symbols_override=["btc/usdt"]) == ["BTC_USDT"]
    assert _as_float(1.2) == 1.2
    assert _as_int("1") is None
    assert _frames_equal(_crypto_frame(), _crypto_frame()) is True


def test_crypto_verify_repair_backfills_dirty_symbols(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.verify as crypto_verify
    import tradinglab_data.crypto.workflows as crypto_workflows

    class FakeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT", "ETH_USDT"]

        def fetch_ohlcv(self, symbol: str, interval: str, *, start=None, end=None, limit=None) -> pl.DataFrame:
            return _crypto_frame(symbol=symbol, timestamps=["2026-04-20T17:00:00", "2026-04-20T18:00:00"])

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeProvider())
    monkeypatch.setattr(crypto_verify, "_is_stale", lambda value, interval, stale_multiple: False)
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )

    result = run_crypto_verify_checks(
        cfg,
        CryptoVerifyConfig(interval="1h", universe="crypto_majors", repair=True, max_missing_ratio=1.0),
    )

    assert "BTC_USDT" in result["repaired_symbols"]
    assert "ETH_USDT" in result["repaired_symbols"]
    assert all(symbol in {"BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "BNB_USDT"} for symbol in result["missing_symbols"])


def test_crypto_verify_repair_recounts_existing_zero_byte_files(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )
    root = tmp_path / "crypto" / "binance" / "spot" / "1h"
    root.mkdir(parents=True, exist_ok=True)
    (root / "BTC_USDT.parquet").write_bytes(b"")
    monkeypatch.setattr("tradinglab_data.crypto.verify.crypto_backfill_from_config", lambda *args, **kwargs: None)

    result = run_crypto_verify_checks(
        cfg,
        CryptoVerifyConfig(interval="1h", universe="crypto_majors", repair=True, max_missing_ratio=1.0),
    )

    assert result["zero_byte_files"] == 1
    assert result["files_present"] >= 1
    assert any(item["symbol"] == "BTC_USDT" and item["exists"] is True for item in result["dirty_symbols"])


def test_crypto_backfill_incremental_uses_existing_timestamp_and_unchanged_path(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    starts: list[datetime | None] = []

    class FakeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT"]

    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeProvider())
    monkeypatch.setattr(
        crypto_workflows,
        "_fetch_symbol_history",
        lambda provider, symbol, interval, start=None, total_limit=None, batch_limit=None: starts.append(start) or pl.DataFrame(schema=_crypto_frame().schema),
    )
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {"exchange": "binance", "market_type": "spot", "default_universe": "crypto_majors", "quote_assets": ["USDT"]},
        }
    )
    root = tmp_path / "crypto" / "binance" / "spot" / "1h"
    root.mkdir(parents=True, exist_ok=True)
    _crypto_frame(symbol="BTC_USDT", timestamps=["2026-04-20T17:00:00", "2026-04-20T18:00:00"]).write_parquet(root / "BTC_USDT.parquet")

    result = crypto_backfill_from_config(cfg, interval="1h", symbols_override=["BTC_USDT"], incremental=True)

    assert result["files_written"] == 0
    assert starts == [datetime(2026, 4, 20, 17, 0, tzinfo=timezone.utc)]


def test_crypto_refresh_universe_breaks_on_empty_page_and_skips_duplicates(monkeypatch, dummy_cfg_factory, tmp_path: Path):
    import tradinglab_data.crypto.workflows as crypto_workflows

    class FakeExchangeProvider:
        def list_symbols(self) -> list[str]:
            return ["BTC_USDT", "ETH_USDT"]

    pages = {
        1: [{"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "market_cap": 10.0, "total_volume": 10.0}],
        2: [{"id": "bitcoin-dup", "symbol": "btc", "name": "Bitcoin Dup", "market_cap_rank": 2, "market_cap": 9.0, "total_volume": 9.0}],
        3: [],
    }
    monkeypatch.setattr(crypto_workflows, "_provider_for", lambda crypto_cfg: FakeExchangeProvider())
    monkeypatch.setattr(crypto_workflows.CoinGeckoProvider, "fetch_markets", lambda self, **kwargs: pages[kwargs["page"]])
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": tmp_path / "daily",
                "crypto_root": tmp_path / "crypto",
                "crypto_registry_json": tmp_path / "meta" / "crypto" / "registry.json",
                "crypto_universe_dir": tmp_path / "meta" / "crypto" / "universes",
                "runs_root": tmp_path / "runs",
                "universe_csv": tmp_path / "meta" / "universe.csv",
            },
            "crypto": {
                "exchange": "binance",
                "market_type": "spot",
                "quote_assets": ["USDT"],
                "universe_refresh_limit": 10,
                "universe_refresh_pages": 3,
            },
        }
    )

    result = crypto_refresh_universe_from_config(cfg, universe="crypto_dynamic")
    assert result["symbols_selected"] == ["BTC_USDT"]
