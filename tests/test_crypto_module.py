from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from tradinglab_data.crypto.providers.binance_ccxt import _normalize_ohlcv_rows
from tradinglab_data.crypto.registry import load_crypto_universes, resolve_crypto_universe
from tradinglab_data.crypto.symbols import normalize_crypto_symbol, split_crypto_symbol, to_source_symbol
from tradinglab_data.crypto.validation import (
    filter_closed_bars,
    merge_crypto_frames,
    normalize_crypto_frame_schema,
    validate_crypto_ohlcv_frame,
)
from tradinglab_data.crypto.verify import CryptoVerifyConfig, run_crypto_verify_checks
from tradinglab_data.crypto.workflows import (
    _coingecko_item_to_metadata,
    _read_crypto_config,
    crypto_backfill_from_config,
    crypto_diff_universe_from_config,
    crypto_inspect_from_config,
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
        [[1713571200000, 1.0, 2.0, 0.5, 1.5, 100.0]],
        symbol="BTC_USDT",
        exchange="binance",
        market_type="spot",
        interval="1h",
        provider="ccxt",
        base_asset="BTC",
        quote_asset="USDT",
        source_symbol="BTC/USDT",
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
    assert frame.get_column("symbol").to_list() == ["BTC_USDT"]


def test_filter_closed_bars_drops_open_bar():
    frame = _crypto_frame(timestamps=["2026-04-20T08:00:00", "2026-04-20T09:00:00"])
    filtered = filter_closed_bars(frame, interval="1h", now_ts=datetime(2026, 4, 20, 9, 30, tzinfo=timezone.utc))
    assert filtered.get_column("timestamp").to_list() == [datetime(2026, 4, 20, 8, 0)]


def test_merge_and_validate_crypto_frames():
    old = _crypto_frame(timestamps=["2026-04-18T00:00:00", "2026-04-18T01:00:00"])
    new = _crypto_frame(timestamps=["2026-04-18T01:00:00", "2026-04-18T02:00:00"])
    merged = merge_crypto_frames(old, new)
    validate_crypto_ohlcv_frame(merged, interval="1h")
    assert merged.height == 3


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
