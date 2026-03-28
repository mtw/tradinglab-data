from __future__ import annotations

from pathlib import Path

import polars as pl

import tradinglab_data as pkg
import tradinglab_data.data_yf as data_yf
import tradinglab_data.ticker_map as ticker_map


def test_top_level_exports_include_public_helpers():
    assert pkg.load_universe is not None
    assert pkg.build_universe is not None
    assert pkg.run_parquet_sanity_checks is not None
    assert pkg.validate_daily_frame is not None
    assert pkg.API_CONTRACT_VERSION == "v0.1.0"


def test_clear_currency_cache_clears_module_cache():
    data_yf._CURRENCY_CACHE["AAPL"] = "USD"
    pkg.clear_currency_cache()
    assert data_yf._CURRENCY_CACHE == {}


def test_clear_override_cache_resets_cache_markers(tmp_path: Path):
    override_file = tmp_path / "ticker_overrides.csv"
    override_file.write_text("raw,yahoo\nABC,ABC.DE\n", encoding="utf-8")
    ticker_map._load_overrides(override_file)
    pkg.clear_override_cache()
    assert ticker_map._OVERRIDE_CACHE is None
    assert ticker_map._OVERRIDE_CACHE_SOURCE is None


def test_lazy_schema_validator_export_runs():
    df = pl.DataFrame(
        {
            "date": [None],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "adj_close": [1.0],
            "volume": [100.0],
            "currency": ["USD"],
        },
        schema_overrides={"date": pl.Datetime},
    )
    pkg.validate_daily_frame(df)
