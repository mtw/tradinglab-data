from __future__ import annotations

from pathlib import Path

import polars as pl

import tradinglab_data as pkg
import tradinglab_data.data_yf as data_yf
import tradinglab_data.ticker_map as ticker_map


def test_top_level_exports_include_public_helpers():
    assert pkg.load_universe is not None
    assert pkg.build_universe is not None
    assert pkg.compatibility_manifest is not None
    assert pkg.generate_parquet_store_report is not None
    assert pkg.generate_universe_consistency_report is not None
    assert pkg.backfill_extended_hours_from_config is not None
    assert pkg.intraday_live_update_from_config is not None
    assert pkg.intraday_sync_from_config is not None
    assert pkg.intraday_research_update_from_config is not None
    assert pkg.crypto_backfill_from_config is not None
    assert pkg.crypto_show_universe_from_config is not None
    assert pkg.crypto_diff_universe_from_config is not None
    assert pkg.crypto_inspect_from_config is not None
    assert pkg.crypto_prune_from_config is not None
    assert pkg.crypto_refresh_universe_from_config is not None
    assert pkg.CompatibilityManifest is not None
    assert pkg.StoreIntegrityReport is not None
    assert pkg.StoreIntegritySection is not None
    assert pkg.run_parquet_sanity_checks is not None
    assert pkg.render_universe_consistency_markdown is not None
    assert pkg.build_symbol_master_frame is not None
    assert pkg.inspect_symbol_master_frame is not None
    assert pkg.load_symbol_master_frame is not None
    assert pkg.load_fx_pair is not None
    assert pkg.get_universe_symbols is not None
    assert pkg.get_total_returns is not None
    assert pkg.get_adjusted_prices is not None
    assert pkg.get_market_caps is not None
    assert pkg.get_sector_assignments is not None
    assert pkg.get_index_returns is not None
    assert pkg.sync_market_data_from_config is not None
    assert pkg.sync_market_caps_yahoo is not None
    assert pkg.sync_sector_assignments_yahoo is not None
    assert pkg.sync_index_returns_yahoo is not None
    assert pkg.validate_market_data_from_config is not None
    assert pkg.DataNotFoundError is not None
    assert pkg.UniverseNotFoundError is not None
    assert pkg.sync_fx_pair_yahoo is not None
    assert pkg.available_fx_pairs is not None
    assert pkg.validate_daily_frame is not None
    assert pkg.validate_crypto_frame is not None
    assert pkg.validate_fx_daily_frame is not None
    assert pkg.validate_market_cap_frame is not None
    assert pkg.validate_sector_assignment_frame is not None
    assert pkg.validate_index_return_frame is not None
    assert pkg.validate_symbol_master_frame is not None
    assert pkg.validate_intraday_live_frame is not None
    assert pkg.validate_intraday_research_frame is not None
    assert isinstance(pkg.ARTIFACT_SCHEMA_VERSION, str)
    assert pkg.ARTIFACT_SCHEMA_VERSION.startswith("v")
    assert pkg.DATAFRAME_POLICY == "polars-first"


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


def test_store_report_exports_move_with_contract_version():
    assert pkg.generate_parquet_store_report.__name__ == "generate_parquet_store_report"
    assert pkg.StoreIntegrityReport.__name__ == "StoreIntegrityReport"
    assert pkg.ARTIFACT_SCHEMA_VERSION.startswith("v0.")


def test_compatibility_manifest_export_contains_artifact_version():
    manifest = pkg.compatibility_manifest()
    assert manifest["artifact_schema_version"] == pkg.ARTIFACT_SCHEMA_VERSION
    assert manifest["dataframe_policy"] == pkg.DATAFRAME_POLICY
