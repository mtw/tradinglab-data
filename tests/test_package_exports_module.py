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
    assert pkg.validate_daily_frame is not None
    assert pkg.validate_crypto_frame is not None
    assert pkg.validate_intraday_research_frame is not None
    assert pkg.ARTIFACT_SCHEMA_VERSION == "v0.2.0"


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
