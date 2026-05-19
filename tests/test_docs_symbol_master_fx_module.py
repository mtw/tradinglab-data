from __future__ import annotations

from pathlib import Path

import polars as pl

from tradinglab_data.schema import render_schema_markdown
from tradinglab_data.symbol_master import load_exchange_defaults, load_symbol_master_frame, load_symbol_overrides

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_documents_symbol_master_and_fx_commands():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "Artifact schema version: `v0.3.0`" in readme
    assert "build-symbol-master" in readme
    assert "validate-symbol-master" in readme
    assert "fx-backfill" in readme
    assert "fx-update" in readme
    assert "fx-validate" in readme
    assert "fx-inspect" in readme
    assert "Daily OHLC `currency` remains provider-derived diagnostic data." in readme
    assert "non_authoritative_country" in readme
    assert "non_authoritative_tax_country" in readme


def test_api_contract_documents_authoritative_symbol_master_and_fx_surface():
    text = (REPO_ROOT / "docs" / "API_CONTRACT.md").read_text(encoding="utf-8")
    assert "current value: `v0.3.0`" in text
    assert "symbol_master.csv" in text
    assert "exchange_defaults.csv" in text
    assert "symbol_overrides.csv" in text
    assert "<paths.fx_daily_root>/<PAIR>.parquet" in text
    assert "daily OHLC `currency` is diagnostic provider data" in text
    assert "`USDEUR` means EUR value of `1` USD" in text
    assert "non_authoritative_country" in text
    assert "non_authoritative_tax_country" in text


def test_workflows_and_troubleshooting_cover_symbol_master_and_fx_paths():
    workflows = (REPO_ROOT / "docs" / "WORKFLOWS.md").read_text(encoding="utf-8")
    troubleshooting = (REPO_ROOT / "docs" / "TROUBLESHOOTING.md").read_text(encoding="utf-8")
    checklist = (REPO_ROOT / "docs" / "CONSUMER_COMPATIBILITY_CHECKLIST.md").read_text(encoding="utf-8")

    assert "build-symbol-master" in workflows
    assert "fx-backfill" in workflows
    assert "fx-update" in workflows
    assert "fx-validate" in workflows
    assert "fx-inspect" in workflows
    assert "non_authoritative_country" in workflows
    assert "non_authoritative_tax_country" in workflows
    assert "positive `retention_days` trims existing parquet even when Yahoo returns no new rows for a symbol during an update" in workflows
    assert "a symbol is reported as `unchanged` only when the fetched frame is empty and the retention trim leaves the existing file unchanged" in workflows

    assert "Symbol Master Validation Failures" in troubleshooting
    assert "FX Pair Direction Errors" in troubleshooting
    assert "Missing FX Parquet" in troubleshooting

    assert "load `symbol_master.csv` before portfolio simulation" in checklist
    assert "treat `fx_pair_to_base` as authoritative" in checklist
    assert "never infer asset currency from ticker suffixes" in checklist


def test_parquet_schema_doc_tracks_rendered_symbol_master_and_fx_sections():
    doc_text = (REPO_ROOT / "docs" / "PARQUET_SCHEMA.md").read_text(encoding="utf-8")
    rendered = render_schema_markdown()

    for needle in [
        "## FX Daily",
        "## Symbol Master CSV",
        "| `pair` | `String` |",
        "| `fx_pair_to_base` | `String` |",
        "Artifact schema version: `v0.3.0`",
        "non_authoritative_country",
        "non_authoritative_tax_country",
    ]:
        assert needle in doc_text
    for needle in [
        "## FX Daily",
        "## Symbol Master CSV",
        "| `pair` | `String` |",
        "| `fx_pair_to_base` | `String` |",
    ]:
        assert needle in rendered


def test_example_metadata_files_are_loadable_and_match_expected_contract_shape():
    exchange_defaults = load_exchange_defaults(REPO_ROOT / "examples" / "meta" / "exchange_defaults.csv")
    symbol_overrides = load_symbol_overrides(REPO_ROOT / "examples" / "meta" / "symbol_overrides.csv")
    symbol_master = load_symbol_master_frame(REPO_ROOT / "examples" / "meta" / "symbol_master.example.csv")

    assert exchange_defaults.height >= 4
    assert symbol_overrides.height >= 1
    assert symbol_master.height >= 4
    assert "default_asset_currency" in exchange_defaults.columns
    assert "fx_pair_to_base" in symbol_overrides.columns
    assert "fx_pair_to_base" in symbol_master.columns
    assert "AAPL" in symbol_master.get_column("symbol").to_list()
    assert "EUREUR" in symbol_master.get_column("fx_pair_to_base").to_list()


def test_example_symbol_master_contains_positive_numeric_metadata():
    symbol_master = pl.read_csv(REPO_ROOT / "examples" / "meta" / "symbol_master.example.csv")
    assert symbol_master.select((pl.col("lot_size").cast(pl.Float64) > 0).all()).item() is True
    assert symbol_master.select((pl.col("price_multiplier").cast(pl.Float64) > 0).all()).item() is True
