from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tradinglab_data.symbol_master import (
    build_symbol_master_frame,
    inspect_symbol_master_frame,
    load_symbol_master_frame,
    load_symbol_master_map,
    validate_symbol_master,
    write_symbol_master_frame,
)


def _defaults() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["NASDAQ", "VIE"],
            "country": ["US", "AT"],
            "default_asset_currency": ["USD", "EUR"],
            "default_tax_country": ["US", "AT"],
            "default_lot_size": [1.0, 1.0],
            "default_price_multiplier": [1.0, 1.0],
            "default_asset_class": ["stock", "stock"],
        }
    )


def test_build_symbol_master_applies_exchange_defaults():
    universe = pl.DataFrame({"symbol": ["AAPL"], "exchange": ["NASDAQ"], "country": ["US"], "active": [1]})
    out = build_symbol_master_frame(universe, exchange_defaults=_defaults(), base_currency="EUR")
    row = out.row(0, named=True)
    assert row["asset_currency"] == "USD"
    assert row["tax_country"] == "US"
    assert row["asset_class"] == "stock"


def test_build_symbol_master_fills_missing_country_from_exchange_defaults():
    universe = pl.DataFrame({"symbol": ["EBS.VI"], "exchange": ["VIE"], "country": [""], "active": [1]})
    out = build_symbol_master_frame(universe, exchange_defaults=_defaults(), base_currency="EUR")
    row = out.row(0, named=True)
    assert row["country"] == "AT"
    assert "exchange_defaults" in row["metadata_source"]
    assert "defaulted_country" in row["metadata_quality"]
    assert "non_authoritative_country" in row["metadata_quality"]
    assert "non_authoritative_tax_country" in row["metadata_quality"]


def test_build_symbol_master_uses_universe_currency_when_asset_currency_missing():
    universe = pl.DataFrame(
        {
            "symbol": ["XDW0.L"],
            "exchange": ["LSE"],
            "country": ["GB"],
            "currency": ["USD"],
            "instrument_type": ["etf"],
            "active": [1],
        }
    )
    exchange_defaults = pl.DataFrame(
        {
            "exchange": ["LSE"],
            "country": ["GB"],
            "default_asset_currency": ["GBP"],
            "default_tax_country": ["GB"],
            "default_lot_size": [1.0],
            "default_price_multiplier": [1.0],
            "default_asset_class": ["equity"],
        }
    )
    out = build_symbol_master_frame(universe, exchange_defaults=exchange_defaults, base_currency="EUR")
    row = out.row(0, named=True)
    assert row["asset_currency"] == "USD"
    assert row["base_listing_currency"] == "USD"
    assert row["fx_pair_to_base"] == "USDEUR"
    assert "defaulted_asset_currency" not in row["metadata_quality"]


def test_build_symbol_master_applies_overrides_last():
    universe = pl.DataFrame({"symbol": ["AAPL"], "exchange": ["NASDAQ"], "country": ["US"], "active": [1]})
    overrides = pl.DataFrame({"symbol": ["AAPL"], "tax_country": ["AT"]})
    out = build_symbol_master_frame(universe, exchange_defaults=_defaults(), symbol_overrides=overrides, base_currency="EUR")
    row = out.row(0, named=True)
    assert row["asset_currency"] == "USD"
    assert row["fx_pair_to_base"] == "USDEUR"
    assert row["tax_country"] == "AT"


def test_build_symbol_master_computes_fx_pair_to_base():
    universe = pl.DataFrame({"symbol": ["AAPL"], "exchange": ["NASDAQ"], "country": ["US"], "active": [1]})
    out = build_symbol_master_frame(universe, exchange_defaults=_defaults(), base_currency="EUR")
    assert out.row(0, named=True)["fx_pair_to_base"] == "USDEUR"


def test_build_symbol_master_uses_identity_pair_for_base_currency():
    universe = pl.DataFrame({"symbol": ["EBS.VI"], "exchange": ["VIE"], "country": ["AT"], "active": [1]})
    out = build_symbol_master_frame(universe, exchange_defaults=_defaults(), base_currency="EUR")
    assert out.row(0, named=True)["fx_pair_to_base"] == "EUREUR"


def test_load_symbol_master_rejects_duplicate_symbols_in_strict_mode(tmp_path: Path):
    path = tmp_path / "symbol_master.csv"
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1\n"
        "aapl,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate_symbols"):
        load_symbol_master_frame(path, strict=True)


def test_validate_symbol_master_requires_required_columns(tmp_path: Path):
    path = tmp_path / "symbol_master.csv"
    path.write_text("symbol,exchange\nAAPL,NASDAQ\n", encoding="utf-8")
    errors = validate_symbol_master(path)
    assert any("missing_required_columns" in error for error in errors)


def test_validate_symbol_master_rejects_nonpositive_lot_size(tmp_path: Path):
    path = tmp_path / "symbol_master.csv"
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,USDEUR,0,1\n",
        encoding="utf-8",
    )
    errors = validate_symbol_master(path)
    assert any("lot_size_nonpositive_rows" in error for error in errors)


def test_validate_symbol_master_rejects_bad_fx_pair(tmp_path: Path):
    path = tmp_path / "symbol_master.csv"
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,EURUSD,1,1\n",
        encoding="utf-8",
    )
    errors = validate_symbol_master(path)
    assert any("invalid_fx_pair_rows" in error for error in errors)


def test_load_symbol_master_map_returns_symbol_keyed_dict(tmp_path: Path):
    path = tmp_path / "symbol_master.csv"
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1\n",
        encoding="utf-8",
    )
    loaded = load_symbol_master_map(path)
    assert "AAPL" in loaded
    assert loaded["AAPL"]["asset_currency"] == "USD"


def test_inspect_symbol_master_frame_filters_by_exchange_and_issue(tmp_path: Path):
    universe = pl.DataFrame(
        {
            "symbol": ["AAPL", "EBS.VI"],
            "exchange": ["NASDAQ", "VIE"],
            "country": ["US", ""],
            "active": [1, 1],
        }
    )
    out = build_symbol_master_frame(universe, exchange_defaults=_defaults(), base_currency="EUR")
    path = tmp_path / "symbol_master.csv"
    write_symbol_master_frame(out, path)
    exchange_filtered = inspect_symbol_master_frame(path, exchange="VIE")
    issue_filtered = inspect_symbol_master_frame(path, issues="defaulted_country")
    assert exchange_filtered.height == 1
    assert exchange_filtered.row(0, named=True)["symbol"] == "EBS.VI"
    assert issue_filtered.height == 1
    assert issue_filtered.row(0, named=True)["symbol"] == "EBS.VI"
