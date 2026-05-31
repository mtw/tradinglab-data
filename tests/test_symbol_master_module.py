from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import tradinglab_data.symbol_master as symbol_master
from tradinglab_data.symbol_master import (
    build_symbol_master_frame,
    inspect_symbol_master_frame,
    load_exchange_defaults,
    load_symbol_master_frame,
    load_symbol_master_map,
    load_symbol_overrides,
    normalize_currency,
    normalize_symbol,
    require_exchange_defaults_frame,
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


def test_symbol_master_basic_helpers():
    assert normalize_currency(None) == ""
    assert normalize_symbol(None) == ""
    assert symbol_master.normalize_fx_pair(" usd eur ") == "USD EUR"
    assert symbol_master.make_fx_pair_to_base(" usd ", " eur ") == "USDEUR"
    assert symbol_master._csv_flags(["a", "", "b"]) == "a,b"


def test_load_exchange_defaults_and_overrides_cover_missing_empty_and_optional_columns(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_exchange_defaults(tmp_path / "missing.csv")

    empty_defaults = tmp_path / "exchange_defaults_empty.csv"
    empty_defaults.write_text("exchange,country\n", encoding="utf-8")
    assert load_exchange_defaults(empty_defaults).is_empty()

    defaults_path = tmp_path / "exchange_defaults.csv"
    defaults_path.write_text("exchange,country\n nasdaq , us \n", encoding="utf-8")
    defaults = load_exchange_defaults(defaults_path)
    row = defaults.row(0, named=True)
    assert row["exchange"] == "NASDAQ"
    assert row["default_asset_currency"] == ""

    assert load_symbol_overrides(tmp_path / "missing_overrides.csv").is_empty()

    empty_overrides = tmp_path / "symbol_overrides_empty.csv"
    empty_overrides.write_text("symbol,exchange\n", encoding="utf-8")
    assert load_symbol_overrides(empty_overrides).is_empty()

    overrides_path = tmp_path / "symbol_overrides.csv"
    overrides_path.write_text("symbol,exchange,lot_size\naapl,nasdaq,5\n", encoding="utf-8")
    overrides = load_symbol_overrides(overrides_path)
    override = overrides.row(0, named=True)
    assert override["symbol"] == "AAPL"
    assert override["exchange"] == "NASDAQ"
    assert override["lot_size"] == 5.0


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


def test_load_symbol_master_frame_and_internal_helpers_cover_missing_paths_and_casts(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_symbol_master_frame(tmp_path / "missing.csv")

    path = tmp_path / "symbol_master.csv"
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier,notes\n"
        "aapl,nasdaq,us,usd,usd,us,stock,USDEUR,2,3,note\n",
        encoding="utf-8",
    )
    frame = load_symbol_master_frame(path, strict=False)
    row = frame.row(0, named=True)
    assert row["symbol"] == "AAPL"
    assert row["exchange"] == "NASDAQ"
    assert row["lot_size"] == 2.0

    helper_df = pl.DataFrame({"present": [" x "], "numeric": ["4"]})
    assert symbol_master._optional_column(helper_df, "present").meta.output_name() == "present"
    assert (
        helper_df.select(symbol_master._string_value_or_null(helper_df, "present").alias("value")).item()
        == "x"
    )
    assert (
        helper_df.select(symbol_master._string_value_or_null(helper_df, "missing").alias("value")).item()
        is None
    )
    assert (
        helper_df.select(symbol_master._float_value_or_null(helper_df, "numeric").alias("value")).item()
        == 4.0
    )
    assert (
        helper_df.select(symbol_master._float_value_or_null(helper_df, "missing").alias("value")).item()
        is None
    )
    assert symbol_master._normalize_string_columns(pl.DataFrame({"name": [" a "]}), ["name"], uppercase=True).item(0, 0) == "A"


def test_build_symbol_master_handles_empty_missing_symbol_and_non_strict_defaults():
    empty = build_symbol_master_frame(pl.DataFrame(), exchange_defaults=_defaults(), base_currency="EUR")
    assert empty.is_empty()

    with pytest.raises(ValueError, match="universe_frame must contain symbol"):
        build_symbol_master_frame(pl.DataFrame({"exchange": ["NASDAQ"]}), exchange_defaults=_defaults())

    with pytest.raises(FileNotFoundError, match="exchange_defaults.csv is required"):
        build_symbol_master_frame(pl.DataFrame({"symbol": ["AAPL"]}), exchange_defaults=None, strict=True)

    out = build_symbol_master_frame(pl.DataFrame({"symbol": ["AAPL"]}), exchange_defaults=None, strict=False)
    row = out.row(0, named=True)
    assert row["active"] == 1
    assert row["source"] == ""


def test_build_symbol_master_applies_missing_exchange_column_and_numeric_overrides():
    universe = pl.DataFrame({"symbol": ["AAPL"], "country": ["US"]})
    overrides = pl.DataFrame({"symbol": ["AAPL"], "lot_size": [5.0], "price_multiplier": [2.0], "notes": ["manual"]})
    out = build_symbol_master_frame(
        universe,
        exchange_defaults=_defaults(),
        symbol_overrides=overrides,
        base_currency="EUR",
        strict=False,
    )
    row = out.row(0, named=True)
    assert row["exchange"] == ""
    assert row["lot_size"] == 5.0
    assert row["price_multiplier"] == 2.0
    assert row["notes"] == "manual"


def test_build_symbol_master_strict_raises_on_invalid_built_output():
    universe = pl.DataFrame({"symbol": ["AAPL"], "country": ["US"]})

    with pytest.raises(ValueError, match="empty_exchange_rows"):
        build_symbol_master_frame(universe, exchange_defaults=_defaults(), base_currency="EUR", strict=True)


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


def test_validate_symbol_master_and_require_defaults_report_file_errors(tmp_path: Path):
    missing = tmp_path / "missing.csv"
    assert validate_symbol_master(missing) == [str(missing)]

    defaults_path = tmp_path / "exchange_defaults.csv"
    defaults_path.write_text("exchange,country\nNASDAQ,US\n", encoding="utf-8")
    assert require_exchange_defaults_frame(defaults_path).height == 1

    with pytest.raises(FileNotFoundError, match="required in strict mode"):
        require_exchange_defaults_frame(tmp_path / "missing_defaults.csv", strict=True)

    assert require_exchange_defaults_frame(tmp_path / "missing_defaults.csv", strict=False).is_empty()


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


def test_inspect_symbol_master_frame_filters_by_fx_pair_and_symbols(tmp_path: Path):
    universe = pl.DataFrame(
        {
            "symbol": ["AAPL", "EBS.VI"],
            "exchange": ["NASDAQ", "VIE"],
            "country": ["US", "AT"],
            "active": [1, 1],
        }
    )
    out = build_symbol_master_frame(universe, exchange_defaults=_defaults(), base_currency="EUR")
    path = tmp_path / "symbol_master.csv"
    write_symbol_master_frame(out, path)

    fx_filtered = inspect_symbol_master_frame(path, fx_pair="usdeur")
    symbol_filtered = inspect_symbol_master_frame(path, symbols=["ebs.vi"])

    assert fx_filtered.height == 1
    assert fx_filtered.row(0, named=True)["symbol"] == "AAPL"
    assert symbol_filtered.height == 1
    assert symbol_filtered.row(0, named=True)["symbol"] == "EBS.VI"
