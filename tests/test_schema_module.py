from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

import tradinglab_data.schema as schema_mod
from tradinglab_data.contracts import ARTIFACT_SCHEMA_VERSION, DATAFRAME_POLICY
from tradinglab_data.schema import (
    _normalize_text,
    compatibility_manifest,
    render_schema_json,
    render_schema_markdown,
    schema_manifest,
    validate_alerts_frame,
    validate_crypto_frame,
    validate_daily_frame,
    validate_frame_schema,
    validate_fx_daily_frame,
    validate_index_return_frame,
    validate_intraday_frame,
    validate_intraday_live_frame,
    validate_intraday_research_frame,
    validate_market_cap_frame,
    validate_moves_frame,
    validate_sector_assignment_frame,
    validate_symbol_master_frame,
)


def test_schema_manifest_has_daily_and_intraday():
    manifest = schema_manifest()
    assert "daily" in manifest
    assert "intraday" in manifest
    assert "intraday_research" in manifest
    assert "intraday_live" in manifest
    assert "crypto" in manifest
    assert "market_cap" in manifest
    assert "sector_assignments" in manifest
    assert "index_returns" in manifest
    assert "date" in manifest["daily"]
    assert "timestamp" in manifest["crypto"]
    assert manifest["artifact_schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert manifest["dataframe_policy"] == DATAFRAME_POLICY
    assert "package_version" in manifest
    assert "fx_daily" in manifest
    assert "symbol_master" in manifest


def test_render_schema_markdown_contains_header():
    text = render_schema_markdown()
    assert "# Data Parquet Schema" in text
    assert f"Dataframe policy: `{DATAFRAME_POLICY}`" in text
    assert "## Daily" in text
    assert "non_authoritative_country" in text
    assert "non_authoritative_tax_country" in text
    assert "Polars-first" in text


def test_render_schema_json_contains_adj_close():
    text = render_schema_json()
    assert '"adj_close"' in text
    assert '"crypto"' in text
    assert '"fx_daily"' in text
    assert '"market_cap"' in text
    assert '"sector_assignments"' in text
    assert '"index_returns"' in text
    assert '"package_version"' in text
    assert '"artifact_schema_version"' in text
    assert '"dataframe_policy"' in text


def test_package_version_none_branch(monkeypatch):
    monkeypatch.setattr(schema_mod, "version", lambda name: (_ for _ in ()).throw(schema_mod.PackageNotFoundError()))
    assert compatibility_manifest()["package_version"] is None


def test_validate_daily_frame_accepts_canonical_schema():
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
    assert validate_daily_frame(df) == []


def test_validate_frame_schema_reports_dtype_and_extra_columns_without_raising():
    df = pl.DataFrame({"date": ["2026-03-27"], "extra": [1]})

    errors = validate_frame_schema(
        df,
        {"date": pl.Datetime},
        allow_extra_columns=False,
        raise_on_error=False,
    )

    assert errors == ["dtype=['date:String!=Datetime']", "extra=['extra']"]
    with pytest.raises(ValueError, match="missing="):
        validate_frame_schema(pl.DataFrame({"other": [1]}), {"date": pl.Datetime})
    assert validate_frame_schema(
        pl.DataFrame({"date": [datetime(2026, 1, 1)]}),
        {"date": pl.Datetime},
        allow_extra_columns=False,
        raise_on_error=False,
    ) == []

    class BrokenType:
        def base_type(self):
            raise TypeError("boom")

    assert schema_mod._dtype_matches(BrokenType(), pl.Datetime) is False


def test_validate_moves_frame_rejects_missing_columns():
    df = pl.DataFrame({"symbol": ["AAPL"], "pct_move": [2.0]})
    with pytest.raises(ValueError, match="missing="):
        validate_moves_frame(df)


def test_validate_crypto_frame_accepts_canonical_schema():
    df = pl.DataFrame(
        {
            "timestamp": [None],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "volume": [100.0],
            "provider": ["ccxt"],
            "exchange": ["binance"],
            "market_type": ["spot"],
            "symbol": ["BTC_USDT"],
            "base_asset": ["BTC"],
            "quote_asset": ["USDT"],
            "interval": ["1h"],
            "is_closed": [True],
            "ingested_at": [None],
            "source_symbol": ["BTC/USDT"],
        },
        schema_overrides={"timestamp": pl.Datetime, "ingested_at": pl.Datetime},
    )
    assert validate_crypto_frame(df) == []


def test_validate_daily_frame_rejects_wrong_dtype():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "adj_close": [1.0],
            "volume": [100.0],
            "currency": ["USD"],
        }
    )
    with pytest.raises(ValueError, match="dtype="):
        validate_daily_frame(df)


def test_validate_intraday_research_frame_accepts_canonical_schema():
    df = pl.DataFrame(
        {
            "timestamp": ["2026-03-27T13:30:00"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "volume": [100.0],
            "currency": ["USD"],
            "symbol": ["SPY"],
            "interval": ["5m"],
            "provider": ["yahoo"],
            "session": ["regular"],
            "session_date": ["2026-03-27"],
            "is_regular_session": [True],
            "ingested_at": ["2026-03-27T20:01:00"],
        },
    ).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("session_date").str.strptime(pl.Date, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    assert validate_intraday_research_frame(df) == []


def test_validate_intraday_research_frame_accepts_non_5m_interval():
    df = pl.DataFrame(
        {
            "timestamp": ["2026-03-27T13:30:00"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "volume": [100.0],
            "currency": ["USD"],
            "symbol": ["SPY"],
            "interval": ["1m"],
            "provider": ["yahoo"],
            "session": ["regular"],
            "session_date": ["2026-03-27"],
            "is_regular_session": [True],
            "ingested_at": ["2026-03-27T20:01:00"],
        },
    ).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("session_date").str.strptime(pl.Date, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    assert validate_intraday_research_frame(df) == []


def test_validate_intraday_research_frame_rejects_bad_metadata():
    df = pl.DataFrame(
        {
            "timestamp": [None, None],
            "open": [1.0, 1.1],
            "high": [1.2, 1.3],
            "low": [0.9, 1.0],
            "close": [1.1, 1.2],
            "volume": [100.0, 110.0],
            "currency": ["USD", "USD"],
            "symbol": ["SPY", "QQQ"],
            "interval": ["5m", "5m"],
            "provider": ["yahoo", "yahoo"],
            "session": ["regular", "pre"],
            "session_date": [None, None],
            "is_regular_session": [True, False],
            "ingested_at": [None, None],
        },
        schema_overrides={"timestamp": pl.Datetime, "session_date": pl.Date, "ingested_at": pl.Datetime},
    )
    with pytest.raises(ValueError, match="Intraday research frame does not match contract"):
        validate_intraday_research_frame(df)


def test_validate_intraday_live_frame_accepts_canonical_schema():
    df = pl.DataFrame(
        {
            "timestamp": ["2026-03-27T13:30:00"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "volume": [100.0],
            "currency": ["USD"],
            "symbol": ["SPY"],
            "interval": ["5m"],
            "provider": ["yahoo"],
            "session": ["regular"],
            "session_date": ["2026-03-27"],
            "is_regular_session": [True],
            "is_closed_bar": [True],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("session_date").str.strptime(pl.Date, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    assert validate_intraday_live_frame(df) == []


def test_validate_alerts_frame_rejects_wrong_dtype():
    df = pl.DataFrame(
        {
            "symbol": ["AAPL"],
            "ref_close": [1.0],
            "last_price": [1.1],
            "pct_move": [10.0],
            "last_volume": [100.0],
            "currency": ["USD"],
            "last_ts": ["2026-03-27T08:00:00"],
            "session": ["pre"],
        }
    )
    with pytest.raises(ValueError, match="dtype="):
        validate_alerts_frame(df)


def test_schema_manifest_contains_artifact_schema_version():
    manifest = schema_manifest()
    assert manifest["artifact_schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert manifest["dataframe_policy"] == DATAFRAME_POLICY


def test_compatibility_manifest_separates_package_and_artifact_versions():
    manifest = compatibility_manifest()
    assert manifest["package_name"] == "tradinglab-data"
    assert manifest["python_package_name"] == "tradinglab_data"
    assert manifest["artifact_schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert manifest["dataframe_policy"] == DATAFRAME_POLICY
    assert "daily_parquet" in manifest["artifact_families"]
    assert "crypto_parquet" in manifest["artifact_families"]
    assert "fx_daily_parquet" in manifest["artifact_families"]
    assert "market_cap_parquet" in manifest["artifact_families"]
    assert "sector_assignments_csv" in manifest["artifact_families"]
    assert "index_return_parquet" in manifest["artifact_families"]
    assert "symbol_master_csv" in manifest["artifact_families"]
    assert manifest["artifact_families"]["daily_parquet"]["category"] == "parquet"
    assert manifest["artifact_families"]["parquet_store_report_markdown"]["category"] == "markdown"
    assert set(manifest) == {
        "package_name",
        "python_package_name",
        "package_version",
        "artifact_schema_version",
        "dataframe_policy",
        "artifact_families",
    }


def test_validate_fx_daily_frame_accepts_valid_frame():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "open": [0.92],
            "high": [0.93],
            "low": [0.91],
            "close": [0.925],
            "provider": ["yahoo"],
            "pair": ["USDEUR"],
            "base_currency": ["USD"],
            "quote_currency": ["EUR"],
            "source_symbol": ["USDEUR=X"],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    assert validate_fx_daily_frame(df, pair="USDEUR") == []


def test_validate_fx_daily_frame_counts_invalid_rows_once():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "open": [-0.92],
            "high": [0.93],
            "low": [0.91],
            "close": [0.925],
            "provider": ["yahoo"],
            "pair": ["GBPEUR"],
            "base_currency": ["USD"],
            "quote_currency": ["EUR"],
            "source_symbol": ["GBPEUR=X"],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )

    assert validate_fx_daily_frame(df, pair="USDEUR") == ["invalid_rows=1"]


def test_validate_fx_daily_frame_reports_missing_columns_without_raising():
    assert validate_fx_daily_frame(pl.DataFrame({"date": []}), raise_on_error=False) == [
        "missing_required_columns=open,high,low,close,provider,pair,base_currency,quote_currency,source_symbol,ingested_at"
    ]
    bad_dtype = pl.DataFrame({"date": ["2026-03-27"], "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "provider": ["x"], "pair": ["USDEUR"], "base_currency": ["USD"], "quote_currency": ["EUR"], "source_symbol": ["x"], "ingested_at": ["2026-03-27T00:00:00"]})
    assert any(item.startswith("dtype=") for item in validate_fx_daily_frame(bad_dtype, raise_on_error=False))
    empty = pl.DataFrame(
        schema={
            "date": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "provider": pl.String,
            "pair": pl.String,
            "base_currency": pl.String,
            "quote_currency": pl.String,
            "source_symbol": pl.String,
            "ingested_at": pl.Datetime,
        }
    )
    assert validate_fx_daily_frame(empty, raise_on_error=False) == []
    unsorted_dup = pl.DataFrame(
        {
            "date": ["2026-03-28", "2026-03-27", "2026-03-27"],
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
            "provider": ["x", "x", "x"],
            "pair": ["USDEUR", "USDEUR", "USDEUR"],
            "base_currency": ["USD", "USD", "USD"],
            "quote_currency": ["EUR", "EUR", "EUR"],
            "source_symbol": ["x", "x", "x"],
            "ingested_at": ["2026-03-28T00:00:00", "2026-03-27T00:00:00", "2026-03-27T00:00:00"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False), pl.col("ingested_at").str.strptime(pl.Datetime, strict=False))
    errors = validate_fx_daily_frame(unsorted_dup, raise_on_error=False)
    assert "dates_not_sorted" in errors
    assert "duplicate_dates=1" in errors


def test_validate_market_cap_frame_accepts_valid_frame():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "symbol": ["AAPL"],
            "market_cap_usd_millions": [3000000.0],
            "provider": ["fixture"],
            "source_symbol": ["AAPL"],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    assert validate_market_cap_frame(df) == []


def test_validate_market_cap_frame_rejects_mixed_symbols():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27", "2026-03-28"],
            "symbol": ["AAPL", "MSFT"],
            "market_cap_usd_millions": [3000000.0, 2800000.0],
            "provider": ["fixture", "fixture"],
            "source_symbol": ["AAPL", "MSFT"],
            "ingested_at": ["2026-03-27T20:01:00", "2026-03-28T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    with pytest.raises(ValueError, match="mixed_symbols=2"):
        validate_market_cap_frame(df)


def test_validate_market_cap_frame_reports_sorting_and_invalid_rows_without_raising():
    df = pl.DataFrame(
        {
            "date": ["2026-03-28", "2026-03-27"],
            "symbol": ["AAPL", "AAPL"],
            "market_cap_usd_millions": [1.0, -1.0],
            "provider": ["fixture", ""],
            "source_symbol": ["AAPL", ""],
            "ingested_at": ["2026-03-28T20:01:00", "2026-03-27T20:01:00"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False), pl.col("ingested_at").str.strptime(pl.Datetime, strict=False))

    assert validate_market_cap_frame(df, raise_on_error=False) == ["dates_not_sorted", "invalid_rows=1"]
    empty = pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "market_cap_usd_millions": pl.Float64, "provider": pl.String, "source_symbol": pl.String, "ingested_at": pl.Datetime})
    assert validate_market_cap_frame(empty, raise_on_error=False) == []
    dup = df.vstack(df.head(1))
    assert "duplicate_dates=1" in validate_market_cap_frame(dup, raise_on_error=False)


def test_validate_sector_assignment_frame_accepts_valid_frame():
    df = pl.DataFrame(
        {
            "symbol": ["AAPL"],
            "sector": ["Information Technology"],
            "effective_start": ["2026-01-01"],
            "effective_end": ["2026-12-31"],
            "source": ["fixture"],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("effective_start").str.strptime(pl.Date, strict=False),
        pl.col("effective_end").str.strptime(pl.Date, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    assert validate_sector_assignment_frame(df) == []


def test_validate_sector_assignment_frame_reports_invalid_rows_without_raising():
    df = pl.DataFrame(
        {
            "symbol": [""],
            "sector": ["Bad Sector"],
            "effective_start": ["2026-01-01"],
            "effective_end": ["2026-12-31"],
            "source": ["fixture"],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("effective_start").str.strptime(pl.Date, strict=False),
        pl.col("effective_end").str.strptime(pl.Date, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )

    assert validate_sector_assignment_frame(df, raise_on_error=False) == ["invalid_rows=1"]
    assert validate_sector_assignment_frame(pl.DataFrame(schema={"symbol": pl.String, "sector": pl.String, "effective_start": pl.Date, "effective_end": pl.Date, "source": pl.String, "ingested_at": pl.Datetime}), raise_on_error=False) == []
    assert any(item.startswith("missing=") for item in validate_sector_assignment_frame(pl.DataFrame({"symbol": ["A"]}), raise_on_error=False))


def test_validate_index_return_frame_accepts_valid_frame():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "index_id": ["SPX"],
            "return": [0.01],
            "total_return_level": [1000.0],
            "provider": ["fixture"],
            "source_symbol": ["SPXTR"],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("return").cast(pl.Float64),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    assert validate_index_return_frame(df) == []


def test_validate_index_return_frame_rejects_null_returns_after_first_row():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27", "2026-03-30"],
            "index_id": ["SPX", "SPX"],
            "return": [None, None],
            "total_return_level": [1000.0, 1001.0],
            "provider": ["fixture", "fixture"],
            "source_symbol": ["SPXTR", "SPXTR"],
            "ingested_at": ["2026-03-27T20:01:00", "2026-03-30T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("return").cast(pl.Float64),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    with pytest.raises(ValueError, match="null_returns_after_first=1"):
        validate_index_return_frame(df)


def test_validate_index_return_frame_rejects_mixed_index_ids():
    df = pl.DataFrame(
        {
            "date": ["2026-03-27", "2026-03-28"],
            "index_id": ["SPX", "NDX"],
            "return": [0.01, 0.02],
            "total_return_level": [1000.0, 1020.0],
            "provider": ["fixture", "fixture"],
            "source_symbol": ["SPXTR", "NDXTR"],
            "ingested_at": ["2026-03-27T20:01:00", "2026-03-28T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )
    with pytest.raises(ValueError, match="mixed_index_ids=2"):
        validate_index_return_frame(df)


def test_validate_index_return_frame_reports_multiple_problem_types_without_raising():
    df = pl.DataFrame(
        {
            "date": ["2026-03-28", "2026-03-27"],
            "index_id": ["SPX", "NDX"],
            "return": [-1.5, None],
            "total_return_level": [1000.0, 1020.0],
            "provider": ["fixture", ""],
            "source_symbol": ["SPXTR", ""],
            "ingested_at": ["2026-03-28T20:01:00", "2026-03-27T20:01:00"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False), pl.col("return").cast(pl.Float64), pl.col("ingested_at").str.strptime(pl.Datetime, strict=False))

    assert validate_index_return_frame(df, raise_on_error=False) == [
        "dates_not_sorted",
        "mixed_index_ids=2",
        "invalid_rows=2",
        "null_returns_after_first=1",
    ]
    assert validate_index_return_frame(pl.DataFrame(schema={"date": pl.Datetime, "index_id": pl.String, "return": pl.Float64, "total_return_level": pl.Float64, "provider": pl.String, "source_symbol": pl.String, "ingested_at": pl.Datetime}), raise_on_error=False) == []
    dup = df.head(1).vstack(df.head(1))
    assert "duplicate_dates=1" in validate_index_return_frame(dup, raise_on_error=False)
    bad_dtype = pl.DataFrame({"date": ["2026-01-01"], "index_id": ["SPX"], "return": [0.1], "total_return_level": [100.0], "provider": ["fixture"], "source_symbol": ["SPXTR"], "ingested_at": [None]})
    assert any(item.startswith("dtype=") for item in validate_index_return_frame(bad_dtype, raise_on_error=False))


def test_validate_symbol_master_frame_accepts_valid_frame():
    df = pl.DataFrame(
        {
            "symbol": ["AAPL"],
            "exchange": ["NASDAQ"],
            "country": ["US"],
            "asset_currency": ["USD"],
            "base_listing_currency": ["USD"],
            "tax_country": ["US"],
            "asset_class": ["stock"],
            "fx_pair_to_base": ["USDEUR"],
            "lot_size": [1.0],
            "price_multiplier": [1.0],
            "active": [1],
        }
    )
    assert validate_symbol_master_frame(df) == []


def test_validate_symbol_master_frame_non_strict_is_warn_only():
    df = pl.DataFrame(
        {
            "symbol": ["AAPL"],
            "exchange": ["NASDAQ"],
            "country": [""],
            "asset_currency": ["USD"],
            "base_listing_currency": ["USD"],
            "tax_country": ["US"],
            "asset_class": ["stock"],
            "fx_pair_to_base": ["USDEUR"],
            "lot_size": [1.0],
            "price_multiplier": [1.0],
            "active": [1],
        }
    )

    assert validate_symbol_master_frame(df, strict=True) == ["empty_country_rows=1"]
    assert validate_symbol_master_frame(df, strict=False) == []
    assert validate_symbol_master_frame(pl.DataFrame({"symbol": ["AAPL"]}), strict=True) == [
        "missing_required_columns=exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier"
    ]
    assert validate_symbol_master_frame(pl.DataFrame({"symbol": ["AAPL"]}), strict=False) == []


def test_validate_symbol_master_frame_reports_multiple_field_errors():
    df = pl.DataFrame(
        {
            "symbol": ["dup", "DUP"],
            "exchange": ["NASDAQ", "NASDAQ"],
            "country": ["US", "US"],
            "asset_currency": ["eur", "EUR"],
            "base_listing_currency": ["EUR", "EUR"],
            "tax_country": ["at", "AT"],
            "asset_class": ["stock", "stock"],
            "fx_pair_to_base": ["BAD", "USDEUR"],
            "lot_size": [0.0, 1.0],
            "price_multiplier": ["bad", "1"],
            "active": [1, 1],
        }
    )

    errors = validate_symbol_master_frame(df, strict=True)
    assert "duplicate_symbols" in errors
    assert "asset_currency_not_uppercase=1" in errors
    assert "tax_country_not_uppercase=1" in errors
    assert "lot_size_nonpositive_rows=1" in errors
    assert "price_multiplier_not_float" in errors
    assert "invalid_fx_pair_rows=2" in errors
    assert _normalize_text(None) == ""
    assert _normalize_text(True) == "1"
    assert _normalize_text(False) == "0"


def test_validate_intraday_research_and_live_frames_report_problem_lists_without_raising():
    research = pl.DataFrame(
        {
            "timestamp": ["2026-03-27T13:35:00", "2026-03-27T13:30:00"],
            "open": [1.0, 1.0],
            "high": [0.5, 1.1],
            "low": [1.1, 0.9],
            "close": [1.0, 1.0],
            "volume": [100.0, 100.0],
            "currency": ["USD", ""],
            "symbol": ["SPY", ""],
            "interval": ["1m", ""],
            "provider": ["yahoo", ""],
            "session": ["regular", "pre"],
            "session_date": ["2026-03-27", None],
            "is_regular_session": [True, False],
            "ingested_at": ["2026-03-27T20:01:00", "2026-03-27T20:02:00"],
        }
    ).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False), pl.col("session_date").str.strptime(pl.Date, strict=False), pl.col("ingested_at").str.strptime(pl.Datetime, strict=False))
    research_errors = validate_intraday_research_frame(research, raise_on_error=False)
    assert "timestamps_not_sorted" in research_errors
    assert "metadata_inconsistent=symbol,interval,provider,session,currency,is_regular_session" in research_errors
    assert any(item.startswith("bad_ohlc_rows=") for item in research_errors)
    assert any(item.startswith("invalid_metadata_rows=") for item in research_errors)

    live = pl.DataFrame(
        {
            "timestamp": ["2026-03-27T13:35:00", "2026-03-27T13:30:00"],
            "open": [1.0, -1.0],
            "high": [1.2, 0.5],
            "low": [0.9, 1.0],
            "close": [1.1, 0.0],
            "volume": [100.0, 100.0],
            "currency": ["USD", "EUR"],
            "symbol": ["SPY", ""],
            "interval": ["5m", ""],
            "provider": ["yahoo", ""],
            "session": ["regular", "overnight"],
            "session_date": ["2026-03-27", None],
            "is_regular_session": [True, False],
            "is_closed_bar": [True, False],
            "ingested_at": ["2026-03-27T20:01:00", "2026-03-27T20:02:00"],
        }
    ).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False), pl.col("session_date").str.strptime(pl.Date, strict=False), pl.col("ingested_at").str.strptime(pl.Datetime, strict=False))
    live_errors = validate_intraday_live_frame(live, raise_on_error=False)
    assert "timestamps_not_sorted" in live_errors
    assert "metadata_inconsistent=symbol,interval,provider,currency" in live_errors
    assert any(item.startswith("invalid_rows=") for item in live_errors)
    assert validate_intraday_frame(pl.DataFrame({"date": [None], "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "adj_close": [1.0], "volume": [1.0], "currency": ["USD"]}, schema_overrides={"date": pl.Datetime}), raise_on_error=False) == []
    assert any(item.startswith("missing=") for item in validate_intraday_research_frame(pl.DataFrame({"timestamp": [None]}), raise_on_error=False))
    assert validate_intraday_research_frame(pl.DataFrame(schema={"timestamp": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "currency": pl.String, "symbol": pl.String, "interval": pl.String, "provider": pl.String, "session": pl.String, "session_date": pl.Date, "is_regular_session": pl.Boolean, "ingested_at": pl.Datetime}), raise_on_error=False) == []
    assert validate_intraday_live_frame(pl.DataFrame(schema={"timestamp": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "currency": pl.String, "symbol": pl.String, "interval": pl.String, "provider": pl.String, "session": pl.String, "session_date": pl.Date, "is_regular_session": pl.Boolean, "is_closed_bar": pl.Boolean, "ingested_at": pl.Datetime}), raise_on_error=False) == []
    dup_live = live.head(1).vstack(live.head(1))
    assert "duplicate_timestamps=1" in validate_intraday_live_frame(dup_live, raise_on_error=False)


def test_non_raising_validator_mode_returns_errors_list():
    market_cap_errors = validate_market_cap_frame(pl.DataFrame({"symbol": ["AAPL"]}), raise_on_error=False)
    live_errors = validate_intraday_live_frame(pl.DataFrame({"symbol": ["AAPL"]}), raise_on_error=False)

    assert market_cap_errors
    assert live_errors
