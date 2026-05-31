from __future__ import annotations

import polars as pl
import pytest

from tradinglab_data.contracts import ARTIFACT_SCHEMA_VERSION, DATAFRAME_POLICY
from tradinglab_data.schema import (
    compatibility_manifest,
    render_schema_json,
    render_schema_markdown,
    schema_manifest,
    validate_alerts_frame,
    validate_crypto_frame,
    validate_daily_frame,
    validate_fx_daily_frame,
    validate_index_return_frame,
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
    validate_daily_frame(df)


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
    validate_crypto_frame(df)


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
    validate_intraday_research_frame(df)


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
    validate_intraday_research_frame(df)


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
    validate_intraday_live_frame(df)


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
    validate_market_cap_frame(df)


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
    validate_sector_assignment_frame(df)


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
    validate_index_return_frame(df)


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
