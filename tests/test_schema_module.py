from __future__ import annotations

import polars as pl
import pytest

from tradinglab_data.schema import (
    compatibility_manifest,
    render_schema_json,
    render_schema_markdown,
    schema_manifest,
    validate_alerts_frame,
    validate_crypto_frame,
    validate_daily_frame,
    validate_fx_daily_frame,
    validate_intraday_live_frame,
    validate_intraday_research_frame,
    validate_moves_frame,
    validate_symbol_master_frame,
)


def test_schema_manifest_has_daily_and_intraday():
    manifest = schema_manifest()
    assert "daily" in manifest
    assert "intraday" in manifest
    assert "intraday_research" in manifest
    assert "intraday_live" in manifest
    assert "crypto" in manifest
    assert "date" in manifest["daily"]
    assert "timestamp" in manifest["crypto"]
    assert manifest["artifact_schema_version"] == "v0.3.0"
    assert "package_version" in manifest
    assert "fx_daily" in manifest
    assert "symbol_master" in manifest


def test_render_schema_markdown_contains_header():
    text = render_schema_markdown()
    assert "# Data Parquet Schema" in text
    assert "## Daily" in text
    assert "non_authoritative_country" in text
    assert "non_authoritative_tax_country" in text


def test_render_schema_json_contains_adj_close():
    text = render_schema_json()
    assert '"adj_close"' in text
    assert '"crypto"' in text
    assert '"fx_daily"' in text
    assert '"package_version"' in text
    assert '"artifact_schema_version"' in text


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
    assert manifest["artifact_schema_version"] == "v0.3.0"


def test_compatibility_manifest_separates_package_and_artifact_versions():
    manifest = compatibility_manifest()
    assert manifest["package_name"] == "tradinglab-data"
    assert manifest["python_package_name"] == "tradinglab_data"
    assert manifest["artifact_schema_version"] == "v0.3.0"
    assert "daily_parquet" in manifest["artifact_families"]
    assert "crypto_parquet" in manifest["artifact_families"]
    assert "fx_daily_parquet" in manifest["artifact_families"]
    assert "symbol_master_csv" in manifest["artifact_families"]
    assert manifest["artifact_families"]["daily_parquet"]["category"] == "parquet"
    assert manifest["artifact_families"]["parquet_store_report_markdown"]["category"] == "markdown"
    assert set(manifest) == {
        "package_name",
        "python_package_name",
        "package_version",
        "artifact_schema_version",
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
