from __future__ import annotations

import polars as pl
import pytest

from tradinglab_data.schema import compatibility_manifest, render_schema_json, render_schema_markdown, schema_manifest, validate_alerts_frame, validate_daily_frame, validate_moves_frame


def test_schema_manifest_has_daily_and_intraday():
    manifest = schema_manifest()
    assert "daily" in manifest
    assert "intraday" in manifest
    assert "date" in manifest["daily"]
    assert manifest["artifact_schema_version"] == "v0.1.0"
    assert "package_version" in manifest


def test_render_schema_markdown_contains_header():
    text = render_schema_markdown()
    assert "# TradingLab Data Parquet Schema" in text
    assert "## Daily" in text


def test_render_schema_json_contains_adj_close():
    text = render_schema_json()
    assert '"adj_close"' in text
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
    assert manifest["artifact_schema_version"] == "v0.1.0"


def test_compatibility_manifest_separates_package_and_artifact_versions():
    manifest = compatibility_manifest()
    assert manifest["package_name"] == "tradinglab-data"
    assert manifest["python_package_name"] == "tradinglab_data"
    assert manifest["artifact_schema_version"] == "v0.1.0"
    assert "daily_parquet" in manifest["artifact_families"]
    assert manifest["artifact_families"]["daily_parquet"]["category"] == "parquet"
    assert manifest["artifact_families"]["parquet_store_report_markdown"]["category"] == "markdown"
