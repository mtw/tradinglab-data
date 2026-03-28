from __future__ import annotations

import polars as pl
import pytest

from tradinglab_data.schema import render_schema_json, render_schema_markdown, schema_manifest, validate_daily_frame, validate_moves_frame


def test_schema_manifest_has_daily_and_intraday():
    manifest = schema_manifest()
    assert "daily" in manifest
    assert "intraday" in manifest
    assert "date" in manifest["daily"]


def test_render_schema_markdown_contains_header():
    text = render_schema_markdown()
    assert "# TradingLab Data Parquet Schema" in text
    assert "## Daily" in text


def test_render_schema_json_contains_adj_close():
    text = render_schema_json()
    assert '"adj_close"' in text
    assert '"api_contract_version"' in text


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


def test_schema_manifest_contains_api_contract_version():
    manifest = schema_manifest()
    assert manifest["api_contract_version"] == "v0.2.0"
