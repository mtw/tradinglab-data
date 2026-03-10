from __future__ import annotations

from tradinglab_data.schema import render_schema_json, render_schema_markdown, schema_manifest


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
