from __future__ import annotations

import json

import polars as pl


DAILY_PARQUET_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.Utf8,
}


INTRADAY_PARQUET_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.Utf8,
}


SCHEMA_NOTES = {
    "partitioning": "One parquet file per symbol. Daily store: <paths.parquet_root>/<SYMBOL>.parquet. Intraday store: <extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet.",
    "semantics": "OHLC columns are raw vendor OHLC. adj_close is adjusted close when supplied by the upstream provider. currency is the listing currency when known.",
    "timestamps": "date is stored as Polars Datetime. Daily bars represent session dates. Intraday bars should be normalized to UTC internally and written without mixed timezone types.",
    "constraints": [
        "Rows must be sorted by date ascending.",
        "date values must be unique within a file.",
        "open/high/low/close must be non-null and positive for valid rows.",
        "high must be >= open, close, low. low must be <= open, close.",
    ],
}


def schema_manifest() -> dict[str, object]:
    return {
        "daily": {k: str(v) for k, v in DAILY_PARQUET_SCHEMA.items()},
        "intraday": {k: str(v) for k, v in INTRADAY_PARQUET_SCHEMA.items()},
        "notes": SCHEMA_NOTES,
    }


def render_schema_json() -> str:
    return json.dumps(schema_manifest(), indent=2)


def render_schema_markdown() -> str:
    def _table(title: str, schema: dict[str, pl.DataType]) -> str:
        rows = "\n".join(f"| `{col}` | `{dtype}` |" for col, dtype in schema.items())
        return f"## {title}\n\n| Column | Type |\n|---|---|\n{rows}\n"

    notes = "\n".join(f"- {item}" for item in SCHEMA_NOTES["constraints"])
    return (
        "# TradingLab Data Parquet Schema\n\n"
        + _table("Daily", DAILY_PARQUET_SCHEMA)
        + "\n"
        + _table("Intraday", INTRADAY_PARQUET_SCHEMA)
        + "\n## Notes\n\n"
        + f"- {SCHEMA_NOTES['partitioning']}\n"
        + f"- {SCHEMA_NOTES['semantics']}\n"
        + f"- {SCHEMA_NOTES['timestamps']}\n"
        + notes
        + "\n"
    )
