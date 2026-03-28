from __future__ import annotations

import json

import polars as pl

from .contracts import API_CONTRACT_VERSION


OHLC_PARQUET_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.Utf8,
}


DAILY_PARQUET_SCHEMA: dict[str, pl.DataType] = dict(OHLC_PARQUET_SCHEMA)
INTRADAY_PARQUET_SCHEMA: dict[str, pl.DataType] = dict(OHLC_PARQUET_SCHEMA)


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
        "api_contract_version": API_CONTRACT_VERSION,
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


def _dtype_matches(actual: pl.DataType, expected: pl.DataType) -> bool:
    if actual == expected:
        return True
    try:
        return actual.base_type() == expected
    except Exception:
        return False


def validate_frame_schema(
    df: pl.DataFrame,
    expected_schema: dict[str, pl.DataType],
    *,
    allow_extra_columns: bool = True,
) -> None:
    missing = [column for column in expected_schema if column not in df.columns]
    extras = [column for column in df.columns if column not in expected_schema]
    mismatched = []
    for column, dtype in expected_schema.items():
        actual = df.schema.get(column)
        if actual is None:
            continue
        if not _dtype_matches(actual, dtype):
            mismatched.append(f"{column}:{actual!s}!={dtype!s}")
    if missing or mismatched or (extras and not allow_extra_columns):
        problems = []
        if missing:
            problems.append(f"missing={missing}")
        if mismatched:
            problems.append(f"dtype={mismatched}")
        if extras and not allow_extra_columns:
            problems.append(f"extra={extras}")
        raise ValueError("Frame does not match contract: " + "; ".join(problems))


def validate_daily_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, DAILY_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_intraday_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, INTRADAY_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_moves_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    expected = {
        "symbol": pl.Utf8,
        "ref_close": pl.Float64,
        "last_price": pl.Float64,
        "pct_move": pl.Float64,
        "last_volume": pl.Float64,
        "currency": pl.Utf8,
        "last_ts": pl.Datetime,
        "session": pl.Utf8,
    }
    validate_frame_schema(df, expected, allow_extra_columns=allow_extra_columns)


def validate_alerts_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    expected = {
        "symbol": pl.Utf8,
        "ref_close": pl.Float64,
        "last_price": pl.Float64,
        "pct_move": pl.Float64,
        "last_volume": pl.Float64,
        "currency": pl.Utf8,
        "last_ts": pl.Datetime,
        "session": pl.Utf8,
    }
    validate_frame_schema(df, expected, allow_extra_columns=allow_extra_columns)
