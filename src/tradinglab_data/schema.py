from __future__ import annotations

import json
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, TypeAlias

import polars as pl

from .contracts import ARTIFACT_SCHEMA_VERSION, OHLC_COLUMNS, PACKAGE_NAME, PYTHON_PACKAGE_NAME, CompatibilityManifest

# Use the public PolarsDataType alias for typing while keeping runtime on the stable pl.DataType export.
if TYPE_CHECKING:
    from polars import PolarsDataType as _PolarsDataType  # type: ignore[attr-defined]
else:
    _PolarsDataType = pl.DataType

SchemaDtype: TypeAlias = _PolarsDataType

OHLC_SCHEMA: dict[str, SchemaDtype] = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.String,
}

CRYPTO_OHLC_SCHEMA: dict[str, SchemaDtype] = {
    "timestamp": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "provider": pl.String,
    "exchange": pl.String,
    "market_type": pl.String,
    "symbol": pl.String,
    "base_asset": pl.String,
    "quote_asset": pl.String,
    "interval": pl.String,
    "is_closed": pl.Boolean,
    "ingested_at": pl.Datetime,
    "source_symbol": pl.String,
}


OHLC_PARQUET_SCHEMA: dict[str, SchemaDtype] = OHLC_SCHEMA
DAILY_PARQUET_SCHEMA: dict[str, SchemaDtype] = OHLC_SCHEMA
INTRADAY_PARQUET_SCHEMA: dict[str, SchemaDtype] = OHLC_SCHEMA
CRYPTO_PARQUET_SCHEMA: dict[str, SchemaDtype] = CRYPTO_OHLC_SCHEMA
MOVE_ALERT_FRAME_SCHEMA: dict[str, SchemaDtype] = {
    "symbol": pl.String,
    "ref_close": pl.Float64,
    "last_price": pl.Float64,
    "pct_move": pl.Float64,
    "last_volume": pl.Float64,
    "currency": pl.String,
    "last_ts": pl.Datetime,
    "session": pl.String,
}


SCHEMA_NOTES = {
    "partitioning": "One parquet file per symbol. Daily store: <paths.parquet_root>/<SYMBOL>.parquet. Intraday store: <extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet.",
    "crypto_partitioning": "Crypto store: <paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet.",
    "semantics": "OHLC columns are raw vendor OHLC. adj_close is adjusted close when supplied by the upstream provider. currency is the listing currency when known.",
    "crypto_semantics": "Crypto parquet persists closed exchange-native OHLCV bars with explicit exchange, market type, interval, and canonical symbol metadata.",
    "timestamps": "date is stored as Polars Datetime. Daily bars represent session dates. Intraday bars should be normalized to UTC internally and written without mixed timezone types.",
    "crypto_timestamps": "Crypto timestamp columns are UTC-normalized bar-open timestamps; ingested_at records the last local write time in UTC.",
    "constraints": [
        "Rows must be sorted by date ascending.",
        "date values must be unique within a file.",
        "open/high/low/close must be non-null and positive for valid rows.",
        "high must be >= open, close, low. low must be <= open, close.",
    ],
    "crypto_constraints": [
        "Rows must be sorted by timestamp ascending.",
        "timestamp values must be unique within a file.",
        "Only closed bars belong in the canonical crypto parquet history.",
        "exchange, market_type, symbol, interval, and source_symbol must be populated on every row.",
    ],
}


def _package_version() -> str | None:
    try:
        return version(PACKAGE_NAME)
    except PackageNotFoundError:
        return None


def compatibility_manifest() -> CompatibilityManifest:
    return {
        "package_name": PACKAGE_NAME,
        "python_package_name": PYTHON_PACKAGE_NAME,
        "package_version": _package_version(),
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "artifact_families": {
            "daily_parquet": {
                "category": "parquet",
                "path_pattern": "<paths.parquet_root>/<SYMBOL>.parquet",
                "schema_name": "daily_ohlc",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "intraday_parquet": {
                "category": "parquet",
                "path_pattern": "<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet",
                "schema_name": "intraday_ohlc",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "crypto_parquet": {
                "category": "parquet",
                "path_pattern": "<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet",
                "schema_name": "crypto_ohlcv",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "extended_hours_alerts_csv": {
                "category": "csv",
                "path_pattern": "<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_alerts.csv",
                "schema_name": "extended_hours_alerts",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "extended_hours_report_html": {
                "category": "html",
                "path_pattern": "<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_report.html",
                "schema_name": None,
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "parquet_store_report_json": {
                "category": "json",
                "path_pattern": "<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.json",
                "schema_name": "store_integrity_report",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "parquet_store_report_markdown": {
                "category": "markdown",
                "path_pattern": "<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.md",
                "schema_name": None,
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
        },
    }


def schema_manifest() -> dict[str, object]:
    return {
        **compatibility_manifest(),
        "daily": {k: str(v) for k, v in DAILY_PARQUET_SCHEMA.items()},
        "intraday": {k: str(v) for k, v in INTRADAY_PARQUET_SCHEMA.items()},
        "crypto": {k: str(v) for k, v in CRYPTO_PARQUET_SCHEMA.items()},
        "notes": SCHEMA_NOTES,
    }


def render_schema_json() -> str:
    return json.dumps(schema_manifest(), indent=2)


def render_schema_markdown() -> str:
    def _table(title: str, schema: dict[str, SchemaDtype]) -> str:
        rows = "\n".join(f"| `{col}` | `{dtype}` |" for col, dtype in schema.items())
        return f"## {title}\n\n| Column | Type |\n|---|---|\n{rows}\n"

    notes = "\n".join(f"- {item}" for item in SCHEMA_NOTES["constraints"])
    return (
        "# Data Parquet Schema\n\n"
        + _table("Daily", DAILY_PARQUET_SCHEMA)
        + "\n"
        + _table("Intraday", INTRADAY_PARQUET_SCHEMA)
        + "\n"
        + _table("Crypto", CRYPTO_PARQUET_SCHEMA)
        + "\n## Notes\n\n"
        + f"- {SCHEMA_NOTES['partitioning']}\n"
        + f"- {SCHEMA_NOTES['crypto_partitioning']}\n"
        + f"- {SCHEMA_NOTES['semantics']}\n"
        + f"- {SCHEMA_NOTES['crypto_semantics']}\n"
        + f"- {SCHEMA_NOTES['timestamps']}\n"
        + f"- {SCHEMA_NOTES['crypto_timestamps']}\n"
        + notes
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["crypto_constraints"])
        + "\n"
    )


def _dtype_matches(actual: pl.DataType, expected: SchemaDtype) -> bool:
    if actual == expected:
        return True
    try:
        return actual.base_type() == expected
    except Exception:
        return False


def validate_frame_schema(
    df: pl.DataFrame,
    expected_schema: dict[str, SchemaDtype],
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


def validate_crypto_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, CRYPTO_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_moves_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, MOVE_ALERT_FRAME_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_alerts_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, MOVE_ALERT_FRAME_SCHEMA, allow_extra_columns=allow_extra_columns)


assert tuple(DAILY_PARQUET_SCHEMA) == OHLC_COLUMNS
