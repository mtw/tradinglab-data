from __future__ import annotations

import json
import math
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, TypeAlias

import polars as pl

from .contracts import (
    ARTIFACT_SCHEMA_VERSION,
    DATAFRAME_POLICY,
    EXCHANGE_DEFAULT_COLUMNS,
    FX_DAILY_COLUMNS,
    INDEX_RETURN_COLUMNS,
    MARKET_CAP_COLUMNS,
    OHLC_COLUMNS,
    PACKAGE_NAME,
    PYTHON_PACKAGE_NAME,
    SECTOR_ASSIGNMENT_COLUMNS,
    SYMBOL_MASTER_COLUMNS,
    SYMBOL_MASTER_OPTIONAL_COLUMNS,
    CompatibilityManifest,
)

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

INTRADAY_RESEARCH_SCHEMA: dict[str, SchemaDtype] = {
    "timestamp": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.String,
    "symbol": pl.String,
    "interval": pl.String,
    "provider": pl.String,
    "session": pl.String,
    "session_date": pl.Date,
    "is_regular_session": pl.Boolean,
    "ingested_at": pl.Datetime,
}

INTRADAY_LIVE_SCHEMA: dict[str, SchemaDtype] = {
    "timestamp": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.String,
    "symbol": pl.String,
    "interval": pl.String,
    "provider": pl.String,
    "session": pl.String,
    "session_date": pl.Date,
    "is_regular_session": pl.Boolean,
    "is_closed_bar": pl.Boolean,
    "ingested_at": pl.Datetime,
}

FX_DAILY_PARQUET_SCHEMA: dict[str, SchemaDtype] = {
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

MARKET_CAP_PARQUET_SCHEMA: dict[str, SchemaDtype] = {
    "date": pl.Datetime,
    "symbol": pl.String,
    "market_cap_usd_millions": pl.Float64,
    "provider": pl.String,
    "source_symbol": pl.String,
    "ingested_at": pl.Datetime,
}

SECTOR_ASSIGNMENT_SCHEMA: dict[str, SchemaDtype] = {
    "symbol": pl.String,
    "sector": pl.String,
    "effective_start": pl.Date,
    "effective_end": pl.Date,
    "source": pl.String,
    "ingested_at": pl.Datetime,
}

INDEX_RETURN_PARQUET_SCHEMA: dict[str, SchemaDtype] = {
    "date": pl.Datetime,
    "index_id": pl.String,
    "return": pl.Float64,
    "total_return_level": pl.Float64,
    "provider": pl.String,
    "source_symbol": pl.String,
    "ingested_at": pl.Datetime,
}

SYMBOL_MASTER_SCHEMA: dict[str, SchemaDtype] = {
    **{column: pl.String for column in SYMBOL_MASTER_COLUMNS[:8]},
    "lot_size": pl.Float64,
    "price_multiplier": pl.Float64,
    **{column: pl.String for column in SYMBOL_MASTER_OPTIONAL_COLUMNS},
}

EXCHANGE_DEFAULT_SCHEMA: dict[str, SchemaDtype] = {
    **{column: pl.String for column in EXCHANGE_DEFAULT_COLUMNS[:4]},
    "default_lot_size": pl.Float64,
    "default_price_multiplier": pl.Float64,
    "default_asset_class": pl.String,
}


OHLC_PARQUET_SCHEMA: dict[str, SchemaDtype] = OHLC_SCHEMA
DAILY_PARQUET_SCHEMA: dict[str, SchemaDtype] = OHLC_SCHEMA
INTRADAY_PARQUET_SCHEMA: dict[str, SchemaDtype] = OHLC_SCHEMA
INTRADAY_RESEARCH_PARQUET_SCHEMA: dict[str, SchemaDtype] = INTRADAY_RESEARCH_SCHEMA
INTRADAY_LIVE_PARQUET_SCHEMA: dict[str, SchemaDtype] = INTRADAY_LIVE_SCHEMA
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
    "dataframe_policy": "Polars-first: public tabular Python APIs return polars.DataFrame objects, schemas are expressed with Polars dtypes, and pandas-shaped provider outputs are normalized at ingestion boundaries.",
    "partitioning": "One parquet file per symbol. Daily store: <paths.parquet_root>/<SYMBOL>.parquet. Intraday store: <extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet.",
    "crypto_partitioning": "Crypto store: <paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet.",
    "intraday_research_partitioning": "Intraday research store: <intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet.",
    "intraday_live_partitioning": "Intraday live store: <intraday_live.live_root>/<INTERVAL>/<SYMBOL>.parquet.",
    "fx_daily_partitioning": "FX daily store: <paths.fx_daily_root>/<PAIR>.parquet.",
    "market_cap_partitioning": "Market-cap store: <paths.market_cap_root>/<SYMBOL>.parquet.",
    "sector_assignment_partitioning": "Sector assignments live in <paths.sector_assignments_csv>.",
    "index_return_partitioning": "Index total-return store: <paths.index_returns_root>/<INDEX_ID>.parquet.",
    "symbol_master_partitioning": "Authoritative symbol metadata lives under <paths.meta_root>/symbol_master.csv, with exchange defaults and symbol overrides as companion CSV artifacts.",
    "semantics": "OHLC columns are raw vendor OHLC. adj_close is adjusted close when supplied by the upstream provider. currency is the listing currency when known.",
    "symbol_master_semantics": "symbol_master.csv is the authoritative accounting metadata surface. Daily OHLC currency remains diagnostic provider data and is not authoritative accounting metadata. metadata_quality=non_authoritative_country and metadata_quality=non_authoritative_tax_country mark fallback fields derived from exchange_defaults.csv rather than provider-authoritative source data.",
    "intraday_research_semantics": "Intraday research parquet persists regular-session raw OHLCV bars with explicit UTC timestamp, session_date, provider, and symbol metadata.",
    "intraday_live_semantics": "Intraday live parquet persists session-aware raw OHLCV bars for pre, regular, and post sessions with explicit closed-bar and session metadata.",
    "crypto_semantics": "Crypto parquet persists closed exchange-native OHLCV bars with explicit exchange, market type, interval, and canonical symbol metadata.",
    "fx_daily_semantics": "FX daily parquet persists explicit source-to-target conversion pairs such as USDEUR, meaning EUR value of 1 USD. Consumers must not silently invert pair direction.",
    "market_cap_semantics": "Market-cap parquet persists point-in-time market capitalisation in USD millions for public consumer size splits.",
    "sector_assignment_semantics": "Sector assignment CSV persists GICS sector names using the fixed 11-sector vocabulary.",
    "index_return_semantics": "Index return parquet persists daily total returns for supported market indices such as SPX, RTY, and NDX.",
    "timestamps": "date is stored as Polars Datetime. Daily bars represent session dates. Intraday bars should be normalized to UTC internally and written without mixed timezone types.",
    "intraday_research_timestamps": "Intraday research timestamp and ingested_at are stored as UTC-normalized datetimes; session_date is the exchange-local trading date.",
    "intraday_live_timestamps": "Intraday live timestamp and ingested_at are stored as UTC-normalized datetimes; session_date is the exchange-local trading date.",
    "crypto_timestamps": "Crypto timestamp columns are UTC-normalized bar-open timestamps; ingested_at records the last local write time in UTC.",
    "fx_daily_timestamps": "FX daily date follows the same daily-bar normalization as the existing daily parquet contract; ingested_at is stored in UTC.",
    "market_cap_timestamps": "Market-cap date follows the effective trading date of the observation; ingested_at is stored in UTC.",
    "sector_assignment_timestamps": "Sector effective_start and effective_end are inclusive point-in-time dates when history is available.",
    "index_return_timestamps": "Index return date follows the trading date of the total-return observation; ingested_at is stored in UTC.",
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
    "intraday_research_constraints": [
        "Rows must be sorted by timestamp ascending.",
        "timestamp values must be unique within a file.",
        "session must be regular and is_regular_session must be true in the first implementation.",
        "interval, provider, and symbol metadata must be populated on every row and remain file-consistent.",
    ],
    "intraday_live_constraints": [
        "Rows must be sorted by timestamp ascending.",
        "timestamp values must be unique within a file.",
        "session must be one of pre, regular, post, or unknown.",
        "interval, provider, symbol, and is_closed_bar metadata must be populated on every row and remain file-consistent.",
    ],
    "fx_daily_constraints": [
        "Rows must be sorted by date ascending.",
        "date values must be unique within a file.",
        "base_currency + quote_currency must equal pair on every row.",
        "open, high, low, and close must be positive finite conversion values.",
    ],
    "market_cap_constraints": [
        "Rows must be sorted by date ascending within each symbol file.",
        "date values must be unique within each symbol file.",
        "market_cap_usd_millions must be strictly positive for valid rows.",
        "symbol, provider, and source_symbol must be populated on every row.",
    ],
    "sector_assignment_constraints": [
        "symbol and sector must be populated on every row.",
        "sector must use the fixed 11-sector GICS vocabulary.",
        "effective_start and effective_end are inclusive when populated.",
    ],
    "index_return_constraints": [
        "Rows must be sorted by date ascending within each index file.",
        "date values must be unique within each index file.",
        "return must be a simple daily total return.",
        "index_id, provider, and source_symbol must be populated on every row.",
    ],
    "symbol_master_constraints": [
        "All required symbol master columns must be present.",
        "Active rows must have non-empty symbol, exchange, country, asset_currency, base_listing_currency, tax_country, asset_class, and fx_pair_to_base values.",
        "lot_size and price_multiplier must be strictly positive.",
        "fx_pair_to_base must be a six-letter uppercase pair, including explicit identity pairs such as EUREUR.",
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
        "dataframe_policy": DATAFRAME_POLICY,
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
            "intraday_research_parquet": {
                "category": "parquet",
                "path_pattern": "<intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet",
                "schema_name": "intraday_research_ohlcv",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "intraday_live_parquet": {
                "category": "parquet",
                "path_pattern": "<intraday_live.live_root>/<INTERVAL>/<SYMBOL>.parquet",
                "schema_name": "intraday_live_ohlcv",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "crypto_parquet": {
                "category": "parquet",
                "path_pattern": "<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet",
                "schema_name": "crypto_ohlcv",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "fx_daily_parquet": {
                "category": "parquet",
                "path_pattern": "<paths.fx_daily_root>/<PAIR>.parquet",
                "schema_name": "fx_daily_parquet",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "market_cap_parquet": {
                "category": "parquet",
                "path_pattern": "<paths.market_cap_root>/<SYMBOL>.parquet",
                "schema_name": "market_cap_parquet",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "sector_assignments_csv": {
                "category": "csv",
                "path_pattern": "<paths.sector_assignments_csv>",
                "schema_name": "sector_assignments_csv",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "index_return_parquet": {
                "category": "parquet",
                "path_pattern": "<paths.index_returns_root>/<INDEX_ID>.parquet",
                "schema_name": "index_return_parquet",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "symbol_master_csv": {
                "category": "csv",
                "path_pattern": "<paths.meta_root>/symbol_master.csv",
                "schema_name": "symbol_master_csv",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "exchange_defaults_csv": {
                "category": "csv",
                "path_pattern": "<paths.meta_root>/exchange_defaults.csv",
                "schema_name": "exchange_defaults_csv",
                "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
            },
            "symbol_overrides_csv": {
                "category": "csv",
                "path_pattern": "<paths.meta_root>/symbol_overrides.csv",
                "schema_name": "symbol_overrides_csv",
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
        "intraday_research": {k: str(v) for k, v in INTRADAY_RESEARCH_PARQUET_SCHEMA.items()},
        "intraday_live": {k: str(v) for k, v in INTRADAY_LIVE_PARQUET_SCHEMA.items()},
        "crypto": {k: str(v) for k, v in CRYPTO_PARQUET_SCHEMA.items()},
        "fx_daily": {k: str(v) for k, v in FX_DAILY_PARQUET_SCHEMA.items()},
        "market_cap": {k: str(v) for k, v in MARKET_CAP_PARQUET_SCHEMA.items()},
        "sector_assignments": {k: str(v) for k, v in SECTOR_ASSIGNMENT_SCHEMA.items()},
        "index_returns": {k: str(v) for k, v in INDEX_RETURN_PARQUET_SCHEMA.items()},
        "symbol_master": {k: str(v) for k, v in SYMBOL_MASTER_SCHEMA.items()},
        "notes": SCHEMA_NOTES,
    }


def render_schema_json() -> str:
    return json.dumps(schema_manifest(), indent=2)


def render_schema_markdown() -> str:
    def _table(title: str, schema: dict[str, SchemaDtype]) -> str:
        rows = "\n".join(f"| `{col}` | `{dtype}` |" for col, dtype in schema.items())
        return f"## {title}\n\n| Column | Type |\n|---|---|\n{rows}\n"

    return (
        "# Data Parquet Schema\n\n"
        + f"Artifact schema version: `{ARTIFACT_SCHEMA_VERSION}`\n\n"
        + f"Dataframe policy: `{DATAFRAME_POLICY}`\n\n"
        + "Schema dtypes are rendered from Polars definitions. Public tabular Python APIs return `polars.DataFrame`; pandas is not part of the public dataframe contract.\n\n"
        + "Machine-readable sources:\n\n"
        + '- `tradinglab_data.compatibility_manifest()["artifact_schema_version"]`\n'
        + '- `tradinglab_data.compatibility_manifest()["dataframe_policy"]`\n'
        + '- `tradinglab_data.schema_manifest()["artifact_schema_version"]`\n'
        + '- `tradinglab_data.schema_manifest()["dataframe_policy"]`\n\n'
        + _table("Daily", DAILY_PARQUET_SCHEMA)
        + "\n"
        + _table("Intraday", INTRADAY_PARQUET_SCHEMA)
        + "\n"
        + _table("Intraday Research", INTRADAY_RESEARCH_PARQUET_SCHEMA)
        + "\n"
        + _table("Intraday Live", INTRADAY_LIVE_PARQUET_SCHEMA)
        + "\n"
        + _table("Crypto", CRYPTO_PARQUET_SCHEMA)
        + "\n"
        + _table("FX Daily", FX_DAILY_PARQUET_SCHEMA)
        + "\n"
        + _table("Market Cap", MARKET_CAP_PARQUET_SCHEMA)
        + "\n"
        + _table("Sector Assignments CSV", SECTOR_ASSIGNMENT_SCHEMA)
        + "\n"
        + _table("Index Returns", INDEX_RETURN_PARQUET_SCHEMA)
        + "\n"
        + _table("Symbol Master CSV", SYMBOL_MASTER_SCHEMA)
        + "\n## Notes\n\n"
        + f"- {SCHEMA_NOTES['dataframe_policy']}\n"
        + f"- {SCHEMA_NOTES['partitioning']}\n"
        + f"- {SCHEMA_NOTES['intraday_research_partitioning']}\n"
        + f"- {SCHEMA_NOTES['intraday_live_partitioning']}\n"
        + f"- {SCHEMA_NOTES['crypto_partitioning']}\n"
        + f"- {SCHEMA_NOTES['fx_daily_partitioning']}\n"
        + f"- {SCHEMA_NOTES['market_cap_partitioning']}\n"
        + f"- {SCHEMA_NOTES['sector_assignment_partitioning']}\n"
        + f"- {SCHEMA_NOTES['index_return_partitioning']}\n"
        + f"- {SCHEMA_NOTES['symbol_master_partitioning']}\n"
        + f"- {SCHEMA_NOTES['semantics']}\n"
        + f"- {SCHEMA_NOTES['symbol_master_semantics']}\n"
        + f"- {SCHEMA_NOTES['intraday_research_semantics']}\n"
        + f"- {SCHEMA_NOTES['intraday_live_semantics']}\n"
        + f"- {SCHEMA_NOTES['crypto_semantics']}\n"
        + f"- {SCHEMA_NOTES['fx_daily_semantics']}\n"
        + f"- {SCHEMA_NOTES['market_cap_semantics']}\n"
        + f"- {SCHEMA_NOTES['sector_assignment_semantics']}\n"
        + f"- {SCHEMA_NOTES['index_return_semantics']}\n"
        + f"- {SCHEMA_NOTES['timestamps']}\n"
        + f"- {SCHEMA_NOTES['intraday_research_timestamps']}\n"
        + f"- {SCHEMA_NOTES['intraday_live_timestamps']}\n"
        + f"- {SCHEMA_NOTES['crypto_timestamps']}\n"
        + f"- {SCHEMA_NOTES['fx_daily_timestamps']}\n"
        + f"- {SCHEMA_NOTES['market_cap_timestamps']}\n"
        + f"- {SCHEMA_NOTES['sector_assignment_timestamps']}\n"
        + f"- {SCHEMA_NOTES['index_return_timestamps']}\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["intraday_research_constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["intraday_live_constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["crypto_constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["fx_daily_constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["market_cap_constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["sector_assignment_constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["index_return_constraints"])
        + "\n"
        + "\n".join(f"- {item}" for item in SCHEMA_NOTES["symbol_master_constraints"])
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


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    text = str(value).strip()
    return "" if text.lower() == "null" else text


def _normalize_upper(value: object) -> str:
    return _normalize_text(value).upper()


def _pair_is_valid(pair: str) -> bool:
    return len(pair) == 6 and pair.isalpha() and pair == pair.upper()


def validate_symbol_master_frame(df: pl.DataFrame, *, strict: bool = True) -> list[str]:
    errors: list[str] = []
    missing = [column for column in SYMBOL_MASTER_COLUMNS if column not in df.columns]
    if missing:
        errors.append("missing_required_columns=" + ",".join(missing))
        if strict:
            return errors
        return []
    active_mask = pl.lit(True)
    if "active" in df.columns:
        active_mask = (
            pl.col("active")
            .cast(pl.String, strict=False)
            .fill_null("1")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["1", "true", "yes", "y"])
        )
    required_active_columns = [
        "symbol",
        "exchange",
        "country",
        "asset_currency",
        "base_listing_currency",
        "tax_country",
        "asset_class",
        "fx_pair_to_base",
    ]
    for column in required_active_columns:
        missing_rows = int(
            df.select((active_mask & pl.col(column).cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")).sum()).item()
        )
        if missing_rows > 0:
            errors.append(f"empty_{column}_rows={missing_rows}")
    normalized_symbols = [_normalize_upper(value) for value in df.get_column("symbol").to_list()]
    if len(normalized_symbols) != len(set(sym for sym in normalized_symbols if sym)):
        errors.append("duplicate_symbols")
    for column in ("asset_currency", "base_listing_currency", "tax_country"):
        bad = sum(1 for value in df.get_column(column).to_list() if _normalize_upper(value) != _normalize_text(value))
        if bad > 0:
            errors.append(f"{column}_not_uppercase={bad}")
    for column in ("lot_size", "price_multiplier"):
        try:
            casted = df.get_column(column).cast(pl.Float64, strict=True)
        except Exception:
            errors.append(f"{column}_not_float")
            continue
        bad = int(casted.is_null().sum()) + int((casted <= 0).sum())
        if bad > 0:
            errors.append(f"{column}_nonpositive_rows={bad}")
    bad_pairs = 0
    for pair_value, asset_value in zip(df.get_column("fx_pair_to_base").to_list(), df.get_column("asset_currency").to_list(), strict=False):
        pair = _normalize_upper(pair_value)
        asset = _normalize_upper(asset_value)
        if not _pair_is_valid(pair):
            bad_pairs += 1
            continue
        if asset and pair[:3] != asset:
            bad_pairs += 1
    if bad_pairs > 0:
        errors.append(f"invalid_fx_pair_rows={bad_pairs}")
    if strict:
        return errors
    return []


def validate_fx_daily_frame(df: pl.DataFrame, *, pair: str | None = None) -> list[str]:
    errors: list[str] = []
    missing = [column for column in FX_DAILY_COLUMNS if column not in df.columns]
    if missing:
        errors.append("missing_required_columns=" + ",".join(missing))
        return errors
    try:
        validate_frame_schema(df, FX_DAILY_PARQUET_SCHEMA)
    except ValueError as exc:
        errors.append(str(exc))
        return errors
    expected_pair = _normalize_upper(pair) if pair else ""
    if df.is_empty():
        return errors
    if not bool(df.get_column("date").is_sorted()):
        errors.append("dates_not_sorted")
    duplicate_dates = int(df.height - df.select(pl.col("date").n_unique()).item())
    if duplicate_dates > 0:
        errors.append(f"duplicate_dates={duplicate_dates}")
    bad_rows = 0
    for row in df.select(["open", "high", "low", "close", "pair", "base_currency", "quote_currency"]).iter_rows(named=True):
        row_invalid = False
        if expected_pair and _normalize_upper(row["pair"]) != expected_pair:
            row_invalid = True
        if _normalize_upper(row["base_currency"]) + _normalize_upper(row["quote_currency"]) != _normalize_upper(row["pair"]):
            row_invalid = True
        for column in ("open", "high", "low", "close"):
            value = row[column]
            if value is None or not math.isfinite(float(value)) or float(value) <= 0:
                row_invalid = True
                break
        if row_invalid:
            bad_rows += 1
    if bad_rows > 0:
        errors.append(f"invalid_rows={bad_rows}")
    return errors


def validate_market_cap_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, MARKET_CAP_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)
    if df.is_empty():
        return
    problems: list[str] = []
    if not bool(df.get_column("date").is_sorted()):
        problems.append("dates_not_sorted")
    duplicate_dates = int(df.height - df.select(pl.col("date").n_unique()).item())
    if duplicate_dates > 0:
        problems.append(f"duplicate_dates={duplicate_dates}")
    symbol_count = int(df.select(pl.col("symbol").n_unique()).item())
    if symbol_count != 1:
        problems.append(f"mixed_symbols={symbol_count}")
    bad_rows = int(
        df.select(
            (
                pl.col("date").is_null()
                | pl.col("symbol").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")
                | pl.col("provider").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")
                | pl.col("source_symbol").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")
                | pl.col("market_cap_usd_millions").is_null()
                | (pl.col("market_cap_usd_millions") <= 0)
            ).sum()
        ).item()
    )
    if bad_rows > 0:
        problems.append(f"invalid_rows={bad_rows}")
    if problems:
        raise ValueError("Market-cap frame does not match contract: " + "; ".join(problems))


def validate_sector_assignment_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, SECTOR_ASSIGNMENT_SCHEMA, allow_extra_columns=allow_extra_columns)
    if df.is_empty():
        return
    valid_sectors = [
        "Information Technology",
        "Financials",
        "Energy",
        "Health Care",
        "Industrials",
        "Consumer Staples",
        "Consumer Discretionary",
        "Utilities",
        "Real Estate",
        "Materials",
        "Communication Services",
    ]
    bad_rows = int(
        df.select(
            (
                pl.col("symbol").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")
                | (~pl.col("sector").is_in(valid_sectors))
            ).sum()
        ).item()
    )
    if bad_rows > 0:
        raise ValueError(f"Sector assignment frame does not match contract: invalid_rows={bad_rows}")


def validate_index_return_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, INDEX_RETURN_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)
    if df.is_empty():
        return
    problems: list[str] = []
    if not bool(df.get_column("date").is_sorted()):
        problems.append("dates_not_sorted")
    duplicate_dates = int(df.height - df.select(pl.col("date").n_unique()).item())
    if duplicate_dates > 0:
        problems.append(f"duplicate_dates={duplicate_dates}")
    index_id_count = int(df.select(pl.col("index_id").n_unique()).item())
    if index_id_count != 1:
        problems.append(f"mixed_index_ids={index_id_count}")
    bad_rows = int(
        df.select(
            (
                pl.col("date").is_null()
                | pl.col("index_id").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")
                | pl.col("provider").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")
                | pl.col("source_symbol").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq("")
                | (pl.col("return") <= -1)
            ).sum()
        ).item()
    )
    if bad_rows > 0:
        problems.append(f"invalid_rows={bad_rows}")
    null_returns_after_first = int(df.slice(1).select(pl.col("return").is_null().sum()).item())
    if null_returns_after_first > 0:
        problems.append(f"null_returns_after_first={null_returns_after_first}")
    if problems:
        raise ValueError("Index-return frame does not match contract: " + "; ".join(problems))


def validate_daily_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, DAILY_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_intraday_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, INTRADAY_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_intraday_research_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, INTRADAY_RESEARCH_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)
    problems: list[str] = []
    if df.is_empty():
        return
    if not bool(df.get_column("timestamp").is_sorted()):
        problems.append("timestamps_not_sorted")
    duplicate_timestamps = int(df.height - df.select(pl.col("timestamp").n_unique()).item())
    if duplicate_timestamps > 0:
        problems.append(f"duplicate_timestamps={duplicate_timestamps}")
    bad_ohlc = int(
        df.select(
            (
                pl.col("timestamp").is_null()
                | pl.col("session_date").is_null()
                | pl.col("open").is_null()
                | pl.col("high").is_null()
                | pl.col("low").is_null()
                | pl.col("close").is_null()
                | (pl.col("open") <= 0)
                | (pl.col("high") <= 0)
                | (pl.col("low") <= 0)
                | (pl.col("close") <= 0)
                | (pl.col("high") < pl.col("low"))
                | (pl.col("high") < pl.col("open"))
                | (pl.col("high") < pl.col("close"))
                | (pl.col("low") > pl.col("open"))
                | (pl.col("low") > pl.col("close"))
            ).sum()
        ).item()
    )
    if bad_ohlc > 0:
        problems.append(f"bad_ohlc_rows={bad_ohlc}")
    metadata_columns = ["symbol", "interval", "provider", "session", "currency", "is_regular_session"]
    inconsistent = []
    for column in metadata_columns:
        unique_count = int(df.select(pl.col(column).drop_nulls().n_unique()).item())
        if unique_count > 1:
            inconsistent.append(column)
    if inconsistent:
        problems.append("metadata_inconsistent=" + ",".join(inconsistent))
    off_session = int(
        df.select(
            (
                (pl.col("session") != "regular")
                | (~pl.col("is_regular_session"))
                | (pl.col("interval") == "")
                | (pl.col("provider") == "")
                | (pl.col("symbol") == "")
                | (pl.col("currency") == "")
            ).sum()
        ).item()
    )
    if off_session > 0:
        problems.append(f"invalid_metadata_rows={off_session}")
    if problems:
        raise ValueError("Intraday research frame does not match contract: " + "; ".join(problems))


def validate_intraday_live_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, INTRADAY_LIVE_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)
    if df.is_empty():
        return
    problems: list[str] = []
    if not bool(df.get_column("timestamp").is_sorted()):
        problems.append("timestamps_not_sorted")
    duplicate_timestamps = int(df.height - df.select(pl.col("timestamp").n_unique()).item())
    if duplicate_timestamps > 0:
        problems.append(f"duplicate_timestamps={duplicate_timestamps}")
    invalid_rows = int(
        df.select(
            (
                pl.col("timestamp").is_null()
                | pl.col("session_date").is_null()
                | pl.col("open").is_null()
                | pl.col("high").is_null()
                | pl.col("low").is_null()
                | pl.col("close").is_null()
                | (pl.col("open") <= 0)
                | (pl.col("high") <= 0)
                | (pl.col("low") <= 0)
                | (pl.col("close") <= 0)
                | (pl.col("high") < pl.col("low"))
                | (pl.col("high") < pl.col("open"))
                | (pl.col("high") < pl.col("close"))
                | (pl.col("low") > pl.col("open"))
                | (pl.col("low") > pl.col("close"))
                | (~pl.col("session").is_in(["pre", "regular", "post", "unknown"]))
                | (pl.col("provider") == "")
                | (pl.col("symbol") == "")
                | (pl.col("interval") == "")
            ).sum()
        ).item()
    )
    if invalid_rows > 0:
        problems.append(f"invalid_rows={invalid_rows}")
    metadata_columns = ["symbol", "interval", "provider", "currency"]
    inconsistent = []
    for column in metadata_columns:
        unique_count = int(df.select(pl.col(column).drop_nulls().n_unique()).item())
        if unique_count > 1:
            inconsistent.append(column)
    if inconsistent:
        problems.append("metadata_inconsistent=" + ",".join(inconsistent))
    if problems:
        raise ValueError("Intraday live frame does not match contract: " + "; ".join(problems))


def validate_crypto_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, CRYPTO_PARQUET_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_moves_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, MOVE_ALERT_FRAME_SCHEMA, allow_extra_columns=allow_extra_columns)


def validate_alerts_frame(df: pl.DataFrame, *, allow_extra_columns: bool = True) -> None:
    validate_frame_schema(df, MOVE_ALERT_FRAME_SCHEMA, allow_extra_columns=allow_extra_columns)


assert tuple(DAILY_PARQUET_SCHEMA) == OHLC_COLUMNS
assert tuple(MARKET_CAP_PARQUET_SCHEMA) == MARKET_CAP_COLUMNS
assert tuple(SECTOR_ASSIGNMENT_SCHEMA) == SECTOR_ASSIGNMENT_COLUMNS
assert tuple(INDEX_RETURN_PARQUET_SCHEMA) == INDEX_RETURN_COLUMNS
