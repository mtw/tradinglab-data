from __future__ import annotations

import logging
import math
import warnings
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta
from pathlib import Path

import polars as pl

from .config import (
    Config,
    ConfigLike,
    default_config_path,
    index_returns_root_path,
    market_cap_root_path,
    parquet_root_path,
    sector_assignments_path,
    ticker_overrides_path,
    universe_csv_path,
    universe_dir_path,
)
from .exceptions import DataNotFoundError, UniverseNotFoundError
from .universe import load_ticker_overrides

logger = logging.getLogger(__name__)

GICS_SECTORS = frozenset(
    {
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
    }
)

SUPPORTED_INDEX_IDS = frozenset({"SPX", "RTY", "NDX"})

_START_COLUMNS = ("effective_start", "start_date", "listed_date", "valid_from", "from", "start")
_END_COLUMNS = ("effective_end", "end_date", "delisted_date", "valid_to", "to", "end")
_SECTOR_COLUMNS = ("sector", "gics_sector", "gics_sector_name")
_MARKET_CAP_COLUMNS = ("market_cap_usd_millions", "market_cap_usd_mn", "market_cap_musd", "market_cap")
_INDEX_LEVEL_COLUMNS = ("total_return_level", "adj_close", "adjusted_close")
_INDEX_PRICE_COLUMNS = ("close", "price_return_level")


def _load_config() -> Config:
    return Config.load(default_config_path())


def _is_nullish(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    return False


def _as_datetime(value: str | date | datetime | None, *, name: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.combine(value, time.min) if isinstance(value, date) and not isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid date") from exc
    return parsed.replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)


def _date_bounds(start: str | date | datetime, end: str | date | datetime) -> tuple[datetime, datetime]:
    start_ts = _as_datetime(start, name="start")
    end_ts = _as_datetime(end, name="end")
    if start_ts is None or end_ts is None:
        raise ValueError("start and end are required")
    if start_ts >= end_ts:
        raise ValueError("start must be before end")
    return start_ts, end_ts


def _normalize_symbol(symbol: object) -> str:
    if _is_nullish(symbol):
        return ""
    return str(symbol or "").strip().upper()


def _first_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    by_lower = {column.lower(): column for column in columns}
    for candidate in candidates:
        found = by_lower.get(candidate)
        if found is not None:
            return found
    return None


def _read_csv(path: Path) -> pl.DataFrame:
    try:
        return pl.read_csv(path, infer_schema_length=1000)
    except FileNotFoundError as exc:
        raise exc
    except Exception as exc:
        raise DataNotFoundError(f"failed to read {path}: {exc}") from exc


def _resolve_universe_path(cfg: ConfigLike, universe_id: str) -> Path:
    clean_id = str(universe_id or "").strip()
    if not clean_id:
        raise UniverseNotFoundError("universe_id must be non-empty")
    if clean_id == "default":
        default_path = universe_csv_path(cfg)
        if default_path.exists():
            return default_path
        shard_path = universe_dir_path(cfg) / "default.csv"
        if shard_path.exists():
            return shard_path
        raise UniverseNotFoundError("default universe is not available")
    path = universe_dir_path(cfg) / f"{clean_id}.csv"
    if not path.exists():
        raise UniverseNotFoundError(f"universe_id not recognised: {clean_id}")
    return path


def _truthy_active_expr(column: str) -> pl.Expr:
    return pl.col(column).fill_null("1").cast(pl.String).str.strip_chars().str.to_lowercase().is_in(["1", "true", "yes", "y", "active"])


def _apply_point_in_time_filter(
    df: pl.DataFrame,
    as_of: datetime | None,
    *,
    warn_current_only: bool,
    require_history: bool = False,
) -> pl.DataFrame:
    if as_of is None:
        return df
    start_col = _first_column(df.columns, _START_COLUMNS)
    end_col = _first_column(df.columns, _END_COLUMNS)
    if start_col is None and end_col is None:
        if require_history:
            raise DataNotFoundError("point-in-time history is unavailable for the requested as_of date")
        if warn_current_only:
            warnings.warn("point-in-time history is unavailable; returning current classifications", UserWarning, stacklevel=3)
        return df
    conditions: list[pl.Expr] = []
    if start_col is not None:
        start_expr = pl.col(start_col).cast(pl.String).str.strptime(pl.Datetime, strict=False)
        conditions.append(start_expr.is_null() | (start_expr.dt.date() <= as_of.date()))
    if end_col is not None:
        end_expr = pl.col(end_col).cast(pl.String).str.strptime(pl.Datetime, strict=False)
        conditions.append(end_expr.is_null() | (end_expr.dt.date() >= as_of.date()))
    predicate = conditions[0]
    for condition in conditions[1:]:
        predicate = predicate & condition
    return df.filter(predicate)


def _normalize_date_column(frame: pl.DataFrame, column: str = "date") -> pl.DataFrame:
    return frame.with_columns(pl.col(column).cast(pl.Datetime, strict=False).dt.date().cast(pl.Datetime).alias(column))


def _ordered_unique(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _matrix_from_long(frame: pl.DataFrame, *, value_column: str, id_column: str = "symbol") -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame(schema={"date": pl.Datetime})
    return (
        frame.select(["date", id_column, value_column])
        .pivot(index="date", on=id_column, values=value_column, aggregate_function="last")
        .sort("date")
    )


def _drop_all_null_value_columns(frame: pl.DataFrame, *, label: str) -> pl.DataFrame:
    drop_columns = [
        column
        for column in frame.columns
        if column != "date" and frame.get_column(column).null_count() == frame.height
    ]
    for column in drop_columns:
        logger.warning("dropping %s with all-null values: %s", label, column)
    return frame.drop(drop_columns) if drop_columns else frame


def _ensure_value_columns(frame: pl.DataFrame, *, message: str) -> pl.DataFrame:
    if frame.is_empty() or len([column for column in frame.columns if column != "date"]) == 0:
        raise DataNotFoundError(message)
    return frame


def _non_null_numeric_values(frame: pl.DataFrame) -> pl.Series:
    value_columns = [column for column in frame.columns if column != "date"]
    if not value_columns:
        return pl.Series(dtype=pl.Float64)
    return frame.select(pl.concat_list([pl.col(column).cast(pl.Float64, strict=False) for column in value_columns]).alias("values")).explode("values").drop_nulls().get_column("values")


def get_universe_symbols(as_of: str | date | datetime | None = None, universe_id: str = "default") -> list[str]:
    """
    Return ticker symbols constituting the specified universe.

    If ``as_of`` is provided, the universe artifact must have point-in-time
    columns such as ``effective_start``/``effective_end`` or
    ``listed_date``/``delisted_date``. Current-only artifacts are accepted only
    when ``as_of`` is ``None``. Use
    ``get_sector_assignments(..., require_history=True)`` when sector lookups
    should follow the same strict point-in-time rule.
    """
    cfg = _load_config()
    as_of_ts = _as_datetime(as_of, name="as_of")
    path = _resolve_universe_path(cfg, universe_id)
    try:
        df = _read_csv(path)
    except FileNotFoundError as exc:
        raise UniverseNotFoundError(f"universe not found: {path}") from exc
    if "symbol" not in df.columns:
        raise UniverseNotFoundError(f"universe has no symbol column: {path}")
    if "active" in df.columns:
        df = df.filter(_truthy_active_expr("active"))
    df = _apply_point_in_time_filter(df, as_of_ts, warn_current_only=False, require_history=True)
    overrides = load_ticker_overrides(ticker_overrides_path(cfg))
    symbols = []
    for value in df.get_column("symbol").to_list():
        symbol = _normalize_symbol(value)
        if not symbol or "$" in symbol or any(ch.isspace() for ch in symbol):
            continue
        symbols.append(overrides.get(symbol, symbol))
    result = sorted(set(symbols))
    if not result:
        raise DataNotFoundError(f"universe has no usable symbols: {universe_id}")
    return result


def _read_daily_adjusted_close(root: Path, symbol: str, start: datetime, end: datetime) -> pl.DataFrame:
    path = root / f"{symbol}.parquet"
    if not path.exists():
        logger.warning("dropping symbol with no daily parquet: %s", symbol)
        return pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "adj_close": pl.Float64})
    try:
        df = pl.read_parquet(path, columns=["date", "adj_close"])
    except Exception as exc:
        logger.warning("dropping symbol with unreadable daily parquet %s: %s", symbol, exc)
        return pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "adj_close": pl.Float64})
    if "date" not in df.columns or "adj_close" not in df.columns:
        logger.warning("dropping symbol with incomplete daily parquet schema: %s", symbol)
        return pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "adj_close": pl.Float64})
    out = (
        _normalize_date_column(df)
        .with_columns(
            pl.col("adj_close").cast(pl.Float64, strict=False),
            pl.lit(symbol).alias("symbol"),
        )
        .drop_nulls(["date"])
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .unique(subset=["date"], keep="last")
        .sort("date")
    )
    if out.is_empty():
        logger.warning("dropping symbol with no daily data in requested window: %s", symbol)
    return out.select(["date", "symbol", "adj_close"])


def _adjusted_price_matrix(
    symbols: list[str],
    start: datetime,
    end: datetime,
    *,
    max_ffill: int,
    drop_sparse: bool,
) -> pl.DataFrame:
    if max_ffill < 0:
        raise ValueError("max_ffill must be non-negative")
    root = parquet_root_path(_load_config())
    frames = []
    for symbol in _ordered_unique(_normalize_symbol(raw_symbol) for raw_symbol in symbols):
        frame = _read_daily_adjusted_close(root, symbol, start, end)
        if not frame.is_empty():
            frames.append(frame)
    if not frames:
        raise DataNotFoundError("no requested symbols could be loaded")
    out = _matrix_from_long(pl.concat(frames, how="vertical"), value_column="adj_close")
    value_columns = [column for column in out.columns if column != "date"]
    out = out.with_columns([pl.col(column).forward_fill(limit=max_ffill) for column in value_columns])
    drop_columns = [column for column in value_columns if out.get_column(column).null_count() == out.height]
    if drop_sparse:
        drop_columns.extend(
            column
            for column in value_columns
            if column not in drop_columns and out.get_column(column).null_count() / max(1, out.height) > 0.5
        )
    for column in drop_columns:
        logger.warning("dropping symbol with insufficient adjusted-price coverage: %s", column)
    out = out.drop(drop_columns) if drop_columns else out
    return _ensure_value_columns(out, message="no requested symbols have usable adjusted prices")


def get_adjusted_prices(
    symbols: list[str],
    start: str | date | datetime,
    end: str | date | datetime,
    max_ffill: int = 5,
) -> pl.DataFrame:
    """Return a Polars wide frame: ``date`` plus one adjusted-close column per symbol."""
    start_ts, end_ts = _date_bounds(start, end)
    prices = _adjusted_price_matrix(symbols, start_ts, end_ts, max_ffill=max_ffill, drop_sparse=False)
    valid = _non_null_numeric_values(prices)
    if not valid.is_empty() and bool((valid <= 0).any()):
        raise ValueError("adjusted prices must be strictly positive")
    return prices


def get_total_returns(
    symbols: list[str],
    start: str | date | datetime,
    end: str | date | datetime,
    max_ffill: int = 5,
    max_daily_return: float = 0.5,
) -> pl.DataFrame:
    """Return a Polars wide frame of simple daily total returns derived from adjusted close."""
    if max_daily_return <= 0:
        raise ValueError("max_daily_return must be positive")
    start_ts, end_ts = _date_bounds(start, end)
    prices = _adjusted_price_matrix(symbols, start_ts, end_ts, max_ffill=max_ffill, drop_sparse=True)
    value_columns = [column for column in prices.columns if column != "date"]
    returns = prices.with_columns([pl.col(column).pct_change().alias(column) for column in value_columns])
    returns = _drop_all_null_value_columns(returns, label="symbol")
    returns = _ensure_value_columns(returns, message="no requested symbols have usable total returns")
    valid = _non_null_numeric_values(returns)
    if not valid.is_empty() and bool(((valid <= -1) | (valid > max_daily_return)).any()):
        raise ValueError(f"total returns outside (-1, {max_daily_return}] indicate invalid adjusted-price data")
    return returns


def _read_market_cap_frame(root: Path, symbol: str, start: datetime, end: datetime) -> pl.DataFrame:
    path = root / f"{symbol}.parquet"
    if not path.exists():
        logger.warning("dropping symbol with no market-cap parquet: %s", symbol)
        return pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "market_cap_usd_millions": pl.Float64})
    try:
        df = pl.read_parquet(path)
    except Exception as exc:
        logger.warning("dropping symbol with unreadable market-cap parquet %s: %s", symbol, exc)
        return pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "market_cap_usd_millions": pl.Float64})
    date_col = _first_column(df.columns, ("date", "timestamp"))
    value_col = _first_column(df.columns, _MARKET_CAP_COLUMNS)
    if date_col is None or value_col is None:
        logger.warning("dropping symbol with incomplete market-cap schema: %s", symbol)
        return pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "market_cap_usd_millions": pl.Float64})
    value_expr = pl.col(value_col).cast(pl.Float64, strict=False)
    if value_col == "market_cap":
        value_expr = value_expr / 1_000_000.0
    out = (
        df.select(
            pl.col(date_col).cast(pl.Datetime, strict=False).dt.date().cast(pl.Datetime).alias("date"),
            value_expr.alias("market_cap_usd_millions"),
        )
        .with_columns(pl.lit(symbol).alias("symbol"))
        .drop_nulls(["date", "market_cap_usd_millions"])
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .unique(subset=["date"], keep="last")
        .sort("date")
    )
    if out.is_empty():
        logger.warning("dropping symbol with no market-cap data in requested window: %s", symbol)
    return out.select(["date", "symbol", "market_cap_usd_millions"])


def _daily_calendar(symbols: list[str], start: datetime, end: datetime) -> pl.DataFrame:
    root = parquet_root_path(_load_config())
    frames = []
    for symbol in symbols:
        path = root / f"{symbol}.parquet"
        if not path.exists():
            continue
        try:
            frame = pl.read_parquet(path, columns=["date"])
        except Exception:
            continue
        frames.append(_normalize_date_column(frame).filter((pl.col("date") >= start) & (pl.col("date") <= end)).select("date"))
    if not frames:
        return pl.DataFrame(schema={"date": pl.Datetime})
    return pl.concat(frames, how="vertical").drop_nulls("date").unique().sort("date")


def _daily_market_cap_frame(symbol: str, market_caps: pl.DataFrame, calendar: pl.DataFrame) -> pl.DataFrame:
    if market_caps.is_empty() or calendar.is_empty():
        return pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "market_cap_usd_millions": pl.Float64})
    joined = calendar.join_asof(
        market_caps.select(
            pl.col("date").alias("effective_date"),
            "market_cap_usd_millions",
        ).sort("effective_date"),
        left_on="date",
        right_on="effective_date",
        strategy="backward",
    )
    return (
        joined.with_columns(
            pl.int_range(pl.len()).over("effective_date").alias("_age_rows"),
            pl.lit(symbol).alias("symbol"),
        )
        .with_columns(
            pl.when(pl.col("_age_rows") < 21)
            .then(pl.col("market_cap_usd_millions"))
            .otherwise(None)
            .alias("market_cap_usd_millions")
        )
        .select(["date", "symbol", "market_cap_usd_millions"])
    )


def get_market_caps(
    symbols: list[str],
    start: str | date | datetime,
    end: str | date | datetime,
    frequency: str = "monthly",
) -> pl.DataFrame:
    """Return a Polars wide frame of market capitalisation in USD millions."""
    if frequency not in {"daily", "monthly"}:
        raise ValueError('frequency must be one of {"daily", "monthly"}')
    start_ts, end_ts = _date_bounds(start, end)
    root = market_cap_root_path(_load_config())
    clean_symbols = _ordered_unique(_normalize_symbol(raw_symbol) for raw_symbol in symbols)
    frames = []
    for symbol in clean_symbols:
        read_start = start_ts if frequency == "monthly" else start_ts - timedelta(days=45)
        frame = _read_market_cap_frame(root, symbol, read_start, end_ts)
        if frame.is_empty():
            continue
        if frequency == "daily":
            frame = _daily_market_cap_frame(symbol, frame, _daily_calendar([symbol], start_ts, end_ts))
        else:
            frame = frame.filter((pl.col("date") >= start_ts) & (pl.col("date") <= end_ts))
        if not frame.is_empty():
            frames.append(frame)
    if not frames:
        raise DataNotFoundError("no requested symbols could be loaded from market-cap artifacts")
    out = _matrix_from_long(pl.concat(frames, how="vertical"), value_column="market_cap_usd_millions")
    out = _drop_all_null_value_columns(out, label="symbol")
    out = _ensure_value_columns(out, message="no requested symbols have usable market-cap data")
    valid = _non_null_numeric_values(out)
    if not valid.is_empty() and bool((valid <= 0).any()):
        raise ValueError("market caps must be strictly positive")
    return out


def get_sector_assignments(
    symbols: list[str],
    as_of: str | date | datetime | None = None,
    *,
    require_history: bool = False,
) -> pl.DataFrame:
    """
    Return a Polars frame with ``symbol`` and ``sector`` columns.

    Current-only sector artifacts are supported. If ``as_of`` is provided and
    the artifact has no point-in-time columns, a ``UserWarning`` is issued and
    current classifications are returned unless ``require_history=True``, in
    which case ``DataNotFoundError`` is raised to match
    ``get_universe_symbols(as_of=...)``.
    """
    cfg = _load_config()
    path = sector_assignments_path(cfg)
    if not path.exists():
        raise DataNotFoundError(f"sector assignment artifact not found: {path}")
    df = _read_csv(path)
    if "symbol" not in df.columns:
        raise DataNotFoundError("sector assignment artifact has no symbol column")
    sector_col = _first_column(df.columns, _SECTOR_COLUMNS)
    if sector_col is None:
        raise DataNotFoundError("sector assignment artifact has no sector column")
    as_of_ts = _as_datetime(as_of, name="as_of")
    df = _apply_point_in_time_filter(
        df,
        as_of_ts,
        warn_current_only=not require_history,
        require_history=require_history,
    )
    df = df.with_columns(
        pl.col("symbol").map_elements(_normalize_symbol, return_dtype=pl.String),
        pl.col(sector_col).cast(pl.String).alias("sector"),
    )
    start_col = _first_column(df.columns, _START_COLUMNS)
    if start_col is not None:
        df = df.with_columns(pl.col(start_col).cast(pl.String).str.strptime(pl.Datetime, strict=False).alias("_sort_start")).sort(["symbol", "_sort_start"])
    sector_by_symbol = {
        row["symbol"]: row["sector"]
        for row in df.unique(subset=["symbol"], keep="last").select(["symbol", "sector"]).iter_rows(named=True)
    }
    ordered_symbols = [_normalize_symbol(symbol) for symbol in symbols]
    out = pl.DataFrame({"symbol": ordered_symbols, "sector": [sector_by_symbol.get(symbol) for symbol in ordered_symbols]})
    bad = out.filter(pl.col("sector").is_not_null() & ~pl.col("sector").is_in(list(GICS_SECTORS)))
    if not bad.is_empty():
        raise ValueError("sector assignments must use the fixed GICS sector vocabulary")
    if out.get_column("sector").drop_nulls().is_empty():
        raise DataNotFoundError("no requested symbols have sector assignments")
    return out


def _read_index_return_frame(root: Path, index_id: str, start: datetime, end: datetime) -> pl.DataFrame:
    path = root / f"{index_id}.parquet"
    if not path.exists():
        logger.warning("dropping index with no return artifact: %s", index_id)
        return pl.DataFrame(schema={"date": pl.Datetime, "index_id": pl.String, "return": pl.Float64})
    try:
        df = pl.read_parquet(path)
    except Exception as exc:
        logger.warning("dropping index with unreadable return artifact %s: %s", index_id, exc)
        return pl.DataFrame(schema={"date": pl.Datetime, "index_id": pl.String, "return": pl.Float64})
    date_col = _first_column(df.columns, ("date", "timestamp"))
    return_col = _first_column(df.columns, ("return", "total_return", "total_return_simple"))
    level_col = _first_column(df.columns, _INDEX_LEVEL_COLUMNS)
    price_col = _first_column(df.columns, _INDEX_PRICE_COLUMNS)
    if date_col is None:
        logger.warning("dropping index with no date column: %s", index_id)
        return pl.DataFrame(schema={"date": pl.Datetime, "index_id": pl.String, "return": pl.Float64})
    out = df.with_columns(pl.col(date_col).cast(pl.Datetime, strict=False).dt.date().cast(pl.Datetime).alias("date"))
    if return_col is not None:
        out = out.with_columns(pl.col(return_col).cast(pl.Float64, strict=False).alias("return"))
    elif level_col is not None:
        out = out.sort("date").with_columns(pl.col(level_col).cast(pl.Float64, strict=False).pct_change().alias("return"))
    elif price_col is not None:
        warnings.warn(f"{index_id} returns are derived from price-return levels, not documented total-return levels", UserWarning, stacklevel=3)
        out = out.sort("date").with_columns(pl.col(price_col).cast(pl.Float64, strict=False).pct_change().alias("return"))
    else:
        logger.warning("dropping index with no return or total-return level column: %s", index_id)
        return pl.DataFrame(schema={"date": pl.Datetime, "index_id": pl.String, "return": pl.Float64})
    out = (
        out.with_columns(pl.lit(index_id).alias("index_id"))
        .select(["date", "index_id", "return"])
        .drop_nulls(["date"])
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .unique(subset=["date"], keep="last")
        .sort("date")
    )
    if out.is_empty():
        logger.warning("dropping index with no data in requested window: %s", index_id)
    return out


def get_index_returns(
    index_ids: list[str],
    start: str | date | datetime,
    end: str | date | datetime,
) -> pl.DataFrame:
    """Return a Polars wide frame: ``date`` plus one total-return column per index id."""
    start_ts, end_ts = _date_bounds(start, end)
    root = index_returns_root_path(_load_config())
    frames = []
    seen: set[str] = set()
    for raw_index_id in index_ids:
        index_id = str(raw_index_id or "").strip().upper()
        if not index_id or index_id in seen:
            continue
        seen.add(index_id)
        if index_id not in SUPPORTED_INDEX_IDS:
            logger.warning("dropping unsupported index_id: %s", index_id)
            continue
        frame = _read_index_return_frame(root, index_id, start_ts, end_ts)
        if not frame.is_empty():
            frames.append(frame)
    if not frames:
        raise DataNotFoundError("no requested index_ids could be loaded")
    out = _matrix_from_long(pl.concat(frames, how="vertical"), value_column="return", id_column="index_id")
    out = _drop_all_null_value_columns(out, label="index")
    return _ensure_value_columns(out, message="no requested index_ids have usable returns")
