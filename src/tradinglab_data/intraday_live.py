from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from ._intraday_fetch import (
    MAX_PERIOD_BY_INTERVAL,
    UPDATE_PERIOD_BY_INTERVAL,
    fetch_extended_intraday,
    period_for_interval,
)
from ._yf_utils import coerce_standard_schema
from .contracts import IntradayLiveSyncResult, IntradayLiveValidateResult
from .data_yf import fetch_symbol_currency, read_parquet_if_exists
from .schema import INTRADAY_LIVE_PARQUET_SCHEMA, validate_intraday_live_frame

SUPPORTED_INTERVALS = {"5m"}
SUPPORTED_PROVIDER = "yahoo"
DEFAULT_EXCHANGE_TIMEZONE = "America/New_York"


@dataclass(frozen=True)
class IntradayLiveInspectEntry:
    symbol: str
    exists: bool
    rows: int
    start: str | None
    end: str | None
    valid: bool
    issues: list[str]
    path: str


def intraday_live_parquet_path(root: str | Path, *, interval: str, symbol: str) -> Path:
    return Path(root) / interval / f"{symbol}.parquet"


def empty_intraday_live_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=INTRADAY_LIVE_PARQUET_SCHEMA)


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _validate_options(*, interval: str, provider: str) -> None:
    if interval not in SUPPORTED_INTERVALS:
        supported = ", ".join(sorted(SUPPORTED_INTERVALS))
        raise ValueError(f"Unsupported intraday live interval: {interval!r}. Supported intervals: {supported}.")
    if provider.strip().lower() != SUPPORTED_PROVIDER:
        raise ValueError(f"Unsupported intraday live provider: {provider!r}. Supported provider: {SUPPORTED_PROVIDER}.")


def _local_ts(timestamp_column: str, *, exchange_timezone: str) -> pl.Expr:
    return pl.col(timestamp_column).dt.replace_time_zone("UTC").dt.convert_time_zone(exchange_timezone)


def _session_label_expr(timestamp_column: str, *, exchange_timezone: str) -> pl.Expr:
    local = _local_ts(timestamp_column, exchange_timezone=exchange_timezone)
    weekday = local.dt.weekday()
    hour = local.dt.hour()
    minute = local.dt.minute()
    return (
        pl.when(weekday > 5)
        .then(pl.lit("unknown"))
        .when((hour < 9) | ((hour == 9) & (minute < 30)))
        .then(pl.lit("pre"))
        .when(hour < 16)
        .then(pl.lit("regular"))
        .when((hour < 20) | ((hour == 20) & (minute == 0)))
        .then(pl.lit("post"))
        .otherwise(pl.lit("unknown"))
    )


def normalize_intraday_live_frame(
    df: pl.DataFrame | None,
    *,
    symbol: str,
    currency: str,
    interval: str = "5m",
    provider: str = SUPPORTED_PROVIDER,
    exchange_timezone: str = DEFAULT_EXCHANGE_TIMEZONE,
    ingested_at: datetime | None = None,
) -> pl.DataFrame:
    _validate_options(interval=interval, provider=provider)
    if df is None or df.is_empty():
        return empty_intraday_live_frame()
    prepared = coerce_standard_schema(df)
    if prepared.is_empty():
        return empty_intraday_live_frame()
    timestamp_name = "date" if "date" in prepared.columns else "timestamp"
    tz_name = ZoneInfo(exchange_timezone).key
    session_expr = _session_label_expr("timestamp", exchange_timezone=tz_name)
    out = (
        prepared.rename({timestamp_name: "timestamp"})
        .select(["timestamp", "open", "high", "low", "close", "volume"])
        .filter(pl.col("timestamp").is_not_null())
        .filter(
            ~(
                pl.col("open").is_null()
                & pl.col("high").is_null()
                & pl.col("low").is_null()
                & pl.col("close").is_null()
            )
        )
        .with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime),
                session_expr.alias("session"),
                pl.col("timestamp").dt.replace_time_zone("UTC").dt.convert_time_zone(tz_name).dt.date().alias("session_date"),
                pl.lit(currency or "UNKNOWN").cast(pl.String).alias("currency"),
                pl.lit(symbol).cast(pl.String).alias("symbol"),
                pl.lit(interval).cast(pl.String).alias("interval"),
                pl.lit(provider.strip().lower()).cast(pl.String).alias("provider"),
                (session_expr == "regular").cast(pl.Boolean).alias("is_regular_session"),
                pl.lit(True).cast(pl.Boolean).alias("is_closed_bar"),
                pl.lit(ingested_at or _utc_now_naive()).cast(pl.Datetime).alias("ingested_at"),
            ]
        )
        .unique(subset=["timestamp"], keep="last")
        .sort("timestamp")
        .select(list(INTRADAY_LIVE_PARQUET_SCHEMA))
    )
    return out


def trim_intraday_live_window(df: pl.DataFrame, *, retention_days: int) -> pl.DataFrame:
    if df.is_empty() or retention_days <= 0:
        return df.sort("timestamp")
    cutoff = _utc_now_naive() - timedelta(days=max(1, int(retention_days)))
    return df.filter(pl.col("timestamp") >= cutoff).sort("timestamp")


def update_intraday_live_store(
    symbols: list[str],
    *,
    live_root: str | Path,
    interval: str = "5m",
    provider: str = SUPPORTED_PROVIDER,
    exchange_timezone: str = DEFAULT_EXCHANGE_TIMEZONE,
    universe_name: str = "intraday_live_core",
    retention_days: int = 0,
    full_window: bool = False,
    chunk_size: int = 20,
    sleep_seconds: float = 1.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_repeat_cooldown_hours: float = 24.0,
    log_path: Path | None = None,
    warning_state_path: Path | None = None,
    fetch_intraday_fn=None,
    read_frame_fn=None,
    fetch_currency_fn=None,
) -> IntradayLiveSyncResult:
    _validate_options(interval=interval, provider=provider)
    if fetch_intraday_fn is None:
        fetch_intraday_fn = fetch_extended_intraday
    if read_frame_fn is None:
        read_frame_fn = read_parquet_if_exists
    if fetch_currency_fn is None:
        fetch_currency_fn = fetch_symbol_currency
    root = Path(live_root)
    interval_root = root / interval
    interval_root.mkdir(parents=True, exist_ok=True)
    if not symbols:
        return {
            "interval": interval,
            "universe": universe_name,
            "root": str(interval_root),
            "symbols": [],
            "files_written": 0,
            "rows_written": 0,
            "unchanged_symbols": [],
            "skipped_symbols": [],
        }
    existing = [symbol for symbol in symbols if intraday_live_parquet_path(root, interval=interval, symbol=symbol).exists()]
    missing = [symbol for symbol in symbols if symbol not in set(existing)]
    initial_map = (
        fetch_intraday_fn(
            symbols=missing,
            interval=interval,
            period=period_for_interval(interval, MAX_PERIOD_BY_INTERVAL, purpose="initial intraday live fetch"),
            prepost=True,
            chunk_size=chunk_size,
            sleep_seconds=sleep_seconds,
            max_retries=max_retries,
            backoff_max_seconds=backoff_max_seconds,
            threads=threads,
            log_repeat_cooldown_hours=log_repeat_cooldown_hours,
            log_path=log_path,
            warning_state_path=warning_state_path,
        )
        if missing
        else {}
    )
    update_period = MAX_PERIOD_BY_INTERVAL if full_window else UPDATE_PERIOD_BY_INTERVAL
    update_map = (
        fetch_intraday_fn(
            symbols=existing,
            interval=interval,
            period=period_for_interval(interval, update_period, purpose="incremental intraday live fetch"),
            prepost=True,
            chunk_size=chunk_size,
            sleep_seconds=sleep_seconds,
            max_retries=max_retries,
            backoff_max_seconds=backoff_max_seconds,
            threads=threads,
            log_repeat_cooldown_hours=log_repeat_cooldown_hours,
            log_path=log_path,
            warning_state_path=warning_state_path,
        )
        if existing
        else {}
    )
    files_written = 0
    rows_written = 0
    unchanged_symbols: list[str] = []
    skipped_symbols: list[str] = []
    for symbol in symbols:
        path = intraday_live_parquet_path(root, interval=interval, symbol=symbol)
        fetched = initial_map.get(symbol) if symbol in initial_map else update_map.get(symbol)
        existing_df = read_frame_fn(path)
        current = empty_intraday_live_frame() if existing_df is None or existing_df.is_empty() else existing_df
        currency = (fetch_currency_fn(symbol) or "UNKNOWN").strip().upper() or "UNKNOWN"
        new_rows = normalize_intraday_live_frame(
            fetched,
            symbol=symbol,
            currency=currency,
            interval=interval,
            provider=provider,
            exchange_timezone=exchange_timezone,
        )
        if current.is_empty() and new_rows.is_empty():
            skipped_symbols.append(symbol)
            continue
        if current.is_empty():
            combined = new_rows
        elif new_rows.is_empty():
            unchanged_symbols.append(symbol)
            validate_intraday_live_frame(current, allow_extra_columns=False)
            continue
        else:
            combined = pl.concat([current, new_rows], how="vertical").unique(subset=["timestamp"], keep="last").sort("timestamp")
        combined = trim_intraday_live_window(combined, retention_days=retention_days)
        validate_intraday_live_frame(combined, allow_extra_columns=False)
        combined.write_parquet(str(path))
        files_written += 1
        rows_written += int(combined.height)
    return {
        "interval": interval,
        "universe": universe_name,
        "root": str(interval_root),
        "symbols": list(symbols),
        "files_written": files_written,
        "rows_written": rows_written,
        "unchanged_symbols": unchanged_symbols,
        "skipped_symbols": skipped_symbols,
    }


def inspect_intraday_live_store(symbols: list[str], *, live_root: str | Path, interval: str = "5m") -> list[dict[str, object]]:
    _validate_options(interval=interval, provider=SUPPORTED_PROVIDER)
    root = Path(live_root)
    entries: list[dict[str, object]] = []
    for symbol in symbols:
        path = intraday_live_parquet_path(root, interval=interval, symbol=symbol)
        if not path.exists():
            entries.append(IntradayLiveInspectEntry(symbol, False, 0, None, None, False, ["missing_file"], str(path)).__dict__)
            continue
        issues: list[str] = []
        valid = True
        try:
            frame = pl.read_parquet(str(path))
            validate_intraday_live_frame(frame, allow_extra_columns=False)
        except Exception as exc:
            valid = False
            issues.append(str(exc))
            frame = pl.read_parquet(str(path))
        start = frame.select(pl.col("timestamp").min()).item() if not frame.is_empty() else None
        end = frame.select(pl.col("timestamp").max()).item() if not frame.is_empty() else None
        entries.append(
            IntradayLiveInspectEntry(
                symbol=symbol,
                exists=True,
                rows=int(frame.height),
                start=start.isoformat() if isinstance(start, datetime) else None,
                end=end.isoformat() if isinstance(end, datetime) else None,
                valid=valid,
                issues=issues,
                path=str(path),
            ).__dict__
        )
    return entries


def validate_intraday_live_store(
    symbols: list[str], *, live_root: str | Path, interval: str = "5m", universe_name: str = "intraday_live_core"
) -> IntradayLiveValidateResult:
    inspected = inspect_intraday_live_store(symbols, live_root=live_root, interval=interval)
    dirty_files = [str(item["path"]) for item in inspected if (not bool(item["exists"])) or (not bool(item["valid"]))]
    errors = []
    for item in inspected:
        issues = item.get("issues")
        if isinstance(issues, list):
            errors.extend([f"{item['symbol']}: {issue}" for issue in issues])
    return {
        "ok": not dirty_files,
        "interval": interval,
        "universe": universe_name,
        "root": str(Path(live_root) / interval),
        "files_checked": len(inspected),
        "dirty_files": dirty_files,
        "errors": errors,
    }
