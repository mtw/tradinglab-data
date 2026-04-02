from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import yfinance as yf
from tqdm import tqdm

from ._yf_utils import (
    backoff_sleep,
    classify_yf_download_issue,
    coerce_standard_schema,
    is_rate_limit_error,
    normalize_yf_df_to_polars,
    run_yf_download,
    split_bulk_download,
)
from .data_yf import append_update_log
from .schema import SchemaDtype

INTRADAY_SCHEMA: dict[str, SchemaDtype] = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.String,
}

MAX_PERIOD_BY_INTERVAL = {
    "5m": "60d",
    "1m": "7d",
}

UPDATE_PERIOD_BY_INTERVAL = {
    "5m": "10d",
    "1m": "2d",
}


def period_for_interval(interval: str, mapping: dict[str, str], *, purpose: str) -> str:
    period = mapping.get(interval)
    if period is None:
        supported = ", ".join(sorted(mapping))
        raise ValueError(f"Unsupported intraday interval for {purpose}: {interval!r}. Supported intervals: {supported}.")
    return period


def sanitize_intraday_df(df: pl.DataFrame | None) -> pl.DataFrame:
    if df is None or df.is_empty():
        return pl.DataFrame(schema=INTRADAY_SCHEMA)
    out = df
    needed = [c for c in ["date", "open", "high", "low", "close", "adj_close", "volume"] if c in out.columns]
    if needed:
        out = out.select(needed + ([c for c in ["currency"] if c in out.columns]))
    out = out.filter(
        ~(
            pl.col("open").is_null()
            & pl.col("high").is_null()
            & pl.col("low").is_null()
            & pl.col("close").is_null()
        )
    )
    if "date" in out.columns:
        out = out.filter(pl.col("date").is_not_null())
    return out.sort("date")


def normalize_intraday_pd(df_pd) -> pl.DataFrame:
    try:
        idx = getattr(df_pd, "index", None)
        if idx is not None and getattr(idx, "tz", None) is not None:
            df_pd = df_pd.copy()
            df_pd.index = idx.tz_convert("UTC").tz_localize(None)
    except Exception:
        pass
    df = normalize_yf_df_to_polars(df_pd)
    df = coerce_standard_schema(df)
    return sanitize_intraday_df(df.select(["date", "open", "high", "low", "close", "adj_close", "volume"]))


def fetch_intraday_bulk(
    symbols: list[str],
    interval: str,
    period: str,
    prepost: bool = True,
    chunk_size: int = 20,
    sleep_seconds: float = 1.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
    show_progress: bool = False,
    progress_desc: str = "extended-hours fetch",
) -> dict[str, pl.DataFrame]:
    if not symbols:
        return {}

    results: dict[str, pl.DataFrame] = {}
    chunk_starts = range(0, len(symbols), chunk_size)
    iterator = tqdm(chunk_starts, desc=progress_desc, unit="chunk") if show_progress else chunk_starts
    for i in iterator:
        chunk = symbols[i : i + chunk_size]
        attempt = 0
        while True:
            try:
                df_pd, output, exc = run_yf_download(
                    yf.download,
                    chunk,
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    prepost=prepost,
                    progress=False,
                    group_by="column",
                    threads=threads,
                )
                issue = classify_yf_download_issue(f"{output}\n{exc}" if exc is not None else output)
                if exc is not None and issue is None:
                    raise exc
                chunk_map = split_bulk_download(df_pd, chunk)
                if issue is not None and not chunk_map:
                    if log_path is not None:
                        for sym in chunk:
                            append_update_log(log_path, sym, f"intraday_{interval}_{issue}", attempt + 1)
                    break
                out_chunk: dict[str, pl.DataFrame] = {}
                for sym, df_one in chunk_map.items():
                    try:
                        out_chunk[sym] = coerce_standard_schema(df_one)
                    except Exception:
                        continue
                results.update(out_chunk)
                break
            except Exception as e:
                attempt += 1
                if is_rate_limit_error(e) and attempt <= max_retries:
                    backoff_sleep(attempt, backoff_max_seconds)
                    continue
                if log_path is not None:
                    for sym in chunk:
                        append_update_log(log_path, sym, f"intraday_{interval}_error:{e}", attempt)
                break
        time.sleep(sleep_seconds)
    return results


def fetch_intraday_one(
    symbol: str,
    interval: str,
    period: str,
    prepost: bool = True,
) -> pl.DataFrame:
    df_pd, output, exc = run_yf_download(
        yf.download,
        symbol,
        period=period,
        interval=interval,
        auto_adjust=False,
        prepost=prepost,
        progress=False,
        group_by="column",
        threads=False,
    )
    issue = classify_yf_download_issue(f"{output}\n{exc}" if exc is not None else output)
    if exc is not None and issue is None:
        raise exc
    if df_pd is None or len(df_pd) == 0:
        return pl.DataFrame(schema=INTRADAY_SCHEMA)
    return normalize_intraday_pd(df_pd)


def fetch_extended_intraday(
    symbols: list[str],
    interval: str,
    period: str,
    prepost: bool = True,
    chunk_size: int = 20,
    sleep_seconds: float = 1.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
) -> dict[str, pl.DataFrame]:
    out = fetch_intraday_bulk(
        symbols=symbols,
        interval=interval,
        period=period,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
        show_progress=True,
        progress_desc=f"YF intraday {interval}",
    )
    missing = [s for s in symbols if s not in out or out[s].is_empty()]
    for sym in missing:
        try:
            df = fetch_intraday_one(sym, interval=interval, period=period, prepost=prepost)
            if not df.is_empty():
                out[sym] = df
        except Exception as e:
            if log_path is not None:
                append_update_log(log_path, sym, f"intraday_{interval}_single_error:{e}", 1)
    return out


def trim_rolling_window(df: pl.DataFrame, retention_days: int) -> pl.DataFrame:
    if df.is_empty():
        return df
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=max(1, retention_days))
    return df.filter(pl.col("date") >= pl.lit(cutoff)).sort("date")
