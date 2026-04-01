from __future__ import annotations

import csv
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import polars as pl
import yfinance as yf
from tqdm import tqdm

from ._yf_utils import (
    STANDARD_PRICE_SCHEMA,
)
from ._yf_utils import (
    backoff_sleep as _backoff_sleep,
)
from ._yf_utils import (
    coerce_standard_schema as _coerce_standard_schema,
)
from ._yf_utils import (
    is_rate_limit_error as _is_rate_limit_error,
)
from ._yf_utils import (
    normalize_yf_df_to_polars as _normalize_yf_df_to_polars,
)
from ._yf_utils import (
    share_class_fallback as _share_class_fallback,
)
from ._yf_utils import (
    split_bulk_download as _split_bulk_download,
)
from ._yf_utils import (
    yf_date_window as _yf_date_window,
)

_CURRENCY_CACHE: dict[str, str | None] = {}
_CURRENCY_CACHE_LOCK = Lock()


@dataclass(frozen=True)
class YFDownloadSpec:
    symbol: str
    interval: str = "1d"
    lookback_days: int = 2000


def fetch_yfinance_history(spec: YFDownloadSpec) -> pl.DataFrame:
    start_s, end_s = _yf_date_window(spec.lookback_days)

    df_pd = yf.download(
        spec.symbol,
        start=start_s,
        end=end_s,
        interval=spec.interval,
        auto_adjust=False,  # keep raw OHLC; use adj_close if you want adjusted
        progress=False,
        group_by="column",
    )

    if df_pd is None or len(df_pd) == 0:
        fallback = _share_class_fallback(spec.symbol)
        if fallback and fallback != spec.symbol:
            df_pd = yf.download(
                fallback,
                start=start_s,
                end=end_s,
                interval=spec.interval,
                auto_adjust=False,
                progress=False,
                group_by="column",
            )
    if df_pd is None or len(df_pd) == 0:
        return pl.DataFrame(schema=STANDARD_PRICE_SCHEMA)

    return _normalize_yf_df_to_polars(df_pd)


def fetch_symbol_currency(symbol: str) -> str | None:
    with _CURRENCY_CACHE_LOCK:
        if symbol in _CURRENCY_CACHE:
            return _CURRENCY_CACHE[symbol]

    currency: str | None = None
    try:
        ticker = yf.Ticker(symbol)
        fast_info = getattr(ticker, "fast_info", None)
        if fast_info is not None:
            try:
                currency = fast_info.get("currency")
            except Exception:
                try:
                    currency = fast_info["currency"]
                except Exception:
                    currency = None
        if not currency:
            info = ticker.get_info()
            if isinstance(info, dict):
                currency = info.get("currency")
    except Exception:
        currency = None

    if isinstance(currency, str):
        currency = currency.strip().upper() or None
    else:
        currency = None
    with _CURRENCY_CACHE_LOCK:
        _CURRENCY_CACHE[symbol] = currency
    return currency


def fetch_yfinance_history_bulk(
    symbols: list[str],
    interval: str,
    lookback_days: int,
    chunk_size: int = 100,
    sleep_seconds: float = 2.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
    show_progress: bool = False,
    progress_desc: str = "yfinance bulk",
) -> dict[str, pl.DataFrame]:
    if not symbols:
        return {}

    start_s, end_s = _yf_date_window(lookback_days)

    results: dict[str, pl.DataFrame] = {}
    chunk_starts = range(0, len(symbols), chunk_size)
    iterator = tqdm(chunk_starts, desc=progress_desc, unit="chunk") if show_progress else chunk_starts
    for i in iterator:
        chunk = symbols[i : i + chunk_size]
        attempt = 0
        while True:
            try:
                df_pd = yf.download(
                    chunk,
                    start=start_s,
                    end=end_s,
                    interval=interval,
                    auto_adjust=False,
                    progress=False,
                    group_by="column",
                    threads=threads,
                )
                chunk_map = _split_bulk_download(df_pd, chunk)
                # Share-class fallback (e.g. BRK.B -> BRK-B) for symbols not present in bulk response.
                missing_syms = [sym for sym in chunk if sym not in chunk_map]
                for msym in missing_syms:
                    alt = _share_class_fallback(msym)
                    if not alt:
                        continue
                    try:
                        df_one = yf.download(
                            alt,
                            start=start_s,
                            end=end_s,
                            interval=interval,
                            auto_adjust=False,
                            progress=False,
                            group_by="column",
                            threads=False,
                        )
                        if df_one is not None and len(df_one) > 0:
                            chunk_map[msym] = _normalize_yf_df_to_polars(df_one)
                    except Exception:
                        pass
                results.update(chunk_map)
                break
            except Exception as e:
                attempt += 1
                if _is_rate_limit_error(e) and attempt <= max_retries:
                    _backoff_sleep(attempt, backoff_max_seconds)
                    continue
                if log_path is not None:
                    for sym in chunk:
                        append_update_log(log_path, sym, str(e), attempt)
                break

        time.sleep(sleep_seconds)

    return results


def read_parquet_if_exists(path: Path) -> pl.DataFrame | None:
    if path.exists():
        return pl.read_parquet(str(path))
    return None


def upsert_symbol_parquet(
    symbol: str,
    interval: str,
    lookback_days: int,
    parquet_root: str | Path,
) -> Path:
    """Legacy single-symbol parquet updater.

    This helper predates the bulk workflow layer in `workflows.py`. It performs
    a direct fetch/merge/write cycle for one symbol and bypasses the
    workflow-level currency resolution, sanitization policy, and post-write
    integrity assertions used by the main package update commands.
    """
    warnings.warn(
        "upsert_symbol_parquet() is deprecated; prefer config-driven bulk update workflows in tradinglab_data.workflows.",
        DeprecationWarning,
        stacklevel=2,
    )
    root = Path(parquet_root)
    root.mkdir(parents=True, exist_ok=True)
    out_path = root / f"{symbol}.parquet"

    existing = read_parquet_if_exists(out_path)

    if existing is None or existing.is_empty():
        df_new = fetch_yfinance_history(YFDownloadSpec(symbol=symbol, interval=interval, lookback_days=lookback_days))
        df_new = _coerce_standard_schema(df_new)
        df_new.write_parquet(str(out_path))
        return out_path

    # Incremental update: fetch a small recent window and merge
    last_dt = existing.select(pl.col("date").max()).item()
    # pull a bit earlier to be safe re: revisions/holes
    lookback_buffer = 14
    fetch_days = 60
    if isinstance(last_dt, datetime):
        now = datetime.now(last_dt.tzinfo) if last_dt.tzinfo is not None else datetime.now()
        fetch_days = max(lookback_buffer, max(0, (now - last_dt).days) + 5)

    df_inc = fetch_yfinance_history(YFDownloadSpec(symbol=symbol, interval=interval, lookback_days=fetch_days))
    if df_inc.is_empty():
        return out_path

    existing = _coerce_standard_schema(existing)
    df_inc = _coerce_standard_schema(df_inc)

    combined = (
        pl.concat([existing, df_inc], how="vertical")
        .unique(subset=["date"], keep="last")
        .sort("date")
    )

    combined.write_parquet(str(out_path))
    return out_path


def append_update_log(log_path: Path, symbol: str, error: str, attempt_count: int) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    exists = log_path.exists()
    with log_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["timestamp", "symbol", "error", "attempt_count"])
        writer.writerow([datetime.now(timezone.utc).isoformat(), symbol, error, attempt_count])


def clear_currency_cache() -> None:
    with _CURRENCY_CACHE_LOCK:
        _CURRENCY_CACHE.clear()
