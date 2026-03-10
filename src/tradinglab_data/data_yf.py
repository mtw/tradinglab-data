from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import time
import re

import yfinance as yf
import polars as pl
from tqdm import tqdm

_CURRENCY_CACHE: dict[str, str | None] = {}


_STANDARD_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
}


def _yf_date_window(lookback_days: int) -> tuple[str, str]:
    # Yahoo end-date is effectively exclusive for history requests.
    # Shift end by +1 day to reduce "missing latest bar" issues near timezone boundaries.
    end = datetime.now(timezone.utc) + timedelta(days=1)
    start = end - timedelta(days=lookback_days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class YFDownloadSpec:
    symbol: str
    interval: str = "1d"
    lookback_days: int = 2000


def _normalize_yf_df_to_polars(df_pd) -> pl.DataFrame:
    # 1) Flatten MultiIndex columns if present (can happen depending on yfinance/Yahoo response)
    try:
        import pandas as pd  # yfinance returns pandas
        if isinstance(df_pd.columns, pd.MultiIndex):
            df_pd = df_pd.copy()
            df_pd.columns = [c[0] for c in df_pd.columns]
    except Exception:
        pass

    # 2) Bring index into a column
    df_pd = df_pd.copy()
    df_pd.reset_index(inplace=True)

    # 3) Identify the datetime column robustly
    # Common cases: "Date", "Datetime", or "index" (when index name is None)
    date_col = None
    for cand in ["Date", "Datetime", "date", "datetime", "index"]:
        if cand in df_pd.columns:
            date_col = cand
            break

    # Fallback: first column that looks like datetime
    if date_col is None:
        for c in df_pd.columns:
            if str(df_pd[c].dtype).startswith("datetime"):
                date_col = c
                break

    if date_col is None:
        raise ValueError(f"Could not identify a datetime column. Columns: {list(df_pd.columns)}")

    # 4) Rename to canonical names
    colmap = {
        date_col: "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df_pd.rename(columns={k: v for k, v in colmap.items() if k in df_pd.columns}, inplace=True)

    # 4) Ensure unique column names to satisfy Polars
    try:
        df_pd = df_pd.loc[:, ~df_pd.columns.duplicated()]
    except Exception:
        pass

    df = pl.from_pandas(df_pd)

    # 5) Ensure date is datetime
    df = df.with_columns(pl.col("date").cast(pl.Datetime))

    # 6) Keep standard columns only
    keep = [c for c in ["date", "open", "high", "low", "close", "adj_close", "volume"] if c in df.columns]
    df = df.select(keep).sort("date")

    return _coerce_standard_schema(df)


def _coerce_standard_schema(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    for c, dt in _STANDARD_SCHEMA.items():
        if c not in out.columns:
            out = out.with_columns(pl.lit(None).cast(dt).alias(c))
    casts = []
    for c, dt in _STANDARD_SCHEMA.items():
        if c in out.columns:
            casts.append(pl.col(c).cast(dt, strict=False).alias(c))
    out = out.with_columns(casts)
    keep = [c for c in ["date", "open", "high", "low", "close", "adj_close", "volume"] if c in out.columns]
    return out.select(keep).sort("date")


def _share_class_fallback(symbol: str) -> str | None:
    s = (symbol or "").strip()
    m = re.match(r"^([A-Z]+)\.([A-Z])$", s)
    if not m:
        return None
    base, cls = m.group(1), m.group(2)
    return f"{base}-{cls}"


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
        return pl.DataFrame(schema=_STANDARD_SCHEMA)

    return _normalize_yf_df_to_polars(df_pd)


def fetch_symbol_currency(symbol: str) -> str | None:
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
    _CURRENCY_CACHE[symbol] = currency
    return currency


def _is_rate_limit_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    msg = str(exc).lower()
    if "ratelimit" in name or "rate limit" in msg:
        return True
    if "too many requests" in msg or "429" in msg:
        return True
    return False


def _backoff_sleep(attempt: int, backoff_max_seconds: float) -> float:
    base = 5.0
    delay = min(backoff_max_seconds, base * (2 ** max(0, attempt - 1)))
    jitter = random.uniform(0, 1.0)
    total = delay + jitter
    time.sleep(total)
    return total


def _split_bulk_download(df_pd, symbols: list[str]) -> dict[str, pl.DataFrame]:
    if df_pd is None or len(df_pd) == 0:
        return {}

    out: dict[str, pl.DataFrame] = {}
    try:
        import pandas as pd
        if isinstance(df_pd.columns, pd.MultiIndex):
            # yfinance layouts vary by version/options:
            #   A) level0 = symbol, level1 = field
            #   B) level0 = field,  level1 = symbol
            level0 = set(map(str, df_pd.columns.get_level_values(0)))
            level1 = set(map(str, df_pd.columns.get_level_values(1)))

            # Layout A: symbol on level0
            if any(s in level0 for s in symbols):
                for sym in symbols:
                    if sym in level0:
                        sym_df = df_pd[sym]
                        out[sym] = _normalize_yf_df_to_polars(sym_df)
                if out:
                    return out

            # Layout B: symbol on level1
            if any(s in level1 for s in symbols):
                for sym in symbols:
                    if sym in level1:
                        try:
                            sym_df = df_pd.xs(sym, axis=1, level=1)
                        except Exception:
                            continue
                        out[sym] = _normalize_yf_df_to_polars(sym_df)
                if out:
                    return out
    except Exception:
        pass

    # Fallback for non-MultiIndex (usually single-symbol response).
    if len(symbols) == 1:
        sym = symbols[0]
        out[sym] = _normalize_yf_df_to_polars(df_pd)
    return out


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
    fetch_days = max(lookback_buffer, 60)

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
    import csv

    exists = log_path.exists()
    with log_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["timestamp", "symbol", "error", "attempt_count"])
        writer.writerow([datetime.now(timezone.utc).isoformat(), symbol, error, attempt_count])
