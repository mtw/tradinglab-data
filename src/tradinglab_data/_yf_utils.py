from __future__ import annotations

import contextlib
import io
import random
import re
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

STANDARD_PRICE_SCHEMA: dict[str, type[pl.DataType]] = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
}


def yf_date_window(lookback_days: int) -> tuple[str, str]:
    # Yahoo end-date is effectively exclusive for history requests.
    # Shift end by +1 day to reduce "missing latest bar" issues near timezone boundaries.
    end = datetime.now(timezone.utc) + timedelta(days=1)
    start = end - timedelta(days=lookback_days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def coerce_standard_schema(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    for column, dtype in STANDARD_PRICE_SCHEMA.items():
        if column not in out.columns:
            out = out.with_columns(pl.lit(None).cast(dtype).alias(column))
    casts = [
        pl.col(column).cast(dtype, strict=False).alias(column)
        for column, dtype in STANDARD_PRICE_SCHEMA.items()
        if column in out.columns
    ]
    out = out.with_columns(casts)
    keep = [column for column in STANDARD_PRICE_SCHEMA if column in out.columns]
    return out.select(keep).sort("date")


def normalize_yf_df_to_polars(df_pd) -> pl.DataFrame:
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
    date_col = None
    for candidate in ["Date", "Datetime", "date", "datetime", "index"]:
        if candidate in df_pd.columns:
            date_col = candidate
            break

    if date_col is None:
        for column in df_pd.columns:
            if str(df_pd[column].dtype).startswith("datetime"):
                date_col = column
                break

    if date_col is None:
        raise ValueError(f"Could not identify a datetime column. Columns: {list(df_pd.columns)}")

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

    try:
        df_pd = df_pd.loc[:, ~df_pd.columns.duplicated()]
    except Exception:
        pass

    df = pl.from_pandas(df_pd)
    df = df.with_columns(pl.col("date").cast(pl.Datetime))
    keep = [column for column in STANDARD_PRICE_SCHEMA if column in df.columns]
    df = df.select(keep).sort("date")
    return coerce_standard_schema(df)


def share_class_fallback(symbol: str) -> str | None:
    raw = (symbol or "").strip()
    match = re.match(r"^([A-Z]+)\.([A-Z])$", raw)
    if not match:
        return None
    base, cls = match.group(1), match.group(2)
    return f"{base}-{cls}"


def is_rate_limit_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    msg = str(exc).lower()
    if "ratelimit" in name or "rate limit" in msg:
        return True
    if "too many requests" in msg or "429" in msg:
        return True
    return False


def backoff_sleep(attempt: int, backoff_max_seconds: float) -> float:
    base = 5.0
    delay = min(backoff_max_seconds, base * (2 ** max(0, attempt - 1)))
    jitter = random.uniform(0, 1.0)
    total = delay + jitter
    time.sleep(total)
    return total


def split_bulk_download(df_pd, symbols: list[str]) -> dict[str, pl.DataFrame]:
    if df_pd is None or len(df_pd) == 0:
        return {}

    out: dict[str, pl.DataFrame] = {}
    try:
        import pandas as pd

        if isinstance(df_pd.columns, pd.MultiIndex):
            level0 = set(map(str, df_pd.columns.get_level_values(0)))
            level1 = set(map(str, df_pd.columns.get_level_values(1)))

            if any(symbol in level0 for symbol in symbols):
                for symbol in symbols:
                    if symbol in level0:
                        out[symbol] = normalize_yf_df_to_polars(df_pd[symbol])
                if out:
                    return out

            if any(symbol in level1 for symbol in symbols):
                for symbol in symbols:
                    if symbol not in level1:
                        continue
                    try:
                        sym_df = df_pd.xs(symbol, axis=1, level=1)
                    except Exception:
                        continue
                    out[symbol] = normalize_yf_df_to_polars(sym_df)
                if out:
                    return out
    except Exception:
        pass

    if len(symbols) == 1:
        out[symbols[0]] = normalize_yf_df_to_polars(df_pd)
    return out


def run_yf_download(download_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> tuple[Any, str]:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
        df_pd = download_fn(*args, **kwargs)
    return df_pd, buffer.getvalue()


def classify_yf_download_issue(output: str) -> str | None:
    text = (output or "").strip()
    if not text:
        return None

    host_match = re.search(r"could not resolve host:\s*([^\s.]+(?:\.[^\s.]+)*)", text, flags=re.IGNORECASE)
    if host_match:
        return f"yahoo_connectivity_error: could not resolve host {host_match.group(1)}"

    if re.search(r"temporary failure in name resolution", text, flags=re.IGNORECASE):
        return "yahoo_connectivity_error: temporary failure in name resolution"

    if re.search(r"name or service not known", text, flags=re.IGNORECASE):
        return "yahoo_connectivity_error: name or service not known"

    curl_match = re.search(r"curl:\s*\((\d+)\)", text, flags=re.IGNORECASE)
    if curl_match:
        return f"yahoo_connectivity_error: curl ({curl_match.group(1)})"

    return None
