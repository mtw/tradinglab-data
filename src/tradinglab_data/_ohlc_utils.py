from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
from polars.datatypes.classes import DataTypeClass

from .schema import DAILY_PARQUET_SCHEMA, INTRADAY_PARQUET_SCHEMA

PriceFrame = pl.DataFrame | None


def scalar_eq(a: object, b: object, tol: float = 1e-12) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(str(a)) - float(str(b))) <= tol
    except Exception:
        return str(a) == str(b)


def currency_from_df(df: PriceFrame) -> str | None:
    if df is None or df.is_empty() or "currency" not in df.columns:
        return None
    try:
        values = (
            df.select(pl.col("currency").cast(pl.String, strict=False))
            .drop_nulls()
            .get_column("currency")
            .to_list()
        )
    except Exception:
        return None
    if not values:
        return None
    value = str(values[0]).strip().upper()
    return value or None


def resolve_currency(
    symbol: str,
    fetch_currency: Callable[[str], str | None],
    *,
    df_hint: PriceFrame = None,
    cache: dict[str, str] | None = None,
    default_currency: str = "UNKNOWN",
) -> str:
    if cache is not None and symbol in cache:
        return cache[symbol]
    from_df = currency_from_df(df_hint)
    if from_df:
        if cache is not None:
            cache[symbol] = from_df
        return from_df
    fetched = fetch_currency(symbol) or default_currency
    if cache is not None:
        cache[symbol] = fetched
    return fetched


def ensure_currency(
    df: PriceFrame,
    currency: str | None,
    *,
    postprocess: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
) -> PriceFrame:
    if df is None:
        return None
    resolved = (currency or "UNKNOWN").strip().upper() or "UNKNOWN"
    if "currency" in df.columns:
        out = df.with_columns(
            pl.when(
                pl.col("currency").cast(pl.String, strict=False).is_null()
                | (pl.col("currency").cast(pl.String, strict=False) == "")
            )
            .then(pl.lit(resolved))
            .otherwise(pl.col("currency").cast(pl.String, strict=False))
            .alias("currency")
        )
    else:
        out = df.with_columns(pl.lit(resolved).alias("currency"))
    return postprocess(out) if postprocess is not None else out


def align_for_concat(
    df_left: pl.DataFrame,
    df_right: pl.DataFrame,
    *,
    schema: dict[str, pl.DataType | DataTypeClass],
    preferred_columns: list[str] | None = None,
    postprocess: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    preferred = preferred_columns or list(schema.keys())
    columns = list(dict.fromkeys(preferred + sorted(set(df_left.columns) | set(df_right.columns))))
    left_missing = [column for column in columns if column not in df_left.columns]
    right_missing = [column for column in columns if column not in df_right.columns]
    if left_missing:
        df_left = df_left.with_columns([pl.lit(None).alias(column) for column in left_missing])
    if right_missing:
        df_right = df_right.with_columns([pl.lit(None).alias(column) for column in right_missing])

    casts_left = []
    casts_right = []
    for column in columns:
        dtype = schema.get(column)
        if dtype is None:
            continue
        casts_left.append(pl.col(column).cast(dtype, strict=False).alias(column))
        casts_right.append(pl.col(column).cast(dtype, strict=False).alias(column))
    if casts_left:
        df_left = df_left.with_columns(casts_left)
    if casts_right:
        df_right = df_right.with_columns(casts_right)

    df_left = df_left.select(columns)
    df_right = df_right.select(columns)
    if postprocess is not None:
        df_left = postprocess(df_left)
        df_right = postprocess(df_right)
    return df_left, df_right


def needs_incremental_write(
    df_old: PriceFrame,
    df_inc: PriceFrame,
    *,
    compare_columns: list[str] | None = None,
    compare_values: Callable[[object, object], bool] = scalar_eq,
) -> bool:
    if df_old is None or df_old.is_empty():
        return True
    if df_inc is None or df_inc.is_empty():
        return False
    if "date" not in df_old.columns or "date" not in df_inc.columns:
        return True
    try:
        old_last_date = df_old.select(pl.col("date").max()).item()
        inc_last_date = df_inc.select(pl.col("date").max()).item()
    except Exception:
        return True
    if old_last_date is None or inc_last_date is None:
        return True
    if inc_last_date > old_last_date:
        return True
    columns = compare_columns or [
        column
        for column in ["open", "high", "low", "close", "adj_close", "volume", "currency"]
        if column in df_old.columns and column in df_inc.columns
    ]
    if not columns:
        return False
    try:
        old_last = df_old.filter(pl.col("date") == old_last_date).tail(1)
        inc_last = df_inc.filter(pl.col("date") == old_last_date).tail(1)
        if old_last.is_empty() or inc_last.is_empty():
            return False
        for column in columns:
            if not compare_values(
                old_last.get_column(column).to_list()[0],
                inc_last.get_column(column).to_list()[0],
            ):
                return True
    except Exception:
        return True
    return False


def sanitize_ohlc_df(df: PriceFrame) -> PriceFrame:
    if df is None or df.is_empty():
        return df
    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        return df
    return (
        df.filter(
            pl.col("date").is_not_null()
            & pl.col("open").is_not_null()
            & pl.col("high").is_not_null()
            & pl.col("low").is_not_null()
            & pl.col("close").is_not_null()
        )
        .filter((pl.col("open") > 0) & (pl.col("high") > 0) & (pl.col("low") > 0) & (pl.col("close") > 0))
        .filter(
            (pl.col("high") >= pl.col("low"))
            & (pl.col("high") >= pl.col("open"))
            & (pl.col("high") >= pl.col("close"))
            & (pl.col("low") <= pl.col("open"))
            & (pl.col("low") <= pl.col("close"))
        )
    )


def ohlc_quality_counts(df: PriceFrame) -> dict[str, int]:
    if df is None or df.is_empty():
        return {"null_ohlc": 0, "bad_ohlc": 0, "dup_dates": 0}
    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        return {"null_ohlc": 1, "bad_ohlc": 1, "dup_dates": 1}
    null_ohlc = int(
        df.select(
            pl.any_horizontal(
                [
                    pl.col("open").is_null(),
                    pl.col("high").is_null(),
                    pl.col("low").is_null(),
                    pl.col("close").is_null(),
                ]
            ).sum()
        ).item()
    )
    bad_ohlc = int(
        df.select(
            (
                (pl.col("open") <= 0)
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
    dup_dates = int(df.height - df.select(pl.col("date").n_unique()).item())
    return {"null_ohlc": null_ohlc, "bad_ohlc": bad_ohlc, "dup_dates": dup_dates}


def assert_postwrite_integrity(
    path: Path,
    symbol: str,
    *,
    enabled: bool,
    read_frame: Callable[[Path], PriceFrame],
    append_log: Callable[[Path, str, str, int], None],
    log_path: Path,
) -> None:
    if not enabled:
        return
    checked = read_frame(path)
    quality = ohlc_quality_counts(checked)
    if quality["null_ohlc"] > 0 or quality["bad_ohlc"] > 0 or quality["dup_dates"] > 0:
        msg = (
            "postwrite_integrity_failed:"
            f"null_ohlc={quality['null_ohlc']},"
            f"bad_ohlc={quality['bad_ohlc']},"
            f"dup_dates={quality['dup_dates']}"
        )
        append_log(log_path, symbol, msg, 1)
        raise RuntimeError(f"{symbol}: {msg}")


DAILY_SCHEMA_COLUMNS = list(DAILY_PARQUET_SCHEMA.keys())
INTRADAY_SCHEMA_COLUMNS = list(INTRADAY_PARQUET_SCHEMA.keys())
