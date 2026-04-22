from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

from ..schema import CRYPTO_PARQUET_SCHEMA, validate_crypto_frame

INTERVAL_EXPECTED_DELTA = {
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
}


def filter_closed_bars(df: pl.DataFrame, *, interval: str, now_ts: datetime | None = None) -> pl.DataFrame:
    if df.is_empty():
        return df
    expected = INTERVAL_EXPECTED_DELTA[interval]
    cutoff = (now_ts or datetime.now(timezone.utc)).replace(tzinfo=None) - expected
    return df.filter(pl.col("timestamp") <= cutoff)


def normalize_crypto_frame_schema(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema=CRYPTO_PARQUET_SCHEMA)
    return (
        df.with_columns(
            pl.col("timestamp").cast(pl.Datetime("us"), strict=False),
            pl.col("ingested_at").cast(pl.Datetime("us"), strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("provider").cast(pl.String, strict=False),
            pl.col("exchange").cast(pl.String, strict=False),
            pl.col("market_type").cast(pl.String, strict=False),
            pl.col("symbol").cast(pl.String, strict=False),
            pl.col("base_asset").cast(pl.String, strict=False),
            pl.col("quote_asset").cast(pl.String, strict=False),
            pl.col("interval").cast(pl.String, strict=False),
            pl.col("is_closed").cast(pl.Boolean, strict=False),
            pl.col("source_symbol").cast(pl.String, strict=False),
        )
        .select(list(CRYPTO_PARQUET_SCHEMA))
    )


def validate_crypto_ohlcv_frame(
    df: pl.DataFrame,
    *,
    interval: str,
    require_continuity: bool = True,
) -> None:
    df = normalize_crypto_frame_schema(df)
    validate_crypto_frame(df, allow_extra_columns=False)
    if df.is_empty():
        return
    if df.get_column("timestamp").null_count() > 0:
        raise ValueError("timestamp contains nulls")
    if df.get_column("timestamp").n_unique() != df.height:
        raise ValueError("timestamp values must be unique")
    sorted_df = df.sort("timestamp")
    if not sorted_df.equals(df):
        raise ValueError("rows must be sorted by timestamp ascending")
    if sorted_df.filter(~pl.col("is_closed")).height > 0:
        raise ValueError("canonical crypto history may only contain closed bars")
    invalid_ohlc = sorted_df.filter(
        (pl.col("low") > pl.min_horizontal("open", "close", "high"))
        | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("volume") < 0)
    )
    if invalid_ohlc.height > 0:
        raise ValueError("ohlcv constraints failed")
    for column in ["provider", "exchange", "market_type", "symbol", "base_asset", "quote_asset", "interval", "source_symbol"]:
        if sorted_df.get_column(column).null_count() > 0:
            raise ValueError(f"{column} contains nulls")
    if require_continuity and sorted_df.height > 1:
        expected = INTERVAL_EXPECTED_DELTA[interval]
        diffs = sorted_df.get_column("timestamp").diff().drop_nulls()
        invalid = [diff for diff in diffs.to_list() if diff != expected]
        if invalid:
            raise ValueError(f"interval continuity failed for {interval}")


def merge_crypto_frames(existing: pl.DataFrame | None, incoming: pl.DataFrame) -> pl.DataFrame:
    normalized_incoming = normalize_crypto_frame_schema(incoming)
    if existing is None or existing.is_empty():
        return normalized_incoming.sort("timestamp")
    normalized_existing = normalize_crypto_frame_schema(existing)
    combined = pl.concat([normalized_existing, normalized_incoming], how="vertical").unique(subset=["timestamp"], keep="last").sort("timestamp")
    return combined.select(list(CRYPTO_PARQUET_SCHEMA))
