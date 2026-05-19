from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import yfinance as yf

from ._yf_utils import normalize_yf_df_to_polars
from .schema import FX_DAILY_PARQUET_SCHEMA, validate_fx_daily_frame


def normalize_currency(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def normalize_pair(pair: object) -> str:
    normalized = normalize_currency(pair)
    if len(normalized) != 6 or not normalized.isalpha():
        raise ValueError(f"Invalid FX pair: {pair!r}")
    return normalized


def split_pair(pair: str) -> tuple[str, str]:
    normalized = normalize_pair(pair)
    return normalized[:3], normalized[3:]


def fx_pair_path(root: str | Path, pair: str) -> Path:
    return Path(root) / f"{normalize_pair(pair)}.parquet"


def provider_symbol_for_pair(pair: str, *, provider: str = "yahoo") -> str:
    normalized = normalize_pair(pair)
    if provider != "yahoo":
        raise ValueError(f"Unsupported FX provider: {provider}")
    return f"{normalized}=X"


def load_fx_pair(root: str | Path, pair: str, *, strict: bool = True) -> pl.DataFrame:
    path = fx_pair_path(root, pair)
    if not path.exists():
        raise FileNotFoundError(path)
    df = pl.read_parquet(path)
    if strict:
        errors = validate_fx_daily_frame(df, pair=pair)
        if errors:
            raise ValueError("\n".join(errors))
    return df


def write_fx_pair(df: pl.DataFrame, root: str | Path, pair: str) -> Path:
    path = fx_pair_path(root, pair)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def validate_fx_pair(root: str | Path, pair: str) -> list[str]:
    try:
        df = load_fx_pair(root, pair, strict=False)
    except Exception as exc:
        return [str(exc)]
    return validate_fx_daily_frame(df, pair=pair)


def available_fx_pairs(root: str | Path) -> list[str]:
    root_path = Path(root)
    if not root_path.exists():
        return []
    return sorted(path.stem.upper() for path in root_path.glob("*.parquet"))


def derive_inverse_fx_frame(df: pl.DataFrame, inverse_pair: str) -> pl.DataFrame:
    pair = normalize_pair(inverse_pair)
    base_currency, quote_currency = split_pair(pair)
    source_symbol_expr = pl.concat_str([pl.col("source_symbol"), pl.lit(" inverted")])
    return (
        df.with_columns(
            (1.0 / pl.col("open")).alias("open"),
            (1.0 / pl.col("low")).alias("high"),
            (1.0 / pl.col("high")).alias("low"),
            (1.0 / pl.col("close")).alias("close"),
            pl.lit(pair).alias("pair"),
            pl.lit(base_currency).alias("base_currency"),
            pl.lit(quote_currency).alias("quote_currency"),
            source_symbol_expr.alias("source_symbol"),
        )
        .select(list(FX_DAILY_PARQUET_SCHEMA))
        .sort("date")
    )


def identity_fx_frame(pair: str, dates: Iterable[object]) -> pl.DataFrame:
    normalized = normalize_pair(pair)
    base_currency, quote_currency = split_pair(normalized)
    if base_currency != quote_currency:
        raise ValueError(f"Identity frame requires base == quote: {pair}")
    date_series = pl.Series("date", list(dates)).cast(pl.Datetime, strict=False)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return pl.DataFrame(
        {
            "date": date_series,
            "open": [1.0] * len(date_series),
            "high": [1.0] * len(date_series),
            "low": [1.0] * len(date_series),
            "close": [1.0] * len(date_series),
            "provider": ["identity"] * len(date_series),
            "pair": [normalized] * len(date_series),
            "base_currency": [base_currency] * len(date_series),
            "quote_currency": [quote_currency] * len(date_series),
            "source_symbol": [normalized] * len(date_series),
            "ingested_at": [now] * len(date_series),
        },
        schema_overrides=FX_DAILY_PARQUET_SCHEMA,
    )


def _fetch_yahoo_pair(symbol: str, *, start: str | None = None, end: str | None = None) -> pl.DataFrame:
    frame = yf.download(symbol, start=start, end=end, interval="1d", auto_adjust=False, progress=False, group_by="column")
    if frame is None or len(frame) == 0:
        return pl.DataFrame(schema={"date": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64})
    normalized = normalize_yf_df_to_polars(frame)
    return normalized.select("date", "open", "high", "low", "close")


def _finalize_fx_frame(df: pl.DataFrame, *, pair: str, provider: str, source_symbol: str) -> pl.DataFrame:
    base_currency, quote_currency = split_pair(pair)
    return (
        df.with_columns(
            pl.lit(provider).alias("provider"),
            pl.lit(pair).alias("pair"),
            pl.lit(base_currency).alias("base_currency"),
            pl.lit(quote_currency).alias("quote_currency"),
            pl.lit(source_symbol).alias("source_symbol"),
            pl.lit(datetime.now(timezone.utc).replace(tzinfo=None)).alias("ingested_at"),
        )
        .select(list(FX_DAILY_PARQUET_SCHEMA))
        .sort("date")
    )


def sync_fx_pair_yahoo(
    pair: str,
    root: str | Path,
    *,
    start: str | None = None,
    end: str | None = None,
    provider: str = "yahoo",
    allow_inverse: bool = True,
) -> dict[str, object]:
    normalized = normalize_pair(pair)
    direct_symbol = provider_symbol_for_pair(normalized, provider=provider)
    direct_df = _fetch_yahoo_pair(direct_symbol, start=start, end=end)
    used_inverse = False
    source_symbol = direct_symbol
    if direct_df.is_empty():
        if not allow_inverse:
            raise ValueError(f"No FX data returned for {normalized}")
        inverse_base, inverse_quote = split_pair(normalized)
        inverse_pair = inverse_quote + inverse_base
        inverse_symbol = provider_symbol_for_pair(inverse_pair, provider=provider)
        inverse_df = _fetch_yahoo_pair(inverse_symbol, start=start, end=end)
        if inverse_df.is_empty():
            raise ValueError(f"No FX data returned for {normalized} or inverse {inverse_pair}")
        direct_df = derive_inverse_fx_frame(_finalize_fx_frame(inverse_df, pair=inverse_pair, provider=provider, source_symbol=inverse_symbol), normalized)
        used_inverse = True
        source_symbol = f"{inverse_symbol} inverted"
    else:
        direct_df = _finalize_fx_frame(direct_df, pair=normalized, provider=provider, source_symbol=direct_symbol)
    errors = validate_fx_daily_frame(direct_df, pair=normalized)
    if errors:
        raise ValueError("\n".join(errors))
    path = write_fx_pair(direct_df, root, normalized)
    return {
        "ok": True,
        "pair": normalized,
        "provider": provider,
        "rows_written": direct_df.height,
        "path": str(path),
        "source_symbol": source_symbol,
        "used_inverse": used_inverse,
    }
