from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from urllib.request import urlopen

import polars as pl


@dataclass(frozen=True)
class StooqDownloadSpec:
    symbol: str
    timeout_seconds: int = 30


_SUFFIX_TO_STOOQ_MARKET = {
    "VI": "AT",
    "DE": "DE",
    "F": "DE",
    "PA": "FR",
    "MI": "IT",
    "AS": "NL",
    "L": "UK",
    "SW": "CH",
    "CO": "DK",
    "ST": "SE",
    "HE": "FI",
    "OL": "NO",
    "BR": "BE",
    "TO": "CA",
    "V": "CA",
    "AX": "AU",
    "T": "JP",
    "HK": "HK",
    "SI": "SG",
}

_SUFFIX_TO_CURRENCY = {
    "VI": "EUR",
    "DE": "EUR",
    "F": "EUR",
    "PA": "EUR",
    "MI": "EUR",
    "AS": "EUR",
    "L": "GBP",
    "SW": "CHF",
    "CO": "DKK",
    "ST": "SEK",
    "HE": "EUR",
    "OL": "NOK",
    "BR": "EUR",
    "TO": "CAD",
    "V": "CAD",
    "AX": "AUD",
    "T": "JPY",
    "HK": "HKD",
    "SI": "SGD",
}


def infer_currency_from_symbol(symbol: str) -> str:
    raw = (symbol or "").strip().upper()
    if "." not in raw:
        return "USD"
    suffix = raw.rsplit(".", 1)[1]
    return _SUFFIX_TO_CURRENCY.get(suffix, "UNKNOWN")


def stooq_symbol_from_yahoo(symbol: str) -> str:
    raw = (symbol or "").strip().upper()
    if not raw:
        return ""
    if "." in raw:
        base, suffix = raw.rsplit(".", 1)
        mapped = _SUFFIX_TO_STOOQ_MARKET.get(suffix)
        if mapped:
            return f"{base}.{mapped}".lower()
        return raw.lower()
    return f"{raw}.US".lower()


def _stooq_daily_url(stooq_symbol: str) -> str:
    return f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"


def _parse_stooq_csv_text(text: str) -> pl.DataFrame:
    if not text.strip():
        return pl.DataFrame(
            schema={
                "date": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "adj_close": pl.Float64,
                "volume": pl.Int64,
            }
        )

    df = pl.read_csv(StringIO(text), ignore_errors=True)
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "date": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "adj_close": pl.Float64,
                "volume": pl.Int64,
            }
        )

    rename_map = {}
    for c in df.columns:
        low = c.strip().lower()
        if low == "date":
            rename_map[c] = "date"
        elif low == "open":
            rename_map[c] = "open"
        elif low == "high":
            rename_map[c] = "high"
        elif low == "low":
            rename_map[c] = "low"
        elif low == "close":
            rename_map[c] = "close"
        elif low == "volume":
            rename_map[c] = "volume"
    df = df.rename(rename_map)

    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        return pl.DataFrame(
            schema={
                "date": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "adj_close": pl.Float64,
                "volume": pl.Int64,
            }
        )

    if "volume" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("volume"))

    out = (
        df.select(["date", "open", "high", "low", "close", "volume"])
        .with_columns(
            pl.col("date").str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False).alias("date"),
            pl.col("open").cast(pl.Float64, strict=False).alias("open"),
            pl.col("high").cast(pl.Float64, strict=False).alias("high"),
            pl.col("low").cast(pl.Float64, strict=False).alias("low"),
            pl.col("close").cast(pl.Float64, strict=False).alias("close"),
            pl.col("volume").cast(pl.Int64, strict=False).fill_null(0).alias("volume"),
        )
        .drop_nulls(subset=["date", "open", "high", "low", "close"])
        .with_columns(pl.col("close").alias("adj_close"))
        .sort("date")
    )
    return out


def fetch_stooq_history(spec: StooqDownloadSpec) -> pl.DataFrame:
    symbol = (spec.symbol or "").strip()
    if not symbol:
        return pl.DataFrame(
            schema={
                "date": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "adj_close": pl.Float64,
                "volume": pl.Int64,
            }
        )

    stooq_primary = stooq_symbol_from_yahoo(symbol)
    candidates = [stooq_primary, symbol.lower()]
    if "." in symbol:
        base = symbol.split(".", 1)[0]
        candidates.append(f"{base.lower()}.us")
    else:
        candidates.append(f"{symbol.lower()}.us")

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        url = _stooq_daily_url(candidate)
        try:
            with urlopen(url, timeout=spec.timeout_seconds) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            df = _parse_stooq_csv_text(text)
            if not df.is_empty():
                return df
        except Exception:
            continue

    return pl.DataFrame(
        schema={
            "date": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "adj_close": pl.Float64,
            "volume": pl.Int64,
        }
    )
