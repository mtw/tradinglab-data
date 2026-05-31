from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import yfinance as yf

from ._yf_utils import normalize_yf_df_to_polars
from .config import (
    ConfigLike,
    index_returns_root_path,
    market_cap_root_path,
    parquet_root_path,
    sector_assignments_path,
    ticker_overrides_path,
    universe_csv_path,
    universe_dir_path,
)
from .market_data import GICS_SECTORS, SUPPORTED_INDEX_IDS
from .schema import (
    INDEX_RETURN_PARQUET_SCHEMA,
    MARKET_CAP_PARQUET_SCHEMA,
    SECTOR_ASSIGNMENT_SCHEMA,
    validate_index_return_frame,
    validate_market_cap_frame,
    validate_sector_assignment_frame,
)
from .universe import load_universe_frame

INDEX_PROVIDER_SYMBOLS = {
    "SPX": "^SP500TR",
    "RTY": "^RUTTR",
    "NDX": "^NDXT",
}

YAHOO_SECTOR_TO_GICS = {
    "Basic Materials": "Materials",
    "Communication Services": "Communication Services",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Energy": "Energy",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Industrials": "Industrials",
    "Real Estate": "Real Estate",
    "Technology": "Information Technology",
    "Utilities": "Utilities",
}

logger = logging.getLogger(__name__)


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_symbol(symbol: object) -> str:
    if symbol is None:
        return ""
    if isinstance(symbol, float) and symbol != symbol:
        return ""
    return str(symbol or "").strip().upper()


def _normalized_currency_series(df: pl.DataFrame) -> pl.Series | None:
    if "currency" not in df.columns:
        return None
    return df.get_column("currency").cast(pl.String, strict=False).fill_null("").str.strip_chars().str.to_uppercase()


def _load_symbols_from_config(cfg: ConfigLike, symbols_override: list[str] | None = None) -> list[str]:
    if symbols_override:
        return sorted({_normalize_symbol(symbol) for symbol in symbols_override if _normalize_symbol(symbol)})
    df = load_universe_frame(
        universe_csv_path(cfg),
        universe_dir=universe_dir_path(cfg),
        ticker_overrides_path=ticker_overrides_path(cfg),
    )
    if df.is_empty():
        raise ValueError("No universe symbols available for market-data workflow")
    return sorted(df.get_column("symbol").to_list())


def _read_daily_close(root: Path, symbol: str) -> pl.DataFrame:
    path = root / f"{symbol}.parquet"
    if not path.exists():
        return pl.DataFrame(schema={"date": pl.Datetime, "close": pl.Float64, "currency": pl.String})
    df = pl.read_parquet(path)
    if "date" not in df.columns or "close" not in df.columns:
        return pl.DataFrame(schema={"date": pl.Datetime, "close": pl.Float64, "currency": pl.String})
    currency_expr = pl.col("currency").cast(pl.String).str.to_uppercase() if "currency" in df.columns else pl.lit("USD")
    return (
        df.select(
            pl.col("date").cast(pl.Datetime, strict=False).dt.date().cast(pl.Datetime).alias("date"),
            pl.col("close").cast(pl.Float64, strict=False).alias("close"),
            currency_expr.alias("currency"),
        )
        .drop_nulls(["date", "close"])
        .sort("date")
    )


def _fetch_shares_full(symbol: str, *, start: str | None = None, end: str | None = None) -> pl.DataFrame:
    ticker = yf.Ticker(symbol)
    shares = ticker.get_shares_full(start=start, end=end)
    if shares is None:
        return pl.DataFrame(schema={"date": pl.Datetime, "shares_outstanding": pl.Float64})
    try:
        frame = pl.DataFrame({"date": list(shares.index), "shares_outstanding": list(shares.to_numpy())})
    except Exception:
        return pl.DataFrame(schema={"date": pl.Datetime, "shares_outstanding": pl.Float64})
    return (
        frame.select(
            pl.col("date").cast(pl.Datetime, strict=False).dt.date().cast(pl.Datetime).alias("date"),
            pl.col("shares_outstanding").cast(pl.Float64, strict=False),
        )
        .drop_nulls(["date", "shares_outstanding"])
        .unique(subset=["date"], keep="last")
        .sort("date")
    )


def market_cap_parquet_path(root: str | Path, symbol: str) -> Path:
    return Path(root) / f"{_normalize_symbol(symbol)}.parquet"


def build_market_cap_frame(
    symbol: str,
    daily_prices: pl.DataFrame,
    shares: pl.DataFrame,
    *,
    provider: str = "yahoo",
) -> pl.DataFrame:
    clean_symbol = _normalize_symbol(symbol)
    if daily_prices.is_empty() or shares.is_empty():
        return pl.DataFrame(schema=MARKET_CAP_PARQUET_SCHEMA)
    prices = daily_prices.select(
        pl.col("date").cast(pl.Datetime, strict=False).dt.date().cast(pl.Datetime).alias("date"),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("currency").cast(pl.String).str.to_uppercase() if "currency" in daily_prices.columns else pl.lit("USD").alias("currency"),
    ).drop_nulls(["date", "close"]).sort("date")
    if "currency" in prices.columns:
        dropped_currencies = sorted(
            {
                currency
                for currency in prices.get_column("currency").fill_null("USD").to_list()
                if currency and currency != "USD"
            }
        )
        if dropped_currencies:
            logger.warning(
                "dropping non-USD daily prices for market-cap sync %s: %s",
                clean_symbol,
                ",".join(dropped_currencies),
            )
        prices = prices.filter(pl.col("currency").fill_null("USD") == "USD")
    if prices.is_empty():
        return pl.DataFrame(schema=MARKET_CAP_PARQUET_SCHEMA)
    share_frame = shares.select(
        pl.col("date").cast(pl.Datetime, strict=False).dt.date().cast(pl.Datetime).alias("date"),
        pl.col("shares_outstanding").cast(pl.Float64, strict=False),
    ).drop_nulls(["date", "shares_outstanding"]).sort("date")
    if share_frame.is_empty():
        return pl.DataFrame(schema=MARKET_CAP_PARQUET_SCHEMA)
    merged = (
        prices.select(["date", "close"])
        .join_asof(share_frame, on="date", strategy="backward")
        .with_columns((pl.col("close") * pl.col("shares_outstanding") / 1_000_000.0).alias("market_cap_usd_millions"))
        .drop_nulls(["market_cap_usd_millions"])
    )
    if merged.is_empty():
        return pl.DataFrame(schema=MARKET_CAP_PARQUET_SCHEMA)
    monthly = merged.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("_month")).group_by("_month", maintain_order=True).tail(1).sort("date")
    now = _now_naive_utc()
    return monthly.select(
        "date",
        pl.lit(clean_symbol).alias("symbol"),
        pl.col("market_cap_usd_millions"),
        pl.lit(provider).alias("provider"),
        pl.lit(clean_symbol).alias("source_symbol"),
        pl.lit(now).alias("ingested_at"),
    ).with_columns(
        [pl.col(column).cast(dtype, strict=False) for column, dtype in MARKET_CAP_PARQUET_SCHEMA.items()]
    ).select(list(MARKET_CAP_PARQUET_SCHEMA)).sort("date")


def sync_market_caps_yahoo(
    symbols: Iterable[str],
    *,
    daily_root: str | Path,
    market_cap_root: str | Path,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, object]:
    symbol_list = list(symbols)
    root = Path(market_cap_root)
    root.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    skipped: dict[str, str] = {}
    for raw_symbol in symbol_list:
        symbol = _normalize_symbol(raw_symbol)
        if not symbol:
            continue
        daily_prices = _read_daily_close(Path(daily_root), symbol)
        if start:
            daily_prices = daily_prices.filter(pl.col("date") >= datetime.fromisoformat(start).replace(hour=0, minute=0, second=0, microsecond=0))
        if end:
            daily_prices = daily_prices.filter(pl.col("date") <= datetime.fromisoformat(end).replace(hour=0, minute=0, second=0, microsecond=0))
        # Fetch the full shares history up to end so observations immediately
        # before the requested price window can be used without look-ahead.
        shares = _fetch_shares_full(symbol, start=None, end=end)
        frame = build_market_cap_frame(symbol, daily_prices, shares, provider="yahoo")
        if frame.is_empty():
            currency_series = _normalized_currency_series(daily_prices)
            currency_values = currency_series.to_list() if currency_series is not None else []
            non_usd_currencies = sorted({currency for currency in currency_values if currency and currency != "USD"})
            if non_usd_currencies and "USD" not in set(currency_values):
                skipped[symbol] = "non_usd_listing_currency:" + ",".join(non_usd_currencies)
                logger.warning(
                    "skipping market-cap sync for non-USD listing %s: %s",
                    symbol,
                    ",".join(non_usd_currencies),
                )
            else:
                skipped[symbol] = "no_usd_price_or_shares"
            continue
        validate_market_cap_frame(frame)
        frame.write_parquet(market_cap_parquet_path(root, symbol))
        written.append(symbol)
    return {
        "ok": True,
        "symbols_requested": len(symbol_list),
        "symbols_written": len(written),
        "written": written,
        "skipped": skipped,
        "root": str(root),
    }


def _fetch_sector(symbol: str) -> str | None:
    try:
        info = yf.Ticker(symbol).get_info()
    except Exception:
        return None
    if not isinstance(info, dict):
        return None
    sector = str(info.get("sector") or "").strip()
    if not sector:
        return None
    return YAHOO_SECTOR_TO_GICS.get(sector, sector)


def sync_sector_assignments_yahoo(
    symbols: Iterable[str],
    *,
    output_path: str | Path,
) -> dict[str, object]:
    symbol_list = list(symbols)
    rows: list[dict[str, object]] = []
    skipped: dict[str, str] = {}
    now = _now_naive_utc()
    observation_date = now.date()
    for raw_symbol in symbol_list:
        symbol = _normalize_symbol(raw_symbol)
        if not symbol:
            continue
        sector = _fetch_sector(symbol)
        if not sector:
            skipped[symbol] = "missing_sector"
            continue
        if sector not in GICS_SECTORS:
            skipped[symbol] = f"unsupported_sector:{sector}"
            continue
        rows.append(
            {
                "symbol": symbol,
                "sector": sector,
                "effective_start": observation_date,
                "effective_end": None,
                "source": "yahoo_current",
                "ingested_at": now,
            }
        )
    frame = pl.DataFrame(rows, schema_overrides=SECTOR_ASSIGNMENT_SCHEMA) if rows else pl.DataFrame(schema=SECTOR_ASSIGNMENT_SCHEMA)
    if not frame.is_empty():
        validate_sector_assignment_frame(frame)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(path)
    return {
        "ok": True,
        "symbols_requested": len(symbol_list),
        "symbols_written": frame.height,
        "skipped": skipped,
        "path": str(path),
    }


def _download_index_level(source_symbol: str, *, start: str | None = None, end: str | None = None) -> pl.DataFrame:
    data = yf.download(source_symbol, start=start, end=end, interval="1d", auto_adjust=False, progress=False, group_by="column", threads=False)
    if data is None or len(data) == 0:
        return pl.DataFrame(schema={"date": pl.Datetime, "adj_close": pl.Float64, "close": pl.Float64})
    return normalize_yf_df_to_polars(data)


def build_index_return_frame(
    index_id: str,
    price_frame: pl.DataFrame,
    *,
    source_symbol: str,
    provider: str = "yahoo",
    allow_price_fallback: bool = False,
) -> pl.DataFrame:
    clean_id = str(index_id or "").strip().upper()
    if clean_id not in SUPPORTED_INDEX_IDS:
        raise ValueError(f"Unsupported index_id: {index_id}")
    if price_frame.is_empty():
        return pl.DataFrame(schema=INDEX_RETURN_PARQUET_SCHEMA)
    level_column = "adj_close" if "adj_close" in price_frame.columns else ""
    if not level_column or price_frame.get_column(level_column).null_count() == price_frame.height:
        if not allow_price_fallback:
            raise ValueError(f"No total-return level available for {clean_id}")
        level_column = "close"
    frame = (
        price_frame.select(["date", level_column])
        .rename({level_column: "total_return_level"})
        .with_columns(
            pl.col("total_return_level").cast(pl.Float64, strict=False),
            pl.lit(clean_id).alias("index_id"),
            pl.lit(provider).alias("provider"),
            pl.lit(source_symbol).alias("source_symbol"),
            pl.lit(_now_naive_utc()).alias("ingested_at"),
        )
        .drop_nulls(["date", "total_return_level"])
        .sort("date")
        .with_columns(pl.col("total_return_level").pct_change().alias("return"))
        .select(list(INDEX_RETURN_PARQUET_SCHEMA))
    )
    return frame


def index_return_parquet_path(root: str | Path, index_id: str) -> Path:
    return Path(root) / f"{str(index_id).strip().upper()}.parquet"


def sync_index_returns_yahoo(
    index_ids: Iterable[str],
    *,
    root: str | Path,
    start: str | None = None,
    end: str | None = None,
    allow_price_fallback: bool = False,
) -> dict[str, object]:
    index_id_list = list(index_ids)
    out_root = Path(root)
    out_root.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    skipped: dict[str, str] = {}
    for raw_index_id in index_id_list:
        index_id = str(raw_index_id or "").strip().upper()
        source_symbol = INDEX_PROVIDER_SYMBOLS.get(index_id)
        if source_symbol is None:
            skipped[index_id] = "unsupported_index_id"
            continue
        try:
            frame = build_index_return_frame(
                index_id,
                _download_index_level(source_symbol, start=start, end=end),
                source_symbol=source_symbol,
                provider="yahoo",
                allow_price_fallback=allow_price_fallback,
            )
        except Exception as exc:
            skipped[index_id] = str(exc)
            continue
        if frame.is_empty():
            skipped[index_id] = "empty_download"
            continue
        validate_index_return_frame(frame)
        frame.write_parquet(index_return_parquet_path(out_root, index_id))
        written.append(index_id)
    return {
        "ok": True,
        "index_ids_requested": len(index_id_list),
        "index_ids_written": len(written),
        "written": written,
        "skipped": skipped,
        "root": str(out_root),
    }


def validate_market_cap_store(root: str | Path, symbols: Iterable[str] | None = None) -> dict[str, object]:
    root_path = Path(root)
    files = [market_cap_parquet_path(root_path, symbol) for symbol in symbols] if symbols is not None else sorted(root_path.glob("*.parquet"))
    errors: list[str] = []
    checked = 0
    for path in files:
        try:
            frame = pl.read_parquet(path)
            validate_market_cap_frame(frame)
            checked += 1
        except Exception as exc:
            errors.append(f"{path}: {exc}")
    return {"ok": not errors, "files_checked": checked, "errors": errors, "root": str(root_path)}


def validate_sector_assignment_file(path: str | Path) -> dict[str, object]:
    target = Path(path)
    try:
        frame = pl.read_csv(
            target,
            try_parse_dates=True,
            schema_overrides={"effective_start": pl.Date, "effective_end": pl.Date, "ingested_at": pl.Datetime},
        )
        validate_sector_assignment_frame(frame)
    except Exception as exc:
        return {"ok": False, "rows": 0, "errors": [str(exc)], "path": str(target)}
    return {"ok": True, "rows": frame.height, "errors": [], "path": str(target)}


def validate_index_return_store(root: str | Path, index_ids: Iterable[str] | None = None) -> dict[str, object]:
    root_path = Path(root)
    files = [index_return_parquet_path(root_path, index_id) for index_id in index_ids] if index_ids is not None else sorted(root_path.glob("*.parquet"))
    errors: list[str] = []
    checked = 0
    for path in files:
        try:
            frame = pl.read_parquet(path)
            validate_index_return_frame(frame)
            checked += 1
        except Exception as exc:
            errors.append(f"{path}: {exc}")
    return {"ok": not errors, "files_checked": checked, "errors": errors, "root": str(root_path)}


def sync_market_data_from_config(
    cfg: ConfigLike,
    *,
    symbols_override: list[str] | None = None,
    index_ids: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    include_market_caps: bool = True,
    include_sectors: bool = True,
    include_index_returns: bool = True,
    allow_price_fallback: bool = False,
) -> dict[str, object]:
    needs_symbols = include_market_caps or include_sectors
    symbols = _load_symbols_from_config(cfg, symbols_override) if needs_symbols else []
    result: dict[str, object] = {"ok": True}
    if include_market_caps:
        result["market_caps"] = sync_market_caps_yahoo(
            symbols,
            daily_root=parquet_root_path(cfg),
            market_cap_root=market_cap_root_path(cfg),
            start=start,
            end=end,
        )
    if include_sectors:
        result["sectors"] = sync_sector_assignments_yahoo(symbols, output_path=sector_assignments_path(cfg))
    if include_index_returns:
        result["index_returns"] = sync_index_returns_yahoo(
            index_ids or sorted(SUPPORTED_INDEX_IDS),
            root=index_returns_root_path(cfg),
            start=start,
            end=end,
            allow_price_fallback=allow_price_fallback,
        )
    return result


def validate_market_data_from_config(
    cfg: ConfigLike,
    *,
    symbols_override: list[str] | None = None,
    index_ids: list[str] | None = None,
) -> dict[str, object]:
    symbols = _load_symbols_from_config(cfg, symbols_override)
    market_caps = validate_market_cap_store(market_cap_root_path(cfg), symbols)
    sectors = validate_sector_assignment_file(sector_assignments_path(cfg))
    indexes = validate_index_return_store(index_returns_root_path(cfg), index_ids or sorted(SUPPORTED_INDEX_IDS))
    ok = bool(market_caps["ok"] and sectors["ok"] and indexes["ok"])
    return {"ok": ok, "market_caps": market_caps, "sectors": sectors, "index_returns": indexes}


def inspect_market_data_from_config(
    cfg: ConfigLike,
    *,
    symbols_override: list[str] | None = None,
    index_ids: list[str] | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for symbol in _load_symbols_from_config(cfg, symbols_override):
        path = market_cap_parquet_path(market_cap_root_path(cfg), symbol)
        if path.exists():
            frame = pl.read_parquet(path)
            rows.append({"artifact": "market_cap", "id": symbol, "exists": True, "rows": frame.height, "path": str(path)})
        else:
            rows.append({"artifact": "market_cap", "id": symbol, "exists": False, "rows": 0, "path": str(path)})
    sector_path = sector_assignments_path(cfg)
    sector_rows = pl.read_csv(sector_path).height if sector_path.exists() else 0
    rows.append({"artifact": "sector_assignments", "id": "sectors", "exists": sector_path.exists(), "rows": sector_rows, "path": str(sector_path)})
    for index_id in index_ids or sorted(SUPPORTED_INDEX_IDS):
        path = index_return_parquet_path(index_returns_root_path(cfg), index_id)
        if path.exists():
            frame = pl.read_parquet(path)
            rows.append({"artifact": "index_return", "id": str(index_id).upper(), "exists": True, "rows": frame.height, "path": str(path)})
        else:
            rows.append({"artifact": "index_return", "id": str(index_id).upper(), "exists": False, "rows": 0, "path": str(path)})
    return rows
