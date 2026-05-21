#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tradinglab_data.config import (  # noqa: E402
    Config,
    default_config_path,
    parquet_root_path,
    universe_dir_path,
    update_log_path,
)
from tradinglab_data.data_stooq import (  # noqa: E402
    StooqDownloadSpec,
    fetch_stooq_history,
    infer_currency_from_symbol,
)
from tradinglab_data.data_yf import (  # noqa: E402
    append_update_log,
    fetch_yfinance_history_bulk,
    read_parquet_if_exists,
)
from tradinglab_data.universe_listing import list_available_universes, render_available_universes  # noqa: E402


def _symbols_from_universe_csv(path: Path) -> list[str]:
    try:
        df = pl.read_csv(str(path))
    except Exception:
        return []
    if df.is_empty() or "symbol" not in df.columns:
        return []
    if "active" in df.columns:
        df = df.with_columns(pl.col("active").cast(pl.Int64, strict=False)).filter(pl.col("active") == 1)
    df = df.with_columns(pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"))
    df = df.filter((pl.col("symbol") != "") & (~pl.col("symbol").str.contains(r"[\$\s]")))
    return [str(s) for s in df.get_column("symbol").to_list()]


def _ensure_currency(df: pl.DataFrame, currency: str) -> pl.DataFrame:
    if "currency" in df.columns:
        return df.with_columns(
            pl.when(pl.col("currency").cast(pl.Utf8, strict=False).is_null() | (pl.col("currency").cast(pl.Utf8, strict=False) == ""))
            .then(pl.lit(currency))
            .otherwise(pl.col("currency").cast(pl.Utf8, strict=False))
            .alias("currency")
        )
    return df.with_columns(pl.lit(currency).alias("currency"))


def _align_for_concat(df_left: pl.DataFrame, df_right: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    preferred = ["date", "open", "high", "low", "close", "adj_close", "volume", "currency"]
    cols = list(dict.fromkeys(preferred + sorted(set(df_left.columns) | set(df_right.columns))))
    l_missing = [c for c in cols if c not in df_left.columns]
    r_missing = [c for c in cols if c not in df_right.columns]
    if l_missing:
        df_left = df_left.with_columns([pl.lit(None).alias(c) for c in l_missing])
    if r_missing:
        df_right = df_right.with_columns([pl.lit(None).alias(c) for c in r_missing])
    dtype_map: dict[str, pl.DataType] = {
        "date": pl.Datetime(),
        "open": pl.Float64(),
        "high": pl.Float64(),
        "low": pl.Float64(),
        "close": pl.Float64(),
        "adj_close": pl.Float64(),
        "volume": pl.Float64(),
        "currency": pl.Utf8(),
    }
    casts_left = []
    casts_right = []
    for col in cols:
        dtype = dtype_map.get(col)
        if dtype is not None:
            casts_left.append(pl.col(col).cast(dtype, strict=False).alias(col))
            casts_right.append(pl.col(col).cast(dtype, strict=False).alias(col))
    if casts_left:
        df_left = df_left.with_columns(casts_left)
    if casts_right:
        df_right = df_right.with_columns(casts_right)
    return df_left.select(cols), df_right.select(cols)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Bootstrap full daily OHLC parquet history from Stooq for symbols in universe CSVs."
    )
    ap.add_argument("--config", default=str(default_config_path()), help="Path to configuration YAML")
    ap.add_argument("--universe-dir", type=Path, default=None, help="Folder with universe CSV files (defaults to paths.universe_dir)")
    ap.add_argument("--parquet-root", type=Path, default=None, help="Output parquet folder (defaults to paths.parquet_root)")
    ap.add_argument("--recent-yf-days", type=int, default=5, help="After Stooq load, merge this many recent days from yfinance")
    ap.add_argument("--skip-yf-recent", action="store_true", help="Skip yfinance recent-day merge")
    ap.add_argument("--refresh-existing", action="store_true", help="Refresh symbols even if parquet already exists")
    ap.add_argument("--only-universes", nargs="*", default=None, help="Universe basenames to include, e.g. sp500 djia atx")
    ap.add_argument("--list-universes", action="store_true", help="List available universes and exit.")
    ap.add_argument("--log-path", type=Path, default=None, help="Update log CSV (defaults to paths.update_log_csv)")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    if bool(args.list_universes):
        print(render_available_universes(list_available_universes(cfg)), end="")
        return
    universe_dir = args.universe_dir or universe_dir_path(cfg)
    parquet_root = args.parquet_root or parquet_root_path(cfg)
    log_path = args.log_path or update_log_path(cfg)
    parquet_root.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(universe_dir.glob("*.csv"))
    if args.only_universes:
        wanted = {item.strip().lower() for item in args.only_universes if item.strip()}
        csv_files = [path for path in csv_files if path.stem.lower() in wanted]

    if not csv_files:
        raise SystemExit(f"No universe CSVs found in {universe_dir}")

    symbols_by_universe: dict[str, list[str]] = {}
    all_symbols: list[str] = []
    for path in csv_files:
        symbols = _symbols_from_universe_csv(path)
        symbols_by_universe[path.stem] = symbols
        all_symbols.extend(symbols)

    dedup_symbols = list(dict.fromkeys(all_symbols))
    print(f"Universes: {len(symbols_by_universe)}")
    for name, symbols in symbols_by_universe.items():
        print(f"  {name}: {len(symbols)} symbols")
    print(f"Unique symbols: {len(dedup_symbols)}")

    written = 0
    skipped = 0
    failed = 0

    for symbol in tqdm(dedup_symbols, desc="Stooq history"):
        out_path = parquet_root / f"{symbol}.parquet"
        if out_path.exists() and not args.refresh_existing:
            skipped += 1
            continue
        try:
            df_hist = fetch_stooq_history(StooqDownloadSpec(symbol=symbol))
            if df_hist.is_empty():
                append_update_log(log_path, symbol, "stooq_empty_data", 1)
                failed += 1
                continue
            currency = infer_currency_from_symbol(symbol)
            df_hist = _ensure_currency(df_hist, currency)
            df_hist.write_parquet(str(out_path))
            written += 1
        except Exception as exc:
            append_update_log(log_path, symbol, f"stooq_error:{exc}", 1)
            failed += 1

    if not args.skip_yf_recent and args.recent_yf_days > 0 and dedup_symbols:
        inc_map = fetch_yfinance_history_bulk(
            dedup_symbols,
            interval="1d",
            lookback_days=int(args.recent_yf_days),
            chunk_size=100,
            sleep_seconds=1.0,
            max_retries=5,
            backoff_max_seconds=120.0,
            threads=False,
            log_path=log_path,
        )
        merged = 0
        for symbol in tqdm(dedup_symbols, desc="YF recent merge"):
            path = parquet_root / f"{symbol}.parquet"
            try:
                df_old = read_parquet_if_exists(path)
                if df_old is None or df_old.is_empty():
                    continue
                df_inc = inc_map.get(symbol)
                if df_inc is None or df_inc.is_empty():
                    continue
                currency = infer_currency_from_symbol(symbol)
                df_old = _ensure_currency(df_old, currency)
                df_inc = _ensure_currency(df_inc, currency)
                df_old, df_inc = _align_for_concat(df_old, df_inc)
                combined = (
                    pl.concat([df_old, df_inc], how="vertical")
                    .unique(subset=["date"], keep="last")
                    .sort("date")
                )
                combined.write_parquet(str(path))
                merged += 1
            except Exception as exc:
                append_update_log(log_path, symbol, f"stooq_yf_recent_error:{exc}", 1)
        print(f"YF recent merged: {merged}")

    print("Summary")
    print(f"  written: {written}")
    print(f"  skipped_existing: {skipped}")
    print(f"  failed: {failed}")


if __name__ == "__main__":
    main()
