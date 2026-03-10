#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import math
import random
import contextlib
import io
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from tqdm import tqdm
from tradinglab_data.config import Config, default_config_path, parquet_root_path, universe_csv_path, universe_dir_path, update_log_path
from tradinglab_data.data_yf import (
    fetch_yfinance_history_bulk,
    fetch_yfinance_history,
    YFDownloadSpec,
    read_parquet_if_exists,
    append_update_log,
    fetch_symbol_currency,
)
from tradinglab_data.extended_hours_monitor import (
    MAX_PERIOD_BY_INTERVAL,
    UPDATE_PERIOD_BY_INTERVAL,
    _sanitize_intraday_df,
    fetch_extended_intraday,
)
from tradinglab_data.parquet_verify import (
    ParquetVerifyConfig,
    run_parquet_sanity_checks,
    write_verification_summary,
)
from tradinglab_data.universe import load_universe_frame


REQUIRED_COLS = ["date", "open", "high", "low", "close"]
DEFAULT_ROOT = Path()
DEFAULT_UNIVERSE = Path()
DEFAULT_UNIVERSE_DIR = Path()
INTRADAY_VERIFY_PERIOD = {
    "1m": "2d",
    "5m": "10d",
}


@dataclass
class FileStatus:
    symbol: str
    path: Path
    exists: bool
    readable: bool
    rows: int
    start_date: str
    end_date: str
    last_open: float | None
    last_high: float | None
    last_low: float | None
    last_close: float | None
    period_label: str
    period_rows: int
    period_start_date: str
    period_end_date: str
    period_high: float | None
    period_low: float | None
    required_cols_ok: bool
    missing_cols: list[str]
    duplicate_dates: int
    null_ohlc_rows: int
    bad_ohlc_rows: int
    large_gap_count: int
    extreme_move_count: int
    sorted_dates: bool
    valid: bool
    error: str


@dataclass
class SymbolMeta:
    name: str
    isin: str
    exchange: str
    country: str
    source: str


@dataclass
class YFLatest:
    ok: bool
    date: str
    close: float | None
    currency: str
    error: str


@dataclass
class YFSampleAudit:
    ok: bool
    checked_days: int
    mismatch_days: int
    first_mismatch_dates: list[str]
    max_abs_ohlc_diff: float
    error: str


def _fmt_price(value: float | None) -> str:
    if value is None:
        return "-"
    if math.isnan(value):
        return "nan"
    return f"{value:.4f}"


def _parse_date_ymd(value: str) -> datetime.date | None:
    try:
        return datetime.fromisoformat(str(value).split(" ")[0]).date()
    except Exception:
        return None


def _period_filter_expr(period_year: int | None, period_month: str | None) -> pl.Expr | None:
    if period_month:
        dt = datetime.strptime(period_month, "%Y-%m")
        return (pl.col("date").dt.year() == dt.year) & (pl.col("date").dt.month() == dt.month)
    if period_year is not None:
        return pl.col("date").dt.year() == period_year
    return None


def _period_label(period_year: int | None, period_month: str | None) -> str:
    if period_month:
        return f"month:{period_month}"
    if period_year is not None:
        return f"year:{period_year}"
    return "all"


def _infer_parquet_mode(root: Path, requested_mode: str) -> tuple[str, int | None]:
    if requested_mode in {"daily", "intraday"}:
        mode = requested_mode
    else:
        parts = [p.lower() for p in root.parts]
        mode = "intraday" if ("intraday" in parts or root.name.lower() in {"1m", "5m"}) else "daily"
    interval_minutes = None
    if mode == "intraday":
        name = root.name.strip().lower()
        if name.endswith("m"):
            try:
                interval_minutes = int(name[:-1])
            except Exception:
                interval_minutes = None
    return mode, interval_minutes


def _count_intraday_large_gaps(date_values: list[object], interval_minutes: int | None) -> int:
    if not date_values or interval_minutes is None or interval_minutes <= 0:
        return 0
    tz = ZoneInfo("America/New_York")
    threshold = max(interval_minutes * 3, 30)
    out = 0
    prev_local = None
    for raw in date_values:
        if raw is None:
            continue
        try:
            dt = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
        except Exception:
            prev_local = None
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        local = dt.astimezone(tz)
        if prev_local is not None and prev_local.date() == local.date():
            delta_min = (local - prev_local).total_seconds() / 60.0
            if delta_min > threshold:
                out += 1
        prev_local = local
    return out


def _validate_file(
    path: Path,
    symbol: str,
    period_year: int | None,
    period_month: str | None,
    mode: str = "daily",
    intraday_interval_minutes: int | None = None,
) -> FileStatus:
    period = _period_label(period_year, period_month)
    if not path.exists():
        return FileStatus(
            symbol=symbol,
            path=path,
            exists=False,
            readable=False,
            rows=0,
            start_date="-",
            end_date="-",
            last_open=None,
            last_high=None,
            last_low=None,
            last_close=None,
            period_label=period,
            period_rows=0,
            period_start_date="-",
            period_end_date="-",
            period_high=None,
            period_low=None,
            required_cols_ok=False,
            missing_cols=REQUIRED_COLS[:],
            duplicate_dates=0,
            null_ohlc_rows=0,
            bad_ohlc_rows=0,
            large_gap_count=0,
            extreme_move_count=0,
            sorted_dates=False,
            valid=False,
            error="missing file",
        )

    try:
        df = pl.read_parquet(str(path))
    except Exception as e:
        return FileStatus(
            symbol=symbol,
            path=path,
            exists=True,
            readable=False,
            rows=0,
            start_date="-",
            end_date="-",
            last_open=None,
            last_high=None,
            last_low=None,
            last_close=None,
            period_label=period,
            period_rows=0,
            period_start_date="-",
            period_end_date="-",
            period_high=None,
            period_low=None,
            required_cols_ok=False,
            missing_cols=REQUIRED_COLS[:],
            duplicate_dates=0,
            null_ohlc_rows=0,
            bad_ohlc_rows=0,
            large_gap_count=0,
            extreme_move_count=0,
            sorted_dates=False,
            valid=False,
            error=str(e),
        )

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        return FileStatus(
            symbol=symbol,
            path=path,
            exists=True,
            readable=True,
            rows=df.height,
            start_date="-",
            end_date="-",
            last_open=None,
            last_high=None,
            last_low=None,
            last_close=None,
            period_label=period,
            period_rows=0,
            period_start_date="-",
            period_end_date="-",
            period_high=None,
            period_low=None,
            required_cols_ok=False,
            missing_cols=missing,
            duplicate_dates=0,
            null_ohlc_rows=0,
            bad_ohlc_rows=0,
            large_gap_count=0,
            extreme_move_count=0,
            sorted_dates=False,
            valid=False,
            error=f"missing columns: {', '.join(missing)}",
        )

    if df.is_empty():
        return FileStatus(
            symbol=symbol,
            path=path,
            exists=True,
            readable=True,
            rows=0,
            start_date="-",
            end_date="-",
            last_open=None,
            last_high=None,
            last_low=None,
            last_close=None,
            period_label=period,
            period_rows=0,
            period_start_date="-",
            period_end_date="-",
            period_high=None,
            period_low=None,
            required_cols_ok=True,
            missing_cols=[],
            duplicate_dates=0,
            null_ohlc_rows=0,
            bad_ohlc_rows=0,
            large_gap_count=0,
            extreme_move_count=0,
            sorted_dates=True,
            valid=False,
            error="empty file",
        )

    # Cast date for robust sort/summary.
    work = df.with_columns(pl.col("date").cast(pl.Datetime, strict=False).alias("date"))

    with_checks = (
        work.sort("date")
        .with_columns(
            pl.col("date").diff().dt.total_days().fill_null(0).alias("_gap_days"),
            pl.col("close").shift(1).alias("_prev_close"),
        )
        .with_columns(
            pl.when(pl.col("_prev_close").is_null() | (pl.col("_prev_close") == 0))
            .then(None)
            .otherwise((pl.col("close") / pl.col("_prev_close")) - 1.0)
            .alias("_ret")
        )
    )

    summary = with_checks.select(
        pl.len().alias("rows"),
        pl.col("date").min().alias("start_date"),
        pl.col("date").max().alias("end_date"),
        (pl.len() - pl.col("date").n_unique()).alias("duplicate_dates"),
        pl.any_horizontal(
            [pl.col("open").is_null(), pl.col("high").is_null(), pl.col("low").is_null(), pl.col("close").is_null()]
        ).sum().alias("null_ohlc_rows"),
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
        ).sum().alias("bad_ohlc_rows"),
        (pl.col("_ret").abs() > 0.4).sum().alias("extreme_move_count"),
    ).row(0, named=True)

    ordered = with_checks
    is_sorted = ordered.select(pl.col("date").eq_missing(pl.col("date").sort()).all()).item()

    last = ordered.select(["open", "high", "low", "close"]).tail(1).row(0, named=True)

    period_expr = _period_filter_expr(period_year, period_month)
    period_df = ordered.filter(period_expr) if period_expr is not None else ordered
    if period_df.is_empty():
        period_rows = 0
        period_start_date = "-"
        period_end_date = "-"
        period_high = None
        period_low = None
    else:
        p = period_df.select(
            pl.len().alias("rows"),
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
        ).row(0, named=True)
        period_rows = int(p["rows"])
        period_start_date = str(p["start_date"])
        period_end_date = str(p["end_date"])
        period_high = float(p["high"]) if p["high"] is not None else None
        period_low = float(p["low"]) if p["low"] is not None else None

    duplicate_dates = int(summary["duplicate_dates"])
    null_ohlc_rows = int(summary["null_ohlc_rows"])
    bad_ohlc_rows = int(summary["bad_ohlc_rows"])
    if mode == "intraday":
        large_gap_count = _count_intraday_large_gaps(
            ordered.select("date").get_column("date").to_list(),
            interval_minutes=intraday_interval_minutes,
        )
    else:
        large_gap_count = int(ordered.select((pl.col("_gap_days") > 7).sum()).item())
    extreme_move_count = int(summary["extreme_move_count"])

    valid = (
        duplicate_dates == 0
        and null_ohlc_rows == 0
        and bad_ohlc_rows == 0
        and large_gap_count == 0
        and bool(is_sorted)
    )

    return FileStatus(
        symbol=symbol,
        path=path,
        exists=True,
        readable=True,
        rows=int(summary["rows"]),
        start_date=str(summary["start_date"]),
        end_date=str(summary["end_date"]),
        last_open=float(last["open"]) if last["open"] is not None else None,
        last_high=float(last["high"]) if last["high"] is not None else None,
        last_low=float(last["low"]) if last["low"] is not None else None,
        last_close=float(last["close"]) if last["close"] is not None else None,
        period_label=period,
        period_rows=period_rows,
        period_start_date=period_start_date,
        period_end_date=period_end_date,
        period_high=period_high,
        period_low=period_low,
        required_cols_ok=True,
        missing_cols=[],
        duplicate_dates=duplicate_dates,
        null_ohlc_rows=null_ohlc_rows,
        bad_ohlc_rows=bad_ohlc_rows,
        large_gap_count=large_gap_count,
        extreme_move_count=extreme_move_count,
        sorted_dates=bool(is_sorted),
        valid=valid,
        error="",
    )


def _collect_targets(
    root: Path,
    symbols: list[str],
    paths: list[str],
    universe_csv: Path,
    ignore_orphans: bool = True,
) -> list[tuple[str, Path]]:
    targets: list[tuple[str, Path]] = []

    if paths:
        for p in paths:
            path = Path(p)
            symbol = path.stem
            targets.append((symbol, path))
        return targets

    if symbols:
        for sym in symbols:
            targets.append((sym, root / f"{sym}.parquet"))
        return targets

    active_symbols: set[str] | None = None
    if ignore_orphans:
        try:
            udf = load_universe_frame(universe_csv)
            active_symbols = {str(s).strip().upper() for s in udf.get_column("symbol").to_list()}
        except Exception:
            active_symbols = None

    for p in sorted(root.glob("*.parquet")):
        if active_symbols is not None and p.stem.strip().upper() not in active_symbols:
            continue
        targets.append((p.stem, p))
    return targets


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _parse_index_from_source(source: str) -> str | None:
    s = source.strip().lower()
    if "etf" in s:
        return "etf"
    for idx in ("sp500", "djia", "dax", "mdax", "atx"):
        if s == idx or s.startswith(f"{idx}_"):
            return idx
    return None


def _load_symbol_index_map(universe_csv: Path, universe_dir: Path) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}

    if universe_csv.exists():
        try:
            df = pl.read_csv(str(universe_csv))
            if not df.is_empty() and "symbol" in df.columns:
                cols = set(df.columns)
                for row in df.iter_rows(named=True):
                    sym = _norm(str(row.get("symbol") or "")).upper()
                    if not sym:
                        continue
                    idxs: set[str] = set()
                    if "index_memberships" in cols:
                        raw = _norm(str(row.get("index_memberships") or ""))
                        if raw:
                            for x in raw.split(","):
                                t = x.strip().lower()
                                if t:
                                    idxs.add(t)
                    if not idxs and "source" in cols:
                        src = _norm(str(row.get("source") or ""))
                        idx = _parse_index_from_source(src)
                        if idx:
                            idxs.add(idx)
                    if idxs:
                        out.setdefault(sym, set()).update(idxs)
        except Exception:
            pass

    if not out and universe_dir.exists():
        for p in sorted(universe_dir.glob("*.csv")):
            idx = p.stem.strip().lower()
            try:
                df = pl.read_csv(str(p))
                if "symbol" not in df.columns:
                    continue
                for sym in df.get_column("symbol").cast(pl.Utf8, strict=False).to_list():
                    s = _norm(str(sym)).upper()
                    if s:
                        out.setdefault(s, set()).add(idx)
            except Exception:
                continue

    return out


def _load_symbol_meta(universe_csv: Path, universe_dir: Path) -> dict[str, SymbolMeta]:
    frames: list[pl.DataFrame] = []
    if universe_csv.exists():
        try:
            df = pl.read_csv(str(universe_csv))
            if not df.is_empty():
                frames.append(df)
        except Exception:
            pass

    if not frames and universe_dir.exists():
        for p in sorted(universe_dir.glob("*.csv")):
            try:
                df = pl.read_csv(str(p))
                if not df.is_empty() and "symbol" in df.columns:
                    frames.append(df)
            except Exception:
                continue

    if not frames:
        return {}

    meta: dict[str, SymbolMeta] = {}
    for df in frames:
        cols = set(df.columns)
        if "symbol" not in cols:
            continue
        for row in df.iter_rows(named=True):
            symbol = _norm(str(row.get("symbol") or "")).upper()
            if not symbol:
                continue
            if symbol in meta:
                continue
            meta[symbol] = SymbolMeta(
                name=_norm(str(row.get("name") or "")),
                isin=_norm(str(row.get("isin") or "")),
                exchange=_norm(str(row.get("exchange") or "")),
                country=_norm(str(row.get("country") or "")),
                source=_norm(str(row.get("source") or "")),
            )
    return meta


def _possible_venue_mismatch(symbol: str, meta: SymbolMeta | None) -> str:
    if meta is None:
        return "unknown_meta"

    sym = symbol.upper()
    ex = meta.exchange.upper()
    country = meta.country.upper()

    expected_vi = ("VIENNA" in ex) or (country in {"AT", "AUSTRIA"})
    has_vi = sym.endswith(".VI")

    if expected_vi and not has_vi:
        return "expected_.VI_suffix"
    if has_vi and not expected_vi and (ex or country):
        return "symbol_.VI_but_meta_not_vienna"
    return ""


def _fetch_yf_latest(symbol: str, mode: str = "daily", intraday_interval: str = "5m") -> YFLatest:
    try:
        import contextlib
        import io
        import yfinance as yf
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            if mode == "intraday":
                df = yf.download(
                    symbol,
                    period=INTRADAY_VERIFY_PERIOD.get(intraday_interval, "2d"),
                    interval=intraday_interval,
                    auto_adjust=False,
                    prepost=True,
                    progress=False,
                    group_by="column",
                    threads=False,
                )
            else:
                df = yf.download(
                    symbol,
                    period="10d",
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    group_by="column",
                    threads=False,
                )
        if df is None or len(df) == 0:
            return YFLatest(ok=False, date="-", close=None, currency="", error="no_data")

        try:
            import pandas as pd
            if isinstance(df.columns, pd.MultiIndex):
                df = df.copy()
                df.columns = [c[0] for c in df.columns]
        except Exception:
            pass

        tail = df.tail(1)
        if tail is None or len(tail) == 0:
            return YFLatest(ok=False, date="-", close=None, currency="", error="no_tail")

        idx = tail.index[-1]
        date_s = str(getattr(idx, "to_pydatetime", lambda: idx)())
        close_v = tail["Close"].iloc[-1] if "Close" in tail.columns else None
        close_f = float(close_v) if close_v is not None else None

        currency = ""
        try:
            fi = getattr(yf.Ticker(symbol), "fast_info", None)
            if fi is not None:
                currency = str(fi.get("currency") or "").strip().upper()
        except Exception:
            currency = ""

        return YFLatest(ok=True, date=date_s, close=close_f, currency=currency, error="")
    except Exception as e:
        return YFLatest(ok=False, date="-", close=None, currency="", error=str(e))


def _sample_yf_consistency(
    status: FileStatus,
    sample_days: int,
    atol: float,
    sample_recent_days: int = 0,
    mode: str = "daily",
    intraday_interval: str = "5m",
) -> YFSampleAudit:
    if sample_days <= 0:
        return YFSampleAudit(ok=True, checked_days=0, mismatch_days=0, first_mismatch_dates=[], max_abs_ohlc_diff=0.0, error="")
    try:
        if mode == "intraday":
            import yfinance as yf
            period = INTRADAY_VERIFY_PERIOD.get(intraday_interval, "2d")
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                y_pd = yf.download(
                    status.symbol,
                    period=period,
                    interval=intraday_interval,
                    auto_adjust=False,
                    prepost=True,
                    progress=False,
                    group_by="column",
                    threads=False,
                )
            if y_pd is None or len(y_pd) == 0:
                y = pl.DataFrame()
            else:
                from tradinglab_data.data_yf import _normalize_yf_df_to_polars
                y = _normalize_yf_df_to_polars(y_pd)
        else:
            try:
                s = datetime.fromisoformat(status.start_date.split(" ")[0])
                e = datetime.fromisoformat(status.end_date.split(" ")[0])
                span_days = max(365, (e - s).days + 30)
            except Exception:
                span_days = 5000

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                yf_map = fetch_yfinance_history_bulk(
                    [status.symbol],
                    interval="1d",
                    lookback_days=span_days,
                    chunk_size=1,
                    sleep_seconds=0.0,
                    max_retries=3,
                    backoff_max_seconds=30.0,
                    threads=False,
                    log_path=None,
                )
            y = yf_map.get(status.symbol)
            if y is None or y.is_empty():
                # Fallback: direct single-symbol path
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    y = fetch_yfinance_history(
                        YFDownloadSpec(symbol=status.symbol, interval="1d", lookback_days=span_days)
                    )
        if y is None or y.is_empty():
            return YFSampleAudit(
                ok=False,
                checked_days=0,
                mismatch_days=0,
                first_mismatch_dates=[],
                max_abs_ohlc_diff=0.0,
                error="no_yf_history",
            )
        if any(c not in y.columns for c in ["date", "open", "high", "low", "close"]):
            return YFSampleAudit(ok=False, checked_days=0, mismatch_days=0, first_mismatch_dates=[], max_abs_ohlc_diff=0.0, error="yf_missing_ohlc_cols")

        if mode == "intraday":
            p = pl.read_parquet(str(status.path)).select(["date", "open", "high", "low", "close"]).with_columns(
                pl.col("date").cast(pl.Datetime, strict=False).alias("date")
            )
            y = y.select(["date", "open", "high", "low", "close"]).rename(
                {"open": "yf_open", "high": "yf_high", "low": "yf_low", "close": "yf_close"}
            ).with_columns(
                pl.col("date").cast(pl.Datetime, strict=False).alias("date")
            )
        else:
            p = pl.read_parquet(str(status.path)).select(["date", "open", "high", "low", "close"]).with_columns(
                pl.col("date").cast(pl.Datetime, strict=False).dt.date().alias("date")
            )
            y = y.select(["date", "open", "high", "low", "close"]).rename(
                {"open": "yf_open", "high": "yf_high", "low": "yf_low", "close": "yf_close"}
            ).with_columns(
                pl.col("date").cast(pl.Datetime, strict=False).dt.date().alias("date")
            )

        j = p.join(y, on="date", how="inner").sort("date")
        if int(sample_recent_days) > 0 and not j.is_empty():
            max_d = j.select(pl.col("date").max()).item()
            if max_d is not None:
                cutoff = max_d - timedelta(days=int(sample_recent_days))
                j = j.filter(pl.col("date") >= pl.lit(cutoff))
        if j.is_empty():
            reason = "no_recent_overlap_dates" if int(sample_recent_days) > 0 else "no_overlap_dates"
            return YFSampleAudit(ok=False, checked_days=0, mismatch_days=0, first_mismatch_dates=[], max_abs_ohlc_diff=0.0, error=reason)

        n = min(sample_days, j.height)
        seed = int(hashlib.md5(status.symbol.encode("utf-8")).hexdigest()[:8], 16)
        idxs = sorted(random.Random(seed).sample(range(j.height), n))
        s = (
            j.with_row_index("__row_idx")
            .filter(pl.col("__row_idx").is_in(idxs))
            .drop("__row_idx")
            .sort("date")
        )

        diff = s.with_columns(
            (pl.col("open") - pl.col("yf_open")).abs().alias("d_open"),
            (pl.col("high") - pl.col("yf_high")).abs().alias("d_high"),
            (pl.col("low") - pl.col("yf_low")).abs().alias("d_low"),
            (pl.col("close") - pl.col("yf_close")).abs().alias("d_close"),
        ).with_columns(
            pl.max_horizontal(["d_open", "d_high", "d_low", "d_close"]).alias("d_max")
        )

        mism = diff.filter(pl.col("d_max") > float(atol))
        mismatch_days = mism.height
        first_dates = [str(v) for v in mism.get_column("date").head(10).to_list()] if mismatch_days else []
        max_abs = float(diff.select(pl.col("d_max").max()).item() or 0.0)
        return YFSampleAudit(
            ok=(mismatch_days == 0),
            checked_days=n,
            mismatch_days=mismatch_days,
            first_mismatch_dates=first_dates,
            max_abs_ohlc_diff=max_abs,
            error="",
        )
    except Exception as e:
        return YFSampleAudit(ok=False, checked_days=0, mismatch_days=0, first_mismatch_dates=[], max_abs_ohlc_diff=0.0, error=str(e))


def _infer_mismatch_cause(
    status: FileStatus,
    venue_mismatch: str,
    parquet_vs_yf: str,
    yf_error: str,
    yf_close_diff_pct: str,
    sample_audit: YFSampleAudit | None,
    provider_baseline: str = "yf",
) -> str:
    if venue_mismatch:
        return "symbol_or_venue_mapping_mismatch"
    if sample_audit is not None:
        if sample_audit.error:
            return "historical_yf_sampling_failed"
        if sample_audit.mismatch_days > 0:
            if provider_baseline in {"stooq", "mixed"}:
                return "historical_provider_difference_expected_stooq_vs_yf"
            return "historical_ohlc_mismatch_detected_against_yfinance"
    if parquet_vs_yf == "yf_error":
        return "yfinance_fetch_failure_or_symbol_unavailable"
    if parquet_vs_yf == "date_mismatch":
        return "stale_parquet_or_incremental_update_failure"
    if parquet_vs_yf == "price_mismatch":
        try:
            diff = abs(float(yf_close_diff_pct))
        except Exception:
            diff = 0.0
        if diff > 10.0:
            return "large_price_gap_possible_wrong_ticker_or_split_event"
        return "close_price_revision_or_data_source_difference"
    if status.large_gap_count > 0:
        return "historical_gap_detected_in_parquet"
    if yf_error:
        return "yfinance_error"
    return ""


def _repair_symbol_from_yf(
    status: FileStatus,
    lookback_days: int,
    log_path: Path,
) -> str:
    symbol = status.symbol
    try:
        data = fetch_yfinance_history_bulk(
            [symbol],
            interval="1d",
            lookback_days=lookback_days,
            chunk_size=1,
            sleep_seconds=0.0,
            max_retries=3,
            backoff_max_seconds=30.0,
            threads=False,
            log_path=log_path,
        )
        df_new = data.get(symbol)
        if df_new is None or df_new.is_empty():
            append_update_log(log_path, symbol, "repair_no_data", 1)
            return "repair_no_data"

        df_old = read_parquet_if_exists(status.path)
        if df_old is None or df_old.is_empty():
            df_new.write_parquet(str(status.path))
            return "repaired_replace"

        combined = (
            pl.concat([df_old, df_new], how="diagonal")
            .unique(subset=["date"], keep="last")
            .sort("date")
        )
        combined.write_parquet(str(status.path))
        return "repaired_upsert"
    except Exception as e:
        append_update_log(log_path, symbol, f"repair_error:{e}", 1)
        return f"repair_error:{e}"


def _repair_intraday_symbol_from_yf(
    status: FileStatus,
    interval: str,
    log_path: Path,
) -> str:
    symbol = status.symbol
    try:
        data = fetch_extended_intraday(
            [symbol],
            interval=interval,
            period=UPDATE_PERIOD_BY_INTERVAL.get(interval, "2d"),
            prepost=True,
            chunk_size=1,
            sleep_seconds=0.0,
            max_retries=3,
            backoff_max_seconds=30.0,
            threads=False,
            log_path=log_path,
        )
        df_new = data.get(symbol)
        if df_new is None or df_new.is_empty():
            append_update_log(log_path, symbol, f"repair_intraday_{interval}_no_data", 1)
            return "repair_no_data"

        cur = fetch_symbol_currency(symbol) or "UNKNOWN"
        if "currency" in df_new.columns:
            df_new = df_new.with_columns(
                pl.when(pl.col("currency").cast(pl.Utf8, strict=False).is_null() | (pl.col("currency").cast(pl.Utf8, strict=False) == ""))
                .then(pl.lit(cur))
                .otherwise(pl.col("currency").cast(pl.Utf8, strict=False))
                .alias("currency")
            )
        else:
            df_new = df_new.with_columns(pl.lit(cur).alias("currency"))

        cols = ["date", "open", "high", "low", "close", "adj_close", "volume", "currency"]
        df_new = df_new.select([c for c in cols if c in df_new.columns])
        df_old = read_parquet_if_exists(status.path)
        if df_old is None or df_old.is_empty():
            df_new.sort("date").write_parquet(str(status.path))
            return "repaired_replace"

        if "currency" not in df_old.columns:
            df_old = df_old.with_columns(pl.lit(cur).alias("currency"))
        combined = (
            pl.concat([df_old.select([c for c in cols if c in df_old.columns]), df_new], how="diagonal")
            .unique(subset=["date"], keep="last")
            .sort("date")
        )
        combined.write_parquet(str(status.path))
        return "repaired_upsert"
    except Exception as e:
        append_update_log(log_path, symbol, f"repair_intraday_{interval}_error:{e}", 1)
        return f"repair_error:{e}"


def _repair_symbol_full_history_from_yf(status: FileStatus, log_path: Path) -> str:
    symbol = status.symbol
    try:
        try:
            s = datetime.fromisoformat(status.start_date.split(" ")[0])
            e = datetime.fromisoformat(status.end_date.split(" ")[0])
            span_days = max(365, (e - s).days + 30)
        except Exception:
            span_days = 5000
        lookback_days = max(5000, span_days)
        data = fetch_yfinance_history_bulk(
            [symbol],
            interval="1d",
            lookback_days=lookback_days,
            chunk_size=1,
            sleep_seconds=0.0,
            max_retries=3,
            backoff_max_seconds=30.0,
            threads=False,
            log_path=log_path,
        )
        df_new = data.get(symbol)
        if df_new is None or df_new.is_empty():
            append_update_log(log_path, symbol, "repair_full_no_data", 1)
            return "repair_full_no_data"
        df_new.sort("date").write_parquet(str(status.path))
        return "repaired_full_replace"
    except Exception as e:
        append_update_log(log_path, symbol, f"repair_full_error:{e}", 1)
        return f"repair_full_error:{e}"


def _repair_intraday_symbol_full_history_from_yf(
    status: FileStatus,
    interval: str,
    log_path: Path,
) -> str:
    symbol = status.symbol
    try:
        data = fetch_extended_intraday(
            [symbol],
            interval=interval,
            period=MAX_PERIOD_BY_INTERVAL.get(interval, "7d"),
            prepost=True,
            chunk_size=1,
            sleep_seconds=0.0,
            max_retries=3,
            backoff_max_seconds=30.0,
            threads=False,
            log_path=log_path,
        )
        df_new = data.get(symbol)
        if df_new is None or df_new.is_empty():
            append_update_log(log_path, symbol, f"repair_full_intraday_{interval}_no_data", 1)
            return "repair_full_no_data"
        cur = fetch_symbol_currency(symbol) or "UNKNOWN"
        if "currency" in df_new.columns:
            df_new = df_new.with_columns(
                pl.when(pl.col("currency").cast(pl.Utf8, strict=False).is_null() | (pl.col("currency").cast(pl.Utf8, strict=False) == ""))
                .then(pl.lit(cur))
                .otherwise(pl.col("currency").cast(pl.Utf8, strict=False))
                .alias("currency")
            )
        else:
            df_new = df_new.with_columns(pl.lit(cur).alias("currency"))
        df_new.select(["date", "open", "high", "low", "close", "adj_close", "volume", "currency"]).sort("date").write_parquet(str(status.path))
        return "repaired_full_replace"
    except Exception as e:
        append_update_log(log_path, symbol, f"repair_full_intraday_{interval}_error:{e}", 1)
        return f"repair_full_error:{e}"


def _clean_intraday_cache(
    targets: list[tuple[str, Path]],
    retention_days: int = 10,
) -> dict[str, str]:
    actions: dict[str, str] = {}
    cutoff = datetime.now().replace(microsecond=0, second=0, minute=0, hour=0) - timedelta(days=max(1, retention_days))
    for symbol, path in targets:
        try:
            df = read_parquet_if_exists(path)
            if df is None or df.is_empty():
                actions[symbol.upper()] = "skip_empty"
                continue
            before = int(df.height)
            cleaned = _sanitize_intraday_df(df)
            if "date" in cleaned.columns:
                cleaned = cleaned.filter(pl.col("date") >= pl.lit(cutoff)).sort("date")
            after = int(cleaned.height)
            if after == 0:
                actions[symbol.upper()] = "skip_clean_empty"
                continue
            if before != after:
                cleaned.write_parquet(str(path))
                actions[symbol.upper()] = f"cleaned_removed_{before - after}"
            else:
                actions[symbol.upper()] = "skip_clean"
        except Exception as e:
            actions[symbol.upper()] = f"clean_error:{e}"
    return actions


def _status_rows(
    statuses: list[FileStatus],
    meta_by_symbol: dict[str, SymbolMeta],
    yf_latest_by_symbol: dict[str, YFLatest],
    sample_audit_by_symbol: dict[str, YFSampleAudit],
    include_meta: bool,
    include_yf: bool,
    include_sample: bool,
    provider_baseline: str = "yf",
    yf_close_diff_threshold_pct: float = 1.0,
    yf_ignore_current_day: bool = True,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for s in statuses:
        meta = meta_by_symbol.get(s.symbol.upper())
        mismatch = _possible_venue_mismatch(s.symbol, meta) if include_meta else ""
        yf = yf_latest_by_symbol.get(s.symbol.upper()) if include_yf else None
        yf_date = yf.date if yf is not None else ""
        yf_close = _fmt_price(yf.close) if yf is not None else ""
        yf_currency = yf.currency if yf is not None else ""
        yf_error = yf.error.replace(",", ";") if yf is not None else ""
        sample = sample_audit_by_symbol.get(s.symbol.upper()) if include_sample else None
        yf_close_diff_pct = ""
        parquet_vs_yf = ""
        cause = ""
        repair_action = ""
        if yf is not None and yf.ok and s.last_close is not None and yf.close is not None and yf.close != 0:
            diff_pct = ((s.last_close - yf.close) / yf.close) * 100.0
            yf_close_diff_pct = f"{diff_pct:.2f}"
            parquet_day = _parse_date_ymd(s.end_date)
            yf_day = _parse_date_ymd(yf.date)
            today = datetime.now().date()
            ignore_live_day = bool(yf_ignore_current_day) and (parquet_day == today or yf_day == today)
            same_day = parquet_day is not None and yf_day is not None and parquet_day == yf_day
            close_far = abs(diff_pct) > float(yf_close_diff_threshold_pct)
            if ignore_live_day:
                parquet_vs_yf = "ok"
            elif not same_day:
                parquet_vs_yf = "date_mismatch"
            elif close_far:
                parquet_vs_yf = "price_mismatch"
            else:
                parquet_vs_yf = "ok"
        elif yf is not None and not yf.ok:
            parquet_vs_yf = "yf_error"

        if include_yf or include_meta or include_sample:
            cause = _infer_mismatch_cause(
                status=s,
                venue_mismatch=mismatch,
                parquet_vs_yf=parquet_vs_yf,
                yf_error=yf_error,
                yf_close_diff_pct=yf_close_diff_pct,
                sample_audit=sample,
                provider_baseline=provider_baseline,
            )

        rows.append(
            {
                "symbol": s.symbol,
                "status": "ok" if s.valid else "issue",
                "rows": str(s.rows),
                "start_date": s.start_date,
                "end_date": s.end_date,
                "last_open": _fmt_price(s.last_open),
                "last_high": _fmt_price(s.last_high),
                "last_low": _fmt_price(s.last_low),
                "last_close": _fmt_price(s.last_close),
                "period": s.period_label,
                "period_rows": str(s.period_rows),
                "period_start": s.period_start_date,
                "period_end": s.period_end_date,
                "period_high": _fmt_price(s.period_high),
                "period_low": _fmt_price(s.period_low),
                "dup_dates": str(s.duplicate_dates),
                "null_ohlc": str(s.null_ohlc_rows),
                "bad_ohlc": str(s.bad_ohlc_rows),
                "large_gaps": str(s.large_gap_count),
                "extreme_moves": str(s.extreme_move_count),
                "sorted_dates": str(s.sorted_dates),
                "error": s.error.replace(",", ";"),
                "company_name": meta.name if (include_meta and meta is not None) else "",
                "isin": meta.isin if (include_meta and meta is not None) else "",
                "exchange": meta.exchange if (include_meta and meta is not None) else "",
                "country": meta.country if (include_meta and meta is not None) else "",
                "venue_mismatch": mismatch if include_meta else "",
                "yf_last_date": yf_date if include_yf else "",
                "yf_last_close": yf_close if include_yf else "",
                "yf_currency": yf_currency if include_yf else "",
                "yf_close_diff_pct": yf_close_diff_pct if include_yf else "",
                "parquet_vs_yf": parquet_vs_yf if include_yf else "",
                "yf_error": yf_error if include_yf else "",
                "sample_checked_days": str(sample.checked_days) if sample is not None else "",
                "sample_mismatch_days": str(sample.mismatch_days) if sample is not None else "",
                "sample_status": (
                    "error"
                    if (sample is not None and sample.error)
                    else ("mismatch" if (sample is not None and sample.mismatch_days > 0) else ("ok" if sample is not None else ""))
                ),
                "sample_max_abs_ohlc_diff": (f"{sample.max_abs_ohlc_diff:.6f}" if sample is not None else ""),
                "sample_mismatch_dates": ("|".join(sample.first_mismatch_dates) if sample is not None else ""),
                "sample_error": (sample.error.replace(",", ";") if sample is not None else ""),
                "suspected_cause": cause,
                "repair_action": repair_action,
            }
        )
    return rows


def _row_has_issue(
    row: dict[str, str],
    include_meta: bool,
    include_yf: bool,
    include_sample: bool,
    provider_baseline: str = "yf",
) -> bool:
    if row.get("status") != "ok":
        return True
    if include_meta and (row.get("venue_mismatch") or ""):
        return True
    if include_yf and row.get("parquet_vs_yf") in {"date_mismatch", "price_mismatch", "yf_error"}:
        return True
    if include_sample:
        sample_status = row.get("sample_status")
        if sample_status == "error":
            return True
        if sample_status == "mismatch":
            if provider_baseline not in {"stooq", "mixed"}:
                return True
    return False


def _row_has_critical_issue(
    row: dict[str, str],
    large_gaps_critical: bool = False,
    symbol_asset_type: str = "unknown",
    etf_large_gap_tolerance: int = 0,
    etf_max_large_gaps_per_year: float = 0.0,
    parquet_mode: str = "daily",
    intraday_large_gaps_critical: bool = False,
) -> bool:
    # Critical = parquet integrity/data correctness issue.
    # External/provider mismatches and metadata mismatches are non-critical.
    if row.get("status") == "ok":
        return False

    def _to_int(key: str) -> int:
        try:
            return int(str(row.get(key, "0")).strip() or "0")
        except Exception:
            return 0

    if _to_int("dup_dates") > 0:
        return True
    if _to_int("null_ohlc") > 0:
        return True
    if _to_int("bad_ohlc") > 0:
        return True
    if str(row.get("sorted_dates", "")).strip().lower() == "false":
        return True
    if str(row.get("error", "")).strip():
        return True
    large_gaps = _to_int("large_gaps")
    if large_gaps > 0:
        if parquet_mode == "intraday" and not intraday_large_gaps_critical:
            return False
        kind = str(symbol_asset_type).strip().lower()
        if kind == "stock":
            return True
        if kind == "etf":
            tol = max(0, int(etf_large_gap_tolerance))
            if large_gaps <= tol:
                return False

            # Avoid permanent failures from very long histories: for ETFs,
            # evaluate large gaps relative to data span when configured.
            max_per_year = max(0.0, float(etf_max_large_gaps_per_year))
            if max_per_year > 0:
                start_d = _parse_date_ymd(str(row.get("start_date", "")))
                end_d = _parse_date_ymd(str(row.get("end_date", "")))
                if start_d is not None and end_d is not None and end_d >= start_d:
                    span_days = max(1, (end_d - start_d).days + 1)
                    span_years = max(1.0 / 12.0, float(span_days) / 365.25)
                    gaps_per_year = float(large_gaps) / span_years
                    return gaps_per_year > max_per_year
            return True
        if large_gaps_critical:
            return True
    return False


def _is_etf_index_name(index_name: str) -> bool:
    n = str(index_name or "").strip().lower()
    if not n:
        return False
    return n == "etf" or n.startswith("etf_")


def _infer_symbol_asset_type(
    symbol: str,
    index_map: dict[str, set[str]],
    meta_by_symbol: dict[str, SymbolMeta],
) -> str:
    sym = str(symbol or "").strip().upper()
    idxs = {str(x).strip().lower() for x in index_map.get(sym, set()) if str(x).strip()}
    if any(_is_etf_index_name(i) for i in idxs):
        return "etf"
    if idxs:
        return "stock"
    meta = meta_by_symbol.get(sym)
    if meta is not None and "etf" in str(meta.source or "").strip().lower():
        return "etf"
    return "unknown"


def _print_statuses(
    statuses: list[FileStatus],
    meta_by_symbol: dict[str, SymbolMeta],
    yf_latest_by_symbol: dict[str, YFLatest],
    sample_audit_by_symbol: dict[str, YFSampleAudit],
    include_meta: bool,
    include_yf: bool,
    include_sample: bool,
    provider_baseline: str = "yf",
) -> None:
    if not statuses:
        print("No parquet files matched.")
        return

    cols = [
        "symbol",
        "status",
        "rows",
        "start_date",
        "end_date",
        "last_open",
        "last_high",
        "last_low",
        "last_close",
        "period",
        "period_rows",
        "period_start",
        "period_end",
        "period_high",
        "period_low",
        "dup_dates",
        "null_ohlc",
        "bad_ohlc",
        "large_gaps",
        "extreme_moves",
        "sorted_dates",
        "error",
    ]
    if include_meta:
        cols += ["company_name", "isin", "exchange", "country", "venue_mismatch"]
    if include_yf:
        cols += ["yf_last_date", "yf_last_close", "yf_currency", "yf_close_diff_pct", "parquet_vs_yf", "yf_error"]
    if include_sample:
        cols += [
            "sample_checked_days",
            "sample_mismatch_days",
            "sample_status",
            "sample_max_abs_ohlc_diff",
            "sample_mismatch_dates",
            "sample_error",
        ]
    if include_meta or include_yf or include_sample:
        cols += ["suspected_cause", "repair_action"]

    rows = _status_rows(
        statuses=statuses,
        meta_by_symbol=meta_by_symbol,
        yf_latest_by_symbol=yf_latest_by_symbol,
        sample_audit_by_symbol=sample_audit_by_symbol,
        include_meta=include_meta,
        include_yf=include_yf,
        include_sample=include_sample,
        provider_baseline=provider_baseline,
    )

    header = ",".join(cols)
    print(header)
    for row in rows:
        print(",".join(row[c] for c in cols))

    print("\nDetails (table)")
    display_cols = [
        "symbol",
        "last_open",
        "last_high",
        "last_low",
        "last_close",
        "end_date",
        "status",
    ]
    if include_meta:
        display_cols += ["company_name", "isin", "venue_mismatch"]
    if include_yf:
        display_cols += ["yf_last_date", "yf_last_close", "yf_close_diff_pct", "parquet_vs_yf"]
    if include_sample:
        display_cols += ["sample_checked_days", "sample_mismatch_days", "sample_status"]
    if include_meta or include_yf or include_sample:
        display_cols += ["suspected_cause", "repair_action"]
    print(pl.DataFrame(rows).select(display_cols))


def _print_summary(
    statuses: list[FileStatus],
    shown_count: int | None = None,
    universe_counts: dict[str, int] | None = None,
    parquet_mode: str = "daily",
) -> None:
    total = len(statuses)
    ok = sum(1 for s in statuses if s.valid)
    missing = sum(1 for s in statuses if not s.exists)
    unreadable = sum(1 for s in statuses if s.exists and not s.readable)
    empty = sum(1 for s in statuses if s.readable and s.rows == 0)
    with_duplicates = sum(1 for s in statuses if s.duplicate_dates > 0)
    with_nulls = sum(1 for s in statuses if s.null_ohlc_rows > 0)
    with_bad_ohlc = sum(1 for s in statuses if s.bad_ohlc_rows > 0)
    with_large_gaps = sum(1 for s in statuses if s.large_gap_count > 0)
    with_extreme_moves = sum(1 for s in statuses if s.extreme_move_count > 0)
    unsorted = sum(1 for s in statuses if s.readable and s.rows > 0 and not s.sorted_dates)
    total_rows = sum(s.rows for s in statuses)

    print("\nSummary")
    if shown_count is not None:
        print(f"  rows_shown: {shown_count}")
    print(f"  files_checked: {total}")
    print(f"  valid_files: {ok}")
    print(f"  missing_files: {missing}")
    print(f"  unreadable_files: {unreadable}")
    print(f"  empty_files: {empty}")
    print(f"  files_with_duplicate_dates: {with_duplicates}")
    print(f"  files_with_null_ohlc: {with_nulls}")
    print(f"  files_with_bad_ohlc: {with_bad_ohlc}")
    gap_label = "same-day gap anomalies" if parquet_mode == "intraday" else "large_gaps(>7d)"
    print(f"  files_with_{gap_label}: {with_large_gaps}")
    print(f"  files_with_extreme_daily_moves(>|40%|): {with_extreme_moves}")
    print(f"  files_with_unsorted_dates: {unsorted}")
    print(f"  total_rows: {total_rows}")
    if universe_counts:
        print("  checked_by_universe:")
        for idx in sorted(universe_counts.keys()):
            print(f"    {idx}: {universe_counts[idx]}")


def _print_extreme_moves(statuses: list[FileStatus]) -> None:
    rows = [s for s in statuses if int(s.extreme_move_count) > 0]
    rows.sort(key=lambda s: int(s.extreme_move_count), reverse=True)
    print("\nExtreme Moves (>|40%| close-to-close daily, full file range)")
    print(f"  symbols_with_extreme_moves: {len(rows)}")
    if not rows:
        return
    print("symbol,extreme_moves,start_date,end_date,status,path")
    for s in rows:
        status = "ok" if s.valid else "issue"
        print(
            f"{s.symbol},{int(s.extreme_move_count)},{s.start_date},{s.end_date},{status},{s.path}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Validate OHLC parquet files and report last OHLC prices + summary stats."
    )
    ap.add_argument("--config", default=str(default_config_path()), help="YAML config path")
    ap.add_argument("--root", default="", help="Parquet root directory")
    ap.add_argument(
        "--parquet-kind",
        choices=["auto", "daily", "intraday"],
        default="auto",
        help="Validation mode. 'auto' infers from --root (default: auto).",
    )
    ap.add_argument(
        "--symbols",
        nargs="*",
        default=[],
        help="Symbols to check (e.g. --symbols AAPL MSFT). If omitted, checks all under --root.",
    )
    ap.add_argument(
        "--paths",
        nargs="*",
        default=[],
        help="Explicit parquet paths to check. Overrides --symbols/--root selection.",
    )
    ap.add_argument("--year", type=int, default=None, help="Report period high/low for a year, e.g. 2022")
    ap.add_argument("--yyear", type=int, default=None, help="Alias for --year")
    ap.add_argument("--month", default=None, help="Report period high/low for a month, format YYYY-MM")
    ap.add_argument(
        "--with-meta",
        action="store_true",
        help="Attach company metadata (name, ISIN, exchange/country) and venue mismatch flag from universe files.",
    )
    ap.add_argument(
        "--verify-yf",
        action="store_true",
        help="Fetch latest daily close from yfinance and compare with parquet last close/date.",
    )
    ap.add_argument(
        "--yf-close-diff-threshold-pct",
        type=float,
        default=1.0,
        help="Percent diff threshold to flag parquet_vs_yf=price_mismatch (default: 1.0).",
    )
    ap.add_argument(
        "--yf-ignore-current-day",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Ignore YF latest-vs-parquet mismatch checks when either side is dated today (default: on).",
    )
    ap.add_argument(
        "--repair-mismatches",
        action="store_true",
        help="With --verify-yf, refetch and upsert symbols flagged as date/price mismatch.",
    )
    ap.add_argument(
        "--clean-intraday-cache",
        action="store_true",
        help="For intraday roots, rewrite parquet files to remove malformed all-null bars before validation.",
    )
    ap.add_argument(
        "--repair-lookback-days",
        type=int,
        default=180,
        help="Lookback window for mismatch repair fetches (default: 180).",
    )
    ap.add_argument(
        "--issues-only",
        action="store_true",
        help="Show only symbols that have issues (validation, metadata mismatch, or yfinance mismatch/errors).",
    )
    ap.add_argument(
        "--list-extreme-moves",
        action="store_true",
        help="Print a compact CSV list of symbols where extreme_moves > 0 (>|40%%| close-to-close, full file range).",
    )
    ap.add_argument(
        "--sample-yf-days",
        type=int,
        default=0,
        help="Sample N random overlapping days and compare parquet vs yfinance OHLC for consistency.",
    )
    ap.add_argument(
        "--sample-yf-recent-days",
        type=int,
        default=0,
        help="Limit sampled OHLC consistency checks to the most recent N calendar days of overlap.",
    )
    ap.add_argument(
        "--sample-yf-atol",
        type=float,
        default=1e-4,
        help="Absolute tolerance for sampled OHLC comparisons (default: 1e-4).",
    )
    ap.add_argument(
        "--provider-baseline",
        choices=["yf", "stooq", "mixed"],
        default="yf",
        help="Data baseline policy for issue detection. Use stooq/mixed to treat historical YF sample mismatches as expected provider differences.",
    )
    ap.add_argument(
        "--universe",
        default="",
        help="Universe CSV path used for metadata lookup (used with --with-meta).",
    )
    ap.add_argument(
        "--ignore-orphans",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When scanning a parquet root without explicit --symbols/--paths, ignore parquet files not present in the active universe (default: on).",
    )
    ap.add_argument(
        "--universe-dir",
        default="",
        help="Fallback directory with per-index universe CSVs (used with --with-meta).",
    )
    ap.add_argument(
        "--summary-json",
        default="",
        help="Write machine-readable verification summary JSON to this path.",
    )
    ap.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit with non-zero status if any issue is detected (automation gate).",
    )
    ap.add_argument(
        "--fail-severity",
        choices=["critical", "all"],
        default="all",
        help="Severity used with --fail-on-issues: critical blocks only integrity issues; all blocks any issue (default: all).",
    )
    ap.add_argument(
        "--large-gaps-critical",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Treat large historical gaps as critical for unknown asset type symbols (default: false).",
    )
    ap.add_argument(
        "--etf-large-gap-tolerance",
        type=int,
        default=2,
        help="Allowed number of >7d historical gaps for ETF symbols before becoming critical (default: 2).",
    )
    ap.add_argument(
        "--etf-max-large-gaps-per-year",
        type=float,
        default=3.0,
        help="ETF critical gate: if total gaps exceed tolerance, fail only when large-gap rate is above this per-year threshold (default: 3.0). Set 0 to disable rate check.",
    )
    ap.add_argument(
        "--intraday-large-gaps-critical",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="For intraday roots, treat same-day gap anomalies as critical gate failures (default: off).",
    )
    ap.add_argument(
        "--gate-min-files",
        type=int,
        default=400,
        help="Gate threshold: minimum parquet file count (default: 400).",
    )
    ap.add_argument(
        "--gate-max-zero-byte",
        type=int,
        default=0,
        help="Gate threshold: maximum allowed zero-byte parquet files (default: 0).",
    )
    ap.add_argument(
        "--gate-max-missing-ratio",
        type=float,
        default=0.20,
        help="Gate threshold: maximum allowed missing ratio per tracked universe (default: 0.20).",
    )
    ap.add_argument(
        "--gate-sample-read-files",
        type=int,
        default=30,
        help="Gate threshold: sampled parquet files for read checks (default: 30).",
    )
    ap.add_argument(
        "--gate-max-drop-ratio",
        type=float,
        default=0.10,
        help="Gate threshold: max file-count drop ratio vs baseline summary (default: 0.10).",
    )
    ap.add_argument(
        "--gate-baseline-summary",
        default="",
        help="Optional previous summary JSON for drop-vs-baseline check.",
    )
    args = ap.parse_args()

    year = args.year if args.year is not None else args.yyear
    if args.month is not None:
        try:
            datetime.strptime(args.month, "%Y-%m")
        except ValueError:
            raise SystemExit("--month must be in YYYY-MM format")
    if year is not None and args.month is not None:
        raise SystemExit("Use only one of --year/--yyear or --month")

    cfg = Config.load(args.config)
    root = Path(args.root) if str(args.root).strip() else parquet_root_path(cfg)
    parquet_mode, intraday_interval_minutes = _infer_parquet_mode(root, str(args.parquet_kind))
    intraday_interval = f"{intraday_interval_minutes}m" if intraday_interval_minutes else "5m"
    targets = _collect_targets(
        root=root,
        symbols=args.symbols,
        paths=args.paths,
        universe_csv=(Path(args.universe) if str(args.universe).strip() else universe_csv_path(cfg)),
        ignore_orphans=bool(args.ignore_orphans),
    )
    clean_actions: dict[str, str] = {}
    if args.clean_intraday_cache:
        if parquet_mode != "intraday":
            raise SystemExit("--clean-intraday-cache is supported only for intraday parquet roots.")
        clean_actions = _clean_intraday_cache(targets)
    statuses = [
        _validate_file(
            path=path,
            symbol=symbol,
            period_year=year,
            period_month=args.month,
            mode=parquet_mode,
            intraday_interval_minutes=intraday_interval_minutes,
        )
        for symbol, path in targets
    ]
    all_statuses = statuses[:]

    meta_by_symbol: dict[str, SymbolMeta] = {}
    if args.with_meta:
        meta_by_symbol = _load_symbol_meta(
            universe_csv=(Path(args.universe) if str(args.universe).strip() else universe_csv_path(cfg)),
            universe_dir=Path(args.universe_dir),
        )

    yf_latest_by_symbol: dict[str, YFLatest] = {}
    if args.verify_yf:
        for s in tqdm(statuses, desc="Verifying latest vs yfinance", unit="symbol"):
            yf_latest_by_symbol[s.symbol.upper()] = _fetch_yf_latest(
                s.symbol,
                mode=parquet_mode,
                intraday_interval=intraday_interval,
            )

    sample_audit_by_symbol: dict[str, YFSampleAudit] = {}
    if int(args.sample_yf_days) > 0:
        for s in tqdm(statuses, desc="Sampling historical YF consistency", unit="symbol"):
            sample_audit_by_symbol[s.symbol.upper()] = _sample_yf_consistency(
                status=s,
                sample_days=int(args.sample_yf_days),
                atol=float(args.sample_yf_atol),
                sample_recent_days=int(args.sample_yf_recent_days),
                mode=parquet_mode,
                intraday_interval=intraday_interval,
            )

    repair_by_symbol: dict[str, str] = {}
    if args.repair_mismatches:
        if not args.verify_yf:
            raise SystemExit("--repair-mismatches requires --verify-yf")
        log_path = update_log_path(cfg)
        for s in tqdm(statuses, desc="Repairing mismatches from yfinance", unit="symbol"):
            yf = yf_latest_by_symbol.get(s.symbol.upper())
            if yf is None:
                continue
            if not yf.ok:
                repair_by_symbol[s.symbol.upper()] = "skip_yf_error"
                continue
            if s.last_close is None or yf.close is None or yf.close == 0:
                repair_by_symbol[s.symbol.upper()] = "skip_insufficient_data"
                continue

            parquet_day = _parse_date_ymd(s.end_date)
            yf_day = _parse_date_ymd(yf.date)
            same_day = parquet_day is not None and yf_day is not None and parquet_day == yf_day
            today = datetime.now().date()
            ignore_live_day = bool(args.yf_ignore_current_day) and (parquet_day == today or yf_day == today)
            diff_pct = abs(((s.last_close - yf.close) / yf.close) * 100.0)
            mismatch = (not ignore_live_day) and ((not same_day) or (diff_pct > float(args.yf_close_diff_threshold_pct)))
            if mismatch:
                if parquet_mode == "intraday":
                    repair_by_symbol[s.symbol.upper()] = _repair_intraday_symbol_from_yf(
                        status=s,
                        interval=intraday_interval,
                        log_path=log_path,
                    )
                else:
                    repair_by_symbol[s.symbol.upper()] = _repair_symbol_from_yf(
                        status=s,
                        lookback_days=int(args.repair_lookback_days),
                        log_path=log_path,
                    )
            else:
                repair_by_symbol[s.symbol.upper()] = "skip_no_mismatch"

        # If sampling found historical mismatches, do full-history replacement.
        for s in tqdm(statuses, desc="Repairing historical sample mismatches", unit="symbol"):
            sample = sample_audit_by_symbol.get(s.symbol.upper())
            if sample is None:
                continue
            if sample.error:
                continue
            if sample.mismatch_days > 0:
                if parquet_mode == "intraday":
                    repair_by_symbol[s.symbol.upper()] = _repair_intraday_symbol_full_history_from_yf(
                        status=s,
                        interval=intraday_interval,
                        log_path=log_path,
                    )
                else:
                    repair_by_symbol[s.symbol.upper()] = _repair_symbol_full_history_from_yf(
                        status=s,
                        log_path=log_path,
                    )

        # Re-read and re-check after repairs so reported rows reflect post-repair state,
        # not the pre-repair snapshot.
        repaired_syms = {
            sym
            for sym, action in repair_by_symbol.items()
            if action
            and not action.startswith("skip_")
            and not action.startswith("repair_error:")
            and action not in {"repair_no_data", "repair_full_no_data", "repair_full_error"}
        }
        if repaired_syms:
            statuses = [
                _validate_file(
                    path=path,
                    symbol=symbol,
                    period_year=year,
                    period_month=args.month,
                    mode=parquet_mode,
                    intraday_interval_minutes=intraday_interval_minutes,
                )
                for symbol, path in targets
            ]
            if args.verify_yf:
                for s in tqdm(statuses, desc="Re-verifying latest vs yfinance (post-repair)", unit="symbol"):
                    yf_latest_by_symbol[s.symbol.upper()] = _fetch_yf_latest(
                        s.symbol,
                        mode=parquet_mode,
                        intraday_interval=intraday_interval,
                    )
            if int(args.sample_yf_days) > 0:
                for s in tqdm(statuses, desc="Re-sampling historical YF consistency (post-repair)", unit="symbol"):
                    sample_audit_by_symbol[s.symbol.upper()] = _sample_yf_consistency(
                        status=s,
                        sample_days=int(args.sample_yf_days),
                        atol=float(args.sample_yf_atol),
                        sample_recent_days=int(args.sample_yf_recent_days),
                        mode=parquet_mode,
                        intraday_interval=intraday_interval,
                    )

    rows = _status_rows(
        statuses=statuses,
        meta_by_symbol=meta_by_symbol,
        yf_latest_by_symbol=yf_latest_by_symbol,
        sample_audit_by_symbol=sample_audit_by_symbol,
        include_meta=args.with_meta,
        include_yf=args.verify_yf,
        include_sample=int(args.sample_yf_days) > 0,
        provider_baseline=str(args.provider_baseline),
        yf_close_diff_threshold_pct=float(args.yf_close_diff_threshold_pct),
        yf_ignore_current_day=bool(args.yf_ignore_current_day),
    )
    if repair_by_symbol:
        for row in rows:
            row["repair_action"] = repair_by_symbol.get(row["symbol"].upper(), "")
    if clean_actions:
        for row in rows:
            sym = row["symbol"].upper()
            prior = row.get("repair_action", "")
            clean = clean_actions.get(sym, "")
            if clean:
                row["repair_action"] = f"{prior};{clean}".strip(";") if prior else clean

    index_map = _load_symbol_index_map(
        universe_csv=(Path(args.universe) if str(args.universe).strip() else universe_csv_path(cfg)),
        universe_dir=Path(args.universe_dir),
    )
    symbol_asset_type: dict[str, str] = {}
    for s in all_statuses:
        symbol_asset_type[s.symbol.upper()] = _infer_symbol_asset_type(
            symbol=s.symbol,
            index_map=index_map,
            meta_by_symbol=meta_by_symbol,
        )

    issue_rows_all = [
        r
        for r in rows
        if _row_has_issue(
            r,
            include_meta=args.with_meta,
            include_yf=args.verify_yf,
            include_sample=int(args.sample_yf_days) > 0,
            provider_baseline=str(args.provider_baseline),
        )
    ]
    issue_rows_critical = [
        r
        for r in rows
        if _row_has_critical_issue(
            r,
            large_gaps_critical=bool(args.large_gaps_critical),
            symbol_asset_type=symbol_asset_type.get(str(r.get("symbol", "")).strip().upper(), "unknown"),
            etf_large_gap_tolerance=int(args.etf_large_gap_tolerance),
            etf_max_large_gaps_per_year=float(args.etf_max_large_gaps_per_year),
            parquet_mode=parquet_mode,
            intraday_large_gaps_critical=bool(args.intraday_large_gaps_critical),
        )
    ]

    if args.issues_only:
        issue_symbols = {r["symbol"] for r in issue_rows_all}
        rows = issue_rows_all
        statuses = [s for s in statuses if s.symbol in issue_symbols]
    universe_counts: dict[str, int] = {}
    for s in all_statuses:
        idxs = index_map.get(s.symbol.upper(), set())
        if not idxs:
            universe_counts["unknown"] = universe_counts.get("unknown", 0) + 1
            continue
        for idx in idxs:
            universe_counts[idx] = universe_counts.get(idx, 0) + 1

    # print using the same renderer but with our enriched rows
    if not rows:
        print("No issue rows matched current checks." if args.issues_only else "No parquet files matched.")
    elif not statuses:
        _print_statuses(
            statuses=statuses,
            meta_by_symbol=meta_by_symbol,
            yf_latest_by_symbol=yf_latest_by_symbol,
            sample_audit_by_symbol=sample_audit_by_symbol,
            include_meta=args.with_meta,
            include_yf=args.verify_yf,
            include_sample=int(args.sample_yf_days) > 0,
            provider_baseline=str(args.provider_baseline),
        )
    else:
        cols = [
            "symbol",
            "status",
            "rows",
            "start_date",
            "end_date",
            "last_open",
            "last_high",
            "last_low",
            "last_close",
            "period",
            "period_rows",
            "period_start",
            "period_end",
            "period_high",
            "period_low",
            "dup_dates",
            "null_ohlc",
            "bad_ohlc",
            "large_gaps",
            "extreme_moves",
            "sorted_dates",
            "error",
        ]
        if args.with_meta:
            cols += ["company_name", "isin", "exchange", "country", "venue_mismatch"]
        if args.verify_yf:
            cols += ["yf_last_date", "yf_last_close", "yf_currency", "yf_close_diff_pct", "parquet_vs_yf", "yf_error"]
        if int(args.sample_yf_days) > 0:
            cols += [
                "sample_checked_days",
                "sample_mismatch_days",
                "sample_status",
                "sample_max_abs_ohlc_diff",
                "sample_mismatch_dates",
                "sample_error",
            ]
        if args.with_meta or args.verify_yf or int(args.sample_yf_days) > 0:
            cols += ["suspected_cause", "repair_action"]
        print(",".join(cols))
        for row in rows:
            print(",".join(row.get(c, "") for c in cols))
        print("\nDetails (table)")
        display_cols = ["symbol", "last_open", "last_high", "last_low", "last_close", "end_date", "status"]
        if args.with_meta:
            display_cols += ["company_name", "isin", "venue_mismatch"]
        if args.verify_yf:
            display_cols += ["yf_last_date", "yf_last_close", "yf_close_diff_pct", "parquet_vs_yf"]
        if int(args.sample_yf_days) > 0:
            display_cols += ["sample_checked_days", "sample_mismatch_days", "sample_status"]
        if args.with_meta or args.verify_yf or int(args.sample_yf_days) > 0:
            display_cols += ["suspected_cause", "repair_action"]
        print(pl.DataFrame(rows).select(display_cols))
    _print_summary(
        all_statuses,
        shown_count=len(rows),
        universe_counts=universe_counts,
        parquet_mode=parquet_mode,
    )
    if bool(args.list_extreme_moves):
        _print_extreme_moves(all_statuses)

    parquet_sanity = run_parquet_sanity_checks(
        ParquetVerifyConfig(
            root=Path(args.root),
            universe_dir=Path(args.universe_dir),
            min_parquet_files=int(args.gate_min_files),
            max_zero_byte=int(args.gate_max_zero_byte),
            max_missing_ratio=float(args.gate_max_missing_ratio),
            sample_read_files=int(args.gate_sample_read_files),
            max_drop_ratio=float(args.gate_max_drop_ratio),
            baseline_summary_path=(Path(args.gate_baseline_summary) if str(args.gate_baseline_summary).strip() else None),
        )
    )

    summary = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "root": str(args.root),
        "parquet_kind": parquet_mode,
        "intraday_interval": intraday_interval if parquet_mode == "intraday" else "",
        "verify_yf": bool(args.verify_yf),
        "clean_intraday_cache": bool(args.clean_intraday_cache),
        "provider_baseline": str(args.provider_baseline),
        "issues_only": bool(args.issues_only),
        "shown_rows": int(len(rows)),
        "issue_rows": int(len(issue_rows_all)),
        "issue_rows_critical": int(len(issue_rows_critical)),
        "issue_symbols": sorted({str(r.get("symbol", "")).strip() for r in issue_rows_all if str(r.get("symbol", "")).strip()}),
        "issue_symbols_critical": sorted(
            {str(r.get("symbol", "")).strip() for r in issue_rows_critical if str(r.get("symbol", "")).strip()}
        ),
        "parquet_sanity": parquet_sanity,
        "fail_severity": str(args.fail_severity),
        "large_gaps_critical": bool(args.large_gaps_critical),
        "etf_large_gap_tolerance": int(args.etf_large_gap_tolerance),
        "etf_max_large_gaps_per_year": float(args.etf_max_large_gaps_per_year),
        "intraday_large_gaps_critical": bool(args.intraday_large_gaps_critical),
        "clean_actions": clean_actions,
        "ok": bool(
            (
                (len(issue_rows_all) == 0)
                if str(args.fail_severity) == "all"
                else (len(issue_rows_critical) == 0)
            )
            and bool(parquet_sanity.get("ok", False))
        ),
    }

    if str(args.summary_json).strip():
        outp = Path(str(args.summary_json))
        write_verification_summary(outp, summary)
        print(f"Wrote verification summary: {outp}")

    if bool(args.fail_on_issues) and not bool(summary["ok"]):
        reasons: list[str] = []
        if str(args.fail_severity) == "all":
            if len(issue_rows_all) > 0:
                reasons.append(f"issue_rows={len(issue_rows_all)}")
        else:
            if len(issue_rows_critical) > 0:
                reasons.append(f"issue_rows_critical={len(issue_rows_critical)}")
        for e in parquet_sanity.get("errors", []):
            reasons.append(str(e))
        msg = "verification gate failed: " + "; ".join(reasons) if reasons else "verification gate failed"
        print(msg)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
