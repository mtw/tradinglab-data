from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from .data_stooq import StooqDownloadSpec, fetch_stooq_history, infer_currency_from_symbol
from .data_yf import append_update_log, fetch_symbol_currency, fetch_yfinance_history_bulk, read_parquet_if_exists
from .config import intraday_root_path, parquet_root_path, runs_root_path, ticker_overrides_path, universe_csv_path, universe_dir_path, update_log_path
from .extended_hours_monitor import persist_extended_hours_report_html, summarize_gap_report, update_extended_hours_store
from .universe import canonicalize_symbol, load_ticker_overrides, load_universe_frame


STRICT_SINGLE_SYMBOL_SUFFIXES = (".VI",)


def _run_dir(runs_root: str | Path) -> Path:
    d = datetime.now().strftime("%Y-%m-%d")
    p = Path(runs_root) / d
    p.mkdir(parents=True, exist_ok=True)
    return p


def _load_active_symbols_from_cfg(cfg: Any, symbols_override: list[str] | None = None) -> list[str]:
    universe_csv = universe_csv_path(cfg)
    universe_dir = universe_dir_path(cfg)
    overrides_path = ticker_overrides_path(cfg)
    df = load_universe_frame(universe_csv, universe_dir=universe_dir, ticker_overrides_path=overrides_path)
    if df.is_empty():
        raise ValueError(f"No universe data found. Build {universe_csv} or add CSVs under {universe_dir}.")
    if "source" in df.columns:
        df = df.filter(pl.col("source").fill_null("").str.to_lowercase() != "exchange")
    symbols = df.get_column("symbol").to_list()
    overrides = load_ticker_overrides(overrides_path)

    if symbols_override:
        requested = []
        seen_req: set[str] = set()
        for s in symbols_override:
            up = canonicalize_symbol(str(s), overrides=overrides)
            if up and up not in seen_req:
                seen_req.add(up)
                requested.append(up)
        if requested:
            universe_map = {str(s).strip().upper(): str(s).strip() for s in symbols}
            selected = [universe_map[s] for s in requested if s in universe_map]
            missing = [s for s in requested if s not in universe_map]
            if missing:
                print("[WARN] symbols not present in active universe and will be skipped: " + ",".join(missing))
            if not selected:
                raise SystemExit("No requested symbols found in active universe.")
            symbols = selected
    return symbols


def _migrate_symbol_alias_parquet(symbols: list[str], parquet_root: str | Path, intraday_root: str | Path | None = None) -> None:
    overrides = load_ticker_overrides()
    if not overrides:
        return
    canonical = {canonicalize_symbol(sym, overrides=overrides) for sym in symbols}
    daily_root = Path(parquet_root)
    for raw, yahoo in overrides.items():
        if yahoo not in canonical:
            continue
        old_daily = daily_root / f"{raw}.parquet"
        new_daily = daily_root / f"{yahoo}.parquet"
        if old_daily.exists() and not new_daily.exists():
            old_daily.rename(new_daily)
            print(f"[SYMBOL_OVERRIDE] migrated daily parquet {raw} -> {yahoo}")
        if intraday_root is None:
            continue
        intr_root = Path(intraday_root)
        if not intr_root.exists():
            continue
        for interval_dir in sorted([p for p in intr_root.iterdir() if p.is_dir()]):
            old_intr = interval_dir / f"{raw}.parquet"
            new_intr = interval_dir / f"{yahoo}.parquet"
            if old_intr.exists() and not new_intr.exists():
                old_intr.rename(new_intr)
                print(f"[SYMBOL_OVERRIDE] migrated intraday parquet {interval_dir.name}/{raw} -> {yahoo}")


def monitor_extended_hours_from_config(
    cfg: Any,
    symbols_override: list[str] | None = None,
    top_n: int = 25,
    session_filter: str = "all",
) -> dict[str, Any]:
    symbols = _load_active_symbols_from_cfg(cfg, symbols_override=symbols_override)
    daily_root = parquet_root_path(cfg)
    runs_root = runs_root_path(cfg)
    log_path = update_log_path(cfg)

    intraday_root = str(intraday_root_path(cfg))
    intraday_preferred_interval = str(cfg.get("extended_hours", "preferred_interval", default="5m")).strip() or "5m"
    intraday_fallback_interval = str(cfg.get("extended_hours", "fallback_interval", default="1m")).strip() or "1m"
    intraday_retention_days = int(cfg.get("extended_hours", "retention_days", default=10))
    intraday_prepost = bool(cfg.get("extended_hours", "prepost", default=True))
    intraday_chunk_size = int(cfg.get("extended_hours", "chunk_size", default=20))
    intraday_sleep_seconds = float(cfg.get("extended_hours", "sleep_seconds", default=1.0))
    intraday_max_retries = int(cfg.get("extended_hours", "max_retries", default=5))
    intraday_backoff_max_seconds = float(cfg.get("extended_hours", "backoff_max_seconds", default=120.0))
    intraday_threads = bool(cfg.get("extended_hours", "threads", default=False))
    intraday_pct_move_threshold = float(cfg.get("extended_hours", "pct_move_threshold", default=2.0))
    intraday_min_volume = float(cfg.get("extended_hours", "min_volume", default=0.0))

    alert_dir = _run_dir(runs_root) / "monitor"
    alert_path = alert_dir / "extended_hours_alerts.csv"
    report_html_path = alert_dir / "extended_hours_report.html"
    res = update_extended_hours_store(
        symbols=symbols,
        intraday_root=intraday_root,
        daily_root=daily_root,
        preferred_interval=intraday_preferred_interval,
        fallback_interval=intraday_fallback_interval,
        retention_days=intraday_retention_days,
        prepost=intraday_prepost,
        pct_move_threshold=intraday_pct_move_threshold,
        min_volume=intraday_min_volume,
        alerts_path=alert_path,
        chunk_size=intraday_chunk_size,
        sleep_seconds=intraday_sleep_seconds,
        max_retries=intraday_max_retries,
        backoff_max_seconds=intraday_backoff_max_seconds,
        threads=intraday_threads,
        log_path=log_path,
    )
    moves_df = res.get("moves_df")
    alerts_df = res.get("alerts_df")
    report_path = persist_extended_hours_report_html(
        moves_df=moves_df,
        alerts_df=alerts_df,
        path=report_html_path,
        threshold=intraday_pct_move_threshold,
        top_n=max(10, int(top_n)),
        session_filter=session_filter,
    )
    top_moves = summarize_gap_report(
        moves_df=moves_df,
        threshold=intraday_pct_move_threshold,
        min_volume=intraday_min_volume,
        top_n=max(1, int(top_n)),
        session_filter=session_filter,
    )
    print(
        "[EXTENDED_HOURS] "
        f"preferred_written={res.get('preferred_written', 0)} "
        f"fallback_written={res.get('fallback_written', 0)} "
        f"alerts={res.get('alerts', 0)} "
        f"path={res.get('alerts_path', '')} "
        f"html={report_path}"
    )
    if top_moves is not None and not top_moves.is_empty():
        display_cols = [c for c in ["symbol", "pct_move", "ref_close", "last_price", "last_volume", "session", "last_ts"] if c in top_moves.columns]
        print("\nExtended-Hours Top Movers")
        print(top_moves.select(display_cols))
    res["report_html"] = str(report_path)
    return res


def update_from_config(cfg: Any, symbols_override: list[str] | None = None) -> dict[str, Any]:
    universe_csv = universe_csv_path(cfg)
    parquet_root = parquet_root_path(cfg)
    interval = cfg.get("timeframe", default="1d")
    lookback_days = int(cfg.get("lookback_days", default=2000))
    log_path = update_log_path(cfg)

    chunk_size = int(cfg.get("yf", "chunk_size", default=100))
    sleep_seconds = float(cfg.get("yf", "sleep_seconds", default=2.0))
    max_retries = int(cfg.get("yf", "max_retries", default=5))
    backoff_max_seconds = float(cfg.get("yf", "backoff_max_seconds", default=120))
    threads = bool(cfg.get("yf", "threads", default=False))
    history_provider = str(cfg.get("update", "history_provider", default="yfinance")).strip().lower()
    recent_provider = str(cfg.get("update", "recent_provider", default="yfinance")).strip().lower()
    recent_days = int(cfg.get("update", "recent_days", default=5))
    incremental_days = int(cfg.get("update", "incremental_days", default=60))
    assert_postwrite_integrity = bool(cfg.get("update", "assert_postwrite_integrity", default=True))
    stooq_refresh_all = bool(cfg.get("update", "stooq_refresh_all", default=False))
    runs_root = runs_root_path(cfg)
    intraday_enabled = bool(cfg.get("extended_hours", "enabled", default=True))
    intraday_root = str(intraday_root_path(cfg))
    intraday_preferred_interval = str(cfg.get("extended_hours", "preferred_interval", default="5m")).strip() or "5m"
    intraday_fallback_interval = str(cfg.get("extended_hours", "fallback_interval", default="1m")).strip() or "1m"
    intraday_retention_days = int(cfg.get("extended_hours", "retention_days", default=10))
    intraday_prepost = bool(cfg.get("extended_hours", "prepost", default=True))
    intraday_chunk_size = int(cfg.get("extended_hours", "chunk_size", default=20))
    intraday_sleep_seconds = float(cfg.get("extended_hours", "sleep_seconds", default=1.0))
    intraday_max_retries = int(cfg.get("extended_hours", "max_retries", default=5))
    intraday_backoff_max_seconds = float(cfg.get("extended_hours", "backoff_max_seconds", default=120.0))
    intraday_threads = bool(cfg.get("extended_hours", "threads", default=False))
    intraday_pct_move_threshold = float(cfg.get("extended_hours", "pct_move_threshold", default=2.0))
    intraday_min_volume = float(cfg.get("extended_hours", "min_volume", default=0.0))

    symbols = _load_active_symbols_from_cfg(cfg, symbols_override=symbols_override)
    _migrate_symbol_alias_parquet(symbols, parquet_root=parquet_root, intraday_root=intraday_root if intraday_enabled else None)
    print(f"Updating {len(symbols)} symbols into {parquet_root} ...")

    def _is_strict_single_symbol(sym: str) -> bool:
        up = sym.upper()
        return any(up.endswith(sfx) for sfx in STRICT_SINGLE_SYMBOL_SUFFIXES)

    root = Path(parquet_root)
    root.mkdir(parents=True, exist_ok=True)
    currency_cache: dict[str, str] = {}

    def _currency_from_df(df: pl.DataFrame | None) -> str | None:
        if df is None or df.is_empty() or "currency" not in df.columns:
            return None
        try:
            vals = df.select(pl.col("currency").cast(pl.Utf8, strict=False)).drop_nulls().get_column("currency").to_list()
            if vals:
                v = str(vals[0]).strip()
                return v.upper() if v else None
        except Exception:
            return None
        return None

    def _resolve_currency(sym: str, df_hint: pl.DataFrame | None = None) -> str:
        if sym in currency_cache:
            return currency_cache[sym]
        from_df = _currency_from_df(df_hint)
        if from_df:
            currency_cache[sym] = from_df
            return from_df
        fetched = fetch_symbol_currency(sym) or "UNKNOWN"
        currency_cache[sym] = fetched
        return fetched

    def _ensure_currency(df: pl.DataFrame | None, currency: str) -> pl.DataFrame | None:
        if df is None:
            return None
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
            "date": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "adj_close": pl.Float64,
            "volume": pl.Float64,
            "currency": pl.Utf8,
        }
        casts_left = []
        casts_right = []
        for c in cols:
            dt = dtype_map.get(c)
            if dt is not None:
                casts_left.append(pl.col(c).cast(dt, strict=False).alias(c))
                casts_right.append(pl.col(c).cast(dt, strict=False).alias(c))
        if casts_left:
            df_left = df_left.with_columns(casts_left)
        if casts_right:
            df_right = df_right.with_columns(casts_right)
        return df_left.select(cols), df_right.select(cols)

    def _sanitize_ohlc_df(df: pl.DataFrame | None) -> pl.DataFrame | None:
        if df is None or df.is_empty():
            return df
        req = {"date", "open", "high", "low", "close"}
        if not req.issubset(set(df.columns)):
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

    def _ohlc_quality_counts(df: pl.DataFrame | None) -> dict[str, int]:
        if df is None or df.is_empty():
            return {"null_ohlc": 0, "bad_ohlc": 0, "dup_dates": 0}
        req = {"date", "open", "high", "low", "close"}
        if not req.issubset(set(df.columns)):
            return {"null_ohlc": 1, "bad_ohlc": 1, "dup_dates": 1}
        null_ohlc = int(df.select(pl.any_horizontal([pl.col("open").is_null(), pl.col("high").is_null(), pl.col("low").is_null(), pl.col("close").is_null()]).sum()).item())
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

    def _assert_postwrite(path: Path, sym: str) -> None:
        if not assert_postwrite_integrity:
            return
        chk = read_parquet_if_exists(path)
        q = _ohlc_quality_counts(chk)
        if q["null_ohlc"] > 0 or q["bad_ohlc"] > 0 or q["dup_dates"] > 0:
            msg = f"postwrite_integrity_failed:null_ohlc={q['null_ohlc']},bad_ohlc={q['bad_ohlc']},dup_dates={q['dup_dates']}"
            append_update_log(log_path, sym, msg, 1)
            raise RuntimeError(f"{sym}: {msg}")

    def _scalar_eq(a: object, b: object, tol: float = 1e-12) -> bool:
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        try:
            return abs(float(a) - float(b)) <= tol
        except Exception:
            return str(a) == str(b)

    def _needs_incremental_write(df_old: pl.DataFrame, df_inc: pl.DataFrame) -> bool:
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
        try:
            old_last = df_old.filter(pl.col("date") == old_last_date).tail(1)
            inc_last = df_inc.filter(pl.col("date") == old_last_date).tail(1)
            if old_last.is_empty() or inc_last.is_empty():
                return False
            cols = [c for c in ["open", "high", "low", "close", "adj_close", "volume", "currency"] if c in old_last.columns and c in inc_last.columns]
            if not cols:
                return False
            for c in cols:
                if not _scalar_eq(old_last.get_column(c).to_list()[0], inc_last.get_column(c).to_list()[0]):
                    return True
            return False
        except Exception:
            return True

    def _write_extended_hours_artifacts(intraday_res: dict[str, Any], top_n: int = 25) -> str:
        report_html_path = _run_dir(runs_root) / "monitor" / "extended_hours_report.html"
        report_path = persist_extended_hours_report_html(
            moves_df=intraday_res.get("moves_df"),
            alerts_df=intraday_res.get("alerts_df"),
            path=report_html_path,
            threshold=intraday_pct_move_threshold,
            top_n=max(10, int(top_n)),
        )
        top_moves = summarize_gap_report(
            moves_df=intraday_res.get("moves_df"),
            threshold=intraday_pct_move_threshold,
            min_volume=intraday_min_volume,
            top_n=max(1, int(top_n)),
        )
        print("[EXTENDED_HOURS] " f"preferred_written={intraday_res.get('preferred_written', 0)} " f"fallback_written={intraday_res.get('fallback_written', 0)} " f"alerts={intraday_res.get('alerts', 0)} " f"path={intraday_res.get('alerts_path', '')} " f"html={report_path}")
        if top_moves is not None and not top_moves.is_empty():
            display_cols = [c for c in ["symbol", "pct_move", "ref_close", "last_price", "last_volume", "session", "last_ts"] if c in top_moves.columns]
            print("\nExtended-Hours Top Movers")
            print(top_moves.select(display_cols))
        return str(report_path)

    def _intraday_update() -> dict[str, Any] | None:
        if not intraday_enabled:
            return None
        alert_dir = _run_dir(runs_root) / "monitor"
        alert_path = alert_dir / "extended_hours_alerts.csv"
        try:
            intraday_res = update_extended_hours_store(
                symbols=symbols,
                intraday_root=intraday_root,
                daily_root=parquet_root,
                preferred_interval=intraday_preferred_interval,
                fallback_interval=intraday_fallback_interval,
                retention_days=intraday_retention_days,
                prepost=intraday_prepost,
                pct_move_threshold=intraday_pct_move_threshold,
                min_volume=intraday_min_volume,
                alerts_path=alert_path,
                chunk_size=intraday_chunk_size,
                sleep_seconds=intraday_sleep_seconds,
                max_retries=intraday_max_retries,
                backoff_max_seconds=intraday_backoff_max_seconds,
                threads=intraday_threads,
                log_path=log_path,
            )
            _write_extended_hours_artifacts(intraday_res)
            return intraday_res
        except Exception as e:
            append_update_log(log_path, "__extended_hours__", str(e), 1)
            print(f"[WARN] extended-hours update failed: {e}")
            return None

    missing = []
    existing = []
    for sym in symbols:
        path = root / f"{sym}.parquet"
        if path.exists():
            existing.append(sym)
        else:
            missing.append(sym)

    strict_symbols = [s for s in symbols if _is_strict_single_symbol(s)]
    strict_set = set(strict_symbols)
    missing_regular = [s for s in missing if s not in strict_set]
    existing_regular = [s for s in existing if s not in strict_set]

    if history_provider in {"stooq"}:
        stooq_targets = symbols if stooq_refresh_all else missing
        for sym in tqdm(stooq_targets):
            try:
                df_hist = fetch_stooq_history(StooqDownloadSpec(symbol=sym))
                if df_hist.is_empty():
                    append_update_log(log_path, sym, "stooq_empty_data", 1)
                    continue
                cur = infer_currency_from_symbol(sym)
                df_hist = _ensure_currency(df_hist, cur)
                out_path = root / f"{sym}.parquet"
                df_hist = _sanitize_ohlc_df(df_hist)
                if df_hist is None or df_hist.is_empty():
                    append_update_log(log_path, sym, "stooq_empty_after_sanitize", 1)
                    continue
                df_hist.write_parquet(str(out_path))
                _assert_postwrite(out_path, sym)
            except Exception as e:
                append_update_log(log_path, sym, f"stooq_error:{e}", 1)

        if recent_provider in {"yfinance", "yf"} and recent_days > 0:
            inc_map = fetch_yfinance_history_bulk(symbols, interval=interval, lookback_days=recent_days, chunk_size=chunk_size, sleep_seconds=sleep_seconds, max_retries=max_retries, backoff_max_seconds=backoff_max_seconds, threads=threads, log_path=log_path, show_progress=True, progress_desc="YF recent merge fetch (stooq mode)")
            for sym in tqdm(symbols):
                path = root / f"{sym}.parquet"
                try:
                    df_old = read_parquet_if_exists(path)
                    if df_old is None or df_old.is_empty():
                        continue
                    cur = _currency_from_df(df_old) or infer_currency_from_symbol(sym)
                    df_old = _ensure_currency(df_old, cur)
                    df_inc = inc_map.get(sym)
                    if df_inc is None or df_inc.is_empty():
                        continue
                    df_inc = _ensure_currency(df_inc, cur)
                    df_old, df_inc = _align_for_concat(df_old, df_inc)
                    combined = pl.concat([df_old, df_inc], how="vertical").unique(subset=["date"], keep="last").sort("date")
                    combined = _sanitize_ohlc_df(combined)
                    if combined is None or combined.is_empty():
                        append_update_log(log_path, sym, "stooq_yf_recent_empty_after_sanitize", 1)
                        continue
                    combined.write_parquet(str(path))
                    _assert_postwrite(path, sym)
                except Exception as e:
                    append_update_log(log_path, sym, f"stooq_yf_recent_error:{e}", 1)
        intraday_res = _intraday_update()
        print("Done.")
        return {"symbols": symbols, "parquet_root": str(parquet_root), "intraday": intraday_res}

    full_map = fetch_yfinance_history_bulk(missing_regular, interval=interval, lookback_days=lookback_days, chunk_size=1, sleep_seconds=sleep_seconds, max_retries=max_retries, backoff_max_seconds=backoff_max_seconds, threads=False, log_path=log_path, show_progress=True, progress_desc="YF full-history fetch (missing regular)")
    for sym in tqdm(missing_regular):
        df_new = full_map.get(sym)
        if df_new is None or df_new.is_empty():
            retry_map = fetch_yfinance_history_bulk([sym], interval=interval, lookback_days=lookback_days, chunk_size=1, sleep_seconds=sleep_seconds, max_retries=max_retries, backoff_max_seconds=backoff_max_seconds, threads=False, log_path=log_path)
            df_new = retry_map.get(sym)
        df_new = _sanitize_ohlc_df(df_new)
        if df_new is None or df_new.is_empty():
            append_update_log(log_path, sym, "empty_data", 1)
            continue
        cur = _resolve_currency(sym, df_new)
        df_new = _ensure_currency(df_new, cur)
        out_path = root / f"{sym}.parquet"
        df_new.write_parquet(str(out_path))
        _assert_postwrite(out_path, sym)

    inc_days = max(1, int(incremental_days))
    inc_map = fetch_yfinance_history_bulk(existing_regular, interval=interval, lookback_days=inc_days, chunk_size=chunk_size, sleep_seconds=sleep_seconds, max_retries=max_retries, backoff_max_seconds=backoff_max_seconds, threads=threads, log_path=log_path, show_progress=True, progress_desc="YF incremental fetch (existing regular)")
    skipped_unchanged = 0
    for sym in tqdm(existing_regular):
        path = root / f"{sym}.parquet"
        try:
            df_old = read_parquet_if_exists(path)
            cur = _resolve_currency(sym, df_old)
            df_old = _sanitize_ohlc_df(_ensure_currency(df_old, cur))
            df_inc = inc_map.get(sym)
            if df_inc is None or df_inc.is_empty():
                retry_map = fetch_yfinance_history_bulk([sym], interval=interval, lookback_days=inc_days, chunk_size=1, sleep_seconds=sleep_seconds, max_retries=max_retries, backoff_max_seconds=backoff_max_seconds, threads=False, log_path=log_path)
                df_inc = retry_map.get(sym)
            df_inc = _sanitize_ohlc_df(_ensure_currency(df_inc, cur))
            if df_inc is None or df_inc.is_empty():
                append_update_log(log_path, sym, "empty_incremental", 1)
                continue
            if df_old is None or df_old.is_empty():
                df_inc.write_parquet(str(path))
                _assert_postwrite(path, sym)
                continue
            if not _needs_incremental_write(df_old, df_inc):
                skipped_unchanged += 1
                continue
            df_old, df_inc = _align_for_concat(df_old, df_inc)
            combined = pl.concat([df_old, df_inc], how="vertical").unique(subset=["date"], keep="last").sort("date")
            combined = _sanitize_ohlc_df(combined)
            if combined is None or combined.is_empty():
                append_update_log(log_path, sym, "empty_combined_after_sanitize", 1)
                continue
            combined.write_parquet(str(path))
            _assert_postwrite(path, sym)
        except Exception as e:
            append_update_log(log_path, sym, str(e), 1)
    if skipped_unchanged > 0:
        print(f"[UPDATE] skipped unchanged existing symbols: {skipped_unchanged}/{len(existing_regular)}")

    if strict_symbols:
        strict_map = fetch_yfinance_history_bulk(strict_symbols, interval=interval, lookback_days=lookback_days, chunk_size=1, sleep_seconds=sleep_seconds, max_retries=max_retries, backoff_max_seconds=backoff_max_seconds, threads=False, log_path=log_path, show_progress=True, progress_desc="YF strict full-history fetch")
        for sym in tqdm(strict_symbols):
            try:
                df_new = strict_map.get(sym)
                if df_new is None or df_new.is_empty():
                    retry_map = fetch_yfinance_history_bulk([sym], interval=interval, lookback_days=lookback_days, chunk_size=1, sleep_seconds=sleep_seconds, max_retries=max_retries, backoff_max_seconds=backoff_max_seconds, threads=False, log_path=log_path)
                    df_new = retry_map.get(sym)
                df_new = _sanitize_ohlc_df(df_new)
                if df_new is None or df_new.is_empty():
                    append_update_log(log_path, sym, "empty_data_strict", 1)
                    continue
                cur = _resolve_currency(sym, df_new)
                df_new = _ensure_currency(df_new, cur)
                out_path = root / f"{sym}.parquet"
                df_new.sort("date").write_parquet(str(out_path))
                _assert_postwrite(out_path, sym)
            except Exception as e:
                append_update_log(log_path, sym, str(e), 1)

    intraday_res = _intraday_update()
    print("Done.")
    return {"symbols": symbols, "parquet_root": str(parquet_root), "intraday": intraday_res}
