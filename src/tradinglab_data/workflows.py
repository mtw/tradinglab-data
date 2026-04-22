from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl
from tqdm import tqdm

from ._ohlc_utils import (
    DAILY_SCHEMA_COLUMNS,
    align_for_concat,
    assert_postwrite_integrity,
    currency_from_df,
    ensure_currency,
    needs_incremental_write,
    resolve_currency,
    sanitize_ohlc_df,
)
from .config import (
    ConfigLike,
    intraday_root_path,
    parquet_root_path,
    runs_root_path,
    ticker_overrides_path,
    universe_csv_path,
    universe_dir_path,
    update_log_path,
    update_warning_state_path,
)
from .contracts import ExtendedHoursResult, MonitorExtendedHoursResult, UpdateResult
from .data_stooq import StooqDownloadSpec, fetch_stooq_history, infer_currency_from_symbol
from .data_yf import append_update_log, fetch_symbol_currency, fetch_yfinance_history_bulk, read_parquet_if_exists
from .extended_hours_monitor import (
    backfill_intraday_interval_store,
    persist_extended_hours_report_html,
    summarize_gap_report,
    update_extended_hours_store,
)
from .schema import DAILY_PARQUET_SCHEMA
from .universe import canonicalize_symbol, load_ticker_overrides, load_universe_frame

STRICT_SINGLE_SYMBOL_SUFFIXES = (".VI",)


@dataclass(frozen=True)
class _IntradayConfig:
    enabled: bool
    root: str
    preferred_interval: str
    fallback_interval: str
    retention_days: int
    prepost: bool
    chunk_size: int
    sleep_seconds: float
    max_retries: int
    backoff_max_seconds: float
    threads: bool
    warning_state_path: Path
    log_repeat_cooldown_hours: float
    pct_move_threshold: float
    min_volume: float


@dataclass(frozen=True)
class _UpdateConfig:
    parquet_root: str | Path
    interval: str
    lookback_days: int
    log_path: Path
    chunk_size: int
    sleep_seconds: float
    max_retries: int
    backoff_max_seconds: float
    threads: bool
    history_provider: str
    recent_provider: str
    recent_days: int
    incremental_days: int
    postwrite_integrity_enabled: bool
    stooq_refresh_all: bool
    runs_root: str | Path
    intraday: _IntradayConfig


def _run_dir(runs_root: str | Path) -> Path:
    d = datetime.now().strftime("%Y-%m-%d")
    p = Path(runs_root) / d
    p.mkdir(parents=True, exist_ok=True)
    return p


def _is_strict_single_symbol(sym: str) -> bool:
    up = sym.upper()
    return any(up.endswith(sfx) for sfx in STRICT_SINGLE_SYMBOL_SUFFIXES)


def _read_intraday_config(cfg: ConfigLike) -> _IntradayConfig:
    return _IntradayConfig(
        enabled=bool(cfg.get("extended_hours", "enabled", default=True)),
        root=str(intraday_root_path(cfg)),
        preferred_interval=str(cfg.get("extended_hours", "preferred_interval", default="5m")).strip() or "5m",
        fallback_interval=str(cfg.get("extended_hours", "fallback_interval", default="1m")).strip() or "1m",
        retention_days=int(cfg.get("extended_hours", "retention_days", default=0)),
        prepost=bool(cfg.get("extended_hours", "prepost", default=True)),
        chunk_size=int(cfg.get("extended_hours", "chunk_size", default=20)),
        sleep_seconds=float(cfg.get("extended_hours", "sleep_seconds", default=1.0)),
        max_retries=int(cfg.get("extended_hours", "max_retries", default=5)),
        backoff_max_seconds=float(cfg.get("extended_hours", "backoff_max_seconds", default=120.0)),
        threads=bool(cfg.get("extended_hours", "threads", default=False)),
        warning_state_path=update_warning_state_path(cfg),
        log_repeat_cooldown_hours=float(cfg.get("extended_hours", "log_repeat_cooldown_hours", default=24.0)),
        pct_move_threshold=float(cfg.get("extended_hours", "pct_move_threshold", default=2.0)),
        min_volume=float(cfg.get("extended_hours", "min_volume", default=0.0)),
    )


def _read_update_config(cfg: ConfigLike) -> _UpdateConfig:
    return _UpdateConfig(
        parquet_root=parquet_root_path(cfg),
        interval=cfg.get("timeframe", default="1d"),
        lookback_days=int(cfg.get("lookback_days", default=2000)),
        log_path=update_log_path(cfg),
        chunk_size=int(cfg.get("yf", "chunk_size", default=100)),
        sleep_seconds=float(cfg.get("yf", "sleep_seconds", default=2.0)),
        max_retries=int(cfg.get("yf", "max_retries", default=5)),
        backoff_max_seconds=float(cfg.get("yf", "backoff_max_seconds", default=120)),
        threads=bool(cfg.get("yf", "threads", default=False)),
        history_provider=str(cfg.get("update", "history_provider", default="yfinance")).strip().lower(),
        recent_provider=str(cfg.get("update", "recent_provider", default="yfinance")).strip().lower(),
        recent_days=int(cfg.get("update", "recent_days", default=5)),
        incremental_days=int(cfg.get("update", "incremental_days", default=60)),
        postwrite_integrity_enabled=bool(cfg.get("update", "assert_postwrite_integrity", default=True)),
        stooq_refresh_all=bool(cfg.get("update", "stooq_refresh_all", default=False)),
        runs_root=runs_root_path(cfg),
        intraday=_read_intraday_config(cfg),
    )


def _load_active_symbols_from_cfg(cfg: ConfigLike, symbols_override: list[str] | None = None) -> list[str]:
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


def _write_extended_hours_artifacts(
    intraday_res: ExtendedHoursResult,
    *,
    runs_root: str | Path,
    threshold: float,
    min_volume: float,
    top_n: int = 25,
    session_filter: str = "all",
) -> str:
    report_html_path = _run_dir(runs_root) / "monitor" / "extended_hours_report.html"
    report_path = persist_extended_hours_report_html(
        moves_df=intraday_res.get("moves_df"),
        alerts_df=intraday_res.get("alerts_df"),
        path=report_html_path,
        threshold=threshold,
        top_n=max(10, int(top_n)),
        session_filter=session_filter,
    )
    top_moves = summarize_gap_report(
        moves_df=intraday_res.get("moves_df"),
        threshold=threshold,
        min_volume=min_volume,
        top_n=max(1, int(top_n)),
        session_filter=session_filter,
    )
    print(
        "[EXTENDED_HOURS] "
        f"preferred_written={intraday_res.get('preferred_written', 0)} "
        f"fallback_written={intraday_res.get('fallback_written', 0)} "
        f"alerts={intraday_res.get('alerts', 0)} "
        f"path={intraday_res.get('alerts_path', '')} "
        f"html={report_path}"
    )
    if top_moves is not None and not top_moves.is_empty():
        display_cols = [
            c
            for c in ["symbol", "pct_move", "ref_close", "last_price", "last_volume", "session", "last_ts"]
            if c in top_moves.columns
        ]
        print("\nExtended-Hours Top Movers")
        print(top_moves.select(display_cols))
    return str(report_path)


def _execute_intraday_update(
    *,
    symbols: list[str],
    runs_root: str | Path,
    parquet_root: str | Path,
    intraday_cfg: _IntradayConfig,
    log_path: Path,
    top_n: int = 25,
    session_filter: str = "all",
) -> tuple[ExtendedHoursResult, str]:
    alert_dir = _run_dir(runs_root) / "monitor"
    alert_path = alert_dir / "extended_hours_alerts.csv"
    intraday_res = update_extended_hours_store(
        symbols=symbols,
        intraday_root=intraday_cfg.root,
        daily_root=parquet_root,
        preferred_interval=intraday_cfg.preferred_interval,
        fallback_interval=intraday_cfg.fallback_interval,
        retention_days=intraday_cfg.retention_days,
        prepost=intraday_cfg.prepost,
        pct_move_threshold=intraday_cfg.pct_move_threshold,
        min_volume=intraday_cfg.min_volume,
        alerts_path=alert_path,
        chunk_size=intraday_cfg.chunk_size,
        sleep_seconds=intraday_cfg.sleep_seconds,
        max_retries=intraday_cfg.max_retries,
        backoff_max_seconds=intraday_cfg.backoff_max_seconds,
        threads=intraday_cfg.threads,
        log_repeat_cooldown_hours=intraday_cfg.log_repeat_cooldown_hours,
        log_path=log_path,
        warning_state_path=intraday_cfg.warning_state_path,
    )
    report_path = _write_extended_hours_artifacts(
        intraday_res,
        runs_root=runs_root,
        threshold=intraday_cfg.pct_move_threshold,
        min_volume=intraday_cfg.min_volume,
        top_n=top_n,
        session_filter=session_filter,
    )
    return intraday_res, report_path


def _run_intraday_update(
    *,
    symbols: list[str],
    runs_root: str | Path,
    parquet_root: str | Path,
    intraday_cfg: _IntradayConfig,
    log_path: Path,
) -> ExtendedHoursResult | None:
    if not intraday_cfg.enabled:
        return None
    try:
        intraday_res, _ = _execute_intraday_update(
            symbols=symbols,
            runs_root=runs_root,
            parquet_root=parquet_root,
            intraday_cfg=intraday_cfg,
            log_path=log_path,
        )
        return intraday_res
    except Exception as e:
        append_update_log(log_path, "__extended_hours__", str(e), 1)
        print(f"[WARN] extended-hours update failed: {e}")
        return None


def monitor_extended_hours_from_config(
    cfg: ConfigLike,
    symbols_override: list[str] | None = None,
    top_n: int = 25,
    session_filter: str = "all",
) -> MonitorExtendedHoursResult:
    symbols = _load_active_symbols_from_cfg(cfg, symbols_override=symbols_override)
    daily_root = parquet_root_path(cfg)
    runs_root = runs_root_path(cfg)
    log_path = update_log_path(cfg)
    intraday_cfg = _read_intraday_config(cfg)
    res, report_path = _execute_intraday_update(
        symbols=symbols,
        runs_root=runs_root,
        parquet_root=daily_root,
        intraday_cfg=intraday_cfg,
        log_path=log_path,
        top_n=top_n,
        session_filter=session_filter,
    )
    return {**res, "report_html": str(report_path)}


def backfill_extended_hours_from_config(
    cfg: ConfigLike,
    *,
    interval: str,
    symbols_override: list[str] | None = None,
) -> dict[str, object]:
    symbols = _load_active_symbols_from_cfg(cfg, symbols_override=symbols_override)
    intraday_cfg = _read_intraday_config(cfg)
    return backfill_intraday_interval_store(
        symbols=symbols,
        intraday_root=intraday_cfg.root,
        interval=interval,
        retention_days=intraday_cfg.retention_days,
        prepost=intraday_cfg.prepost,
        chunk_size=intraday_cfg.chunk_size,
        sleep_seconds=intraday_cfg.sleep_seconds,
        max_retries=intraday_cfg.max_retries,
        backoff_max_seconds=intraday_cfg.backoff_max_seconds,
        threads=intraday_cfg.threads,
        log_repeat_cooldown_hours=intraday_cfg.log_repeat_cooldown_hours,
        log_path=update_log_path(cfg),
        warning_state_path=intraday_cfg.warning_state_path,
    )


def update_from_config(cfg: ConfigLike, symbols_override: list[str] | None = None) -> UpdateResult:
    update_cfg = _read_update_config(cfg)
    symbols = _load_active_symbols_from_cfg(cfg, symbols_override=symbols_override)
    _migrate_symbol_alias_parquet(
        symbols,
        parquet_root=update_cfg.parquet_root,
        intraday_root=update_cfg.intraday.root if update_cfg.intraday.enabled else None,
    )
    print(f"Updating {len(symbols)} symbols into {update_cfg.parquet_root} ...")
    if update_cfg.history_provider in {"stooq"}:
        intraday_res = _run_stooq_update(update_cfg, symbols)
    else:
        intraday_res = _run_yfinance_update(update_cfg, symbols)
    print("Done.")
    return {"symbols": symbols, "parquet_root": str(update_cfg.parquet_root), "intraday": intraday_res}


def _run_stooq_update(update_cfg: _UpdateConfig, symbols: list[str]) -> ExtendedHoursResult | None:
    root = Path(update_cfg.parquet_root)
    root.mkdir(parents=True, exist_ok=True)
    existing = [sym for sym in symbols if (root / f"{sym}.parquet").exists()]
    missing = [sym for sym in symbols if sym not in existing]

    stooq_targets = symbols if update_cfg.stooq_refresh_all else missing
    for sym in tqdm(stooq_targets):
        try:
            fetched_hist = fetch_stooq_history(StooqDownloadSpec(symbol=sym))
            if fetched_hist.is_empty():
                append_update_log(update_cfg.log_path, sym, "stooq_empty_data", 1)
                continue
            cur = infer_currency_from_symbol(sym)
            df_hist = ensure_currency(fetched_hist, cur)
            out_path = root / f"{sym}.parquet"
            _upsert_symbol_parquet(
                out_path,
                sym,
                df_hist,
                currency=cur,
                cfg=update_cfg,
                empty_reason="stooq_empty_after_sanitize",
            )
        except Exception as e:
            append_update_log(update_cfg.log_path, sym, f"stooq_error:{e}", 1)

    if update_cfg.recent_provider in {"yfinance", "yf"} and update_cfg.recent_days > 0:
        inc_map = fetch_yfinance_history_bulk(
            symbols,
            interval=update_cfg.interval,
            lookback_days=update_cfg.recent_days,
            chunk_size=update_cfg.chunk_size,
            sleep_seconds=update_cfg.sleep_seconds,
            max_retries=update_cfg.max_retries,
            backoff_max_seconds=update_cfg.backoff_max_seconds,
            threads=update_cfg.threads,
            log_path=update_cfg.log_path,
            show_progress=True,
            progress_desc="YF recent merge fetch (stooq mode)",
        )
        for sym in tqdm(symbols):
            path = root / f"{sym}.parquet"
            try:
                df_old = read_parquet_if_exists(path)
                if df_old is None or df_old.is_empty():
                    continue
                cur = currency_from_df(df_old) or infer_currency_from_symbol(sym)
                df_old = _prepare_history_frame(df_old, cur)
                df_inc = inc_map.get(sym)
                if df_inc is None or df_inc.is_empty():
                    continue
                df_inc = _prepare_history_frame(df_inc, cur)
                if df_old is None or df_inc is None:
                    continue
                df_old, df_inc = align_for_concat(
                    df_old,
                    df_inc,
                    schema=DAILY_PARQUET_SCHEMA,
                    preferred_columns=DAILY_SCHEMA_COLUMNS,
                )
                combined = (
                    pl.concat([df_old, df_inc], how="vertical")
                    .unique(subset=["date"], keep="last")
                    .sort("date")
                )
                sanitized_combined = sanitize_ohlc_df(combined)
                if sanitized_combined is None or sanitized_combined.is_empty():
                    append_update_log(update_cfg.log_path, sym, "stooq_yf_recent_empty_after_sanitize", 1)
                    continue
                sanitized_combined.write_parquet(str(path))
                assert_postwrite_integrity(
                    path,
                    sym,
                    enabled=update_cfg.postwrite_integrity_enabled,
                    read_frame=read_parquet_if_exists,
                    append_log=append_update_log,
                    log_path=update_cfg.log_path,
                )
            except Exception as e:
                append_update_log(update_cfg.log_path, sym, f"stooq_yf_recent_error:{e}", 1)

    intraday_res = _run_intraday_update(
        symbols=symbols,
        runs_root=update_cfg.runs_root,
        parquet_root=update_cfg.parquet_root,
        intraday_cfg=update_cfg.intraday,
        log_path=update_cfg.log_path,
    )
    return intraday_res


def _run_yfinance_update(update_cfg: _UpdateConfig, symbols: list[str]) -> ExtendedHoursResult | None:
    root = Path(update_cfg.parquet_root)
    root.mkdir(parents=True, exist_ok=True)
    currency_cache: dict[str, str] = {}

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

    _fetch_and_write_new_symbols(
        missing_regular,
        root=root,
        update_cfg=update_cfg,
        currency_cache=currency_cache,
    )
    skipped_unchanged = _merge_incremental_symbols(
        existing_regular,
        root=root,
        update_cfg=update_cfg,
        currency_cache=currency_cache,
    )
    if skipped_unchanged > 0:
        print(f"[UPDATE] skipped unchanged existing symbols: {skipped_unchanged}/{len(existing_regular)}")
    _fetch_and_write_strict_symbols(
        strict_symbols,
        root=root,
        update_cfg=update_cfg,
        currency_cache=currency_cache,
    )

    intraday_res = _run_intraday_update(
        symbols=symbols,
        runs_root=update_cfg.runs_root,
        parquet_root=update_cfg.parquet_root,
        intraday_cfg=update_cfg.intraday,
        log_path=update_cfg.log_path,
    )
    return intraday_res


def _fetch_and_write_new_symbols(
    symbols: list[str],
    *,
    root: Path,
    update_cfg: _UpdateConfig,
    currency_cache: dict[str, str],
) -> None:
    full_map = _bulk_fetch_with_retry(
        symbols,
        cfg=update_cfg,
        lookback_days=update_cfg.lookback_days,
        chunk_size=1,
        threads=False,
        show_progress=True,
        progress_desc="YF full-history fetch (missing regular)",
    )
    for sym in tqdm(symbols):
        df_new = full_map.get(sym)
        cur = resolve_currency(sym, fetch_symbol_currency, df_hint=df_new, cache=currency_cache)
        out_path = root / f"{sym}.parquet"
        _write_symbol_parquet(
            out_path,
            sym,
            df_new,
            currency=cur,
            cfg=update_cfg,
            empty_reason="empty_data",
        )


def _merge_incremental_symbols(
    symbols: list[str],
    *,
    root: Path,
    update_cfg: _UpdateConfig,
    currency_cache: dict[str, str],
) -> int:
    inc_days = max(1, int(update_cfg.incremental_days))
    inc_map = _bulk_fetch_with_retry(
        symbols,
        cfg=update_cfg,
        lookback_days=inc_days,
        chunk_size=update_cfg.chunk_size,
        threads=update_cfg.threads,
        show_progress=True,
        progress_desc="YF incremental fetch (existing regular)",
    )
    skipped_unchanged = 0
    for sym in tqdm(symbols):
        path = root / f"{sym}.parquet"
        try:
            df_old = read_parquet_if_exists(path)
            cur = resolve_currency(sym, fetch_symbol_currency, df_hint=df_old, cache=currency_cache)
            df_old = _prepare_history_frame(df_old, cur)
            df_inc = _prepare_history_frame(inc_map.get(sym), cur)
            if df_inc is None:
                append_update_log(update_cfg.log_path, sym, "empty_incremental", 1)
                continue
            if df_old is None:
                _write_symbol_parquet(
                    path,
                    sym,
                    df_inc,
                    currency=cur,
                    cfg=update_cfg,
                    empty_reason="empty_incremental",
                )
                continue
            if not needs_incremental_write(df_old, df_inc):
                skipped_unchanged += 1
                continue
            df_old, df_inc = align_for_concat(
                df_old,
                df_inc,
                schema=DAILY_PARQUET_SCHEMA,
                preferred_columns=DAILY_SCHEMA_COLUMNS,
            )
            combined = (
                pl.concat([df_old, df_inc], how="vertical")
                .unique(subset=["date"], keep="last")
                .sort("date")
            )
            _write_symbol_parquet(
                path,
                sym,
                combined,
                currency=cur,
                cfg=update_cfg,
                empty_reason="empty_combined_after_sanitize",
            )
        except Exception as e:
            append_update_log(update_cfg.log_path, sym, str(e), 1)
    return skipped_unchanged


def _fetch_and_write_strict_symbols(
    symbols: list[str],
    *,
    root: Path,
    update_cfg: _UpdateConfig,
    currency_cache: dict[str, str],
) -> None:
    if not symbols:
        return
    strict_map = _bulk_fetch_with_retry(
        symbols,
        cfg=update_cfg,
        lookback_days=update_cfg.lookback_days,
        chunk_size=1,
        threads=False,
        show_progress=True,
        progress_desc="YF strict full-history fetch",
    )
    for sym in tqdm(symbols):
        try:
            df_new = strict_map.get(sym)
            cur = resolve_currency(sym, fetch_symbol_currency, df_hint=df_new, cache=currency_cache)
            out_path = root / f"{sym}.parquet"
            _upsert_symbol_parquet(
                out_path,
                sym,
                df_new,
                currency=cur,
                cfg=update_cfg,
                empty_reason="empty_data_strict",
                sort_before_write=True,
            )
        except Exception as e:
            append_update_log(update_cfg.log_path, sym, str(e), 1)


def _bulk_fetch_with_retry(
    symbols: list[str],
    *,
    cfg: _UpdateConfig,
    lookback_days: int,
    chunk_size: int,
    threads: bool,
    show_progress: bool = False,
    progress_desc: str = "yfinance bulk",
) -> dict[str, pl.DataFrame]:
    if not symbols:
        return {}
    result = fetch_yfinance_history_bulk(
        symbols,
        interval=cfg.interval,
        lookback_days=lookback_days,
        chunk_size=chunk_size,
        sleep_seconds=cfg.sleep_seconds,
        max_retries=cfg.max_retries,
        backoff_max_seconds=cfg.backoff_max_seconds,
        threads=threads,
        log_path=cfg.log_path,
        show_progress=show_progress,
        progress_desc=progress_desc,
    )
    misses = [sym for sym in symbols if sym not in result or result[sym] is None or result[sym].is_empty()]
    for sym in misses:
        single = fetch_yfinance_history_bulk(
            [sym],
            interval=cfg.interval,
            lookback_days=lookback_days,
            chunk_size=1,
            sleep_seconds=cfg.sleep_seconds,
            max_retries=cfg.max_retries,
            backoff_max_seconds=cfg.backoff_max_seconds,
            threads=False,
            log_path=cfg.log_path,
        )
        df_one = single.get(sym)
        if df_one is not None and not df_one.is_empty():
            result[sym] = df_one
    return result


def _prepare_history_frame(df: pl.DataFrame | None, currency: str) -> pl.DataFrame | None:
    prepared = sanitize_ohlc_df(ensure_currency(df, currency))
    if prepared is None or prepared.is_empty():
        return None
    return prepared


def _write_symbol_parquet(
    path: Path,
    symbol: str,
    df: pl.DataFrame | None,
    *,
    currency: str,
    cfg: _UpdateConfig,
    empty_reason: str,
    sort_before_write: bool = False,
) -> bool:
    prepared = _prepare_history_frame(df, currency)
    if prepared is None:
        append_update_log(cfg.log_path, symbol, empty_reason, 1)
        return False
    if sort_before_write:
        prepared = prepared.sort("date")
    prepared.write_parquet(str(path))
    assert_postwrite_integrity(
        path,
        symbol,
        enabled=cfg.postwrite_integrity_enabled,
        read_frame=read_parquet_if_exists,
        append_log=append_update_log,
        log_path=cfg.log_path,
    )
    return True


def _upsert_symbol_parquet(
    path: Path,
    symbol: str,
    df: pl.DataFrame | None,
    *,
    currency: str,
    cfg: _UpdateConfig,
    empty_reason: str,
    sort_before_write: bool = False,
) -> bool:
    prepared = _prepare_history_frame(df, currency)
    if prepared is None:
        append_update_log(cfg.log_path, symbol, empty_reason, 1)
        return False
    existing = read_parquet_if_exists(path)
    existing_prepared = _prepare_history_frame(existing, currency)
    if existing_prepared is not None:
        existing_prepared, prepared = align_for_concat(
            existing_prepared,
            prepared,
            schema=DAILY_PARQUET_SCHEMA,
            preferred_columns=DAILY_SCHEMA_COLUMNS,
        )
        prepared = (
            pl.concat([existing_prepared, prepared], how="vertical")
            .unique(subset=["date"], keep="last")
            .sort("date")
        )
    elif sort_before_write:
        prepared = prepared.sort("date")
    path.parent.mkdir(parents=True, exist_ok=True)
    prepared.write_parquet(str(path))
    assert_postwrite_integrity(
        path,
        symbol,
        enabled=cfg.postwrite_integrity_enabled,
        read_frame=read_parquet_if_exists,
        append_log=append_update_log,
        log_path=cfg.log_path,
    )
    return True
