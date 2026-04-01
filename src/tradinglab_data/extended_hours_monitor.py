from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl

from . import _alert_report, _intraday_fetch, _move_compute
from ._ohlc_utils import align_for_concat, ensure_currency, needs_incremental_write
from ._yf_utils import coerce_standard_schema
from .contracts import ExtendedHoursResult
from .data_yf import fetch_symbol_currency, read_parquet_if_exists

persist_alerts = _alert_report.persist_alerts
persist_extended_hours_report_html = _alert_report.persist_extended_hours_report_html
render_extended_hours_report_html = _alert_report.render_extended_hours_report_html

INTRADAY_SCHEMA = _intraday_fetch.INTRADAY_SCHEMA
MAX_PERIOD_BY_INTERVAL = _intraday_fetch.MAX_PERIOD_BY_INTERVAL
UPDATE_PERIOD_BY_INTERVAL = _intraday_fetch.UPDATE_PERIOD_BY_INTERVAL
fetch_extended_intraday = _intraday_fetch.fetch_extended_intraday
period_for_interval = _intraday_fetch.period_for_interval
sanitize_intraday_df = _intraday_fetch.sanitize_intraday_df
trim_rolling_window = _intraday_fetch.trim_rolling_window

compute_moves_vs_close = _move_compute.compute_moves_vs_close
detect_alerts = _move_compute.detect_alerts
load_daily_reference_closes = _move_compute.load_daily_reference_closes
summarize_gap_report = _move_compute.summarize_gap_report

# Private compat alias used by repo-maintenance scripts.
_sanitize_intraday_df = sanitize_intraday_df


def _update_intraday_interval(
    target_symbols: list[str],
    interval: str,
    period: str,
    out_dir: Path,
    *,
    retention_days: int,
    prepost: bool,
    chunk_size: int,
    sleep_seconds: float,
    max_retries: int,
    backoff_max_seconds: float,
    threads: bool,
    log_path: Path | None,
    fetch_intraday_fn=None,
    read_frame_fn=None,
    fetch_currency_fn=None,
) -> list[str]:
    if fetch_intraday_fn is None:
        fetch_intraday_fn = fetch_extended_intraday
    if read_frame_fn is None:
        read_frame_fn = read_parquet_if_exists
    if fetch_currency_fn is None:
        fetch_currency_fn = fetch_symbol_currency
    fetched = fetch_intraday_fn(
        symbols=target_symbols,
        interval=interval,
        period=period,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    resolved: list[str] = []
    for sym in target_symbols:
        df_new = fetched.get(sym)
        path = out_dir / f"{sym}.parquet"
        df_old = read_frame_fn(path)
        cur = fetch_currency_fn(sym) or "UNKNOWN"
        if df_new is None or df_new.is_empty():
            if df_old is None or df_old.is_empty():
                continue
            df_old_raw = coerce_standard_schema(df_old)
            df_old_prepared = ensure_currency(df_old_raw, cur, postprocess=sanitize_intraday_df)
            df_old_clean = trim_rolling_window(
                df_old_prepared if df_old_prepared is not None else pl.DataFrame(schema=INTRADAY_SCHEMA),
                retention_days=retention_days,
            )
            if df_old_clean.height != df_old_raw.height:
                df_old_clean.write_parquet(str(path))
                resolved.append(sym)
            continue
        df_new_prepared = ensure_currency(df_new, cur, postprocess=sanitize_intraday_df)
        df_new = trim_rolling_window(
            df_new_prepared if df_new_prepared is not None else pl.DataFrame(schema=INTRADAY_SCHEMA),
            retention_days=retention_days,
        )
        if df_new.is_empty():
            continue
        if df_old is None or df_old.is_empty():
            df_new.write_parquet(str(path))
            resolved.append(sym)
            continue
        df_old_raw = coerce_standard_schema(df_old)
        old_rows_before = df_old_raw.height
        df_old_prepared = ensure_currency(df_old_raw, cur, postprocess=sanitize_intraday_df)
        df_old = trim_rolling_window(
            df_old_prepared if df_old_prepared is not None else pl.DataFrame(schema=INTRADAY_SCHEMA),
            retention_days=retention_days,
        )
        old_sanitized = df_old.height != old_rows_before
        df_old, df_new = align_for_concat(
            df_old,
            df_new,
            schema=INTRADAY_SCHEMA,
            postprocess=sanitize_intraday_df,
        )
        if not old_sanitized and not needs_incremental_write(df_old, df_new):
            resolved.append(sym)
            continue
        combined = (
            pl.concat([df_old, df_new], how="vertical")
            .unique(subset=["date"], keep="last")
            .sort("date")
        )
        combined = trim_rolling_window(combined, retention_days=retention_days)
        combined.write_parquet(str(path))
        resolved.append(sym)
    return resolved


def update_extended_hours_store(
    symbols: list[str],
    intraday_root: str | Path,
    daily_root: str | Path,
    preferred_interval: str = "5m",
    fallback_interval: str = "1m",
    retention_days: int = 10,
    prepost: bool = True,
    pct_move_threshold: float = 2.0,
    min_volume: float = 0.0,
    alerts_path: str | Path | None = None,
    chunk_size: int = 20,
    sleep_seconds: float = 1.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
) -> ExtendedHoursResult:
    root = Path(intraday_root)
    pref_dir = root / preferred_interval
    fb_dir = root / fallback_interval
    pref_dir.mkdir(parents=True, exist_ok=True)
    fb_dir.mkdir(parents=True, exist_ok=True)

    pref_missing: list[str] = []
    pref_existing: list[str] = []
    for sym in symbols:
        if (pref_dir / f"{sym}.parquet").exists():
            pref_existing.append(sym)
        else:
            pref_missing.append(sym)

    pref_resolved_missing = _update_intraday_interval(
        pref_missing,
        preferred_interval,
        period_for_interval(preferred_interval, MAX_PERIOD_BY_INTERVAL, purpose="initial fetch"),
        pref_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    pref_resolved_existing = _update_intraday_interval(
        pref_existing,
        preferred_interval,
        period_for_interval(preferred_interval, UPDATE_PERIOD_BY_INTERVAL, purpose="incremental fetch"),
        pref_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    unresolved = [s for s in symbols if s not in set(pref_resolved_missing) | set(pref_resolved_existing)]

    fb_missing: list[str] = []
    fb_existing: list[str] = []
    for sym in unresolved:
        if (fb_dir / f"{sym}.parquet").exists():
            fb_existing.append(sym)
        else:
            fb_missing.append(sym)
    fb_resolved_missing = _update_intraday_interval(
        fb_missing,
        fallback_interval,
        period_for_interval(fallback_interval, MAX_PERIOD_BY_INTERVAL, purpose="initial fetch"),
        fb_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    fb_resolved_existing = _update_intraday_interval(
        fb_existing,
        fallback_interval,
        period_for_interval(fallback_interval, UPDATE_PERIOD_BY_INTERVAL, purpose="incremental fetch"),
        fb_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )

    latest_frames: dict[str, pl.DataFrame] = {}
    for sym in symbols:
        candidates: list[tuple[datetime | None, str, pl.DataFrame]] = []
        for interval, directory in ((preferred_interval, pref_dir), (fallback_interval, fb_dir)):
            path = directory / f"{sym}.parquet"
            df = read_parquet_if_exists(path)
            if df is None or df.is_empty():
                continue
            try:
                last_ts = df.select(pl.col("date").max()).item()
            except Exception:
                last_ts = None
            candidates.append((last_ts, interval, df.with_columns(pl.lit(interval).alias("interval"))))
        if not candidates:
            continue
        candidates.sort(key=lambda item: (item[0] is not None, item[0], item[1] == preferred_interval), reverse=True)
        latest_frames[sym] = candidates[0][2]

    daily_close_map = load_daily_reference_closes(symbols, daily_root=daily_root)
    moves_df = compute_moves_vs_close(latest_frames, daily_close_map)
    alerts = detect_alerts(moves_df, threshold=pct_move_threshold, min_volume=min_volume)

    alert_file = None
    if alerts_path is not None:
        alert_file = persist_alerts(alerts, alerts_path)

    return {
        "preferred_interval": preferred_interval,
        "fallback_interval": fallback_interval,
        "symbols": len(symbols),
        "preferred_written": len(set(pref_resolved_missing) | set(pref_resolved_existing)),
        "fallback_written": len(set(fb_resolved_missing) | set(fb_resolved_existing)),
        "alerts": alerts.height if alerts is not None and not alerts.is_empty() else 0,
        "alerts_path": str(alert_file) if alert_file is not None else "",
        "moves_df": moves_df,
        "alerts_df": alerts,
    }
