from __future__ import annotations

import csv
import json
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock

import polars as pl
import yfinance as yf
from tqdm import tqdm

from ._yf_utils import (
    STANDARD_PRICE_SCHEMA,
)
from ._yf_utils import (
    backoff_sleep as _backoff_sleep,
)
from ._yf_utils import (
    classify_yf_download_issue as _classify_yf_download_issue,
)
from ._yf_utils import (
    coerce_standard_schema as _coerce_standard_schema,
)
from ._yf_utils import (
    is_rate_limit_error as _is_rate_limit_error,
)
from ._yf_utils import (
    normalize_yf_df_to_polars as _normalize_yf_df_to_polars,
)
from ._yf_utils import (
    run_yf_download as _run_yf_download,
)
from ._yf_utils import (
    share_class_fallback as _share_class_fallback,
)
from ._yf_utils import (
    split_bulk_download as _split_bulk_download,
)
from ._yf_utils import (
    yf_date_window as _yf_date_window,
)

_CURRENCY_CACHE: dict[str, str | None] = {}
_CURRENCY_CACHE_LOCK = Lock()
_UPDATE_WARNING_STATE_BY_PATH: dict[str, dict[tuple[str, str], datetime]] = {}
_UPDATE_LOG_CACHE_LOCK = Lock()
_WARNING_STATE_TTL_DAYS = 30


@dataclass(frozen=True)
class YFDownloadSpec:
    symbol: str
    interval: str = "1d"
    lookback_days: int = 2000


def fetch_yfinance_history(spec: YFDownloadSpec) -> pl.DataFrame:
    start_s, end_s = _yf_date_window(spec.lookback_days)

    df_pd, output, exc = _run_yf_download(
        yf.download,
        spec.symbol,
        start=start_s,
        end=end_s,
        interval=spec.interval,
        auto_adjust=False,  # keep raw OHLC; use adj_close if you want adjusted
        progress=False,
        group_by="column",
    )
    issue = _classify_yf_download_issue(f"{output}\n{exc!s}" if exc is not None else output)
    if exc is not None and issue is None:
        raise exc

    if (df_pd is None or len(df_pd) == 0) and issue is None:
        fallback = _share_class_fallback(spec.symbol)
        if fallback and fallback != spec.symbol:
            df_pd, _, exc = _run_yf_download(
                yf.download,
                fallback,
                start=start_s,
                end=end_s,
                interval=spec.interval,
                auto_adjust=False,
                progress=False,
                group_by="column",
            )
            if exc is not None:
                raise exc
    if df_pd is None or len(df_pd) == 0:
        return pl.DataFrame(schema=STANDARD_PRICE_SCHEMA)

    return _normalize_yf_df_to_polars(df_pd)


def fetch_symbol_currency(symbol: str) -> str | None:
    with _CURRENCY_CACHE_LOCK:
        if symbol in _CURRENCY_CACHE:
            return _CURRENCY_CACHE[symbol]

    currency: str | None = None
    try:
        ticker = yf.Ticker(symbol)
        fast_info = getattr(ticker, "fast_info", None)
        if fast_info is not None:
            try:
                currency = fast_info.get("currency")
            except Exception:
                try:
                    currency = fast_info["currency"]
                except Exception:
                    currency = None
        if not currency:
            info = ticker.get_info()
            if isinstance(info, dict):
                currency = info.get("currency")
    except Exception:
        currency = None

    if isinstance(currency, str):
        currency = currency.strip().upper() or None
    else:
        currency = None
    with _CURRENCY_CACHE_LOCK:
        _CURRENCY_CACHE[symbol] = currency
    return currency


def fetch_yfinance_history_bulk(
    symbols: list[str],
    interval: str,
    lookback_days: int,
    chunk_size: int = 100,
    sleep_seconds: float = 2.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
    show_progress: bool = False,
    progress_desc: str = "yfinance bulk",
) -> dict[str, pl.DataFrame]:
    if not symbols:
        return {}

    start_s, end_s = _yf_date_window(lookback_days)

    results: dict[str, pl.DataFrame] = {}
    chunk_starts = range(0, len(symbols), chunk_size)
    iterator = tqdm(chunk_starts, desc=progress_desc, unit="chunk") if show_progress else chunk_starts
    for i in iterator:
        chunk = symbols[i : i + chunk_size]
        attempt = 0
        while True:
            try:
                df_pd, output, exc = _run_yf_download(
                    yf.download,
                    chunk,
                    start=start_s,
                    end=end_s,
                    interval=interval,
                    auto_adjust=False,
                    progress=False,
                    group_by="column",
                    threads=threads,
                )
                issue = _classify_yf_download_issue(f"{output}\n{exc!s}" if exc is not None else output)
                if exc is not None and issue is None:
                    raise exc
                chunk_map = _split_bulk_download(df_pd, chunk)
                if issue is not None and not chunk_map:
                    if log_path is not None:
                        for sym in chunk:
                            append_update_log(log_path, sym, issue, attempt + 1)
                    break
                # Share-class fallback (e.g. BRK.B -> BRK-B) for symbols not present in bulk response.
                missing_syms = [sym for sym in chunk if sym not in chunk_map]
                for msym in missing_syms:
                    alt = _share_class_fallback(msym)
                    if not alt:
                        continue
                    try:
                        df_one, single_output, single_exc = _run_yf_download(
                            yf.download,
                            alt,
                            start=start_s,
                            end=end_s,
                            interval=interval,
                            auto_adjust=False,
                            progress=False,
                            group_by="column",
                            threads=False,
                        )
                        single_issue = _classify_yf_download_issue(
                            f"{single_output}\n{single_exc!s}" if single_exc is not None else single_output
                        )
                        if single_exc is not None and single_issue is None:
                            raise single_exc
                        if single_issue is not None:
                            if log_path is not None:
                                append_update_log(log_path, msym, single_issue, attempt + 1)
                            continue
                        if df_one is not None and len(df_one) > 0:
                            chunk_map[msym] = _normalize_yf_df_to_polars(df_one)
                    except Exception:
                        pass
                results.update(chunk_map)
                break
            except Exception as e:
                attempt += 1
                if _is_rate_limit_error(e) and attempt <= max_retries:
                    _backoff_sleep(attempt, backoff_max_seconds)
                    continue
                if log_path is not None:
                    for sym in chunk:
                        append_update_log(log_path, sym, str(e), attempt)
                break

        time.sleep(sleep_seconds)

    return results


def read_parquet_if_exists(path: Path) -> pl.DataFrame | None:
    if path.exists():
        return pl.read_parquet(str(path))
    return None


def upsert_symbol_parquet(
    symbol: str,
    interval: str,
    lookback_days: int,
    parquet_root: str | Path,
) -> Path:
    """Legacy single-symbol parquet updater.

    This helper predates the bulk workflow layer in `workflows.py`. It performs
    a direct fetch/merge/write cycle for one symbol and bypasses the
    workflow-level currency resolution, sanitization policy, and post-write
    integrity assertions used by the main package update commands.
    """
    warnings.warn(
        "upsert_symbol_parquet() is deprecated; prefer config-driven bulk update workflows in tradinglab_data.workflows.",
        DeprecationWarning,
        stacklevel=2,
    )
    root = Path(parquet_root)
    root.mkdir(parents=True, exist_ok=True)
    out_path = root / f"{symbol}.parquet"

    existing = read_parquet_if_exists(out_path)

    if existing is None or existing.is_empty():
        df_new = fetch_yfinance_history(YFDownloadSpec(symbol=symbol, interval=interval, lookback_days=lookback_days))
        df_new = _coerce_standard_schema(df_new)
        df_new.write_parquet(str(out_path))
        return out_path

    # Incremental update: fetch a small recent window and merge
    last_dt = existing.select(pl.col("date").max()).item()
    # pull a bit earlier to be safe re: revisions/holes
    lookback_buffer = 14
    fetch_days = 60
    if isinstance(last_dt, datetime):
        now = datetime.now(last_dt.tzinfo) if last_dt.tzinfo is not None else datetime.now()
        fetch_days = max(lookback_buffer, max(0, (now - last_dt).days) + 5)

    df_inc = fetch_yfinance_history(YFDownloadSpec(symbol=symbol, interval=interval, lookback_days=fetch_days))
    if df_inc.is_empty():
        return out_path

    existing = _coerce_standard_schema(existing)
    df_inc = _coerce_standard_schema(df_inc)

    combined = (
        pl.concat([existing, df_inc], how="vertical")
        .unique(subset=["date"], keep="last")
        .sort("date")
    )

    combined.write_parquet(str(out_path))
    return out_path


def append_update_log(log_path: Path, symbol: str, error: str, attempt_count: int) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    exists = log_path.exists()
    with log_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["timestamp", "symbol", "error", "attempt_count"])
        writer.writerow([datetime.now(timezone.utc).isoformat(), symbol, error, attempt_count])


def append_update_log_throttled(
    log_path: Path,
    symbol: str,
    error: str,
    attempt_count: int,
    *,
    cooldown_hours: float,
    state_path: Path | None = None,
) -> bool:
    if cooldown_hours <= 0:
        append_update_log(log_path, symbol, error, attempt_count)
        return True

    resolved_state_path = _warning_state_path(log_path, state_path=state_path)
    cache_key = str(resolved_state_path.resolve(strict=False))
    now = datetime.now(timezone.utc)
    entry_key = (str(symbol).strip().upper(), str(error))
    with _UPDATE_LOG_CACHE_LOCK:
        seen = _UPDATE_WARNING_STATE_BY_PATH.get(cache_key)
        if seen is None:
            seen = _load_warning_state(resolved_state_path)
            _UPDATE_WARNING_STATE_BY_PATH[cache_key] = seen
        last_seen = seen.get(entry_key)
        if last_seen is not None and (now - last_seen) < timedelta(hours=float(cooldown_hours)):
            return False
        append_update_log(log_path, symbol, error, attempt_count)
        seen[entry_key] = now
        _write_warning_state(resolved_state_path, seen)
    return True


def _warning_state_path(log_path: Path, *, state_path: Path | None = None) -> Path:
    if state_path is not None:
        return state_path
    stem = log_path.stem
    suffix = ".json"
    return log_path.with_name(f"{stem}_warning_state{suffix}")


def _load_warning_state(state_path: Path) -> dict[tuple[str, str], datetime]:
    if not state_path.exists() or state_path.stat().st_size == 0:
        return {}
    seen: dict[tuple[str, str], datetime] = {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}
        for key, timestamp_raw in payload.items():
            if not isinstance(key, str) or not isinstance(timestamp_raw, str):
                continue
            if "\u241f" not in key:
                continue
            symbol, error = key.split("\u241f", 1)
            if not symbol or not error:
                continue
            try:
                timestamp = datetime.fromisoformat(timestamp_raw)
            except Exception:
                continue
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)
            seen[(symbol, error)] = timestamp
    except Exception:
        return {}
    return seen


def _write_warning_state(state_path: Path, seen: dict[tuple[str, str], datetime]) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=_WARNING_STATE_TTL_DAYS)
    pruned = {
        key: value
        for key, value in seen.items()
        if value.astimezone(timezone.utc) >= cutoff
    }
    payload = {
        f"{symbol}\u241f{error}": value.astimezone(timezone.utc).isoformat()
        for (symbol, error), value in pruned.items()
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def clear_currency_cache() -> None:
    with _CURRENCY_CACHE_LOCK:
        _CURRENCY_CACHE.clear()
