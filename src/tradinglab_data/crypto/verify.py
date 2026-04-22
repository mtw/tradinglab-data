from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TypedDict

import polars as pl

from ..config import ConfigLike
from .storage import crypto_parquet_path
from .validation import INTERVAL_EXPECTED_DELTA, validate_crypto_ohlcv_frame
from .workflows import _read_crypto_config, _resolve_symbols, crypto_backfill_from_config


class CryptoIssueEntry(TypedDict):
    symbol: str
    path: str
    exists: bool
    rows: int
    start: str | None
    end: str | None
    reasons: list[str]
    error: str


class CryptoVerifyResult(TypedDict):
    ok: bool
    exchange: str
    market_type: str
    interval: str
    universe: str
    root: str
    expected_symbols: int
    files_present: int
    zero_byte_files: int
    missing_symbols: list[str]
    dirty_symbols: list[CryptoIssueEntry]
    repaired_symbols: list[str]
    config: dict[str, object]
    errors: list[str]


@dataclass(frozen=True)
class CryptoVerifyConfig:
    interval: str
    universe: str | None = None
    exchange: str | None = None
    max_missing_ratio: float = 0.0
    max_zero_byte: int = 0
    stale_multiple: int = 2
    repair: bool = False


def run_crypto_verify_checks(cfg: ConfigLike, verify_cfg: CryptoVerifyConfig) -> CryptoVerifyResult:
    crypto_cfg = _read_crypto_config(cfg, exchange=verify_cfg.exchange)
    selected_universe = verify_cfg.universe or crypto_cfg.default_universe
    symbols = _resolve_symbols(cfg, crypto_cfg, universe=selected_universe, symbols_override=None)
    dirty_entries: list[CryptoIssueEntry] = []
    missing_symbols: list[str] = []
    zero_byte_files = 0
    files_present = 0

    for symbol in symbols:
        issue = _check_symbol(crypto_cfg, symbol=symbol, interval=verify_cfg.interval, stale_multiple=verify_cfg.stale_multiple)
        if issue is None:
            files_present += 1
            continue
        if "missing_file" in issue["reasons"]:
            missing_symbols.append(symbol)
        if "zero_byte" in issue["reasons"]:
            zero_byte_files += 1
        dirty_entries.append(issue)
        if issue["exists"]:
            files_present += 1

    repaired_symbols: list[str] = []
    if verify_cfg.repair and dirty_entries:
        for issue in list(dirty_entries):
            symbol = issue["symbol"]
            incremental = issue["reasons"] == ["stale"]
            crypto_backfill_from_config(
                cfg,
                exchange=crypto_cfg.exchange,
                interval=verify_cfg.interval,
                universe=selected_universe,
                symbols_override=[symbol],
                incremental=incremental,
            )
            rechecked = _check_symbol(crypto_cfg, symbol=symbol, interval=verify_cfg.interval, stale_multiple=verify_cfg.stale_multiple)
            if rechecked is None:
                repaired_symbols.append(symbol)
        dirty_entries = []
        missing_symbols = []
        zero_byte_files = 0
        files_present = 0
        for symbol in symbols:
            issue = _check_symbol(crypto_cfg, symbol=symbol, interval=verify_cfg.interval, stale_multiple=verify_cfg.stale_multiple)
            if issue is None:
                files_present += 1
                continue
            if "missing_file" in issue["reasons"]:
                missing_symbols.append(symbol)
            if "zero_byte" in issue["reasons"]:
                zero_byte_files += 1
            dirty_entries.append(issue)
            if issue["exists"]:
                files_present += 1

    errors: list[str] = []
    missing_ratio = (len(missing_symbols) / len(symbols)) if symbols else 0.0
    if zero_byte_files > verify_cfg.max_zero_byte:
        errors.append(f"zero_byte_files:{zero_byte_files}>{verify_cfg.max_zero_byte}")
    if missing_ratio > verify_cfg.max_missing_ratio:
        errors.append(f"high_missing_ratio:{missing_ratio:.4f}>{verify_cfg.max_missing_ratio:.4f}")
    for issue in dirty_entries:
        errors.append(f"dirty_crypto:{issue['symbol']}:{','.join(issue['reasons'])}")

    return {
        "ok": not errors,
        "exchange": crypto_cfg.exchange,
        "market_type": crypto_cfg.market_type,
        "interval": verify_cfg.interval,
        "universe": selected_universe,
        "root": str(crypto_cfg.root),
        "expected_symbols": len(symbols),
        "files_present": files_present,
        "zero_byte_files": zero_byte_files,
        "missing_symbols": missing_symbols,
        "dirty_symbols": dirty_entries,
        "repaired_symbols": repaired_symbols,
        "config": {
            "max_missing_ratio": float(verify_cfg.max_missing_ratio),
            "max_zero_byte": int(verify_cfg.max_zero_byte),
            "stale_multiple": int(verify_cfg.stale_multiple),
            "repair": bool(verify_cfg.repair),
        },
        "errors": errors,
    }


def _check_symbol(
    crypto_cfg: object,
    *,
    symbol: str,
    interval: str,
    stale_multiple: int,
) -> CryptoIssueEntry | None:
    path = crypto_parquet_path(
        getattr(crypto_cfg, "root"),
        exchange=getattr(crypto_cfg, "exchange"),
        market_type=getattr(crypto_cfg, "market_type"),
        interval=interval,
        symbol=symbol,
    )
    reasons: list[str] = []
    rows = 0
    start: str | None = None
    end: str | None = None
    if not path.exists():
        return {
            "symbol": symbol,
            "path": str(path),
            "exists": False,
            "rows": 0,
            "start": None,
            "end": None,
            "reasons": ["missing_file"],
            "error": "missing file",
        }
    if path.stat().st_size == 0:
        reasons.append("zero_byte")
    try:
        frame = pl.read_parquet(str(path))
        rows = frame.height
        if rows == 0:
            reasons.append("empty_file")
        validate_crypto_ohlcv_frame(frame, interval=interval, require_continuity=True)
        if rows > 0:
            start_dt = frame.select(pl.col("timestamp").min()).item()
            end_dt = frame.select(pl.col("timestamp").max()).item()
            start = start_dt.isoformat() if isinstance(start_dt, datetime) else str(start_dt) if start_dt is not None else None
            end = end_dt.isoformat() if isinstance(end_dt, datetime) else str(end_dt) if end_dt is not None else None
            if _is_stale(end_dt, interval=interval, stale_multiple=stale_multiple):
                reasons.append("stale")
    except Exception as exc:
        reasons.append("invalid")
        return {
            "symbol": symbol,
            "path": str(path),
            "exists": True,
            "rows": rows,
            "start": start,
            "end": end,
            "reasons": sorted(set(reasons)),
            "error": str(exc),
        }
    if not reasons:
        return None
    return {
        "symbol": symbol,
        "path": str(path),
        "exists": True,
        "rows": rows,
        "start": start,
        "end": end,
        "reasons": sorted(set(reasons)),
        "error": "",
    }


def _is_stale(value: object, *, interval: str, stale_multiple: int) -> bool:
    if not isinstance(value, datetime):
        return True
    expected = INTERVAL_EXPECTED_DELTA[interval]
    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    return value < (now_naive - (expected * max(1, stale_multiple)))
