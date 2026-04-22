from __future__ import annotations

import builtins
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .config import (
    ConfigLike,
    intraday_root_path,
    parquet_root_path,
    ticker_overrides_path,
    universe_csv_path,
    universe_dir_path,
)
from .crypto.storage import crypto_parquet_path
from .crypto.validation import validate_crypto_ohlcv_frame
from .crypto.verify import _is_stale
from .crypto.workflows import _read_crypto_config, _resolve_symbols
from .schema import validate_daily_frame, validate_intraday_frame
from .universe import load_universe_frame


@dataclass(frozen=True)
class UniverseConsistencyOptions:
    dataset: str
    interval: str | None = None
    universe: str | None = None
    exchange: str | None = None
    instrument_type: str | None = None
    symbols_override: list[str] | None = None


_REPORT_COLUMNS = [
    "dataset",
    "interval",
    "symbol",
    "name",
    "instrument_type",
    "asset_class",
    "exists",
    "status",
    "rows",
    "start",
    "end",
    "schema_ok",
    "sorted",
    "duplicate_rows",
    "null_ohlc_rows",
    "bad_ohlc_rows",
    "stale",
    "issues",
    "path",
]


def generate_universe_consistency_report(
    cfg: ConfigLike,
    *,
    dataset: str,
    interval: str | None = None,
    universe: str | None = None,
    exchange: str | None = None,
    instrument_type: str | None = None,
    symbols_override: list[str] | None = None,
) -> pl.DataFrame:
    selected = str(dataset).strip().lower()
    if selected == "daily":
        return _equity_report(
            cfg,
            dataset="daily",
            interval=None,
            instrument_type=instrument_type,
            symbols_override=symbols_override,
        )
    if selected == "intraday":
        if not interval:
            raise ValueError("--interval is required for intraday consistency reports")
        return _equity_report(
            cfg,
            dataset="intraday",
            interval=interval,
            instrument_type=instrument_type,
            symbols_override=symbols_override,
        )
    if selected == "crypto":
        if not interval:
            raise ValueError("--interval is required for crypto consistency reports")
        return _crypto_report(
            cfg,
            interval=interval,
            universe=universe,
            exchange=exchange,
            symbols_override=symbols_override,
        )
    raise ValueError(f"Unsupported dataset: {dataset}")


def render_universe_consistency_markdown(
    frame: pl.DataFrame,
    *,
    dataset: str,
    interval: str | None = None,
    universe: str | None = None,
    instrument_type: str | None = None,
) -> str:
    title_bits = [dataset]
    if interval:
        title_bits.append(interval)
    if universe:
        title_bits.append(f"universe={universe}")
    if instrument_type:
        title_bits.append(f"instrument_type={instrument_type}")
    header = " ".join(title_bits)
    rows = frame.sort(["status", "symbol"]).iter_rows(named=True)
    lines = [
        "# Universe Consistency Report",
        "",
        f"- scope: `{header}`",
        f"- symbols: `{frame.height}`",
        "",
        "| Symbol | Status | Rows | Start | End | Issues | Path |",
        "|---|---|---:|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| {symbol} | {status} | {rows} | {start} | {end} | {issues} | `{path}` |".format(
                symbol=row["symbol"],
                status=row["status"],
                rows=row["rows"],
                start=row["start"] or "-",
                end=row["end"] or "-",
                issues=row["issues"] or "-",
                path=row["path"],
            )
        )
    return "\n".join(lines) + "\n"


def render_universe_consistency_json(frame: pl.DataFrame) -> str:
    return json.dumps(frame.to_dicts(), indent=2)


def _equity_report(
    cfg: ConfigLike,
    *,
    dataset: str,
    interval: str | None,
    instrument_type: str | None,
    symbols_override: list[str] | None,
) -> pl.DataFrame:
    universe = _load_equity_universe(cfg, instrument_type=instrument_type, symbols_override=symbols_override)
    root = parquet_root_path(cfg) if dataset == "daily" else intraday_root_path(cfg) / str(interval)
    rows = [
        _inspect_equity_symbol(
            root / f"{symbol}.parquet",
            dataset=dataset,
            interval=interval,
            symbol=symbol,
            name=str(item.get("name") or ""),
            instrument=str(item.get("instrument_type") or ""),
            asset_class=str(item.get("asset_class") or ""),
        )
        for item in universe.iter_rows(named=True)
        for symbol in [str(item.get("symbol") or "").strip().upper()]
        if symbol
    ]
    return _rows_to_frame(rows)


def _crypto_report(
    cfg: ConfigLike,
    *,
    interval: str,
    universe: str | None,
    exchange: str | None,
    symbols_override: list[str] | None,
) -> pl.DataFrame:
    crypto_cfg = _read_crypto_config(cfg, exchange=exchange)
    symbols = _resolve_symbols(cfg, crypto_cfg, universe=universe, symbols_override=symbols_override)
    rows = []
    for symbol in symbols:
        path = crypto_parquet_path(
            crypto_cfg.root,
            exchange=crypto_cfg.exchange,
            market_type=crypto_cfg.market_type,
            interval=interval,
            symbol=symbol,
        )
        rows.append(_inspect_crypto_symbol(path, symbol=symbol, interval=interval))
    return _rows_to_frame(rows)


def _load_equity_universe(
    cfg: ConfigLike,
    *,
    instrument_type: str | None,
    symbols_override: list[str] | None,
) -> pl.DataFrame:
    if symbols_override:
        return pl.DataFrame(
            {
                "symbol": [str(symbol).strip().upper() for symbol in symbols_override],
                "name": ["" for _ in symbols_override],
                "instrument_type": [instrument_type or "" for _ in symbols_override],
                "asset_class": ["" for _ in symbols_override],
            }
        )
    frame = load_universe_frame(
        universe_csv_path(cfg),
        universe_dir=universe_dir_path(cfg),
        ticker_overrides_path=ticker_overrides_path(cfg),
    )
    if instrument_type:
        wanted = str(instrument_type).strip().lower()
        if "instrument_type" in frame.columns:
            frame = frame.filter(pl.col("instrument_type").cast(pl.String, strict=False).str.to_lowercase() == wanted)
    keep = [column for column in ["symbol", "name", "instrument_type", "asset_class"] if column in frame.columns]
    if keep:
        frame = frame.select(keep)
    for column in ["name", "instrument_type", "asset_class"]:
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit("").alias(column))
    return frame


def _inspect_equity_symbol(
    path: Path,
    *,
    dataset: str,
    interval: str | None,
    symbol: str,
    name: str,
    instrument: str,
    asset_class: str,
) -> dict[str, Any]:
    validator = validate_daily_frame if dataset == "daily" else validate_intraday_frame
    time_column = "date"
    return _inspect_ohlc_path(
        path,
        dataset=dataset,
        interval=interval,
        symbol=symbol,
        name=name,
        instrument_type=instrument,
        asset_class=asset_class,
        time_column=time_column,
        validator=lambda frame: validator(frame, allow_extra_columns=True),
        stale=False,
    )


def _inspect_crypto_symbol(path: Path, *, symbol: str, interval: str) -> dict[str, Any]:
    return _inspect_ohlc_path(
        path,
        dataset="crypto",
        interval=interval,
        symbol=symbol,
        name="",
        instrument_type="crypto",
        asset_class="crypto",
        time_column="timestamp",
        validator=lambda frame: validate_crypto_ohlcv_frame(frame, interval=interval, require_continuity=True),
        stale_check=lambda value: _is_stale(value, interval=interval, stale_multiple=2),
    )


def _inspect_ohlc_path(
    path: Path,
    *,
    dataset: str,
    interval: str | None,
    symbol: str,
    name: str,
    instrument_type: str,
    asset_class: str,
    time_column: str,
    validator: Any,
    stale: bool | None = None,
    stale_check: Any | None = None,
) -> dict[str, Any]:
    issues: list[str] = []
    if not path.exists():
        return _report_row(
            dataset=dataset,
            interval=interval,
            symbol=symbol,
            name=name,
            instrument_type=instrument_type,
            asset_class=asset_class,
            exists=False,
            status="missing",
            rows=0,
            start=None,
            end=None,
            schema_ok=False,
            sorted_ok=False,
            duplicate_rows=0,
            null_ohlc_rows=0,
            bad_ohlc_rows=0,
            stale=False,
            issues=["missing_file"],
            path=path,
        )
    if path.stat().st_size == 0:
        issues.append("zero_byte")
    try:
        frame = pl.read_parquet(str(path))
    except Exception as exc:
        return _report_row(
            dataset=dataset,
            interval=interval,
            symbol=symbol,
            name=name,
            instrument_type=instrument_type,
            asset_class=asset_class,
            exists=True,
            status="invalid",
            rows=0,
            start=None,
            end=None,
            schema_ok=False,
            sorted_ok=False,
            duplicate_rows=0,
            null_ohlc_rows=0,
            bad_ohlc_rows=0,
            stale=False,
            issues=issues + [f"read_error:{type(exc).__name__}"],
            path=path,
        )

    rows = int(frame.height)
    if rows == 0:
        issues.append("empty_file")
    schema_ok = True
    try:
        validator(frame)
    except Exception:
        schema_ok = False
        issues.append("schema_mismatch")
    duplicate_rows = _duplicate_count(frame, time_column=time_column)
    null_ohlc_rows = _null_ohlc_count(frame)
    bad_ohlc_rows = _bad_ohlc_count(frame)
    sorted_ok = _sorted_times(frame, time_column=time_column)
    if duplicate_rows > 0:
        issues.append("duplicate_rows")
    if null_ohlc_rows > 0:
        issues.append("null_ohlc_rows")
    if bad_ohlc_rows > 0:
        issues.append("bad_ohlc_rows")
    if not sorted_ok:
        issues.append("unsorted_rows")
    start, end, end_value = _time_bounds(frame, time_column=time_column)
    is_stale = False
    if stale_check is not None:
        is_stale = bool(stale_check(end_value))
        if is_stale:
            issues.append("stale")
    elif stale is not None:
        is_stale = bool(stale)
    status = "ok" if not issues else "dirty"
    return _report_row(
        dataset=dataset,
        interval=interval,
        symbol=symbol,
        name=name,
        instrument_type=instrument_type,
        asset_class=asset_class,
        exists=True,
        status=status,
        rows=rows,
        start=start,
        end=end,
        schema_ok=schema_ok,
        sorted_ok=sorted_ok,
        duplicate_rows=duplicate_rows,
        null_ohlc_rows=null_ohlc_rows,
        bad_ohlc_rows=bad_ohlc_rows,
        stale=is_stale,
        issues=issues,
        path=path,
    )


def _report_row(
    *,
    dataset: str,
    interval: str | None,
    symbol: str,
    name: str,
    instrument_type: str,
    asset_class: str,
    exists: bool,
    status: str,
    rows: int,
    start: str | None,
    end: str | None,
    schema_ok: bool,
    sorted_ok: bool,
    duplicate_rows: int,
    null_ohlc_rows: int,
    bad_ohlc_rows: int,
    stale: bool,
    issues: list[str],
    path: Path,
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "interval": interval or "",
        "symbol": symbol,
        "name": name,
        "instrument_type": instrument_type,
        "asset_class": asset_class,
        "exists": exists,
        "status": status,
        "rows": rows,
        "start": start,
        "end": end,
        "schema_ok": schema_ok,
        "sorted": sorted_ok,
        "duplicate_rows": duplicate_rows,
        "null_ohlc_rows": null_ohlc_rows,
        "bad_ohlc_rows": bad_ohlc_rows,
        "stale": stale,
        "issues": ",".join(builtins.sorted(set(issues))),
        "path": str(path),
    }


def _rows_to_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={column: pl.String for column in _REPORT_COLUMNS})
    frame = pl.DataFrame(rows)
    for column in _REPORT_COLUMNS:
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None).alias(column))
    return frame.select(_REPORT_COLUMNS)


def _duplicate_count(frame: pl.DataFrame, *, time_column: str) -> int:
    if time_column not in frame.columns or frame.is_empty():
        return 0
    return int(frame.height - frame.select(pl.col(time_column).n_unique()).item())


def _null_ohlc_count(frame: pl.DataFrame) -> int:
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(frame.columns)):
        return 0
    return int(
        frame.select(
            pl.any_horizontal(
                [
                    pl.col("open").is_null(),
                    pl.col("high").is_null(),
                    pl.col("low").is_null(),
                    pl.col("close").is_null(),
                ]
            ).sum()
        ).item()
    )


def _bad_ohlc_count(frame: pl.DataFrame) -> int:
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(frame.columns)):
        return 0
    return int(
        frame.select(
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


def _sorted_times(frame: pl.DataFrame, *, time_column: str) -> bool:
    if time_column not in frame.columns or frame.is_empty():
        return False
    try:
        return bool(frame.get_column(time_column).is_sorted())
    except Exception:
        return False


def _time_bounds(frame: pl.DataFrame, *, time_column: str) -> tuple[str | None, str | None, datetime | None]:
    if time_column not in frame.columns or frame.is_empty():
        return None, None, None
    start_value = frame.select(pl.col(time_column).min()).item()
    end_value = frame.select(pl.col(time_column).max()).item()
    return _fmt_datetime(start_value), _fmt_datetime(end_value), end_value if isinstance(end_value, datetime) else None


def _fmt_datetime(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
