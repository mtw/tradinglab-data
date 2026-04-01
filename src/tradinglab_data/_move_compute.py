from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import polars as pl

from .contracts import DailyCloseInfo
from .data_yf import read_parquet_if_exists
from .schema import MOVE_ALERT_FRAME_SCHEMA


def session_label(dt_value: Any) -> str:
    if dt_value is None:
        return "unknown"
    try:
        dt = dt_value if isinstance(dt_value, datetime) else datetime.fromisoformat(str(dt_value))
    except Exception:
        return "unknown"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    et = dt.astimezone(ZoneInfo("America/New_York"))
    hm = et.hour * 60 + et.minute
    if 4 * 60 <= hm < 9 * 60 + 30:
        return "pre"
    if 9 * 60 + 30 <= hm < 16 * 60:
        return "regular"
    if 16 * 60 <= hm < 20 * 60:
        return "post"
    return "closed"


def empty_move_alert_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=MOVE_ALERT_FRAME_SCHEMA)


def load_daily_reference_closes(
    symbols: list[str],
    daily_root: str | Path,
) -> dict[str, DailyCloseInfo]:
    root = Path(daily_root)
    out: dict[str, DailyCloseInfo] = {}
    for sym in symbols:
        path = root / f"{sym}.parquet"
        df = read_parquet_if_exists(path)
        if df is None or df.is_empty() or "close" not in df.columns:
            continue
        tail = df.sort("date").tail(1)
        if tail.is_empty():
            continue
        close_v = tail.get_column("close").to_list()[0]
        currency = None
        if "currency" in tail.columns:
            vals = [v for v in tail.get_column("currency").to_list() if v is not None and str(v).strip()]
            if vals:
                currency = str(vals[0]).strip().upper()
        out[sym] = {"close": float(close_v), "currency": currency}
    return out


def _daily_close_frame(daily_close_map: Mapping[str, DailyCloseInfo | float]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for sym, info in daily_close_map.items():
        if isinstance(info, dict):
            ref_close = info.get("close")
            ref_currency = info.get("currency")
        else:
            ref_close = info
            ref_currency = None
        if ref_close in {None, 0}:
            continue
        rows.append(
            {
                "symbol": str(sym),
                "ref_close": float(ref_close),
                "ref_currency": (
                    str(ref_currency).strip().upper()
                    if ref_currency is not None and str(ref_currency).strip()
                    else None
                ),
            }
        )
    if not rows:
        return pl.DataFrame(schema={"symbol": pl.String, "ref_close": pl.Float64, "ref_currency": pl.String})
    return pl.DataFrame(rows, schema={"symbol": pl.String, "ref_close": pl.Float64, "ref_currency": pl.String})


def compute_moves_vs_close(
    intraday_df: pl.DataFrame | dict[str, pl.DataFrame],
    daily_close_map: Mapping[str, DailyCloseInfo | float],
) -> pl.DataFrame:
    if isinstance(intraday_df, dict):
        frames: list[pl.DataFrame] = []
        for sym, df in intraday_df.items():
            if df is None or df.is_empty():
                continue
            frames.append(df.with_columns(pl.lit(sym).alias("symbol")))
        data = pl.concat(frames, how="vertical") if frames else empty_move_alert_frame()
    else:
        data = intraday_df

    if data.is_empty() or "symbol" not in data.columns:
        return empty_move_alert_frame()

    valid = data.filter(pl.col("close").is_not_null())
    if valid.is_empty():
        return empty_move_alert_frame()

    valid = valid.with_columns(pl.col("symbol").cast(pl.String, strict=False))
    if "date" in valid.columns:
        valid = valid.sort(["symbol", "date"])
    else:
        valid = valid.sort("symbol")

    aggregate_exprs: list[pl.Expr] = [
        pl.col("close").last().cast(pl.Float64).alias("last_price"),
        pl.col("date").last().alias("last_ts"),
    ]
    if "volume" in valid.columns:
        aggregate_exprs.append(pl.col("volume").last().cast(pl.Float64, strict=False).alias("last_volume"))
    else:
        aggregate_exprs.append(pl.lit(None, dtype=pl.Float64).alias("last_volume"))
    if "currency" in valid.columns:
        aggregate_exprs.append(pl.col("currency").last().cast(pl.String, strict=False).alias("last_currency"))
    else:
        aggregate_exprs.append(pl.lit(None, dtype=pl.String).alias("last_currency"))

    last_per_symbol = valid.group_by("symbol").agg(*aggregate_exprs)
    ref_df = _daily_close_frame(daily_close_map)
    if ref_df.is_empty():
        return empty_move_alert_frame()

    moves = last_per_symbol.join(ref_df, on="symbol", how="inner")
    if moves.is_empty():
        return empty_move_alert_frame()

    cleaned_last_currency = pl.col("last_currency").cast(pl.String, strict=False).str.strip_chars().str.to_uppercase()
    cleaned_ref_currency = pl.col("ref_currency").cast(pl.String, strict=False).str.strip_chars().str.to_uppercase()
    moves = moves.with_columns(
        pl.when(cleaned_last_currency.is_null() | (cleaned_last_currency == ""))
        .then(
            pl.when(cleaned_ref_currency.is_null() | (cleaned_ref_currency == ""))
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(cleaned_ref_currency)
        )
        .otherwise(cleaned_last_currency)
        .alias("currency"),
        (((pl.col("last_price") / pl.col("ref_close")) - 1.0) * 100.0).alias("pct_move"),
        pl.col("last_ts").map_elements(session_label, return_dtype=pl.String).alias("session"),
    )
    return moves.select(list(MOVE_ALERT_FRAME_SCHEMA)).sort("symbol")


def detect_alerts(
    moves_df: pl.DataFrame,
    threshold: float,
    min_volume: float | None = None,
) -> pl.DataFrame:
    if moves_df is None or moves_df.is_empty():
        return empty_move_alert_frame()
    out = moves_df.filter(pl.col("pct_move").abs() >= float(threshold))
    if min_volume is not None and float(min_volume) > 0:
        out = out.filter(pl.col("last_volume").fill_null(0.0) >= float(min_volume))
    return out.sort("pct_move", descending=True)


def summarize_gap_report(
    moves_df: pl.DataFrame,
    threshold: float,
    min_volume: float | None = None,
    top_n: int = 25,
    session_filter: str = "all",
) -> pl.DataFrame:
    if moves_df is None or moves_df.is_empty():
        return empty_move_alert_frame()
    out = moves_df
    wanted = str(session_filter or "all").strip().lower()
    if wanted not in {"all", "pre", "post", "regular", "closed"}:
        wanted = "all"
    if wanted != "all" and "session" in out.columns:
        out = out.filter(pl.col("session").cast(pl.String, strict=False) == wanted)
    if min_volume is not None and float(min_volume) > 0:
        out = out.filter(pl.col("last_volume").fill_null(0.0) >= float(min_volume))
    out = out.with_columns(pl.col("pct_move").abs().alias("abs_pct_move")).sort(
        ["abs_pct_move", "pct_move"], descending=[True, True]
    )
    if top_n and int(top_n) > 0:
        out = out.head(int(top_n))
    return out
