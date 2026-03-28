from __future__ import annotations

from typing import Literal, TypedDict

import polars as pl


SessionLabel = Literal["pre", "regular", "post", "closed", "unknown"]
VerifyStatus = Literal["ok", "fail"]

OHLC_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "currency",
)

MOVE_FRAME_COLUMNS = (
    "symbol",
    "ref_close",
    "last_price",
    "pct_move",
    "last_volume",
    "currency",
    "last_ts",
    "session",
)

ALERT_FRAME_COLUMNS = MOVE_FRAME_COLUMNS


class CoverageEntry(TypedDict):
    symbols: int
    present: int
    missing: int
    missing_ratio: float


class VerifyResult(TypedDict):
    ok: bool
    status: VerifyStatus
    errors: list[str]
    parquet_root: str
    file_count: int
    zero_byte: int
    sample_read_checked: int
    sample_read_failures: list[str]
    coverage: dict[str, CoverageEntry]
    prev_file_count: int | None
    config: dict[str, object]


class DailyCloseInfo(TypedDict):
    close: float
    currency: str | None


class ExtendedHoursResult(TypedDict):
    preferred_interval: str
    fallback_interval: str
    symbols: int
    preferred_written: int
    fallback_written: int
    alerts: int
    alerts_path: str
    moves_df: pl.DataFrame
    alerts_df: pl.DataFrame


class MonitorExtendedHoursResult(ExtendedHoursResult):
    report_html: str


class UpdateResult(TypedDict):
    symbols: list[str]
    parquet_root: str
    intraday: ExtendedHoursResult | None
