from __future__ import annotations

from typing import Literal, TypedDict

import polars as pl


API_CONTRACT_VERSION = "v0.2.0"

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


class StoreHistoryEntry(TypedDict):
    symbol: str
    path: str
    rows: int
    start_date: str | None
    end_date: str | None
    currencies: list[str]


class StoreIntegritySection(TypedDict):
    section: str
    root: str
    files_total: int
    files_readable: int
    files_dirty: int
    rows_total: int
    rows_min: int
    rows_median: float
    rows_max: int
    earliest_date: str | None
    latest_date: str | None
    currencies_seen: list[str]
    dirty_reason_counts: dict[str, int]
    top_histories: list[StoreHistoryEntry]


class StoreIntegrityFileIssue(TypedDict):
    section: str
    symbol: str
    path: str
    rows: int
    start_date: str | None
    end_date: str | None
    dirty_reasons: list[str]
    schema_error: str | None
    read_error: str | None


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


class StoreIntegrityReport(TypedDict):
    generated_at: str
    config_path: str
    daily_root: str
    intraday_root: str
    sections: list[StoreIntegritySection]
    dirty_files: list[StoreIntegrityFileIssue]
    parquet_sanity: VerifyResult
    json_path: str
    markdown_path: str
