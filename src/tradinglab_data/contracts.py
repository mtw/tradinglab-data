from __future__ import annotations

from typing import Literal, TypedDict

import polars as pl

PACKAGE_NAME = "tradinglab-data"
PYTHON_PACKAGE_NAME = "tradinglab_data"
ARTIFACT_SCHEMA_VERSION = "v0.3.0"

SessionLabel = Literal["pre", "regular", "post", "closed", "unknown"]
VerifyStatus = Literal["ok", "fail"]
ArtifactCategory = Literal["parquet", "csv", "html", "json", "markdown"]

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

SYMBOL_MASTER_COLUMNS = (
    "symbol",
    "exchange",
    "country",
    "asset_currency",
    "base_listing_currency",
    "tax_country",
    "asset_class",
    "fx_pair_to_base",
    "lot_size",
    "price_multiplier",
)

SYMBOL_MASTER_OPTIONAL_COLUMNS = (
    "name",
    "isin",
    "instrument_type",
    "active",
    "source",
    "metadata_source",
    "metadata_quality",
    "notes",
)

EXCHANGE_DEFAULT_COLUMNS = (
    "exchange",
    "country",
    "default_asset_currency",
    "default_tax_country",
    "default_lot_size",
    "default_price_multiplier",
    "default_asset_class",
)

SYMBOL_OVERRIDE_COLUMNS = (
    "symbol",
    "exchange",
    "country",
    "asset_currency",
    "base_listing_currency",
    "tax_country",
    "asset_class",
    "fx_pair_to_base",
    "lot_size",
    "price_multiplier",
    "notes",
)

FX_DAILY_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "provider",
    "pair",
    "base_currency",
    "quote_currency",
    "source_symbol",
    "ingested_at",
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


class ArtifactFamilyEntry(TypedDict):
    category: ArtifactCategory
    path_pattern: str
    schema_name: str | None
    artifact_schema_version: str


class CompatibilityManifest(TypedDict):
    package_name: str
    python_package_name: str
    package_version: str | None
    artifact_schema_version: str
    artifact_families: dict[str, ArtifactFamilyEntry]


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


class SymbolMasterEntry(TypedDict, total=False):
    symbol: str
    exchange: str
    country: str
    asset_currency: str
    base_listing_currency: str
    tax_country: str
    asset_class: str
    fx_pair_to_base: str
    lot_size: float
    price_multiplier: float
    name: str
    isin: str
    instrument_type: str
    active: int | bool
    source: str
    metadata_source: str
    metadata_quality: str
    notes: str


class ExchangeDefaultEntry(TypedDict, total=False):
    exchange: str
    country: str
    default_asset_currency: str
    default_tax_country: str
    default_lot_size: float
    default_price_multiplier: float
    default_asset_class: str
    notes: str


class SymbolOverrideEntry(TypedDict, total=False):
    symbol: str
    exchange: str
    country: str
    asset_currency: str
    base_listing_currency: str
    tax_country: str
    asset_class: str
    fx_pair_to_base: str
    lot_size: float
    price_multiplier: float
    notes: str


class FxDailyEntry(TypedDict):
    date: str | None
    open: float
    high: float
    low: float
    close: float
    provider: str
    pair: str
    base_currency: str
    quote_currency: str
    source_symbol: str
    ingested_at: str | None


class FxSyncResult(TypedDict):
    ok: bool
    pair: str
    provider: str
    rows_written: int
    path: str
    source_symbol: str
    used_inverse: bool


class FxValidateResult(TypedDict):
    ok: bool
    pair: str
    path: str
    rows: int
    errors: list[str]


class SymbolMasterBuildResult(TypedDict):
    symbols: int
    base_currency: str
    missing_required: int
    output: str


class SymbolMasterValidateResult(TypedDict):
    ok: bool
    path: str
    rows: int
    errors: list[str]


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


class IntradayResearchSyncResult(TypedDict):
    interval: str
    universe: str
    root: str
    symbols: list[str]
    files_written: int
    rows_written: int
    unchanged_symbols: list[str]
    skipped_symbols: list[str]


class IntradayResearchValidateResult(TypedDict):
    ok: bool
    interval: str
    universe: str
    root: str
    files_checked: int
    dirty_files: list[str]
    errors: list[str]


class IntradayLiveSyncResult(TypedDict):
    interval: str
    universe: str
    root: str
    symbols: list[str]
    files_written: int
    rows_written: int
    unchanged_symbols: list[str]
    skipped_symbols: list[str]


class IntradayLiveValidateResult(TypedDict):
    ok: bool
    interval: str
    universe: str
    root: str
    files_checked: int
    dirty_files: list[str]
    errors: list[str]


class IntradayDualSyncResult(TypedDict):
    interval: str
    universe: str
    symbols: list[str]
    fetched_symbols: int
    live: IntradayLiveSyncResult
    research: IntradayResearchSyncResult


class CryptoRegistryEntry(TypedDict):
    symbol_canonical: str
    source_symbol: str
    exchange: str
    market_type: str
    base_asset: str
    quote_asset: str
    is_active: bool
    universe_tags: list[str]


class CryptoMetadataEntry(TypedDict):
    coingecko_id: str
    symbol_canonical: str
    source_symbol: str
    name: str
    base_asset: str
    quote_asset: str
    market_cap_rank: int | None
    market_cap: float | None
    total_volume: float | None
    exchange: str
    market_type: str
    is_active: bool
    universe_tags: list[str]


class CryptoSyncResult(TypedDict):
    exchange: str
    market_type: str
    interval: str
    universe: str
    symbols: list[str]
    files_written: int
    rows_written: int
    unchanged_symbols: list[str]
    skipped_symbols: list[str]
    pruned_files: list[str]
    root: str


class CryptoValidateResult(TypedDict):
    ok: bool
    exchange: str
    market_type: str
    interval: str
    universe: str
    root: str
    files_checked: int
    dirty_files: list[str]
    errors: list[str]


class CryptoUniverseRefreshResult(TypedDict):
    provider: str
    exchange: str
    market_type: str
    universe: str
    registry_path: str
    universe_path: str
    candidates_seen: int
    symbols_selected: list[str]


class StoreIntegrityReport(TypedDict):
    generated_at: str
    config_path: str
    daily_root: str
    intraday_root: str
    crypto_root: str
    sections: list[StoreIntegritySection]
    dirty_files: list[StoreIntegrityFileIssue]
    parquet_sanity: VerifyResult
    json_path: str
    markdown_path: str
