# API Contract

## Purpose

This document records the current externally relevant contract of `tradinglab-data` as implemented in this repository today.

It is a compatibility snapshot for upcoming review and refactoring work. If code changes must preserve existing consumers, this document is the baseline unless a later migration deliberately updates it.

Current package version in [`pyproject.toml`](../pyproject.toml): `0.1.0`

## Version

API contract version:

- `v0.1.0`

This version identifies the documented consumer-facing compatibility surface of the package.

Contract versioning rule:

- start aligned with the package version at first publication
- increment whenever the documented compatibility surface changes
- a package release may keep the same contract version if the external contract is unchanged
- incompatible contract changes must update the contract version and include migration notes

Programmatic surface:

- `tradinglab_data.API_CONTRACT_VERSION`
- `tradinglab_data.contracts.API_CONTRACT_VERSION`
- `tradinglab_data.schema.schema_manifest()["api_contract_version"]`

Contract history:

| Contract Version | Package Version | Notes |
|---|---|---|
| `v0.1.0` | `0.1.0` | Initial formalized package contract baseline |

## Stability Boundary

The following are treated as part of the external contract:

- CLI command names, required arguments, and primary output locations
- YAML config keys consumed by CLI and workflow entrypoints
- On-disk artifact locations, file naming, and schemas
- Public Python names that do not start with `_`
- declared typed result contracts and explicit contract-version metadata
- Error behavior that downstream automation is likely to depend on

The following are not part of the external contract:

- Functions, constants, and helpers whose names start with `_`
- Internal fetch sequencing, retry strategy, and progress output details
- HTML report styling and presentation details, as long as the artifact path and basic purpose remain the same

## Package Surface

Installable package name:

- `tradinglab-data`

Python import package:

- `tradinglab_data`

Console script:

- `tradinglab-data`

Module-level exports declared in [`src/tradinglab_data/__init__.py`](../src/tradinglab_data/__init__.py):

- `cli`
- `config`
- `contracts`
- `data_stooq`
- `data_yf`
- `extended_hours_monitor`
- `parquet_verify`
- `schema`
- `ticker_map`
- `universe`
- `universe_build`
- `workflows`

Additive top-level lazy re-exports are also available for commonly used public names, including:

- `load_universe`
- `load_universe_frame`
- `build_universe`
- `run_parquet_sanity_checks`
- `schema_manifest`
- `render_schema_json`
- `render_schema_markdown`
- `validate_daily_frame`
- `validate_intraday_frame`
- `validate_moves_frame`
- `validate_alerts_frame`
- `update_from_config`
- `monitor_extended_hours_from_config`
- `UniverseRow`
- `UpdateResult`
- `ExtendedHoursResult`
- `MonitorExtendedHoursResult`
- `VerifyResult`
- `API_CONTRACT_VERSION`

## CLI Contract

Implementation source: [`src/tradinglab_data/cli.py`](../src/tradinglab_data/cli.py)

Global option:

- `--config <path>`
  - defaults by discovery order:
    - `TRADINGLAB_DATA_CONFIG` when set
    - source-tree `configs/config.yaml` when running from a checkout and it exists
    - `./config.yaml`
    - `./configs/config.yaml`
  - is not required for `schema`
  - is required in practice for `update`, `monitor-extended-hours`, and `build-universe` because those code paths load `Config`

Subcommands:

### `schema`

Usage:

```bash
tradinglab-data schema --format markdown|json [--out PATH]
```

Contract:

- renders the canonical parquet schema
- works without a valid config file
- prints to stdout unless `--out` is given
- returns process exit code `0` on success

### `build-universe`

Usage:

```bash
tradinglab-data build-universe --indices <one-or-more> --out PATH [--overrides-dir PATH] [--inactive-too]
```

Supported indices in current implementation:

- `sp500`
- `djia`
- `dax`
- `mdax`
- `atx`

Contract:

- writes a merged universe CSV to `--out`
- uses `--overrides-dir` when provided
- otherwise defaults overrides directory from `universe_dir_path(cfg)`
- applies ticker normalization through `normalize_to_yahoo(...)`
- keeps only `active == 1` rows unless `--inactive-too` is passed
- raises `RuntimeError` if no constituents can be built

### `update`

Usage:

```bash
tradinglab-data update [--symbols SYMBOL ...]
```

Contract:

- loads active symbols from configured universe inputs
- updates daily parquet under `paths.parquet_root`
- may also update intraday parquet and extended-hours artifacts when `extended_hours.enabled` is true
- accepts an optional symbol subset; unknown requested symbols are skipped with a warning, and an all-missing selection exits with an error
- returns a dict from the Python entrypoint `update_from_config(...)`; CLI itself returns exit code `0` when no exception is raised

### `monitor-extended-hours`

Usage:

```bash
tradinglab-data monitor-extended-hours [--symbols SYMBOL ...] [--top-n N] [--session all|pre|post|regular|closed]
```

Contract:

- updates intraday parquet only
- reads reference closes from the daily parquet store
- writes alert CSV and HTML report under `<paths.runs_root>/YYYY-MM-DD/monitor/`
- accepts the same optional symbol filtering behavior as `update`

## Config Contract

Implementation source: [`src/tradinglab_data/config.py`](../src/tradinglab_data/config.py)

Config format:

- YAML document that must parse to a top-level mapping

Path expansion behavior:

- environment variables are expanded
- `~` is expanded
- expansion applies recursively inside nested dicts and lists

Missing config behavior:

- `Config.load(...)` raises `FileNotFoundError`
- the current error message tells the caller to create a config from the bundled `config.yaml.example` template, pass `--config`, or set `TRADINGLAB_DATA_CONFIG`

Required path keys for the main workflows:

- `paths.universe_csv`
- `paths.parquet_root`
- `paths.runs_root`

Derived path defaults:

- `paths.meta_root`
  - defaults to `dirname(paths.universe_csv)`
- `paths.universe_dir`
  - defaults to `<meta_root>/universes`
- `paths.update_log_csv`
  - defaults to `<meta_root>/update_log.csv`
- `paths.ticker_overrides_csv`
  - defaults to `<meta_root>/ticker_overrides.csv`
- `extended_hours.intraday_root`
  - defaults to sibling directory of daily parquet root: `<parent(paths.parquet_root)>/intraday`
- `paths.registry_root`
  - defaults to `<paths.runs_root>/runs_registry`

Operational keys currently consumed by workflows:

- top level:
  - `timeframe`
  - `lookback_days`
- `yf.*`:
  - `chunk_size`
  - `sleep_seconds`
  - `max_retries`
  - `backoff_max_seconds`
  - `threads`
- `update.*`:
  - `history_provider`
  - `recent_provider`
  - `recent_days`
  - `incremental_days`
  - `assert_postwrite_integrity`
  - `stooq_refresh_all`
- `extended_hours.*`:
  - `enabled`
  - `intraday_root`
  - `preferred_interval`
  - `fallback_interval`
  - `retention_days`
  - `prepost`
  - `chunk_size`
  - `sleep_seconds`
  - `max_retries`
  - `backoff_max_seconds`
  - `threads`
  - `pct_move_threshold`
  - `min_volume`

Reference template:

- [`configs/config.yaml.example`](../configs/config.yaml.example)
- bundled wheel copy: [`src/tradinglab_data/config.yaml.example`](../src/tradinglab_data/config.yaml.example)

## Artifact Contract

### Daily Parquet Store

Primary path:

- `<paths.parquet_root>/<SYMBOL>.parquet`

Schema source:

- [`src/tradinglab_data/schema.py`](../src/tradinglab_data/schema.py)

Columns:

| Column | Type |
|---|---|
| `date` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `adj_close` | `Float64` |
| `volume` | `Float64` |
| `currency` | `String` |

Current invariants enforced by workflow code:

- rows are sorted ascending by `date`
- `date` should be unique within a file
- OHLC rows with nulls or invalid price relationships are filtered out before write in the main daily workflow
- `currency` is filled from Yahoo symbol metadata when possible, otherwise `"UNKNOWN"`

Notes:

- `volume` is normalized to `Float64` in the canonical daily contract, even when upstream providers return integral volume
- per-symbol files are the main downstream artifact surface

### Intraday Parquet Store

Primary path:

- `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`

Current intervals used by workflow code:

- preferred: default `5m`
- fallback: default `1m`

Schema:

| Column | Type |
|---|---|
| `date` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `adj_close` | `Float64` |
| `volume` | `Float64` |
| `currency` | `String` |

Current invariants:

- one symbol per file
- one interval per directory
- rows are sorted by `date`
- rolling retention window is applied
- rows with all OHLC values null are removed
- daily close comparison reads whichever interval file has the latest timestamp, preferring the preferred interval on ties

### Universe CSV

Produced by:

- `build_universe(...)`
- `tradinglab-data build-universe`

Current column order written by implementation:

| Column |
|---|
| `symbol` |
| `name` |
| `exchange` |
| `country` |
| `source` |
| `active` |
| `isin` |
| `index_memberships` |
| `needs_mapping` |

Semantics:

- `symbol`
  - canonicalized Yahoo-style symbol when mapping succeeds
- `active`
  - integer flag, usually `1`
- `source`
  - origin label for the constituent row
- `index_memberships`
  - comma-separated uppercase index names accumulated during merge
- `needs_mapping`
  - `1` when no tradable Yahoo-style symbol was derived and manual follow-up is needed

Consumer behavior when loading universes:

- `load_universe_frame(...)` requires a `symbol` column
- rows are uppercased and stripped
- rows with blank symbols or symbols containing whitespace or `$` are dropped
- if an `active` column exists, only rows with `active == 1` are kept
- ticker overrides are applied after load
- duplicate symbols introduced by overrides are deduplicated, keeping the first row

### Ticker Overrides CSV

Current expected columns:

- `raw`
- `yahoo`

Observed behavior:

- missing file, empty file, parse errors, or missing required columns all degrade to an empty override mapping

### Update Log CSV

Primary path:

- `<paths.update_log_csv>` or derived default

Header written by `append_update_log(...)`:

| Column |
|---|
| `timestamp` |
| `symbol` |
| `error` |
| `attempt_count` |

### Extended-Hours Alerts CSV

Primary path:

- `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_alerts.csv`

Produced from `detect_alerts(...)`, which filters the moves-vs-close frame. Current columns therefore follow the move summary shape:

| Column |
|---|
| `symbol` |
| `ref_close` |
| `last_price` |
| `pct_move` |
| `last_volume` |
| `currency` |
| `last_ts` |
| `session` |

### Extended-Hours HTML Report

Primary path:

- `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_report.html`

Contract:

- HTML artifact is generated for human inspection
- current content includes alerts and top movers derived from intraday-vs-close comparisons
- path and report purpose are contractual; exact markup and styling are not

### Verification Summary JSON

Produced by:

- `write_verification_summary(...)`

Summary producer:

- `run_parquet_sanity_checks(...)`

Current top-level keys returned by `run_parquet_sanity_checks(...)`:

- `ok`
- `status`
- `errors`
- `parquet_root`
- `file_count`
- `zero_byte`
- `sample_read_checked`
- `sample_read_failures`
- `coverage`
- `prev_file_count`
- `config`

## Python API Contract

This section records public names currently exposed by submodules. All names below are public in the Python sense because they do not start with `_`.

### `tradinglab_data.config`

- `default_config_path() -> Path`
- `packaged_config_example_text() -> str`
- `resolve_config_path(path) -> Path`
- `Config`
  - fields: `raw`, `source_path`
  - methods: `load(...)`, `get(...)`, `path(...)`
- `universe_csv_path(cfg) -> Path`
- `meta_root_path(cfg) -> Path`
- `universe_dir_path(cfg) -> Path`
- `update_log_path(cfg) -> Path`
- `ticker_overrides_path(cfg) -> Path`
- `parquet_root_path(cfg) -> Path`
- `intraday_root_path(cfg) -> Path`
- `runs_root_path(cfg) -> Path`
- `registry_root_path(cfg) -> Path`

### `tradinglab_data.schema`

- `schema_manifest() -> dict[str, object]`
- `render_schema_json() -> str`
- `render_schema_markdown() -> str`
- `validate_frame_schema(df, expected_schema, allow_extra_columns=True) -> None`
- `validate_daily_frame(df, allow_extra_columns=True) -> None`
- `validate_intraday_frame(df, allow_extra_columns=True) -> None`
- `validate_moves_frame(df, allow_extra_columns=True) -> None`
- `validate_alerts_frame(df, allow_extra_columns=True) -> None`

### `tradinglab_data.universe`

- `load_ticker_overrides(csv_path=None) -> dict[str, str]`
- `canonicalize_symbol(symbol, overrides=None) -> str`
- `load_universe_frame(csv_path, universe_dir=None, ticker_overrides_path=None) -> pl.DataFrame`
- `load_universe(csv_path, universe_dir=None, ticker_overrides_path=None) -> list[str]`

### `tradinglab_data.ticker_map`

- `normalize_to_yahoo(symbol, exchange, country, overrides_path=None) -> str`
- `clear_override_cache() -> None`

### `tradinglab_data.universe_build`

- `UniverseRow`
  - fields: `symbol`, `name`, `exchange`, `country`, `source`, `active`, `isin`, `index_memberships`, `needs_mapping`
- `build_universe(indices, out_path, active_only=True, overrides_dir=\"\", ticker_overrides_path=None) -> pl.DataFrame`

### `tradinglab_data.data_yf`

- `YFDownloadSpec`
  - fields: `symbol`, `interval`, `lookback_days`
- `fetch_yfinance_history(spec) -> pl.DataFrame`
- `fetch_symbol_currency(symbol) -> str | None`
- `fetch_yfinance_history_bulk(...) -> dict[str, pl.DataFrame]`
- `read_parquet_if_exists(path) -> pl.DataFrame | None`
- `upsert_symbol_parquet(symbol, interval, lookback_days, parquet_root) -> Path`
- `append_update_log(log_path, symbol, error, attempt_count) -> None`
- `clear_currency_cache() -> None`

### `tradinglab_data.data_stooq`

- `StooqDownloadSpec`
  - fields: `symbol`, `timeout_seconds`
- `infer_currency_from_symbol(symbol) -> str`
- `stooq_symbol_from_yahoo(symbol) -> str`
- `fetch_stooq_history(spec) -> pl.DataFrame`

### `tradinglab_data.parquet_verify`

- `ParquetVerifyConfig`
  - fields: `root`, `universe_dir`, `universes`, `min_parquet_files`, `max_zero_byte`, `max_missing_ratio`, `sample_read_files`, `max_drop_ratio`, `baseline_summary_path`
- `run_parquet_sanity_checks(cfg) -> VerifyResult`
- `write_verification_summary(path, summary) -> None`

### `tradinglab_data.contracts`

- `API_CONTRACT_VERSION`
- `CoverageEntry`
- `DailyCloseInfo`
- `ExtendedHoursResult`
- `MonitorExtendedHoursResult`
- `UpdateResult`
- `VerifyResult`
- `SessionLabel`
- `VerifyStatus`
- `OHLC_COLUMNS`
- `MOVE_FRAME_COLUMNS`
- `ALERT_FRAME_COLUMNS`

### `tradinglab_data.extended_hours_monitor`

- `fetch_extended_intraday(...) -> dict[str, pl.DataFrame]`
- `load_daily_reference_closes(symbols, daily_root) -> dict[str, DailyCloseInfo]`
- `compute_moves_vs_close(intraday_df, daily_close_map) -> pl.DataFrame`
- `detect_alerts(moves_df, threshold, min_volume=None) -> pl.DataFrame`
- `persist_alerts(alerts, path) -> Path`
- `summarize_gap_report(moves_df, threshold, min_volume=None, top_n=25, session_filter=\"all\") -> pl.DataFrame`
- `render_extended_hours_report_html(moves_df, alerts_df, threshold, generated_at=None, top_n=50, session_filter=\"all\") -> str`
- `persist_extended_hours_report_html(moves_df, alerts_df, path, threshold, top_n=50, session_filter=\"all\") -> Path`
- `update_extended_hours_store(...) -> ExtendedHoursResult`

### `tradinglab_data.workflows`

- `monitor_extended_hours_from_config(cfg, symbols_override=None, top_n=25, session_filter=\"all\") -> MonitorExtendedHoursResult`
- `update_from_config(cfg, symbols_override=None) -> UpdateResult`

## Behavioral Notes That Matter For Compatibility

- Symbol canonicalization is uppercase-oriented and override-driven.
- `build_universe(...)` merges duplicate members by `(symbol, isin)` before writing CSV output.
- The daily workflow may use either Yahoo Finance or Stooq depending on config.
- In Stooq mode, full history can come from Stooq while recent bars can still be merged from Yahoo Finance.
- The extended-hours workflow compares intraday last price to the latest daily regular-session close and assigns a session label of `pre`, `regular`, `post`, or `closed`.
- Current strict full-refresh handling is suffix-based and only applies to symbols ending in `.VI`.
- Installed package config discovery no longer assumes a source checkout; the wheel ships a bundled `config.yaml.example` template and typed marker file `py.typed`.

## Non-Goals Of This Contract

This document does not guarantee:

- provider availability or identical upstream data values over time
- stable progress-print formatting
- stable internals for `_...` helpers
- preservation of incidental implementation quirks that are not externally consumed

## Related Documents

- [`README.md`](../README.md)
- [`ARCHITECTURE.md`](../ARCHITECTURE.md)
- [`docs/BOUNDARY.md`](BOUNDARY.md)
- [`docs/PARQUET_SCHEMA.md`](PARQUET_SCHEMA.md)
- [`docs/WORKFLOWS.md`](WORKFLOWS.md)
