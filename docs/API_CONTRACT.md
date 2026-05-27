# Compatibility Contract

## Purpose

This document records the compatibility surface of `tradinglab-data` for consumers.

The package now uses a simpler model:

- package version is the official dependency compatibility signal
- `ARTIFACT_SCHEMA_VERSION` is the separate compatibility signal for on-disk parquet/report outputs

Consumers should not depend on a second standalone API-version number.

Current package version in [`pyproject.toml`](../pyproject.toml): `0.4.0`

## Compatibility Model

Use these signals for different needs:

- package version
  - use this for dependency pins and Python/CLI compatibility
  - example: `tradinglab-data>=0.4,<0.5`
- artifact schema version
  - use this when consuming parquet files or generated reports across package releases
  - current value: `v0.4.0`
- dataframe policy
  - use this when asserting the public tabular Python contract
  - current value: `polars-first`

Programmatic surface:

- `tradinglab_data.ARTIFACT_SCHEMA_VERSION`
- `tradinglab_data.DATAFRAME_POLICY`
- `tradinglab_data.contracts.ARTIFACT_SCHEMA_VERSION`
- `tradinglab_data.contracts.DATAFRAME_POLICY`
- `tradinglab_data.compatibility_manifest()`
- `tradinglab_data.schema.compatibility_manifest()`
- `tradinglab_data.schema.schema_manifest()["artifact_schema_version"]`
- `tradinglab_data.schema.schema_manifest()["dataframe_policy"]`

Recommended consumer model:

- depend on package version ranges for installed-package compatibility
- inspect `ARTIFACT_SCHEMA_VERSION` when validating data-store compatibility
- add consumer compatibility tests when another package depends on this one

## For Downstream Agents

This section is the shortest accurate description of what sibling packages and agents should assume about `tradinglab-data`.

What this package provides:

- canonical local data artifacts for daily stock/ETF history
- canonical local data artifacts for extended-hours intraday stock/ETF history
- canonical local data artifacts for regular-session intraday research stock/ETF history
- canonical local data artifacts for daily FX conversion pairs
- canonical local data artifacts for crypto OHLCV history
- canonical local data artifacts for point-in-time market capitalisation
- canonical local data artifacts for market index total returns
- canonical local GICS sector assignment artifacts
- authoritative symbol-master accounting metadata, exchange defaults, and symbol overrides
- universe metadata artifacts and ticker normalization behavior
- verification, integrity reporting, and maintenance wrappers around those artifacts

What downstream packages may rely on:

- daily parquet under `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday parquet under `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- intraday research parquet under `<intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet`
- intraday live parquet under `<intraday_live.live_root>/<INTERVAL>/<SYMBOL>.parquet`
- FX daily parquet under `<paths.fx_daily_root>/<PAIR>.parquet`
- crypto parquet under `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`
- market-cap parquet under `<paths.market_cap_root>/<SYMBOL>.parquet`
- sector assignments under `<paths.sector_assignments_csv>`
- index-return parquet under `<paths.index_returns_root>/<INDEX_ID>.parquet`
- authoritative symbol master under `<paths.meta_root>/symbol_master.csv`
- exchange defaults under `<paths.meta_root>/exchange_defaults.csv`
- symbol overrides under `<paths.meta_root>/symbol_overrides.csv`
- published parquet schemas in `docs/PARQUET_SCHEMA.md`
- public CLI entrypoints documented in this file
- public Python exports documented in this file
- additive config keys documented in this file
- `ARTIFACT_SCHEMA_VERSION` as the compatibility signal for on-disk parquet and report artifacts
- `DATAFRAME_POLICY == "polars-first"` as the compatibility signal for public tabular Python APIs

What downstream packages must not assume:

- that internal helper functions, fetch sequencing, retries, or repair logic are stable
- that report JSON keys or dirty-reason enums are closed to additive extension unless explicitly guaranteed here
- that all crypto universes are static; dynamic universes may be refreshed and persisted locally
- that maintenance wrappers are pure fetch commands; they may perform verification, repair, and gate failures
- that this package owns signal generation, screening, plotting UX, or research logic

What downstream packages should do:

- treat this package as the source of truth for maintained market-data artifacts
- load `symbol_master.csv` before portfolio simulation or accounting-sensitive workflows
- treat `fx_pair_to_base` as authoritative and use identity pairs such as `EUREUR` as a conversion factor of `1.0`
- consume the published artifact paths and schemas rather than re-deriving provider-specific formats
- consume public Python dataframe outputs as `polars.DataFrame`; pandas objects are not part of the public contract
- tolerate additive fields and additive CLI/config surface where this document does not promise exact closure
- pin package versions for Python/CLI compatibility
- check `ARTIFACT_SCHEMA_VERSION` when validating artifact compatibility
- keep their own integration tests that exercise their real usage of this package

If a sibling package needs to know whether a behavior is safe to depend on, this document is the primary source of truth, with `docs/PARQUET_SCHEMA.md` as the schema-level companion.

## Stability Boundary

The following are treated as part of the external contract:

- package versioned Python and CLI behavior
- YAML config keys consumed by CLI and workflow entrypoints
- on-disk artifact locations, file naming, and schemas
- public Python names that do not start with `_`
- declared typed result contracts and manifest metadata
- error behavior that consumer automation is likely to depend on

The following are not part of the external contract:

- functions, constants, and helpers whose names start with `_`
- internal fetch sequencing, retry strategy, and progress output details
- HTML report styling and presentation details, as long as the artifact path and basic purpose remain the same

## Machine-Readable Manifest

Primary manifest:

- `tradinglab_data.compatibility_manifest()`

Current top-level keys:

- `package_name`
- `python_package_name`
- `package_version`
- `artifact_schema_version`
- `dataframe_policy`
- `artifact_families`

`schema_manifest()` extends that manifest with:

- `daily`
- `intraday`
- `intraday_research`
- `intraday_live`
- `crypto`
- `fx_daily`
- `market_cap`
- `sector_assignments`
- `index_returns`
- `symbol_master`
- `notes`

Migration note:

- earlier pre-release revisions exposed `API_CONTRACT_VERSION`
- as of `0.1.0`, package compatibility is tracked by the package version itself
- on-disk parquet/report compatibility is tracked separately by `ARTIFACT_SCHEMA_VERSION`

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
- `crypto`
- `data_stooq`
- `data_yf`
- `extended_hours_monitor`
- `fx`
- `intraday_research`
- `intraday_live`
- `market_data`
- `market_data_workflows`
- `parquet_verify`
- `schema`
- `store_report`
- `symbol_master`
- `ticker_map`
- `universe`
- `universe_build`
- `workflows`

Additive top-level lazy re-exports are also available for commonly used public names, including:

- `ARTIFACT_SCHEMA_VERSION`
- `DATAFRAME_POLICY`
- `compatibility_manifest`
- `load_universe`
- `load_universe_frame`
- `build_universe`
- `run_parquet_sanity_checks`
- `schema_manifest`
- `render_schema_json`
- `render_schema_markdown`
- `generate_parquet_store_report`
- `load_symbol_master_frame`
- `load_symbol_master_map`
- `build_symbol_master_frame`
- `validate_symbol_master`
- `load_fx_pair`
- `get_universe_symbols`
- `get_total_returns`
- `get_adjusted_prices`
- `get_market_caps`
- `get_sector_assignments`
- `get_index_returns`
- `available_fx_pairs`
- `sync_market_data_from_config`
- `sync_market_caps_yahoo`
- `sync_sector_assignments_yahoo`
- `sync_index_returns_yahoo`
- `validate_market_data_from_config`
- `sync_fx_pair_yahoo`
- `crypto_backfill_from_config`
- `crypto_diff_universe_from_config`
- `crypto_inspect_from_config`
- `crypto_list_symbols_from_config`
- `crypto_prune_from_config`
- `crypto_refresh_universe_from_config`
- `crypto_show_universe_from_config`
- `crypto_validate_from_config`
- `validate_daily_frame`
- `validate_crypto_frame`
- `validate_intraday_frame`
- `validate_intraday_research_frame`
- `validate_intraday_live_frame`
- `validate_symbol_master_frame`
- `validate_fx_daily_frame`
- `validate_market_cap_frame`
- `validate_sector_assignment_frame`
- `validate_index_return_frame`
- `validate_moves_frame`
- `validate_alerts_frame`
- `update_from_config`
- `intraday_research_update_from_config`
- `intraday_research_validate_from_config`
- `intraday_research_inspect_from_config`
- `intraday_live_update_from_config`
- `intraday_live_validate_from_config`
- `intraday_live_inspect_from_config`
- `intraday_sync_from_config`
- `monitor_extended_hours_from_config`
- `UniverseRow`
- `UpdateResult`
- `ExtendedHoursResult`
- `IntradayDualSyncResult`
- `MonitorExtendedHoursResult`
- `VerifyResult`
- `StoreIntegrityReport`
- `StoreIntegritySection`
- `StoreIntegrityFileIssue`
- `StoreHistoryEntry`
- `CompatibilityManifest`
- `ArtifactFamilyEntry`

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
- is required in practice for `update`, `build-symbol-master`, `validate-symbol-master`, `inspect-symbol-master`, `fx-*`, `market-data ...`, `intraday ...`, `monitor-extended-hours`, `build-universe`, `report-parquet-store`, and `crypto ...` because those code paths load `Config`

## Accounting Metadata Contract

Authoritative CSV artifacts:

- `<paths.meta_root>/symbol_master.csv`
- `<paths.meta_root>/exchange_defaults.csv`
- `<paths.meta_root>/symbol_overrides.csv`

Required `symbol_master.csv` columns:

- `symbol`
- `exchange`
- `country`
- `asset_currency`
- `base_listing_currency`
- `tax_country`
- `asset_class`
- `fx_pair_to_base`
- `lot_size`
- `price_multiplier`

Important consumer rule:

- daily OHLC `currency` is diagnostic provider data and must not be treated as authoritative accounting metadata

Provenance rule:

- `symbol_master.csv` is still the consumer-facing accounting surface, but some required fields may be derived to satisfy the contract.
- `metadata_source` records whether a row used `universe`, `exchange_defaults`, and/or `symbol_overrides`.
- `metadata_quality` records whether fields were derived instead of sourced directly.
- `non_authoritative_country` means `country` was filled from `exchange_defaults.csv` and is fallback metadata rather than provider-authoritative source data.
- `non_authoritative_tax_country` means `tax_country` was filled from `exchange_defaults.csv` and is fallback metadata rather than provider-authoritative source data.
- Yahoo quote-page audits currently provide authoritative `symbol`, `name`, `exchange`, and `currency`, but not authoritative `country` or `tax_country`.

Resolution order for symbol-master construction:

1. universe CSV data
2. exchange defaults
3. symbol overrides

Overrides win over both provider metadata and exchange defaults.

## FX Daily Contract

Artifact path:

- `<paths.fx_daily_root>/<PAIR>.parquet`

Pair naming:

- `USDEUR` means EUR value of `1` USD
- `EURUSD` means USD value of `1` EUR
- consumers must not silently invert pair direction

Identity convention:

- `symbol_master.csv` uses explicit identity pairs such as `EUREUR`
- identity pairs do not require parquet files by default

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

### `build-symbol-master`

Usage:

```bash
tradinglab-data build-symbol-master [--base-currency CCY] [--universe-csv PATH] [--exchange-defaults PATH] [--symbol-overrides PATH] [--output PATH] [--strict|--no-strict]
```

Contract:

- builds the authoritative `symbol_master.csv` artifact from universe rows, exchange defaults, and symbol overrides
- writes to `--output` when provided, otherwise to `paths.symbol_master_csv`
- uses `--base-currency` to derive `fx_pair_to_base`, default `EUR`
- validates the produced frame when strict mode is enabled

### `validate-symbol-master`

Usage:

```bash
tradinglab-data validate-symbol-master [--path PATH] [--strict]
```

Contract:

- validates `symbol_master.csv` required columns, active-row metadata completeness, positive lot metadata, and explicit FX pair direction
- reads `--path` when provided, otherwise `paths.symbol_master_csv`
- exits non-zero when validation errors remain

### `inspect-symbol-master`

Usage:

```bash
tradinglab-data inspect-symbol-master [--path PATH] [--exchange EXCHANGE] [--fx-pair PAIR] [--issues ISSUE[,ISSUE...]] [--symbols SYMBOL ...] [--limit N] [--format markdown|json|csv] [--out PATH]
```

Contract:

- filters symbol-master rows for review without modifying artifacts
- supports markdown, JSON, and CSV output
- writes to stdout unless `--out` is provided

### `fx-backfill`

Usage:

```bash
tradinglab-data fx-backfill --pairs PAIR ... [--start DATE] [--end DATE] [--provider yahoo] [--allow-inverse|--no-allow-inverse]
```

Contract:

- fetches and writes daily FX parquet under `<paths.fx_daily_root>/<PAIR>.parquet`
- treats pair direction as explicit, e.g. `USDEUR` means EUR value of `1` USD
- may derive from the inverse Yahoo pair only when inverse fallback is allowed

### `fx-update`

Usage:

```bash
tradinglab-data fx-update [--pairs PAIR ...] [--provider yahoo] [--allow-inverse|--no-allow-inverse]
```

Contract:

- refreshes requested pairs or infers required non-identity pairs from `symbol_master.csv`
- preserves the same explicit pair-direction contract as `fx-backfill`

### `fx-validate`

Usage:

```bash
tradinglab-data fx-validate [--pairs PAIR ...]
```

Contract:

- validates local FX parquet files for required schema, pair direction, positive rates, sorted dates, and unique dates
- exits non-zero when validation errors remain

### `fx-inspect`

Usage:

```bash
tradinglab-data fx-inspect [--pairs PAIR ...] [--tail N]
```

Contract:

- inspects local FX parquet coverage and recent rows without modifying artifacts

### `update`

Usage:

```bash
tradinglab-data update [--symbols SYMBOL ...]
```

Contract:

- loads active symbols from configured universe inputs
- updates daily parquet under `paths.parquet_root`
- may also write intraday parquet and extended-hours artifacts when `extended_hours.enabled` is true
- accepts an optional symbol subset; unknown requested symbols are skipped with a warning, and an all-missing selection exits with an error
- returns a dict from the Python entrypoint `update_from_config(...)`; CLI itself returns exit code `0` when no exception is raised

### `market-data sync`

Usage:

```bash
tradinglab-data market-data sync [--symbols SYMBOL ...] [--index-ids INDEX ...] [--start DATE] [--end DATE] [--skip-market-caps] [--skip-sectors] [--skip-index-returns] [--allow-price-index-fallback]
```

Contract:

- writes market-cap parquet, sector assignments, and index-return parquet for downstream consumers
- accepts repeated values or comma-separated values for `--symbols` and `--index-ids`
- defaults index ids to all supported ids when `--index-ids` is omitted
- prints one `[MARKET_DATA_SYNC]` summary line per artifact family attempted

### `market-data validate`

Usage:

```bash
tradinglab-data market-data validate [--symbols SYMBOL ...] [--index-ids INDEX ...]
```

Contract:

- validates configured market-cap, sector-assignment, and index-return artifacts
- accepts repeated values or comma-separated values for `--symbols` and `--index-ids`
- prints `[MARKET_DATA_VALIDATE] ok=1` on success and exits non-zero with validation errors on failure

### `market-data inspect`

Usage:

```bash
tradinglab-data market-data inspect [--symbols SYMBOL ...] [--index-ids INDEX ...]
```

Contract:

- prints artifact existence, row count, and path for each requested market-data consumer artifact

### `intraday backfill`

Usage:

```bash
tradinglab-data intraday backfill [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- writes the dedicated intraday research parquet store under `<intraday.research_root>/5m/`
- uses the provider's full allowed `5m` window for both missing and existing symbols
- preserves older locally accumulated rows when `intraday.retention_days` is `0`

### `intraday update`

Usage:

```bash
tradinglab-data intraday update [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- resolves a pilot universe from `paths.universe_dir/<NAME>.csv` unless `--symbols` is provided
- currently supports only `5m`, `regular`, and `yahoo`
- normalizes UTC `timestamp` and exchange-local `session_date`
- incrementally refreshes existing files while keeping the research store separate from the extended-hours cache

### `intraday-live backfill`

Usage:

```bash
tradinglab-data intraday-live backfill [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- writes the session-aware intraday live parquet store under `<intraday_live.live_root>/5m/`
- uses `prepost=True`
- uses the provider's full allowed `5m` window for both missing and existing symbols

### `intraday-live update`

Usage:

```bash
tradinglab-data intraday-live update [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- resolves a stock/ETF live universe from `paths.universe_dir/<NAME>.csv` unless `--symbols` is provided
- currently supports only `5m` and `yahoo`
- persists explicit session labeling via `session` and `is_regular_session`

### `intraday-sync backfill`

Usage:

```bash
tradinglab-data intraday-sync backfill [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- performs one shared Yahoo `5m` fetch with `prepost=True`
- writes both `<intraday_live.live_root>/5m/` and `<intraday.research_root>/5m/`
- derives the research store from the same fetched frames rather than issuing a second provider call

### `intraday-sync update`

Usage:

```bash
tradinglab-data intraday-sync update [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- uses the configured live universe resolution path
- refreshes both live and research stores from one fetched symbol map
- requires matching `intraday_live` and `intraday` interval/provider/timezone settings

### `intraday-live validate`

Usage:

```bash
tradinglab-data intraday-live validate [--universe NAME] [--symbols SYMBOL ...]
```

### `intraday-live inspect`

Usage:

```bash
tradinglab-data intraday-live inspect [--universe NAME] [--symbols SYMBOL ...]
```

### `intraday validate`

Usage:

```bash
tradinglab-data intraday validate [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- validates local intraday research parquet files against the research-store schema and metadata rules
- exits non-zero when files are missing or invalid

### `intraday inspect`

Usage:

```bash
tradinglab-data intraday inspect [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- prints one line per symbol with existence, row count, validity, bounds, and target path

### `monitor-extended-hours`

Usage:

```bash
tradinglab-data monitor-extended-hours [--symbols SYMBOL ...] [--top-n N] [--session all|pre|post|regular|closed]
```

Contract:

- writes intraday parquet only
- reads reference closes from the daily parquet store
- writes alert CSV and HTML report under `<paths.runs_root>/YYYY-MM-DD/monitor/`
- accepts the same optional symbol filtering behavior as `update`

### `report-parquet-store`

Usage:

```bash
tradinglab-data report-parquet-store [--out-dir PATH] [--format both|json|markdown]
```

Contract:

- scans the full daily parquet store
- scans intraday parquet stores by interval directory when present
- scans crypto parquet stores by exchange/market-type/interval directory when present
- writes integrity reports under `<paths.runs_root>/YYYY-MM-DD/integrity/` unless `--out-dir` is given
- report filenames are `parquet_store_report.json` and `parquet_store_report.md`
- JSON report shape follows `StoreIntegrityReport`
- markdown report includes section summaries, retained-history detail, dirty files, and daily parquet sanity status

### `crypto list-symbols`

Usage:

```bash
tradinglab-data crypto list-symbols [--exchange EXCHANGE]
```

Contract:

- prints one canonical crypto symbol per line
- currently supports Binance, Kraken, and Coinbase spot through CCXT

### `crypto backfill`

Usage:

```bash
tradinglab-data crypto backfill --interval 1d|1h|15m [--exchange EXCHANGE] [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- writes crypto parquet under `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`
- keeps only closed bars in canonical history
- deduplicates on `timestamp`
- atomically replaces each parquet file after validation

### `crypto update`

Usage:

```bash
tradinglab-data crypto update --interval 1d|1h|15m [--exchange EXCHANGE] [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- reads existing crypto parquet when present
- fetches a recent overlap window and rewrites the merged file under the same artifact path
- skips symbols no longer tradable on the selected exchange instead of failing the batch
- leaves the local file unchanged when the overlap fetch produces no new closed bars

### `crypto validate`

Usage:

```bash
tradinglab-data crypto validate --interval 1d|1h|15m [--exchange EXCHANGE] [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- validates the local crypto parquet store against the crypto OHLCV contract
- exits non-zero when files are missing or invalid

### `crypto refresh-universe`

Usage:

```bash
tradinglab-data crypto refresh-universe [--exchange EXCHANGE] [--provider coingecko] [--universe NAME] [--limit N]
```

Contract:

- fetches ranked crypto metadata from CoinGecko
- filters out configured stablecoins, excluded ids/symbols, and optionally wrapped assets
- intersects candidates with configured exchange tradability for the configured quote asset
- writes the merged dynamic registry JSON and a per-universe JSON file

### `crypto show-universe`

Usage:

```bash
tradinglab-data crypto show-universe [--exchange EXCHANGE] [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- resolves the selected crypto universe or explicit symbol override set
- prints one canonical symbol per line

### `crypto diff-universe`

Usage:

```bash
tradinglab-data crypto diff-universe [--exchange EXCHANGE] --left-universe NAME --right-universe NAME
```

Contract:

- compares two resolved crypto universes after exchange and quote-asset filtering
- prints left-only, right-only, and shared canonical symbol sets

### `crypto inspect`

Usage:

```bash
tradinglab-data crypto inspect --interval 1d|1h|15m [--exchange EXCHANGE] [--universe NAME] [--symbols SYMBOL ...]
```

Contract:

- inspects local parquet presence and retained timestamp range for the resolved symbol set
- prints one line per symbol with existence, row count, bounds, and target path

### `crypto prune`

Usage:

```bash
tradinglab-data crypto prune --interval 1d|1h|15m [--exchange EXCHANGE] [--universe NAME] [--symbols SYMBOL ...] [--apply]
```

Contract:

- identifies local parquet files for the selected interval that are not present in the resolved symbol set
- prints candidate file paths always
- deletes them only when `--apply` is passed

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

- `paths.store_root`, or the legacy explicit pair `paths.universe_csv` and `paths.parquet_root`
- `paths.runs_root`

Derived path defaults:

- `paths.store_root`
  - defaults to `dirname(dirname(paths.parquet_root))` when `paths.parquet_root` is set
  - otherwise defaults to `dirname(dirname(paths.universe_csv))` when `paths.universe_csv` is set
- `paths.meta_root`
  - defaults to `dirname(paths.universe_csv)` when `paths.universe_csv` is set
  - otherwise defaults to `<paths.store_root>/meta`
- `paths.universe_csv`
  - defaults to `<meta_root>/universe_master.csv`
- `paths.universe_dir`
  - defaults to `<meta_root>/universes`
- `paths.update_log_csv`
  - defaults to `<meta_root>/update_log.csv`
- `paths.update_warning_state_json`
  - defaults to `<meta_root>/update_warning_state.json`
- `paths.ticker_overrides_csv`
  - defaults to `<meta_root>/ticker_overrides.csv`
- `paths.symbol_master_csv`
  - defaults to `<meta_root>/symbol_master.csv`
- `paths.exchange_defaults_csv`
  - defaults to `<meta_root>/exchange_defaults.csv`
- `paths.symbol_overrides_csv`
  - defaults to `<meta_root>/symbol_overrides.csv`
- `paths.fx_daily_root`
  - defaults to `<paths.store_root>/parquet/fx_daily`
- `paths.crypto_root`
  - defaults to sibling directory of daily parquet root: `<parent(paths.parquet_root)>/crypto`
- `paths.market_cap_root`
  - defaults to `<paths.store_root>/parquet/market_caps`
- `paths.sector_assignments_csv`
  - defaults to `<paths.meta_root>/sector_assignments.csv`
- `paths.index_returns_root`
  - defaults to `<paths.store_root>/parquet/index_returns`
- `paths.crypto_metadata_root`
  - defaults to `<meta_root>/crypto`
- `paths.crypto_registry_json`
  - defaults to `<paths.crypto_metadata_root>/registry.json`
- `paths.crypto_universe_dir`
  - defaults to `<paths.crypto_metadata_root>/universes`
- `extended_hours.intraday_root`
  - defaults to sibling directory of daily parquet root: `<parent(paths.parquet_root)>/intraday`
- `intraday.research_root`
  - defaults to sibling directory of daily parquet root: `<parent(paths.parquet_root)>/intraday_research`
- `intraday_live.live_root`
  - defaults to sibling directory of daily parquet root: `<parent(paths.parquet_root)>/intraday_live`
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
  - `log_repeat_cooldown_hours`
  - `pct_move_threshold`
  - `min_volume`
- `intraday.*`:
  - `enabled`
  - `research_root`
  - `interval`
  - `provider`
  - `session`
  - `exchange_timezone`
  - `default_universe`
  - `retention_days`
  - `chunk_size`
  - `sleep_seconds`
  - `max_retries`
  - `backoff_max_seconds`
  - `threads`
  - `log_repeat_cooldown_hours`
- `intraday_live.*`:
  - `enabled`
  - `live_root`
  - `interval`
  - `provider`
  - `exchange_timezone`
  - `default_universe`
  - `retention_days`
  - `chunk_size`
  - `sleep_seconds`
  - `max_retries`
  - `backoff_max_seconds`
  - `threads`
  - `log_repeat_cooldown_hours`
- `crypto.*`:
  - `provider`
  - `exchange`
  - `market_type`
  - `default_universe`
  - `quote_assets`
  - `max_batch_limit`
  - `incremental_lookback_bars`
  - `full_backfill_limit`
  - `validate_continuity`
  - `universe_refresh_provider`
  - `universe_refresh_limit`
  - `universe_refresh_pages`
  - `universe_refresh_min_market_cap`
  - `universe_refresh_min_volume`
  - `stablecoin_ids`
  - `excluded_symbols`
  - `excluded_ids`
  - `exclude_wrapped_assets`

Current interval support:

- `extended_hours.preferred_interval`
  - supported values: `5m`, `1m`
- `intraday.interval`
  - supported value in the first implementation: `5m`
- `intraday_live.interval`
  - supported value in the first implementation: `5m`

Intraday retention semantics:

- `extended_hours.retention_days: 0` means keep the full accumulated local intraday history
- positive values apply a rolling truncation window at write time
- default behavior is append-only local accumulation, bounded only by upstream fetch windows for newly missing history
- repeated Yahoo intraday symbol warnings are throttled through `<paths.update_warning_state_json>` using `extended_hours.log_repeat_cooldown_hours`
- `extended_hours.fallback_interval`
  - supported values: `5m`, `1m`
- unsupported values raise a clear `ValueError` at workflow runtime

Reference template:

- [`configs/config.yaml.example`](../configs/config.yaml.example)
- bundled wheel copy: [`src/tradinglab_data/config.yaml.example`](../src/tradinglab_data/config.yaml.example)

## Artifact Contract

Artifact schema version for the produced data-store and report families:

- `v0.4.0`

Machine-readable sources:

- `tradinglab_data.ARTIFACT_SCHEMA_VERSION`
- `tradinglab_data.compatibility_manifest()["artifact_schema_version"]`
- `tradinglab_data.schema.schema_manifest()["artifact_schema_version"]`

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
- per-symbol files are the primary artifact surface

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

### Intraday Research Parquet Store

Primary path:

- `<intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet`

Current first-iteration scope:

- interval: `5m`
- session: `regular`
- provider: `yahoo`

Schema:

| Column | Type |
|---|---|
| `timestamp` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `volume` | `Float64` |
| `currency` | `String` |
| `symbol` | `String` |
| `interval` | `String` |
| `provider` | `String` |
| `session` | `String` |
| `session_date` | `Date` |
| `is_regular_session` | `Boolean` |
| `ingested_at` | `Datetime` |

Current invariants:

- one symbol per file
- rows are sorted by `timestamp`
- `timestamp` is unique within a file
- `session` is `regular`
- `is_regular_session` is `true`
- `timestamp` and `ingested_at` are UTC-normalized datetimes
- `session_date` is derived in `America/New_York`

### Intraday Live Parquet Store

Primary path:

- `<intraday_live.live_root>/<INTERVAL>/<SYMBOL>.parquet`

Schema:

| Column | Type |
|---|---|
| `timestamp` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `volume` | `Float64` |
| `currency` | `String` |
| `symbol` | `String` |
| `interval` | `String` |
| `provider` | `String` |
| `session` | `String` |
| `session_date` | `Date` |
| `is_regular_session` | `Boolean` |
| `is_closed_bar` | `Boolean` |
| `ingested_at` | `Datetime` |

### Crypto Parquet Store

Primary path:

- `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`

Current intervals used by workflow code:

- `1d`
- `1h`
- `15m`

Schema:

| Column | Type |
|---|---|
| `timestamp` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `volume` | `Float64` |
| `provider` | `String` |
| `exchange` | `String` |
| `market_type` | `String` |
| `symbol` | `String` |
| `base_asset` | `String` |
| `quote_asset` | `String` |
| `interval` | `String` |
| `is_closed` | `Boolean` |
| `ingested_at` | `Datetime` |
| `source_symbol` | `String` |

Current invariants:

- one symbol per file
- one exchange, market type, and interval per directory subtree
- rows are sorted by `timestamp`
- `timestamp` is unique within a file
- canonical history persists closed bars only

### Crypto Metadata Registry

Primary paths:

- `<paths.crypto_registry_json>`
- `<paths.crypto_universe_dir>/<UNIVERSE>.json`

Contract:

- registry JSON stores merged dynamic crypto metadata entries keyed by canonical symbol
- universe JSON stores the selected symbols and refresh metadata for one dynamic universe

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
- duplicate symbols encountered during merged-universe normalization are merged field-by-field, preferring later non-empty metadata while unioning `index_memberships`

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

### Parquet Store Integrity Reports

Primary paths:

- `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.json`
- `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.md`

Producer:

- `generate_parquet_store_report(...)`
- `tradinglab-data report-parquet-store`

JSON report top-level keys:

- `generated_at`
- `config_path`
- `daily_root`
- `intraday_root`
- `crypto_root`
- `sections`
- `dirty_files`
- `parquet_sanity`
- `json_path`
- `markdown_path`

## Python API Contract

This section records public names currently exposed by submodules. All names below are public in the Python sense because they do not start with `_`.

Python API/CLI compatibility follows the package version rather than a second API-version number.

### `tradinglab_data.config`

- `default_config_path() -> Path`
- `packaged_config_example_text() -> str`
- `resolve_config_path(path) -> Path`
- `Config`
- `ConfigLike`
  - fields: `raw`, `source_path`
  - methods: `load(...)`, `get(...)`, `path(...)`
- `store_root_path(cfg) -> Path`
- `universe_csv_path(cfg) -> Path`
- `meta_root_path(cfg) -> Path`
- `universe_dir_path(cfg) -> Path`
- `update_log_path(cfg) -> Path`
- `ticker_overrides_path(cfg) -> Path`
- `symbol_master_path(cfg) -> Path`
- `exchange_defaults_path(cfg) -> Path`
- `symbol_overrides_path(cfg) -> Path`
- `parquet_root_path(cfg) -> Path`
- `crypto_root_path(cfg) -> Path`
- `fx_daily_root_path(cfg) -> Path`
- `market_cap_root_path(cfg) -> Path`
- `sector_assignments_path(cfg) -> Path`
- `index_returns_root_path(cfg) -> Path`
- `intraday_root_path(cfg) -> Path`
- `intraday_research_root_path(cfg) -> Path`
- `intraday_live_root_path(cfg) -> Path`
- `runs_root_path(cfg) -> Path`
- `registry_root_path(cfg) -> Path`

### `tradinglab_data.schema`

- `compatibility_manifest() -> CompatibilityManifest`
- `schema_manifest() -> dict[str, object]`
- `render_schema_json() -> str`
- `render_schema_markdown() -> str`
- `validate_frame_schema(df, expected_schema, allow_extra_columns=True) -> None`
- `validate_daily_frame(df, allow_extra_columns=True) -> None`
- `validate_crypto_frame(df, allow_extra_columns=True) -> None`
- `validate_intraday_frame(df, allow_extra_columns=True) -> None`
- `validate_intraday_research_frame(df, allow_extra_columns=True) -> None`
- `validate_intraday_live_frame(df, allow_extra_columns=True) -> None`
- `validate_market_cap_frame(df, allow_extra_columns=True) -> None`
- `validate_sector_assignment_frame(df, allow_extra_columns=True) -> None`
- `validate_index_return_frame(df, allow_extra_columns=True) -> None`
- `validate_moves_frame(df, allow_extra_columns=True) -> None`
- `validate_alerts_frame(df, allow_extra_columns=True) -> None`

### `tradinglab_data.market_data`

Generic Polars-first public data facade for sibling packages. It has no dependency on the private `tradinglab` package.

Configuration:

- resolves artifacts through normal config discovery, so callers should set `TRADINGLAB_DATA_CONFIG` or run with a valid default config
- returns only `polars.DataFrame` or built-in Python values
- accepts inclusive `start`/`end` date bounds
- omits non-trading days from returned `date` columns

Public functions:

- `get_universe_symbols(as_of=None, universe_id="default") -> list[str]`
- `get_adjusted_prices(symbols, start, end, max_ffill=5) -> polars.DataFrame`
- `get_total_returns(symbols, start, end, max_ffill=5) -> polars.DataFrame`
- `get_market_caps(symbols, start, end, frequency="monthly") -> polars.DataFrame`
- `get_sector_assignments(symbols, as_of=None) -> polars.DataFrame`
- `get_index_returns(index_ids, start, end) -> polars.DataFrame`

Behavioral contract:

- wide time-series frames contain a `date` column plus one numeric column per requested symbol or index id
- `get_sector_assignments` returns `symbol` and `sector` columns in the same order as requested symbols
- `get_total_returns` derives simple total returns from the same cleaned adjusted-price matrix as `get_adjusted_prices`
- missing symbols and unsupported index identifiers are dropped with logged warnings
- `DataNotFoundError` is raised when no requested data can be loaded
- `get_universe_symbols(as_of=...)` raises `DataNotFoundError` unless the universe artifact has point-in-time history columns
- `UniverseNotFoundError` is raised for unknown universe identifiers
- sector assignments must use the fixed 11-sector GICS vocabulary
- current-only sector artifacts emit `UserWarning` when `as_of` is provided
- index returns must be total returns; price-return fallback emits `UserWarning`

### `tradinglab_data.exceptions`

- `DataNotFoundError`
- `UniverseNotFoundError`

### `tradinglab_data.market_data_workflows`

Producer-side workflows for the artifact families consumed by `tradinglab_data.market_data`.

Public functions:

- `sync_market_data_from_config(cfg, symbols_override=None, index_ids=None, start=None, end=None, ...) -> dict[str, object]`
- `validate_market_data_from_config(cfg, symbols_override=None, index_ids=None) -> dict[str, object]`
- `inspect_market_data_from_config(cfg, symbols_override=None, index_ids=None) -> list[dict[str, object]]`
- `sync_market_caps_yahoo(symbols, daily_root, market_cap_root, start=None, end=None) -> dict[str, object]`
- `sync_sector_assignments_yahoo(symbols, output_path) -> dict[str, object]`
- `sync_index_returns_yahoo(index_ids, root, start=None, end=None, allow_price_fallback=False) -> dict[str, object]`
- `validate_market_cap_store(root, symbols=None) -> dict[str, object]`
- `validate_sector_assignment_file(path) -> dict[str, object]`
- `validate_index_return_store(root, index_ids=None) -> dict[str, object]`

Provider defaults:

- market caps use Yahoo shares outstanding history plus local daily USD close prices
- sector assignments use current Yahoo quote metadata and are therefore current-only unless replaced by a curated point-in-time CSV
- index total-return provider symbols are `SPX -> ^SP500TR`, `RTY -> ^RUTTR`, and `NDX -> ^XNDX`

### `tradinglab_data.intraday_research`

- `normalize_intraday_research_frame(...) -> pl.DataFrame`
- `update_intraday_research_store(...) -> IntradayResearchSyncResult`
- `inspect_intraday_research_store(...) -> list[dict[str, object]]`
- `validate_intraday_research_store(...) -> IntradayResearchValidateResult`

### `tradinglab_data.intraday_live`

- `normalize_intraday_live_frame(...) -> pl.DataFrame`
- `update_intraday_live_store(...) -> IntradayLiveSyncResult`
- `inspect_intraday_live_store(...) -> list[dict[str, object]]`
- `validate_intraday_live_store(...) -> IntradayLiveValidateResult`

### `tradinglab_data.crypto`

- `CRYPTO_UNIVERSES`
- `load_crypto_registry(exchange="binance", market_type="spot", quote_assets=("USDT",)) -> list[CryptoRegistryEntry]`
- `load_crypto_universes(cfg=None) -> dict[str, tuple[str, ...]]`
- `normalize_crypto_symbol(symbol) -> str`
- `resolve_crypto_universe(universe, exchange="binance", market_type="spot", quote_assets=("USDT",)) -> list[CryptoRegistryEntry]`
- `crypto_backfill_from_config(cfg, exchange=None, interval=..., universe=None, symbols_override=None, incremental=False) -> CryptoSyncResult`
- `crypto_diff_universe_from_config(cfg, exchange=None, left_universe=..., right_universe=...) -> dict[str, object]`
- `crypto_inspect_from_config(cfg, exchange=None, interval=..., universe=None, symbols_override=None) -> list[dict[str, object]]`
- `crypto_list_symbols_from_config(cfg, exchange=None) -> list[str]`
- `crypto_prune_from_config(cfg, exchange=None, interval=..., universe=None, symbols_override=None, apply=False) -> list[str]`
- `crypto_refresh_universe_from_config(cfg, exchange=None, provider_name=None, universe=None, limit=None) -> CryptoUniverseRefreshResult`
- `crypto_show_universe_from_config(cfg, exchange=None, universe=None, symbols_override=None) -> list[str]`
- `crypto_validate_from_config(cfg, exchange=None, interval=..., universe=None, symbols_override=None) -> CryptoValidateResult`

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
- `build_universe(indices, out_path, active_only=True, overrides_dir="", ticker_overrides_path=None) -> pl.DataFrame`

### `tradinglab_data.data_yf`

- `YFDownloadSpec`
  - fields: `symbol`, `interval`, `lookback_days`
- `fetch_yfinance_history(spec) -> pl.DataFrame`
- `fetch_symbol_currency(symbol) -> str | None`
- `fetch_yfinance_history_bulk(...) -> dict[str, pl.DataFrame]`
- `read_parquet_if_exists(path) -> pl.DataFrame | None`
- `upsert_symbol_parquet(symbol, interval, lookback_days, parquet_root) -> Path`
  - deprecated legacy single-symbol helper; it bypasses workflow-level currency resolution, sanitization policy, and post-write integrity checks
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

### `tradinglab_data.store_report`

- `render_store_integrity_report_markdown(report) -> str`
- `generate_parquet_store_report(cfg, out_dir=None, write_json=True, write_markdown=True) -> StoreIntegrityReport`

### `tradinglab_data.contracts`

- `PACKAGE_NAME`
- `PYTHON_PACKAGE_NAME`
- `ARTIFACT_SCHEMA_VERSION`
- `ArtifactFamilyEntry`
- `CompatibilityManifest`
- `CoverageEntry`
- `CryptoMetadataEntry`
- `CryptoRegistryEntry`
- `CryptoSyncResult`
- `CryptoUniverseRefreshResult`
- `CryptoValidateResult`
- `DailyCloseInfo`
- `ExtendedHoursResult`
- `IntradayResearchSyncResult`
- `IntradayResearchValidateResult`
- `IntradayLiveSyncResult`
- `IntradayLiveValidateResult`
- `MonitorExtendedHoursResult`
- `StoreHistoryEntry`
- `StoreIntegritySection`
- `StoreIntegrityFileIssue`
- `StoreIntegrityReport`
- `UpdateResult`
- `VerifyResult`
- `SessionLabel`
- `VerifyStatus`
- `OHLC_COLUMNS`
- `MOVE_FRAME_COLUMNS`
- `ALERT_FRAME_COLUMNS`

Typed result note:

- `ExtendedHoursResult` and `MonitorExtendedHoursResult` include live `polars.DataFrame` objects in `moves_df` and `alerts_df`
- these result objects are part of the in-process Python API contract, not a JSON wire contract

### `tradinglab_data.extended_hours_monitor`

- `fetch_extended_intraday(...) -> dict[str, pl.DataFrame]`
- `load_daily_reference_closes(symbols, daily_root) -> dict[str, DailyCloseInfo]`
- `compute_moves_vs_close(intraday_df, daily_close_map) -> pl.DataFrame`
- `detect_alerts(moves_df, threshold, min_volume=None) -> pl.DataFrame`
- `persist_alerts(alerts, path) -> Path`
- `summarize_gap_report(moves_df, threshold, min_volume=None, top_n=25, session_filter="all") -> pl.DataFrame`
- `render_extended_hours_report_html(moves_df, alerts_df, threshold, generated_at=None, top_n=50, session_filter="all") -> str`
- `persist_extended_hours_report_html(moves_df, alerts_df, path, threshold, top_n=50, session_filter="all") -> Path`
- `update_extended_hours_store(...) -> ExtendedHoursResult`

### `tradinglab_data.workflows`

- `monitor_extended_hours_from_config(cfg, symbols_override=None, top_n=25, session_filter="all") -> MonitorExtendedHoursResult`
- `backfill_extended_hours_from_config(cfg, interval, symbols_override=None) -> dict[str, object]`
- `update_from_config(cfg, symbols_override=None) -> UpdateResult`
- `intraday_research_update_from_config(cfg, universe=None, symbols_override=None, full_window=False) -> IntradayResearchSyncResult`
- `intraday_research_validate_from_config(cfg, universe=None, symbols_override=None) -> IntradayResearchValidateResult`
- `intraday_research_inspect_from_config(cfg, universe=None, symbols_override=None) -> list[dict[str, object]]`
- `intraday_live_update_from_config(cfg, universe=None, symbols_override=None, full_window=False) -> IntradayLiveSyncResult`
- `intraday_live_validate_from_config(cfg, universe=None, symbols_override=None) -> IntradayLiveValidateResult`
- `intraday_live_inspect_from_config(cfg, universe=None, symbols_override=None) -> list[dict[str, object]]`
- `intraday_sync_from_config(cfg, universe=None, symbols_override=None, full_window=False) -> IntradayDualSyncResult`

## Behavioral Notes That Matter For Compatibility

- Symbol canonicalization is uppercase-oriented and override-driven.
- `build_universe(...)` merges duplicate members by `(symbol, isin)` before writing CSV output.
- The daily workflow may use either Yahoo Finance or Stooq depending on config.
- In Stooq mode, full history can come from Stooq while recent bars can still be merged from Yahoo Finance.
- The extended-hours workflow compares intraday last price to the latest daily regular-session close and assigns a session label of `pre`, `regular`, `post`, or `closed`.
- Current strict full-refresh handling is suffix-based and only applies to symbols ending in `.VI`.
- Crypto workflows currently support Binance, Kraken, and Coinbase spot through CCXT, with Binance remaining the default config path.
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
