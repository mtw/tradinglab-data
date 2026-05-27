# Scope

`tradinglab-data` is a market-data maintenance package. It retrieves upstream data, normalizes it into canonical parquet artifacts, maintains universe metadata, and provides verification and integrity reporting for the local store.

The package is Polars-first. Public tabular Python APIs return `polars.DataFrame`, schema definitions use Polars dtypes, and pandas is allowed only as an external provider or parsing boundary before immediate conversion into Polars.

## Responsibilities

- acquire daily and intraday OHLC data from configured providers
- normalize provider data into the canonical parquet schema
- maintain per-symbol parquet history
- maintain separate extended-hours cache, regular-session intraday research, and session-aware intraday live stores
- build and merge universe CSVs
- apply ticker normalization and override mappings
- generate extended-hours alerts and HTML reports
- verify parquet integrity, schema conformance, and history coverage

## Public Artifacts

Primary artifacts produced by this package:

- daily parquet: `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday parquet: `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- intraday research parquet: `<intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet`
- intraday live parquet: `<intraday_live.live_root>/<INTERVAL>/<SYMBOL>.parquet`
- crypto parquet: `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`
- FX daily parquet: `<paths.fx_daily_root>/<PAIR>.parquet`
- market-cap parquet: `<paths.market_cap_root>/<SYMBOL>.parquet`
- sector assignments CSV: `<paths.sector_assignments_csv>`
- index-return parquet: `<paths.index_returns_root>/<INDEX_ID>.parquet`
- symbol master CSV: `<paths.meta_root>/symbol_master.csv`
- exchange defaults CSV: `<paths.meta_root>/exchange_defaults.csv`
- symbol overrides CSV: `<paths.meta_root>/symbol_overrides.csv`
- universe CSV: `<paths.universe_csv>` or `<paths.universe_dir>/*.csv`
- maintenance log: `<paths.update_log_csv>`
- extended-hours alerts/report: `<paths.runs_root>/YYYY-MM-DD/monitor/*`
- integrity reports: `<paths.runs_root>/YYYY-MM-DD/integrity/*`

## Contract Guidance

- Schema modifications require edits to `docs/PARQUET_SCHEMA.md`.
- Workflow modifications require edits to `docs/WORKFLOWS.md` when user-visible behavior changes.
- `ARTIFACT_SCHEMA_VERSION` tracks parquet/report compatibility across releases.
- Downstream packages should consume Polars frames from public Python APIs and must not rely on pandas objects as part of this package's public contract.
