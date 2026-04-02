# Scope

`tradinglab-data` is a market-data maintenance package. It retrieves upstream data, normalizes it into canonical parquet artifacts, maintains universe metadata, and provides verification and integrity reporting for the local store.

## Responsibilities

- acquire daily and intraday OHLC data from configured providers
- normalize provider data into the canonical parquet schema
- maintain per-symbol parquet history
- build and merge universe CSVs
- apply ticker normalization and override mappings
- generate extended-hours alerts and HTML reports
- verify parquet integrity, schema conformance, and history coverage

## Public Artifacts

Primary artifacts produced by this package:

- daily parquet: `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday parquet: `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- universe CSV: `<paths.universe_csv>` or `<paths.universe_dir>/*.csv`
- maintenance log: `<paths.update_log_csv>`
- extended-hours alerts/report: `<paths.runs_root>/YYYY-MM-DD/monitor/*`
- integrity reports: `<paths.runs_root>/YYYY-MM-DD/integrity/*`

## Contract Guidance

- Schema modifications require edits to `docs/PARQUET_SCHEMA.md`.
- Workflow modifications require edits to `docs/WORKFLOWS.md` when user-visible behavior changes.
- `ARTIFACT_SCHEMA_VERSION` tracks parquet/report compatibility across releases.
