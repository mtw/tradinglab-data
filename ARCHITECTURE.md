# Architecture

`tradinglab-data` is a market-data maintenance package. It retrieves upstream data, normalizes it into a canonical parquet schema, maintains a universe registry, and produces integrity reports and monitoring artifacts.

## Data Flow

1. Universe inputs define the target symbol set.
2. Provider adapters fetch daily and intraday history.
3. Normalization enforces the canonical schema.
4. Per-symbol parquet files are written and verified.
5. Extended-hours monitoring computes moves and writes alerts/reports.
6. Store-wide audits summarize integrity and coverage.

## Module Map

- `src/tradinglab_data/data_yf.py`: Yahoo Finance history and update log helpers
- `src/tradinglab_data/data_stooq.py`: Stooq history and CSV parsing helpers
- `src/tradinglab_data/ticker_map.py`: ticker normalization and overrides
- `src/tradinglab_data/universe.py`: universe loading and canonicalization
- `src/tradinglab_data/universe_build.py`: index fetchers and merged universe construction
- `src/tradinglab_data/extended_hours_monitor.py`: intraday orchestration and alert/report outputs
- `src/tradinglab_data/_intraday_fetch.py`: intraday Yahoo fetch helpers
- `src/tradinglab_data/_move_compute.py`: move-vs-close computation
- `src/tradinglab_data/_alert_report.py`: alert filtering and HTML rendering
- `src/tradinglab_data/parquet_verify.py`: parquet sanity checks
- `src/tradinglab_data/store_report.py`: store-wide integrity reporting
- `src/tradinglab_data/schema.py`: canonical schema definitions and renderers
- `src/tradinglab_data/workflows.py`: config-driven daily and intraday workflows
- `src/tradinglab_data/cli.py`: CLI entrypoints

## Contract Surface

- Schema and artifact contract: `docs/PARQUET_SCHEMA.md`
- CLI and config contract: `docs/API_CONTRACT.md`
- Workflow behavior: `docs/WORKFLOWS.md`

## Design Notes

- Per-symbol parquet files are the primary persistence layer.
- Intraday data is stored under interval-specific directories.
- Provider adapters are isolated behind normalization helpers for consistency.
