# Architecture

`tradinglab-data` is a market-data maintenance package. It retrieves upstream data, normalizes it into a canonical parquet schema, maintains a universe registry, and produces integrity reports and monitoring artifacts.

The package is Polars-first. Internal normalization, validation, parquet schema definitions, and public tabular Python APIs use `polars.DataFrame` and Polars dtypes. Pandas-shaped data is allowed only at external provider boundaries and must be converted into Polars before entering the package's canonical workflow logic.

## Data Flow

1. Universe inputs define the target symbol set.
2. Provider adapters fetch daily, extended-hours intraday, intraday research, and intraday live history.
3. Crypto provider adapters fetch exchange-native OHLCV into a dedicated crypto parquet store.
4. Normalization enforces the canonical schema.
5. Per-symbol parquet files are written and verified.
6. Intraday sync can fetch Yahoo `5m` data once and write both live and research stores.
7. Extended-hours monitoring computes moves and writes alerts/reports.
8. Store-wide audits summarize integrity and coverage.

## Module Map

- `src/tradinglab_data/data_yf.py`: Yahoo Finance history and log helpers
- `src/tradinglab_data/data_stooq.py`: Stooq history and CSV parsing helpers
- `src/tradinglab_data/crypto/`: crypto registry, provider adapters, CoinGecko metadata refresh, storage, validation, and workflows
- `src/tradinglab_data/ticker_map.py`: ticker normalization and overrides
- `src/tradinglab_data/universe.py`: universe loading and canonicalization
- `src/tradinglab_data/universe_build.py`: index fetchers and merged universe construction
- `src/tradinglab_data/extended_hours_monitor.py`: intraday orchestration and alert/report outputs
- `src/tradinglab_data/intraday_research.py`: regular-session `5m` research parquet normalization, writes, validation, and inspection
- `src/tradinglab_data/intraday_live.py`: session-aware `5m` live parquet normalization, writes, validation, and inspection
- `src/tradinglab_data/market_data.py`: Polars-first public consumer facade for adjusted prices, returns, market caps, sectors, index returns, and universes
- `src/tradinglab_data/market_data_workflows.py`: Polars-first sync, validation, and inspection workflows for consumer market-data artifacts
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
- Extended-hours, intraday research, and intraday live data are separate stores with separate semantics.
- Intraday data is stored under interval-specific directories.
- The shared `intraday-sync` workflow derives regular-session research bars from the same fetched frames used for the live store.
- Crypto data is stored under exchange/market-type/interval-specific directories.
- Provider adapters are isolated behind normalization helpers for consistency.
- Pandas is not part of the public dataframe contract; it is an upstream adapter detail only.
