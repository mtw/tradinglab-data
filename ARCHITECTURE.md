# Architecture

## Purpose

`tradinglab-data` is the standalone artifact-production layer for market-data maintenance.
It is responsible for obtaining raw market data, normalizing it, and materializing deterministic local artifacts that other packages consume.

Primary artifacts:

- universe CSV files
- daily parquet store
- intraday parquet store
- verification summaries and maintenance reports

## Dependency Direction

Allowed direction:

- `tradinglab-data` -> upstream providers (`yfinance`, `stooq`, web sources for universe construction)
- downstream consumers -> `tradinglab-data` artifacts and package APIs

Disallowed direction:

- `tradinglab-data` importing research or prediction logic from downstream applications
- downstream research code fetching providers directly as an implicit fallback

## Module Map

- `src/tradinglab_data/data_yf.py`
  - Yahoo Finance download helpers
  - parquet read/update helpers
  - update log writing

- `src/tradinglab_data/data_stooq.py`
  - Stooq symbol mapping
  - Stooq CSV parsing
  - Stooq history fetch

- `src/tradinglab_data/ticker_map.py`
  - symbol normalization to Yahoo-compatible tickers
  - override CSV handling

- `src/tradinglab_data/universe.py`
  - universe CSV loading
  - active filtering
  - canonicalization through overrides

- `src/tradinglab_data/universe_build.py`
  - index constituent acquisition
  - merged universe construction
  - normalized universe CSV output

- `src/tradinglab_data/extended_hours_monitor.py`
  - intraday extended-hours retrieval
  - move-vs-close computation
  - alert generation
  - intraday parquet maintenance
  - HTML/CSV reporting for extended-hours monitoring

- `src/tradinglab_data/parquet_verify.py`
  - parquet store sanity checks
  - coverage thresholds
  - summary serialization helpers

- `src/tradinglab_data/schema.py`
  - canonical parquet schema definition
  - schema rendering helpers

- `src/tradinglab_data/workflows.py`
  - config-driven operational workflows
  - daily update orchestration
  - extended-hours monitor orchestration

- `src/tradinglab_data/cli.py`
  - standalone package CLI entrypoint

## Artifact Contract

The core contract is intentionally simple:

- one parquet file per symbol
- stable column schema across files of the same store kind
- stable file naming based on canonicalized symbol
- parquet files are the primary runtime data interface for downstream research/predict/screen/plot workflows

Detailed schema and constraints live in `docs/PARQUET_SCHEMA.md`.
The broader package/API compatibility snapshot lives in `docs/API_CONTRACT.md`.

## Operational Model

Daily update:

1. load active universe
2. canonicalize symbols
3. fetch missing full history
4. incrementally update existing history
5. refresh strict-symbol full histories when required
6. update extended-hours intraday cache
7. write alerts/report artifacts

Verification and repair:

1. run parquet sanity checks
2. compare against provider snapshots where configured
3. classify critical vs non-critical issues
4. optionally repair mismatches or rebuild specific files

## Design Constraints

- Provider instability is expected; workflows must degrade gracefully and log errors.
- Extended-hours intraday data is inherently sparse; policy should distinguish sparse but valid data from corruption.
- ETF history is allowed to be less strict than stock history where explicitly configured.
- The package should be reusable by multiple downstream codebases.

## Non-Goals

- strategy evaluation
- alpha research
- model training
- portfolio simulation
- registry or dashboard analysis not directly tied to data-maintenance outputs
