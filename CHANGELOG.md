# Changelog

## Unreleased

## [0.1.0] - 2026-03-28

### Added

- formal package documentation for architecture, workflows, parquet schema, release, and compatibility expectations
- typed result contracts, dataframe validation helpers, and a machine-readable `compatibility_manifest()`
- standalone parquet-store integrity reporting with JSON and markdown outputs
- GitHub CI for tests, schema CLI smoke checks, build validation, and distribution checks
- test infrastructure via `tests/__init__.py`, `tests/conftest.py`, and expanded workflow/report regression coverage
- `ARTIFACT_SCHEMA_VERSION` to track parquet/report compatibility independently from package release cadence

### Changed

- extracted shared OHLC helpers and Yahoo-specific helpers into reusable internal modules
- refactored workflow intraday config handling into `_IntradayConfig` and `_read_intraday_config()`
- promoted intraday interval updates to a module-level `_update_intraday_interval()` helper
- made package config discovery wheel-safe and portable outside of a source checkout
- clarified the public package surface, exported typed/public helpers lazily, and documented the current compatibility model
- updated the schema markdown title from `TradingLab Data Parquet Schema` to `Data Parquet Schema`

### Fixed

- aligned Stooq daily `volume` typing with the canonical parquet schema
- fixed ticker override caching behavior and added explicit cache reset hooks
- removed pandas from parquet verification and standardized on polars
- fixed extended-hours report and store-report write ordering regressions
- fixed workflow post-write integrity invocation and incremental Yahoo fetch window sizing
- made empty move/alert report frames conform to the documented 8-column contract
- ensured release artifacts include the MIT notice while keeping local `build` and `twine check` validation green

### Migration Notes

- pre-release revisions exposed `API_CONTRACT_VERSION`; `0.1.0` removes that name in favor of package-version compatibility plus `ARTIFACT_SCHEMA_VERSION` for on-disk artifacts
