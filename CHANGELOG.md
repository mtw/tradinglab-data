# Changelog

## [0.1.0] - 2026-04-02

First public release.

This release provides a complete market-data maintenance workflow:

- daily OHLC parquet maintenance with a stable schema contract
- intraday parquet maintenance with extended-hours monitoring
- universe CSV generation, ticker normalization, and overrides
- integrity verification and store-wide reporting
- CLI workflows for daily maintenance, monitoring, universe building, schema inspection, and integrity reporting
- a Yahoo accessibility verifier for quick operational checks
