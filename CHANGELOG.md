# Changelog

## [0.2.0] - 2026-04-20

Second public release.

This release expands the package from daily and intraday equity maintenance into a broader multi-asset maintenance workflow with first-class crypto support.

Highlights:

- crypto OHLCV parquet maintenance under a dedicated canonical store layout
- dynamic crypto universe refresh from CoinGecko metadata
- Binance, Kraken, and Coinbase spot support through a shared CCXT adapter layer
- dedicated crypto CLI workflows for symbol listing, backfill, update, validate, refresh, show, diff, inspect, and prune
- store-wide integrity reporting extended to crypto stores with continuity, zero-volume, and metadata-consistency checks
- daily maintenance wrapper integration for crypto universe refresh, crypto updates, and crypto validation
- additive config, manifest, and schema-contract updates for crypto artifacts
- expanded test coverage for crypto workflows, CLI behavior, store reporting, packaging, and backward-compatibility expectations
- new consumer compatibility checklist for downstream adopters

## [0.1.0] - 2026-04-02

First public release.

This release provides a complete market-data maintenance workflow:

- daily OHLC parquet maintenance with a stable schema contract
- intraday parquet maintenance with extended-hours monitoring
- universe CSV generation, ticker normalization, and overrides
- integrity verification and store-wide reporting
- CLI workflows for daily maintenance, monitoring, universe building, schema inspection, and integrity reporting
- a Yahoo accessibility verifier for quick operational checks
