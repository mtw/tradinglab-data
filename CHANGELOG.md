# Changelog

## [Unreleased]

Work targeting the next patch release, `0.4.3`, should be recorded here.

## [0.4.2] - 2026-05-31

Patch release.

This release keeps the `0.4.x` public API and artifact contracts intact while tightening validator behavior, metadata handling, and release documentation.

Highlights:

- remove the dead `_column_nonempty_count` helper
- fix `validate_intraday_research_frame` so non-`5m` intraday research frames validate correctly
- fix `validate_symbol_master_frame` so `strict=False` is a real tolerant mode
- fix `validate_fx_daily_frame` so row-level failures are counted once per row
- warn and surface explicit skip reasons for non-USD market-cap sync rows instead of silently dropping them
- align `UniverseNotFoundError` with the package exception hierarchy by subclassing `Exception`
- make return-threshold and history-tolerance checks configurable for downstream consumers
- stamp Yahoo sector assignments with their observation date instead of leaving point-in-time columns empty
- document the legacy thin intraday schema as compatibility-only and keep the release notes aligned with the `1m` live-store interval support
- update the release contract and changelog to match the `0.4.2` public package state

## [0.4.1] - 2026-05-28

Patch release.

This release keeps the `v0.4.0` artifact schema and public data contract intact while tightening release and CI behavior around the `0.4.x` package line.

Highlights:

- add GitHub Trusted Publishing through `.github/workflows/publish.yml`
- document the Trusted Publishing release flow and required PyPI publisher configuration
- remove public documentation wording that implied a separate private application repository
- fix Python 3.10 CI compatibility in intraday-fetch tests
- fix the docs policy test to ignore untracked `local_docs/` notes in CI
- bump package metadata to `0.4.1` while keeping `ARTIFACT_SCHEMA_VERSION` at `v0.4.0`

## [0.4.0] - 2026-05-27

Fourth public release.

This release adds the public market-data API and artifact contracts needed by downstream analytical packages, and makes the package's public tabular contract explicitly Polars-first.

Highlights:

- add the public `tradinglab_data.market_data` Polars-first facade for downstream packages
- add `market-data sync|validate|inspect` producer workflows for consumer artifacts
- add `DataNotFoundError` and `UniverseNotFoundError` public exception types
- add artifact contracts for market caps, sector assignments, and index total returns
- expose schema validators and config path helpers for the new artifact families
- convert the public market-data consumer API and producer-side market-data workflows to Polars-first dataframe handling
- document and export `DATAFRAME_POLICY == "polars-first"` in manifests, schema output, and compatibility docs
- remove public documentation wording that implied a separate private application repository
- fix Python 3.10 CI compatibility in intraday-fetch tests
- fix the docs policy test to ignore untracked `local_docs/` notes in CI
- bump package metadata and artifact schema contract to `0.4.0` / `v0.4.0`

## [0.3.1] - 2026-05-26

Maintenance release.

- fix EU intraday session classification for the canonical intraday research and live stores
- restore the missing `0.3.0` changelog entry
- bump package metadata for a follow-up patch release

## [0.3.0] - 2026-05-26

Third public release.

This release adds the first canonical `5m` intraday stores for research and live operational use, with a shared sync workflow that fetches Yahoo data once and writes both stores.

Highlights:

- dedicated regular-session `5m` intraday research parquet store
- session-aware `5m` intraday live parquet store
- shared `intraday-sync` workflow that fetches once and writes both intraday stores
- public CLI, config, schema, workflow, and API-contract documentation for the new intraday stores
- source-tree config templates aligned with the bundled package config template
- migrated legacy data maintenance scripts into this package
- broader parquet store reporting and validation coverage for configured intraday stores
- stronger empty-fetch, status-validation, crypto OHLCV, and network-smoke handling
- expanded regression coverage for intraday DST handling and maintenance workflows

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
