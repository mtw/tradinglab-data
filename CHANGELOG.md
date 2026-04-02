# Changelog

## Unreleased

- Rewrote user-facing documentation to present `tradinglab-data` as a standalone package with a positive, self-contained scope.

## [0.1.0] - 2026-03-28

### Added

- formal package documentation for architecture, workflows, parquet schema, release, and compatibility expectations
- typed result contracts, dataframe validation helpers, and a machine-readable `compatibility_manifest()`
- standalone parquet-store integrity reporting with JSON and markdown outputs
- GitHub CI for tests, schema CLI smoke checks, build validation, and distribution checks
- test infrastructure via `tests/__init__.py`, `tests/conftest.py`, and expanded workflow/report regression coverage
- `ARTIFACT_SCHEMA_VERSION` to track parquet/report compatibility independently from package release cadence
- `scripts/verify_yahoo_access.py`, a lightweight Yahoo Finance accessibility verifier that samples configured universe symbols and probes multiple intervals before a full update run

### Changed

- extracted shared OHLC helpers and Yahoo-specific helpers into reusable internal modules
- refactored workflow intraday config handling into `_IntradayConfig` and `_read_intraday_config()`
- promoted intraday interval updates to a module-level `_update_intraday_interval()` helper
- made package config discovery wheel-safe and portable outside of a source checkout
- clarified the public package surface, exported typed/public helpers lazily, and documented the current compatibility model
- updated the schema markdown title from `TradingLab Data Parquet Schema` to `Data Parquet Schema`
- invalid extended-hours intraday intervals now fail fast with a clear `ValueError`
- refactored the update workflow into provider-specific internal runners and aligned intraday update plumbing around `_IntradayConfig`
- replaced hardcoded universe index dispatch with an internal fetcher registry
- switched the universe fetcher registry to typed callable entries instead of string-based global lookup
- routed explicit extended-hours monitoring through the shared intraday runner to keep config plumbing in one place
- split shared intraday execution from the update-only wrapper so monitor and update flows no longer depend on boolean control flags
- reduced duplicated Yahoo update logic with shared bulk-fetch retry and parquet-write helpers
- vectorized move-vs-close computation across symbols and consolidated the daily/intraday OHLC schema aliases around one definition
- made config compatibility aliases lazy, introduced a structural config protocol, and added explicit typing for `Config.get(...)`
- split extended-hours fetch, move computation, and HTML/report writing into dedicated internal modules while preserving the public monitoring API
- further decomposed the Yahoo update runner into explicit new-symbol, incremental-merge, and strict-symbol write helpers
- widened Ruff from Pyflakes-only to a broader import/upgrade/style gate, and replaced workflow `cfg: Any` usage with a structural config protocol so mypy can run without broad suppressions
- replaced Python callback-based symbol override application with a native Polars join/coalesce path
- added a minimum `lxml` version floor and developer extras for `ruff`, `mypy`, and `pytest-cov`
- reverted the tracked checkout config to the generic `./var/...` layout and documented the untracked `configs/config.local.yaml` override pattern for machine-specific sibling-store paths
- made `scripts/run_daily_update_verify.sh` prefer `configs/config.local.yaml` automatically when present, while still allowing `TLD_CONFIG_PATH` to override it explicitly
- changed the Yahoo accessibility verifier to sample different symbols on each invocation by default, with optional `--seed` support for reproducible debugging
- switched schema typing to Polars’ public `PolarsDataType` alias and exported `ConfigLike` as part of the package surface

### Fixed

- aligned Stooq daily `volume` typing with the canonical parquet schema
- fixed ticker override caching behavior and added explicit cache reset hooks
- removed pandas from parquet verification and standardized on polars
- fixed extended-hours report and store-report write ordering regressions
- fixed workflow post-write integrity invocation and incremental Yahoo fetch window sizing
- made empty move/alert report frames conform to the documented 8-column contract
- ensured release artifacts include the MIT notice while keeping local `build` and `twine check` validation green
- added locking around module-level currency and override caches for safer concurrent access
- tightened override-cache locking so file loads and cache population stay in one critical section
- avoided mutating global random state in parquet verification and replaced Python-side date sorting checks in store audits with native Polars operations
- narrowed silent CSV/source loading failures into explicit warnings for unexpected universe and override input errors
- added explicit `@pytest.mark.network` live smoke coverage and made those tests skip cleanly when upstreams block or return no data
- now treat name-only ATX fallback rows as mapping-required
- marked `upsert_symbol_parquet(...)` as deprecated so the legacy single-symbol path does not masquerade as a first-class workflow API
- captured noisy `yfinance` stderr/stdout during Yahoo downloads and now classify DNS/connectivity failures as explicit Yahoo connectivity errors instead of leaking misleading `possibly delisted` messages into maintenance logs
- made Yahoo download capture return any raised exception for consistent connectivity classification, and classified `possibly delisted` output explicitly

### Migration Notes

- pre-release revisions exposed `API_CONTRACT_VERSION`; `0.1.0` removes that name in favor of package-version compatibility plus `ARTIFACT_SCHEMA_VERSION` for on-disk artifacts
