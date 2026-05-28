# tradinglab-data

`tradinglab-data` is a Polars-first market-data maintenance toolkit for building canonical parquet data stores across daily, intraday, FX, crypto, market-cap, sector, and index-return workflows.
It retrieves upstream market data, normalizes it into versioned parquet contracts, maintains universe metadata, and verifies the local data store consumed by downstream applications.

This repository owns:

- universe loading, merged-universe construction, ticker normalization, and override mapping
- upstream provider retrieval for stock/ETF daily, stock/ETF intraday, and crypto OHLCV data
- canonical parquet schemas and storage layouts
- daily parquet workflows
- extended-hours monitoring cache and reports
- regular-session intraday research store
- session-aware intraday live store
- crypto universe and parquet workflows
- store integrity, universe consistency, and schema verification tooling

This repository does not own signal generation, screening decisions, research workflows, predictive modeling, experiment registries, or downstream plotting/report UX.

Latest PyPI release: `0.4.1`

Current development version: `0.4.2`

Artifact schema version: `v0.4.0`

Dataframe policy: `polars-first`

`tradinglab-data` is Polars-first. Public tabular Python APIs return `polars.DataFrame` objects, persisted schemas are expressed with Polars dtypes, and pandas is permitted only at external provider or ingestion boundaries such as Yahoo Finance and HTML table parsing before immediate normalization into Polars.

## Install

From PyPI:

```bash
pip install tradinglab-data
```

For local development:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test,dev]"
```

Use from another local checkout:

```bash
pip install -e /path/to/tradinglab-data
```

The console command is:

```bash
tradinglab-data --help
```

The module entrypoint also works:

```bash
PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown
```

## Configuration

Most commands require a YAML config. The `schema` command is the main exception.

Config discovery order:

1. `--config <path>`
2. `TRADINGLAB_DATA_CONFIG`
3. source-tree `configs/config.yaml` when running from a checkout
4. `./config.yaml`
5. `./configs/config.yaml`

Templates:

- `configs/config.yaml.example`
- `src/tradinglab_data/config.yaml.example`

Keep tracked configs generic. Put machine-specific paths in an untracked local override:

```bash
export TRADINGLAB_DATA_CONFIG=configs/config.local.yaml
tradinglab-data update
```

The local maintenance wrappers prefer `TLD_CONFIG_PATH`, then `configs/config.local.yaml`, then `configs/config.yaml`.

```bash
TLD_CONFIG_PATH=configs/config.local.yaml ./scripts/run_daily_update_verify.sh
```

## Artifact Families

| Family | Store Layout | Notes |
|---|---|---|
| Daily stock/ETF | `<paths.parquet_root>/<SYMBOL>.parquet` | regular-session daily OHLCV with listing currency |
| Extended-hours intraday cache | `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet` | monitoring cache, currently `5m` and `1m` |
| Intraday research | `<intraday.research_root>/5m/<SYMBOL>.parquet` | regular-session-only US stock/ETF `5m` bars |
| Intraday live | `<intraday_live.live_root>/5m/<SYMBOL>.parquet` | session-aware US stock/ETF `5m` bars labeled `pre`, `regular`, `post`, or `unknown` |
| Crypto | `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet` | exchange-native closed OHLCV bars |
| FX daily | `<paths.fx_daily_root>/<PAIR>.parquet` | explicit source-to-target daily conversion pairs such as `USDEUR` |
| Market caps | `<paths.market_cap_root>/<SYMBOL>.parquet` | point-in-time market capitalisation in USD millions |
| Sector assignments | `<paths.sector_assignments_csv>` | GICS sector assignments using the fixed 11-sector vocabulary |
| Index returns | `<paths.index_returns_root>/<INDEX_ID>.parquet` | daily total returns for supported market indices |
| Symbol master | `<paths.meta_root>/symbol_master.csv` | authoritative accounting metadata for downstream consumers |
| Exchange defaults | `<paths.meta_root>/exchange_defaults.csv` | maintained exchange-level metadata defaults |
| Symbol overrides | `<paths.meta_root>/symbol_overrides.csv` | last-wins per-symbol metadata overrides |
| Universe metadata | `<paths.universe_csv>` and `<paths.universe_dir>/<UNIVERSE>.csv` | canonical public CSV artifacts |
| Dynamic crypto universes | `<paths.crypto_universe_dir>/<UNIVERSE>.json` | persisted universe selections from metadata refreshes |

Schema details live in `docs/PARQUET_SCHEMA.md`.
Consumer compatibility details live in `docs/API_CONTRACT.md`.

The market-data consumer facade is available from `tradinglab_data.market_data`.
Producer workflows for market caps, sector assignments, and index total returns are available via:

```bash
tradinglab-data --config /path/to/config.yaml market-data sync
tradinglab-data --config /path/to/config.yaml market-data validate
tradinglab-data --config /path/to/config.yaml market-data inspect
```

## Core Commands

Inspect the published schema:

```bash
tradinglab-data schema --format markdown
tradinglab-data schema --format json --out schema.json
```

Build or normalize universe inputs:

```bash
tradinglab-data build-universe --indices sp500 djia dax mdax atx --out <paths.universe_csv>
python scripts/normalize_universe_schema.py --config configs/config.local.yaml
python scripts/build_index_override.py --help
```

Run simple parquet health checks:

```bash
python scripts/parquet_status.py --config configs/config.local.yaml --daily
python scripts/parquet_status.py --config configs/config.local.yaml --intraday --universe intraday_live_core
python scripts/parquet_status.py --config configs/config.local.yaml --list-universes
```

The wrapper uses different validation paths on purpose:
- `--daily` uses the legacy file-level OHLC checker plus universe completeness reporting.
- `--intraday` uses the schema-aware research/live store validators and completeness checks for the requested universe shard.

Universe-selecting scripts support `--list-universes` so you can discover available equity and crypto universe names before choosing `--universe`, `--indices`, or `--only-universes`.

Build and validate the authoritative symbol master:

```bash
tradinglab-data --config configs/config.local.yaml build-symbol-master --base-currency EUR
tradinglab-data --config configs/config.local.yaml validate-symbol-master
tradinglab-data --config configs/config.local.yaml inspect-symbol-master --exchange VIE --issues defaulted_country
```

Maintain daily FX parquet:

```bash
tradinglab-data --config configs/config.local.yaml fx-backfill --pairs USDEUR CHFEUR GBPEUR
tradinglab-data --config configs/config.local.yaml fx-update
tradinglab-data --config configs/config.local.yaml fx-validate
tradinglab-data --config configs/config.local.yaml fx-inspect
```

`symbol_master.csv` is the authoritative accounting metadata surface. Daily OHLC `currency` remains provider-derived diagnostic data. When `metadata_quality` contains `non_authoritative_country` or `non_authoritative_tax_country`, those fields were derived from `exchange_defaults.csv` as fallback metadata rather than sourced directly from Yahoo/provider metadata.

Maintain daily stock/ETF parquet:

```bash
tradinglab-data --config configs/config.local.yaml update
tradinglab-data --config configs/config.local.yaml update --symbols AAPL MSFT
```

The daily update flow loads active universe symbols, applies ticker overrides, migrates renamed files where possible, fetches missing or recent history, upserts canonical parquet files, and can optionally refresh the legacy extended-hours cache and reports.

Maintain extended-hours monitoring data:

```bash
tradinglab-data --config configs/config.local.yaml monitor-extended-hours --session pre --top-n 25
tradinglab-data --config configs/config.local.yaml backfill-extended-hours --interval 5m
```

The extended-hours cache is append-oriented by default. Set `extended_hours.retention_days` only when a rolling cache is intentional.

Maintain the regular-session intraday research store:

```bash
tradinglab-data --config configs/config.local.yaml intraday backfill --universe intraday_pilot
tradinglab-data --config configs/config.local.yaml intraday update --universe intraday_pilot
tradinglab-data --config configs/config.local.yaml intraday validate --universe intraday_pilot
tradinglab-data --config configs/config.local.yaml intraday inspect --universe intraday_pilot
```

Maintain the session-aware intraday live store:

```bash
tradinglab-data --config configs/config.local.yaml intraday-live backfill --universe intraday_live_core
tradinglab-data --config configs/config.local.yaml intraday-live update --universe intraday_live_core
tradinglab-data --config configs/config.local.yaml intraday-live validate --universe intraday_live_core
tradinglab-data --config configs/config.local.yaml intraday-live inspect --universe intraday_live_core
```

Refresh live and research intraday stores from one shared Yahoo fetch:

```bash
tradinglab-data --config configs/config.local.yaml intraday-sync backfill --universe intraday_live_core
tradinglab-data --config configs/config.local.yaml intraday-sync update --universe intraday_live_core
```

The sync workflow fetches Yahoo `5m` data once with `prepost=True`, writes the live store, and derives the regular-session research store from the same fetched frames.

Maintain crypto parquet:

```bash
tradinglab-data --config configs/config.local.yaml crypto list-symbols --exchange binance
tradinglab-data --config configs/config.local.yaml crypto refresh-universe --provider coingecko --universe crypto_high_liquidity
tradinglab-data --config configs/config.local.yaml crypto backfill --exchange binance --interval 1d --universe crypto_majors
tradinglab-data --config configs/config.local.yaml crypto update --exchange binance --interval 1h --universe crypto_high_liquidity
tradinglab-data --config configs/config.local.yaml crypto validate --exchange binance --interval 15m --universe crypto_high_liquidity
tradinglab-data --config configs/config.local.yaml crypto show-universe --universe crypto_high_liquidity
tradinglab-data --config configs/config.local.yaml crypto diff-universe --left-universe crypto_majors --right-universe crypto_high_liquidity
tradinglab-data --config configs/config.local.yaml crypto inspect --exchange binance --interval 1h --universe crypto_high_liquidity
tradinglab-data --config configs/config.local.yaml crypto prune --exchange binance --interval 1h --universe crypto_high_liquidity --apply
```

Supported crypto workflow intervals are currently `1d`, `1h`, and `15m`.

Audit local artifacts:

```bash
tradinglab-data --config configs/config.local.yaml report-parquet-store
tradinglab-data --config configs/config.local.yaml report-universe-consistency --dataset daily --instrument-type stock
tradinglab-data --config configs/config.local.yaml report-universe-consistency --dataset intraday --interval 5m --instrument-type stock
tradinglab-data --config configs/config.local.yaml report-universe-consistency --dataset crypto --interval 1h --universe crypto_core
python scripts/parquet_status.py --config configs/config.local.yaml --daily
python scripts/parquet_status.py --config configs/config.local.yaml --intraday --universe intraday_live_core
```

## Maintenance Scripts

Daily update and verification wrapper:

```bash
TLD_CONFIG_PATH=configs/config.local.yaml ./scripts/run_daily_update_verify.sh
```

Dedicated crypto update and verification wrapper:

```bash
TLD_CONFIG_PATH=configs/config.local.yaml ./scripts/run_crypto_update_verify.sh
```

Crypto status checker:

```bash
python scripts/check_crypto_status.py --config configs/config.local.yaml --interval 1h --universe crypto_core --repair --fail-on-issues
```

Yahoo provider diagnostic:

```bash
python scripts/verify_yahoo_access.py --config configs/config.local.yaml --sample-size 15 --intervals 1d,5m,1m
```

Yahoo quote metadata audit for ETF source rows:

```bash
python scripts/audit_yahoo_quote_metadata.py --config configs/config.local.yaml --format markdown
python scripts/audit_yahoo_quote_metadata.py --config configs/config.local.yaml --symbols MVEU.L IPLT.L --format json --out /tmp/yahoo_quote_audit.json
```

Pass `--seed <n>` to make provider sampling reproducible during debugging.
Live Yahoo and exchange providers can fail for individual symbols without proving that the symbol is delisted; use the diagnostic script and `@pytest.mark.network` tests when live upstream confidence matters.

## Generated Reports

Common report locations:

- extended-hours alerts: `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_alerts.csv`
- extended-hours HTML report: `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_report.html`
- parquet store integrity report: `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.{md,json}`
- Yahoo warning throttle state: `<paths.update_warning_state_json>`
- crypto metadata registry: `<paths.crypto_registry_json>`

## Public Python Surface

Import package:

```python
import tradinglab_data
```

Compatibility and schema manifests:

```python
tradinglab_data.ARTIFACT_SCHEMA_VERSION
tradinglab_data.DATAFRAME_POLICY
tradinglab_data.compatibility_manifest()
tradinglab_data.schema_manifest()
```

Common public helpers are lazily re-exported from `tradinglab_data`, including universe loading/building, parquet validation, schema rendering, store reporting, daily workflows, intraday workflows, and crypto workflows.
The full compatibility surface is documented in `docs/API_CONTRACT.md`.

## Development Checks

Run the package checks before finishing code changes:

```bash
python -m ruff check src tests
python -m mypy src
PYTHONPATH=src python -m pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=85 -m "not network" tests
PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown
python -m build
python -m twine check dist/*
```

Use `@pytest.mark.network` for live upstream smoke tests so CI can exclude them by default.
Network tests should skip cleanly when an upstream provider blocks or returns no live data.

GitHub CI runs Ruff, mypy, pytest with non-network coverage, a schema CLI smoke check, build validation, `twine check`, and wheel install smoke checks across Python 3.10 through 3.13.

PyPI publishing is handled by GitHub Trusted Publishing through `.github/workflows/publish.yml`. The publishing job uses GitHub OIDC with a `pypi` environment and does not require a long-lived PyPI API token secret.

## Reference Docs

- `ARCHITECTURE.md`
- `docs/API_CONTRACT.md`
- `docs/PARQUET_SCHEMA.md`
- `docs/WORKFLOWS.md`
- `docs/INTRADAY_5M_CONTRACT.md`
- `docs/TROUBLESHOOTING.md`
- `RELEASE.md`
- `CHANGELOG.md`
