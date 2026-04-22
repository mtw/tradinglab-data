# tradinglab-data

`tradinglab-data` is a standalone market-data maintenance package. It retrieves upstream price history, normalizes it into a stable parquet contract, maintains universe metadata, and validates the resulting local data store.

It is designed to be the system of record for:
- daily OHLC parquet history
- extended-hours intraday parquet history
- crypto OHLCV parquet history
- universe CSVs and ticker overrides
- store-wide integrity and verification reports

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
```

Core commands:

```bash
tradinglab-data schema --format markdown
tradinglab-data build-universe --indices sp500 djia dax mdax atx --out <paths.universe_csv>
tradinglab-data --config configs/config.yaml update
tradinglab-data --config configs/config.yaml monitor-extended-hours --session pre --top-n 25
tradinglab-data --config configs/config.yaml backfill-extended-hours --interval 5m
tradinglab-data --config configs/config.yaml report-parquet-store
tradinglab-data --config configs/config.yaml report-universe-consistency --dataset daily --instrument-type stock
tradinglab-data --config configs/config.yaml report-universe-consistency --dataset crypto --interval 1h --universe crypto_core
tradinglab-data --config configs/config.yaml crypto backfill --interval 1d --universe crypto_majors
tradinglab-data --config configs/config.yaml crypto update --interval 1h --universe crypto_high_liquidity
tradinglab-data --config configs/config.yaml crypto validate --interval 15m --universe crypto_high_liquidity
tradinglab-data --config configs/config.yaml crypto refresh-universe --provider coingecko --universe crypto_high_liquidity
tradinglab-data --config configs/config.yaml crypto show-universe --universe crypto_high_liquidity
tradinglab-data --config configs/config.yaml crypto diff-universe --left-universe crypto_majors --right-universe crypto_high_liquidity
tradinglab-data --config configs/config.yaml crypto inspect --interval 1h --universe crypto_high_liquidity
tradinglab-data --config configs/config.yaml crypto prune --interval 1h --universe crypto_high_liquidity
```

Install from PyPI:

```bash
pip install tradinglab-data
```

Use from another local checkout:

```bash
pip install -e /path/to/tradinglab-data
```

## Primary Commands

- `tradinglab-data update`
  - refresh daily parquet history
  - optionally refresh extended-hours intraday parquet
  - write extended-hours alert/report artifacts
  - existing daily parquet files are merged/upserted rather than replaced
  - intraday parquet is append-only by default; set `extended_hours.retention_days` to a positive number only if you explicitly want rolling truncation

- `tradinglab-data monitor-extended-hours`
  - refresh extended-hours intraday parquet only
  - compute moves versus latest regular-session close
  - write CSV and HTML monitoring outputs
  - repeated Yahoo intraday symbol warnings are throttled by `extended_hours.log_repeat_cooldown_hours` to avoid cron-log spam

- `tradinglab-data backfill-extended-hours`
  - refetch the provider's full allowed intraday window for one interval
  - merge it into existing local parquet without deleting older locally accumulated rows

- `tradinglab-data build-universe`
  - build a merged universe CSV from supported index sources and fallback overrides

- `tradinglab-data schema`
  - print the canonical parquet schema in markdown or JSON form

- `tradinglab-data report-parquet-store`
  - audit every daily, intraday, and crypto parquet file
  - write JSON and markdown integrity reports
  - highlight dirty files, schema issues, history ranges, and currency coverage

- `tradinglab-data report-universe-consistency`
  - render a symbol-level table for one daily, intraday, or crypto universe slice
  - show file existence, row counts, start and end coverage, and issue flags per symbol
  - useful for checking stock, ETF, and crypto coverage without running repair flows

- `tradinglab-data crypto`
  - list exchange symbols through the configured crypto provider
  - refresh dynamic crypto universes from CoinGecko metadata, intersected with configured exchange tradability
  - backfill and refresh crypto OHLCV parquet under a separate crypto store root
  - inspect, diff, and prune local crypto universe coverage
  - validate crypto parquet against the canonical crypto schema

- `scripts/check_crypto_status.py`
  - verify crypto coverage, file health, and stale-bar conditions for one interval and universe
  - optionally repair dirty symbols by rerunning crypto backfill/update logic

Operational verifier:

```bash
python scripts/verify_yahoo_access.py --config configs/config.local.yaml --sample-size 15 --intervals 1d,5m,1m
```

By default, each invocation samples a fresh random set of symbols. Pass `--seed <n>` when you want reproducible sampling for debugging.

Provider caveat:

- live Yahoo and exchange providers can fail on individual symbols without the symbol being truly delisted
- repeated intraday Yahoo symbol warnings are throttled through `<paths.update_warning_state_json>` so the first signal is preserved without spamming every cron run
- use `python scripts/verify_yahoo_access.py ...` and the `@pytest.mark.network` suite when you want live upstream confidence rather than fixture-only confidence

## Primary Outputs

- universe CSV
  - `<paths.universe_csv>`
- Yahoo warning throttle state
  - `<paths.update_warning_state_json>`
- daily parquet
  - `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday parquet
  - `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- crypto parquet
  - `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`
- crypto metadata registry
  - `<paths.crypto_registry_json>`
- dynamic crypto universes
  - `<paths.crypto_universe_dir>/<UNIVERSE>.json`
- extended-hours alerts
  - `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_alerts.csv`
- extended-hours HTML report
  - `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_report.html`
- parquet store integrity report
  - `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.{md,json}`

## Configuration

The CLI expects a YAML config with paths and workflow settings.
Example templates:

- `configs/config.yaml.example`
- bundled package template: `src/tradinglab_data/config.yaml.example`

If a config-backed command is run without a valid config file, the CLI raises a clear error telling you to create one from the bundled template, pass `--config`, or set `TRADINGLAB_DATA_CONFIG`.

Keep the tracked `configs/config.yaml` generic. For machine-specific path layouts, create an untracked `configs/config.local.yaml` and point commands at it:

```bash
export TRADINGLAB_DATA_CONFIG=configs/config.local.yaml
tradinglab-data update
```

The maintenance wrapper supports the same local-override pattern:

```bash
TLD_CONFIG_PATH=configs/config.local.yaml ./scripts/run_daily_update_verify.sh
```

Dedicated crypto maintenance wrapper:

```bash
TLD_CONFIG_PATH=configs/config.local.yaml ./scripts/run_crypto_update_verify.sh
```

Read-only universe consistency report:

```bash
python scripts/report_universe_consistency.py --config configs/config.local.yaml --dataset daily --instrument-type etf
python scripts/report_universe_consistency.py --config configs/config.local.yaml --dataset intraday --interval 5m --instrument-type stock
python scripts/report_universe_consistency.py --config configs/config.local.yaml --dataset crypto --interval 1h --universe crypto_core
```

Default crypto refresh, update, and validation in the maintenance wrapper:

```bash
TLD_CRYPTO_REFRESH_UNIVERSE=1 \
TLD_CRYPTO_REFRESH_UNIVERSE_NAME=crypto_high_liquidity \
TLD_CRYPTO_UPDATE=1 \
TLD_VERIFY_CRYPTO=1 \
TLD_CRYPTO_INTERVALS=1d,1h,15m \
./scripts/run_daily_update_verify.sh
```

Relevant wrapper toggles:

- `TLD_CRYPTO_REFRESH_UNIVERSE`
- `TLD_CRYPTO_REFRESH_PROVIDER`
- `TLD_CRYPTO_REFRESH_UNIVERSE_NAME`
- `TLD_CRYPTO_REFRESH_LIMIT`
- `TLD_CRYPTO_UPDATE`
- `TLD_VERIFY_CRYPTO`
- `TLD_CRYPTO_INTERVALS`
- `TLD_CRYPTO_UNIVERSE`
- `TLD_CRYPTO_REPAIR`
- `TLD_CRYPTO_MAX_MISSING_RATIO`
- `TLD_CRYPTO_MAX_ZERO_BYTE`
- `TLD_CRYPTO_STALE_MULTIPLE`

The dedicated crypto wrapper performs a full `check -> update -> verify/fix -> strict post-check` sequence for each configured interval and writes per-interval JSON summaries under the crypto gate directory.

If `TLD_CONFIG_PATH` is not set, the wrapper prefers `configs/config.local.yaml` automatically when it exists, and falls back to `configs/config.yaml` otherwise.

## Programmatic Surface

Machine-readable manifests:

- `tradinglab_data.compatibility_manifest()`
- `tradinglab_data.schema_manifest()`

Schema contract and artifacts:

- `docs/PARQUET_SCHEMA.md`
- `docs/API_CONTRACT.md`
- `docs/TROUBLESHOOTING.md`

## Testing

Run the package test suite directly:

```bash
PYTHONPATH=src python -m pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=60 -m "not network" tests
```

For this standalone repo:

```bash
python -m ruff check src tests
python -m mypy src
PYTHONPATH=src python -m pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=60 -m "not network" tests
PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown
python -m build
python -m twine check dist/*
```

GitHub CI:

- `.github/workflows/ci.yml`
- tests Python 3.10, 3.11, 3.12, and 3.13
- runs Ruff, mypy, pytest with coverage and `-m "not network"`, schema CLI smoke, build, twine check
- builds and installs the wheel to smoke-check the installed package and CLI

## Release

Release notes and process:

- `RELEASE.md`
- `CHANGELOG.md`

Current release: `0.2.0`.
