# tradinglab-data

`tradinglab-data` is a standalone market-data maintenance package. It retrieves upstream price history, normalizes it into a stable parquet contract, maintains universe metadata, and validates the resulting local data store.

It is designed to be the system of record for:
- daily OHLC parquet history
- extended-hours intraday parquet history
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
tradinglab-data --config configs/config.yaml report-parquet-store
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
  - update daily parquet history
  - optionally refresh extended-hours intraday parquet
  - write extended-hours alert/report artifacts

- `tradinglab-data monitor-extended-hours`
  - refresh extended-hours intraday parquet only
  - compute moves versus latest regular-session close
  - write CSV and HTML monitoring outputs

- `tradinglab-data build-universe`
  - build a merged universe CSV from supported index sources and fallback overrides

- `tradinglab-data schema`
  - print the canonical parquet schema in markdown or JSON form

- `tradinglab-data report-parquet-store`
  - audit every daily and intraday parquet file
  - write JSON and markdown integrity reports
  - highlight dirty files, schema issues, history ranges, and currency coverage

Operational verifier:

```bash
python scripts/verify_yahoo_access.py --config configs/config.local.yaml --sample-size 15 --intervals 1d,5m,1m
```

By default, each invocation samples a fresh random set of symbols. Pass `--seed <n>` when you want reproducible sampling for debugging.

## Primary Outputs

- universe CSV
  - `<paths.universe_csv>`
- daily parquet
  - `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday parquet
  - `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- extended-hours alerts
  - `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_alerts.csv`
- extended-hours HTML report
  - `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_report.html`
- parquet store integrity report
  - `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.{md,json}`

## Configuration

The CLI expects a YAML config with paths and update settings.
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

If `TLD_CONFIG_PATH` is not set, the wrapper prefers `configs/config.local.yaml` automatically when it exists, and falls back to `configs/config.yaml` otherwise.

## Programmatic Surface

Machine-readable manifests:

- `tradinglab_data.compatibility_manifest()`
- `tradinglab_data.schema_manifest()`

Schema contract and artifacts:

- `docs/PARQUET_SCHEMA.md`
- `docs/API_CONTRACT.md`

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

Current release: `0.1.0`.
