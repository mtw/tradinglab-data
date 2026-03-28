# tradinglab-data

`tradinglab-data` is the standalone data-maintenance package for the TradingLab ecosystem.
It is responsible for fetching market data, normalizing it into a stable parquet contract, maintaining universe files, and validating the resulting local data store.

It does not own research, screening, plotting, prediction, or experiment analysis. Those remain in `tradinglab`.

## Why This Exists

Splitting data maintenance from TradingLab has three purposes:

- keep research infrastructure independent from provider and cache-maintenance code
- make parquet/universe artifacts reusable from other packages
- allow data maintenance to evolve and release on its own lifecycle

## Scope

`tradinglab-data` owns:

- universe loading and merged-universe construction
- ticker normalization and override mapping
- daily market data retrieval and parquet writes
- extended-hours intraday retrieval and parquet writes
- parquet sanity verification primitives
- canonical parquet schema definitions

`tradinglab-data` does not own:

- signal generation
- strategy research
- model training
- screening outputs
- research dashboards unrelated to data maintenance

## 5-Minute Quick Start

Standalone package install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
```

Run a few core commands:

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

## Package Modules

| Module | Purpose |
|---|---|
| `data_yf.py` | Yahoo Finance download helpers, parquet read/update helpers, update log writing |
| `data_stooq.py` | Stooq symbol mapping, CSV parsing, Stooq history fetch |
| `ticker_map.py` | Ticker normalization and override handling |
| `universe.py` | Universe CSV loading and canonicalization |
| `universe_build.py` | Index constituent acquisition and merged universe construction |
| `extended_hours_monitor.py` | Intraday retrieval, alert computation, intraday parquet maintenance, HTML/CSV outputs |
| `parquet_verify.py` | Parquet sanity checks and summary helpers |
| `schema.py` | Canonical parquet schema definitions and rendering helpers |
| `contracts.py` | Typed result contracts and dataframe column-set constants |
| `store_report.py` | Store-wide parquet integrity auditing and report rendering |
| `workflows.py` | Config-driven update and extended-hours operational workflows |
| `cli.py` | Standalone package CLI |

## Data Contract

The package writes symbol-partitioned parquet stores with stable columns and file naming.

- daily: `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday: `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`

Exact schema and constraints:

- [`docs/PARQUET_SCHEMA.md`](docs/PARQUET_SCHEMA.md)
- [`docs/API_CONTRACT.md`](docs/API_CONTRACT.md)

Current compatibility signals:

- package version
  - `0.1.0`
- artifact schema version
  - `v0.1.0`

Machine-readable manifest:

- `tradinglab_data.compatibility_manifest()`
- `tradinglab_data.schema_manifest()`

Contract history:

- package version is the dependency compatibility signal for Python/CLI consumers
- `ARTIFACT_SCHEMA_VERSION` tracks parquet/report compatibility independently from package release cadence

Operational boundary and ownership rules:

- [`docs/BOUNDARY.md`](docs/BOUNDARY.md)
- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`docs/WORKFLOWS.md`](docs/WORKFLOWS.md)

## Configuration

The standalone CLI expects a YAML config with path and update settings.
Example template:

- [`configs/config.yaml.example`](configs/config.yaml.example)
- bundled package template: [`src/tradinglab_data/config.yaml.example`](src/tradinglab_data/config.yaml.example)

If a config-backed command is run without a valid config file, the CLI raises a clear error telling you to create one from the bundled template, pass `--config`, or set `TRADINGLAB_DATA_CONFIG`.

## Testing

Run the package test suite directly:

```bash
PYTHONPATH=src pytest -q tests
```

For this standalone repo:

```bash
PYTHONPATH=src pytest -q tests
```

GitHub CI:

- [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
- runs on push and pull request
- tests Python `3.11` and `3.12`
- executes the same core checks used for local release discipline:
  - `PYTHONPATH=src pytest -q tests`
  - `PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown`
  - `python -m build`
  - `python -m twine check dist/*`

## Release

Release and repo-split notes:

- [`RELEASE.md`](RELEASE.md)
- [`CHANGELOG.md`](CHANGELOG.md)

This repository is the standalone package and is intended to be published to PyPI as `tradinglab-data`.
