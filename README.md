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
cd packages/tradinglab-data
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
```

Run a few core commands:

```bash
tradinglab-data schema --format markdown
tradinglab-data build-universe --indices sp500 djia dax mdax atx --out <paths.universe_csv>
tradinglab-data update --config configs/config.yaml
tradinglab-data monitor-extended-hours --config configs/config.yaml --session pre --top-n 25
```

Monorepo usage together with TradingLab:

```bash
pip install -e ./packages/tradinglab-data
pip install -e .
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

## Primary Outputs

- universe CSV
  - `<paths.universe_csv>`
- daily parquet
  - `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday parquet
  - `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- extended-hours alerts
  - `runs/YYYY-MM-DD/monitor/extended_hours_alerts.csv`
- extended-hours HTML report
  - `runs/YYYY-MM-DD/monitor/extended_hours_report.html`

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
| `workflows.py` | Config-driven update and extended-hours operational workflows |
| `cli.py` | Standalone package CLI |

## Data Contract

The package writes symbol-partitioned parquet stores with stable columns and file naming.

- daily: `<paths.parquet_root>/<SYMBOL>.parquet`
- intraday: `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`

Exact schema and constraints:

- [`docs/PARQUET_SCHEMA.md`](docs/PARQUET_SCHEMA.md)

Operational boundary and ownership rules:

- [`docs/BOUNDARY.md`](docs/BOUNDARY.md)
- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`docs/WORKFLOWS.md`](docs/WORKFLOWS.md)

## Configuration

The standalone CLI expects a YAML config with path and update settings.
Example template:

- [`configs/config.yaml.example`](configs/config.yaml.example)

If a config-backed command is run without a valid config file, the CLI raises a clear error telling you to create one from the example template or pass `--config`.

## Testing

Run the package test suite directly:

```bash
PYTHONPATH=packages/tradinglab-data/src pytest -q packages/tradinglab-data/tests
```

Package CI:

- `.github/workflows/tradinglab-data.yml`

## Release

Release and repo-split notes:

- [`RELEASE.md`](RELEASE.md)

The package is designed to be split into its own repository and later published to PyPI as `tradinglab-data`.
