# Consumer Compatibility Checklist

Use this checklist before upgrading external consumers to a new `tradinglab-data` build.

## Package Import Contract

- install the target wheel or editable build
- import `tradinglab_data`
- verify expected public names still resolve
- assert `tradinglab_data.DATAFRAME_POLICY == "polars-first"` when the consumer uses public tabular Python APIs
- verify consumer code does not assume the export set is closed or fixed-length

## Dataframe Contract

- consume public tabular Python API outputs as `polars.DataFrame`
- do not require or assert pandas return values from this package
- treat pandas as an internal provider-boundary detail that may disappear from any non-boundary module

## CLI Contract

- run the exact CLI commands the consumer uses today
- confirm exit codes are unchanged
- confirm any stdout or stderr parsing still works
- confirm `report-parquet-store` consumers tolerate additive crypto sections and fields

## Artifact Contract

- confirm the consumer checks `ARTIFACT_SCHEMA_VERSION`
- verify daily and intraday parquet readers still accept the current schema
- verify the consumer treats daily OHLC `currency` as diagnostic provider data rather than authoritative accounting metadata
- verify the consumer reads `symbol_master.csv` before portfolio simulation or accounting-sensitive workflows
- verify the consumer accepts:
  - `<paths.meta_root>/symbol_master.csv`
  - `<paths.meta_root>/exchange_defaults.csv`
  - `<paths.meta_root>/symbol_overrides.csv`
  - `<paths.fx_daily_root>/<PAIR>.parquet`
- if consuming v0.4 market-data artifacts, verify the reader accepts:
  - `<paths.market_cap_root>/<SYMBOL>.parquet`
  - `<paths.sector_assignments_csv>`
  - `<paths.index_returns_root>/<INDEX_ID>.parquet`
- if consuming crypto artifacts, verify the reader accepts:
  - `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`
  - `<paths.crypto_registry_json>`
  - `<paths.crypto_universe_dir>/<UNIVERSE>.json`

## TradingLab Accounting Consumer Checks

- load `symbol_master.csv` before portfolio simulation
- treat `fx_pair_to_base` as authoritative
- for identity pairs such as `EUREUR`, use a conversion factor of `1.0`
- for non-identity pairs, require `<paths.fx_daily_root>/<PAIR>.parquet`
- never infer asset currency from ticker suffixes inside the consumer

## Integrity Report Contract

- load `parquet_store_report.json`
- verify the parser tolerates additive keys such as `crypto_root`
- verify the parser tolerates additive `section` values such as `crypto:binance:spot:1h`
- verify the parser tolerates additive dirty-reason strings

## Config Contract

- verify existing configs still load unchanged
- verify the consumer tolerates additive `crypto.*`, `paths.crypto_*`, `paths.market_cap_root`, `paths.sector_assignments_csv`, and `paths.index_returns_root` keys
- verify the consumer does not assume the known path key set is exhaustive

## Maintenance Wrapper Contract

- if the consumer or operator relies on `scripts/run_daily_update_verify.sh`, verify the current crypto defaults are intended
- disable crypto behavior explicitly when needed:
  - `TLD_CRYPTO_REFRESH_UNIVERSE=0`
  - `TLD_CRYPTO_UPDATE=0`
  - `TLD_VERIFY_CRYPTO=0`

## Runtime Verification

- run at least one end-to-end consumer workflow against the target build
- compare outputs before and after the upgrade
- inspect logs, generated artifacts, and any downstream reports for unexpected differences
