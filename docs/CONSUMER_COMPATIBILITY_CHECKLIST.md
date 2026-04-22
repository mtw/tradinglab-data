# Consumer Compatibility Checklist

Use this checklist before upgrading external consumers to a new `tradinglab-data` build.

## Package Import Contract

- install the target wheel or editable build
- import `tradinglab_data`
- verify expected public names still resolve
- verify consumer code does not assume the export set is closed or fixed-length

## CLI Contract

- run the exact CLI commands the consumer uses today
- confirm exit codes are unchanged
- confirm any stdout or stderr parsing still works
- confirm `report-parquet-store` consumers tolerate additive crypto sections and fields

## Artifact Contract

- confirm the consumer checks `ARTIFACT_SCHEMA_VERSION`
- verify daily and intraday parquet readers still accept the current schema
- if consuming crypto artifacts, verify the reader accepts:
  - `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`
  - `<paths.crypto_registry_json>`
  - `<paths.crypto_universe_dir>/<UNIVERSE>.json`

## Integrity Report Contract

- load `parquet_store_report.json`
- verify the parser tolerates additive keys such as `crypto_root`
- verify the parser tolerates additive `section` values such as `crypto:binance:spot:1h`
- verify the parser tolerates additive dirty-reason strings

## Config Contract

- verify existing configs still load unchanged
- verify the consumer tolerates additive `crypto.*` and `paths.crypto_*` keys
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
