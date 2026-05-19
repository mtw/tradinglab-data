# Troubleshooting

## Purpose

This page centralizes common operator-facing issues for `tradinglab-data`, especially where upstream provider behavior can look like a local package failure.

## Yahoo Intraday Warnings

Typical warning text:

- `possibly delisted`
- `no timezone found`
- `intraday_5m_yahoo_symbol_warning: possibly delisted or no timezone found`

What this usually means:

- Yahoo did not return usable intraday data for that symbol
- Yahoo did not provide timezone metadata for that symbol
- the symbol may still be active in the market and in the package universe

What it does not prove:

- it does not prove the instrument is actually delisted
- it does not prove your local parquet store is corrupted

What to check:

1. Run the universe consistency report for the symbol and interval.
2. Check whether a parquet file already exists and whether it is stale or missing.
3. Run `python scripts/verify_yahoo_access.py ...` to distinguish broad Yahoo access issues from symbol-specific failures.
4. Review symbol overrides in `<paths.ticker_overrides_csv>` when the warning is concentrated in one mapping.

If `verify_yahoo_access.py` exits immediately with a message about no loaded symbols or an empty sample:

1. check the selected `--indices` values
2. check the configured universe CSV or universe directory paths
3. treat that result as a configuration/input problem, not a clean Yahoo connectivity pass

## Warning Throttle State

Repeated intraday Yahoo warnings are throttled so the same symbol and issue do not spam every cron run.

State file:

- `<paths.update_warning_state_json>`

Behavior:

- the first warning is logged
- repeats within `extended_hours.log_repeat_cooldown_hours` are suppressed
- stale warning-state entries are pruned automatically on write

If you want to reset suppression:

- remove `<paths.update_warning_state_json>`

## Intraday Retention

Default behavior:

- `extended_hours.retention_days: 0`
- intraday parquet is append-only

If you set a positive `retention_days`:

- files are trimmed to a rolling window during writes
- this can make old intraday history disappear intentionally

If older intraday history is already gone:

- use `tradinglab-data --config /path/to/config.yaml backfill-extended-hours --interval 5m`
- recovery is limited by the provider window
  - Yahoo `5m`: about `60d`
  - Yahoo `1m`: about `7d`

## Daily Parquet Replace vs Merge

Current behavior:

- existing daily parquet files are upserted and merged
- strict-symbol refreshes and `stooq_refresh_all` also merge into existing local history

If a daily file looks unexpectedly short:

1. inspect the symbol with the consistency report
2. review recent update-log entries
3. verify whether the file was already short before the latest run

## Symbol Master Validation Failures

Typical failures:

- missing `asset_currency`
- missing `base_listing_currency`
- missing `tax_country`
- missing `asset_class`
- duplicate normalized `symbol`
- malformed `fx_pair_to_base`
- non-positive `lot_size`
- non-positive `price_multiplier`

What to check:

1. verify the symbol has an `exchange` value that matches a row in `exchange_defaults.csv`
2. confirm overrides use canonical uppercase symbols
3. confirm the intended account-base pair direction, for example `USDEUR` rather than `EURUSD` for a USD asset in a EUR account
4. rebuild with `build-symbol-master` after changing defaults or overrides

## FX Pair Direction Errors

Typical problem:

- the symbol master says `EURUSD` when the consumer actually needs `USDEUR`

What to remember:

- `USDEUR` means EUR value of `1` USD
- pair direction is explicit and must match the consumer contract
- consumers must not silently invert pair direction

If Yahoo lacks the direct pair:

- run `fx-backfill` or `fx-update` with inverse fetch allowed
- the package can fetch the inverse Yahoo symbol and derive the requested pair by inversion

## Missing FX Parquet

If a non-base-currency asset is present in `symbol_master.csv` but the corresponding FX parquet file is missing:

1. run `tradinglab-data --config /path/to/config.yaml fx-update`
2. inspect the pair with `tradinglab-data --config /path/to/config.yaml fx-inspect --pairs <PAIR>`
3. validate the file with `tradinglab-data --config /path/to/config.yaml fx-validate --pairs <PAIR>`

Identity pairs such as `EUREUR` do not require parquet files by default.

## Crypto Provider Limitations

Typical causes of crypto update issues:

- missing `ccxt` in the active environment
- exchange rate limits
- symbol no longer tradable on the selected exchange
- dynamic universe including symbols that fail exchange tradability checks

What to check:

1. confirm the command is using the intended virtual environment
2. verify `ccxt` imports in that environment
3. inspect the crypto universe with `crypto show-universe`
4. inspect local parquet coverage with `crypto inspect`
5. run the crypto consistency or verification helpers

## Live Provider Confidence

The local test suite is strong, but fixture tests do not fully prove live upstream behavior.

For live checks:

- Yahoo smoke coverage: `tests/test_yahoo_network_module.py`
- crypto smoke coverage: `tests/test_crypto_network_module.py`
- operator verifier: `python scripts/verify_yahoo_access.py ...`

Network-marked tests are expected to skip cleanly when the provider or the network is unavailable.
