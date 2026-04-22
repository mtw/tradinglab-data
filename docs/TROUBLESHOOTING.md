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
