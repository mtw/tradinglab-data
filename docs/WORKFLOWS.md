# Workflows

## Daily Workflow

Primary command:

```bash
tradinglab-data --config /path/to/config.yaml update
```

High-level behavior:

1. load active universe symbols from the configured universe CSV
2. apply ticker overrides and canonicalization
3. migrate parquet files for renamed symbols where possible
4. fetch full history for missing symbols
5. fetch incremental recent history for existing symbols
6. refresh strict-symbol histories with single-symbol requests when configured
7. upsert canonical daily parquet files without dropping older local rows
8. optionally refresh extended-hours intraday parquet
9. generate extended-hours alert/report artifacts

Intraday retention behavior:

- intraday parquet is append-only by default
- `extended_hours.retention_days: 0` means keep the full accumulated local history
- set a positive `retention_days` value only when you intentionally want a rolling local cache
- repeated Yahoo intraday symbol warnings are throttled by `extended_hours.log_repeat_cooldown_hours`, default `24`

Intraday backfill command:

```bash
tradinglab-data --config /path/to/config.yaml backfill-extended-hours --interval 5m
```

Behavior:

1. fetch the provider's full allowed intraday window for the selected interval
2. merge that window into each existing intraday parquet file
3. preserve older already-accumulated local rows when `extended_hours.retention_days` is `0`

Outputs:

- daily parquet under `paths.parquet_root`
- intraday parquet under `extended_hours.intraday_root`
- maintenance log under `<paths.update_log_csv>`
- extended-hours alerts/report under `<paths.runs_root>/YYYY-MM-DD/monitor/`

## Symbol Master Workflow

Primary commands:

```bash
tradinglab-data --config /path/to/config.yaml build-symbol-master --base-currency EUR
tradinglab-data --config /path/to/config.yaml validate-symbol-master
tradinglab-data --config /path/to/config.yaml inspect-symbol-master --exchange VIE --issues defaulted_country
```

High-level behavior:

1. load the configured universe CSV
2. normalize symbols using the existing universe and ticker-override rules
3. join exchange defaults from `exchange_defaults.csv`
4. apply `symbol_overrides.csv` last
5. fill authoritative accounting fields such as `asset_currency`, `tax_country`, `asset_class`, `fx_pair_to_base`, `lot_size`, and `price_multiplier`
6. write `symbol_master.csv`
7. validate required columns, duplicate symbols, positive lot metadata, and explicit FX pair direction

Outputs:

- authoritative symbol master under `paths.symbol_master_csv`
- exchange defaults under `paths.exchange_defaults_csv`
- symbol overrides under `paths.symbol_overrides_csv`

Important rule:

- daily OHLC `currency` is not the authoritative accounting metadata source
- `metadata_quality=non_authoritative_country` means `country` was filled from `exchange_defaults.csv`
- `metadata_quality=non_authoritative_tax_country` means `tax_country` was filled from `exchange_defaults.csv`
- those flags are expected when Yahoo quote-page audits were used as the source of truth, because Yahoo does not provide authoritative `country` or `tax_country`

## Yahoo Quote Metadata Audit

Primary command:

```bash
python scripts/audit_yahoo_quote_metadata.py --config /path/to/config.yaml --format markdown
```

High-level behavior:

1. read ETF source rows from `paths.universe_dir/etf_all.csv` unless `--path` overrides it
2. fetch exact Yahoo Finance quote pages for each symbol
3. parse the displayed exchange and currency from the quote page header
4. compare Yahoo metadata against the local ETF source row
5. report clean matches, exchange mismatches, currency mismatches, combined mismatches, and ambiguous pages separately

Useful options:

- `--symbols <SYM...>` to audit only selected symbols
- `--format markdown|json|csv`
- `--out <PATH>` to persist the report
- `--fail-on-mismatch` to exit non-zero when any row is not a clean match

Important rule:

- ambiguous Yahoo pages are expected for some naked symbols; those rows should be reviewed manually instead of silently normalized

## FX Daily Workflow

Primary commands:

```bash
tradinglab-data --config /path/to/config.yaml fx-backfill --pairs USDEUR CHFEUR GBPEUR
tradinglab-data --config /path/to/config.yaml fx-update
tradinglab-data --config /path/to/config.yaml fx-validate
tradinglab-data --config /path/to/config.yaml fx-inspect
```

High-level behavior:

1. resolve requested pairs explicitly, or infer non-identity pairs from `symbol_master.csv` for `fx-update`
2. fetch daily Yahoo FX history for the direct pair when available
3. optionally fetch the inverse pair and invert OHLC correctly when the direct pair is unavailable
4. write canonical per-pair parquet under `fx_daily_root`
5. validate pair direction, positive rates, sorted dates, and date uniqueness

Outputs:

- daily FX parquet under `paths.fx_daily_root/<PAIR>.parquet`

Pair convention:

- `USDEUR` means EUR value of `1` USD
- inverse derivation must preserve high/low correctly by using `1/low` for inverse high and `1/high` for inverse low

## Crypto Workflow

Primary commands:

```bash
tradinglab-data --config /path/to/config.yaml crypto list-symbols --exchange binance
tradinglab-data --config /path/to/config.yaml crypto refresh-universe --provider coingecko --universe crypto_high_liquidity
tradinglab-data --config /path/to/config.yaml crypto backfill --exchange binance --interval 1d --universe crypto_majors
tradinglab-data --config /path/to/config.yaml crypto update --exchange binance --interval 1h --universe crypto_high_liquidity
tradinglab-data --config /path/to/config.yaml crypto validate --exchange binance --interval 15m --universe crypto_high_liquidity
tradinglab-data --config /path/to/config.yaml crypto show-universe --universe crypto_high_liquidity
tradinglab-data --config /path/to/config.yaml crypto diff-universe --left-universe crypto_majors --right-universe crypto_high_liquidity
tradinglab-data --config /path/to/config.yaml crypto inspect --exchange binance --interval 1h --universe crypto_high_liquidity
tradinglab-data --config /path/to/config.yaml crypto prune --exchange binance --interval 1h --universe crypto_high_liquidity --apply
```

High-level behavior:

1. resolve crypto config, exchange, market type, and quote-asset filters
2. optionally refresh dynamic universes from CoinGecko metadata intersected with the configured exchange tradable symbol set
3. select symbols from the curated or persisted dynamic registry, or `--symbols` overrides
4. fetch exchange-native OHLCV through CCXT
5. keep closed bars only
6. skip symbols no longer tradable on the selected exchange instead of failing the batch
7. merge, deduplicate, sort, validate, and atomically rewrite per-symbol parquet files
8. inspect and prune local stores against the selected universe when needed

Crypto verification helper:

```bash
python scripts/check_crypto_status.py --config /path/to/config.yaml --interval 1h --universe crypto_core --repair --fail-on-issues
```

Behavior:

1. resolve the expected symbol set for one crypto interval and universe
2. verify coverage, file readability, schema/continuity validity, and stale-last-bar conditions
3. optionally attempt single-symbol repair for dirty files
4. emit a JSON summary when requested
5. exit non-zero only if issues remain after repair when `--fail-on-issues` is passed

Outputs:

- crypto parquet under `paths.crypto_root/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/`
- dynamic registry JSON under `paths.crypto_registry_json`
- dynamic universe JSON under `paths.crypto_universe_dir/<UNIVERSE>.json`

Supported crypto intervals for this workflow:

- `1d`
- `1h`
- `15m`

## Intraday Research Workflow

Primary commands:

```bash
tradinglab-data --config /path/to/config.yaml intraday backfill --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday update --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday validate --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday inspect --universe intraday_pilot
```

Current behavior:

1. resolve a pilot universe CSV from `paths.universe_dir/<UNIVERSE>.csv`
2. limit the first implementation to `5m` regular-session stock/ETF symbols
3. fetch Yahoo intraday bars with `prepost=False`
4. normalize timestamps to UTC and derive exchange-local `session_date` in `America/New_York`
5. persist parquet under a dedicated research root rather than the extended-hours monitoring cache
6. merge append-style history per symbol, preserving older local rows when retention is disabled
7. validate ordering, uniqueness, metadata consistency, and OHLC quality before write

Retention behavior:

- `retention_days: 0` keeps the full accumulated local history
- positive `retention_days` trims existing parquet even when Yahoo returns no new rows for a symbol during an update
- a symbol is reported as `unchanged` only when the fetched frame is empty and the retention trim leaves the existing file unchanged

Outputs:

- intraday research parquet under `intraday.research_root/5m/`

Current first-iteration scope:

- regular session only
- interval `5m` only
- Yahoo provider only
- pilot universe CSV required unless `--symbols` is passed

The target contract and longer-term roadmap remain documented in [INTRADAY_5M_CONTRACT.md](INTRADAY_5M_CONTRACT.md).

## Intraday Live Workflow

Primary commands:

```bash
tradinglab-data --config /path/to/config.yaml intraday-live backfill --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-live update --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-sync backfill --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-sync update --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-live validate --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-live inspect --universe intraday_live_core
```

Current behavior:

1. resolve a stock/ETF live universe from `paths.universe_dir/<UNIVERSE>.csv`
2. fetch Yahoo `5m` bars with `prepost=True`
3. normalize timestamps to UTC and derive exchange-local `session_date` in `America/New_York`
4. label each bar as `pre`, `regular`, `post`, or `unknown`
5. persist a separate session-aware live store under `intraday_live.live_root/5m/`
6. validate ordering, uniqueness, metadata consistency, and OHLC quality before write

Retention behavior:

- `retention_days: 0` keeps the full accumulated local history
- positive `retention_days` trims existing parquet even when Yahoo returns no new rows for a symbol during an update
- a symbol is reported as `unchanged` only when the fetched frame is empty and the retention trim leaves the existing file unchanged

Outputs:

- intraday live parquet under `intraday_live.live_root/5m/`

Shared-sync behavior:

1. resolve the live universe once
2. fetch Yahoo `5m` bars once with `prepost=True`
3. write the session-aware live store under `intraday_live.live_root/5m/`
4. derive the regular-session research store from the same fetched frames under `intraday.research_root/5m/`
5. avoid duplicate provider pulls when both stores should be refreshed together

## Extended-Hours Monitoring Workflow

Primary command:

```bash
tradinglab-data --config /path/to/config.yaml monitor-extended-hours --session pre --top-n 25
```

High-level behavior:

1. read daily reference close from the daily parquet store
2. fetch intraday Yahoo data with `prepost=True`
3. prefer `5m` interval and fallback to `1m` when needed
4. write intraday parquet files incrementally
5. compute `% move` vs most recent regular-session close
6. write CSV alert output
7. render HTML report

Current supported intraday intervals for this workflow:

- `5m`
- `1m`

Configuring another interval currently fails fast with a clear validation error.

Outputs:

- `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_alerts.csv`
- `<paths.runs_root>/YYYY-MM-DD/monitor/extended_hours_report.html`

## Universe Build Workflow

Primary command:

```bash
tradinglab-data build-universe --indices sp500 djia dax mdax atx --out <paths.universe_csv>
```

High-level behavior:

1. fetch constituents from supported upstream sources
2. fall back to override CSVs when primary sources fail
3. normalize symbols via ticker map rules
4. merge duplicate symbols field-by-field, preferring later non-empty metadata while accumulating index memberships
5. write a canonical CSV with stable columns

ATX-specific rule:

- ATX override generation must produce real non-empty symbols
- if the upstream ATX source does not expose a usable symbol column, the override builder now fails fast instead of emitting unusable blank symbols

## Schema Inspection Workflow

Primary command:

```bash
tradinglab-data schema --format markdown
```

Use this when:

- documenting external consumers
- validating schema assumptions during migrations
- reviewing whether a proposed schema change needs a versioned migration

## Verification Workflow

Primary command:

```bash
tradinglab-data --config /path/to/config.yaml report-parquet-store
```

High-level behavior:

1. scan every daily parquet file under `paths.parquet_root`
2. scan every intraday parquet file under `extended_hours.intraday_root`, separated by interval directory
3. scan every crypto parquet file under `paths.crypto_root`, separated by exchange, market type, and interval
4. validate canonical schema presence and readability
5. check ordering, duplicate timestamps, OHLC quality, and currency or quote-asset storage quality
6. summarize retained history ranges, row counts, and currencies seen
7. write JSON and markdown integrity reports
8. include a dirty-files section and the current daily parquet sanity summary

Outputs:

- `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.json`
- `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.md`

Legacy maintenance wrapper:

```bash
scripts/check_parquet_status.py
```

It still exists for provider verification, intraday cleaning, and mismatch repair, but the package-level integrity audit above is now the regular store-health entrypoint.

Universe consistency report:

```bash
tradinglab-data --config /path/to/config.yaml report-universe-consistency --dataset daily --instrument-type etf
tradinglab-data --config /path/to/config.yaml report-universe-consistency --dataset intraday --interval 5m --instrument-type stock
tradinglab-data --config /path/to/config.yaml report-universe-consistency --dataset crypto --interval 1h --universe crypto_core
```

Behavior:

- exits non-zero when no symbols can be loaded from the requested universe inputs
- exits non-zero when the effective Yahoo probe sample is empty

1. resolve the requested symbol scope from the active equity universe or selected crypto universe
2. read one parquet file per expected symbol without mutating local state
3. summarize row counts, start and end coverage, schema validity, ordering, duplicate or bad OHLC rows, and stale crypto bars
4. render a table in markdown, JSON, or CSV format for operator review

Outputs:

- stdout by default
- optional file output via `--out`

Helper script:

```bash
python scripts/report_universe_consistency.py --config /path/to/config.yaml --dataset crypto --interval 15m --universe crypto_core
```

Daily maintenance wrapper:

```bash
./scripts/run_daily_update_verify.sh
```

Dedicated crypto maintenance wrapper:

```bash
./scripts/run_crypto_update_verify.sh
```

Config precedence for this wrapper:

- `TLD_CONFIG_PATH` when set
- `configs/config.local.yaml` when it exists
- `configs/config.yaml` otherwise

Crypto maintenance behavior for this wrapper:

- `TLD_CRYPTO_REFRESH_UNIVERSE=1` refreshes the configured dynamic universe before updates
- `TLD_CRYPTO_REFRESH_PROVIDER` selects the metadata provider, default `coingecko`
- `TLD_CRYPTO_REFRESH_UNIVERSE_NAME` selects the refreshed universe, default `crypto_high_liquidity`
- `TLD_CRYPTO_REFRESH_LIMIT` limits metadata-selected symbols, default `25`
- `TLD_CRYPTO_UPDATE=1` runs `tradinglab-data crypto update` for each configured interval
- `TLD_VERIFY_CRYPTO=1` runs `scripts/check_crypto_status.py` after updates
- `TLD_CRYPTO_REPAIR=1` enables automatic single-symbol repair during crypto verification
- `TLD_CRYPTO_MAX_MISSING_RATIO` sets the allowed post-repair missing-symbol ratio, default `0.0`
- `TLD_CRYPTO_MAX_ZERO_BYTE` sets the allowed zero-byte crypto file count, default `0`
- `TLD_CRYPTO_STALE_MULTIPLE` sets the tolerated stale-bar multiple by interval, default `2`
- `TLD_CRYPTO_INTERVALS` selects intervals, default `1d,1h,15m`
- `TLD_CRYPTO_UNIVERSE` selects the update and validation universe, default `crypto_high_liquidity`

Dedicated crypto wrapper behavior:

1. optional preflight crypto check for each configured interval
2. crypto update for each configured interval
3. crypto verification with optional repair
4. strict post-check that fails if any issues remain
5. gate files, logs, lockfiles, and per-interval JSON summaries under the crypto gate directory

Yahoo accessibility verifier:

```bash
python scripts/verify_yahoo_access.py --config /path/to/config.yaml --sample-size 15 --intervals 1d,5m,1m
```

Troubleshooting reference:

- `docs/TROUBLESHOOTING.md`

Use this when:

- upstream Yahoo behavior looks suspicious
- maintenance logs show repeated connectivity or consent-host failures
- you want a fast operational check before running a full daily run

Behavior:

- loads symbols from the configured universe CSV or selected universe shard files
- picks a fresh random sample on each run by default
- supports `--seed` when reproducible sampling is needed for debugging
- probes Yahoo per symbol and interval
- classifies connectivity failures separately from empty/no-data responses
- optionally writes a JSON summary for later inspection

## Failure Semantics

- Provider no-data responses are expected occasionally and should be classified carefully.
- Historical gaps may be tolerated differently for ETFs vs stocks according to configured policy.
- Sparse extended-hours data is not automatically corruption.
- Unsupported intraday interval configuration is a hard configuration error and raises immediately.
- Missing or invalid crypto parquet is a hard validation failure for `tradinglab-data crypto validate`.
