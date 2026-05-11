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

## Planned General Intraday 5m Research Workflow

This workflow is not fully implemented yet.
The target contract is documented in [INTRADAY_5M_CONTRACT.md](INTRADAY_5M_CONTRACT.md).

Intended behavior:

1. ingest regular-session `5m` bars for a curated pilot universe
2. persist them under a dedicated research intraday root rather than the extended-hours monitoring cache
3. normalize timestamps to UTC while preserving exchange-local `session_date` semantics
4. validate continuity, duplicates, OHLC quality, and stale coverage
5. expose a stable parquet contract for downstream `tradinglab` intraday research

Recommended first implementation scope:

- regular session only
- interval `5m` only
- US stocks and ETFs
- pilot universe before full-universe rollout

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
4. merge duplicate symbols and accumulate index memberships
5. write a canonical CSV with stable columns

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
