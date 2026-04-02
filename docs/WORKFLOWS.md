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
7. write canonical daily parquet files
8. optionally refresh extended-hours intraday parquet
9. generate extended-hours alert/report artifacts

Outputs:

- daily parquet under `paths.parquet_root`
- intraday parquet under `extended_hours.intraday_root`
- maintenance log under `<paths.update_log_csv>`
- extended-hours alerts/report under `<paths.runs_root>/YYYY-MM-DD/monitor/`

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
3. validate canonical schema presence and readability
4. check ordering, duplicate timestamps, OHLC quality, and currency storage quality
5. summarize retained history ranges, row counts, and currencies seen
6. write JSON and markdown integrity reports
7. include a dirty-files section and the current daily parquet sanity summary

Outputs:

- `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.json`
- `<paths.runs_root>/YYYY-MM-DD/integrity/parquet_store_report.md`

Legacy maintenance wrapper:

```bash
scripts/check_parquet_status.py
```

It still exists for provider verification, intraday cleaning, and mismatch repair, but the package-level integrity audit above is now the regular store-health entrypoint.

Daily maintenance wrapper:

```bash
./scripts/run_daily_update_verify.sh
```

Config precedence for this wrapper:

- `TLD_CONFIG_PATH` when set
- `configs/config.local.yaml` when it exists
- `configs/config.yaml` otherwise

Yahoo accessibility verifier:

```bash
python scripts/verify_yahoo_access.py --config /path/to/config.yaml --sample-size 15 --intervals 1d,5m,1m
```

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
