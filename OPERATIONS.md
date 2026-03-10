# tradinglab-data Operations Handoff

This handoff assumes `tradinglab-data` is a standalone repository with its own config and does not depend on `tradinglab` being installed.

## Standalone Commands

### Daily Update

```bash
tradinglab-data update --config ./configs/config.yaml
```

### Extended-Hours Monitoring

```bash
tradinglab-data monitor-extended-hours \
  --config ./configs/config.yaml \
  --session pre \
  --top-n 25
```

### Universe Rebuild

```bash
tradinglab-data build-universe \
  --indices sp500 djia dax mdax atx \
  --out ./<paths.universe_csv>
```

### Schema Inspection

```bash
tradinglab-data schema --format markdown
```

## Important Boundary Note

At the moment, full nightly verification is not yet fully standalone because the verifier and gate script still live in the TradingLab repository:

- `scripts/check_parquet_status.py`
- `scripts/run_daily_update_verify.sh`

So `tradinglab-data` is already standalone for update, intraday monitoring, schema inspection, and universe generation, but not yet for the full nightly update+gate workflow.

## What Should Move Next

To make `tradinglab-data` fully standalone, move these into the new repo:

1. parquet status verification
2. repair/clean maintenance entrypoints
3. nightly update+verify orchestration

Prepared handoff files for this migration:

- `scripts/check_parquet_status.py`
- `scripts/run_daily_update_verify.sh`

Once that happens, the standalone repo should provide its own:

```bash
./scripts/run_daily_update_verify.sh
```

without any dependency on a local TradingLab checkout.
