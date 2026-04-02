# Operations

This runbook covers the common operational workflows for `tradinglab-data`.

## Daily Update

```bash
tradinglab-data --config ./configs/config.yaml update
```

## Extended-Hours Monitor

```bash
tradinglab-data --config ./configs/config.yaml monitor-extended-hours --session pre --top-n 25
```

## Store Integrity Report

```bash
tradinglab-data --config ./configs/config.yaml report-parquet-store
```

## Universe Build

```bash
tradinglab-data build-universe --indices sp500 djia dax mdax atx --out <paths.universe_csv>
```

## Schema Inspection

```bash
tradinglab-data schema --format markdown
```

## Yahoo Accessibility Probe

```bash
python scripts/verify_yahoo_access.py --config ./configs/config.yaml --sample-size 15 --intervals 1d,5m,1m
```

## Maintenance Wrapper

```bash
./scripts/run_daily_update_verify.sh
```

Config precedence for the wrapper:

- `TLD_CONFIG_PATH` when set
- `configs/config.local.yaml` when it exists
- `configs/config.yaml` otherwise

## Notes

- Use `configs/config.local.yaml` for machine-specific paths.
- Schedule store integrity reports as part of routine maintenance.
