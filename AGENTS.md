# AGENTS.md

This repository is a standalone data-maintenance package.

## What This Repo Owns

- universe loading and merged-universe construction
- ticker normalization and override mapping
- market data retrieval from upstream providers
- normalization into the canonical parquet schema
- daily parquet workflows
- extended-hours intraday workflows
- parquet sanity checks and verification primitives
- schema specification for parquet artifacts

## What This Repo Does Not Own

- signal generation
- screening decisions
- plotting/report UX outside of data-maintenance reports
- research workflows
- predictive modeling
- experiment registry analysis

Those remain out of scope for this package.

## Hard Boundary Rules

- Do not add research or prediction logic here.
- Treat parquet and universe CSVs as public artifacts consumed by external applications.
- Schema modifications require edits to `docs/PARQUET_SCHEMA.md`.
- Workflow modifications require edits to `docs/WORKFLOWS.md` when user-visible behavior changes.

## Source Of Truth

- package code: `src/tradinglab_data/`
- tests: `tests/`
- schema contract: `docs/PARQUET_SCHEMA.md`
- architecture overview: `ARCHITECTURE.md`
- release process: `RELEASE.md`

## Commands To Run Before Finishing Changes

Package tests:

```bash
PYTHONPATH=src python -m pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=60 -m "not network" tests
```

Use `@pytest.mark.network` for live upstream smoke tests so CI can exclude them by default.
Those tests should skip cleanly when the upstream blocks or returns no live data.

Static checks:

```bash
python -m ruff check src tests
python -m mypy src
```

Build validation:

```bash
python -m build
python -m twine check dist/*
```

CLI smoke checks:

```bash
PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown
```

If the change affects daily workflows, also run at least one config-backed smoke command with a real config path.
If the change affects Yahoo provider accessibility or error classification, consider running `python scripts/verify_yahoo_access.py --config /path/to/config.yaml --sample-size 10 --intervals 1d,5m`. Use `--seed` only when you need reproducible sampling during debugging.

## Testing Ownership

- Unit tests for provider adapters, universe handling, parquet verification, schema rendering, and package CLI belong here.
- External applications should keep only integration tests that verify they delegate correctly to this package and respect the artifact boundary.

## Change Discipline

- Preserve backward compatibility of the parquet schema unless there is a deliberate versioned migration.
- Prefer additive config changes over silent behavior changes.
- Avoid assumptions about repository layout beyond this package.
- Keep committed configs generic. Machine-specific path layouts should live in an untracked local override such as `configs/config.local.yaml`, passed via `--config`, `TRADINGLAB_DATA_CONFIG`, or `TLD_CONFIG_PATH`.
- The local maintenance wrapper `scripts/run_daily_update_verify.sh` prefers `TLD_CONFIG_PATH`, then `configs/config.local.yaml`, then `configs/config.yaml`.
