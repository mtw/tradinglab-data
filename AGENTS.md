# AGENTS.md

This repository is a standalone data-maintenance package.

## What This Repo Owns

- universe loading and merged-universe construction
- ticker normalization and override mapping
- market data retrieval from upstream providers
- normalization into the canonical parquet schema
- daily parquet update workflows
- extended-hours intraday update workflows
- parquet sanity checks and verification primitives
- schema specification for parquet artifacts

## What This Repo Does Not Own

- signal generation
- screening decisions
- plotting/report UX outside of data-maintenance reports
- research workflows
- predictive modeling
- experiment registry analysis

Those remain in separate downstream applications.

## Hard Boundary Rules

- Do not add research or prediction logic here.
- Do not reintroduce provider fetches into downstream research code.
- Treat parquet and universe CSVs as public artifacts consumed by downstream packages.
- Schema changes must update `docs/PARQUET_SCHEMA.md`.
- Workflow changes must update `docs/WORKFLOWS.md` when user-visible behavior changes.

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

If the change affects update behavior, also run at least one config-backed smoke command with a real config path.

## Testing Ownership

- Unit tests for provider adapters, universe handling, parquet verification, schema rendering, and package CLI belong here.
- Downstream applications should keep only integration tests that verify they delegate correctly to this package and respect the artifact boundary.

## Change Discipline

- Preserve backward compatibility of the parquet schema unless there is a deliberate versioned migration.
- Prefer additive config changes over silent behavior changes.
- Avoid writing code that assumes the package lives inside any larger monorepo.
