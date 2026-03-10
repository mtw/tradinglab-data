# AGENTS.md

This repository is the standalone data-maintenance package behind TradingLab.

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

Those remain in the separate `tradinglab` repository/package.

## Hard Boundary Rules

- Do not add research or prediction logic here.
- Do not reintroduce provider fetches into TradingLab research code.
- Treat parquet and universe CSVs as public artifacts consumed by TradingLab and potentially other packages.
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
PYTHONPATH=src pytest -q tests
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
- TradingLab should keep only integration tests that verify it delegates correctly to this package and respects the artifact boundary.

## Change Discipline

- Preserve backward compatibility of the parquet schema unless there is a deliberate versioned migration.
- Prefer additive config changes over silent behavior changes.
- Avoid writing code that assumes the package lives inside the TradingLab monorepo.
