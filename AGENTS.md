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
- Treat `tradinglab-data` as Polars-first: public tabular Python APIs return `polars.DataFrame`, schemas use Polars dtypes, and pandas belongs only at external provider or parser boundaries before immediate conversion into Polars.
- Schema modifications require edits to `docs/PARQUET_SCHEMA.md`.
- Workflow modifications require edits to `docs/WORKFLOWS.md` when user-visible behavior changes.

## Source Of Truth

- package code: `src/tradinglab_data/`
- tests: `tests/`
- schema contract: `docs/PARQUET_SCHEMA.md`
- downstream consumer contract: `docs/API_CONTRACT.md`
- architecture overview: `ARCHITECTURE.md`
- release process: `RELEASE.md`

For downstream packages and agentic consumers, treat `docs/API_CONTRACT.md` as the primary published statement of what this package provides and what downstream code may rely on.

## Commands To Run Before Finishing Changes

Package tests:

```bash
PYTHONPATH=src python -m pytest -q -m "not network" tests --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=85
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
- When universes, config-exposed artifact names, or maintained metadata shards change here, inspect sibling repositories for references to those names and sync their configs, docs, scripts, and tests where needed.
- Keep committed configs generic. Machine-specific path layouts should live in an untracked local override such as `configs/config.local.yaml`, passed via `--config`, `TRADINGLAB_DATA_CONFIG`, or `TLD_CONFIG_PATH`.
- The local maintenance wrapper `scripts/run_daily_update_verify.sh` prefers `TLD_CONFIG_PATH`, then `configs/config.local.yaml`, then `configs/config.yaml`.

## Restart / Resume Notes

Use this section to avoid rediscovering the current intraday setup when resuming work later.

Current intraday topology:

- `extended_hours.intraday_root`
  - legacy monitoring cache
  - can include pre/regular/post data
  - remains separate from the new stores
- `intraday.research_root`
  - canonical regular-session research store
  - current first implementation is `5m` US stock/ETF only
- `intraday_live.live_root`
  - session-aware operational live store
  - current first implementation is `5m` US stock/ETF only
- `intraday-sync`
  - shared workflow that fetches Yahoo `5m` once with `prepost=True`
  - writes `intraday_live`
  - derives `intraday_research` from the same fetched frames

Important implementation details already in the package:

- `python -m tradinglab_data.cli ...` works because `src/tradinglab_data/cli.py` has a `__main__` entrypoint.
- Shared sync entrypoint:
  - `tradinglab-data intraday-sync backfill`
  - `tradinglab-data intraday-sync update`
- Live workflow entrypoint:
  - `tradinglab-data intraday-live backfill|update|validate|inspect`
- Research workflow entrypoint:
  - `tradinglab-data intraday backfill|update|validate|inspect`

Real local environment assumptions discovered in this repo:

- tracked generic config:
  - `configs/config.yaml`
  - points to repo-local `var/...`
- real machine-specific config:
  - `configs/config.local.yaml`
  - points to sibling store `../tradinglab-store/...`
- when the task is about the real maintained parquet store, prefer `configs/config.local.yaml`

Known real local store roots from `configs/config.local.yaml`:

- daily:
  - `~/Python/Investing/tradinglab-store/parquet/daily`
- legacy intraday monitoring cache:
  - `~/Python/Investing/tradinglab-store/parquet/intraday`
- intraday research:
  - `~/Python/Investing/tradinglab-store/parquet/intraday_research/5m`
- intraday live:
  - `~/Python/Investing/tradinglab-store/parquet/intraday_live/5m`
- local universes:
  - `~/Python/Investing/tradinglab-store/meta/universes/`

Current pilot/live universe names:

- `intraday_pilot`
  - semiconductor + software S&P 500 pilot slice
  - CSV in repo under `var/meta/universes/intraday_pilot.csv`
  - real local copy expected under sibling store metadata root
- `intraday_live_core`
  - current live-core seed universe
  - CSV is intentionally not tracked because `var/` is gitignored
  - if missing locally, recreate/copy it into `../tradinglab-store/meta/universes/intraday_live_core.csv`

Last known successful real-store intraday run:

- command:
  - `PYTHONPATH=src .venv/bin/python -m tradinglab_data.cli --config configs/config.local.yaml intraday-sync backfill --universe intraday_live_core`
- result:
  - `33` symbols fetched
  - `33` live files written
  - `33` research files written
- retained live range:
  - `2026-02-13T09:00:00` to `2026-05-11T19:50:00`
- retained research range:
  - `2026-02-13T14:30:00` to `2026-05-11T19:50:00`

Preferred resume commands:

```bash
PYTHONPATH=src .venv/bin/python -m tradinglab_data.cli --config configs/config.local.yaml intraday-sync update --universe intraday_live_core
PYTHONPATH=src .venv/bin/python -m tradinglab_data.cli --config configs/config.local.yaml intraday-live validate --universe intraday_live_core
PYTHONPATH=src .venv/bin/python -m tradinglab_data.cli --config configs/config.local.yaml intraday validate --universe intraday_live_core
```

If the user asks whether data really landed in the sibling store, verify these exact paths first:

- `../tradinglab-store/parquet/intraday_live/5m`
- `../tradinglab-store/parquet/intraday_research/5m`

If Yahoo `5m` retrieval appears broken:

- first distinguish config-path mistakes from provider/network problems
- check whether the command was run against `configs/config.yaml` instead of `configs/config.local.yaml`
- sandboxed runs may fail because Yahoo host resolution is blocked; if a real provider check matters, rerun outside the sandbox with approval
- useful diagnostic:
  - `PYTHONPATH=src .venv/bin/python scripts/verify_yahoo_access.py --config configs/config.local.yaml --sample-size 5 --intervals 5m --seed 42`

When modifying intraday behavior again:

- keep `intraday_live` and `intraday_research` semantically separate
- update `docs/WORKFLOWS.md` for user-visible workflow changes
- update `docs/PARQUET_SCHEMA.md` for schema changes
- update `docs/API_CONTRACT.md` for new public CLI/config/Python surface
