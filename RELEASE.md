# Release

## Goal

Publish this repository as the `tradinglab-data` package on PyPI.

## Source Of Truth

The source of truth is:

- `src/tradinglab_data/`

Consumers should import `tradinglab_data` from the installed package, not from in-repo bridge code.

## PyPI Release Checklist

1. Bump version in `pyproject.toml`
2. Run `python -m ruff check src tests`
3. Run `python -m mypy src`
4. Run `PYTHONPATH=src python -m pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=85 -m "not network" tests`
5. Run `PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown`
6. Verify `tradinglab_data.DATAFRAME_POLICY == "polars-first"` remains true for public tabular Python APIs
7. Build distributions with `python -m build`
8. Check distributions with `python -m twine check dist/*`
9. Verify the CLI locally with a real config path, for example:
   - `tradinglab-data --config /path/to/config.yaml update`
   - `tradinglab-data --config /path/to/config.yaml report-parquet-store`
   - `tradinglab-data --config /path/to/config.yaml intraday-sync update --universe intraday_live_core`
   - `tradinglab-data --config /path/to/config.yaml intraday-live validate --universe intraday_live_core`
   - `tradinglab-data --config /path/to/config.yaml intraday validate --universe intraday_live_core`
10. Publish to TestPyPI
11. Publish to PyPI
12. Tag the release in Git
13. Update consumer dependency pins

Package CI:

- `.github/workflows/ci.yml`
- tests Python 3.10, 3.11, 3.12, and 3.13

## Build Commands

```bash
python -m build
twine check dist/*
```
