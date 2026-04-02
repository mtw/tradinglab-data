# Release

## Goal

Publish this repository as the `tradinglab-data` package on PyPI.

## Source Of Truth

The source of truth is:

- `src/tradinglab_data/`

Downstream consumers should import `tradinglab_data` from the installed package, not from in-repo bridge code.

## PyPI Release Checklist

1. Bump version in `pyproject.toml`
2. Run `python -m ruff check src tests`
3. Run `python -m mypy src`
4. Run `PYTHONPATH=src python -m pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=60 -m "not network" tests`
5. Run `PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown`
6. Build distributions with `python -m build`
7. Check distributions with `python -m twine check dist/*`
8. Verify the CLI locally with a real config path, for example:
   - `tradinglab-data --config /path/to/config.yaml update`
   - `tradinglab-data --config /path/to/config.yaml report-parquet-store`
9. Publish to TestPyPI
10. Publish to PyPI
11. Tag the release in Git
12. Update downstream dependency pins

Package CI:

- `.github/workflows/ci.yml`
- tests Python 3.10, 3.11, 3.12, and 3.13

## Build Commands

```bash
python -m build
twine check dist/*
```
