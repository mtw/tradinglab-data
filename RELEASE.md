# Release

## Goal

Publish this repository as the `tradinglab-data` package on PyPI.

## Source Of Truth

The source of truth is:

- `src/tradinglab_data/`

TradingLab should import `tradinglab_data` from the installed package, not from in-repo bridge code.

## PyPI Release Checklist

1. Bump version in `pyproject.toml`
2. Run `PYTHONPATH=src pytest -q tests`
3. Run `PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown`
4. Build distributions with `python -m build`
5. Check distributions with `python -m twine check dist/*`
6. Verify the CLI locally with a real config path, for example:
   - `tradinglab-data --config /path/to/config.yaml update`
   - `tradinglab-data --config /path/to/config.yaml report-parquet-store`
7. Publish to TestPyPI
8. Publish to PyPI
9. Tag the release in Git
10. Update downstream dependency pins

Package CI:

- `.github/workflows/ci.yml`
- tests Python 3.10, 3.11, 3.12, and 3.13

## Build Commands

```bash
python -m build
twine check dist/*
```
