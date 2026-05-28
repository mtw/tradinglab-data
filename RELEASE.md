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
10. Push the release branch and release tag to GitHub
11. Let `.github/workflows/publish.yml` publish to PyPI through Trusted Publishing
12. Verify the PyPI release and clean install path
13. Update consumer dependency pins

Package CI:

- `.github/workflows/ci.yml`
- tests Python 3.10, 3.11, 3.12, and 3.13
- `.github/workflows/publish.yml`
  - builds distributions on `v*` tags, published GitHub releases, or manual dispatch
  - publishes to PyPI through GitHub OIDC Trusted Publishing

## Trusted Publishing Setup

Configure the `tradinglab-data` project on PyPI to trust this GitHub repository.

PyPI Trusted Publisher fields:

- owner: your GitHub owner or organization
- repository: `tradinglab-data`
- workflow filename: `publish.yml`
- environment name: `pypi`

GitHub repository requirements:

- keep `.github/workflows/publish.yml` as the publishing workflow filename registered on PyPI
- create a GitHub environment named `pypi`
- no `TWINE_USERNAME`, `TWINE_PASSWORD`, or PyPI API token secret is needed for publishing

Release execution:

```bash
git push origin <branch>
git push origin vX.Y.Z
```

The publish workflow will:

1. build `sdist` and wheel artifacts
2. run `twine check dist/*`
3. publish to PyPI using `pypa/gh-action-pypi-publish` with `id-token: write`

## Build Commands

```bash
python -m build
twine check dist/*
```
