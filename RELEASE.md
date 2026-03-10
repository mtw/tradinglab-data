# Release And Repo Split

## Goal

Turn `packages/tradinglab-data/` into its own repository and publish it as `tradinglab-data`.

## Source Of Truth

The source of truth is:

- `packages/tradinglab-data/src/tradinglab_data/`

TradingLab should import `tradinglab_data` from the installed package, not from in-repo bridge code.

## Split Steps

1. Create a new repository, for example `tradinglab-data`.
2. Copy the contents of `packages/tradinglab-data/` into the root of that new repository.
3. Initialize Git in the new repository and commit the copied package.
4. In the TradingLab repository:
   - add a normal dependency on published `tradinglab-data`
   - keep TradingLab importing `tradinglab_data.*`

## Local Monorepo Workflow

Before the split:

```bash
pip install -e ./packages/tradinglab-data
pip install -e .
```

After the split, during local co-development:

```bash
pip install -e /path/to/tradinglab-data
pip install -e /path/to/tradinglab
```

## PyPI Release Checklist

1. Bump version in `pyproject.toml`
2. Run package tests
3. Build distributions
4. Verify `tradinglab-data` CLI locally
5. Publish to TestPyPI
6. Publish to PyPI
7. Update TradingLab dependency pin

Package CI:

- `.github/workflows/tradinglab-data.yml`

## Build Commands

```bash
cd packages/tradinglab-data
python -m build
twine check dist/*
```
