from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_and_ci_workflow_reference_same_core_commands():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    commands = [
        "python -m ruff check src tests",
        "python -m mypy src",
        'PYTHONPATH=src python -m pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=85 -m "not network" tests',
        "PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown",
        "python -m build",
        "python -m twine check dist/*",
    ]

    for command in commands:
        assert command in readme
        assert command in workflow
    assert 'manifest["dataframe_policy"] == "polars-first"' in workflow


def test_user_and_machine_docs_declare_polars_first_contract():
    files = [
        "README.md",
        "AGENTS.md",
        "ARCHITECTURE.md",
        "RELEASE.md",
        "docs/API_CONTRACT.md",
        "docs/BOUNDARY.md",
        "docs/CONSUMER_COMPATIBILITY_CHECKLIST.md",
        "docs/PARQUET_SCHEMA.md",
        "docs/WORKFLOWS.md",
        "pyproject.toml",
    ]

    for relative_path in files:
        text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert "polars-first" in text.lower(), relative_path
