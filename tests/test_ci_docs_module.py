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


def test_publish_workflow_uses_trusted_publishing():
    workflow = (REPO_ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")
    release_doc = (REPO_ROOT / "RELEASE.md").read_text(encoding="utf-8")

    assert "pypa/gh-action-pypi-publish@release/v1" in workflow
    assert "id-token: write" in workflow
    assert "environment: pypi" in workflow
    assert "workflow filename: `publish.yml`" in release_doc
    assert "environment name: `pypi`" in release_doc


def test_pyproject_and_ci_define_hatch_quality_environment():
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "[tool.hatch.envs.default]" in pyproject
    assert 'features = ["test", "dev"]' in pyproject
    assert '[tool.hatch.envs.default.scripts]' in pyproject
    assert 'test = "pytest -q --cov=src/tradinglab_data --cov-report=term-missing --cov-fail-under=85 -m \'not network\' tests"' in pyproject
    assert 'lint = "ruff check src tests scripts"' in pyproject
    assert 'typecheck = "mypy src"' in pyproject
    assert "python -m pip install hatch" in workflow
    assert "Smoke-check Hatch environment tools" in workflow
    assert "hatch run python - <<'PY'" in workflow


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
