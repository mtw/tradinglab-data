from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_and_ci_workflow_reference_same_core_commands():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    commands = [
        "PYTHONPATH=src pytest -q tests",
        "PYTHONPATH=src python -m tradinglab_data.cli schema --format markdown",
        "python -m build",
        "python -m twine check dist/*",
    ]

    for command in commands:
        assert command in readme
        assert command in workflow
