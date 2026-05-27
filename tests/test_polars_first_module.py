from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

PANDAS_BOUNDARY_MODULES = {
    "src/tradinglab_data/_yf_utils.py",  # yfinance returns pandas objects.
    "src/tradinglab_data/universe_build.py",  # pandas.read_html is the maintained HTML table parser.
}


def _imports_pandas(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "pandas" or alias.name.startswith("pandas.") for alias in node.names):
                return True
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            if node.module == "pandas" or node.module.startswith("pandas."):
                return True
    return False


def test_package_modules_do_not_import_pandas_outside_provider_boundaries():
    offenders: list[str] = []
    for path in sorted((REPO_ROOT / "src" / "tradinglab_data").rglob("*.py")):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in PANDAS_BOUNDARY_MODULES:
            continue
        if _imports_pandas(path):
            offenders.append(rel)

    assert offenders == []


def test_pandas_import_detector_ignores_comments_and_strings(tmp_path: Path):
    harmless = tmp_path / "harmless.py"
    harmless.write_text(
        '"""Example: import pandas as pd."""\n'
        "# from pandas import DataFrame\n"
        "VALUE = 'import pandas'\n",
        encoding="utf-8",
    )
    offender = tmp_path / "offender.py"
    offender.write_text("import pandas as pd\n", encoding="utf-8")

    assert _imports_pandas(harmless) is False
    assert _imports_pandas(offender) is True
