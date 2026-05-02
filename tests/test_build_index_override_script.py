from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

from tests._load import load_script_module

mod = load_script_module("build_index_override")


def test_build_dax_mdax_rejects_unknown_index():
    with pytest.raises(RuntimeError, match="dax or mdax"):
        mod.build_dax_mdax("foo")


def test_main_dispatches_and_writes_csv(tmp_path: Path, monkeypatch):
    df = pl.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "name": ["Apple", "Microsoft"],
            "country": ["US", "US"],
            "active": [1, 1],
        }
    )
    out = tmp_path / "sp500.csv"
    monkeypatch.setattr(mod, "build_sp500", lambda: df)
    monkeypatch.setattr(sys, "argv", ["build_index_override.py", "sp500", "--out", str(out)])

    mod.main()

    loaded = pl.read_csv(str(out))
    assert loaded.height == 2
    assert loaded.get_column("symbol").to_list() == ["AAPL", "MSFT"]
