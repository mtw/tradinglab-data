from __future__ import annotations

from pathlib import Path

import tradinglab_data.universe_build as ub


def test_merge_rows_adds_index_memberships():
    rows = ub._merge_rows({"sp500": [{"symbol": "AAPL", "isin": None, "source": "x"}], "djia": [{"symbol": "AAPL", "isin": None, "source": "y"}]})
    assert len(rows) == 1
    assert "DJIA" in (rows[0].get("index_memberships") or "")
    assert "SP500" in (rows[0].get("index_memberships") or "")


def test_build_universe_with_mocked_source(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(ub, "_sp500_rows", lambda: [{"symbol": "AAPL", "name": "Apple", "exchange": "NASDAQ", "country": "US", "source": "sp500_x", "active": 1, "isin": None}])
    monkeypatch.setattr(ub, "normalize_to_yahoo", lambda symbol, exchange, country: symbol)
    out = tmp_path / "universe.csv"
    df = ub.build_universe(indices=["sp500"], out_path=out, active_only=True)
    assert df.height == 1
    assert df.get_column("symbol").to_list() == ["AAPL"]
    assert out.exists()
